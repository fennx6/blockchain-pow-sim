"""Tests for catch-up sync and fork reconciliation."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Sequence
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.chain import Blockchain
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.node.grpc_server import NodeService, build_server
from chainsim.node.sync import SyncService, catch_up
from chainsim.types import AddResult, Block, NodeStatus, Transaction


def _tx(seed: int) -> Transaction:
    return create_transaction(
        sender=f"addr-{seed}",
        receiver=f"addr-{seed}-b",
        amount=1.0 + seed / 100,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000 + seed,
    )


def _mine_block(
    prev: Block,
    txs: Sequence[Transaction],
    *,
    index: int,
    miner: str,
    ts: int,
) -> Block:
    template = build_block_template(
        index=index,
        prev_hash=prev.hash,
        transactions=txs,
        difficulty=1,
        miner_id=miner,
        timestamp_ms=ts,
    )
    block = mine(template, 0, 1_000_000, threading.Event())
    assert block is not None
    return block


def _build_chain(
    length: int,
    *,
    seed_base: int = 0,
    miner: str = "node-1",
    ts_base: int = 1_700_000_000_000,
) -> list[Block]:
    chain = [create_genesis_block(1)]
    for index in range(1, length + 1):
        chain.append(
            _mine_block(
                chain[-1],
                [_tx(seed_base + index)],
                index=index,
                miner=miner,
                ts=ts_base + index,
            )
        )
    return chain


def _status_for(blocks: Sequence[Block], *, node_id: str = "peer-1") -> NodeStatus:
    tip = blocks[-1]
    return NodeStatus(
        node_id=node_id,
        height=tip.index,
        tip_hash=tip.hash,
        peers=[],
        mempool_size=0,
        hashrate=0.0,
        state="up",
    )


class _FakeSyncClient:
    def __init__(
        self,
        blocks: Sequence[Block],
        *,
        fail_status: bool = False,
        fail_blocks: bool = False,
    ) -> None:
        self.blocks = list(blocks)
        self.fail_status = fail_status
        self.fail_blocks = fail_blocks
        self.status_calls = 0
        self.block_calls: list[int] = []
        self.closed = False

    def get_status(self) -> NodeStatus:
        self.status_calls += 1
        if self.fail_status:
            raise RuntimeError("status unavailable")
        return _status_for(self.blocks)

    def get_blocks(self, from_index: int) -> list[Block]:
        self.block_calls.append(from_index)
        if self.fail_blocks:
            raise RuntimeError("blocks unavailable")
        return [block for block in self.blocks if block.index >= from_index]

    def close(self) -> None:
        self.closed = True


def _read_events(log_dir: Path, node_id: str) -> list[dict]:
    path = log_dir / f"{node_id}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_catch_up_pulls_missing_blocks(log_dir_env: Path) -> None:
    local = Blockchain(1)
    peer_chain = _build_chain(2)
    client = _FakeSyncClient(peer_chain)

    pulled = catch_up(local, ["peer-1"], node_id="node-1", client_factory=lambda _: client)

    assert pulled == 2
    assert local.height() == 2
    assert local.tip().hash == peer_chain[-1].hash
    assert client.block_calls == [1]
    assert client.closed is True

    events = _read_events(log_dir_env, "node-1")
    starts = [event for event in events if event["event"] == "sync_start"]
    dones = [event for event in events if event["event"] == "sync_done"]
    assert starts[0]["local_height"] == 0
    assert starts[0]["peer_height"] == 2
    assert dones[0]["blocks_pulled"] == 2
    assert dones[0]["blocks_added"] == 2
    assert dones[0]["fork_replaced"] is False


def test_catch_up_skips_peer_not_ahead(log_dir_env: Path) -> None:
    local = Blockchain(1)
    peer_chain = _build_chain(1)
    assert local.add_block(peer_chain[1]) == AddResult.ACCEPTED
    client = _FakeSyncClient(peer_chain)

    pulled = catch_up(local, ["peer-1"], node_id="node-1", client_factory=lambda _: client)

    assert pulled == 0
    assert client.status_calls == 1
    assert client.block_calls == []
    assert client.closed is True
    assert not (log_dir_env / "node-1.jsonl").exists()


def test_catch_up_replaces_heavier_fork(log_dir_env: Path) -> None:
    local_chain = _build_chain(1, seed_base=100, miner="node-1", ts_base=1_700_000_000_000)
    peer_chain = _build_chain(2, seed_base=200, miner="node-2", ts_base=1_700_000_001_000)

    local = Blockchain(1)
    assert local.add_block(local_chain[1]) == AddResult.ACCEPTED
    client = _FakeSyncClient(peer_chain)

    pulled = catch_up(local, ["peer-1"], node_id="node-1", client_factory=lambda _: client)

    assert pulled == 4
    assert local.height() == 2
    assert local.tip().hash == peer_chain[-1].hash
    assert client.block_calls == [2, 0]

    events = _read_events(log_dir_env, "node-1")
    done = [event for event in events if event["event"] == "sync_done"][0]
    assert done["fork_replaced"] is True
    assert done["blocks_added"] == 1


def test_catch_up_skips_unreachable_peer() -> None:
    local = Blockchain(1)
    client = _FakeSyncClient([], fail_status=True)

    pulled = catch_up(local, ["peer-1"], node_id="node-1", client_factory=lambda _: client)

    assert pulled == 0
    assert local.height() == 0
    assert client.closed is True


def test_catch_up_logs_failed_get_blocks(log_dir_env: Path) -> None:
    local = Blockchain(1)
    client = _FakeSyncClient(_build_chain(2), fail_blocks=True)

    pulled = catch_up(local, ["peer-1"], node_id="node-1", client_factory=lambda _: client)

    assert pulled == 0
    assert local.height() == 0
    events = _read_events(log_dir_env, "node-1")
    assert [event["event"] for event in events] == ["sync_start", "sync_done"]
    assert events[1]["blocks_pulled"] == 0
    assert "blocks unavailable" in events[1]["error"]


def test_catch_up_skips_invalid_peer_address() -> None:
    def factory(_: str) -> _FakeSyncClient:
        raise ValueError("bad address")

    assert catch_up(Blockchain(1), ["bad"], client_factory=factory) == 0


def test_sync_service_sync_now_uses_peers_provider() -> None:
    local = Blockchain(1)
    peer_chain = _build_chain(2)
    client = _FakeSyncClient(peer_chain)
    service = SyncService(
        node_id="node-1",
        chain=local,
        peers_provider=lambda: ["peer-1"],
        interval_s=0.05,
        client_factory=lambda _: client,
    )

    assert service.peers() == ["peer-1"]
    assert service.sync_now() == 2
    assert local.height() == 2
    assert service.sync_now() == 0


def test_sync_service_rejects_invalid_interval() -> None:
    with pytest.raises(ValueError):
        SyncService(
            node_id="node-1",
            chain=Blockchain(1),
            peers_provider=[],
            interval_s=0,
        )


def test_sync_service_background_loop() -> None:
    local = Blockchain(1)
    peer_chain = _build_chain(1)
    client = _FakeSyncClient(peer_chain)
    service = SyncService(
        node_id="node-1",
        chain=local,
        peers_provider=lambda: ["peer-1"],
        interval_s=0.01,
        timeout_s=1.0,
        client_factory=lambda _: client,
    )

    service.start()
    try:
        deadline = time.monotonic() + 2.0
        while local.height() < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        service.stop()

    assert local.height() == 1


def test_sync_service_invokes_on_chain_updated_only_when_tip_changes() -> None:
    local = Blockchain(1)
    peer_chain = _build_chain(2)
    client = _FakeSyncClient(peer_chain)
    notifications = 0

    def on_updated() -> None:
        nonlocal notifications
        notifications += 1

    service = SyncService(
        node_id="node-1",
        chain=local,
        peers_provider=lambda: ["peer-1"],
        interval_s=0.05,
        client_factory=lambda _: client,
        on_chain_updated=on_updated,
    )

    assert service.sync_now() == 2
    assert notifications == 1
    assert service.sync_now() == 0
    assert notifications == 1


def test_catch_up_uses_real_node_service(log_dir_env: Path) -> None:
    peer_chain = _build_chain(2)
    service = NodeService(
        node_id="node-2",
        on_transaction=lambda _: (True, "accepted"),
        on_block=lambda _: (True, "accepted"),
        get_blocks=lambda index: [block for block in peer_chain if block.index >= index],
        get_status=lambda: _status_for(peer_chain, node_id="node-2"),
    )
    server, port = build_server(service, host="127.0.0.1", port=0)
    server.start()
    try:
        local = Blockchain(1)
        pulled = catch_up(local, [f"127.0.0.1:{port}"], node_id="node-1", timeout_s=1.0)
    finally:
        server.stop(0)

    assert pulled == 2
    assert local.height() == 2
    assert local.tip().hash == peer_chain[-1].hash
    events = _read_events(log_dir_env, "node-1")
    assert [event["event"] for event in events].count("sync_done") == 1
