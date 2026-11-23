"""Tests for the node runtime wiring."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Sequence
from pathlib import Path

from chainsim.config import Settings, load_settings
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.node.grpc_client import NodeClient, PeerCallResult
from chainsim.node.main import NodeRuntime
from chainsim.types import Block, NodeStatus, Transaction, WorkAssignment


def _settings(
    tmp_path: Path,
    *,
    mode: str = "compete",
    peers: str = "",
    node_id: str = "node-1",
) -> Settings:
    return load_settings(
        env={
            "NODE_ID": node_id,
            "NODE_REST_PORT": "18081",
            "NODE_GRPC_PORT": "50051",
            "PEERS": peers,
            "DIFFICULTY": "1",
            "MINING_MODE": mode,
            "BLOCK_SIZE": "50",
            "CONFIRMATIONS": "3",
            "HEARTBEAT_INTERVAL_S": "0.05",
            "HEARTBEAT_TIMEOUT_S": "0.05",
            "GRPC_TIMEOUT_S": "0.5",
            "TX_FORWARD_TIMEOUT_S": "0.5",
            "LOG_DIR": str(tmp_path / "logs"),
            "LOG_LEVEL": "WARNING",
        }
    )


def _tx(seed: int) -> Transaction:
    return create_transaction(
        sender=f"addr-{seed}",
        receiver=f"addr-{seed}-b",
        amount=1.0 + seed / 100,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000 + seed,
    )


def _block(
    prev: Block,
    txs: Sequence[Transaction],
    *,
    index: int = 1,
    miner: str = "node-2",
) -> Block:
    template = build_block_template(
        index=index,
        prev_hash=prev.hash,
        transactions=txs,
        difficulty=1,
        miner_id=miner,
        timestamp_ms=1_700_000_000_000 + index,
    )
    block = mine(template, 0, 1_000_000, threading.Event())
    assert block is not None
    return block


def _build_chain(length: int) -> list[Block]:
    chain = [create_genesis_block(1)]
    for index in range(1, length + 1):
        chain.append(_block(chain[-1], [_tx(index)], index=index))
    return chain


def _read_events(log_dir: Path, node_id: str) -> list[dict]:
    path = log_dir / f"{node_id}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


class _FakeSyncClient:
    def __init__(self, blocks: Sequence[Block]) -> None:
        self.blocks = list(blocks)
        self.closed = False

    def get_status(self) -> NodeStatus:
        tip = self.blocks[-1]
        return NodeStatus(
            node_id="peer-1",
            height=tip.index,
            tip_hash=tip.hash,
            peers=[],
            mempool_size=0,
            hashrate=0.0,
            state="up",
        )

    def get_blocks(self, from_index: int) -> list[Block]:
        return [block for block in self.blocks if block.index >= from_index]

    def close(self) -> None:
        self.closed = True


class _FakeCoordinatorClient:
    def __init__(self) -> None:
        self.results: list[WorkResult] = []
        self.closed = False

    @property
    def address(self) -> str:
        return "coordinator:50052"

    def submit_work_result(self, result: WorkResult) -> PeerCallResult:
        self.results.append(result)
        return PeerCallResult("coordinator:50052", True, "ok")

    def close(self) -> None:
        self.closed = True


def test_runtime_grpc_round_trip(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    runtime.start(start_miner=False)
    try:
        with NodeClient(f"127.0.0.1:{runtime.grpc_port}", timeout_s=1.0) as client:
            status = client.get_status()
            assert status.node_id == "node-1"
            assert status.height == 0

            tx = _tx(1)
            ack = client.submit_transaction(tx, from_node="client")
            assert ack.ok is True
            assert runtime.mempool.contains(tx.tx_id)

            block = _block(runtime.chain.tip(), [tx])
            ack = client.announce_block(block, from_node="node-2")
            assert ack.ok is True
            assert runtime.chain.height() == 1
    finally:
        runtime.stop()


def test_runtime_rest_post_tx_and_status(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        client = runtime.rest_app.test_client()
        response = client.post("/tx", json={"sender": "a", "receiver": "b", "amount": 1.0})
        assert response.status_code == 201
        assert runtime.mempool.size() == 1

        status = client.get("/status").get_json()
        assert status["node_id"] == "node-1"
        assert status["mempool_size"] == 1
        assert status["state"] == "idle"

        chain = client.get("/chain").get_json()
        assert chain["height"] == 0
    finally:
        runtime.stop()


def test_runtime_orphan_triggers_sync(tmp_path: Path, log_dir_env: Path) -> None:
    peer_chain = _build_chain(2)
    fake = _FakeSyncClient(peer_chain)
    runtime = NodeRuntime(
        _settings(tmp_path, peers="peer-1"),
        data_dir=tmp_path,
        grpc_port=0,
        sync_client_factory=lambda _: fake,
    )
    try:
        ok, message = runtime.handle_block(peer_chain[2])
        assert ok is True
        assert message == "orphan_sync_triggered"
        assert runtime.chain.height() == 2
        assert runtime.chain.tip().hash == peer_chain[-1].hash
        assert fake.closed is True

        events = _read_events(log_dir_env, "node-1")
        assert [event["event"] for event in events].count("sync_done") == 1
    finally:
        runtime.stop()


def test_runtime_handle_work_result_reports_to_coordinator(
    tmp_path: Path, log_dir_env: Path
) -> None:
    fake = _FakeCoordinatorClient()
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        runtime.handle_work_result(assignment, None)
        assert runtime.chain.height() == 1
        assert runtime.chain.tip().hash == block.hash
    finally:
        runtime.stop()

    assert len(fake.results) == 1
    result = fake.results[0]
    assert result.job_id == "job-1"
    assert result.node_id == "node-1"
    assert result.nonce == block.nonce
    assert result.found is True
    assert result.block_hash == block.hash
    assert fake.closed is True


def test_runtime_handle_work_result_skips_invalid_block(tmp_path: Path, log_dir_env: Path) -> None:
    fake = _FakeCoordinatorClient()
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        block.hash = "0" * 64
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        assert runtime.chain.height() == 0
    finally:
        runtime.stop()

    assert fake.results == []


def test_runtime_handle_block_duplicate_and_invalid(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assert runtime.handle_block(block) == (True, "accepted")
        assert runtime.handle_block(block) == (True, "duplicate")

        bad = _block(runtime.chain.tip(), [_tx(2)])
        bad.hash = "0" * 64
        assert runtime.handle_block(bad) == (False, "invalid")
        assert runtime.chain.height() == 1
    finally:
        runtime.stop()


def test_runtime_handle_transaction_duplicate_and_confirmed(
    tmp_path: Path, log_dir_env: Path
) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        tx = _tx(1)
        assert runtime.handle_transaction(tx) == (True, "accepted")
        assert runtime.handle_transaction(tx) == (True, "duplicate")

        block = _block(runtime.chain.tip(), [tx])
        assert runtime.handle_block(block) == (True, "accepted")
        assert runtime.handle_transaction(tx) == (True, "already_confirmed")
    finally:
        runtime.stop()


def test_runtime_persists_chain_across_restart(tmp_path: Path, log_dir_env: Path) -> None:
    settings = _settings(tmp_path)
    first = NodeRuntime(settings, data_dir=tmp_path, grpc_port=0)
    try:
        block = _block(first.chain.tip(), [_tx(1)])
        assert first.handle_block(block) == (True, "accepted")
    finally:
        first.stop()

    second = NodeRuntime(settings, data_dir=tmp_path, grpc_port=0)
    try:
        assert second.chain.height() == 1
        assert second.chain.tip().hash == block.hash
    finally:
        second.stop()


def test_runtime_pool_work_handlers(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path, mode="pool"), data_dir=tmp_path, grpc_port=0)
    try:
        template = _block(runtime.chain.tip(), [])
        assignment = WorkAssignment("job-1", template, 0, 100)
        assert runtime.miner.mode == "pool"
        assert runtime.handle_start_work(assignment) == (True, "assigned")
        assert runtime.handle_stop_work("job-1") == (True, "stopped")
    finally:
        runtime.stop()


def test_runtime_propagates_tx_and_block_to_peer(tmp_path: Path, log_dir_env: Path) -> None:
    first = NodeRuntime(
        _settings(tmp_path, node_id="node-a"),
        data_dir=tmp_path / "a",
        grpc_port=0,
    )
    second = NodeRuntime(
        _settings(tmp_path, node_id="node-b"),
        data_dir=tmp_path / "b",
        grpc_port=0,
    )
    first.start(start_miner=False)
    second.start(start_miner=False)
    try:
        first.gossip.set_peers([f"127.0.0.1:{second.grpc_port}"])
        second.gossip.set_peers([f"127.0.0.1:{first.grpc_port}"])

        response = first.rest_app.test_client().post(
            "/tx", json={"sender": "alice", "receiver": "bob", "amount": 1.0}
        )
        assert response.status_code == 201
        assert second.mempool.size() == 1

        tx = first.mempool.all()[0]
        block = _block(first.chain.tip(), [tx])
        assert first.handle_block(block) == (True, "accepted")
        results = first.gossip.broadcast_block(block)
        assert len(results) == 1
        assert results[0].ok is True

        deadline = time.monotonic() + 2.0
        while second.chain.height() == 0 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert second.chain.height() == 1
        assert second.chain.tip().hash == block.hash
        assert second.mempool.size() == 0
    finally:
        first.stop()
        second.stop()


def test_runtime_cleans_self_peer_and_status_meta(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(
        _settings(tmp_path, peers="node-1,node-2,node-2"),
        data_dir=tmp_path,
        grpc_port=0,
    )
    try:
        assert runtime.peers == ("node-2",)
        assert runtime.gossip.peers() == ["node-2"]

        status = runtime.status()
        assert status.node_id == "node-1"
        assert status.height == 0
        assert status.tip_hash == runtime.chain.genesis.hash
        assert status.peers == ["node-2"]
        assert status.mempool_size == 0
        assert status.hashrate == 0.0
        assert status.state == "idle"
        assert status.meta["difficulty"] == 1
        assert status.meta["mining_mode"] == "compete"
    finally:
        runtime.stop()
