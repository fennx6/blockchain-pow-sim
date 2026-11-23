"""Tests for transaction and block gossip."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.node.gossip import Gossip
from chainsim.node.grpc_client import PeerCallResult
from chainsim.node.grpc_server import NodeService, build_server
from chainsim.types import Block, NodeStatus, Transaction


def _tx(seed: int) -> Transaction:
    return create_transaction(
        sender=f"addr-{seed}",
        receiver=f"addr-{seed}-b",
        amount=1.0 + seed / 100,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000 + seed,
    )


def _block(tx: Transaction) -> Block:
    genesis = create_genesis_block(1)
    template = build_block_template(
        index=1,
        prev_hash=genesis.hash,
        transactions=[tx],
        difficulty=1,
        miner_id="node-1",
        timestamp_ms=1_700_000_000_001,
    )
    block = mine(template, 0, 1_000_000, threading.Event())
    assert block is not None
    return block


class _FakeGossipClient:
    def __init__(self, name: str) -> None:
        self.name = name
        self.tx_calls: list[tuple[Transaction, str]] = []
        self.block_calls: list[tuple[Block, str]] = []
        self.closed = False
        self.fail_tx = False
        self.fail_block = False

    def submit_transaction(self, tx: Transaction, *, from_node: str = "") -> PeerCallResult:
        if self.fail_tx:
            raise RuntimeError("tx down")
        self.tx_calls.append((tx, from_node))
        return PeerCallResult(self.name, True, "ok")

    def announce_block(self, block: Block, *, from_node: str = "") -> PeerCallResult:
        if self.fail_block:
            raise RuntimeError("block down")
        self.block_calls.append((block, from_node))
        return PeerCallResult(self.name, True, "ok")

    def close(self) -> None:
        self.closed = True


def _status() -> NodeStatus:
    return NodeStatus(
        node_id="node-2",
        height=0,
        tip_hash="0" * 64,
        peers=[],
        mempool_size=0,
        hashrate=0.0,
        state="up",
    )


def _read_events(log_dir: Path, node_id: str) -> list[dict]:
    path = log_dir / f"{node_id}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_gossip_broadcasts_tx_and_block(log_dir_env: Path) -> None:
    clients = {"node-2": _FakeGossipClient("node-2"), "node-3": _FakeGossipClient("node-3")}
    gossip = Gossip(
        node_id="node-1",
        peers=["node-2", "node-3"],
        client_factory=lambda peer: clients[peer],
    )
    tx = _tx(1)
    block = _block(tx)

    tx_results = gossip.broadcast_tx(tx)
    block_results = gossip.broadcast_block(block)

    assert [result.ok for result in tx_results] == [True, True]
    assert [result.ok for result in block_results] == [True, True]
    for client in clients.values():
        assert client.tx_calls == [(tx, "node-1")]
        assert client.block_calls == [(block, "node-1")]

    events = _read_events(log_dir_env, "node-1")
    tx_events = [event for event in events if event["event"] == "tx_forwarded"]
    block_events = [event for event in events if event["event"] == "block_forwarded"]
    assert {event["to_node"] for event in tx_events} == {"node-2", "node-3"}
    assert {event["to_node"] for event in block_events} == {"node-2", "node-3"}
    assert all(event["from_node"] == "node-1" and event["ok"] is True for event in tx_events)
    assert all(event["tx_id"] == tx.tx_id for event in tx_events)
    assert all(event["block_hash"] == block.hash for event in block_events)

    gossip.close()
    assert all(client.closed for client in clients.values())


def test_gossip_reports_peer_failures(log_dir_env: Path) -> None:
    good = _FakeGossipClient("node-2")
    bad = _FakeGossipClient("node-3")
    bad.fail_tx = True
    clients = {"node-2": good, "node-3": bad}
    gossip = Gossip(
        node_id="node-1",
        peers=["node-2", "node-3"],
        client_factory=lambda peer: clients[peer],
    )

    results = gossip.broadcast_tx(_tx(2))

    assert [result.ok for result in results] == [True, False]
    assert "tx down" in results[1].message
    events = _read_events(log_dir_env, "node-1")
    failed = [event for event in events if event["event"] == "tx_forwarded" and not event["ok"]]
    assert len(failed) == 1
    assert failed[0]["to_node"] == "node-3"
    assert "tx down" in failed[0]["error"]


def test_gossip_reports_client_factory_failure(log_dir_env: Path) -> None:
    def factory(_: str) -> _FakeGossipClient:
        raise ValueError("bad address")

    gossip = Gossip(node_id="node-1", peers=["bad"], client_factory=factory)
    results = gossip.broadcast_tx(_tx(3))

    assert [result.ok for result in results] == [False]
    assert "bad address" in results[0].message
    assert gossip.peers() == ["bad"]


def test_gossip_set_peers_closes_removed_clients() -> None:
    clients = {peer: _FakeGossipClient(peer) for peer in ("node-2", "node-3", "node-4")}
    gossip = Gossip(
        node_id="node-1",
        peers=["node-2", "node-3"],
        client_factory=lambda peer: clients[peer],
    )
    gossip.broadcast_tx(_tx(4))
    assert clients["node-2"].closed is False

    gossip.set_peers(["node-2", "node-4"])

    assert gossip.peers() == ["node-2", "node-4"]
    assert clients["node-3"].closed is True
    assert clients["node-4"].closed is False

    gossip.broadcast_tx(_tx(5))
    assert len(clients["node-2"].tx_calls) == 2
    assert len(clients["node-3"].tx_calls) == 1
    assert len(clients["node-4"].tx_calls) == 1

    gossip.close()
    assert clients["node-2"].closed is True
    assert clients["node-4"].closed is True


def test_gossip_with_no_peers() -> None:
    gossip = Gossip(node_id="node-1", peers=[])

    assert gossip.broadcast_tx(_tx(6)) == []
    assert gossip.broadcast_block(_block(_tx(7))) == []


def test_gossip_normalizes_and_deduplicates_peers() -> None:
    gossip = Gossip(node_id="node-1", peers=["node-2", "node-2", " node-3 "])
    assert gossip.peers() == ["node-2", "node-3"]


def test_gossip_validates_constructor() -> None:
    with pytest.raises(ValueError):
        Gossip(node_id="", peers=[])
    with pytest.raises(ValueError):
        Gossip(node_id="node-1", peers=[], timeout_s=0)


def test_gossip_reaches_real_node_service(log_dir_env: Path) -> None:
    received_txs: list[Transaction] = []
    received_blocks: list[Block] = []
    service = NodeService(
        node_id="node-2",
        on_transaction=lambda tx: received_txs.append(tx) or (True, "accepted"),
        on_block=lambda block: received_blocks.append(block) or (True, "accepted"),
        get_blocks=lambda _: [],
        get_status=_status,
    )
    server, port = build_server(service, host="127.0.0.1", port=0)
    server.start()
    try:
        gossip = Gossip(node_id="node-1", peers=[f"127.0.0.1:{port}"], timeout_s=1.0)
        tx = _tx(8)
        block = _block(tx)
        tx_results = gossip.broadcast_tx(tx)
        block_results = gossip.broadcast_block(block)
        gossip.close()
    finally:
        server.stop(0)

    assert [result.ok for result in tx_results] == [True]
    assert [result.ok for result in block_results] == [True]
    assert received_txs == [tx]
    assert received_blocks == [block]
