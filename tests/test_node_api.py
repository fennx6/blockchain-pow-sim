"""Unit tests for the per-node Flask REST API."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template
from chainsim.core.chain import Blockchain
from chainsim.core.mempool import Mempool
from chainsim.core.pow import mine
from chainsim.core.transaction import MAX_IDENTIFIER_LEN
from chainsim.node.api import create_app
from chainsim.node.miner import Miner
from chainsim.types import AddResult, Transaction


def _make_node(
    *,
    difficulty: int = 1,
    on_tx_received=None,
    peers=("node-2", "node-3"),
    state_provider=None,
):
    mempool = Mempool()
    chain = Blockchain(difficulty=difficulty, mempool=mempool)
    miner = Miner(
        node_id="node-1",
        chain=chain,
        mempool=mempool,
        mode="compete",
        block_size=10,
        nonce_space=100_000,
    )
    app = create_app(
        node_id="node-1",
        chain=chain,
        mempool=mempool,
        miner=miner,
        peers=peers,
        on_tx_received=on_tx_received,
        state_provider=state_provider,
    )
    return chain, mempool, miner, app.test_client()


def test_post_tx_validates_adds_logs_and_forwards(log_dir_env: Path) -> None:
    forwarded: list[Transaction] = []
    _, mempool, _, client = _make_node(on_tx_received=forwarded.append)

    resp = client.post(
        "/tx",
        json={"sender": "alice", "receiver": "bob", "amount": 25.5, "timestamp_ms": 12345},
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["status"] == "accepted"
    assert body["forwarded"] is True
    assert body["transaction"]["origin_node"] == "node-1"
    assert len(forwarded) == 1
    assert mempool.size() == 1

    # Submitting the exact same tx again returns 200 duplicate without re-forwarding.
    dup = client.post(
        "/tx",
        json={"sender": "alice", "receiver": "bob", "amount": 25.5, "timestamp_ms": 12345},
    )
    assert dup.status_code == 200
    assert dup.get_json()["status"] == "duplicate"
    assert len(forwarded) == 1

    log_file = log_dir_env / "node-1.jsonl"
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert any(ev["event"] == "tx_received" and ev["tx_id"] == forwarded[0].tx_id for ev in events)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "not-an-object",
        {},
        {"sender": "alice", "receiver": "bob"},
        {"sender": "", "receiver": "bob", "amount": 1.0},
        {"sender": "alice", "receiver": "alice", "amount": 1.0},
        {"sender": "a" * (MAX_IDENTIFIER_LEN + 1), "receiver": "bob", "amount": 1.0},
        {"sender": "alice", "receiver": "bob", "amount": 0},
        {"sender": "alice", "receiver": "bob", "amount": -5},
        {"sender": "alice", "receiver": "bob", "amount": True},
        {"sender": "alice", "receiver": "bob", "amount": "10"},
        {"sender": "alice", "receiver": "bob", "amount": 1e12},
        {"sender": "alice", "receiver": "bob", "amount": 1.0, "timestamp_ms": -1},
    ],
)
def test_post_tx_rejects_invalid_payloads(payload: object) -> None:
    _, mempool, _, client = _make_node()
    if payload is None:
        resp = client.post("/tx", data="not json", content_type="text/plain")
    else:
        resp = client.post("/tx", json=payload)
    assert resp.status_code == 400
    assert "error" in resp.get_json()
    assert mempool.size() == 0


def test_post_tx_handles_forward_failure_and_confirmed_conflict(log_dir_env: Path) -> None:
    def failing_forward(_: Transaction) -> None:
        raise RuntimeError("peer unreachable")

    chain, mempool, _, client = _make_node(on_tx_received=failing_forward)
    resp = client.post(
        "/tx",
        json={"sender": "alice", "receiver": "bob", "amount": 10.0, "timestamp_ms": 500},
    )
    assert resp.status_code == 201
    assert resp.get_json()["forwarded"] is False

    # Mine the tx into block 1, then re-posting the same tx must return 409 Conflict.
    b1 = mine(
        build_block_template(
            1, chain.tip().hash, mempool.all(), difficulty=1, miner_id="node-1", timestamp_ms=600
        ),
        0,
        100_000,
        threading.Event(),
    )
    assert b1 is not None
    assert chain.add_block(b1) is AddResult.ACCEPTED

    conflict = client.post(
        "/tx",
        json={"sender": "alice", "receiver": "bob", "amount": 10.0, "timestamp_ms": 500},
    )
    assert conflict.status_code == 409


def test_get_chain_supports_from_filter_and_validates_query() -> None:
    chain, _, _, client = _make_node()
    b1 = mine(
        build_block_template(
            1, chain.tip().hash, [], difficulty=1, miner_id="node-1", timestamp_ms=10
        ),
        0,
        100_000,
        threading.Event(),
    )
    assert b1 is not None
    assert chain.add_block(b1) is AddResult.ACCEPTED

    full = client.get("/chain")
    assert full.status_code == 200
    full_data = full.get_json()
    assert full_data["height"] == 1
    assert len(full_data["blocks"]) == 2

    partial = client.get("/chain?from=1")
    assert partial.status_code == 200
    assert len(partial.get_json()["blocks"]) == 1
    assert partial.get_json()["blocks"][0]["index"] == 1

    assert client.get("/chain?from=-1").status_code == 400
    assert client.get("/chain?from=abc").status_code == 400


def test_get_status_matches_node_status_contract_and_mempool_endpoint(log_dir_env: Path) -> None:
    _, _, _, client = _make_node(
        peers=lambda: ["node-2", "node-3", "node-4"],
        state_provider=lambda: "syncing",
    )
    client.post(
        "/tx", json={"sender": "alice", "receiver": "bob", "amount": 3.0, "timestamp_ms": 1}
    )

    status_resp = client.get("/status")
    assert status_resp.status_code == 200
    assert status_resp.headers["Cache-Control"] == "no-store"
    status = status_resp.get_json()
    assert status["node_id"] == "node-1"
    assert status["height"] == 0
    assert len(status["tip_hash"]) == 64
    assert status["peers"] == ["node-2", "node-3", "node-4"]
    assert status["mempool_size"] == 1
    assert status["hashrate"] == 0.0
    assert status["state"] == "syncing"
    assert status["meta"]["difficulty"] == 1
    assert status["meta"]["mining_mode"] == "compete"

    mempool_resp = client.get("/mempool")
    assert mempool_resp.status_code == 200
    mempool_body = mempool_resp.get_json()
    assert mempool_body["size"] == 1
    assert mempool_body["transactions"][0]["sender"] == "alice"
