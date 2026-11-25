"""Tests for the NodeService gRPC server and its client round-trips."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.node.grpc_client import NodeClient
from chainsim.node.grpc_server import NodeService, build_server
from chainsim.types import NodeStatus, Transaction, WorkAssignment


def _make_tx():
    return create_transaction(
        sender="addr-a",
        receiver="addr-b",
        amount=5.0,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000,
    )


def _make_block(tx, *, index: int = 1):
    genesis = create_genesis_block(difficulty=1)
    template = build_block_template(
        index=index,
        prev_hash=genesis.hash,
        transactions=[tx],
        difficulty=1,
        miner_id="node-1",
        timestamp_ms=1_700_000_000_001,
    )
    block = mine(template, 0, 10_000, threading.Event())
    assert block is not None
    return block


def _status() -> NodeStatus:
    return NodeStatus(
        node_id="node-1",
        height=1,
        tip_hash="a" * 64,
        peers=["node-2"],
        mempool_size=3,
        hashrate=99.5,
        state="mining",
        meta={"difficulty": 1},
    )


def _start(service: NodeService):
    server, port = build_server(service, host="127.0.0.1", port=0)
    server.start()
    return server, port


def test_submit_transaction_logs_peer(log_dir_env: Path) -> None:
    received: list[Transaction] = []

    def on_tx(tx: Transaction):
        received.append(tx)
        return True, "accepted"

    service = NodeService(
        node_id="node-1",
        on_transaction=on_tx,
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.submit_transaction(_make_tx(), from_node="node-2")
    finally:
        server.stop(0)

    assert result.ok is True
    assert result.message == "accepted"
    assert len(received) == 1

    events = [
        json.loads(line)
        for line in (log_dir_env / "node-1.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    tx_events = [event for event in events if event["event"] == "tx_received"]
    assert len(tx_events) == 1
    assert tx_events[0]["from_node"] == "node-2"
    assert tx_events[0]["to_node"] == "node-1"
    assert tx_events[0]["accepted"] is True


def test_invalid_transaction_is_rejected() -> None:
    received: list[Transaction] = []
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: received.append(tx) or (True, "accepted"),
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        bad_tx = Transaction(
            tx_id="bad-hash",
            sender="addr-a",
            receiver="addr-b",
            amount=1.0,
            timestamp_ms=1_700_000_000_000,
            origin_node="node-1",
        )
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.submit_transaction(bad_tx)
    finally:
        server.stop(0)

    assert result.ok is False
    assert "invalid transaction" in result.message
    assert received == []


def test_announce_block_and_stream_blocks() -> None:
    tx = _make_tx()
    block = _make_block(tx)
    genesis = create_genesis_block(difficulty=1)
    received: list = []
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda incoming: received.append(incoming) or (True, "accepted"),
        get_blocks=lambda index: [genesis, block][index:],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            ack = client.announce_block(block, from_node="node-2")
            from_zero = client.get_blocks(0)
            from_one = client.get_blocks(1)
    finally:
        server.stop(0)

    assert ack.ok is True
    assert received == [block]
    assert [item.hash for item in from_zero] == [genesis.hash, block.hash]
    assert [item.hash for item in from_one] == [block.hash]


def test_get_status_round_trip() -> None:
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            status = client.get_status()
    finally:
        server.stop(0)

    assert status.node_id == "node-1"
    assert status.height == 1
    assert status.tip_hash == "a" * 64
    assert status.peers == ["node-2"]
    assert status.mempool_size == 3
    assert status.hashrate == 99.5
    assert status.state == "mining"
    assert status.meta == {"difficulty": "1"}


def test_start_and_stop_work() -> None:
    assignments: list[WorkAssignment] = []
    stopped: list[str] = []
    block = _make_block(_make_tx())
    assignment = WorkAssignment("job-1", block, 0, 1000)
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda incoming: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
        start_work=lambda item: assignments.append(item) or (True, "assigned"),
        stop_work=lambda job_id: stopped.append(job_id) or (True, "stopped"),
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            start_ack = client.start_work(assignment)
            stop_ack = client.stop_work("job-1")
    finally:
        server.stop(0)

    assert start_ack.ok is True
    assert stop_ack.ok is True
    assert assignments == [assignment]
    assert stopped == ["job-1"]


def test_pool_mode_disabled_reports_error() -> None:
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.start_work(WorkAssignment("job-1", _make_block(_make_tx()), 0, 10))
    finally:
        server.stop(0)

    assert result.ok is False
    assert "pool mode" in result.message


def test_negative_from_index_is_rejected_by_client() -> None:
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            with pytest.raises(ValueError):
                client.get_blocks(-1)
    finally:
        server.stop(0)


def test_build_server_rejects_bad_port() -> None:
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: (True, "accepted"),
        on_block=lambda block: (True, "accepted"),
        get_blocks=lambda index: [],
        get_status=_status,
    )
    with pytest.raises(ValueError):
        build_server(service, port=70_000)


def test_announce_block_records_arrival_timestamp_before_handler(log_dir_env: Path) -> None:
    tx = _make_tx()
    block = _make_block(tx)

    def slow_on_block(_incoming):
        time.sleep(0.05)
        return True, "accepted"

    service = NodeService(
        node_id="node-1",
        on_transaction=lambda item: (True, "accepted"),
        on_block=slow_on_block,
        get_blocks=lambda index: [],
        get_status=_status,
    )
    server, port = _start(service)
    try:
        with NodeClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            ack = client.announce_block(block, from_node="node-2")
    finally:
        server.stop(0)

    assert ack.ok is True
    events = [
        json.loads(line)
        for line in (log_dir_env / "node-1.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    blk_event = [event for event in events if event["event"] == "block_received"][0]
    assert isinstance(blk_event["received_ts_ms"], int)
    assert blk_event["ts_ms"] - blk_event["received_ts_ms"] >= 30
