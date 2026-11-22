"""Tests for the protobuf conversion layer (Owner: Member B)."""

from __future__ import annotations

import threading

import pytest

from chainsim.coordinator.registry import NodeRecord
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.generated import blockchain_pb2
from chainsim.node.convert import (
    block_from_msg,
    block_to_msg,
    node_info_from_msg,
    node_info_to_msg,
    peer_list_from_msg,
    peer_list_to_msg,
    status_from_msg,
    status_to_msg,
    tx_from_msg,
    tx_to_msg,
    work_assignment_from_msg,
    work_assignment_to_msg,
    work_result_from_msg,
    work_result_to_msg,
)
from chainsim.types import NodeStatus, WorkAssignment


def _make_tx():
    return create_transaction(
        sender="addr-a",
        receiver="addr-b",
        amount=12.5,
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


def test_transaction_round_trip() -> None:
    tx = _make_tx()
    assert tx_from_msg(tx_to_msg(tx)) == tx


def test_transaction_from_msg_rejects_bad_id() -> None:
    msg = tx_to_msg(_make_tx())
    msg.tx_id = "not-a-real-hash"
    with pytest.raises(ValueError):
        tx_from_msg(msg)


def test_block_round_trip() -> None:
    block = _make_block(_make_tx())
    assert block_from_msg(block_to_msg(block)) == block


def test_block_from_msg_rejects_invalid_transaction() -> None:
    msg = block_to_msg(_make_block(_make_tx()))
    msg.transactions[0].tx_id = "bad"
    with pytest.raises(ValueError):
        block_from_msg(msg)


def test_status_round_trip_stringifies_meta() -> None:
    status = NodeStatus(
        node_id="node-1",
        height=3,
        tip_hash="a" * 64,
        peers=["node-2", "node-3"],
        mempool_size=7,
        hashrate=1234.5,
        state="mining",
        meta={"difficulty": 4, "side_branch_blocks": 0},
    )
    restored = status_from_msg(status_to_msg(status))
    assert restored.node_id == status.node_id
    assert restored.height == status.height
    assert restored.tip_hash == status.tip_hash
    assert restored.peers == status.peers
    assert restored.mempool_size == status.mempool_size
    assert restored.hashrate == status.hashrate
    assert restored.state == status.state
    assert restored.meta == {"difficulty": "4", "side_branch_blocks": "0"}


def test_work_assignment_round_trip() -> None:
    block = _make_block(_make_tx())
    assignment = WorkAssignment(
        job_id="job-1",
        block_template=block,
        nonce_start=0,
        nonce_end=1_000_000,
    )
    assert work_assignment_from_msg(work_assignment_to_msg(assignment)) == assignment


def test_work_assignment_rejects_bad_range() -> None:
    msg = work_assignment_to_msg(
        WorkAssignment(
            job_id="job-1",
            block_template=_make_block(_make_tx()),
            nonce_start=10,
            nonce_end=20,
        )
    )
    msg.nonce_end = 5
    with pytest.raises(ValueError):
        work_assignment_from_msg(msg)


def test_work_result_round_trip() -> None:
    result = WorkResult(
        job_id="job-1",
        node_id="node-1",
        nonce=42,
        found=True,
        block_hash="b" * 64,
    )
    assert work_result_from_msg(work_result_to_msg(result)) == result


def test_node_info_and_peer_list_round_trip() -> None:
    records = [
        NodeRecord("node-1", "http://node-1:8081", "node-1:50051", ("node-2",)),
        NodeRecord("node-2", "http://node-2:8081", "node-2:50051", ("node-1",)),
    ]
    assert node_info_from_msg(node_info_to_msg(records[0])) == records[0]
    assert peer_list_from_msg(peer_list_to_msg(records)) == records


def test_node_info_rejects_empty_id() -> None:
    with pytest.raises(ValueError):
        node_info_from_msg(blockchain_pb2.NodeInfo(node_id="", rest_url="http://x:8081"))
