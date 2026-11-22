"""Kontrak dataclass (AGENTS.md Bagian 6.1)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from chainsim.types import AddResult, Block, NodeStatus, Transaction, WorkAssignment


def make_transaction(tx_id: str = "tx-1") -> Transaction:
    return Transaction(
        tx_id=tx_id,
        sender="alice",
        receiver="bob",
        amount=1.5,
        timestamp_ms=1_700_000_000_000,
        origin_node="node-1",
    )


def test_transaction_is_frozen() -> None:
    tx = make_transaction()
    with pytest.raises(FrozenInstanceError):
        tx.amount = 2.0  # type: ignore[misc]


def test_block_hash_defaults_to_empty_string() -> None:
    block = Block(
        index=1,
        prev_hash="0" * 64,
        timestamp_ms=1_700_000_000_000,
        transactions=[make_transaction()],
        merkle_root="abc",
        difficulty=3,
        nonce=7,
        miner_id="node-1",
    )
    assert block.hash == ""


def test_add_result_values_match_contract() -> None:
    assert {item.value for item in AddResult} == {
        "accepted",
        "duplicate",
        "orphan",
        "fork_switched",
        "stored_side",
        "invalid",
    }


def test_node_status_meta_is_not_shared_between_instances() -> None:
    first = NodeStatus("node-1", 0, "", [], 0, 0.0, "idle")
    second = NodeStatus("node-2", 0, "", [], 0, 0.0, "idle")
    first.meta["note"] = "hanya milik node-1"
    assert second.meta == {}


def test_work_assignment_holds_nonce_range() -> None:
    template = Block(2, "0" * 64, 1, [], "root", 4, 0, "node-1")
    assignment = WorkAssignment("job-1", template, 0, 1000)
    assert assignment.nonce_start == 0
    assert assignment.nonce_end == 1000
    assert assignment.block_template.index == 2
