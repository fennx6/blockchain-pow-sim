"""Unit tests for transaction creation, hashing, validation, and serialization."""

from __future__ import annotations

import math

import pytest

from chainsim.core.transaction import (
    MAX_IDENTIFIER_LEN,
    compute_tx_id,
    create_transaction,
    is_valid_transaction,
    transaction_from_dict,
    transaction_to_dict,
)
from chainsim.types import Transaction


def test_create_transaction_computes_deterministic_sha256_tx_id() -> None:
    tx1 = create_transaction("alice", "bob", 12.5, "node-1", timestamp_ms=1_700_000_000_000)
    tx2 = create_transaction("alice", "bob", 12.5, "node-1", timestamp_ms=1_700_000_000_000)

    assert tx1 == tx2
    assert len(tx1.tx_id) == 64
    assert tx1.tx_id == compute_tx_id("alice", "bob", 12.5, 1_700_000_000_000, "node-1")
    assert is_valid_transaction(tx1)


def test_tx_id_changes_when_any_field_changes() -> None:
    base = create_transaction("alice", "bob", 10.0, "node-1", timestamp_ms=100)
    variants = [
        create_transaction("charlie", "bob", 10.0, "node-1", timestamp_ms=100),
        create_transaction("alice", "charlie", 10.0, "node-1", timestamp_ms=100),
        create_transaction("alice", "bob", 10.1, "node-1", timestamp_ms=100),
        create_transaction("alice", "bob", 10.0, "node-2", timestamp_ms=100),
        create_transaction("alice", "bob", 10.0, "node-1", timestamp_ms=101),
    ]
    ids = {base.tx_id, *(tx.tx_id for tx in variants)}
    assert len(ids) == 6


def test_create_transaction_trims_whitespace_and_normalizes_int_amount() -> None:
    tx = create_transaction("  alice ", " bob  ", 5, " node-1 ", timestamp_ms=50)
    assert tx.sender == "alice"
    assert tx.receiver == "bob"
    assert tx.origin_node == "node-1"
    assert tx.amount == 5.0
    assert is_valid_transaction(tx)


@pytest.mark.parametrize(
    ("sender", "receiver", "amount", "origin_node", "timestamp_ms"),
    [
        ("", "bob", 1.0, "node-1", 1),
        ("   ", "bob", 1.0, "node-1", 1),
        ("alice", "", 1.0, "node-1", 1),
        ("alice", "alice", 1.0, "node-1", 1),
        ("a" * (MAX_IDENTIFIER_LEN + 1), "bob", 1.0, "node-1", 1),
        ("alice", "bob", 0, "node-1", 1),
        ("alice", "bob", -3.5, "node-1", 1),
        ("alice", "bob", True, "node-1", 1),
        ("alice", "bob", math.nan, "node-1", 1),
        ("alice", "bob", math.inf, "node-1", 1),
        ("alice", "bob", "10", "node-1", 1),
        ("alice", "bob", 1.0, "", 1),
        ("alice", "bob", 1.0, "node-1", -1),
        ("alice", "bob", 1.0, "node-1", False),
    ],
)
def test_create_transaction_rejects_invalid_inputs(
    sender: object,
    receiver: object,
    amount: object,
    origin_node: object,
    timestamp_ms: object,
) -> None:
    with pytest.raises(ValueError):
        create_transaction(
            sender=sender,  # type: ignore[arg-type]
            receiver=receiver,  # type: ignore[arg-type]
            amount=amount,  # type: ignore[arg-type]
            origin_node=origin_node,  # type: ignore[arg-type]
            timestamp_ms=timestamp_ms,  # type: ignore[arg-type]
        )


def test_is_valid_transaction_rejects_tampered_id_and_untrimmed_fields() -> None:
    valid = create_transaction("alice", "bob", 2.5, "node-1", timestamp_ms=42)
    tampered = Transaction(
        tx_id="0" * 64,
        sender=valid.sender,
        receiver=valid.receiver,
        amount=valid.amount,
        timestamp_ms=valid.timestamp_ms,
        origin_node=valid.origin_node,
    )
    assert not is_valid_transaction(tampered)

    untrimmed_id = compute_tx_id(" alice", "bob", 2.5, 42, "node-1")
    untrimmed = Transaction(untrimmed_id, " alice", "bob", 2.5, 42, "node-1")
    assert not is_valid_transaction(untrimmed)


def test_transaction_dict_round_trip_and_missing_fields() -> None:
    tx = create_transaction("alice", "bob", 7.25, "node-3", timestamp_ms=999)
    restored = transaction_from_dict(transaction_to_dict(tx))
    assert restored == tx

    with pytest.raises(ValueError):
        transaction_from_dict({"tx_id": tx.tx_id, "sender": "alice"})
