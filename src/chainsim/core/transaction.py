"""Transaction construction, canonical hashing, and serialization.

Owner: Member A (AGENTS.md Section 3).
Contract: ``tx_id`` is the SHA-256 hex digest of canonical transaction fields
(AGENTS.md Section 6.1).
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Mapping
from typing import Any

from chainsim.types import Transaction

MAX_IDENTIFIER_LEN = 128


def _validate_non_empty_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty")
    if len(cleaned) > MAX_IDENTIFIER_LEN:
        raise ValueError(f"{field_name} exceeds maximum length of {MAX_IDENTIFIER_LEN}")
    return cleaned


def _validate_amount(value: Any) -> float:
    # bool is a subclass of int in Python and must be rejected explicitly.
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("amount must be a finite positive number")
    amount = float(value)
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError("amount must be a finite positive number")
    return amount


def _validate_timestamp_ms(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("timestamp_ms must be a non-negative integer")
    if value < 0:
        raise ValueError("timestamp_ms must be a non-negative integer")
    return value


def validate_transaction_fields(
    sender: Any,
    receiver: Any,
    amount: Any,
    origin_node: Any,
    timestamp_ms: Any | None = None,
) -> tuple[str, str, float, str, int]:
    """Validate and normalize raw transaction fields.

    Returns ``(sender, receiver, amount, origin_node, timestamp_ms)``.
    Raises ``ValueError`` if any field violates schema constraints.
    """
    clean_sender = _validate_non_empty_text(sender, "sender")
    clean_receiver = _validate_non_empty_text(receiver, "receiver")
    if clean_sender == clean_receiver:
        raise ValueError("sender and receiver must be different")
    clean_amount = _validate_amount(amount)
    clean_origin = _validate_non_empty_text(origin_node, "origin_node")
    resolved_ts = (
        time.time_ns() // 1_000_000
        if timestamp_ms is None
        else _validate_timestamp_ms(timestamp_ms)
    )
    return clean_sender, clean_receiver, clean_amount, clean_origin, resolved_ts


def compute_tx_id(
    sender: str,
    receiver: str,
    amount: float,
    timestamp_ms: int,
    origin_node: str,
) -> str:
    """Compute the deterministic SHA-256 hex digest for transaction contents."""
    payload = json.dumps(
        {
            "sender": sender,
            "receiver": receiver,
            "amount": float(amount),
            "timestamp_ms": timestamp_ms,
            "origin_node": origin_node,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def create_transaction(
    sender: str,
    receiver: str,
    amount: float,
    origin_node: str,
    timestamp_ms: int | None = None,
) -> Transaction:
    """Create a validated ``Transaction`` with its computed ``tx_id``."""
    clean_sender, clean_receiver, clean_amount, clean_origin, resolved_ts = (
        validate_transaction_fields(
            sender=sender,
            receiver=receiver,
            amount=amount,
            origin_node=origin_node,
            timestamp_ms=timestamp_ms,
        )
    )
    tx_id = compute_tx_id(
        sender=clean_sender,
        receiver=clean_receiver,
        amount=clean_amount,
        timestamp_ms=resolved_ts,
        origin_node=clean_origin,
    )
    return Transaction(
        tx_id=tx_id,
        sender=clean_sender,
        receiver=clean_receiver,
        amount=clean_amount,
        timestamp_ms=resolved_ts,
        origin_node=clean_origin,
    )


def is_valid_transaction(tx: Transaction) -> bool:
    """Return True if all transaction fields are valid and ``tx_id`` matches."""
    if not isinstance(tx, Transaction):
        return False
    try:
        clean_sender, clean_receiver, clean_amount, clean_origin, resolved_ts = (
            validate_transaction_fields(
                sender=tx.sender,
                receiver=tx.receiver,
                amount=tx.amount,
                origin_node=tx.origin_node,
                timestamp_ms=tx.timestamp_ms,
            )
        )
    except ValueError:
        return False

    # Reject unnormalized whitespace that would diverge from the signed content.
    if tx.sender != clean_sender or tx.receiver != clean_receiver or tx.origin_node != clean_origin:
        return False

    expected_id = compute_tx_id(
        sender=clean_sender,
        receiver=clean_receiver,
        amount=clean_amount,
        timestamp_ms=resolved_ts,
        origin_node=clean_origin,
    )
    return tx.tx_id == expected_id


def transaction_to_dict(tx: Transaction) -> dict[str, Any]:
    """Serialize a ``Transaction`` into a JSON-compatible dictionary."""
    return {
        "tx_id": tx.tx_id,
        "sender": tx.sender,
        "receiver": tx.receiver,
        "amount": tx.amount,
        "timestamp_ms": tx.timestamp_ms,
        "origin_node": tx.origin_node,
    }


def transaction_from_dict(data: Mapping[str, Any]) -> Transaction:
    """Deserialize a ``Transaction`` from a mapping, raising ``ValueError`` on bad shape."""
    if not isinstance(data, Mapping):
        raise ValueError("transaction payload must be a mapping")
    required = ("tx_id", "sender", "receiver", "amount", "timestamp_ms", "origin_node")
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f"transaction missing fields: {', '.join(missing)}")

    tx_id = data["tx_id"]
    if not isinstance(tx_id, str) or not tx_id.strip():
        raise ValueError("tx_id must be a non-empty string")
    sender, receiver, amount, origin_node, timestamp_ms = validate_transaction_fields(
        sender=data["sender"],
        receiver=data["receiver"],
        amount=data["amount"],
        origin_node=data["origin_node"],
        timestamp_ms=data["timestamp_ms"],
    )
    return Transaction(
        tx_id=tx_id,
        sender=sender,
        receiver=receiver,
        amount=amount,
        timestamp_ms=timestamp_ms,
        origin_node=origin_node,
    )
