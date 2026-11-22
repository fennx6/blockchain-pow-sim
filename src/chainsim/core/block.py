"""Block construction, Merkle root calculation, header hashing, and serialization.

Owner: Member A (AGENTS.md Section 3).
Contract: ``compute_hash(block: Block) -> str`` (AGENTS.md Section 6.2).
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any

from chainsim.core.transaction import transaction_from_dict, transaction_to_dict
from chainsim.types import Block, Transaction

GENESIS_PREV_HASH = "0" * 64
GENESIS_MINER_ID = "genesis"
# Fixed genesis timestamp ensures every node derives the exact same genesis hash.
GENESIS_TIMESTAMP_MS = 0


def compute_merkle_root(transactions: Sequence[Transaction]) -> str:
    """Compute a binary SHA-256 Merkle root over transaction IDs.

    An empty transaction list hashes the empty byte string so empty blocks still
    have a well-defined deterministic commitment.
    """
    if not transactions:
        return hashlib.sha256(b"").hexdigest()

    level = [hashlib.sha256(tx.tx_id.encode("utf-8")).digest() for tx in transactions]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])
        next_level: list[bytes] = []
        for idx in range(0, len(level), 2):
            next_level.append(hashlib.sha256(level[idx] + level[idx + 1]).digest())
        level = next_level
    return level[0].hex()


def header_nonce_parts(block: Block) -> tuple[bytes, bytes]:
    """Encode static header bytes before and after the integer ``nonce`` field.

    Concatenating ``prefix + str(nonce).encode("ascii") + suffix`` yields the
    exact same canonical JSON bytes hashed by ``compute_hash``. Pre-encoding
    these halves keeps the PoW loop fast without diverging from the header schema.
    """
    prefix = (
        f'{{"difficulty":{block.difficulty},'
        f'"index":{block.index},'
        f'"merkle_root":{json.dumps(block.merkle_root)},'
        f'"miner_id":{json.dumps(block.miner_id)},'
        f'"nonce":'
    ).encode()
    suffix = (
        f',"prev_hash":{json.dumps(block.prev_hash)},"timestamp_ms":{block.timestamp_ms}}}'
    ).encode()
    return prefix, suffix


def compute_hash(block: Block) -> str:
    """Compute the SHA-256 hex digest of a block header."""
    payload = json.dumps(
        {
            "index": block.index,
            "prev_hash": block.prev_hash,
            "timestamp_ms": block.timestamp_ms,
            "merkle_root": block.merkle_root,
            "difficulty": block.difficulty,
            "nonce": block.nonce,
            "miner_id": block.miner_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def create_genesis_block(
    difficulty: int,
    miner_id: str = GENESIS_MINER_ID,
    timestamp_ms: int = GENESIS_TIMESTAMP_MS,
) -> Block:
    """Create the deterministic genesis block (index 0) shared by all nodes."""
    if isinstance(difficulty, bool) or not isinstance(difficulty, int) or difficulty < 1:
        raise ValueError(f"difficulty must be an integer >= 1, got {difficulty!r}")
    if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int) or timestamp_ms < 0:
        raise ValueError(f"timestamp_ms must be a non-negative integer, got {timestamp_ms!r}")
    if not isinstance(miner_id, str) or not miner_id.strip():
        raise ValueError("miner_id must be a non-empty string")

    block = Block(
        index=0,
        prev_hash=GENESIS_PREV_HASH,
        timestamp_ms=timestamp_ms,
        transactions=[],
        merkle_root=compute_merkle_root([]),
        difficulty=difficulty,
        nonce=0,
        miner_id=miner_id.strip(),
        hash="",
    )
    block.hash = compute_hash(block)
    return block


def build_block_template(
    index: int,
    prev_hash: str,
    transactions: Sequence[Transaction],
    difficulty: int,
    miner_id: str,
    timestamp_ms: int | None = None,
) -> Block:
    """Assemble an unmined block template ready for Proof-of-Work."""
    if isinstance(index, bool) or not isinstance(index, int) or index < 1:
        raise ValueError(f"block template index must be >= 1, got {index!r}")
    if not isinstance(prev_hash, str) or len(prev_hash) != 64:
        raise ValueError("prev_hash must be a 64-character hex string")
    if isinstance(difficulty, bool) or not isinstance(difficulty, int) or difficulty < 1:
        raise ValueError(f"difficulty must be an integer >= 1, got {difficulty!r}")
    if not isinstance(miner_id, str) or not miner_id.strip():
        raise ValueError("miner_id must be a non-empty string")

    resolved_ts = time.time_ns() // 1_000_000 if timestamp_ms is None else timestamp_ms
    if isinstance(resolved_ts, bool) or not isinstance(resolved_ts, int) or resolved_ts < 0:
        raise ValueError(f"timestamp_ms must be a non-negative integer, got {resolved_ts!r}")

    tx_list = list(transactions)
    return Block(
        index=index,
        prev_hash=prev_hash,
        timestamp_ms=resolved_ts,
        transactions=tx_list,
        merkle_root=compute_merkle_root(tx_list),
        difficulty=difficulty,
        nonce=0,
        miner_id=miner_id.strip(),
        hash="",
    )


def block_to_dict(block: Block) -> dict[str, Any]:
    """Serialize a ``Block`` and its transactions into a JSON-compatible dictionary."""
    return {
        "index": block.index,
        "prev_hash": block.prev_hash,
        "timestamp_ms": block.timestamp_ms,
        "transactions": [transaction_to_dict(tx) for tx in block.transactions],
        "merkle_root": block.merkle_root,
        "difficulty": block.difficulty,
        "nonce": block.nonce,
        "miner_id": block.miner_id,
        "hash": block.hash,
    }


def block_from_dict(data: Mapping[str, Any]) -> Block:
    """Deserialize a ``Block`` from a mapping, raising ``ValueError`` on malformed input."""
    if not isinstance(data, Mapping):
        raise ValueError("block payload must be a mapping")
    required = (
        "index",
        "prev_hash",
        "timestamp_ms",
        "transactions",
        "merkle_root",
        "difficulty",
        "nonce",
        "miner_id",
        "hash",
    )
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f"block missing fields: {', '.join(missing)}")

    raw_txs = data["transactions"]
    if not isinstance(raw_txs, list):
        raise ValueError("block transactions must be a list")

    for int_field in ("index", "timestamp_ms", "difficulty", "nonce"):
        val = data[int_field]
        if isinstance(val, bool) or not isinstance(val, int):
            raise ValueError(f"block {int_field} must be an integer")

    for str_field in ("prev_hash", "merkle_root", "miner_id", "hash"):
        if not isinstance(data[str_field], str):
            raise ValueError(f"block {str_field} must be a string")

    if data["index"] < 0:
        raise ValueError("block index must be non-negative")
    if data["timestamp_ms"] < 0:
        raise ValueError("block timestamp_ms must be non-negative")
    if data["difficulty"] < 1:
        raise ValueError("block difficulty must be >= 1")
    if data["nonce"] < 0:
        raise ValueError("block nonce must be non-negative")

    return Block(
        index=data["index"],
        prev_hash=data["prev_hash"],
        timestamp_ms=data["timestamp_ms"],
        transactions=[transaction_from_dict(item) for item in raw_txs],
        merkle_root=data["merkle_root"],
        difficulty=data["difficulty"],
        nonce=data["nonce"],
        miner_id=data["miner_id"],
        hash=data["hash"],
    )
