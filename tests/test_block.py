"""Unit tests for block assembly, Merkle root, header hashing, and serialization."""

from __future__ import annotations

import hashlib

import pytest

from chainsim.core.block import (
    GENESIS_PREV_HASH,
    GENESIS_TIMESTAMP_MS,
    block_from_dict,
    block_to_dict,
    build_block_template,
    compute_hash,
    compute_merkle_root,
    create_genesis_block,
    header_nonce_parts,
)
from chainsim.core.transaction import create_transaction
from chainsim.types import Block


def _sample_txs(count: int) -> list:
    return [
        create_transaction(f"user-{i}", f"user-{i + 1}", float(i + 1), "node-1", timestamp_ms=i)
        for i in range(count)
    ]


def test_merkle_root_empty_single_even_and_odd_lists() -> None:
    assert compute_merkle_root([]) == hashlib.sha256(b"").hexdigest()

    one = _sample_txs(1)
    assert compute_merkle_root(one) == hashlib.sha256(one[0].tx_id.encode("utf-8")).hexdigest()

    two = _sample_txs(2)
    h0 = hashlib.sha256(two[0].tx_id.encode("utf-8")).digest()
    h1 = hashlib.sha256(two[1].tx_id.encode("utf-8")).digest()
    assert compute_merkle_root(two) == hashlib.sha256(h0 + h1).hexdigest()

    # Odd count duplicates the last leaf at that level.
    three = _sample_txs(3)
    h2 = hashlib.sha256(three[2].tx_id.encode("utf-8")).digest()
    left = hashlib.sha256(h0 + h1).digest()
    right = hashlib.sha256(h2 + h2).digest()
    assert compute_merkle_root(three) == hashlib.sha256(left + right).hexdigest()

    # Reordering transactions must change the Merkle root.
    assert compute_merkle_root([two[1], two[0]]) != compute_merkle_root(two)


def test_genesis_block_is_deterministic_across_nodes() -> None:
    g1 = create_genesis_block(difficulty=2)
    g2 = create_genesis_block(difficulty=2)

    assert g1 == g2
    assert g1.index == 0
    assert g1.prev_hash == GENESIS_PREV_HASH
    assert g1.timestamp_ms == GENESIS_TIMESTAMP_MS
    assert g1.transactions == []
    assert g1.hash == compute_hash(g1)


def test_compute_hash_matches_header_nonce_parts_for_special_strings() -> None:
    block = Block(
        index=5,
        prev_hash="a" * 64,
        timestamp_ms=1_700_000_123_456,
        transactions=[],
        merkle_root="b" * 64,
        difficulty=3,
        nonce=987654,
        miner_id='node-"quote"\\slash-é',
        hash="ignored-field",
    )
    prefix, suffix = header_nonce_parts(block)
    fast_digest = hashlib.sha256(prefix + str(block.nonce).encode("ascii") + suffix).hexdigest()

    assert fast_digest == compute_hash(block)
    # Existing block.hash value must not affect compute_hash.
    block.hash = "different-ignored-value"
    assert compute_hash(block) == fast_digest


def test_build_block_template_sets_merkle_root_and_validates_inputs() -> None:
    genesis = create_genesis_block(difficulty=2)
    txs = _sample_txs(2)
    template = build_block_template(
        index=1,
        prev_hash=genesis.hash,
        transactions=txs,
        difficulty=2,
        miner_id="node-1",
        timestamp_ms=12345,
    )
    assert template.index == 1
    assert template.merkle_root == compute_merkle_root(txs)
    assert template.nonce == 0
    assert template.hash == ""

    with pytest.raises(ValueError):
        build_block_template(0, genesis.hash, txs, 2, "node-1")
    with pytest.raises(ValueError):
        build_block_template(1, "too-short", txs, 2, "node-1")
    with pytest.raises(ValueError):
        build_block_template(1, genesis.hash, txs, 0, "node-1")


def test_block_dict_round_trip_and_malformed_input() -> None:
    genesis = create_genesis_block(difficulty=2)
    template = build_block_template(
        index=1,
        prev_hash=genesis.hash,
        transactions=_sample_txs(2),
        difficulty=2,
        miner_id="node-2",
        timestamp_ms=5000,
    )
    template.nonce = 42
    template.hash = compute_hash(template)

    restored = block_from_dict(block_to_dict(template))
    assert restored == template

    with pytest.raises(ValueError):
        block_from_dict({"index": 1})
    bad = block_to_dict(template)
    bad["difficulty"] = True
    with pytest.raises(ValueError):
        block_from_dict(bad)
