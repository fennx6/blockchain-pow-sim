"""Unit tests for Blockchain validation, side branches, fork choice, and persistence."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template, compute_hash, create_genesis_block
from chainsim.core.chain import Blockchain, block_work
from chainsim.core.mempool import Mempool
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.types import AddResult, Block, Transaction


def _tx(tag: str, amount: float = 1.0, ts: int = 100) -> Transaction:
    return create_transaction(f"alice-{tag}", f"bob-{tag}", amount, "node-1", timestamp_ms=ts)


def _mine_on(
    parent: Block,
    txs: list[Transaction],
    *,
    difficulty: int = 1,
    miner_id: str = "node-1",
    timestamp_ms: int = 1000,
) -> Block:
    template = build_block_template(
        index=parent.index + 1,
        prev_hash=parent.hash,
        transactions=txs,
        difficulty=difficulty,
        miner_id=miner_id,
        timestamp_ms=timestamp_ms,
    )
    mined = mine(template, 0, 100_000, threading.Event())
    assert mined is not None
    return mined


def test_initial_state_and_accepting_sequential_blocks() -> None:
    pool = Mempool()
    tx1 = _tx("1")
    pool.add(tx1)
    chain = Blockchain(difficulty=1, mempool=pool, confirmations=2)

    assert chain.height() == 0
    assert chain.total_work() == 0
    assert len(chain.blocks_from(0)) == 1
    assert chain.blocks_from(1) == []

    b1 = _mine_on(chain.tip(), [tx1], difficulty=1, timestamp_ms=1000)
    assert chain.add_block(b1) is AddResult.ACCEPTED
    assert chain.height() == 1
    assert chain.tip() == b1
    assert chain.total_work() == block_work(b1)
    assert tx1.tx_id not in pool
    assert chain.has_transaction(tx1.tx_id)
    assert chain.tx_confirmations(tx1.tx_id) == 1
    assert not chain.is_confirmed(tx1.tx_id)

    b2 = _mine_on(b1, [_tx("2")], difficulty=1, timestamp_ms=2000)
    assert chain.add_block(b2) is AddResult.ACCEPTED
    assert chain.height() == 2
    assert chain.tx_confirmations(tx1.tx_id) == 2
    assert chain.is_confirmed(tx1.tx_id)
    assert chain.blocks_from(1) == [b1, b2]


def test_duplicate_and_orphan_blocks() -> None:
    chain = Blockchain(difficulty=1)
    genesis = chain.tip()
    b1 = _mine_on(genesis, [], difficulty=1, timestamp_ms=1000)
    b2 = _mine_on(b1, [], difficulty=1, timestamp_ms=2000)

    # b2 arrives before b1 -> ORPHAN
    assert chain.add_block(b2) is AddResult.ORPHAN
    assert chain.height() == 0

    assert chain.add_block(b1) is AddResult.ACCEPTED
    assert chain.add_block(b1) is AddResult.DUPLICATE
    assert chain.add_block(genesis) is AddResult.DUPLICATE
    assert chain.add_block(b2) is AddResult.ACCEPTED
    assert chain.height() == 2


def test_fork_tie_keeps_first_seen_then_heavier_branch_switches_and_requeues_txs() -> None:
    pool = Mempool()
    tx_a = _tx("branch-a", ts=10)
    tx_b1 = _tx("branch-b1", ts=20)
    tx_b2 = _tx("branch-b2", ts=30)
    tx_shared = _tx("shared", ts=40)
    for tx in (tx_a, tx_b1, tx_b2, tx_shared):
        pool.add(tx)

    chain = Blockchain(difficulty=1, mempool=pool)
    genesis = chain.tip()

    # Main branch gets b1_a first.
    b1_a = _mine_on(genesis, [tx_a, tx_shared], difficulty=1, miner_id="node-1", timestamp_ms=100)
    assert chain.add_block(b1_a) is AddResult.ACCEPTED
    assert tx_a.tx_id not in pool
    assert tx_shared.tx_id not in pool

    # Competing b1_b has equal work -> stored as side branch, main chain unchanged.
    b1_b = _mine_on(genesis, [tx_b1, tx_shared], difficulty=1, miner_id="node-2", timestamp_ms=101)
    assert chain.add_block(b1_b) is AddResult.STORED_SIDE
    assert chain.tip() == b1_a
    assert chain.side_branch_block_count() == 1

    # b2_b extends the side branch past main chain work -> FORK_SWITCHED.
    b2_b = _mine_on(b1_b, [tx_b2], difficulty=1, miner_id="node-2", timestamp_ms=200)
    assert chain.add_block(b2_b) is AddResult.FORK_SWITCHED
    assert chain.tip() == b2_b
    assert chain.height() == 2

    # tx_a (discarded and absent from new chain) must return to mempool;
    # tx_shared, tx_b1, tx_b2 are confirmed in the new chain and must not be in mempool.
    assert tx_a.tx_id in pool
    assert tx_shared.tx_id not in pool
    assert tx_b1.tx_id not in pool
    assert tx_b2.tx_id not in pool
    assert not chain.has_transaction(tx_a.tx_id)
    assert chain.has_transaction(tx_b1.tx_id)
    assert chain.has_transaction(tx_b2.tx_id)


def test_add_block_rejects_invalid_pow_difficulty_merkle_index_and_double_tx() -> None:
    chain = Blockchain(difficulty=1)
    genesis = chain.tip()
    tx = _tx("dup")

    # Wrong difficulty
    wrong_diff = _mine_on(genesis, [], difficulty=2, timestamp_ms=100)
    assert chain.add_block(wrong_diff) is AddResult.INVALID

    # Invalid PoW
    bad_pow = build_block_template(
        1, genesis.hash, [], difficulty=1, miner_id="node-1", timestamp_ms=100
    )
    bad_pow.hash = "0" + "f" * 63
    assert chain.add_block(bad_pow) is AddResult.INVALID

    # Bad merkle root (even with valid PoW over the tampered header)
    bad_merkle = build_block_template(
        1, genesis.hash, [tx], difficulty=1, miner_id="node-1", timestamp_ms=100
    )
    bad_merkle.merkle_root = "f" * 64
    mined_bad_merkle = mine(bad_merkle, 0, 100_000, threading.Event())
    assert mined_bad_merkle is not None
    assert chain.add_block(mined_bad_merkle) is AddResult.INVALID

    # Non-sequential index
    bad_index = build_block_template(
        2, genesis.hash, [], difficulty=1, miner_id="node-1", timestamp_ms=100
    )
    mined_bad_index = mine(bad_index, 0, 100_000, threading.Event())
    assert mined_bad_index is not None
    assert chain.add_block(mined_bad_index) is AddResult.INVALID

    # Duplicate tx within the same block
    dup_in_block = _mine_on(genesis, [tx, tx], difficulty=1, timestamp_ms=100)
    assert chain.add_block(dup_in_block) is AddResult.INVALID

    # Duplicate tx across ancestor chain
    b1 = _mine_on(genesis, [tx], difficulty=1, timestamp_ms=100)
    assert chain.add_block(b1) is AddResult.ACCEPTED
    b2_replay = _mine_on(b1, [tx], difficulty=1, timestamp_ms=200)
    assert chain.add_block(b2_replay) is AddResult.INVALID


def test_try_replace_adopts_heavier_valid_chain_and_rejects_equal_or_invalid() -> None:
    pool = Mempool()
    tx_old = _tx("old")
    tx_new1 = _tx("new1")
    tx_new2 = _tx("new2")
    pool.add(tx_old)
    pool.add(tx_new1)
    pool.add(tx_new2)

    chain = Blockchain(difficulty=1, mempool=pool)
    genesis = chain.tip()

    b1_old = _mine_on(genesis, [tx_old], difficulty=1, miner_id="node-1", timestamp_ms=100)
    assert chain.add_block(b1_old) is AddResult.ACCEPTED

    b1_new = _mine_on(genesis, [tx_new1], difficulty=1, miner_id="node-2", timestamp_ms=101)
    # Equal work -> tie keeps current chain.
    assert chain.try_replace([genesis, b1_new]) is False
    assert chain.tip() == b1_old

    # Invalid genesis -> rejected.
    other_genesis = create_genesis_block(difficulty=1, miner_id="other")
    assert chain.try_replace([other_genesis, b1_new]) is False

    # Longer valid chain -> adopted and tx_old returned to mempool.
    b2_new = _mine_on(b1_new, [tx_new2], difficulty=1, miner_id="node-2", timestamp_ms=200)
    assert chain.try_replace([genesis, b1_new, b2_new]) is True
    assert chain.tip() == b2_new
    assert chain.height() == 2
    assert tx_old.tx_id in pool
    assert tx_new1.tx_id not in pool
    assert tx_new2.tx_id not in pool


def test_persistence_reloads_saved_chain_and_logs_events(tmp_path: Path, log_dir_env: Path) -> None:
    chain_file = tmp_path / "node-1-data" / "chain.json"
    chain = Blockchain(difficulty=1, storage_path=chain_file, node_id="node-1")
    b1 = _mine_on(chain.tip(), [_tx("p1")], difficulty=1, timestamp_ms=100)
    assert chain.add_block(b1) is AddResult.ACCEPTED

    # Restarted node with intact volume (F2) resumes at height 1.
    reloaded = Blockchain(difficulty=1, storage_path=chain_file, node_id="node-1")
    assert reloaded.height() == 1
    assert reloaded.tip() == b1

    log_file = log_dir_env / "node-1.jsonl"
    assert log_file.is_file()
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert any(ev["event"] == "block_accepted" and ev["index"] == 1 for ev in events)


def test_blocks_from_validates_negative_index() -> None:
    chain = Blockchain(difficulty=1)
    with pytest.raises(ValueError):
        chain.blocks_from(-1)
    with pytest.raises(ValueError):
        chain.blocks_from(False)


def test_invalid_tampered_genesis_hash_rejected_on_init() -> None:
    genesis = create_genesis_block(difficulty=1)
    genesis.hash = compute_hash(genesis)[:-1] + ("0" if genesis.hash[-1] != "0" else "1")
    with pytest.raises(ValueError):
        Blockchain(difficulty=1, genesis=genesis)
