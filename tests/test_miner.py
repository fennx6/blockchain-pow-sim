"""Unit tests for the node Miner in compete and pool modes."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template
from chainsim.core.chain import Blockchain
from chainsim.core.mempool import Mempool
from chainsim.core.pow import is_valid_pow, mine
from chainsim.core.transaction import create_transaction
from chainsim.node.miner import MAX_CANCELLED_JOBS, Miner
from chainsim.types import AddResult, Block, WorkAssignment


def _wait_until(predicate, timeout_s: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_compete_miner_mines_blocks_includes_txs_and_logs_event(log_dir_env: Path) -> None:
    pool = Mempool()
    tx = create_transaction("alice", "bob", 5.0, "node-1", timestamp_ms=100)
    pool.add(tx)
    chain = Blockchain(difficulty=1, mempool=pool, node_id="node-1")

    mined_blocks: list[Block] = []
    mined_event = threading.Event()

    def on_mined(block: Block) -> None:
        mined_blocks.append(block)
        if len(mined_blocks) >= 2:
            mined_event.set()

    miner = Miner(
        node_id="node-1",
        chain=chain,
        mempool=pool,
        mode="compete",
        block_size=10,
        nonce_space=100_000,
        on_block_mined=on_mined,
        batch_size=500,
    )
    miner.start()
    try:
        assert mined_event.wait(timeout=3.0)
    finally:
        miner.stop()

    assert not miner.is_running()
    assert miner.state() == "idle"
    assert miner.hashrate() == 0.0
    assert chain.height() >= 2
    assert chain.has_transaction(tx.tx_id)
    assert tx.tx_id not in pool
    assert all(is_valid_pow(b) for b in mined_blocks)

    log_file = log_dir_env / "node-1.jsonl"
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert any(ev["event"] == "block_mined" and ev["mode"] == "compete" for ev in events)


def test_compete_miner_interrupt_switches_to_new_tip_without_stopping(log_dir_env: Path) -> None:
    # Difficulty 7 prevents the miner from solving block 1 naturally during the test.
    pool = Mempool()
    chain = Blockchain(difficulty=7, mempool=pool)
    miner = Miner(
        node_id="node-1",
        chain=chain,
        mempool=pool,
        mode="compete",
        block_size=10,
        nonce_space=10_000_000,
        batch_size=5_000,
    )
    miner.start()
    try:
        assert _wait_until(lambda: miner.state() == "mining" and miner.hashrate() > 0.0)
        miner.notify_new_block()
        assert miner.is_running()
    finally:
        miner.stop()


def test_compete_miner_idles_when_empty_blocks_disabled(log_dir_env: Path) -> None:
    pool = Mempool()
    chain = Blockchain(difficulty=1, mempool=pool)
    mined_event = threading.Event()
    miner = Miner(
        node_id="node-1",
        chain=chain,
        mempool=pool,
        mode="compete",
        block_size=10,
        nonce_space=100_000,
        mine_empty_blocks=False,
        idle_poll_s=0.01,
        on_block_mined=lambda _: mined_event.set(),
    )
    miner.start()
    try:
        time.sleep(0.05)
        assert chain.height() == 0
        assert miner.state() == "idle"

        pool.add(create_transaction("alice", "bob", 2.0, "node-1", timestamp_ms=10))
        assert mined_event.wait(timeout=3.0)
        assert chain.height() >= 1
    finally:
        miner.stop()


def test_pool_miner_executes_assignment_and_handles_empty_range(log_dir_env: Path) -> None:
    pool = Mempool()
    chain = Blockchain(difficulty=1, mempool=pool)
    template = build_block_template(
        index=1,
        prev_hash=chain.tip().hash,
        transactions=[],
        difficulty=1,
        miner_id="node-2",
        timestamp_ms=500,
    )

    results: list[tuple[WorkAssignment, Block | None]] = []
    done = threading.Event()

    def on_result(assignment: WorkAssignment, block: Block | None) -> None:
        results.append((assignment, block))
        if len(results) >= 2:
            done.set()

    miner = Miner(
        node_id="node-2",
        chain=chain,
        mempool=pool,
        mode="pool",
        block_size=10,
        nonce_space=100_000,
        on_work_result=on_result,
        idle_poll_s=0.01,
    )
    miner.start()
    try:
        # Empty range should report None cleanly without hashing.
        miner.submit_work(WorkAssignment("job-empty", template, 10, 10))
        # Valid range should find a solution.
        miner.submit_work(WorkAssignment("job-real", template, 0, 100_000))
        assert done.wait(timeout=3.0)
    finally:
        miner.stop()

    assert results[0][0].job_id == "job-empty"
    assert results[0][1] is None
    assert results[1][0].job_id == "job-real"
    assert results[1][1] is not None
    assert is_valid_pow(results[1][1])


def test_pool_miner_stop_work_cancels_active_job(log_dir_env: Path) -> None:
    pool = Mempool()
    chain = Blockchain(difficulty=10, mempool=pool)
    template = build_block_template(
        index=1,
        prev_hash=chain.tip().hash,
        transactions=[],
        difficulty=10,
        miner_id="node-3",
        timestamp_ms=500,
    )
    results: list[tuple[WorkAssignment, Block | None]] = []
    miner = Miner(
        node_id="node-3",
        chain=chain,
        mempool=pool,
        mode="pool",
        block_size=10,
        nonce_space=10_000_000,
        on_work_result=lambda a, b: results.append((a, b)),
        batch_size=2_000,
        idle_poll_s=0.01,
    )
    miner.start()
    try:
        miner.submit_work(WorkAssignment("job-cancel", template, 0, 10_000_000))
        assert _wait_until(lambda: miner.state() == "mining")
        miner.stop_work("job-cancel")
        assert _wait_until(lambda: miner.state() == "idle")
    finally:
        miner.stop()

    assert results == []


def test_miner_validation_and_mode_guards() -> None:
    chain = Blockchain(difficulty=1)
    pool = Mempool()

    with pytest.raises(ValueError):
        Miner(node_id="", chain=chain, mempool=pool, mode="compete", block_size=5, nonce_space=100)
    with pytest.raises(ValueError):
        Miner(
            node_id="n1", chain=chain, mempool=pool, mode="invalid", block_size=5, nonce_space=100
        )

    compete_miner = Miner(
        node_id="n1", chain=chain, mempool=pool, mode="compete", block_size=5, nonce_space=100
    )
    tpl = build_block_template(1, chain.tip().hash, [], difficulty=1, miner_id="n1", timestamp_ms=1)
    with pytest.raises(RuntimeError):
        compete_miner.submit_work(WorkAssignment("job-1", tpl, 0, 10))

    # Sanity check: mined block integrates cleanly via Blockchain.add_block
    solved = mine(tpl, 0, 10_000, threading.Event())
    assert solved is not None
    assert chain.add_block(solved) is AddResult.ACCEPTED


def test_pool_miner_skips_stale_block_without_logging_block_mined(log_dir_env: Path) -> None:
    pool = Mempool()
    chain = Blockchain(difficulty=1, mempool=pool, node_id="node-4")
    genesis = chain.tip()

    # Advance chain to height 1 before the stale height-1 pool assignment finishes.
    b1 = mine(
        build_block_template(1, genesis.hash, [], difficulty=1, miner_id="node-1", timestamp_ms=10),
        0,
        100_000,
        threading.Event(),
    )
    assert b1 is not None
    assert chain.add_block(b1) is AddResult.ACCEPTED

    stale_template = build_block_template(
        index=1,
        prev_hash=genesis.hash,
        transactions=[],
        difficulty=1,
        miner_id="node-4",
        timestamp_ms=20,
    )
    results: list[tuple[WorkAssignment, Block | None]] = []
    miner = Miner(
        node_id="node-4",
        chain=chain,
        mempool=pool,
        mode="pool",
        block_size=10,
        nonce_space=100_000,
        on_work_result=lambda a, b: results.append((a, b)),
        idle_poll_s=0.01,
    )
    miner.start()
    try:
        miner.submit_work(WorkAssignment("job-stale", stale_template, 0, 100_000))
        assert _wait_until(lambda: miner._work_queue.empty() and miner.state() == "idle")
    finally:
        miner.stop()

    assert results == []
    log_file = log_dir_env / "node-4.jsonl"
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert not any(ev["event"] == "block_mined" for ev in events)


def test_pool_miner_bounds_cancelled_jobs_cache() -> None:
    chain = Blockchain(difficulty=1)
    pool = Mempool()
    miner = Miner(
        node_id="node-5",
        chain=chain,
        mempool=pool,
        mode="pool",
        block_size=10,
        nonce_space=100,
    )
    for idx in range(MAX_CANCELLED_JOBS + 50):
        miner.stop_work(f"job-{idx}")
    assert len(miner._cancelled_jobs) == MAX_CANCELLED_JOBS
    assert f"job-{MAX_CANCELLED_JOBS + 49}" in miner._cancelled_jobs
    assert "job-0" not in miner._cancelled_jobs
