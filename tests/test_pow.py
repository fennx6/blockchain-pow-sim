"""Unit tests for Proof-of-Work mining and verification."""

from __future__ import annotations

import threading
import time

import pytest

from chainsim.core.block import build_block_template, compute_hash, create_genesis_block
from chainsim.core.pow import is_valid_pow, mine, mine_with_stats
from chainsim.core.transaction import create_transaction


def _make_template(difficulty: int = 2) -> tuple:
    genesis = create_genesis_block(difficulty=difficulty)
    tx = create_transaction("alice", "bob", 4.0, "node-1", timestamp_ms=1000)
    template = build_block_template(
        index=1,
        prev_hash=genesis.hash,
        transactions=[tx],
        difficulty=difficulty,
        miner_id="node-1",
        timestamp_ms=2000,
    )
    return genesis, template


def test_mine_finds_valid_pow_without_mutating_template() -> None:
    _, template = _make_template(difficulty=2)
    stop = threading.Event()

    mined, hashes_tried = mine_with_stats(template, 0, 100_000, stop)

    assert mined is not None
    assert hashes_tried >= 1
    assert 0 <= mined.nonce < 100_000
    assert mined.hash.startswith("00")
    assert mined.hash == compute_hash(mined)
    assert is_valid_pow(mined)
    # Original template must remain unchanged for safe sharing in pool mode.
    assert template.nonce == 0
    assert template.hash == ""


def test_mine_wrapper_matches_contract_and_respects_nonce_bounds() -> None:
    _, template = _make_template(difficulty=1)
    stop = threading.Event()

    mined = mine(template, 500, 10_000, stop)
    assert mined is not None
    assert 500 <= mined.nonce < 10_000
    assert is_valid_pow(mined)


def test_mine_returns_none_for_empty_range_or_exhausted_range() -> None:
    _, template = _make_template(difficulty=4)
    stop = threading.Event()

    block_empty, tried_empty = mine_with_stats(template, 10, 10, stop)
    assert block_empty is None
    assert tried_empty == 0

    # A 1-nonce range at difficulty 4 almost certainly fails; if it happens to hit,
    # test another nonce to guarantee we exercise the exhausted-range path.
    found_miss = False
    for start in range(10):
        block, tried = mine_with_stats(template, start, start + 1, stop)
        if block is None:
            assert tried == 1
            found_miss = True
            break
    assert found_miss


def test_mine_stops_immediately_when_stop_event_is_preset() -> None:
    _, template = _make_template(difficulty=1)
    stop = threading.Event()
    stop.set()

    mined, tried = mine_with_stats(template, 0, 100_000, stop)
    assert mined is None
    assert tried == 0


def test_mine_cancels_promptly_from_another_thread() -> None:
    # High difficulty ensures the loop does not finish naturally before stop fires.
    _, template = _make_template(difficulty=10)
    stop = threading.Event()
    result: dict[str, object] = {}

    def worker() -> None:
        result["block"], result["tried"] = mine_with_stats(template, 0, 10_000_000, stop)

    thread = threading.Thread(target=worker)
    started = time.monotonic()
    thread.start()
    time.sleep(0.05)
    stop.set()
    thread.join(timeout=2.0)
    elapsed = time.monotonic() - started

    assert not thread.is_alive()
    assert result["block"] is None
    assert elapsed < 1.5


def test_is_valid_pow_rejects_tampered_or_forged_blocks() -> None:
    _, template = _make_template(difficulty=2)
    mined = mine(template, 0, 100_000, threading.Event())
    assert mined is not None

    # Forged hash with leading zeroes that does not match header.
    forged = build_block_template(
        index=mined.index,
        prev_hash=mined.prev_hash,
        transactions=mined.transactions,
        difficulty=2,
        miner_id=mined.miner_id,
        timestamp_ms=mined.timestamp_ms,
    )
    forged.nonce = mined.nonce
    forged.hash = "00" + "f" * 62
    assert not is_valid_pow(forged)

    # Tampering with miner_id invalidates the existing hash.
    mined.miner_id = "attacker"
    assert not is_valid_pow(mined)


def test_mine_validates_arguments() -> None:
    _, template = _make_template(difficulty=1)
    stop = threading.Event()

    with pytest.raises(ValueError):
        mine(template, -1, 10, stop)
    with pytest.raises(ValueError):
        mine(template, 10, 5, stop)

    template.difficulty = 0
    with pytest.raises(ValueError):
        mine(template, 0, 10, stop)
