"""Unit tests for the thread-safe transaction mempool."""

from __future__ import annotations

import threading

import pytest

from chainsim.core.mempool import Mempool
from chainsim.core.transaction import create_transaction
from chainsim.types import Transaction


def _tx(idx: int) -> Transaction:
    return create_transaction(f"s-{idx}", f"r-{idx}", float(idx + 1), "node-1", timestamp_ms=idx)


def test_add_deduplicates_and_preserves_fifo_order() -> None:
    pool = Mempool()
    tx0, tx1, tx2 = _tx(0), _tx(1), _tx(2)

    assert pool.add(tx0) is True
    assert pool.add(tx0) is False
    assert pool.add(tx1) is True
    assert pool.add(tx2) is True

    assert pool.size() == 3
    assert len(pool) == 3
    assert tx1.tx_id in pool
    assert pool.get(tx1.tx_id) == tx1
    assert pool.select(2) == [tx0, tx1]
    assert pool.all() == [tx0, tx1, tx2]


def test_add_rejects_invalid_transaction() -> None:
    pool = Mempool()
    bad = Transaction("bad-id", "alice", "bob", 1.0, 10, "node-1")
    with pytest.raises(ValueError):
        pool.add(bad)


def test_remove_remove_many_requeue_and_clear() -> None:
    pool = Mempool()
    txs = [_tx(i) for i in range(4)]
    for tx in txs:
        pool.add(tx)

    assert pool.remove(txs[0].tx_id) == txs[0]
    assert pool.remove("missing") is None
    assert pool.remove_many([txs[1].tx_id, txs[2].tx_id, "missing"]) == 2
    assert pool.all() == [txs[3]]

    # Requeue skips existing tx3 and invalid entries, re-adds tx0 and tx1.
    bad = Transaction("forged", "a", "b", 1.0, 1, "node-1")
    readded = pool.requeue([txs[3], txs[0], bad, txs[1]])
    assert readded == 2
    assert pool.size() == 3

    pool.clear()
    assert pool.size() == 0


def test_select_validates_max_count() -> None:
    pool = Mempool()
    with pytest.raises(ValueError):
        pool.select(0)
    with pytest.raises(ValueError):
        pool.select(True)


def test_mempool_is_thread_safe_under_concurrent_adds() -> None:
    pool = Mempool()
    txs = [_tx(i) for i in range(100)]
    errors: list[Exception] = []

    def worker(chunk: list[Transaction]) -> None:
        try:
            for tx in chunk:
                pool.add(tx)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(txs[i::4],)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert pool.size() == 100
