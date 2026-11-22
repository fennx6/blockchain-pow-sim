"""Thread-safe pending transaction pool (mempool).

Owner: Member A (AGENTS.md Section 3).
Transactions from blocks discarded during a fork switch are returned here
(AGENTS.md Section 6.2).
"""

from __future__ import annotations

import threading
from collections.abc import Iterable

from chainsim.core.transaction import is_valid_transaction
from chainsim.types import Transaction


class Mempool:
    """In-memory, thread-safe pool of unconfirmed transactions."""

    def __init__(self) -> None:
        self._txs: dict[str, Transaction] = {}
        self._lock = threading.Lock()

    def add(self, tx: Transaction) -> bool:
        """Add a validated transaction if not already present.

        Returns True when newly inserted, False if ``tx.tx_id`` is already in
        the pool. Raises ``ValueError`` if ``tx`` is invalid.
        """
        if not is_valid_transaction(tx):
            raise ValueError(f"invalid transaction: {tx!r}")
        with self._lock:
            if tx.tx_id in self._txs:
                return False
            self._txs[tx.tx_id] = tx
            return True

    def requeue(self, txs: Iterable[Transaction]) -> int:
        """Return transactions (e.g. from discarded fork blocks) into the pool.

        Skips duplicates and invalid entries; returns the count of newly
        re-inserted transactions.
        """
        added = 0
        with self._lock:
            for tx in txs:
                if not is_valid_transaction(tx):
                    continue
                if tx.tx_id not in self._txs:
                    self._txs[tx.tx_id] = tx
                    added += 1
        return added

    def remove(self, tx_id: str) -> Transaction | None:
        """Remove and return a transaction by ``tx_id``, or ``None`` if absent."""
        with self._lock:
            return self._txs.pop(tx_id, None)

    def remove_many(self, tx_ids: Iterable[str]) -> int:
        """Remove all matching transaction IDs and return how many were removed."""
        removed = 0
        with self._lock:
            for tx_id in tx_ids:
                if self._txs.pop(tx_id, None) is not None:
                    removed += 1
        return removed

    def get(self, tx_id: str) -> Transaction | None:
        """Return a transaction by ``tx_id`` without removing it."""
        with self._lock:
            return self._txs.get(tx_id)

    def contains(self, tx_id: str) -> bool:
        """Return True if ``tx_id`` is currently in the pool."""
        with self._lock:
            return tx_id in self._txs

    def __contains__(self, tx_id: object) -> bool:
        if not isinstance(tx_id, str):
            return False
        return self.contains(tx_id)

    def select(self, max_count: int) -> list[Transaction]:
        """Return up to ``max_count`` pending transactions in FIFO order without removing them."""
        if isinstance(max_count, bool) or not isinstance(max_count, int) or max_count < 1:
            raise ValueError(f"max_count must be an integer >= 1, got {max_count!r}")
        with self._lock:
            return list(self._txs.values())[:max_count]

    def all(self) -> list[Transaction]:
        """Return a snapshot list of all pending transactions."""
        with self._lock:
            return list(self._txs.values())

    def size(self) -> int:
        """Return the number of pending transactions."""
        with self._lock:
            return len(self._txs)

    def __len__(self) -> int:
        return self.size()

    def clear(self) -> None:
        """Remove all transactions from the pool."""
        with self._lock:
            self._txs.clear()
