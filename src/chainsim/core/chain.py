"""Main chain state, block validation, side-branch tracking, and fork choice.

Owner: Member A (AGENTS.md Section 3).
Contract (AGENTS.md Section 6.2 & 7.3):
- ``Blockchain.add_block(block: Block) -> AddResult``
- ``Blockchain.tip() -> Block``
- ``Blockchain.height() -> int``
- ``Blockchain.blocks_from(index: int) -> list[Block]``
- ``Blockchain.total_work() -> int``
- ``Blockchain.try_replace(candidate: list[Block]) -> bool``
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from pathlib import Path

from chainsim.core.block import (
    GENESIS_PREV_HASH,
    compute_hash,
    compute_merkle_root,
    create_genesis_block,
)
from chainsim.core.mempool import Mempool
from chainsim.core.pow import is_valid_pow
from chainsim.core.storage import load_chain, save_chain
from chainsim.core.transaction import is_valid_transaction
from chainsim.logging_utils import log_event
from chainsim.types import AddResult, Block


def block_work(block: Block) -> int:
    """Return expected PoW work contributed by a single block (0 for genesis)."""
    if block.index == 0:
        return 0
    return 16**block.difficulty


def chain_work(blocks: Sequence[Block]) -> int:
    """Return cumulative PoW work across a chain of blocks."""
    return sum(block_work(block) for block in blocks)


class Blockchain:
    """Thread-safe local blockchain implementing heaviest-chain fork choice.

    All validated blocks (main chain and side branches) are stored in a hash
    map keyed by ``block.hash``. Walking ``prev_hash`` reconstructs any branch
    back to genesis, enabling ``STORED_SIDE`` blocks to later trigger a
    ``FORK_SWITCHED`` when extended past the main chain's cumulative work.
    """

    def __init__(
        self,
        difficulty: int,
        *,
        genesis: Block | None = None,
        mempool: Mempool | None = None,
        storage_path: Path | str | None = None,
        node_id: str = "",
        confirmations: int = 3,
    ) -> None:
        if isinstance(difficulty, bool) or not isinstance(difficulty, int) or difficulty < 1:
            raise ValueError(f"difficulty must be an integer >= 1, got {difficulty!r}")
        if (
            isinstance(confirmations, bool)
            or not isinstance(confirmations, int)
            or confirmations < 0
        ):
            raise ValueError(f"confirmations must be >= 0, got {confirmations!r}")

        self._difficulty = difficulty
        self._mempool = mempool
        self._storage_path = Path(storage_path) if storage_path is not None else None
        self._node_id = node_id
        self._confirmations = confirmations
        self._lock = threading.Lock()

        genesis_block = genesis if genesis is not None else create_genesis_block(difficulty)
        self._validate_genesis(genesis_block)

        self._main_chain: list[Block] = [genesis_block]
        self._blocks_by_hash: dict[str, Block] = {genesis_block.hash: genesis_block}
        self._confirmed_tx_height: dict[str, int] = {}

        if self._storage_path is not None:
            loaded = load_chain(self._storage_path)
            if loaded:
                self._load_persisted_chain(loaded, genesis_block)

    @property
    def difficulty(self) -> int:
        return self._difficulty

    @property
    def genesis(self) -> Block:
        with self._lock:
            return self._main_chain[0]

    def tip(self) -> Block:
        """Return the current head block of the active main chain."""
        with self._lock:
            return self._main_chain[-1]

    def height(self) -> int:
        """Return the index of the current tip (0 for genesis only)."""
        with self._lock:
            return self._main_chain[-1].index

    def total_work(self) -> int:
        """Return cumulative PoW work of the active main chain."""
        with self._lock:
            return chain_work(self._main_chain)

    def blocks_from(self, index: int) -> list[Block]:
        """Return main-chain blocks starting at ``index`` (inclusive)."""
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError(f"index must be a non-negative integer, got {index!r}")
        with self._lock:
            if index >= len(self._main_chain):
                return []
            return list(self._main_chain[index:])

    def get_block(self, block_hash: str) -> Block | None:
        """Look up any known block (main chain or side branch) by hash."""
        with self._lock:
            return self._blocks_by_hash.get(block_hash)

    def side_branch_block_count(self) -> int:
        """Return the number of stored valid blocks not on the active main chain."""
        with self._lock:
            return len(self._blocks_by_hash) - len(self._main_chain)

    def has_transaction(self, tx_id: str) -> bool:
        """Return True if ``tx_id`` is included in the active main chain."""
        with self._lock:
            return tx_id in self._confirmed_tx_height

    def tx_confirmations(self, tx_id: str) -> int:
        """Return confirmation depth of ``tx_id`` on the active chain (0 if absent)."""
        with self._lock:
            included_at = self._confirmed_tx_height.get(tx_id)
            if included_at is None:
                return 0
            return self._main_chain[-1].index - included_at + 1

    def is_confirmed(self, tx_id: str, confirmations: int | None = None) -> bool:
        """Return True if ``tx_id`` has at least ``confirmations`` blocks on the active chain."""
        required = self._confirmations if confirmations is None else confirmations
        if required == 0:
            return self.has_transaction(tx_id)
        return self.tx_confirmations(tx_id) >= required

    def add_block(self, block: Block) -> AddResult:
        """Validate ``block`` and integrate it into the main chain or side-branch tree."""
        event_to_log: tuple[str, dict[str, object]] | None = None

        with self._lock:
            result, event_to_log = self._add_block_locked(block)

        if event_to_log is not None and self._node_id:
            event_name, fields = event_to_log
            log_event(event_name, self._node_id, **fields)

        return result

    def try_replace(self, candidate: list[Block]) -> bool:
        """Replace the active chain if ``candidate`` is valid and has strictly greater work.

        Ties keep the existing (first-seen) chain per AGENTS.md Section 7.3.
        Transactions from discarded blocks not present in ``candidate`` are
        returned to the attached mempool.
        """
        event_to_log: tuple[str, dict[str, object]] | None = None

        with self._lock:
            if not self._is_valid_candidate_chain(candidate):
                return False
            if chain_work(candidate) <= chain_work(self._main_chain):
                return False

            old_tip = self._main_chain[-1]
            discarded = self._switch_main_chain_locked(list(candidate))
            for block in candidate:
                self._blocks_by_hash[block.hash] = block
            self._persist_locked()

            if discarded:
                new_tip = self._main_chain[-1]
                event_to_log = (
                    "fork_switched",
                    {
                        "old_tip": old_tip.hash,
                        "new_tip": new_tip.hash,
                        "old_height": old_tip.index,
                        "new_height": new_tip.index,
                        "blocks_discarded": len(discarded),
                    },
                )

        if event_to_log is not None and self._node_id:
            event_name, fields = event_to_log
            log_event(event_name, self._node_id, **fields)

        return True

    def _add_block_locked(
        self, block: Block
    ) -> tuple[AddResult, tuple[str, dict[str, object]] | None]:
        if not isinstance(block, Block):
            return AddResult.INVALID, ("block_rejected", {"reason": "not_a_block"})

        if block.index == 0:
            if block == self._main_chain[0]:
                return AddResult.DUPLICATE, None
            return AddResult.INVALID, ("block_rejected", {"index": 0, "reason": "genesis_mismatch"})

        reason = self._validate_block_standalone(block)
        if reason is not None:
            return AddResult.INVALID, (
                "block_rejected",
                {"index": block.index, "block_hash": block.hash, "reason": reason},
            )

        if block.hash in self._blocks_by_hash:
            return AddResult.DUPLICATE, None

        parent = self._blocks_by_hash.get(block.prev_hash)
        if parent is None:
            return AddResult.ORPHAN, None

        if block.index != parent.index + 1:
            return AddResult.INVALID, (
                "block_rejected",
                {"index": block.index, "block_hash": block.hash, "reason": "non_sequential_index"},
            )

        ancestor_chain = self._branch_for_locked(parent.hash)
        ancestor_tx_ids = {tx.tx_id for ancestor in ancestor_chain for tx in ancestor.transactions}
        for tx in block.transactions:
            if tx.tx_id in ancestor_tx_ids:
                return AddResult.INVALID, (
                    "block_rejected",
                    {
                        "index": block.index,
                        "block_hash": block.hash,
                        "reason": "duplicate_tx_in_chain",
                    },
                )

        self._blocks_by_hash[block.hash] = block

        if parent.hash == self._main_chain[-1].hash:
            self._main_chain.append(block)
            for tx in block.transactions:
                self._confirmed_tx_height[tx.tx_id] = block.index
            if self._mempool is not None and block.transactions:
                self._mempool.remove_many(tx.tx_id for tx in block.transactions)
            self._persist_locked()
            return AddResult.ACCEPTED, (
                "block_accepted",
                {
                    "index": block.index,
                    "block_hash": block.hash,
                    "miner_id": block.miner_id,
                    "tx_count": len(block.transactions),
                },
            )

        candidate = [*ancestor_chain, block]
        if chain_work(candidate) > chain_work(self._main_chain):
            old_tip = self._main_chain[-1]
            discarded = self._switch_main_chain_locked(candidate)
            self._persist_locked()
            return AddResult.FORK_SWITCHED, (
                "fork_switched",
                {
                    "old_tip": old_tip.hash,
                    "new_tip": block.hash,
                    "old_height": old_tip.index,
                    "new_height": block.index,
                    "blocks_discarded": len(discarded),
                },
            )

        return AddResult.STORED_SIDE, None

    def _validate_genesis(self, genesis: Block) -> None:
        if genesis.index != 0 or genesis.prev_hash != GENESIS_PREV_HASH:
            raise ValueError("invalid genesis block index or prev_hash")
        if genesis.difficulty != self._difficulty:
            raise ValueError(
                f"genesis difficulty ({genesis.difficulty}) does not match "
                f"chain difficulty ({self._difficulty})"
            )
        if genesis.merkle_root != compute_merkle_root(genesis.transactions):
            raise ValueError("genesis merkle_root mismatch")
        if genesis.hash != compute_hash(genesis):
            raise ValueError("genesis hash mismatch")

    def _validate_block_standalone(self, block: Block) -> str | None:
        if block.index < 1:
            return "invalid_index"
        if block.difficulty != self._difficulty:
            return "wrong_difficulty"
        if not is_valid_pow(block):
            return "invalid_pow"
        if block.merkle_root != compute_merkle_root(block.transactions):
            return "invalid_merkle_root"
        seen_txs: set[str] = set()
        for tx in block.transactions:
            if not is_valid_transaction(tx):
                return "invalid_transaction"
            if tx.tx_id in seen_txs:
                return "duplicate_tx_in_block"
            seen_txs.add(tx.tx_id)
        return None

    def _is_valid_candidate_chain(self, candidate: Sequence[Block]) -> bool:
        if not candidate:
            return False
        if candidate[0] != self._main_chain[0]:
            return False

        seen_tx_ids: set[str] = set()
        for idx in range(1, len(candidate)):
            block = candidate[idx]
            prev = candidate[idx - 1]
            if not isinstance(block, Block):
                return False
            if block.index != idx or block.prev_hash != prev.hash:
                return False
            if self._validate_block_standalone(block) is not None:
                return False
            for tx in block.transactions:
                if tx.tx_id in seen_tx_ids:
                    return False
                seen_tx_ids.add(tx.tx_id)
        return True

    def _branch_for_locked(self, block_hash: str) -> list[Block]:
        branch: list[Block] = []
        cursor = self._blocks_by_hash.get(block_hash)
        while cursor is not None:
            branch.append(cursor)
            if cursor.index == 0:
                break
            cursor = self._blocks_by_hash.get(cursor.prev_hash)
        branch.reverse()
        return branch

    def _switch_main_chain_locked(self, new_chain: list[Block]) -> list[Block]:
        common_len = 0
        limit = min(len(self._main_chain), len(new_chain))
        while (
            common_len < limit and self._main_chain[common_len].hash == new_chain[common_len].hash
        ):
            common_len += 1

        discarded = self._main_chain[common_len:]
        self._main_chain = new_chain

        # Rebuild confirmed tx index from the new active chain.
        new_confirmed: dict[str, int] = {}
        for block in self._main_chain:
            for tx in block.transactions:
                new_confirmed[tx.tx_id] = block.index
        self._confirmed_tx_height = new_confirmed

        if self._mempool is not None:
            self._mempool.remove_many(new_confirmed.keys())
            to_requeue = [
                tx
                for block in discarded
                for tx in block.transactions
                if tx.tx_id not in new_confirmed
            ]
            if to_requeue:
                self._mempool.requeue(to_requeue)

        return discarded

    def _load_persisted_chain(self, loaded: list[Block], expected_genesis: Block) -> None:
        if loaded[0] != expected_genesis:
            raise ValueError("persisted chain genesis does not match expected genesis")
        if not self._is_valid_candidate_chain(loaded):
            raise ValueError("persisted chain failed validation")
        self._main_chain = list(loaded)
        self._blocks_by_hash = {block.hash: block for block in loaded}
        self._confirmed_tx_height = {
            tx.tx_id: block.index for block in loaded for tx in block.transactions
        }

    def _persist_locked(self) -> None:
        if self._storage_path is not None:
            save_chain(self._storage_path, self._main_chain)
