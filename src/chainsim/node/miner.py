"""Mining loop for ``compete`` (Nakamoto) and ``pool`` (coordinated worker) modes.

Owner: Member A (AGENTS.md Section 3 & 7.2).
Both modes delegate PoW hashing to ``chainsim.core.pow`` and support prompt
cancellation through ``threading.Event`` when a new block arrives or the
coordinator stops a pool job (AGENTS.md Section 12).
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable

from chainsim.config import MINING_MODES
from chainsim.core.block import build_block_template
from chainsim.core.chain import Blockchain
from chainsim.core.mempool import Mempool
from chainsim.core.pow import mine_with_stats
from chainsim.logging_utils import log_event
from chainsim.types import AddResult, Block, WorkAssignment

logger = logging.getLogger(__name__)

BlockCallback = Callable[[Block], None]
WorkResultCallback = Callable[[WorkAssignment, Block | None], None]

DEFAULT_BATCH_SIZE = 50_000
DEFAULT_IDLE_POLL_S = 0.05


class Miner:
    """Background PoW miner supporting both ``compete`` and ``pool`` modes."""

    def __init__(
        self,
        *,
        node_id: str,
        chain: Blockchain,
        mempool: Mempool,
        mode: str,
        block_size: int,
        nonce_space: int,
        on_block_mined: BlockCallback | None = None,
        on_work_result: WorkResultCallback | None = None,
        mine_empty_blocks: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
        idle_poll_s: float = DEFAULT_IDLE_POLL_S,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if mode not in MINING_MODES:
            raise ValueError(f"mode must be one of {MINING_MODES}, got {mode!r}")
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < 1:
            raise ValueError(f"block_size must be >= 1, got {block_size!r}")
        if isinstance(nonce_space, bool) or not isinstance(nonce_space, int) or nonce_space < 1:
            raise ValueError(f"nonce_space must be >= 1, got {nonce_space!r}")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size!r}")
        if idle_poll_s <= 0:
            raise ValueError(f"idle_poll_s must be > 0, got {idle_poll_s!r}")

        self._node_id = node_id.strip()
        self._chain = chain
        self._mempool = mempool
        self._mode = mode
        self._block_size = block_size
        self._nonce_space = nonce_space
        self._on_block_mined = on_block_mined
        self._on_work_result = on_work_result
        self._mine_empty_blocks = mine_empty_blocks
        self._batch_size = batch_size
        self._idle_poll_s = idle_poll_s

        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._round_stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = "idle"
        self._hashrate = 0.0

        self._work_queue: queue.Queue[WorkAssignment] = queue.Queue()
        self._active_job_id: str | None = None
        self._cancelled_jobs: set[str] = set()

    @property
    def mode(self) -> str:
        return self._mode

    def state(self) -> str:
        """Return current miner state (``'mining'`` or ``'idle'``)."""
        with self._lock:
            return self._state

    def hashrate(self) -> float:
        """Return the latest measured hashrate in hashes/second (0.0 when idle)."""
        with self._lock:
            return self._hashrate

    def is_running(self) -> bool:
        """Return True if the background miner thread is alive."""
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Start the background mining thread (idempotent)."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._shutdown.clear()
            self._round_stop.clear()
            target = self._compete_loop if self._mode == "compete" else self._pool_loop
            self._thread = threading.Thread(
                target=target,
                name=f"miner-{self._node_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        """Stop the background mining thread and cancel any active PoW round."""
        with self._lock:
            self._shutdown.set()
            self._round_stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout_s)
        with self._lock:
            self._thread = None
            self._active_job_id = None
            self._state = "idle"
            self._hashrate = 0.0

    def interrupt(self) -> None:
        """Cancel the current mining round so the next iteration builds on the new tip."""
        with self._lock:
            self._round_stop.set()

    def notify_new_block(self) -> None:
        """Alias for ``interrupt()`` called when a valid external block arrives."""
        self.interrupt()

    def submit_work(self, assignment: WorkAssignment) -> None:
        """Enqueue a coordinator ``WorkAssignment`` for execution in ``pool`` mode."""
        if self._mode != "pool":
            raise RuntimeError(f"submit_work requires mode='pool', current mode is {self._mode!r}")
        self._work_queue.put(assignment)

    def stop_work(self, job_id: str) -> None:
        """Cancel an active or queued pool job matching ``job_id``."""
        with self._lock:
            self._cancelled_jobs.add(job_id)
            if self._active_job_id == job_id:
                self._round_stop.set()

    def _set_idle(self) -> None:
        with self._lock:
            self._state = "idle"
            self._hashrate = 0.0

    def _mine_batched(
        self,
        template: Block,
        nonce_start: int,
        nonce_end: int,
        stop_event: threading.Event,
    ) -> Block | None:
        """Mine ``[nonce_start, nonce_end)`` in slices so ``hashrate`` updates live."""
        with self._lock:
            self._state = "mining"

        cursor = nonce_start
        while cursor < nonce_end:
            if self._shutdown.is_set() or stop_event.is_set():
                return None
            slice_end = min(cursor + self._batch_size, nonce_end)
            started = time.monotonic()
            found, tried = mine_with_stats(template, cursor, slice_end, stop_event)
            elapsed = time.monotonic() - started
            if tried > 0 and elapsed > 0:
                rate = tried / elapsed
                with self._lock:
                    # Smooth brief spikes while keeping the value responsive.
                    self._hashrate = (
                        rate if self._hashrate == 0.0 else (0.5 * self._hashrate + 0.5 * rate)
                    )
            if found is not None:
                return found
            cursor = slice_end
        return None

    def _compete_loop(self) -> None:
        while not self._shutdown.is_set():
            txs = self._mempool.select(self._block_size)
            if not txs and not self._mine_empty_blocks:
                self._set_idle()
                self._shutdown.wait(self._idle_poll_s)
                continue

            tip = self._chain.tip()
            # Filter out any tx already confirmed on the active chain in case of races.
            eligible_txs = [tx for tx in txs if not self._chain.has_transaction(tx.tx_id)]
            template = build_block_template(
                index=tip.index + 1,
                prev_hash=tip.hash,
                transactions=eligible_txs,
                difficulty=self._chain.difficulty,
                miner_id=self._node_id,
            )

            with self._lock:
                round_stop = threading.Event()
                self._round_stop = round_stop
                if self._shutdown.is_set():
                    break

            mined = self._mine_batched(template, 0, self._nonce_space, round_stop)
            if mined is None or self._shutdown.is_set():
                continue

            result = self._chain.add_block(mined)
            if result in (AddResult.ACCEPTED, AddResult.FORK_SWITCHED):
                log_event(
                    "block_mined",
                    self._node_id,
                    index=mined.index,
                    block_hash=mined.hash,
                    nonce=mined.nonce,
                    difficulty=mined.difficulty,
                    tx_count=len(mined.transactions),
                    mode=self._mode,
                )
                self._invoke_block_callback(mined)

        self._set_idle()

    def _pool_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                assignment = self._work_queue.get(timeout=self._idle_poll_s)
            except queue.Empty:
                self._set_idle()
                continue

            with self._lock:
                if assignment.job_id in self._cancelled_jobs:
                    continue
                round_stop = threading.Event()
                self._round_stop = round_stop
                self._active_job_id = assignment.job_id

            mined: Block | None = None
            cancelled = False
            try:
                if assignment.nonce_start < assignment.nonce_end:
                    mined = self._mine_batched(
                        assignment.block_template,
                        assignment.nonce_start,
                        assignment.nonce_end,
                        round_stop,
                    )
                cancelled = self._shutdown.is_set() or round_stop.is_set()
            finally:
                with self._lock:
                    self._active_job_id = None

            if cancelled and mined is None:
                continue

            if mined is not None:
                log_event(
                    "block_mined",
                    self._node_id,
                    job_id=assignment.job_id,
                    index=mined.index,
                    block_hash=mined.hash,
                    nonce=mined.nonce,
                    difficulty=mined.difficulty,
                    tx_count=len(mined.transactions),
                    mode=self._mode,
                )

            self._invoke_work_callback(assignment, mined)

        self._set_idle()

    def _invoke_block_callback(self, block: Block) -> None:
        if self._on_block_mined is None:
            return
        try:
            self._on_block_mined(block)
        except Exception:
            logger.exception("on_block_mined callback failed for block %s", block.index)

    def _invoke_work_callback(self, assignment: WorkAssignment, block: Block | None) -> None:
        if self._on_work_result is None:
            return
        try:
            self._on_work_result(assignment, block)
        except Exception:
            logger.exception("on_work_result callback failed for job %s", assignment.job_id)
