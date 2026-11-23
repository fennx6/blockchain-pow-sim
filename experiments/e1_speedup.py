"""E1: speedup and efficiency of parallel Proof-of-Work mining in pool mode.

Owner: Member A (AGENTS.md Section 3 & Section 9.1).
Measures sequential mining time ``T_sekuensial`` (P=1 worker) against parallel
mining time ``T_paralel`` (P workers splitting the nonce space via
``chainsim.coordinator.scheduler.split_nonce_space``) over identical
seed-generated block payloads, then records ``mining_time_s``, ``speedup``,
``efficiency``, and ``throughput_bps`` into ``experiments/results/e1_speedup.csv``.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import multiprocessing as mp
import random
import statistics
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _EXPERIMENTS_DIR.parent
_SRC_DIR = _REPO_ROOT / "src"
for _path in (_REPO_ROOT, _EXPERIMENTS_DIR, _SRC_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from experiments.metrics import efficiency, speedup, throughput  # noqa: E402
from experiments.runner import RESULTS_DIR, RunContext, new_run_id  # noqa: E402

from chainsim.config import load_settings  # noqa: E402
from chainsim.coordinator.scheduler import split_nonce_space  # noqa: E402
from chainsim.core.block import build_block_template, create_genesis_block  # noqa: E402
from chainsim.core.pow import is_valid_pow, mine_with_stats  # noqa: E402
from chainsim.core.transaction import create_transaction  # noqa: E402
from chainsim.types import Block, Transaction  # noqa: E402

logger = logging.getLogger("e1_speedup")

EXPERIMENT_NAME = "e1_speedup"
DEFAULT_WORKERS = (1, 2, 4, 6)
DEFAULT_BLOCKS_PER_TRIAL = 10
DEFAULT_TRIALS = 5
DEFAULT_TX_PER_BLOCK = 10
DEFAULT_SEED = 42
WORKER_READY_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class TrialMeasurement:
    """Measured wall-clock result for one trial of P workers mining N blocks."""

    workers: int
    trial: int
    blocks_mined: int
    elapsed_s: float
    total_hashes: int


def parse_worker_counts(raw: str) -> tuple[int, ...]:
    """Parse a comma-separated list of positive worker counts (must include 1)."""
    counts: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError(f"invalid worker count: {text!r}") from exc
        if value < 1:
            raise ValueError(f"worker count must be >= 1, got {value}")
        if value not in counts:
            counts.append(value)
    if not counts:
        raise ValueError("worker list must not be empty")
    if 1 not in counts:
        raise ValueError("worker list must include 1 as sequential baseline")
    return tuple(sorted(counts))


def generate_block_transactions(
    *,
    seed: int,
    trial: int,
    block_index: int,
    tx_count: int,
) -> list[Transaction]:
    """Build deterministic synthetic transactions so every worker count mines equal payloads."""
    rng = random.Random(seed * 1_000_003 + trial * 10_007 + block_index)
    txs: list[Transaction] = []
    base_ts = 1_700_000_000_000 + trial * 100_000 + block_index * 1_000
    for idx in range(tx_count):
        sender = f"addr-{rng.randint(1, 50)}"
        receiver = f"addr-{rng.randint(51, 100)}"
        amount = round(rng.uniform(1.0, 100.0), 2)
        txs.append(
            create_transaction(
                sender=sender,
                receiver=receiver,
                amount=amount,
                origin_node="pool-coordinator",
                timestamp_ms=base_ts + idx,
            )
        )
    return txs


def build_trial_templates(
    *,
    seed: int,
    trial: int,
    blocks_per_trial: int,
    difficulty: int,
    tx_per_block: int,
) -> list[Block]:
    """Build identical block templates for a trial across all worker counts."""
    genesis = create_genesis_block(difficulty=difficulty)
    templates: list[Block] = []
    for block_idx in range(1, blocks_per_trial + 1):
        txs = generate_block_transactions(
            seed=seed,
            trial=trial,
            block_index=block_idx,
            tx_count=tx_per_block,
        )
        prev_hash = (
            genesis.hash
            if block_idx == 1
            else hashlib.sha256(f"{seed}:{trial}:{block_idx - 1}".encode()).hexdigest()
        )
        templates.append(
            build_block_template(
                index=block_idx,
                prev_hash=prev_hash,
                transactions=txs,
                difficulty=difficulty,
                miner_id="pool-worker",
                timestamp_ms=1_700_000_000_000 + trial * 100_000 + block_idx * 1_000,
            )
        )
    return templates


def _worker_process(
    task_queue: Any,
    result_queue: Any,
    stop_event: Any,
    ready_queue: Any,
) -> None:
    """Persistent pool worker process executing assigned nonce slices."""
    ready_queue.put(True)
    while True:
        task = task_queue.get()
        if task is None:
            return
        round_id, template, nonce_start, nonce_end = task
        found, hashes_tried = mine_with_stats(template, nonce_start, nonce_end, stop_event)
        if found is not None:
            stop_event.set()
        result_queue.put((round_id, found, hashes_tried))


class _WorkerPool:
    """Managed pool of P OS processes mirroring P single-core worker nodes."""

    def __init__(self, n_workers: int) -> None:
        self._n_workers = n_workers
        ctx = mp.get_context("spawn")
        self._stop_event = ctx.Event()
        self._result_queue = ctx.Queue()
        self._ready_queue = ctx.Queue()
        self._task_queues = [ctx.Queue() for _ in range(n_workers)]
        self._processes = [
            ctx.Process(
                target=_worker_process,
                args=(task_q, self._result_queue, self._stop_event, self._ready_queue),
                daemon=True,
            )
            for task_q in self._task_queues
        ]

    def __enter__(self) -> _WorkerPool:
        try:
            for proc in self._processes:
                proc.start()
            # Wait until every spawned worker has finished importing and is ready to mine
            # so process startup overhead is never included in Trial 1 timing.
            for _ in range(self._n_workers):
                self._ready_queue.get(timeout=WORKER_READY_TIMEOUT_S)
        except Exception:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_: object) -> None:
        self._stop_event.set()
        for task_q in self._task_queues:
            task_q.put(None)
        for proc in self._processes:
            if proc.pid is None:
                continue
            proc.join(timeout=5.0)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2.0)

    def mine_block(
        self,
        round_id: int,
        template: Block,
        nonce_space: int,
    ) -> tuple[Block, float, int]:
        """Split ``[0, nonce_space)`` across P workers and return ``(block, elapsed_s, hashes)``."""
        ranges = split_nonce_space(self._n_workers, 0, nonce_space)
        self._stop_event.clear()

        started = time.perf_counter()
        for task_q, (start, end) in zip(self._task_queues, ranges, strict=True):
            task_q.put((round_id, template, start, end))

        winner: Block | None = None
        elapsed_s = 0.0
        total_hashes = 0
        # Wait for every worker to finish this round before clearing stop_event next round.
        for _ in range(self._n_workers):
            _, found, hashes_tried = self._result_queue.get()
            total_hashes += hashes_tried
            if found is not None and winner is None:
                winner = found
                elapsed_s = time.perf_counter() - started
                self._stop_event.set()

        if winner is None:
            raise RuntimeError(
                f"no valid nonce found in [0, {nonce_space}) for difficulty {template.difficulty}"
            )
        if not is_valid_pow(winner):
            raise RuntimeError(f"worker returned invalid PoW block at index {winner.index}")
        return winner, elapsed_s, total_hashes


def run_trial(
    pool: _WorkerPool,
    *,
    workers: int,
    trial: int,
    blocks_per_trial: int,
    difficulty: int,
    nonce_space: int,
    tx_per_block: int,
    seed: int,
) -> TrialMeasurement:
    """Mine ``blocks_per_trial`` deterministic block templates and return timing stats."""
    templates = build_trial_templates(
        seed=seed,
        trial=trial,
        blocks_per_trial=blocks_per_trial,
        difficulty=difficulty,
        tx_per_block=tx_per_block,
    )
    total_elapsed = 0.0
    total_hashes = 0

    for block_idx, template in enumerate(templates, start=1):
        round_id = trial * (blocks_per_trial + 1) + block_idx
        _, block_elapsed, block_hashes = pool.mine_block(round_id, template, nonce_space)
        total_elapsed += block_elapsed
        total_hashes += block_hashes

    return TrialMeasurement(
        workers=workers,
        trial=trial,
        blocks_mined=blocks_per_trial,
        elapsed_s=total_elapsed,
        total_hashes=total_hashes,
    )


def run_experiment(
    *,
    worker_counts: Sequence[int],
    blocks_per_trial: int,
    trials: int,
    difficulty: int,
    nonce_space: int,
    tx_per_block: int,
    seed: int,
    results_dir: Path = RESULTS_DIR,
) -> tuple[Path, list[dict[str, Any]]]:
    """Execute E1 across all worker counts and trials, writing real measurements to CSV."""
    if 1 not in worker_counts:
        raise ValueError("worker_counts must include 1 for sequential baseline")

    ordered_workers = sorted(set(worker_counts))
    run_id = new_run_id("e1")
    ctx = RunContext(run_id=run_id, experiment=EXPERIMENT_NAME, results_dir=results_dir)
    measurements: dict[int, list[TrialMeasurement]] = {}

    for workers in ordered_workers:
        per_worker: list[TrialMeasurement] = []
        with _WorkerPool(workers) as pool:
            for trial in range(1, trials + 1):
                result = run_trial(
                    pool,
                    workers=workers,
                    trial=trial,
                    blocks_per_trial=blocks_per_trial,
                    difficulty=difficulty,
                    nonce_space=nonce_space,
                    tx_per_block=tx_per_block,
                    seed=seed,
                )
                per_worker.append(result)
                logger.info(
                    "workers=%d trial=%d/%d elapsed=%.4fs hashes=%d",
                    workers,
                    trial,
                    trials,
                    result.elapsed_s,
                    result.total_hashes,
                )
        measurements[workers] = per_worker

    t_seq_mean = statistics.fmean(item.elapsed_s for item in measurements[1])
    written_rows: list[dict[str, Any]] = []

    for workers in ordered_workers:
        times: list[float] = []

        for item in measurements[workers]:
            s_val = 1.0 if workers == 1 else speedup(t_seq_mean, item.elapsed_s)
            e_val = efficiency(s_val, workers)
            bps = throughput(item.blocks_mined, item.elapsed_s)

            times.append(item.elapsed_s)

            common = {
                "n_nodes": workers,
                "mode": "pool",
                "difficulty": difficulty,
                "trial": item.trial,
            }
            written_rows.append(
                ctx.record(metric="mining_time_s", value=item.elapsed_s, unit="s", **common)
            )
            written_rows.append(ctx.record(metric="speedup", value=s_val, unit="ratio", **common))
            written_rows.append(
                ctx.record(metric="efficiency", value=e_val, unit="ratio", **common)
            )
            written_rows.append(
                ctx.record(metric="throughput_bps", value=bps, unit="blocks/s", **common)
            )

        mean_time = statistics.fmean(times)
        t_std = statistics.stdev(times) if len(times) > 1 else 0.0
        # Compute ratio-of-means speedup (T_seq_mean / T_par_mean) to avoid upward
        # Jensen's inequality bias from averaging individual T_seq / T_par ratios.
        mean_speedup = 1.0 if workers == 1 else speedup(t_seq_mean, mean_time)
        mean_efficiency = efficiency(mean_speedup, workers)
        logger.info(
            "summary workers=%d time=%.4f+/-%.4fs speedup=%.3f efficiency=%.3f",
            workers,
            mean_time,
            t_std,
            mean_speedup,
            mean_efficiency,
        )

    return ctx.result_path(), written_rows


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="E1: speedup and efficiency of pool PoW mining")
    parser.add_argument(
        "--dry-run", action="store_true", help="print experiment plan without mining or writing CSV"
    )
    parser.add_argument(
        "--workers",
        default=",".join(str(w) for w in DEFAULT_WORKERS),
        help="comma-separated worker counts (must include 1)",
    )
    parser.add_argument(
        "--blocks",
        type=int,
        default=DEFAULT_BLOCKS_PER_TRIAL,
        help="number of blocks mined per trial",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=DEFAULT_TRIALS,
        help="number of repetitions per worker configuration",
    )
    parser.add_argument(
        "--difficulty",
        type=int,
        default=settings.blockchain.difficulty,
        help="PoW difficulty (leading hex zeroes)",
    )
    parser.add_argument(
        "--nonce-space",
        type=int,
        default=settings.blockchain.nonce_space,
        help="total nonce search space per block",
    )
    parser.add_argument(
        "--tx-per-block",
        type=int,
        default=DEFAULT_TX_PER_BLOCK,
        help="deterministic synthetic transactions per block",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="fixed seed for reproducible block payloads",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=RESULTS_DIR,
        help="directory where e1_speedup.csv will be written",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        worker_counts = parse_worker_counts(args.workers)
    except ValueError as exc:
        logger.error("invalid --workers: %s", exc)
        return 2

    if args.blocks < 1:
        logger.error("--blocks must be >= 1")
        return 2
    if args.trials < 1:
        logger.error("--trials must be >= 1")
        return 2
    if args.difficulty < 1:
        logger.error("--difficulty must be >= 1")
        return 2
    if args.nonce_space < 1:
        logger.error("--nonce-space must be >= 1")
        return 2
    if args.tx_per_block < 0:
        logger.error("--tx-per-block must be >= 0")
        return 2

    output_path = args.output_dir / f"{EXPERIMENT_NAME}.csv"
    logger.info(
        "E1 plan: workers=%s blocks=%d trials=%d difficulty=%d tx_per_block=%d seed=%d",
        list(worker_counts),
        args.blocks,
        args.trials,
        args.difficulty,
        args.tx_per_block,
        args.seed,
    )
    logger.info("  output: %s", output_path)

    if args.dry_run:
        return 0

    csv_path, rows = run_experiment(
        worker_counts=worker_counts,
        blocks_per_trial=args.blocks,
        trials=args.trials,
        difficulty=args.difficulty,
        nonce_space=args.nonce_space,
        tx_per_block=args.tx_per_block,
        seed=args.seed,
        results_dir=args.output_dir,
    )
    logger.info("wrote %d rows to %s", len(rows), csv_path)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
