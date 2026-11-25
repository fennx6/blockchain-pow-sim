"""E4: scalability pada 2, 4, 8, dan 16 node.

Pemilik: anggota C (AGENTS.md Bagian 3 dan 9.1). Setiap konfigurasi dibangkitkan
oleh ``experiments/compose_gen.py`` (keputusan D3), dijalankan dengan beban
transaksi tetap, lalu throughput, efficiency, dan delay propagasi dihitung dari
``logs/*.jsonl``. Baseline satu node dijalankan di dalam trial yang sama supaya
efficiency memakai pembanding yang setara.

Beban transaksi dikirim lewat ``docker compose exec node-1`` sehingga
``scripts/tx_generator.py`` harus ada di dalam image node. Menambah
``COPY scripts ./scripts`` di ``docker/Dockerfile.node`` adalah tugas Docker
terpisah (lihat docs/decisions.md).
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from compose_gen import ComposeSpec, write_compose  # noqa: E402
from e2_propagation import (  # noqa: E402
    LogEvent,
    canonical_block_events,
    load_events,
    propagation_stats,
)
from metrics import efficiency, latency_stats, throughput  # noqa: E402
from runner import (  # noqa: E402
    RESULTS_DIR,
    RunContext,
    compose,
    new_run_id,
)

logger = logging.getLogger("e4_scalability")

EXPERIMENT_NAME = "e4_scalability"
RESULT_FILE = RESULTS_DIR / "e4_scalability.csv"
NODE_COUNTS = (2, 4, 8, 16)
BASELINE_NODES = 1
DEFAULT_TRIALS = 5
DEFAULT_DURATION_S = 30.0
DEFAULT_TX_RATE = 20.0
DEFAULT_NETWORK_TIMEOUT_S = 180.0
DEFAULT_GRACE_S = 3.0
POLL_INTERVAL_S = 1.0
TX_GENERATOR = "scripts/tx_generator.py"


@dataclass(frozen=True)
class ConfigResult:
    """Hasil satu konfigurasi N node dalam satu trial."""

    n_nodes: int
    blocks: int
    txs: int
    duration_s: float
    block_bps: float
    tx_tps: float
    delays_ms: tuple[float, ...]


@dataclass(frozen=True)
class RunConfig:
    """Parameter satu sesi E4."""

    logs_dir: Path
    mode: str
    difficulty: int
    duration_s: float
    tx_rate: float
    network_timeout_s: float
    grace_s: float
    keep: bool


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def window_events(
    events: list[LogEvent], since_ts_ms: int, until_ts_ms: int
) -> list[LogEvent]:
    """Kejadian di dalam jendela pengamatan, inklusif di kedua ujung."""
    return [event for event in events if since_ts_ms <= event.ts_ms <= until_ts_ms]


def blocks_in_window(
    events: list[LogEvent], since_ts_ms: int, until_ts_ms: int
) -> tuple[int, int]:
    """``(jumlah blok kanonik, jumlah transaksi)`` di jendela pengamatan."""
    confirmed = canonical_block_events(window_events(events, since_ts_ms, until_ts_ms))
    txs = 0
    for event in confirmed:
        count = event.fields.get("tx_count")
        if isinstance(count, int) and not isinstance(count, bool):
            txs += count
    return len(confirmed), txs


def propagation_delays(
    events: list[LogEvent], nodes: tuple[str, ...]
) -> tuple[float, ...]:
    """Delay ``block_mined`` ke tiap node, memakai helper E2 milik B."""
    stats = propagation_stats(events, nodes)
    return stats.delays_ms


def efficiency_value(baseline_tps: float, parallel_tps: float, workers: int) -> float | None:
    """``E = (throughput_P / throughput_1) / P``; ``None`` bila data tidak valid."""
    if baseline_tps <= 0 or parallel_tps <= 0 or workers <= 0:
        return None
    return efficiency(parallel_tps / baseline_tps, workers)


def _wait(predicate, timeout_s: float, description: str) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(POLL_INTERVAL_S)
    logger.warning("timeout menunggu %s (%.1fs)", description, timeout_s)
    return False


def _compose_file(n_nodes: int, cfg: RunConfig) -> Path:
    spec = ComposeSpec(
        n_nodes=n_nodes,
        project=f"chainsim-e4-n{n_nodes}",
        difficulty=cfg.difficulty,
        mining_mode=cfg.mode,
    )
    return write_compose(spec)


def _send_tx_load(compose_file: Path, cfg: RunConfig) -> bool:
    """Jalankan generator di dalam network; kembalikan False bila image belum siap."""
    try:
        result = compose(
            "exec",
            "-T",
            "node-1",
            "python",
            TX_GENERATOR,
            "--rate",
            str(cfg.tx_rate),
            "--duration",
            str(cfg.duration_s),
            "--target",
            "http://node-1:8081/tx",
            compose_file=compose_file,
            check=False,
        )
    except OSError as exc:
        logger.error("gagal menjalankan docker compose exec: %s", exc)
        return False
    if result.returncode != 0:
        logger.error(
            "tx_generator gagal di image node (kode %d). Pastikan scripts/ disalin ke "
            "image; lihat docstring modul.",
            result.returncode,
        )
        return False
    return True


def run_config(n_nodes: int, cfg: RunConfig, trial: int) -> ConfigResult | None:
    """Jalankan satu konfigurasi N node, beri beban, lalu hitung metrik."""
    compose_file = _compose_file(n_nodes, cfg)
    compose("up", "-d", "--build", compose_file=compose_file)
    nodes = tuple(f"node-{index}" for index in range(1, n_nodes + 1))
    since = _now_ms()
    try:
        if not _wait(
            lambda: blocks_in_window(load_events(cfg.logs_dir), since, _now_ms())[0] >= 2,
            cfg.network_timeout_s,
            f"{n_nodes} node menghasilkan blok (trial {trial})",
        ):
            return None

        window_start = _now_ms()
        if not _send_tx_load(compose_file, cfg):
            return None
        # Beri waktu blok terakhir menyebar sebelum jendela ditutup.
        time.sleep(cfg.grace_s)
        window_end = _now_ms()
        duration_s = max((window_end - window_start) / 1000.0, 0.0)
        events = window_events(load_events(cfg.logs_dir), window_start, window_end)
        blocks, txs = blocks_in_window(events, window_start, window_end)
        if duration_s <= 0:
            logger.error("jendela pengamatan nol untuk %d node", n_nodes)
            return None
        return ConfigResult(
            n_nodes=n_nodes,
            blocks=blocks,
            txs=txs,
            duration_s=duration_s,
            block_bps=throughput(blocks, duration_s),
            tx_tps=throughput(txs, duration_s),
            delays_ms=propagation_delays(events, nodes),
        )
    finally:
        if not cfg.keep:
            compose("down", "-v", compose_file=compose_file)


def _record_result(ctx: RunContext, result: ConfigResult, cfg: RunConfig, trial: int) -> None:
    common = {
        "n_nodes": result.n_nodes,
        "mode": cfg.mode,
        "difficulty": cfg.difficulty,
        "trial": trial,
    }
    ctx.record(metric="blocks_mined", value=float(result.blocks), unit="blocks", **common)
    ctx.record(
        metric="observation_duration_s", value=result.duration_s, unit="s", **common
    )
    ctx.record(
        metric="throughput_bps", value=result.block_bps, unit="blocks/s", **common
    )
    ctx.record(metric="tx_throughput_tps", value=result.tx_tps, unit="tx/s", **common)
    if result.delays_ms:
        stats = latency_stats(result.delays_ms)
        ctx.record(
            metric="propagation_delay_ms_mean", value=stats["mean"], unit="ms", **common
        )
        ctx.record(
            metric="propagation_delay_ms_p95", value=stats["p95"], unit="ms", **common
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E4: scalability jumlah node")
    parser.add_argument(
        "--dry-run", action="store_true", help="cetak rencana tanpa menyentuh Docker"
    )
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION_S)
    parser.add_argument("--tx-rate", type=float, default=DEFAULT_TX_RATE)
    parser.add_argument("--mode", choices=("compete", "pool"), default="compete")
    parser.add_argument("--difficulty", type=int, default=None)
    parser.add_argument("--logs-dir", type=Path, default=_EXPERIMENTS_DIR.parent / "logs")
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--network-timeout", type=float, default=DEFAULT_NETWORK_TIMEOUT_S
    )
    parser.add_argument("--grace", type=float, default=DEFAULT_GRACE_S)
    parser.add_argument(
        "--keep", action="store_true", help="jangan down -v setelah tiap konfigurasi"
    )
    return parser


def _log_plan(args: argparse.Namespace) -> None:
    logger.info(
        "rencana E4: %d konfigurasi x %d ulangan (+baseline %d node)",
        len(NODE_COUNTS),
        args.trials,
        BASELINE_NODES,
    )
    for count in NODE_COUNTS:
        logger.info(
            "  n_nodes=%d beban=%s tx/s selama %.0fs, mode=%s",
            count,
            args.tx_rate,
            args.duration,
            args.mode,
        )
    logger.info(
        "  metrik: throughput_bps, tx_throughput_tps, efficiency, propagation_delay_ms_mean/p95"
    )
    logger.info("  keluaran: %s", args.output_dir / f"{EXPERIMENT_NAME}.csv")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.trials < 1:
        logger.error("--trials minimal 1")
        return 2
    if args.duration <= 0:
        logger.error("--duration harus > 0")
        return 2
    if args.tx_rate <= 0:
        logger.error("--tx-rate harus > 0")
        return 2
    if args.grace < 0:
        logger.error("--grace tidak boleh negatif")
        return 2
    _log_plan(args)
    if args.dry_run:
        return 0

    from chainsim.config import load_settings

    settings = load_settings()
    cfg = RunConfig(
        logs_dir=args.logs_dir,
        mode=args.mode,
        difficulty=settings.blockchain.difficulty if args.difficulty is None else args.difficulty,
        duration_s=args.duration,
        tx_rate=args.tx_rate,
        network_timeout_s=args.network_timeout,
        grace_s=args.grace,
        keep=args.keep,
    )
    ctx = RunContext(
        run_id=args.run_id or new_run_id("e4"),
        experiment=EXPERIMENT_NAME,
        results_dir=args.output_dir,
    )

    failures: list[str] = []
    for trial in range(1, args.trials + 1):
        logger.info("trial %d/%d: baseline %d node", trial, args.trials, BASELINE_NODES)
        try:
            baseline = run_config(BASELINE_NODES, cfg, trial)
        except (OSError, subprocess.CalledProcessError) as exc:
            logger.error("baseline trial %d gagal menjalankan Docker: %s", trial, exc)
            baseline = None
        if baseline is None:
            failures.append(f"baseline/trial-{trial}")
            continue
        _record_result(ctx, baseline, cfg, trial)
        ctx.record(
            metric="efficiency",
            value=1.0,
            unit="ratio",
            n_nodes=baseline.n_nodes,
            mode=cfg.mode,
            difficulty=cfg.difficulty,
            trial=trial,
        )

        for n_nodes in NODE_COUNTS:
            logger.info("trial %d/%d: %d node", trial, args.trials, n_nodes)
            try:
                result = run_config(n_nodes, cfg, trial)
            except (OSError, subprocess.CalledProcessError) as exc:
                logger.error(
                    "%d node trial %d gagal menjalankan Docker: %s", n_nodes, trial, exc
                )
                result = None
            if result is None:
                failures.append(f"n{n_nodes}/trial-{trial}")
                continue
            _record_result(ctx, result, cfg, trial)
            value = efficiency_value(baseline.tx_tps, result.tx_tps, n_nodes)
            if value is None:
                logger.warning("efficiency tidak bisa dihitung untuk %d node", n_nodes)
                continue
            ctx.record(
                metric="efficiency",
                value=value,
                unit="ratio",
                n_nodes=n_nodes,
                mode=cfg.mode,
                difficulty=cfg.difficulty,
                trial=trial,
            )

    if failures:
        logger.error("konfigurasi tanpa data: %s", ", ".join(failures))
        return 2
    logger.info("E4 selesai: %s", ctx.result_path())
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
