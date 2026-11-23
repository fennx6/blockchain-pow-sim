"""E3: fault tolerance dan waktu pemulihan (skenario F1-F5).

Pemilik: anggota C (AGENTS.md Bagian 3 dan 8). Skrip ini menjalankan injeksi
kegagalan nyata lewat ``scripts/fault_inject.py``, menunggu kondisi yang
tercatat di ``logs/*.jsonl``, lalu menghitung metrik dari kejadian itu. Tidak
ada angka yang dikarang: bila kejadian yang dibutuhkan tidak muncul, metrik
tersebut tidak ditulis dan skrip melaporkan kegagalan.

F1 membutuhkan mode ``pool`` dan pengiriman ``SubmitTemplate`` dari node
(lihat ``docs/b-notes-for-c.md`` Fitur 5). Tanpa itu tidak ada ``job_assigned``
dan F1 dilaporkan gagal, bukan diisi angka palsu.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from e2_propagation import LogEvent, load_events  # noqa: E402
from runner import (  # noqa: E402
    DEFAULT_COMPOSE_FILE,
    RESULTS_DIR,
    RunContext,
    compose,
    new_run_id,
    run_command,
)

logger = logging.getLogger("e3_fault")

EXPERIMENT_NAME = "e3_fault"
RESULT_FILE = RESULTS_DIR / "e3_fault.csv"
COORDINATOR_ID = "coordinator"
FAULT_INJECTOR = Path(__file__).resolve().parents[1] / "scripts" / "fault_inject.py"

DEFAULT_TARGET = "node-3"
DEFAULT_TRIALS = 3
DEFAULT_NODES = ",".join(f"node-{index}" for index in range(1, 7))
DEFAULT_DOWNTIME_S = 5.0
DEFAULT_NETWORK_TIMEOUT_S = 120.0
DEFAULT_SYNC_TIMEOUT_S = 120.0
DEFAULT_JOB_TIMEOUT_S = 60.0
DEFAULT_RECOVERY_TIMEOUT_S = 60.0
POLL_INTERVAL_S = 0.5

SCENARIO_IDS = ("F1", "F2", "F3", "F4", "F5")
REQUIRED_SCENARIOS = ("F1", "F2", "F3")


@dataclass(frozen=True)
class Scenario:
    """Satu skenario kegagalan yang diukur."""

    scenario_id: str
    description: str
    injector_action: str
    metrics: tuple[str, ...]


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "F1",
        "worker mati saat mining (mode pool)",
        "stop",
        ("detection_time_s", "reassignment_time_s", "block_found"),
    ),
    Scenario(
        "F2",
        "node mati lalu hidup lagi dengan volume utuh",
        "stop/start",
        ("sync_time_s", "blocks_pulled"),
    ),
    Scenario(
        "F3",
        "node hidup lagi dengan volume kosong",
        "remove-volume",
        ("full_sync_time_s", "blocks_pulled"),
    ),
    Scenario(
        "F4",
        "partisi jaringan lalu disambungkan lagi",
        "disconnect/connect",
        ("fork_count", "reconcile_time_s", "blocks_discarded"),
    ),
    Scenario(
        "F5",
        "coordinator mati",
        "stop coordinator",
        ("peer_exchange_after_stop",),
    ),
)


@dataclass(frozen=True)
class RunConfig:
    """Parameter satu sesi E3."""

    compose_file: Path
    logs_dir: Path
    target: str
    nodes: tuple[str, ...]
    difficulty: int
    downtime_s: float
    network_timeout_s: float
    sync_timeout_s: float
    job_timeout_s: float


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _first_after(
    events: Sequence[LogEvent],
    event_name: str,
    after_ts_ms: int,
    predicate: Callable[[LogEvent], bool] | None = None,
) -> LogEvent | None:
    for event in events:
        if event.ts_ms < after_ts_ms or event.event != event_name:
            continue
        if predicate is not None and not predicate(event):
            continue
        return event
    return None


def detection_time_s(events: Sequence[LogEvent], node_id: str, fault_ts_ms: int) -> float | None:
    """Waktu dari injeksi sampai coordinator mencatat ``peer_down`` untuk node."""
    event = _first_after(
        events,
        "peer_down",
        fault_ts_ms,
        lambda item: item.fields.get("target") == node_id,
    )
    return None if event is None else (event.ts_ms - fault_ts_ms) / 1000.0


def reassignment_time_s(
    events: Sequence[LogEvent], job_id: str, fault_ts_ms: int
) -> float | None:
    """Waktu dari injeksi sampai ``job_reassigned`` untuk job tersebut."""
    event = _first_after(
        events,
        "job_reassigned",
        fault_ts_ms,
        lambda item: item.fields.get("job_id") == job_id,
    )
    return None if event is None else (event.ts_ms - fault_ts_ms) / 1000.0


def job_completed_after(events: Sequence[LogEvent], job_id: str, after_ts_ms: int) -> bool:
    """True bila job pool tetap menghasilkan blok setelah worker mati."""
    for event in events:
        if event.ts_ms < after_ts_ms:
            continue
        if event.fields.get("job_id") != job_id:
            continue
        if event.event in {"job_done", "job_finished"}:
            return True
        if event.event == "block_mined":
            return True
    return False


def sync_metrics(
    events: Sequence[LogEvent], node_id: str, action_ts_ms: int
) -> tuple[float, int] | None:
    """``(sync_time_s, blocks_pulled)`` dari ``sync_done`` pertama setelah aksi."""
    done = _first_after(events, "sync_done", action_ts_ms, lambda item: item.node_id == node_id)
    if done is None:
        return None
    blocks = _as_int(done.fields.get("blocks_pulled")) or 0
    return (done.ts_ms - action_ts_ms) / 1000.0, blocks


def fork_metrics(events: Sequence[LogEvent], after_ts_ms: int) -> tuple[int, int]:
    """Jumlah ``fork_switched`` dan total blok yang dibuang setelah titik waktu."""
    switched = [
        event
        for event in events
        if event.event == "fork_switched" and event.ts_ms >= after_ts_ms
    ]
    discarded = sum(_as_int(event.fields.get("blocks_discarded")) or 0 for event in switched)
    return len(switched), discarded


def _height_updates(events: Sequence[LogEvent], after_ts_ms: int) -> list[tuple[int, str, int]]:
    updates: list[tuple[int, str, int]] = []
    for event in events:
        if event.ts_ms < after_ts_ms:
            continue
        if event.event not in {"block_mined", "block_received"}:
            continue
        index = _as_int(event.fields.get("index"))
        if index is not None:
            updates.append((event.ts_ms, event.node_id, index))
    return updates


def time_to_consistent_height(
    events: Sequence[LogEvent], nodes: Sequence[str], after_ts_ms: int
) -> float | None:
    """Waktu sampai semua node melaporkan tinggi yang sama (F4)."""
    heights: dict[str, int] = {node: -1 for node in nodes}
    for ts_ms, node_id, index in _height_updates(events, after_ts_ms):
        if node_id not in heights:
            continue
        heights[node_id] = max(heights[node_id], index)
        if min(heights.values()) >= 0 and len(set(heights.values())) == 1:
            return (ts_ms - after_ts_ms) / 1000.0
    return None


def mined_after(events: Sequence[LogEvent], after_ts_ms: int) -> int:
    return sum(1 for event in events if event.event == "block_mined" and event.ts_ms >= after_ts_ms)


def active_miners_after(events: Sequence[LogEvent], after_ts_ms: int) -> set[str]:
    return {
        event.node_id
        for event in events
        if event.event == "block_mined" and event.ts_ms >= after_ts_ms
    }


class DockerController:
    """Pembungkus tipis perintah compose dan fault injector."""

    def __init__(self, compose_file: Path, logs_dir: Path, *, dry_run: bool = False) -> None:
        self._compose_file = compose_file
        self._logs_dir = logs_dir
        self._dry_run = dry_run

    def up(self, *, mining_mode: str, build: bool = True) -> None:
        args = ["up", "-d"]
        if build:
            args.append("--build")
        compose(
            *args,
            compose_file=self._compose_file,
            dry_run=self._dry_run,
            env={"MINING_MODE": mining_mode, "LOG_DIR": str(self._logs_dir)},
        )

    def down(self, *, volumes: bool = False) -> None:
        args = ["down"]
        if volumes:
            args.append("-v")
        compose(*args, compose_file=self._compose_file, dry_run=self._dry_run)

    def stop_service(self, service: str) -> None:
        compose("stop", service, compose_file=self._compose_file, dry_run=self._dry_run)

    def start_service(self, service: str) -> None:
        compose("start", service, compose_file=self._compose_file, dry_run=self._dry_run)

    def fault(self, action: str, service: str) -> None:
        command = [
            sys.executable,
            str(FAULT_INJECTOR),
            "--compose-file",
            str(self._compose_file),
            "--dry-run" if self._dry_run else "",
            action,
            "--service",
            service,
        ]
        run_command(
            [part for part in command if part],
            dry_run=self._dry_run,
            env={"LOG_DIR": str(self._logs_dir)},
        )

    def wait(
        self,
        predicate: Callable[[], bool],
        timeout_s: float,
        description: str,
    ) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self._dry_run or predicate():
                return True
            time.sleep(POLL_INTERVAL_S)
        logger.warning("timeout menunggu %s (%.1fs)", description, timeout_s)
        return False


def _events(logs_dir: Path) -> list[LogEvent]:
    return load_events(logs_dir)


def _record(
    ctx: RunContext,
    metric: str,
    value: float,
    unit: str,
    *,
    cfg: RunConfig,
    mode: str,
    trial: int,
) -> None:
    ctx.record(
        metric=metric,
        value=value,
        unit=unit,
        n_nodes=len(cfg.nodes),
        mode=mode,
        difficulty=cfg.difficulty,
        trial=trial,
    )


def run_f1(
    ctx: RunContext, controller: DockerController, cfg: RunConfig, trial: int
) -> bool:
    """Worker mati saat job pool berjalan; ukur deteksi, reassignment, hasil."""
    controller.up(mining_mode="pool")
    since = _now_ms()
    if not controller.wait(
        lambda: _first_after(_events(cfg.logs_dir), "job_assigned", since) is not None,
        cfg.job_timeout_s,
        "job pool pertama (F1)",
    ):
        logger.error(
            "F1 trial %d: tidak ada job_assigned. Mode pool membutuhkan SubmitTemplate "
            "dari node; lihat docs/b-notes-for-c.md Fitur 5.",
            trial,
        )
        return False

    assigned = _first_after(_events(cfg.logs_dir), "job_assigned", since)
    job_id = str(assigned.fields.get("job_id") or "") if assigned is not None else ""
    controller.fault("stop", cfg.target)
    fault_ts = _now_ms()

    controller.wait(
        lambda: job_completed_after(_events(cfg.logs_dir), job_id, fault_ts),
        cfg.network_timeout_s,
        "job pool selesai setelah worker mati (F1)",
    )
    events = _events(cfg.logs_dir)
    detection = detection_time_s(events, cfg.target, fault_ts)
    reassignment = reassignment_time_s(events, job_id, fault_ts)
    found = job_completed_after(events, job_id, fault_ts)

    if detection is not None:
        _record(ctx, "detection_time_s", detection, "s", cfg=cfg, mode="pool", trial=trial)
    if reassignment is not None:
        _record(
            ctx, "reassignment_time_s", reassignment, "s", cfg=cfg, mode="pool", trial=trial
        )
    _record(ctx, "block_found", 1.0 if found else 0.0, "bool", cfg=cfg, mode="pool", trial=trial)

    controller.fault("start", cfg.target)
    return detection is not None and reassignment is not None and found


def run_f2(
    ctx: RunContext, controller: DockerController, cfg: RunConfig, trial: int
) -> bool:
    """Node mati lalu hidup lagi dengan volume utuh; ukur waktu sync."""
    controller.up(mining_mode="compete")
    since = _now_ms()
    if not controller.wait(
        lambda: mined_after(_events(cfg.logs_dir), since) >= 2,
        cfg.network_timeout_s,
        "blok awal (F2)",
    ):
        logger.error("F2 trial %d: jaringan tidak menghasilkan blok", trial)
        return False

    controller.fault("stop", cfg.target)
    time.sleep(cfg.downtime_s)
    controller.fault("start", cfg.target)
    action_ts = _now_ms()

    if not controller.wait(
        lambda: sync_metrics(_events(cfg.logs_dir), cfg.target, action_ts) is not None,
        cfg.sync_timeout_s,
        f"sync {cfg.target} (F2)",
    ):
        return False

    metrics = sync_metrics(_events(cfg.logs_dir), cfg.target, action_ts)
    assert metrics is not None  # dijaga oleh wait() di atas
    _record(ctx, "sync_time_s", metrics[0], "s", cfg=cfg, mode="compete", trial=trial)
    _record(ctx, "blocks_pulled", float(metrics[1]), "blocks", cfg=cfg, mode="compete", trial=trial)
    return True


def run_f3(
    ctx: RunContext, controller: DockerController, cfg: RunConfig, trial: int
) -> bool:
    """Volume node dihapus; ukur full sync dari genesis."""
    controller.up(mining_mode="compete")
    since = _now_ms()
    if not controller.wait(
        lambda: mined_after(_events(cfg.logs_dir), since) >= 2,
        cfg.network_timeout_s,
        "blok awal (F3)",
    ):
        logger.error("F3 trial %d: jaringan tidak menghasilkan blok", trial)
        return False

    controller.fault("remove-volume", cfg.target)
    controller.up(mining_mode="compete", build=False)
    action_ts = _now_ms()

    if not controller.wait(
        lambda: sync_metrics(_events(cfg.logs_dir), cfg.target, action_ts) is not None,
        cfg.sync_timeout_s,
        f"full sync {cfg.target} (F3)",
    ):
        return False

    metrics = sync_metrics(_events(cfg.logs_dir), cfg.target, action_ts)
    assert metrics is not None
    _record(ctx, "full_sync_time_s", metrics[0], "s", cfg=cfg, mode="compete", trial=trial)
    _record(ctx, "blocks_pulled", float(metrics[1]), "blocks", cfg=cfg, mode="compete", trial=trial)
    return True


def run_f4(
    ctx: RunContext, controller: DockerController, cfg: RunConfig, trial: int
) -> bool:
    """Partisi jaringan lalu sambungkan lagi; ukur fork dan rekonsiliasi."""
    controller.up(mining_mode="compete")
    since = _now_ms()
    if not controller.wait(
        lambda: mined_after(_events(cfg.logs_dir), since) >= 2,
        cfg.network_timeout_s,
        "blok awal (F4)",
    ):
        logger.error("F4 trial %d: jaringan tidak menghasilkan blok", trial)
        return False

    controller.fault("disconnect", cfg.target)
    disconnect_ts = _now_ms()
    controller.wait(
        lambda: mined_after(_events(cfg.logs_dir), disconnect_ts) >= 2,
        cfg.network_timeout_s,
        "blok selama partisi (F4)",
    )
    controller.fault("connect", cfg.target)
    connect_ts = _now_ms()

    controller.wait(
        lambda: time_to_consistent_height(_events(cfg.logs_dir), cfg.nodes, connect_ts)
        is not None,
        cfg.sync_timeout_s,
        f"rekonsiliasi {cfg.target} (F4)",
    )
    events = _events(cfg.logs_dir)
    fork_count, discarded = fork_metrics(events, disconnect_ts)
    reconcile = time_to_consistent_height(events, cfg.nodes, connect_ts)

    _record(ctx, "fork_count", float(fork_count), "forks", cfg=cfg, mode="compete", trial=trial)
    _record(
        ctx, "blocks_discarded", float(discarded), "blocks", cfg=cfg, mode="compete", trial=trial
    )
    if reconcile is not None:
        _record(
            ctx, "reconcile_time_s", reconcile, "s", cfg=cfg, mode="compete", trial=trial
        )
    return reconcile is not None


def run_f5(
    ctx: RunContext, controller: DockerController, cfg: RunConfig, trial: int
) -> bool:
    """Coordinator mati; pastikan node tetap bertukar blok dengan peer tersimpan."""
    controller.up(mining_mode="compete")
    since = _now_ms()
    if not controller.wait(
        lambda: mined_after(_events(cfg.logs_dir), since) >= 1,
        cfg.network_timeout_s,
        "blok awal (F5)",
    ):
        logger.error("F5 trial %d: jaringan tidak menghasilkan blok", trial)
        return False

    controller.stop_service("coordinator")
    stop_ts = _now_ms()
    time.sleep(cfg.downtime_s)
    miners = active_miners_after(_events(cfg.logs_dir), stop_ts)
    controller.start_service("coordinator")

    _record(
        ctx,
        "peer_exchange_after_stop",
        1.0 if len(miners) >= 2 else 0.0,
        "bool",
        cfg=cfg,
        mode="compete",
        trial=trial,
    )
    return len(miners) >= 2


SCENARIO_RUNNERS: dict[str, Callable[[RunContext, DockerController, RunConfig, int], bool]] = {
    "F1": run_f1,
    "F2": run_f2,
    "F3": run_f3,
    "F4": run_f4,
    "F5": run_f5,
}


def parse_scenarios(raw: str) -> tuple[str, ...]:
    selected = tuple(dict.fromkeys(part.strip().upper() for part in raw.split(",") if part.strip()))
    unknown = [item for item in selected if item not in SCENARIO_IDS]
    if unknown:
        raise ValueError(f"skenario tidak dikenal: {', '.join(unknown)}")
    if not selected:
        raise ValueError("daftar skenario tidak boleh kosong")
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E3: fault tolerance dan recovery")
    parser.add_argument(
        "--dry-run", action="store_true", help="cetak rencana tanpa menyentuh Docker"
    )
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="ulangan per skenario")
    parser.add_argument(
        "--target-service", default=DEFAULT_TARGET, help="service yang dijadikan korban"
    )
    parser.add_argument("--scenarios", default=",".join(REQUIRED_SCENARIOS))
    parser.add_argument("--nodes", default=DEFAULT_NODES, help="daftar node yang diharapkan")
    parser.add_argument("--logs-dir", type=Path, default=_EXPERIMENTS_DIR.parent / "logs")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--downtime", type=float, default=DEFAULT_DOWNTIME_S, help="detik node mati"
    )
    parser.add_argument("--network-timeout", type=float, default=DEFAULT_NETWORK_TIMEOUT_S)
    parser.add_argument("--sync-timeout", type=float, default=DEFAULT_SYNC_TIMEOUT_S)
    parser.add_argument("--job-timeout", type=float, default=DEFAULT_JOB_TIMEOUT_S)
    return parser


def _log_plan(args: argparse.Namespace, scenarios: Sequence[str]) -> None:
    logger.info("rencana E3: %d skenario x %d ulangan", len(scenarios), args.trials)
    by_id = {scenario.scenario_id: scenario for scenario in SCENARIOS}
    for scenario_id in scenarios:
        scenario = by_id[scenario_id]
        logger.info(
            "  %s %-42s injeksi=%s metrik=%s",
            scenario.scenario_id,
            scenario.description,
            scenario.injector_action,
            ",".join(scenario.metrics),
        )
    logger.info("  target service: %s", args.target_service)
    logger.info("  compose: %s", args.compose_file)
    logger.info("  logs: %s", args.logs_dir)
    logger.info("  keluaran: %s", args.output_dir / f"{EXPERIMENT_NAME}.csv")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.trials < 1:
        logger.error("--trials minimal 1")
        return 2
    if args.downtime <= 0:
        logger.error("--downtime harus > 0")
        return 2
    try:
        scenarios = parse_scenarios(args.scenarios)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    _log_plan(args, scenarios)
    if args.dry_run:
        return 0

    from chainsim.config import load_settings

    settings = load_settings()
    cfg = RunConfig(
        compose_file=args.compose_file,
        logs_dir=args.logs_dir,
        target=args.target_service,
        nodes=tuple(part.strip() for part in args.nodes.split(",") if part.strip()),
        difficulty=settings.blockchain.difficulty,
        downtime_s=args.downtime,
        network_timeout_s=args.network_timeout,
        sync_timeout_s=args.sync_timeout,
        job_timeout_s=args.job_timeout,
    )
    ctx = RunContext(
        run_id=args.run_id or new_run_id("e3"),
        experiment=EXPERIMENT_NAME,
        results_dir=args.output_dir,
    )
    controller = DockerController(args.compose_file, args.logs_dir)

    failures: list[str] = []
    for trial in range(1, args.trials + 1):
        for scenario_id in scenarios:
            logger.info("menjalankan %s trial %d/%d", scenario_id, trial, args.trials)
            try:
                ok = SCENARIO_RUNNERS[scenario_id](ctx, controller, cfg, trial)
            except (OSError, subprocess.CalledProcessError) as exc:
                logger.error(
                    "skenario %s trial %d gagal menjalankan Docker: %s",
                    scenario_id,
                    trial,
                    exc,
                )
                ok = False
            if not ok:
                failures.append(f"{scenario_id}/trial-{trial}")

    if failures:
        logger.error("skenario tanpa data lengkap: %s", ", ".join(failures))
        return 2
    logger.info("E3 selesai: %s", ctx.result_path())
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
