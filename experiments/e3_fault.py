"""E3: fault tolerance dan waktu pemulihan (skenario F1-F5).

Pemilik: anggota C (AGENTS.md Bagian 3 dan 8). Kerangka Fase 0: daftar skenario
dan rencana pengukuran sudah tetap, orkestrasi container serta pembacaan log
belum diisi. Tidak ada angka hasil di file ini.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from runner import DEFAULT_COMPOSE_FILE, RESULTS_DIR  # noqa: E402

logger = logging.getLogger("e3_fault")

RESULT_FILE = RESULTS_DIR / "e3_fault.csv"


@dataclass(frozen=True)
class Scenario:
    """Satu skenario kegagalan yang wajib diukur."""

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
        "start",
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
        "disconnect",
        ("fork_count", "reconcile_time_s", "blocks_discarded"),
    ),
    Scenario(
        "F5",
        "coordinator mati",
        "stop",
        ("peer_exchange_after_stop",),
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E3: fault tolerance dan recovery")
    parser.add_argument(
        "--dry-run", action="store_true", help="cetak rencana tanpa menyentuh Docker"
    )
    parser.add_argument("--trials", type=int, default=5, help="jumlah ulangan per skenario")
    parser.add_argument("--target-service", default="node-3", help="service yang dijadikan korban")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.trials < 1:
        logger.error("--trials minimal 1")
        return 2

    logger.info("rencana E3: %d skenario x %d ulangan", len(SCENARIOS), args.trials)
    for scenario in SCENARIOS:
        logger.info(
            "  %s %-42s injeksi=%s metrik=%s",
            scenario.scenario_id,
            scenario.description,
            scenario.injector_action,
            ",".join(scenario.metrics),
        )
    logger.info("  target service: %s", args.target_service)
    logger.info("  compose: %s", args.compose_file)
    logger.info("  keluaran: %s", RESULT_FILE)

    if args.dry_run:
        return 0

    logger.error(
        "orkestrasi E3 belum diimplementasikan: perlu menjalankan fault_inject, "
        "menunggu sync, lalu membaca logs/*.jsonl. Lihat docs/decisions.md."
    )
    return 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
