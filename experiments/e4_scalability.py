"""E4: scalability pada 2, 4, 8, dan 16 node.

Pemilik: anggota C (AGENTS.md Bagian 3 dan 9.1). Kerangka Fase 0: konfigurasi
yang dibandingkan sudah tetap, orkestrasi compose dan pembacaan metrik belum
diisi. Tidak ada angka hasil di file ini.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from compose_gen import GENERATED_DIR, ComposeSpec, build_compose  # noqa: E402
from runner import DEFAULT_COMPOSE_FILE, RESULTS_DIR  # noqa: E402

logger = logging.getLogger("e4_scalability")

RESULT_FILE = RESULTS_DIR / "e4_scalability.csv"
NODE_COUNTS = (2, 4, 8, 16)
METRICS = ("throughput_bps", "efficiency", "propagation_delay_ms")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E4: scalability jumlah node")
    parser.add_argument(
        "--dry-run", action="store_true", help="cetak rencana tanpa menyentuh Docker"
    )
    parser.add_argument("--trials", type=int, default=5, help="jumlah ulangan per konfigurasi")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.trials < 1:
        logger.error("--trials minimal 1")
        return 2

    logger.info(
        "rencana E4: %d konfigurasi x %d ulangan",
        len(NODE_COUNTS),
        args.trials,
    )
    for count in NODE_COUNTS:
        try:
            data = build_compose(ComposeSpec(n_nodes=count, template=args.compose_file))
        except (OSError, ValueError) as exc:
            logger.error("gagal membangkitkan compose untuk %d node: %s", count, exc)
            return 2
        logger.info(
            "  n_nodes=%d worker=%d metrik=%s output=%s",
            count,
            len(data["services"]) - 1,
            ",".join(METRICS),
            GENERATED_DIR / f"docker-compose.n{count}.yml",
        )
    logger.info("  template compose: %s", args.compose_file)
    logger.info("  keluaran: %s", RESULT_FILE)
    logger.info(
        "  catatan: compose per konfigurasi dibangkitkan oleh experiments/compose_gen.py "
        "(docs/decisions.md D3)"
    )

    if args.dry_run:
        return 0

    logger.error(
        "orkestrasi E4 belum diimplementasikan: perlu membangkitkan compose untuk "
        "N node, menjalankan beban, lalu membaca logs/*.jsonl."
    )
    return 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
