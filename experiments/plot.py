"""Buat grafik PNG dari CSV hasil eksperimen.

Grafik selalu diturunkan dari file di ``experiments/results/``; tidak ada angka
yang digambar manual (AGENTS.md Bagian 9.2).
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from metrics import read_results  # noqa: E402

REPO_ROOT = _EXPERIMENTS_DIR.parent
RESULTS_DIR = _EXPERIMENTS_DIR / "results"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")

logger = logging.getLogger("plot")


def _series(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], list[tuple[float, float]]]:
    grouped: dict[tuple[str, str, str], list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        try:
            value = float(row["value"])
        except (KeyError, TypeError, ValueError):
            continue
        try:
            n_nodes = int(row.get("n_nodes") or 0)
        except ValueError:
            n_nodes = 0
        try:
            trial = int(row.get("trial") or 0)
        except ValueError:
            trial = 0
        x_value = float(n_nodes if n_nodes > 0 else trial)
        key = (row.get("experiment", "unknown"), row.get("metric", "unknown"), row.get("unit", ""))
        grouped[key].append((x_value, value))
    return grouped


def plot_file(csv_path: Path, out_dir: Path) -> list[Path]:
    rows = read_results(csv_path)
    if not rows:
        logger.warning("%s kosong, tidak ada grafik dibuat", csv_path.name)
        return []

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        raise SystemExit(
            "matplotlib belum terpasang. Jalankan: pip install -r requirements.txt"
        ) from None

    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for (experiment, metric, unit), points in sorted(_series(rows).items()):
        ordered = sorted(points)
        xs = [point[0] for point in ordered]
        ys = [point[1] for point in ordered]
        figure, axes = plt.subplots(figsize=(8, 5))
        axes.plot(xs, ys, marker="o", linewidth=2)
        axes.set_title(f"{experiment}: {metric}")
        axes.set_xlabel("jumlah node" if any(x > 0 for x in xs) else "trial")
        axes.set_ylabel(f"{metric} ({unit})" if unit else metric)
        axes.grid(True, alpha=0.3)
        figure.tight_layout()
        name = _SAFE_NAME.sub("_", f"{experiment}_{metric}.png")
        target = out_dir / name
        figure.savefig(target, dpi=150)
        plt.close(figure)
        written.append(target)
        logger.info("grafik ditulis: %s", target)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Grafik dari CSV hasil eksperimen")
    parser.add_argument(
        "--input", type=Path, default=RESULTS_DIR, help="file CSV atau folder hasil"
    )
    parser.add_argument("--outdir", type=Path, default=RESULTS_DIR, help="folder tujuan PNG")
    args = parser.parse_args(argv)

    if not args.input.exists():
        logger.error("input tidak ditemukan: %s", args.input)
        return 1
    csv_files = [args.input] if args.input.is_file() else sorted(args.input.glob("*.csv"))
    if not csv_files:
        logger.error("tidak ada file CSV di %s", args.input)
        return 1

    total = 0
    for csv_path in csv_files:
        total += len(plot_file(csv_path, args.outdir))
    if total == 0:
        logger.error("tidak ada grafik yang dihasilkan")
        return 1
    logger.info("%d grafik dibuat di %s", total, args.outdir)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
