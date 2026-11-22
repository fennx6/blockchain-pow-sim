"""Fungsi metrik murni untuk eksperimen E1-E4 (AGENTS.md Bagian 9.1).

Semua fungsi hanya menghitung dari angka yang diberikan pemanggil; tidak ada
nilai default yang terlihat seperti hasil pengukuran.
"""

from __future__ import annotations

import csv
import math
import statistics
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

RESULT_FIELDS = (
    "run_id",
    "timestamp",
    "experiment",
    "n_nodes",
    "mode",
    "difficulty",
    "trial",
    "metric",
    "value",
    "unit",
)


def speedup(sequential_s: float, parallel_s: float) -> float:
    """``S = T_sekuensial / T_paralel``."""
    if sequential_s <= 0:
        raise ValueError(f"T_sekuensial harus > 0, bukan {sequential_s}")
    if parallel_s <= 0:
        raise ValueError(f"T_paralel harus > 0, bukan {parallel_s}")
    return sequential_s / parallel_s


def efficiency(speedup_value: float, workers: int) -> float:
    """``E = S / P``."""
    if workers <= 0:
        raise ValueError(f"jumlah worker harus > 0, bukan {workers}")
    if speedup_value <= 0:
        raise ValueError(f"speedup harus > 0, bukan {speedup_value}")
    return speedup_value / workers


def throughput(items: float, duration_s: float) -> float:
    """Item (blok atau transaksi) per detik."""
    if duration_s <= 0:
        raise ValueError(f"durasi harus > 0, bukan {duration_s}")
    if items < 0:
        raise ValueError(f"jumlah item tidak boleh negatif, bukan {items}")
    return items / duration_s


def percentile(samples: Sequence[float], pct: float) -> float:
    """Persentil dengan interpolasi nearest-rank pada data terurut."""
    if not samples:
        raise ValueError("samples tidak boleh kosong")
    if not 0 < pct <= 100:
        raise ValueError(f"persentil harus di rentang (0, 100], bukan {pct}")
    ordered = sorted(float(value) for value in samples)
    rank = math.ceil(pct / 100.0 * len(ordered))
    return ordered[max(rank - 1, 0)]


def latency_stats(samples_ms: Sequence[float]) -> dict[str, float]:
    """Ringkasan latensi/delay dalam milidetik."""
    if not samples_ms:
        raise ValueError("samples_ms tidak boleh kosong")
    values = [float(value) for value in samples_ms]
    return {
        "count": float(len(values)),
        "mean": statistics.fmean(values),
        "min": min(values),
        "max": max(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
    }


def recovery_time_s(down_ts_ms: int, recovered_ts_ms: int) -> float:
    """Selisih waktu mati ke waktu pulih, dalam detik."""
    if recovered_ts_ms < down_ts_ms:
        raise ValueError("waktu pulih tidak boleh lebih awal dari waktu mati")
    return (recovered_ts_ms - down_ts_ms) / 1000.0


def make_result_row(
    *,
    run_id: str,
    experiment: str,
    metric: str,
    value: float,
    unit: str,
    n_nodes: int = 0,
    mode: str = "",
    difficulty: int = 0,
    trial: int = 0,
) -> dict[str, Any]:
    """Baris CSV sesuai format Bagian 9.2."""
    return {
        "run_id": run_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "experiment": experiment,
        "n_nodes": n_nodes,
        "mode": mode,
        "difficulty": difficulty,
        "trial": trial,
        "metric": metric,
        "value": value,
        "unit": unit,
    }


def append_result(path: Path, row: dict[str, Any]) -> None:
    """Tambahkan satu baris hasil; header ditulis hanya saat file masih baru."""
    missing = [field for field in RESULT_FIELDS if field not in row]
    if missing:
        raise ValueError(f"baris hasil kehilangan kolom: {', '.join(missing)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerow({field: row[field] for field in RESULT_FIELDS})


def read_results(path: Path) -> list[dict[str, str]]:
    """Baca kembali CSV hasil eksperimen."""
    with path.open(newline="", encoding="utf-8") as handle:
        return [row for row in csv.DictReader(handle)]
