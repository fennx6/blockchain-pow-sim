"""Fungsi metrik eksperimen."""

from __future__ import annotations

from pathlib import Path

import pytest
from experiments.metrics import (
    RESULT_FIELDS,
    append_result,
    efficiency,
    latency_stats,
    make_result_row,
    percentile,
    read_results,
    recovery_time_s,
    speedup,
    throughput,
)


def test_speedup_and_efficiency() -> None:
    assert speedup(10.0, 5.0) == pytest.approx(2.0)
    assert efficiency(2.0, 4) == pytest.approx(0.5)


@pytest.mark.parametrize(("sequential", "parallel"), [(0, 1), (1, 0), (-1, 1), (1, -1)])
def test_speedup_rejects_non_positive(sequential: float, parallel: float) -> None:
    with pytest.raises(ValueError):
        speedup(sequential, parallel)


def test_efficiency_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError):
        efficiency(2.0, 0)
    with pytest.raises(ValueError):
        efficiency(0.0, 2)


def test_throughput() -> None:
    assert throughput(30, 10.0) == pytest.approx(3.0)
    with pytest.raises(ValueError):
        throughput(1, 0)
    with pytest.raises(ValueError):
        throughput(-1, 1.0)


def test_percentile_nearest_rank() -> None:
    samples = [10, 20, 30, 40, 50]
    assert percentile(samples, 50) == 30
    assert percentile(samples, 95) == 50
    assert percentile([7], 100) == 7
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError):
        percentile([1, 2], 0)


def test_latency_stats_summary() -> None:
    stats = latency_stats([10, 20, 30, 40])
    assert stats["count"] == 4
    assert stats["mean"] == pytest.approx(25.0)
    assert stats["min"] == 10
    assert stats["max"] == 40
    assert stats["p50"] == 20


def test_recovery_time_in_seconds() -> None:
    assert recovery_time_s(1_000, 3_500) == pytest.approx(2.5)
    with pytest.raises(ValueError):
        recovery_time_s(3_000, 1_000)


def test_append_result_writes_header_once(tmp_path: Path) -> None:
    target = tmp_path / "hasil.csv"
    row = make_result_row(run_id="r1", experiment="e1", metric="speedup", value=2.5, unit="x")
    append_result(target, row)
    append_result(target, row)

    rows = read_results(target)
    assert len(rows) == 2
    assert list(rows[0]) == list(RESULT_FIELDS)
    assert rows[0]["metric"] == "speedup"


def test_append_result_rejects_incomplete_row(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="kolom"):
        append_result(tmp_path / "x.csv", {"run_id": "r1"})
