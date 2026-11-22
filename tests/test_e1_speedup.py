"""Unit tests for E1 speedup and efficiency experiment harness."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EXPERIMENTS_DIR = Path(__file__).resolve().parents[1] / "experiments"
if str(EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENTS_DIR))

import e1_speedup  # noqa: E402
from metrics import RESULT_FIELDS, read_results  # noqa: E402


def test_parse_worker_counts_sorts_deduplicates_and_requires_baseline() -> None:
    assert e1_speedup.parse_worker_counts("4,1,2,2") == (1, 2, 4)
    with pytest.raises(ValueError):
        e1_speedup.parse_worker_counts("2,4")
    with pytest.raises(ValueError):
        e1_speedup.parse_worker_counts("0,1")
    with pytest.raises(ValueError):
        e1_speedup.parse_worker_counts("abc")


def test_generate_block_transactions_is_deterministic_for_fixed_seed() -> None:
    txs_a = e1_speedup.generate_block_transactions(seed=42, trial=1, block_index=1, tx_count=5)
    txs_b = e1_speedup.generate_block_transactions(seed=42, trial=1, block_index=1, tx_count=5)
    txs_other = e1_speedup.generate_block_transactions(seed=42, trial=1, block_index=2, tx_count=5)

    assert txs_a == txs_b
    assert len(txs_a) == 5
    assert txs_a != txs_other


def test_dry_run_exits_zero_without_writing_csv(tmp_path: Path) -> None:
    code = e1_speedup.main(["--dry-run", "--output-dir", str(tmp_path)])
    assert code == 0
    assert not (tmp_path / "e1_speedup.csv").exists()


def test_invalid_cli_arguments_return_code_two(tmp_path: Path) -> None:
    assert e1_speedup.main(["--workers", "2,4", "--dry-run"]) == 2
    assert e1_speedup.main(["--trials", "0", "--dry-run"]) == 2
    assert e1_speedup.main(["--blocks", "0", "--dry-run"]) == 2
    assert e1_speedup.main(["--difficulty", "0", "--dry-run", "--output-dir", str(tmp_path)]) == 2


def test_run_experiment_executes_real_mining_and_writes_valid_csv(tmp_path: Path) -> None:
    code = e1_speedup.main(
        [
            "--workers",
            "1,2",
            "--blocks",
            "2",
            "--trials",
            "1",
            "--difficulty",
            "1",
            "--nonce-space",
            "100000",
            "--tx-per-block",
            "2",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert code == 0

    csv_path = tmp_path / "e1_speedup.csv"
    assert csv_path.is_file()
    rows = read_results(csv_path)
    # 2 worker configs x 1 trial x 4 metrics = 8 rows
    assert len(rows) == 8
    assert set(rows[0].keys()) == set(RESULT_FIELDS)

    metrics_by_worker = {(int(r["n_nodes"]), r["metric"]): float(r["value"]) for r in rows}
    assert metrics_by_worker[(1, "speedup")] == 1.0
    assert metrics_by_worker[(1, "efficiency")] == 1.0
    assert metrics_by_worker[(1, "mining_time_s")] > 0.0
    assert metrics_by_worker[(2, "mining_time_s")] > 0.0
    assert metrics_by_worker[(2, "speedup")] > 0.0
    assert metrics_by_worker[(2, "efficiency")] == pytest.approx(
        metrics_by_worker[(2, "speedup")] / 2.0
    )
    assert metrics_by_worker[(2, "throughput_bps")] > 0.0
