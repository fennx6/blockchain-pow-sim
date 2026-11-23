"""Tests for E4 scalability helpers (no Docker required)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EXPERIMENTS_DIR = Path(__file__).resolve().parents[1] / "experiments"
if str(EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENTS_DIR))

from e2_propagation import LogEvent  # noqa: E402
from e4_scalability import (  # noqa: E402
    blocks_in_window,
    efficiency_value,
    main,
    propagation_delays,
    window_events,
)


def event(ts_ms: int, node_id: str, name: str, **fields: object) -> LogEvent:
    return LogEvent(ts_ms=ts_ms, node_id=node_id, event=name, fields=dict(fields))


def test_window_events_includes_both_edges() -> None:
    events = [
        event(999, "node-1", "block_mined", index=1, block_hash="a"),
        event(1_000, "node-1", "block_mined", index=2, block_hash="b"),
        event(2_000, "node-2", "block_received", index=2, block_hash="b"),
        event(2_001, "node-2", "block_received", index=2, block_hash="b"),
    ]
    kept = window_events(events, 1_000, 2_000)
    assert [item.ts_ms for item in kept] == [1_000, 2_000]


def test_blocks_in_window_sums_transactions() -> None:
    events = [
        event(1_000, "node-1", "block_mined", index=1, block_hash="a", tx_count=3),
        event(1_500, "node-2", "block_mined", index=2, block_hash="b", tx_count=2),
        event(1_600, "node-1", "block_received", index=2, block_hash="b"),
        event(3_000, "node-1", "block_mined", index=3, block_hash="c", tx_count=9),
    ]
    assert blocks_in_window(events, 1_000, 2_000) == (2, 5)


def test_propagation_delays_use_mined_to_received() -> None:
    events = [
        event(1_000, "node-1", "block_mined", index=1, block_hash="h1"),
        event(1_150, "node-2", "block_received", index=1, block_hash="h1"),
        event(1_400, "node-3", "block_received", index=1, block_hash="h1"),
    ]
    assert propagation_delays(events, ("node-1", "node-2", "node-3")) == (150.0, 400.0)


def test_efficiency_value_uses_throughput_ratio() -> None:
    assert efficiency_value(10.0, 20.0, 2) == pytest.approx(1.0)
    assert efficiency_value(10.0, 10.0, 4) == pytest.approx(0.25)
    assert efficiency_value(0.0, 10.0, 2) is None
    assert efficiency_value(10.0, 0.0, 2) is None


def test_dry_run_prints_plan() -> None:
    assert main(["--dry-run"]) == 0
    assert main(["--dry-run", "--trials", "0"]) == 2
    assert main(["--dry-run", "--duration", "0"]) == 2
    assert main(["--dry-run", "--tx-rate", "0"]) == 2
    assert main(["--dry-run", "--grace", "-1"]) == 2
