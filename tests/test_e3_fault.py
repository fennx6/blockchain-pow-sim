"""Tests for E3 metric extraction (no Docker required)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EXPERIMENTS_DIR = Path(__file__).resolve().parents[1] / "experiments"
if str(EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENTS_DIR))

from e2_propagation import LogEvent  # noqa: E402
from e3_fault import (  # noqa: E402
    active_miners_after,
    detection_time_s,
    fork_metrics,
    job_completed_after,
    main,
    mined_after,
    parse_scenarios,
    reassignment_time_s,
    sync_metrics,
    time_to_consistent_height,
)


def event(ts_ms: int, node_id: str, name: str, **fields: object) -> LogEvent:
    return LogEvent(ts_ms=ts_ms, node_id=node_id, event=name, fields=dict(fields))


def test_detection_time_uses_first_peer_down_for_target() -> None:
    events = [
        event(4_100, "coordinator", "peer_down", target="node-2"),
        event(5_000, "coordinator", "peer_down", target="node-3"),
        event(6_000, "coordinator", "peer_down", target="node-3"),
    ]
    assert detection_time_s(events, "node-3", 4_000) == pytest.approx(1.0)
    assert detection_time_s(events, "node-9", 4_000) is None


def test_detection_ignores_events_before_injection() -> None:
    events = [event(3_000, "coordinator", "peer_down", target="node-3")]
    assert detection_time_s(events, "node-3", 4_000) is None


def test_reassignment_time_and_completion() -> None:
    events = [
        event(5_000, "coordinator", "job_reassigned", job_id="job-1-1", dead_node="node-3"),
        event(7_500, "node-1", "block_mined", job_id="job-1-1", index=2),
    ]
    assert reassignment_time_s(events, "job-1-1", 4_000) == pytest.approx(1.0)
    assert reassignment_time_s(events, "job-9-9", 4_000) is None
    assert job_completed_after(events, "job-1-1", 4_000) is True
    assert job_completed_after(events, "job-1-1", 8_000) is False


def test_sync_metrics_returns_time_and_blocks() -> None:
    events = [
        event(5_000, "node-3", "sync_done", blocks_pulled=4, from_height=1, to_height=5),
    ]
    assert sync_metrics(events, "node-3", 4_000) == (1.0, 4)
    assert sync_metrics(events, "node-4", 4_000) is None


def test_fork_metrics_sums_discarded_blocks() -> None:
    events = [
        event(4_500, "node-1", "fork_switched", blocks_discarded=2),
        event(5_500, "node-2", "fork_switched", blocks_discarded=3),
        event(3_500, "node-3", "fork_switched", blocks_discarded=99),
    ]
    assert fork_metrics(events, 4_000) == (2, 5)


def test_time_to_consistent_height_waits_for_all_nodes() -> None:
    events = [
        event(5_000, "node-1", "block_mined", index=4),
        event(5_100, "node-2", "block_received", index=3),
        event(5_200, "node-2", "block_received", index=4),
    ]
    assert time_to_consistent_height(events, ("node-1", "node-2"), 4_000) == pytest.approx(1.2)
    assert time_to_consistent_height(events, ("node-1", "node-2", "node-3"), 4_000) is None


def test_mined_and_active_miners_filter_by_time() -> None:
    events = [
        event(3_000, "node-1", "block_mined", index=1),
        event(5_000, "node-1", "block_mined", index=2),
        event(5_100, "node-2", "block_mined", index=2),
    ]
    assert mined_after(events, 4_000) == 2
    assert active_miners_after(events, 4_000) == {"node-1", "node-2"}


def test_parse_scenarios_accepts_known_ids_only() -> None:
    assert parse_scenarios("f1, F2") == ("F1", "F2")
    with pytest.raises(ValueError, match="tidak dikenal"):
        parse_scenarios("F1,F9")
    with pytest.raises(ValueError, match="kosong"):
        parse_scenarios(" , ")


def test_dry_run_prints_plan() -> None:
    assert main(["--dry-run", "--scenarios", "F1,F2,F3"]) == 0
    assert main(["--dry-run", "--trials", "0"]) == 2
