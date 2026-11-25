"""Tests for E2 propagation and throughput analysis."""

from __future__ import annotations

import json
from pathlib import Path

from experiments import e2_propagation
from experiments.metrics import read_results


def _write_log(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def _logs(tmp_path: Path) -> Path:
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_log(
        logs / "node-1.jsonl",
        [
            {
                "ts_ms": 900,
                "node_id": "node-1",
                "event": "tx_forwarded",
                "tx_id": "t1",
                "to_node": "node-2",
                "ok": True,
            },
            {
                "ts_ms": 1000,
                "node_id": "node-1",
                "event": "block_mined",
                "block_hash": "b1",
                "tx_count": 2,
                "index": 1,
            },
            {
                "ts_ms": 1001,
                "node_id": "node-1",
                "event": "block_forwarded",
                "block_hash": "b1",
                "to_node": "node-2",
                "ok": True,
            },
        ],
    )
    _write_log(
        logs / "node-2.jsonl",
        [
            {
                "ts_ms": 905,
                "node_id": "node-2",
                "event": "tx_received",
                "tx_id": "t1",
                "to_node": "node-2",
                "accepted": True,
            },
            {
                "ts_ms": 1005,
                "node_id": "node-2",
                "event": "block_received",
                "block_hash": "b1",
                "accepted": True,
            },
        ],
    )
    _write_log(
        logs / "node-3.jsonl",
        [
            {
                "ts_ms": 1010,
                "node_id": "node-3",
                "event": "block_received",
                "block_hash": "b1",
                "accepted": True,
            },
        ],
    )
    (logs / "bad.jsonl").write_text("{not json}\n", encoding="utf-8")
    return logs


def test_load_events_skips_malformed_lines(tmp_path: Path) -> None:
    events = e2_propagation.load_events(_logs(tmp_path))
    assert len(events) == 6
    assert events[0].ts_ms == 900
    assert events[-1].ts_ms == 1010


def test_forwarding_delays_match_sent_and_received(tmp_path: Path) -> None:
    events = e2_propagation.load_events(_logs(tmp_path))
    assert e2_propagation.tx_forward_delays(events) == [5.0]
    assert e2_propagation.block_forward_delays(events) == [4.0]


def test_propagation_stats_cover_every_expected_node(tmp_path: Path) -> None:
    events = e2_propagation.load_events(_logs(tmp_path))
    stats = e2_propagation.propagation_stats(events, ("node-1", "node-2", "node-3"))
    assert stats.delays_ms == (5.0, 10.0)
    assert stats.full_coverage_ms == (10.0,)
    assert stats.blocks_total == 1
    assert stats.blocks_full == 1
    assert stats.coverage_ratio == 1.0


def test_single_node_run_has_trivial_coverage() -> None:
    event = e2_propagation.LogEvent(
        ts_ms=1000,
        node_id="node-1",
        event="block_mined",
        fields={"block_hash": "b1", "tx_count": 1},
    )
    stats = e2_propagation.propagation_stats([event], ("node-1",))
    assert stats.blocks_total == 1
    assert stats.delays_ms == ()
    assert stats.coverage_ratio == 1.0


def test_throughput_uses_explicit_window(tmp_path: Path) -> None:
    events = e2_propagation.load_events(_logs(tmp_path))
    stats = e2_propagation.throughput_stats(events, duration_s=10.0)
    assert stats.blocks == 1
    assert stats.txs == 2
    assert stats.duration_s == 10.0
    assert stats.block_bps == 0.1
    assert stats.tx_tps == 0.2


def test_main_writes_metrics(tmp_path: Path) -> None:
    code = e2_propagation.main(
        [
            "--logs-dir",
            str(_logs(tmp_path)),
            "--nodes",
            "node-1,node-2,node-3",
            "--duration",
            "10",
            "--output-dir",
            str(tmp_path),
            "--run-id",
            "test-run",
        ]
    )
    assert code == 0
    rows = read_results(tmp_path / "e2_propagation.csv")
    metrics = {row["metric"]: float(row["value"]) for row in rows}
    assert metrics["tx_forward_ms_mean"] == 5.0
    assert metrics["block_forward_ms_mean"] == 4.0
    assert metrics["block_propagation_ms_mean"] == 7.5
    assert metrics["block_propagation_full_coverage_ms_mean"] == 10.0
    assert metrics["block_propagation_coverage_ratio"] == 1.0
    assert metrics["block_throughput_bps"] == 0.1
    assert metrics["tx_throughput_tps"] == 0.2
    assert metrics["blocks_mined"] == 1.0
    assert {row["run_id"] for row in rows} == {"test-run"}
    assert {int(row["n_nodes"]) for row in rows} == {3}


def test_main_dry_run_does_not_write(tmp_path: Path) -> None:
    code = e2_propagation.main(
        [
            "--logs-dir",
            str(_logs(tmp_path)),
            "--nodes",
            "node-1,node-2,node-3",
            "--duration",
            "10",
            "--output-dir",
            str(tmp_path),
            "--dry-run",
        ]
    )
    assert code == 0
    assert not (tmp_path / "e2_propagation.csv").exists()


def test_main_rejects_invalid_arguments(tmp_path: Path) -> None:
    assert e2_propagation.main(["--rtt-samples", "0", "--logs-dir", str(tmp_path)]) == 2
    assert e2_propagation.main(["--duration", "-1", "--logs-dir", str(tmp_path)]) == 2


def test_main_returns_error_for_empty_logs(tmp_path: Path) -> None:
    logs = tmp_path / "empty"
    logs.mkdir()
    assert e2_propagation.main(["--logs-dir", str(logs)]) == 2


def test_forward_and_propagation_use_sent_received_ts_and_strip_ports(tmp_path: Path) -> None:
    """Post-Ack ts_ms ordering and peer:port strings must not drop E2 samples."""
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_log(
        logs / "node-1.jsonl",
        [
            {
                "ts_ms": 1005,
                "sent_ts_ms": 1000,
                "node_id": "node-1",
                "event": "tx_forwarded",
                "tx_id": "t1",
                "to_node": "node-2:50051",
                "ok": True,
            },
            {
                "ts_ms": 2000,
                "node_id": "node-1",
                "event": "block_mined",
                "block_hash": "b1",
                "tx_count": 1,
                "index": 1,
            },
            {
                "ts_ms": 2020,
                "sent_ts_ms": 2001,
                "node_id": "node-1",
                "event": "block_forwarded",
                "block_hash": "b1",
                "to_node": "node-2:50051",
                "ok": True,
            },
        ],
    )
    _write_log(
        logs / "node-2.jsonl",
        [
            {
                "ts_ms": 1004,
                "received_ts_ms": 1002,
                "node_id": "node-2",
                "event": "tx_received",
                "tx_id": "t1",
                "to_node": "node-2",
                "accepted": True,
            },
            {
                "ts_ms": 2060,
                "received_ts_ms": 2004,
                "node_id": "node-2",
                "event": "block_received",
                "block_hash": "b1",
                "to_node": "node-2",
                "accepted": True,
            },
        ],
    )

    events = e2_propagation.load_events(logs)
    assert e2_propagation.tx_forward_delays(events) == [2.0]
    assert e2_propagation.block_forward_delays(events) == [3.0]
    stats = e2_propagation.propagation_stats(events, ("node-1", "node-2"))
    assert stats.delays_ms == (4.0,)
    assert stats.full_coverage_ms == (4.0,)
