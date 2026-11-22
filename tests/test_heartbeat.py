"""Deteksi kegagalan node oleh coordinator."""

from __future__ import annotations

from pathlib import Path

import pytest

from chainsim.coordinator.heartbeat import HeartbeatMonitor
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.logging_utils import read_recent_events


def build_monitor(nodes: list[NodeRecord], healthy: dict[str, bool]) -> HeartbeatMonitor:
    registry = NodeRegistry(nodes)
    return HeartbeatMonitor(
        registry,
        interval_s=1.0,
        timeout_s=1.0,
        probe=lambda record: healthy[record.node_id],
    )


def test_poll_once_reports_transitions(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    healthy = {"node-1": True, "node-2": True}
    monitor = build_monitor(two_nodes, healthy)
    ups: list[NodeRecord] = []
    downs: list[NodeRecord] = []
    monitor.on_up(ups.append)
    monitor.on_down(downs.append)

    monitor.poll_once()
    assert monitor.state() == {"node-1": "up", "node-2": "up"}
    assert [record.node_id for record in ups] == ["node-1", "node-2"]
    assert downs == []

    healthy["node-2"] = False
    monitor.poll_once()
    assert monitor.state()["node-2"] == "down"
    assert [record.node_id for record in downs] == ["node-2"]

    healthy["node-2"] = True
    monitor.poll_once()
    assert [record.node_id for record in ups] == ["node-1", "node-2", "node-2"]


def test_callback_only_fires_on_change(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    monitor = build_monitor(two_nodes, {"node-1": True, "node-2": False})
    calls: list[str] = []
    monitor.on_down(lambda record: calls.append(record.node_id))
    monitor.poll_once()
    monitor.poll_once()
    assert calls == ["node-2"]


def test_probe_exception_is_treated_as_down(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    registry = NodeRegistry(two_nodes)

    def probe(record: NodeRecord) -> bool:
        raise OSError("koneksi ditolak")

    monitor = HeartbeatMonitor(registry, interval_s=1.0, timeout_s=1.0, probe=probe)
    assert monitor.poll_once() == {"node-1": "down", "node-2": "down"}


def test_events_are_written_to_jsonl(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    healthy = {"node-1": True, "node-2": True}
    monitor = build_monitor(two_nodes, healthy)
    monitor.poll_once()
    healthy["node-1"] = False
    monitor.poll_once()

    events = read_recent_events(limit=10, directory=log_dir_env)
    names = [event["event"] for event in events]
    assert "peer_down" in names
    down_event = next(event for event in events if event["event"] == "peer_down")
    assert down_event["node_id"] == "coordinator"
    assert down_event["target"] == "node-1"


def test_invalid_intervals_are_rejected(two_nodes: list[NodeRecord]) -> None:
    registry = NodeRegistry(two_nodes)
    with pytest.raises(ValueError):
        HeartbeatMonitor(registry, interval_s=0, timeout_s=1.0)
    with pytest.raises(ValueError):
        HeartbeatMonitor(registry, interval_s=1.0, timeout_s=0)


def test_start_and_stop_manage_thread(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    monitor = build_monitor(two_nodes, {"node-1": True, "node-2": True})
    monitor.start()
    monitor.stop(timeout_s=2.0)
    assert monitor.state() == {"node-1": "up", "node-2": "up"}
