"""Pengumpul metrik dari endpoint status node."""

from __future__ import annotations

from typing import Any

from chainsim.coordinator.metrics_collector import MetricsCollector
from chainsim.coordinator.registry import NodeRecord, NodeRegistry


def build_collector(nodes: list[NodeRecord], fetcher: Any) -> MetricsCollector:
    return MetricsCollector(NodeRegistry(nodes), interval_s=1.0, timeout_s=1.0, fetcher=fetcher)


def healthy_fetcher(record: NodeRecord) -> dict[str, Any]:
    return {
        "node_id": record.node_id,
        "height": 3,
        "tip_hash": "abc123",
        "mempool_size": 2,
        "hashrate": 12.5,
        "peers": ["node-2"],
        "state": "up",
    }


def test_refresh_once_stores_snapshot(two_nodes: list[NodeRecord]) -> None:
    collector = build_collector(two_nodes, healthy_fetcher)
    snapshot = collector.refresh_once()
    assert set(snapshot) == {"node-1", "node-2"}
    assert snapshot["node-1"]["height"] == 3
    assert snapshot["node-1"]["collected_ms"] > 0


def test_unreachable_node_is_marked_down(two_nodes: list[NodeRecord]) -> None:
    def fetcher(record: NodeRecord) -> dict[str, Any]:
        if record.node_id == "node-2":
            raise OSError("koneksi ditolak")
        return healthy_fetcher(record)

    collector = build_collector(two_nodes, fetcher)
    snapshot = collector.refresh_once()
    assert snapshot["node-2"]["state"] == "down"
    assert "error" in snapshot["node-2"]
    assert "height" not in snapshot["node-2"]


def test_prometheus_output_skips_missing_metrics(two_nodes: list[NodeRecord]) -> None:
    def fetcher(record: NodeRecord) -> dict[str, Any]:
        if record.node_id == "node-2":
            raise OSError("mati")
        return healthy_fetcher(record)

    collector = build_collector(two_nodes, fetcher)
    collector.refresh_once()
    text = collector.prometheus()

    assert 'chainsim_node_up{node_id="node-1"} 1' in text
    assert 'chainsim_node_up{node_id="node-2"} 0' in text
    assert 'chainsim_chain_height{node_id="node-1"} 3' in text
    assert 'chainsim_chain_height{node_id="node-2"}' not in text
    assert "# TYPE chainsim_hashrate gauge" in text


def test_prometheus_is_empty_without_snapshot(two_nodes: list[NodeRecord]) -> None:
    collector = build_collector(two_nodes, healthy_fetcher)
    text = collector.prometheus()
    assert "chainsim_node_up{" not in text
    assert text.endswith("\n")
