"""Endpoint dashboard coordinator."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from flask.testing import FlaskClient

from chainsim.coordinator.dashboard import create_app
from chainsim.coordinator.heartbeat import HeartbeatMonitor
from chainsim.coordinator.metrics_collector import MetricsCollector
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.logging_utils import log_event


def build_client(
    nodes: list[NodeRecord],
    log_dir: Path,
    *,
    healthy: bool = True,
    events_dir: Path | None = None,
) -> FlaskClient:
    registry = NodeRegistry(nodes)

    def fetcher(record: NodeRecord) -> dict[str, Any]:
        return {
            "node_id": record.node_id,
            "height": 5,
            "tip_hash": "deadbeef" * 8,
            "mempool_size": 1,
            "hashrate": 42.0,
            "peers": ["node-2"],
            "state": "up",
        }

    monitor = HeartbeatMonitor(registry, 1.0, 1.0, probe=lambda record: healthy)
    collector = MetricsCollector(registry, 1.0, 1.0, fetcher=fetcher)
    collector.refresh_once()
    monitor.poll_once()
    app = create_app(
        registry,
        monitor,
        collector,
        refresh_ms=1500,
        events_dir=events_dir if events_dir is not None else log_dir,
    )
    app.config["TESTING"] = True
    return app.test_client()


def test_index_renders_dashboard(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    client = build_client(two_nodes, log_dir_env)
    response = client.get("/")
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "chainsim monitoring" in body
    assert "2 node terdaftar" in body


def test_api_nodes_returns_status_rows(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    client = build_client(two_nodes, log_dir_env)
    payload = client.get("/api/nodes").get_json()
    assert payload["generated_ms"] > 0
    rows = {row["node_id"]: row for row in payload["nodes"]}
    assert rows["node-1"]["height"] == 5
    assert rows["node-1"]["hashrate"] == 42.0
    assert rows["node-1"]["state"] in {"up", "unknown"}


def test_api_nodes_marks_down_when_probe_fails(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    client = build_client(two_nodes, log_dir_env, healthy=False)
    payload = client.get("/api/nodes").get_json()
    assert all(row["state"] == "down" for row in payload["nodes"])


def test_api_events_returns_logged_events(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    log_event("block_mined", "node-1", height=1)
    client = build_client(two_nodes, log_dir_env)
    payload = client.get("/api/events?limit=10").get_json()
    assert "block_mined" in [event["event"] for event in payload["events"]]


def test_api_events_empty_state(
    two_nodes: list[NodeRecord], log_dir_env: Path, tmp_path: Path
) -> None:
    client = build_client(two_nodes, log_dir_env, events_dir=tmp_path / "belum-ada")
    assert client.get("/api/events").get_json() == {"events": []}


def test_tx_proxy_forwards_to_first_node(
    two_nodes: list[NodeRecord], log_dir_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = build_client(two_nodes, log_dir_env)

    class FakeResponse:
        status = 201

        def __init__(self) -> None:
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return b'{"ok": true}'

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *exc: object) -> bool:
            return False

    monkeypatch.setattr(
        "chainsim.coordinator.dashboard.app.urllib.request.urlopen",
        lambda request, timeout=None: FakeResponse(),
    )
    response = client.post("/tx", json={"sender": "a", "receiver": "b", "amount": 1})
    assert response.status_code == 201
    assert response.get_json() == {"ok": True}


def test_tx_proxy_rejects_non_json_body(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    client = build_client(two_nodes, log_dir_env)
    response = client.post("/tx", data="not json", content_type="text/plain")
    assert response.status_code == 400
