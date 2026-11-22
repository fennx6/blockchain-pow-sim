"""Aplikasi Flask dashboard monitoring (kontrak Bagian 6.4).

Hanya menampilkan data yang benar-benar dibaca dari node. Node yang tidak
terjangkau ditandai ``down`` dan metriknya tidak diisi angka pengganti.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, render_template, request

from chainsim.coordinator.heartbeat import HeartbeatMonitor
from chainsim.coordinator.metrics_collector import MetricsCollector, now_ms
from chainsim.coordinator.registry import NodeRegistry
from chainsim.logging_utils import log_dir, read_recent_events

MAX_EVENT_LIMIT = 500


def _node_rows(
    registry: NodeRegistry,
    monitor: HeartbeatMonitor,
    collector: MetricsCollector,
) -> list[dict[str, Any]]:
    states = monitor.state()
    metrics = collector.latest()
    rows: list[dict[str, Any]] = []
    for record in registry.all():
        payload = metrics.get(record.node_id, {})
        state = states.get(record.node_id) or payload.get("state") or "unknown"
        rows.append(
            {
                "node_id": record.node_id,
                "state": state,
                "height": payload.get("height"),
                "tip_hash": payload.get("tip_hash"),
                "mempool_size": payload.get("mempool_size"),
                "hashrate": payload.get("hashrate"),
                "peers": payload.get("peers", list(record.peers)),
                "collected_ms": payload.get("collected_ms"),
                "error": payload.get("error"),
            }
        )
    return rows


def create_app(
    registry: NodeRegistry,
    monitor: HeartbeatMonitor,
    collector: MetricsCollector,
    refresh_ms: int = 3000,
    events_dir: Path | None = None,
) -> Flask:
    """Bangun aplikasi Flask; dipisah dari ``main`` agar bisa diuji tanpa server."""
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            refresh_ms=refresh_ms,
            node_count=len(registry.all()),
        )

    @app.get("/api/nodes")
    def api_nodes() -> Response:
        return jsonify(
            {
                "generated_ms": now_ms(),
                "nodes": _node_rows(registry, monitor, collector),
            }
        )

    @app.get("/api/events")
    def api_events() -> Response:
        limit = request.args.get("limit", default=50, type=int)
        limit = max(1, min(limit, MAX_EVENT_LIMIT))
        directory = events_dir if events_dir is not None else log_dir()
        return jsonify({"events": read_recent_events(limit=limit, directory=directory)})

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(
            collector.prometheus(),
            content_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.after_request
    def no_store(response: Response) -> Response:
        # Dashboard selalu menampilkan kondisi terkini; cache browser hanya membingungkan.
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    return app
