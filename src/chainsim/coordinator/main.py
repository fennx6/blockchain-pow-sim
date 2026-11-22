"""Entrypoint container coordinator: heartbeat + metrik + dashboard Flask."""

from __future__ import annotations

import logging

from chainsim.config import Settings, load_settings
from chainsim.coordinator.dashboard import create_app
from chainsim.coordinator.heartbeat import HeartbeatMonitor
from chainsim.coordinator.metrics_collector import MetricsCollector
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.logging_utils import log_event

logger = logging.getLogger(__name__)


def build_runtime(settings: Settings) -> tuple[NodeRegistry, HeartbeatMonitor, MetricsCollector]:
    """Rakit komponen coordinator dari konfigurasi."""
    if not settings.coordinator.nodes:
        logger.warning("NODES kosong: dashboard tidak akan menampilkan node mana pun")
    registry = NodeRegistry.from_config(settings.coordinator.nodes)
    monitor = HeartbeatMonitor(
        registry,
        interval_s=settings.network.heartbeat_interval_s,
        timeout_s=settings.network.heartbeat_timeout_s,
    )
    collector = MetricsCollector(
        registry,
        interval_s=settings.network.heartbeat_interval_s,
        timeout_s=settings.network.heartbeat_timeout_s,
    )
    return registry, monitor, collector


def main() -> int:
    settings = load_settings()
    registry, monitor, collector = build_runtime(settings)

    def on_down(record: NodeRecord) -> None:
        logger.warning("node %s terdeteksi mati", record.node_id)

    def on_up(record: NodeRecord) -> None:
        logger.info("node %s terdeteksi hidup", record.node_id)

    monitor.on_down(on_down)
    monitor.on_up(on_up)

    log_event(
        "coordinator_start",
        "coordinator",
        nodes=registry.node_ids(),
        heartbeat_interval_s=settings.network.heartbeat_interval_s,
    )

    monitor.start()
    collector.start()
    app = create_app(registry, monitor, collector)
    try:
        # debug selalu False: server ini berjalan di dalam container (Bagian 10).
        app.run(
            host=settings.coordinator.host,
            port=settings.coordinator.port,
            debug=False,
            threaded=True,
            use_reloader=False,
        )
    except KeyboardInterrupt:
        logger.info("coordinator dihentikan lewat keyboard")
    finally:
        monitor.stop()
        collector.stop()
        log_event("coordinator_stop", "coordinator")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
