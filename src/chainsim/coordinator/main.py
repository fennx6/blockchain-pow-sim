"""Entrypoint container coordinator: gRPC, heartbeat, metrik, dan dashboard Flask."""

from __future__ import annotations

import logging

from chainsim.config import Settings, load_settings
from chainsim.coordinator.dashboard import create_app
from chainsim.coordinator.grpc_server import (
    CoordinatorService,
    build_server,
    grpc_port_from_env,
)
from chainsim.coordinator.heartbeat import HeartbeatMonitor
from chainsim.coordinator.metrics_collector import MetricsCollector
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager
from chainsim.logging_utils import log_event

logger = logging.getLogger(__name__)


def build_runtime(
    settings: Settings,
) -> tuple[NodeRegistry, HeartbeatMonitor, MetricsCollector, PoolJobManager, CoordinatorService]:
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
    jobs = PoolJobManager(registry, settings.blockchain.nonce_space)
    service = CoordinatorService(
        registry,
        jobs,
        timeout_s=settings.network.grpc_timeout_s,
    )
    return registry, monitor, collector, jobs, service


def make_down_handler(jobs: PoolJobManager, service: CoordinatorService):
    """Callback ``peer_down``: bagikan ulang rentang worker mati ke worker hidup."""

    def on_down(record: NodeRecord) -> None:
        logger.warning("node %s terdeteksi mati", record.node_id)
        job = jobs.active_job()
        if job is None or job.finished:
            return
        try:
            grouped = jobs.reassign_grouped(job.job_id, record.node_id)
        except ValueError:
            logger.exception("reassign job %s gagal untuk %s", job.job_id, record.node_id)
            return
        if not grouped:
            return
        failures = service.dispatch(job.job_id, grouped)
        if failures:
            logger.warning(
                "sebagian assignment job %s tidak terkirim: %s",
                job.job_id,
                "; ".join(failures),
            )

    return on_down


def main() -> int:
    settings = load_settings()
    registry, monitor, collector, jobs, service = build_runtime(settings)

    def on_up(record: NodeRecord) -> None:
        logger.info("node %s terdeteksi hidup", record.node_id)

    monitor.on_down(make_down_handler(jobs, service))
    monitor.on_up(on_up)

    grpc_port = grpc_port_from_env()
    server, bound_port = build_server(
        service,
        host=settings.coordinator.host,
        port=grpc_port,
    )

    log_event(
        "coordinator_start",
        "coordinator",
        nodes=registry.node_ids(),
        heartbeat_interval_s=settings.network.heartbeat_interval_s,
        grpc_port=bound_port,
    )

    server.start()
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
        server.stop(grace=1.0)
        log_event("coordinator_stop", "coordinator")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
