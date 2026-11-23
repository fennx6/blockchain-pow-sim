"""Tests for coordinator runtime wiring (no Docker, no network)."""

from __future__ import annotations

from pathlib import Path

from chainsim.config import load_settings
from chainsim.coordinator.grpc_server import CoordinatorService
from chainsim.coordinator.main import build_runtime, make_down_handler
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager
from chainsim.types import Block


def make_block(index: int = 1) -> Block:
    return Block(
        index=index,
        prev_hash="0" * 64,
        timestamp_ms=1_700_000_000_000,
        transactions=[],
        merkle_root="root",
        difficulty=2,
        nonce=0,
        miner_id="node-1",
    )


def test_build_runtime_wires_registry_and_pool_jobs(log_dir_env: Path) -> None:
    settings = load_settings(
        env={
            "NODES": "node-1=node-1:8081,node-2=node-2:8081",
            "NONCE_SPACE": "100",
            "LOG_DIR": str(log_dir_env),
        }
    )

    registry, monitor, collector, jobs, service = build_runtime(settings)

    assert registry.node_ids() == ["node-1", "node-2"]
    assert jobs.nonce_space == 100
    assert isinstance(service, CoordinatorService)
    assert monitor is not None and collector is not None


class RecordingService:
    """Stand-in that records dispatch calls instead of dialing workers."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, list]]] = []

    def dispatch(self, job_id: str, grouped: dict[str, list]) -> list[str]:
        self.calls.append((job_id, grouped))
        return []


def test_down_handler_reassigns_dead_worker_span(log_dir_env: Path) -> None:
    registry = NodeRegistry(
        [
            NodeRecord(f"node-{index}", f"http://node-{index}:8081", f"node-{index}:50051")
            for index in (1, 2, 3)
        ]
    )
    jobs = PoolJobManager(registry, nonce_space=300)
    job = jobs.start_job(make_block())
    service = RecordingService()

    handler = make_down_handler(jobs, service)
    handler(NodeRecord("node-2", "http://node-2:8081", "node-2:50051"))

    assert job.workers() == ["node-1", "node-3"]
    assert len(service.calls) == 1
    job_id, grouped = service.calls[0]
    assert job_id == job.job_id
    assert sorted(grouped) == ["node-1", "node-3"]
    assert [(item.nonce_start, item.nonce_end) for item in grouped["node-1"]] == [(100, 150)]
    assert [(item.nonce_start, item.nonce_end) for item in grouped["node-3"]] == [(150, 200)]


def test_down_handler_ignores_finished_or_absent_job(log_dir_env: Path) -> None:
    registry = NodeRegistry(
        [NodeRecord("node-1", "http://node-1:8081"), NodeRecord("node-2", "http://node-2:8081")]
    )
    jobs = PoolJobManager(registry, nonce_space=100)
    service = RecordingService()
    handler = make_down_handler(jobs, service)

    handler(NodeRecord("node-2", "http://node-2:8081"))
    assert service.calls == []

    job = jobs.start_job(make_block())
    jobs.record_result(job.job_id, "node-1", nonce=1, block_hash="c" * 64)
    handler(NodeRecord("node-2", "http://node-2:8081"))
    assert service.calls == []
