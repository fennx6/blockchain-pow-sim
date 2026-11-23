"""Tests for the coordinator gRPC service and pool job dispatch."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chainsim.coordinator.grpc_server import (
    CoordinatorService,
    build_server,
    grpc_port_from_env,
)
from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager, WorkResult
from chainsim.core.block import build_block_template
from chainsim.node.grpc_client import CoordinatorClient, PeerCallResult
from chainsim.types import NodeStatus, WorkAssignment

GENESIS_HASH = "0" * 64


def _registry() -> NodeRegistry:
    return NodeRegistry(
        [
            NodeRecord(f"node-{index}", f"http://node-{index}:8081", f"node-{index}:50051")
            for index in (1, 2, 3)
        ]
    )


def _template():
    return build_block_template(
        index=1,
        prev_hash=GENESIS_HASH,
        transactions=[],
        difficulty=1,
        miner_id="node-1",
        timestamp_ms=1_700_000_000_000,
    )


class FakeNodeClient:
    """Stand-in for ``NodeClient`` that records dispatch calls instead of dialing."""

    def __init__(self, address: str, calls: list[tuple[str, str, object]], *, fail: bool) -> None:
        self.address = address
        self._calls = calls
        self._fail = fail

    def __enter__(self) -> FakeNodeClient:
        return self

    def __exit__(self, *_: object) -> bool:
        return False

    def start_work(self, assignment: WorkAssignment) -> PeerCallResult:
        self._calls.append(("start", self.address, assignment))
        if self._fail:
            raise OSError("worker tidak terjangkau")
        return PeerCallResult(self.address, True, "assigned")

    def stop_work(self, job_id: str) -> PeerCallResult:
        self._calls.append(("stop", self.address, job_id))
        if self._fail:
            raise OSError("worker tidak terjangkau")
        return PeerCallResult(self.address, True, "stopped")


def _factory(calls: list[tuple[str, str, object]], *, fail: bool = False):
    return lambda address: FakeNodeClient(address, calls, fail=fail)


def _start(registry: NodeRegistry, jobs: PoolJobManager, factory, log_dir_env: Path):
    service = CoordinatorService(
        registry, jobs, timeout_s=1.0, client_factory=factory
    )
    server, port = build_server(service, host="127.0.0.1", port=0)
    server.start()
    return server, port, service


def _events(directory: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in (directory / "coordinator.jsonl").read_text(encoding="utf-8").splitlines()
    ]


def test_register_returns_peer_list_and_logs(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            peers = client.register(
                NodeRecord("node-9", "http://node-9:8081", "node-9:50051")
            )
    finally:
        server.stop(0)

    assert [record.node_id for record in peers] == ["node-1", "node-2", "node-3", "node-9"]
    assert registry.get("node-9") is not None
    assert _events(log_dir_env)[-1]["event"] == "node_registered"


def test_submit_template_dispatches_every_worker(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.submit_template("node-1", _template())
    finally:
        server.stop(0)

    assert result.accepted is True
    assert result.job_id == "job-1-1"
    assert result.message == ""
    starts = [(address, item) for kind, address, item in calls if kind == "start"]
    assert len(starts) == 3
    ranges = sorted(
        (assignment.nonce_start, assignment.nonce_end) for _, assignment in starts
    )
    assert ranges == [(0, 300), (300, 600), (600, 900)]


def test_submit_template_rejects_unknown_node(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.submit_template("node-99", _template())
    finally:
        server.stop(0)

    assert result.accepted is False
    assert "tidak terdaftar" in result.message
    assert calls == []


def test_submit_template_closes_job_when_no_worker_accepts(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls, fail=True), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            result = client.submit_template("node-1", _template())
    finally:
        server.stop(0)

    assert result.accepted is False
    assert "tidak terjangkau" in result.message
    job = jobs.get("job-1-1")
    assert job.finished is True
    event_names = [event["event"] for event in _events(log_dir_env)]
    assert "job_dispatch_failed" in event_names
    assert event_names[-1] == "job_finished"


def test_work_result_stops_losing_workers(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            started = client.submit_template("node-1", _template())
            ack = client.submit_work_result(
                WorkResult(
                    job_id=started.job_id,
                    node_id="node-2",
                    nonce=350,
                    found=True,
                    block_hash="a" * 64,
                )
            )
    finally:
        server.stop(0)

    assert ack.ok is True
    stops = sorted(address for kind, address, _ in calls if kind == "stop")
    assert stops == ["node-1:50051", "node-3:50051"]
    assert jobs.get("job-1-1").winner_node == "node-2"


def test_work_result_outside_range_is_rejected(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            started = client.submit_template("node-1", _template())
            ack = client.submit_work_result(
                WorkResult(
                    job_id=started.job_id,
                    node_id="node-2",
                    nonce=10,
                    found=True,
                    block_hash="b" * 64,
                )
            )
    finally:
        server.stop(0)

    assert ack.ok is False
    assert "di luar rentang" in ack.message
    assert jobs.get("job-1-1").winner_node is None


def test_heartbeat_accepts_registered_node_only(log_dir_env: Path) -> None:
    calls: list[tuple[str, str, object]] = []
    registry = _registry()
    jobs = PoolJobManager(registry, nonce_space=900)
    server, port, _ = _start(registry, jobs, _factory(calls), log_dir_env)
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            known = client.heartbeat(_status("node-1"))
            unknown = client.heartbeat(_status("node-9"))
    finally:
        server.stop(0)

    assert known[0] is True
    assert [record.node_id for record in known[1]] == ["node-1", "node-2", "node-3"]
    assert unknown[0] is False


def _status(node_id: str) -> NodeStatus:
    return NodeStatus(
        node_id=node_id,
        height=0,
        tip_hash=GENESIS_HASH,
        peers=[],
        mempool_size=0,
        hashrate=0.0,
        state="idle",
    )


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, 50052),
        ({"COORDINATOR_GRPC_PORT": "50053"}, 50053),
    ],
)
def test_grpc_port_from_env_defaults(env: dict[str, str], expected: int) -> None:
    assert grpc_port_from_env(env) == expected


@pytest.mark.parametrize("raw", ["abc", "0", "70000"])
def test_grpc_port_from_env_rejects_invalid(raw: str) -> None:
    with pytest.raises(ValueError):
        grpc_port_from_env({"COORDINATOR_GRPC_PORT": raw})
