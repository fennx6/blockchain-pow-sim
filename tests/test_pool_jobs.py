"""Manajemen job mode pool: pembagian nonce, pemenang, dan reassignment."""

from __future__ import annotations

from pathlib import Path

import pytest

from chainsim.coordinator.registry import NodeRecord, NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager
from chainsim.logging_utils import read_recent_events
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
        miner_id="coordinator",
    )


def make_manager(nodes: list[NodeRecord], nonce_space: int = 900) -> PoolJobManager:
    return PoolJobManager(NodeRegistry(nodes), nonce_space=nonce_space)


@pytest.fixture
def three_nodes() -> list[NodeRecord]:
    return [NodeRecord(f"node-{index}", f"http://node-{index}:8081") for index in (1, 2, 3)]


def test_start_job_splits_whole_nonce_space(
    three_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(three_nodes)
    job = manager.start_job(make_block())

    assert job.job_id == "job-1-1"
    assert job.workers() == ["node-1", "node-2", "node-3"]
    assert job.ranges_for("node-1") == [(0, 300)]
    assert job.ranges_for("node-2") == [(300, 600)]
    assert job.ranges_for("node-3") == [(600, 900)]
    assert manager.active_job() is job

    events = read_recent_events(limit=5, directory=log_dir_env)
    assert events[0]["event"] == "job_assigned"
    assert events[0]["workers"] == ["node-1", "node-2", "node-3"]


def test_start_job_accepts_explicit_workers(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block(), worker_ids=["node-2"])
    assert job.workers() == ["node-2"]
    assert job.ranges_for("node-2") == [(0, 100)]


def test_start_job_rejects_duplicate_workers(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes)
    with pytest.raises(ValueError, match="duplikat"):
        manager.start_job(make_block(), worker_ids=["node-1", "node-1"])


def test_start_job_requires_workers(log_dir_env: Path) -> None:
    manager = PoolJobManager(NodeRegistry(), nonce_space=100)
    with pytest.raises(ValueError, match="worker"):
        manager.start_job(make_block())


def test_second_job_replaces_stale_job(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes)
    first = manager.start_job(make_block())
    with pytest.raises(RuntimeError, match="masih berjalan"):
        manager.start_job(make_block())

    second = manager.start_job(make_block(index=2))
    assert first.finished is True
    assert second.job_id == "job-2-2"
    events = read_recent_events(limit=5, directory=log_dir_env)
    assert any(
        event["event"] == "job_finished" and event["reason"] == "stale_template"
        for event in events
    )


def test_record_result_declares_winner(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    manager = make_manager(two_nodes)
    job = manager.start_job(make_block())

    result = manager.record_result(job.job_id, "node-2", nonce=600, block_hash="beef")

    assert result.found is True
    assert job.finished is True
    assert job.winner_node == "node-2"
    assert job.winner_hash == "beef"
    events = read_recent_events(limit=5, directory=log_dir_env)
    assert events[0]["event"] == "job_done"
    assert events[0]["winner"] == "node-2"


def test_record_result_rejects_nonce_outside_assignment(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block())
    with pytest.raises(ValueError, match="di luar rentang"):
        manager.record_result(job.job_id, "node-1", nonce=80)


def test_record_result_rejects_unknown_worker(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes)
    job = manager.start_job(make_block())
    with pytest.raises(ValueError, match="bukan worker"):
        manager.record_result(job.job_id, "node-9", nonce=1)


def test_record_result_after_finish_is_rejected(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes)
    job = manager.start_job(make_block())
    manager.record_result(job.job_id, "node-1", nonce=1, block_hash="aa")
    with pytest.raises(RuntimeError, match="sudah selesai"):
        manager.record_result(job.job_id, "node-2", nonce=600, block_hash="bb")


def test_exhausted_range_does_not_finish_job(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block())
    manager.record_result(job.job_id, "node-1", nonce=0, found=False)
    assert job.finished is False
    assert job.winner_node is None


def test_all_ranges_exhausted_finishes_job(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block())
    manager.record_result(job.job_id, "node-1", nonce=0, found=False)
    manager.record_result(job.job_id, "node-2", nonce=50, found=False)
    assert job.finished is True
    events = read_recent_events(limit=5, directory=log_dir_env)
    assert any(
        event["event"] == "job_finished" and event["reason"] == "all_exhausted"
        for event in events
    )


def test_empty_nonce_range_can_report_exhaustion(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=1)
    job = manager.start_job(make_block())
    assert job.ranges_for("node-2") == [(1, 1)]
    manager.record_result(job.job_id, "node-2", nonce=1, found=False)
    assert job.finished is False
    manager.record_result(job.job_id, "node-1", nonce=0, found=False)
    assert job.finished is True


def test_reassign_redistributes_dead_worker_span(
    three_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(three_nodes)
    job = manager.start_job(make_block())

    new_assignments = manager.reassign(job.job_id, "node-2")

    assert job.workers() == ["node-1", "node-3"]
    assert job.ranges_for("node-1") == [(0, 300), (300, 450)]
    assert job.ranges_for("node-3") == [(600, 900), (450, 600)]
    assert sorted((item.nonce_start, item.nonce_end) for item in new_assignments) == [
        (300, 450),
        (450, 600),
    ]
    covered = sum(high - low for node in job.workers() for low, high in job.ranges_for(node))
    assert covered == 900

    events = read_recent_events(limit=5, directory=log_dir_env)
    assert events[0]["event"] == "job_reassigned"
    assert events[0]["dead_node"] == "node-2"
    assert events[0]["lost_span"] == 300


def test_reassigned_range_can_produce_result(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block())
    manager.reassign(job.job_id, "node-2")

    result = manager.record_result(job.job_id, "node-1", nonce=75, block_hash="cc")

    assert result.found is True
    assert job.winner_node == "node-1"


def test_reassign_finished_job_is_noop(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    manager = make_manager(two_nodes)
    job = manager.start_job(make_block())
    manager.record_result(job.job_id, "node-1", nonce=1, block_hash="aa")
    assert manager.reassign(job.job_id, "node-2") == []
    assert job.workers() == ["node-1", "node-2"]


def test_reassign_unknown_node_is_noop(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    manager = make_manager(two_nodes)
    job = manager.start_job(make_block())
    assert manager.reassign(job.job_id, "node-9") == []


def test_reassign_with_single_alive_worker_takes_whole_span(
    two_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(two_nodes, nonce_space=100)
    job = manager.start_job(make_block())
    new_assignments = manager.reassign(job.job_id, "node-2")
    assert [(item.nonce_start, item.nonce_end) for item in new_assignments] == [(50, 100)]
    assert job.ranges_for("node-1") == [(0, 50), (50, 100)]


def test_reassign_grouped_keeps_worker_mapping(
    three_nodes: list[NodeRecord], log_dir_env: Path
) -> None:
    manager = make_manager(three_nodes)
    job = manager.start_job(make_block())

    grouped = manager.reassign_grouped(job.job_id, "node-2")

    assert sorted(grouped) == ["node-1", "node-3"]
    assert sorted((item.nonce_start, item.nonce_end) for item in grouped["node-1"]) == [
        (300, 450)
    ]
    assert sorted((item.nonce_start, item.nonce_end) for item in grouped["node-3"]) == [
        (450, 600)
    ]


def test_unknown_job_is_rejected(two_nodes: list[NodeRecord], log_dir_env: Path) -> None:
    manager = make_manager(two_nodes)
    with pytest.raises(ValueError, match="tidak dikenal"):
        manager.get("job-99-1")


def test_invalid_nonce_space_is_rejected(two_nodes: list[NodeRecord]) -> None:
    with pytest.raises(ValueError):
        PoolJobManager(NodeRegistry(two_nodes), nonce_space=0)
