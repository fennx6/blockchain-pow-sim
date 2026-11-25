"""Tests for the node runtime wiring."""

from __future__ import annotations

import json
import signal
import threading
import time
from collections.abc import Sequence
from pathlib import Path

from chainsim.config import Settings, load_settings
from chainsim.coordinator.registry import NodeRecord
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.node import main as node_main
from chainsim.node.grpc_client import JobStartResult, NodeClient, PeerCallResult
from chainsim.node.main import NodeRuntime
from chainsim.types import Block, NodeStatus, Transaction, WorkAssignment


def _settings(
    tmp_path: Path,
    *,
    mode: str = "compete",
    peers: str = "",
    node_id: str = "node-1",
) -> Settings:
    return load_settings(
        env={
            "NODE_ID": node_id,
            "NODE_REST_PORT": "18081",
            "NODE_GRPC_PORT": "50051",
            "PEERS": peers,
            "DIFFICULTY": "1",
            "MINING_MODE": mode,
            "BLOCK_SIZE": "50",
            "CONFIRMATIONS": "3",
            "HEARTBEAT_INTERVAL_S": "0.05",
            "HEARTBEAT_TIMEOUT_S": "0.05",
            "GRPC_TIMEOUT_S": "0.5",
            "TX_FORWARD_TIMEOUT_S": "0.5",
            "LOG_DIR": str(tmp_path / "logs"),
            "LOG_LEVEL": "WARNING",
        }
    )


def _tx(seed: int) -> Transaction:
    return create_transaction(
        sender=f"addr-{seed}",
        receiver=f"addr-{seed}-b",
        amount=1.0 + seed / 100,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000 + seed,
    )


def _block(
    prev: Block,
    txs: Sequence[Transaction],
    *,
    index: int = 1,
    miner: str = "node-2",
) -> Block:
    template = build_block_template(
        index=index,
        prev_hash=prev.hash,
        transactions=txs,
        difficulty=1,
        miner_id=miner,
        timestamp_ms=1_700_000_000_000 + index,
    )
    block = mine(template, 0, 1_000_000, threading.Event())
    assert block is not None
    return block


def _build_chain(length: int) -> list[Block]:
    chain = [create_genesis_block(1)]
    for index in range(1, length + 1):
        chain.append(_block(chain[-1], [_tx(index)], index=index))
    return chain


def _read_events(log_dir: Path, node_id: str) -> list[dict]:
    path = log_dir / f"{node_id}.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


class _FakeSyncClient:
    def __init__(self, blocks: Sequence[Block]) -> None:
        self.blocks = list(blocks)
        self.closed = False

    def get_status(self) -> NodeStatus:
        tip = self.blocks[-1]
        return NodeStatus(
            node_id="peer-1",
            height=tip.index,
            tip_hash=tip.hash,
            peers=[],
            mempool_size=0,
            hashrate=0.0,
            state="up",
        )

    def get_blocks(self, from_index: int) -> list[Block]:
        return [block for block in self.blocks if block.index >= from_index]

    def close(self) -> None:
        self.closed = True


class _FakeCoordinatorClient:
    def __init__(self, *, ok: bool = True, peers: Sequence[NodeRecord] = ()) -> None:
        self.ok = ok
        self.peers = list(peers)
        self.registrations: list[NodeRecord] = []
        self.templates: list[tuple[str, Block]] = []
        self.results: list[WorkResult] = []
        self.closed = False

    @property
    def address(self) -> str:
        return "coordinator:50052"

    def register(self, record: NodeRecord) -> list[NodeRecord]:
        self.registrations.append(record)
        return list(self.peers)

    def submit_template(self, node_id: str, block_template: Block) -> JobStartResult:
        self.templates.append((node_id, block_template))
        return JobStartResult(True, f"job-{block_template.index}-{len(self.templates)}", "started")

    def submit_work_result(self, result: WorkResult) -> PeerCallResult:
        self.results.append(result)
        return PeerCallResult("coordinator:50052", self.ok, "ok" if self.ok else "lost")

    def close(self) -> None:
        self.closed = True


def test_runtime_grpc_round_trip(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    runtime.start(start_miner=False)
    try:
        with NodeClient(f"127.0.0.1:{runtime.grpc_port}", timeout_s=1.0) as client:
            status = client.get_status()
            assert status.node_id == "node-1"
            assert status.height == 0

            tx = _tx(1)
            ack = client.submit_transaction(tx, from_node="client")
            assert ack.ok is True
            assert runtime.mempool.contains(tx.tx_id)

            block = _block(runtime.chain.tip(), [tx])
            ack = client.announce_block(block, from_node="node-2")
            assert ack.ok is True
            assert runtime.chain.height() == 1
    finally:
        runtime.stop()


def test_runtime_rest_post_tx_and_status(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        client = runtime.rest_app.test_client()
        response = client.post("/tx", json={"sender": "a", "receiver": "b", "amount": 1.0})
        assert response.status_code == 201
        assert runtime.mempool.size() == 1

        status = client.get("/status").get_json()
        assert status["node_id"] == "node-1"
        assert status["mempool_size"] == 1
        assert status["state"] == "idle"

        chain = client.get("/chain").get_json()
        assert chain["height"] == 0
    finally:
        runtime.stop()


def test_runtime_orphan_triggers_sync(tmp_path: Path, log_dir_env: Path) -> None:
    peer_chain = _build_chain(2)
    fake = _FakeSyncClient(peer_chain)
    runtime = NodeRuntime(
        _settings(tmp_path, peers="peer-1"),
        data_dir=tmp_path,
        grpc_port=0,
        sync_client_factory=lambda _: fake,
    )
    try:
        ok, message = runtime.handle_block(peer_chain[2])
        assert ok is True
        assert message == "orphan_sync_scheduled"
        runtime.sync.start()
        deadline = time.monotonic() + 2.0
        while (
            runtime.chain.height() < 2 or not fake.closed
        ) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert runtime.chain.height() == 2
        assert runtime.chain.tip().hash == peer_chain[-1].hash
        assert fake.closed is True

        events = _read_events(log_dir_env, "node-1")
        assert [event["event"] for event in events].count("sync_done") == 1
    finally:
        runtime.stop()


def test_runtime_handle_work_result_reports_to_coordinator(
    tmp_path: Path, log_dir_env: Path
) -> None:
    fake = _FakeCoordinatorClient()
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        runtime.handle_work_result(assignment, None)
        assert runtime.chain.height() == 1
        assert runtime.chain.tip().hash == block.hash
    finally:
        runtime.stop()

    assert len(fake.results) == 2
    found_result, exhausted_result = fake.results
    assert found_result.job_id == "job-1"
    assert found_result.node_id == "node-1"
    assert found_result.nonce == block.nonce
    assert found_result.found is True
    assert found_result.block_hash == block.hash
    assert exhausted_result.job_id == "job-1"
    assert exhausted_result.node_id == "node-1"
    assert exhausted_result.nonce == 999
    assert exhausted_result.found is False
    assert exhausted_result.block_hash == ""
    assert fake.closed is True


def test_runtime_handle_work_result_skips_invalid_block(tmp_path: Path, log_dir_env: Path) -> None:
    fake = _FakeCoordinatorClient()
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        block.hash = "0" * 64
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        assert runtime.chain.height() == 0
    finally:
        runtime.stop()

    assert fake.results == []


def test_runtime_pool_loser_does_not_commit_candidate(tmp_path: Path, log_dir_env: Path) -> None:
    fake = _FakeCoordinatorClient(ok=False)
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        assert runtime.chain.height() == 0
        assert runtime.chain.tip().hash == runtime.chain.genesis.hash
    finally:
        runtime.stop()

    assert len(fake.results) == 1
    assert fake.results[0].block_hash == block.hash


def test_runtime_pool_loser_converges_to_winner_block(tmp_path: Path, log_dir_env: Path) -> None:
    winner_fake = _FakeCoordinatorClient(ok=True)
    loser_fake = _FakeCoordinatorClient(ok=False)
    winner = NodeRuntime(
        _settings(tmp_path, mode="pool", node_id="node-a"),
        data_dir=tmp_path / "a",
        grpc_port=0,
        coordinator_client=winner_fake,
    )
    loser = NodeRuntime(
        _settings(tmp_path, mode="pool", node_id="node-b"),
        data_dir=tmp_path / "b",
        grpc_port=0,
        coordinator_client=loser_fake,
    )
    try:
        winner_block = _block(winner.chain.tip(), [_tx(1)])
        loser_block = _block(loser.chain.tip(), [_tx(2)])
        winner_assignment = WorkAssignment("job-1", winner.chain.tip(), 0, 1_000)
        loser_assignment = WorkAssignment("job-1", loser.chain.tip(), 0, 1_000)

        winner.handle_work_result(winner_assignment, winner_block)
        loser.handle_work_result(loser_assignment, loser_block)

        assert winner.chain.height() == 1
        assert winner.chain.tip().hash == winner_block.hash
        assert loser.chain.height() == 0
        assert loser.chain.tip().hash == loser.chain.genesis.hash

        assert loser.handle_block(winner_block) == (True, "accepted")
        assert loser.chain.tip().hash == winner.chain.tip().hash
    finally:
        winner.stop()
        loser.stop()


def test_runtime_pool_without_coordinator_does_not_commit(
    tmp_path: Path, log_dir_env: Path, monkeypatch
) -> None:
    monkeypatch.delenv("COORDINATOR_GRPC", raising=False)
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=None,
    )
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assignment = WorkAssignment("job-1", runtime.chain.tip(), 0, 1_000)
        runtime.handle_work_result(assignment, block)
        assert runtime.chain.height() == 0
        assert runtime.chain.tip().hash == runtime.chain.genesis.hash
    finally:
        runtime.stop()


def test_runtime_handle_block_duplicate_and_invalid(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        block = _block(runtime.chain.tip(), [_tx(1)])
        assert runtime.handle_block(block) == (True, "accepted")
        assert runtime.handle_block(block) == (True, "duplicate")

        bad = _block(runtime.chain.tip(), [_tx(2)])
        bad.hash = "0" * 64
        assert runtime.handle_block(bad) == (False, "invalid")
        assert runtime.chain.height() == 1
    finally:
        runtime.stop()


def test_runtime_handle_transaction_duplicate_and_confirmed(
    tmp_path: Path, log_dir_env: Path
) -> None:
    runtime = NodeRuntime(_settings(tmp_path), data_dir=tmp_path, grpc_port=0)
    try:
        tx = _tx(1)
        assert runtime.handle_transaction(tx) == (True, "accepted")
        assert runtime.handle_transaction(tx) == (True, "duplicate")

        block = _block(runtime.chain.tip(), [tx])
        assert runtime.handle_block(block) == (True, "accepted")
        assert runtime.handle_transaction(tx) == (True, "already_confirmed")
    finally:
        runtime.stop()


def test_runtime_persists_chain_across_restart(tmp_path: Path, log_dir_env: Path) -> None:
    settings = _settings(tmp_path)
    first = NodeRuntime(settings, data_dir=tmp_path, grpc_port=0)
    try:
        block = _block(first.chain.tip(), [_tx(1)])
        assert first.handle_block(block) == (True, "accepted")
    finally:
        first.stop()

    second = NodeRuntime(settings, data_dir=tmp_path, grpc_port=0)
    try:
        assert second.chain.height() == 1
        assert second.chain.tip().hash == block.hash
    finally:
        second.stop()


def test_runtime_pool_work_handlers(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(_settings(tmp_path, mode="pool"), data_dir=tmp_path, grpc_port=0)
    try:
        template = _block(runtime.chain.tip(), [])
        assignment = WorkAssignment("job-1", template, 0, 100)
        assert runtime.miner.mode == "pool"
        assert runtime.handle_start_work(assignment) == (True, "assigned")
        assert runtime.handle_stop_work("job-1") == (True, "stopped")
    finally:
        runtime.stop()


def test_runtime_propagates_tx_and_block_to_peer(tmp_path: Path, log_dir_env: Path) -> None:
    first = NodeRuntime(
        _settings(tmp_path, node_id="node-a"),
        data_dir=tmp_path / "a",
        grpc_port=0,
    )
    second = NodeRuntime(
        _settings(tmp_path, node_id="node-b"),
        data_dir=tmp_path / "b",
        grpc_port=0,
    )
    first.start(start_miner=False)
    second.start(start_miner=False)
    try:
        first.gossip.set_peers([f"127.0.0.1:{second.grpc_port}"])
        second.gossip.set_peers([f"127.0.0.1:{first.grpc_port}"])

        response = first.rest_app.test_client().post(
            "/tx", json={"sender": "alice", "receiver": "bob", "amount": 1.0}
        )
        assert response.status_code == 201
        assert second.mempool.size() == 1

        tx = first.mempool.all()[0]
        block = _block(first.chain.tip(), [tx])
        assert first.handle_block(block) == (True, "accepted")
        results = first.gossip.broadcast_block(block)
        assert len(results) == 1
        assert results[0].ok is True

        deadline = time.monotonic() + 2.0
        while second.chain.height() == 0 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert second.chain.height() == 1
        assert second.chain.tip().hash == block.hash
        assert second.mempool.size() == 0
    finally:
        first.stop()
        second.stop()


def test_runtime_cleans_self_peer_and_status_meta(tmp_path: Path, log_dir_env: Path) -> None:
    runtime = NodeRuntime(
        _settings(tmp_path, peers="node-1,node-2,node-2"),
        data_dir=tmp_path,
        grpc_port=0,
    )
    try:
        assert runtime.peers == ("node-2",)
        assert runtime.gossip.peers() == ["node-2"]

        status = runtime.status()
        assert status.node_id == "node-1"
        assert status.height == 0
        assert status.tip_hash == runtime.chain.genesis.hash
        assert status.peers == ["node-2"]
        assert status.mempool_size == 0
        assert status.hashrate == 0.0
        assert status.state == "idle"
        assert status.meta["difficulty"] == 1
        assert status.meta["mining_mode"] == "compete"
    finally:
        runtime.stop()


def test_runtime_start_registers_and_pool_loop_submits_template(
    tmp_path: Path, log_dir_env: Path
) -> None:
    fake = _FakeCoordinatorClient()
    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=fake,
        pool_poll_s=0.02,
    )
    runtime.start(start_miner=True)
    try:
        deadline = time.monotonic() + 2.0
        while not fake.templates and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        runtime.stop()

    assert len(fake.registrations) == 1
    assert fake.registrations[0].node_id == "node-1"
    assert fake.registrations[0].grpc_address == f"node-1:{runtime.grpc_port}"
    assert len(fake.templates) >= 1
    submitted_node, template = fake.templates[0]
    assert submitted_node == "node-1"
    assert template.index == 1
    assert template.prev_hash == runtime.chain.genesis.hash


def test_runtime_sync_notifies_miner_when_tip_advances(tmp_path: Path, log_dir_env: Path) -> None:
    peer_chain = _build_chain(2)
    fake_sync = _FakeSyncClient(peer_chain)
    runtime = NodeRuntime(
        _settings(tmp_path, peers="peer-1"),
        data_dir=tmp_path,
        grpc_port=0,
        sync_client_factory=lambda _: fake_sync,
    )
    notified = 0
    original_notify = runtime.miner.notify_new_block

    def tracking_notify() -> None:
        nonlocal notified
        notified += 1
        original_notify()

    runtime.miner.notify_new_block = tracking_notify  # type: ignore[method-assign]
    try:
        assert runtime.sync.sync_now() == 2
        assert notified == 1
        assert runtime.sync.sync_now() == 0
        assert notified == 1
    finally:
        runtime.stop()


def test_main_handles_sigterm_gracefully(tmp_path: Path, log_dir_env: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(node_main, "load_settings", lambda: settings)

    stopped = False
    original_stop = NodeRuntime.stop
    real_init = NodeRuntime.__init__

    def patched_init(self: NodeRuntime, s: Settings, **kw) -> None:
        real_init(self, s, data_dir=tmp_path, grpc_port=0, **kw)

    def spy_stop(self: NodeRuntime, *, grpc_grace_s: float = 1.0) -> None:
        nonlocal stopped
        stopped = True
        original_stop(self, grpc_grace_s=grpc_grace_s)

    def trigger_sigterm(self: NodeRuntime) -> None:
        signal.raise_signal(signal.SIGTERM)

    previous = signal.getsignal(signal.SIGTERM)
    monkeypatch.setattr(NodeRuntime, "__init__", patched_init)
    monkeypatch.setattr(NodeRuntime, "stop", spy_stop)
    monkeypatch.setattr(NodeRuntime, "run_rest", trigger_sigterm)

    code = node_main.main()

    assert code == 0
    assert stopped is True
    assert signal.getsignal(signal.SIGTERM) == previous
    events = _read_events(log_dir_env, "node-1")
    event_names = [event["event"] for event in events]
    assert "node_start" in event_names
    assert "node_stop" in event_names


def test_runtime_pool_mode_end_to_end_with_coordinator(
    tmp_path: Path, log_dir_env: Path, monkeypatch
) -> None:
    from chainsim.coordinator.grpc_server import CoordinatorService
    from chainsim.coordinator.grpc_server import build_server as build_coord_server
    from chainsim.coordinator.registry import NodeRegistry
    from chainsim.coordinator.scheduler import PoolJobManager
    from chainsim.node.grpc_client import CoordinatorClient

    monkeypatch.setenv("NODE_ADVERTISE_HOST", "127.0.0.1")
    registry = NodeRegistry()
    jobs = PoolJobManager(registry, nonce_space=100_000)
    coord_service = CoordinatorService(registry, jobs, timeout_s=1.0)
    coord_server, coord_port = build_coord_server(coord_service, host="127.0.0.1", port=0)
    coord_server.start()

    runtime = NodeRuntime(
        _settings(tmp_path, mode="pool"),
        data_dir=tmp_path,
        grpc_port=0,
        coordinator_client=CoordinatorClient(f"127.0.0.1:{coord_port}", timeout_s=1.0),
        pool_poll_s=0.02,
    )
    try:
        runtime.start(start_miner=True)
        deadline = time.monotonic() + 5.0
        while runtime.chain.height() < 1 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert runtime.chain.height() >= 1
        assert registry.get("node-1") is not None
    finally:
        runtime.stop()
        coord_server.stop(0)
