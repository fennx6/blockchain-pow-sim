"""Tests for gRPC clients and the coordinator client round-trip."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from chainsim.coordinator.registry import NodeRecord
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.transaction import create_transaction
from chainsim.generated import blockchain_pb2, blockchain_pb2_grpc
from chainsim.node.grpc_client import (
    CoordinatorClient,
    broadcast_block,
    broadcast_tx,
    fetch_blocks,
    get_status,
    normalize_address,
)
from chainsim.node.grpc_server import NodeService, build_server
from chainsim.types import NodeStatus


def _make_tx():
    return create_transaction(
        sender="addr-a",
        receiver="addr-b",
        amount=2.0,
        origin_node="node-1",
        timestamp_ms=1_700_000_000_000,
    )


def _make_block(tx, *, index: int = 1):
    genesis = create_genesis_block(difficulty=1)
    template = build_block_template(
        index=index,
        prev_hash=genesis.hash,
        transactions=[tx],
        difficulty=1,
        miner_id="node-1",
        timestamp_ms=1_700_000_000_001,
    )
    block = mine(template, 0, 10_000, threading.Event())
    assert block is not None
    return block


def _status() -> NodeStatus:
    return NodeStatus(
        node_id="node-1",
        height=2,
        tip_hash="d" * 64,
        peers=["node-2"],
        mempool_size=1,
        hashrate=10.0,
        state="up",
        meta={"difficulty": 1},
    )


def _start_node_service() -> tuple[grpc.Server, int, list, list]:
    transactions: list = []
    blocks: list = []
    service = NodeService(
        node_id="node-1",
        on_transaction=lambda tx: transactions.append(tx) or (True, "accepted"),
        on_block=lambda block: blocks.append(block) or (True, "accepted"),
        get_blocks=lambda index: [create_genesis_block(difficulty=1)][index:],
        get_status=_status,
    )
    server, port = build_server(service, host="127.0.0.1", port=0)
    server.start()
    return server, port, transactions, blocks


def test_normalize_address() -> None:
    assert normalize_address("node-2") == "node-2:50051"
    assert normalize_address("node-2:6000") == "node-2:6000"
    assert normalize_address("http://node-2:50051/") == "node-2:50051"
    with pytest.raises(ValueError):
        normalize_address("")


def test_broadcast_tx_and_block() -> None:
    server, port, transactions, blocks = _start_node_service()
    address = f"127.0.0.1:{port}"
    try:
        tx = _make_tx()
        tx_results = broadcast_tx(tx, [address], timeout_s=1.0, from_node="node-9")
        block = _make_block(tx)
        block_results = broadcast_block(block, [address], timeout_s=1.0, from_node="node-9")
    finally:
        server.stop(0)

    assert [result.ok for result in tx_results] == [True]
    assert [result.ok for result in block_results] == [True]
    assert transactions == [tx]
    assert blocks == [block]


def test_broadcast_reports_invalid_address() -> None:
    results = broadcast_tx(_make_tx(), [""], timeout_s=0.5)
    assert len(results) == 1
    assert results[0].ok is False


def test_fetch_blocks_and_get_status() -> None:
    server, port, _, _ = _start_node_service()
    address = f"127.0.0.1:{port}"
    try:
        blocks = fetch_blocks(address, 0, timeout_s=1.0)
        status = get_status(address, timeout_s=1.0)
    finally:
        server.stop(0)

    assert len(blocks) == 1
    assert blocks[0].index == 0
    assert status.node_id == "node-1"
    assert status.height == 2
    assert status.tip_hash == "d" * 64


class _FakeCoordinator(blockchain_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self) -> None:
        self.registered: list = []
        self.heartbeats: list = []
        self.work_results: list = []
        self.templates: list = []

    def Register(self, request, context):  # noqa: N802
        self.registered.append(request)
        return blockchain_pb2.PeerList(
            nodes=[
                request,
                blockchain_pb2.NodeInfo(
                    node_id="node-2",
                    rest_url="http://node-2:8081",
                    grpc_address="node-2:50051",
                ),
            ]
        )

    def Heartbeat(self, request, context):  # noqa: N802
        self.heartbeats.append(request)
        return blockchain_pb2.HeartbeatReply(
            ok=True,
            peers=[
                blockchain_pb2.NodeInfo(
                    node_id="node-2",
                    rest_url="http://node-2:8081",
                    grpc_address="node-2:50051",
                )
            ],
        )

    def SubmitWorkResult(self, request, context):  # noqa: N802
        self.work_results.append(request)
        return blockchain_pb2.Ack(ok=True, message="recorded")

    def GetPeers(self, request, context):  # noqa: N802
        return blockchain_pb2.PeerList(
            nodes=[
                blockchain_pb2.NodeInfo(
                    node_id="node-3",
                    rest_url="http://node-3:8081",
                    grpc_address="node-3:50051",
                )
            ]
        )

    def SubmitTemplate(self, request, context):  # noqa: N802
        self.templates.append(request)
        return blockchain_pb2.JobStartedMsg(accepted=True, job_id="job-1", message="started")


def test_coordinator_client_round_trip() -> None:
    fake = _FakeCoordinator()
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    blockchain_pb2_grpc.add_CoordinatorServiceServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with CoordinatorClient(f"127.0.0.1:{port}", timeout_s=1.0) as client:
            peers = client.register(
                NodeRecord("node-1", "http://node-1:8081", "node-1:50051", ("node-2",))
            )
            heartbeat_ok, heartbeat_peers = client.heartbeat(_status())
            work_result = client.submit_work_result(
                WorkResult("job-1", "node-1", 7, True, "c" * 64)
            )
            current_peers = client.get_peers()
            job = client.submit_template("node-1", _make_block(_make_tx()))
    finally:
        server.stop(0)

    assert [peer.node_id for peer in peers] == ["node-1", "node-2"]
    assert heartbeat_ok is True
    assert [peer.node_id for peer in heartbeat_peers] == ["node-2"]
    assert work_result.ok is True
    assert work_result.message == "recorded"
    assert [peer.node_id for peer in current_peers] == ["node-3"]
    assert job.accepted is True
    assert job.job_id == "job-1"
    assert len(fake.registered) == 1
    assert len(fake.heartbeats) == 1
    assert len(fake.work_results) == 1
    assert len(fake.templates) == 1
