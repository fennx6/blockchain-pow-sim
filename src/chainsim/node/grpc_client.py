"""gRPC clients for node-to-node and node-to-coordinator calls.

Owner: Member B (AGENTS.md Sections 3 and 6.2). Module-level helpers match the
frozen contract (``broadcast_tx``, ``broadcast_block``, ``fetch_blocks``);
classes reuse a channel for long-running gossip and sync loops.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import grpc

from chainsim.coordinator.registry import NodeRecord
from chainsim.coordinator.scheduler import WorkResult
from chainsim.generated import blockchain_pb2, blockchain_pb2_grpc
from chainsim.node.convert import (
    FROM_NODE_METADATA_KEY,
    block_from_msg,
    block_to_msg,
    node_info_from_msg,
    node_info_to_msg,
    status_from_msg,
    status_to_msg,
    tx_to_msg,
    work_assignment_to_msg,
    work_result_to_msg,
)
from chainsim.types import Block, NodeStatus, Transaction, WorkAssignment

DEFAULT_TIMEOUT_S = 2.0
DEFAULT_GRPC_PORT = 50051


class GrpcClientError(RuntimeError):
    """Raised when an RPC that must return data fails."""


@dataclass(frozen=True)
class PeerCallResult:
    """Outcome of a best-effort call to one peer."""

    peer: str
    ok: bool
    message: str = ""


@dataclass(frozen=True)
class JobStartResult:
    """Outcome of ``CoordinatorService.SubmitTemplate``."""

    accepted: bool
    job_id: str = ""
    message: str = ""


def normalize_address(address: str, default_port: int = DEFAULT_GRPC_PORT) -> str:
    """Turn ``node-2``, ``node-2:50051``, or ``http://node-2:50051`` into ``host:port``."""
    text = address.strip()
    if not text:
        raise ValueError("gRPC address must not be empty")
    if "://" in text:
        text = text.split("://", 1)[1]
    text = text.rstrip("/")
    if not text:
        raise ValueError(f"invalid gRPC address: {address!r}")
    if ":" not in text:
        text = f"{text}:{default_port}"
    return text


def _metadata(from_node: str) -> tuple[tuple[str, str], ...]:
    return ((FROM_NODE_METADATA_KEY, from_node),) if from_node else ()


def _rpc_message(exc: grpc.RpcError) -> str:
    code = exc.code().name if exc.code() is not None else "UNKNOWN"
    details = exc.details() or str(exc)
    return f"{code}: {details}"


class NodeClient:
    """Reusable client for one node's ``NodeService``."""

    def __init__(self, address: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        if timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s!r}")
        self._address = normalize_address(address)
        self._timeout_s = timeout_s
        self._channel = grpc.insecure_channel(self._address)
        self._stub = blockchain_pb2_grpc.NodeServiceStub(self._channel)

    @property
    def address(self) -> str:
        return self._address

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> NodeClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def submit_transaction(self, tx: Transaction, *, from_node: str = "") -> PeerCallResult:
        try:
            ack = self._stub.SubmitTransaction(
                tx_to_msg(tx), timeout=self._timeout_s, metadata=_metadata(from_node)
            )
        except grpc.RpcError as exc:
            return PeerCallResult(self._address, False, _rpc_message(exc))
        return PeerCallResult(self._address, bool(ack.ok), ack.message)

    def announce_block(self, block: Block, *, from_node: str = "") -> PeerCallResult:
        try:
            ack = self._stub.AnnounceBlock(
                block_to_msg(block), timeout=self._timeout_s, metadata=_metadata(from_node)
            )
        except grpc.RpcError as exc:
            return PeerCallResult(self._address, False, _rpc_message(exc))
        return PeerCallResult(self._address, bool(ack.ok), ack.message)

    def get_blocks(self, from_index: int) -> list[Block]:
        if from_index < 0:
            raise ValueError(f"from_index must be non-negative, got {from_index}")
        try:
            stream = self._stub.GetBlocks(
                blockchain_pb2.BlockRange(from_index=from_index), timeout=self._timeout_s
            )
            return [block_from_msg(msg) for msg in stream]
        except (grpc.RpcError, ValueError) as exc:
            raise GrpcClientError(f"{self._address}: {_rpc_message(exc)}") from exc

    def get_status(self) -> NodeStatus:
        try:
            reply = self._stub.GetStatus(blockchain_pb2.Empty(), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            raise GrpcClientError(f"{self._address}: {_rpc_message(exc)}") from exc
        return status_from_msg(reply)

    def start_work(self, assignment: WorkAssignment) -> PeerCallResult:
        try:
            ack = self._stub.StartWork(work_assignment_to_msg(assignment), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            return PeerCallResult(self._address, False, _rpc_message(exc))
        return PeerCallResult(self._address, bool(ack.ok), ack.message)

    def stop_work(self, job_id: str) -> PeerCallResult:
        try:
            ack = self._stub.StopWork(blockchain_pb2.JobRef(job_id=job_id), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            return PeerCallResult(self._address, False, _rpc_message(exc))
        return PeerCallResult(self._address, bool(ack.ok), ack.message)


class CoordinatorClient:
    """Reusable client for the coordinator's ``CoordinatorService``."""

    def __init__(self, address: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        if timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s!r}")
        self._address = normalize_address(address)
        self._timeout_s = timeout_s
        self._channel = grpc.insecure_channel(self._address)
        self._stub = blockchain_pb2_grpc.CoordinatorServiceStub(self._channel)

    @property
    def address(self) -> str:
        return self._address

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> CoordinatorClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def register(self, record: NodeRecord) -> list[NodeRecord]:
        try:
            reply = self._stub.Register(node_info_to_msg(record), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            raise GrpcClientError(f"{self._address}: {_rpc_message(exc)}") from exc
        return [node_info_from_msg(node) for node in reply.nodes]

    def heartbeat(self, status: NodeStatus) -> tuple[bool, list[NodeRecord]]:
        try:
            reply = self._stub.Heartbeat(
                blockchain_pb2.HeartbeatMsg(node_id=status.node_id, status=status_to_msg(status)),
                timeout=self._timeout_s,
            )
        except grpc.RpcError as exc:
            raise GrpcClientError(f"{self._address}: {_rpc_message(exc)}") from exc
        return bool(reply.ok), [node_info_from_msg(node) for node in reply.peers]

    def submit_work_result(self, result: WorkResult) -> PeerCallResult:
        try:
            ack = self._stub.SubmitWorkResult(work_result_to_msg(result), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            return PeerCallResult(self._address, False, _rpc_message(exc))
        return PeerCallResult(self._address, bool(ack.ok), ack.message)

    def get_peers(self) -> list[NodeRecord]:
        try:
            reply = self._stub.GetPeers(blockchain_pb2.Empty(), timeout=self._timeout_s)
        except grpc.RpcError as exc:
            raise GrpcClientError(f"{self._address}: {_rpc_message(exc)}") from exc
        return [node_info_from_msg(node) for node in reply.nodes]

    def submit_template(self, node_id: str, block_template: Block) -> JobStartResult:
        try:
            reply = self._stub.SubmitTemplate(
                blockchain_pb2.TemplateMsg(
                    node_id=node_id, block_template=block_to_msg(block_template)
                ),
                timeout=self._timeout_s,
            )
        except grpc.RpcError as exc:
            return JobStartResult(False, "", _rpc_message(exc))
        return JobStartResult(bool(reply.accepted), reply.job_id, reply.message)


def broadcast_tx(
    tx: Transaction,
    peers: Sequence[str],
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    from_node: str = "",
) -> list[PeerCallResult]:
    """Send ``tx`` to every peer and return one result per peer."""
    results: list[PeerCallResult] = []
    for peer in peers:
        try:
            with NodeClient(peer, timeout_s=timeout_s) as client:
                results.append(client.submit_transaction(tx, from_node=from_node))
        except (OSError, ValueError) as exc:
            results.append(PeerCallResult(str(peer), False, str(exc)))
    return results


def broadcast_block(
    block: Block,
    peers: Sequence[str],
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    from_node: str = "",
) -> list[PeerCallResult]:
    """Send ``block`` to every peer and return one result per peer."""
    results: list[PeerCallResult] = []
    for peer in peers:
        try:
            with NodeClient(peer, timeout_s=timeout_s) as client:
                results.append(client.announce_block(block, from_node=from_node))
        except (OSError, ValueError) as exc:
            results.append(PeerCallResult(str(peer), False, str(exc)))
    return results


def fetch_blocks(
    peer: str,
    from_index: int,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[Block]:
    """Fetch blocks ``[from_index, ...]`` from one peer (raises on failure)."""
    with NodeClient(peer, timeout_s=timeout_s) as client:
        return client.get_blocks(from_index)


def get_status(peer: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> NodeStatus:
    """Read one peer's ``NodeStatus`` (raises on failure)."""
    with NodeClient(peer, timeout_s=timeout_s) as client:
        return client.get_status()


def start_work(
    peer: str,
    assignment: WorkAssignment,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> PeerCallResult:
    """Send one pool assignment to a worker."""
    with NodeClient(peer, timeout_s=timeout_s) as client:
        return client.start_work(assignment)


def stop_work(
    peer: str,
    job_id: str,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> PeerCallResult:
    """Ask a worker to stop a pool job."""
    with NodeClient(peer, timeout_s=timeout_s) as client:
        return client.stop_work(job_id)


def coordinator_register(
    address: str,
    record: NodeRecord,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[NodeRecord]:
    """Register one node and return the coordinator's peer list."""
    with CoordinatorClient(address, timeout_s=timeout_s) as client:
        return client.register(record)


def coordinator_heartbeat(
    address: str,
    status: NodeStatus,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> tuple[bool, list[NodeRecord]]:
    """Send one heartbeat and return the coordinator's reply."""
    with CoordinatorClient(address, timeout_s=timeout_s) as client:
        return client.heartbeat(status)


def coordinator_submit_work_result(
    address: str,
    result: WorkResult,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> PeerCallResult:
    """Report a pool work result to the coordinator."""
    with CoordinatorClient(address, timeout_s=timeout_s) as client:
        return client.submit_work_result(result)


def coordinator_get_peers(
    address: str,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[NodeRecord]:
    """Fetch the coordinator's current peer list."""
    with CoordinatorClient(address, timeout_s=timeout_s) as client:
        return client.get_peers()


def coordinator_submit_template(
    address: str,
    node_id: str,
    block_template: Block,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> JobStartResult:
    """Submit a block template for a pool job."""
    with CoordinatorClient(address, timeout_s=timeout_s) as client:
        return client.submit_template(node_id, block_template)
