"""Server gRPC ``NodeService`` (AGENTS.md Section 6.3).

Owner: Member B. The servicer is deliberately transport-only: it converts
protobuf messages, calls injected handlers, and logs receipt events. Chain
validation, mempool insertion, and mining stay in the runtime that wires these
handlers (``node/main.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor

import grpc

from chainsim.generated import blockchain_pb2, blockchain_pb2_grpc
from chainsim.logging_utils import log_event
from chainsim.node.convert import (
    FROM_NODE_METADATA_KEY,
    block_from_msg,
    block_to_msg,
    status_to_msg,
    tx_from_msg,
    work_assignment_from_msg,
)
from chainsim.types import Block, NodeStatus, Transaction, WorkAssignment

logger = logging.getLogger(__name__)

HandlerResult = bool | tuple[bool, str]
TransactionHandler = Callable[[Transaction], HandlerResult]
BlockHandler = Callable[[Block], HandlerResult]
BlocksHandler = Callable[[int], Sequence[Block]]
StatusHandler = Callable[[], NodeStatus]
WorkHandler = Callable[[WorkAssignment], HandlerResult]
StopWorkHandler = Callable[[str], HandlerResult]

DEFAULT_MAX_WORKERS = 8


def _normalize_result(value: HandlerResult) -> tuple[bool, str]:
    if isinstance(value, tuple):
        ok, message = value
        return bool(ok), str(message)
    return bool(value), ""


def _from_node(context: grpc.ServicerContext) -> str:
    for item in context.invocation_metadata():
        if item.key == FROM_NODE_METADATA_KEY:
            return item.value
    return ""


def _ack(value: HandlerResult) -> blockchain_pb2.Ack:
    ok, message = _normalize_result(value)
    return blockchain_pb2.Ack(ok=ok, message=message)


class NodeService(blockchain_pb2_grpc.NodeServiceServicer):
    """gRPC facade over one node's chain, mempool, miner, and sync callbacks."""

    def __init__(
        self,
        *,
        node_id: str,
        on_transaction: TransactionHandler,
        on_block: BlockHandler,
        get_blocks: BlocksHandler,
        get_status: StatusHandler,
        start_work: WorkHandler | None = None,
        stop_work: StopWorkHandler | None = None,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        self._node_id = node_id.strip()
        self._on_transaction = on_transaction
        self._on_block = on_block
        self._get_blocks = get_blocks
        self._get_status = get_status
        self._start_work = start_work
        self._stop_work = stop_work

    def _safe_log(self, event: str, **fields: object) -> None:
        try:
            log_event(event, self._node_id, **fields)
        except Exception:
            logger.exception("failed to write %s event for %s", event, self._node_id)

    def SubmitTransaction(
        self, request: blockchain_pb2.TransactionMsg, context: grpc.ServicerContext
    ) -> blockchain_pb2.Ack:
        from_node = _from_node(context)
        try:
            tx = tx_from_msg(request)
        except ValueError as exc:
            self._safe_log(
                "tx_received",
                tx_id=request.tx_id,
                accepted=False,
                reason=str(exc),
                from_node=from_node,
                to_node=self._node_id,
            )
            return blockchain_pb2.Ack(ok=False, message=str(exc))

        try:
            ok, message = _normalize_result(self._on_transaction(tx))
        except Exception as exc:
            logger.exception("on_transaction handler failed for %s", tx.tx_id)
            return blockchain_pb2.Ack(ok=False, message=f"handler failed: {exc}")

        self._safe_log(
            "tx_received",
            tx_id=tx.tx_id,
            origin_node=tx.origin_node,
            accepted=ok,
            result=message,
            from_node=from_node,
            to_node=self._node_id,
        )
        return blockchain_pb2.Ack(ok=ok, message=message)

    def AnnounceBlock(
        self, request: blockchain_pb2.BlockMsg, context: grpc.ServicerContext
    ) -> blockchain_pb2.Ack:
        from_node = _from_node(context)
        try:
            block = block_from_msg(request)
        except ValueError as exc:
            self._safe_log(
                "block_received",
                index=request.index,
                block_hash=request.hash,
                accepted=False,
                reason=str(exc),
                from_node=from_node,
                to_node=self._node_id,
            )
            return blockchain_pb2.Ack(ok=False, message=str(exc))

        try:
            ok, message = _normalize_result(self._on_block(block))
        except Exception as exc:
            logger.exception("on_block handler failed for %s", block.hash)
            return blockchain_pb2.Ack(ok=False, message=f"handler failed: {exc}")

        self._safe_log(
            "block_received",
            index=block.index,
            block_hash=block.hash,
            miner_id=block.miner_id,
            tx_count=len(block.transactions),
            accepted=ok,
            result=message,
            from_node=from_node,
            to_node=self._node_id,
        )
        return blockchain_pb2.Ack(ok=ok, message=message)

    def GetBlocks(
        self, request: blockchain_pb2.BlockRange, context: grpc.ServicerContext
    ) -> Iterator[blockchain_pb2.BlockMsg]:
        if request.from_index < 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"from_index must be non-negative, got {request.from_index}",
            )
        try:
            blocks = list(self._get_blocks(int(request.from_index)))
        except Exception as exc:
            logger.exception("get_blocks handler failed")
            context.abort(grpc.StatusCode.INTERNAL, f"get_blocks failed: {exc}")
        for block in blocks:
            yield block_to_msg(block)

    def GetStatus(
        self, request: blockchain_pb2.Empty, context: grpc.ServicerContext
    ) -> blockchain_pb2.NodeStatusMsg:
        try:
            status = self._get_status()
        except Exception as exc:
            logger.exception("get_status handler failed")
            context.abort(grpc.StatusCode.INTERNAL, f"get_status failed: {exc}")
        return status_to_msg(status)

    def StartWork(
        self, request: blockchain_pb2.WorkAssignmentMsg, context: grpc.ServicerContext
    ) -> blockchain_pb2.Ack:
        if self._start_work is None:
            return blockchain_pb2.Ack(ok=False, message="pool mode is not enabled")
        try:
            assignment = work_assignment_from_msg(request)
        except ValueError as exc:
            return blockchain_pb2.Ack(ok=False, message=str(exc))
        try:
            return _ack(self._start_work(assignment))
        except Exception as exc:
            logger.exception("start_work handler failed for job %s", request.job_id)
            return blockchain_pb2.Ack(ok=False, message=f"handler failed: {exc}")

    def StopWork(
        self, request: blockchain_pb2.JobRef, context: grpc.ServicerContext
    ) -> blockchain_pb2.Ack:
        if self._stop_work is None:
            return blockchain_pb2.Ack(ok=False, message="pool mode is not enabled")
        if not request.job_id.strip():
            return blockchain_pb2.Ack(ok=False, message="job_id must not be empty")
        try:
            return _ack(self._stop_work(request.job_id))
        except Exception as exc:
            logger.exception("stop_work handler failed for job %s", request.job_id)
            return blockchain_pb2.Ack(ok=False, message=f"handler failed: {exc}")


def build_server(
    service: NodeService,
    *,
    host: str = "0.0.0.0",
    port: int = 50051,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> tuple[grpc.Server, int]:
    """Create and bind a gRPC server for ``service``.

    Returns ``(server, bound_port)``. The caller decides when to ``start()`` and
    ``stop()``. Passing ``port=0`` binds an ephemeral port, which tests use.
    """
    if not 0 <= port <= 65535:
        raise ValueError(f"port must be in 0..65535, got {port}")
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")

    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    blockchain_pb2_grpc.add_NodeServiceServicer_to_server(service, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise RuntimeError(f"failed to bind gRPC server to {host}:{port}")
    return server, bound_port
