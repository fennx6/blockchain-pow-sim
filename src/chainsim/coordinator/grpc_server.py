"""Server gRPC coordinator: registry, heartbeat, dan orkestrasi job pool.

Owner: Member C (AGENTS.md Sections 3, 6.3, and 7.2). The coordinator is not a
block authority: this module only registers nodes, records pool work results,
and forwards ``StartWork``/``StopWork`` to workers. Validation and fork choice
stay in ``core`` on each node.

The service is transport-only. Reassignment decisions belong to
``PoolJobManager``; this module just delivers the assignments it returns.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor

import grpc

from chainsim.coordinator.registry import NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager
from chainsim.generated import blockchain_pb2, blockchain_pb2_grpc
from chainsim.logging_utils import log_event
from chainsim.node.convert import (
    block_from_msg,
    node_info_from_msg,
    peer_list_to_msg,
    work_result_from_msg,
)
from chainsim.node.grpc_client import NodeClient
from chainsim.types import WorkAssignment

logger = logging.getLogger(__name__)

COORDINATOR_ID = "coordinator"
DEFAULT_GRPC_PORT = 50052
DEFAULT_MAX_WORKERS = 8
DEFAULT_TIMEOUT_S = 2.0

# A factory keeps the servicer testable without opening real channels.
NodeClientFactory = Callable[[str], NodeClient]


def grpc_port_from_env(env: Mapping[str, str] | None = None) -> int:
    """Read ``COORDINATOR_GRPC_PORT`` with a validated default.

    The port lives here instead of ``config/settings.yaml`` because that file is
    shared by all three members; adding the key there needs a reviewed PR.
    """
    source = os.environ if env is None else env
    raw = (source.get("COORDINATOR_GRPC_PORT") or "").strip()
    if not raw:
        return DEFAULT_GRPC_PORT
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError(f"COORDINATOR_GRPC_PORT harus bilangan bulat, bukan {raw!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"COORDINATOR_GRPC_PORT harus di rentang 1..65535, bukan {port}")
    return port


class CoordinatorService(blockchain_pb2_grpc.CoordinatorServiceServicer):
    """Implementasi ``CoordinatorService`` di atas registry dan job manager."""

    def __init__(
        self,
        registry: NodeRegistry,
        jobs: PoolJobManager,
        *,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        client_factory: NodeClientFactory | None = None,
    ) -> None:
        if timeout_s <= 0:
            raise ValueError(f"timeout_s harus > 0, bukan {timeout_s!r}")
        self._registry = registry
        self._jobs = jobs
        self._timeout_s = timeout_s
        self._client_factory: NodeClientFactory = client_factory or (
            lambda address: NodeClient(address, timeout_s=self._timeout_s)
        )

    # ------------------------------------------------------------------ RPC
    def Register(self, request, context):  # noqa: N802 - nama RPC
        try:
            record = node_info_from_msg(request)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        self._registry.register(record)
        log_event(
            "node_registered",
            COORDINATOR_ID,
            target=record.node_id,
            grpc_address=record.grpc_address,
            rest_url=record.rest_url,
        )
        return peer_list_to_msg(self._registry.all())

    def Heartbeat(self, request, context):  # noqa: N802 - nama RPC
        node_id = (request.node_id or "").strip()
        if not node_id or self._registry.get(node_id) is None:
            return blockchain_pb2.HeartbeatReply(ok=False)
        return blockchain_pb2.HeartbeatReply(ok=True, peers=self._peer_list().nodes)

    def GetPeers(self, request, context):  # noqa: N802 - nama RPC
        return self._peer_list()

    def SubmitWorkResult(self, request, context):  # noqa: N802 - nama RPC
        try:
            result = work_result_from_msg(request)
            self._jobs.record_result(
                result.job_id,
                result.node_id,
                result.nonce,
                block_hash=result.block_hash,
                found=result.found,
            )
        except (ValueError, RuntimeError) as exc:
            return blockchain_pb2.Ack(ok=False, message=str(exc))

        if not result.found:
            return blockchain_pb2.Ack(ok=True, message="recorded")
        failures = self._stop_other_workers(result.job_id, result.node_id)
        message = "recorded" if not failures else f"recorded; {', '.join(failures)}"
        return blockchain_pb2.Ack(ok=True, message=message)

    def SubmitTemplate(self, request, context):  # noqa: N802 - nama RPC
        node_id = (request.node_id or "").strip()
        if not node_id:
            return blockchain_pb2.JobStartedMsg(accepted=False, message="node_id kosong")
        if self._registry.get(node_id) is None:
            return blockchain_pb2.JobStartedMsg(
                accepted=False, message=f"node tidak terdaftar: {node_id!r}"
            )
        try:
            template = block_from_msg(request.block_template)
            job = self._jobs.start_job(template)
        except (ValueError, RuntimeError) as exc:
            return blockchain_pb2.JobStartedMsg(accepted=False, message=str(exc))

        total = sum(len(items) for items in job.assignments.values())
        failures = self.dispatch(job.job_id, job.assignments)
        if failures and len(failures) >= total:
            # Tidak ada worker yang menerima tugas: tutup job agar job
            # berikutnya tidak tertahan oleh job aktif yang mustahil selesai.
            self._jobs.finish(job.job_id, reason="dispatch_failed")
            return blockchain_pb2.JobStartedMsg(
                accepted=False, job_id=job.job_id, message="; ".join(failures)
            )
        return blockchain_pb2.JobStartedMsg(
            accepted=True, job_id=job.job_id, message="; ".join(failures)
        )

    # ------------------------------------------------------------- helpers
    def dispatch(
        self,
        job_id: str,
        assignments: Mapping[str, Sequence[WorkAssignment]],
    ) -> list[str]:
        """Kirim ``StartWork`` ke setiap worker; kembalikan daftar kegagalan."""
        failures: list[str] = []
        for node_id, items in assignments.items():
            record = self._registry.get(node_id)
            if record is None:
                failures.append(f"{node_id}: node tidak terdaftar")
                continue
            try:
                with self._client_factory(record.grpc_address) as client:
                    for assignment in items:
                        result = client.start_work(assignment)
                        if not result.ok:
                            failures.append(f"{node_id}: {result.message}")
            except Exception as exc:  # noqa: BLE001 - satu worker gagal tidak boleh menghentikan job
                failures.append(f"{node_id}: {exc}")
        for failure in failures:
            log_event(
                "job_dispatch_failed",
                COORDINATOR_ID,
                job_id=job_id,
                detail=failure,
            )
        return failures

    def _stop_other_workers(self, job_id: str, winner: str) -> list[str]:
        job = self._jobs.get(job_id)
        failures: list[str] = []
        for node_id in job.workers():
            if node_id == winner:
                continue
            record = self._registry.get(node_id)
            if record is None:
                continue
            try:
                with self._client_factory(record.grpc_address) as client:
                    result = client.stop_work(job_id)
                    if not result.ok:
                        failures.append(f"{node_id}: {result.message}")
            except Exception as exc:  # noqa: BLE001 - worker mati bukan alasan menggagalkan pemenang
                failures.append(f"{node_id}: {exc}")
        return failures

    def _peer_list(self) -> blockchain_pb2.PeerList:
        return peer_list_to_msg(self._registry.all())


def build_server(
    service: CoordinatorService,
    *,
    host: str = "0.0.0.0",
    port: int = DEFAULT_GRPC_PORT,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> tuple[grpc.Server, int]:
    """Buat server gRPC coordinator; ``port=0`` memilih port bebas (untuk test)."""
    if max_workers < 1:
        raise ValueError(f"max_workers minimal 1, bukan {max_workers}")
    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    blockchain_pb2_grpc.add_CoordinatorServiceServicer_to_server(service, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise RuntimeError(f"gagal bind gRPC coordinator ke {host}:{port}")
    return server, bound_port
