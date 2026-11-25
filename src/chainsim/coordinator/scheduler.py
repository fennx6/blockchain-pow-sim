"""Pembagian ruang nonce dan manajemen job mode pool (AGENTS.md Bagian 7.2).

Coordinator memecah satu rentang nonce menjadi bagian yang tidak tumpang tindih
dan menutup seluruh rentang, supaya tidak ada worker yang menambang nonce yang
sama dan tidak ada nonce yang terlewat. Saat satu worker mati, rentangnya
dibagikan ulang ke worker yang masih hidup; waktu reassignment inilah yang
diukur pada skenario F1 (Bagian 8).
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass, field

from chainsim.coordinator.registry import NodeRegistry
from chainsim.logging_utils import log_event
from chainsim.types import Block, WorkAssignment

COORDINATOR_ID = "coordinator"


def split_nonce_space(n_workers: int, start: int, size: int) -> list[tuple[int, int]]:
    """Bagi ``[start, start + size)`` menjadi ``n_workers`` rentang berurutan.

    Nilai kembali adalah daftar ``(nonce_start, nonce_end)`` dengan ``nonce_end``
    eksklusif. Sisa pembagian diberikan satu per satu ke worker pertama. Bila
    ``size < n_workers``, sebagian worker menerima rentang kosong
    (``start == end``) dan harus melapor tanpa menambang.
    """
    if n_workers <= 0:
        raise ValueError(f"n_workers harus lebih besar dari 0, bukan {n_workers}")
    if start < 0:
        raise ValueError(f"start tidak boleh negatif, bukan {start}")
    if size < 0:
        raise ValueError(f"size tidak boleh negatif, bukan {size}")

    base, remainder = divmod(size, n_workers)
    ranges: list[tuple[int, int]] = []
    cursor = start
    for index in range(n_workers):
        span = base + (1 if index < remainder else 0)
        ranges.append((cursor, cursor + span))
        cursor += span
    return ranges


@dataclass(frozen=True)
class WorkResult:
    """Laporan satu worker: menemukan nonce valid atau rentangnya habis."""

    job_id: str
    node_id: str
    nonce: int
    found: bool
    block_hash: str = ""


@dataclass
class PoolJob:
    """Satu blok yang sedang ditambang bersama oleh beberapa worker.

    ``assignments`` berupa daftar per worker karena setelah reassignment satu
    worker bisa memegang lebih dari satu rentang.
    """

    job_id: str
    block_template: Block
    nonce_start: int
    nonce_size: int
    assignments: dict[str, list[WorkAssignment]] = field(default_factory=dict)
    results: dict[str, WorkResult] = field(default_factory=dict)
    winner_node: str | None = None
    winner_hash: str = ""
    finished: bool = False

    def workers(self) -> list[str]:
        return sorted(self.assignments)

    def ranges_for(self, node_id: str) -> list[tuple[int, int]]:
        return [(item.nonce_start, item.nonce_end) for item in self.assignments.get(node_id, [])]

    def covers(self, node_id: str, nonce: int) -> bool:
        return any(
            (item.nonce_start <= nonce < item.nonce_end)
            or (item.nonce_start == item.nonce_end and nonce == item.nonce_start)
            for item in self.assignments.get(node_id, [])
        )


class PoolJobManager:
    """Bagi ruang nonce satu blok ke beberapa worker dan tangani pemenangnya."""

    def __init__(self, registry: NodeRegistry, nonce_space: int) -> None:
        if nonce_space < 1:
            raise ValueError(f"nonce_space minimal 1, bukan {nonce_space}")
        self._registry = registry
        self._nonce_space = nonce_space
        self._jobs: dict[str, PoolJob] = {}
        self._active_job_id: str | None = None
        self._counter = 0
        self._lock = threading.Lock()

    @property
    def nonce_space(self) -> int:
        return self._nonce_space

    def active_job(self) -> PoolJob | None:
        with self._lock:
            if self._active_job_id is None:
                return None
            return self._jobs[self._active_job_id]

    def get(self, job_id: str) -> PoolJob:
        with self._lock:
            return self._require(job_id)

    def start_job(
        self,
        block_template: Block,
        worker_ids: Sequence[str] | None = None,
    ) -> PoolJob:
        """Buat job baru dan bagikan seluruh ruang nonce ke worker.

        ``worker_ids`` kosong berarti memakai semua node yang terdaftar di
        registry. Job sebelumnya harus sudah selesai agar tidak ada dua job yang
        menambang blok berbeda secara bersamaan.
        """
        with self._lock:
            workers = list(worker_ids) if worker_ids is not None else self._registry.node_ids()
            if not workers:
                raise ValueError("tidak ada worker untuk job pool")
            if len(set(workers)) != len(workers):
                raise ValueError(f"worker duplikat: {workers}")
            active = self._jobs.get(self._active_job_id) if self._active_job_id else None
            stale_job_id: str | None = None
            if active is not None and not active.finished:
                # A worker can submit a template on the old tip just before the
                # winner's block arrives. That stale job would otherwise stay
                # active forever and block every later SubmitTemplate.
                if block_template.index > active.block_template.index:
                    active.finished = True
                    stale_job_id = active.job_id
                else:
                    raise RuntimeError(f"job {active.job_id} masih berjalan")

            self._counter += 1
            job_id = f"job-{block_template.index}-{self._counter}"
            ranges = split_nonce_space(len(workers), 0, self._nonce_space)
            assignments = {
                worker: [WorkAssignment(job_id, block_template, start, end)]
                for worker, (start, end) in zip(workers, ranges, strict=True)
            }
            job = PoolJob(
                job_id=job_id,
                block_template=block_template,
                nonce_start=0,
                nonce_size=self._nonce_space,
                assignments=assignments,
            )
            self._jobs[job_id] = job
            self._active_job_id = job_id

        if stale_job_id is not None:
            log_event(
                "job_finished",
                COORDINATOR_ID,
                job_id=stale_job_id,
                reason="stale_template",
            )
        log_event(
            "job_assigned",
            COORDINATOR_ID,
            job_id=job_id,
            block_index=block_template.index,
            workers=workers,
            nonce_space=self._nonce_space,
        )
        return job

    def record_result(
        self,
        job_id: str,
        node_id: str,
        nonce: int,
        block_hash: str = "",
        found: bool = True,
    ) -> WorkResult:
        """Catat laporan worker; nonce di luar rentangnya dianggap tidak sah."""
        exhausted_all = False
        with self._lock:
            job = self._require(job_id)
            if job.finished:
                raise RuntimeError(f"job {job_id} sudah selesai")
            if node_id not in job.assignments:
                raise ValueError(f"node {node_id!r} bukan worker job {job_id}")
            if not job.covers(node_id, nonce):
                raise ValueError(
                    f"nonce {nonce} di luar rentang node {node_id}: {job.ranges_for(node_id)}"
                )
            result = WorkResult(
                job_id=job_id,
                node_id=node_id,
                nonce=nonce,
                found=found,
                block_hash=block_hash,
            )
            job.results[node_id] = result
            if found:
                job.winner_node = node_id
                job.winner_hash = block_hash
                job.finished = True
            elif len(job.results) == len(job.assignments) and all(
                len(items) == 1 for items in job.assignments.values()
            ):
                # No worker has a second (reassigned) range, so every range is
                # exhausted. Finish the job instead of blocking all future jobs.
                job.finished = True
                exhausted_all = True

        if found:
            log_event(
                "job_done",
                COORDINATOR_ID,
                job_id=job_id,
                winner=node_id,
                nonce=nonce,
                block_hash=block_hash,
            )
        elif exhausted_all:
            log_event(
                "job_finished",
                COORDINATOR_ID,
                job_id=job_id,
                reason="all_exhausted",
            )
        return result

    def reassign(self, job_id: str, dead_node_id: str) -> list[WorkAssignment]:
        """Bagikan ulang rentang nonce worker mati ke worker yang masih hidup.

        Mengembalikan daftar assignment baru yang harus dikirim ke worker.
        Job yang sudah selesai tidak diubah.
        """
        grouped = self.reassign_grouped(job_id, dead_node_id)
        return [assignment for items in grouped.values() for assignment in items]

    def reassign_grouped(
        self, job_id: str, dead_node_id: str
    ) -> dict[str, list[WorkAssignment]]:
        """Sama seperti :meth:`reassign`, tetapi hasilnya dikelompokkan per worker.

        Coordinator memakai bentuk ini untuk mengirim setiap potongan rentang ke
        worker yang tepat tanpa menebak dari ``WorkAssignment`` saja.
        """
        grouped: dict[str, list[WorkAssignment]] = {}
        with self._lock:
            job = self._require(job_id)
            if job.finished:
                return grouped
            lost = job.assignments.pop(dead_node_id, [])
            if not lost:
                return grouped
            alive = sorted(job.assignments)
            if alive:
                for lost_item in lost:
                    size = lost_item.nonce_end - lost_item.nonce_start
                    if size <= 0:
                        continue
                    pieces = split_nonce_space(len(alive), lost_item.nonce_start, size)
                    for worker, (start, end) in zip(alive, pieces, strict=True):
                        assignment = WorkAssignment(job_id, job.block_template, start, end)
                        job.assignments[worker].append(assignment)
                        grouped.setdefault(worker, []).append(assignment)
            lost_span = sum(item.nonce_end - item.nonce_start for item in lost)

        log_event(
            "job_reassigned",
            COORDINATOR_ID,
            job_id=job_id,
            dead_node=dead_node_id,
            lost_span=lost_span,
            targets=sorted(
                {item.nonce_start for items in grouped.values() for item in items}
            ),
        )
        return grouped

    def finish(self, job_id: str, reason: str = "block_received") -> None:
        """Tutup job tanpa pemenang, misalnya karena blok datang dari node lain."""
        with self._lock:
            job = self._require(job_id)
            if job.finished:
                return
            job.finished = True
        log_event("job_finished", COORDINATOR_ID, job_id=job_id, reason=reason)

    def _require(self, job_id: str) -> PoolJob:
        job = self._jobs.get(job_id)
        if job is None:
            raise ValueError(f"job tidak dikenal: {job_id!r}")
        return job
