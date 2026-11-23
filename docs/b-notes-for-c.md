# Catatan B untuk C (file milik C)

Dokumen ini adalah catatan koordinasi. B tidak mengubah file milik C; patch di
bawah diterapkan dan direview oleh C.

## Fitur 1: generate gRPC saat build image node

**Masalah.** `src/chainsim/generated/*_pb2*.py` di-gitignore, sedangkan
`docker/Dockerfile.node` belum menyalin `proto/` atau menjalankan generator.
Image yang dibangun dari clone bersih akan gagal saat
`import chainsim.generated`.

**Usulan patch `docker/Dockerfile.node`.** Ganti blok `COPY`/`RUN` yang ada
dengan urutan berikut:

```dockerfile
COPY requirements.txt pyproject.toml ./
COPY proto ./proto
COPY scripts/gen_proto.py ./scripts/gen_proto.py
COPY src ./src
COPY config ./config

RUN pip install --no-cache-dir -r requirements.txt \
    && python scripts/gen_proto.py \
    && pip install --no-cache-dir --no-deps .
```

`grpc_tools` sudah ada di `requirements.txt`, jadi generator bisa dijalankan
setelah `pip install -r requirements.txt`. `scripts/gen_proto.py` menghitung
root repo dari lokasi file (`/app`), sehingga default `proto/` dan
`src/chainsim/generated/` benar di dalam image.

**Untuk pengembangan lokal.** Setiap anggota menjalankan
`.\scripts\gen_proto.ps1` (Windows) atau `./scripts/gen_proto.sh` (POSIX)
sebelum `pytest`, karena file hasil generate tidak di-commit.

## Fitur 3: event gossip dan sync untuk E2/E3

`node/gossip.py` menulis satu event `tx_forwarded` per peer (field `tx_id`,
`origin_node`, `from_node`, `to_node`, `ok`, `error`) dan event tambahan
`block_forwarded` per peer (field `index`, `block_hash`, `from_node`, `to_node`,
`ok`, `error`). `block_forwarded` bukan event baku di AGENTS.md 9.2; usul D9
diperluas agar E2 bisa memisahkan "blok dikirim" dari "blok diterima".

`node/sync.py` menulis `sync_start` (`peer`, `local_height`, `peer_height`) dan
`sync_done` (`peer`, `from_height`, `to_height`, `blocks_pulled`,
`blocks_added`, `fork_replaced`). `catch_up` mengembalikan jumlah blok yang
ditarik, termasuk fetch rantai penuh saat peer berada di fork yang lebih berat.

**Wiring yang dibutuhkan di `node/main.py` (fitur 4, milik B):**

- `on_tx_received=gossip.broadcast_tx` pada `api.create_app`.
- `on_block_mined=gossip.broadcast_block` pada `Miner`.
- Handler `on_block` di `NodeService`: `chain.add_block(block)`; jika
  `ORPHAN` panggil `sync_service.sync_now([sender])` lalu
  `miner.notify_new_block()` bila tip berubah; jika `ACCEPTED` atau
  `FORK_SWITCHED` panggil `miner.notify_new_block()`.
- `SyncService` periodik memakai `heartbeat_interval_s` sebagai interval agar
  node yang restart mengejar rantai tanpa menunggu blok baru.
- Saat shutdown, panggil `gossip.close()` dan `sync_service.stop()` agar channel
  gRPC dan thread latar belakang berhenti bersih.

## Fitur 4: pemenang pool menyiarkan blok sendiri

`NodeRuntime.handle_work_result` sekarang menambahkan blok pemenang ke rantai
lokal, memanggil `miner.notify_new_block()`, lalu `gossip.broadcast_block`
sebelum melapor ke coordinator. Blok yang tidak masuk rantai aktif (invalid,
orphan, side branch, duplikat) tidak dilaporkan sebagai pemenang. Coordinator
**tidak perlu** menyiarkan blok; tugasnya hanya mencatat hasil dan mengirim
`StopWork` ke worker lain. Ini menghindari perubahan proto karena
`WorkResultMsg` tidak membawa blok penuh.

## Fitur 5: CoordinatorService dan mode pool

Proto sudah memuat `CoordinatorService.SubmitTemplate` (lihat komentar D13 di
`proto/blockchain.proto`). Agar fitur ini bisa dijalankan, C perlu menyiapkan:

1. **Port gRPC coordinator** (usul `50052`) di `docker-compose.yml` dan
   `docker/Dockerfile.coordinator`, plus env `COORDINATOR_GRPC_PORT=50052` dan
   `COORDINATOR_GRPC=coordinator:50052` untuk node.
2. **`coordinator/main.py`** menjalankan server gRPC `CoordinatorService`
   (adapter B) di port tersebut.
3. **Alur pool**: node mengirim `SubmitTemplate` berisi template blok; adapter
   memanggil `PoolJobManager.start_job`, lalu coordinator memanggil
   `NodeService.StartWork` ke semua worker dan `StopWork` saat pemenang sudah
   ada.
4. **`docs/decisions.md`** perlu entri D13 untuk keputusan `SubmitTemplate` dan
   D14 untuk join log E2 (`block_mined` ↔ `block_received` lewat `block_hash`).

### Kerangka adapter C (`src/chainsim/coordinator/grpc_server.py`)

Modul ini milik C; B hanya menyediakan kerangka yang memakai kontrak B yang
sudah ada (`NodeClient`, `node_info_from_msg`, `node_info_to_msg`,
`block_from_msg`, `work_result_from_msg`).

```python
from concurrent.futures import ThreadPoolExecutor

import grpc

from chainsim.coordinator.registry import NodeRegistry
from chainsim.coordinator.scheduler import PoolJobManager
from chainsim.generated import blockchain_pb2, blockchain_pb2_grpc
from chainsim.node.convert import (
    block_from_msg,
    node_info_from_msg,
    node_info_to_msg,
    work_result_from_msg,
)
from chainsim.node.grpc_client import NodeClient

COORDINATOR_ID = "coordinator"
DEFAULT_MAX_WORKERS = 8


class CoordinatorService(blockchain_pb2_grpc.CoordinatorServiceServicer):
    def __init__(
        self,
        registry: NodeRegistry,
        jobs: PoolJobManager,
        *,
        timeout_s: float = 2.0,
    ) -> None:
        self._registry = registry
        self._jobs = jobs
        self._timeout_s = timeout_s

    def _peer_list(self) -> blockchain_pb2.PeerList:
        return blockchain_pb2.PeerList(
            nodes=[node_info_to_msg(record) for record in self._registry.all()]
        )

    def Register(self, request, context):
        self._registry.register(node_info_from_msg(request))
        return self._peer_list()

    def Heartbeat(self, request, context):
        if self._registry.get(request.node_id) is None:
            return blockchain_pb2.HeartbeatReply(ok=False)
        return blockchain_pb2.HeartbeatReply(ok=True, peers=self._peer_list().nodes)

    def GetPeers(self, request, context):
        return self._peer_list()

    def SubmitWorkResult(self, request, context):
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
        if result.found:
            self._stop_other_workers(result.job_id, result.node_id)
        return blockchain_pb2.Ack(ok=True, message="recorded")

    def SubmitTemplate(self, request, context):
        if self._registry.get(request.node_id) is None:
            return blockchain_pb2.JobStartedMsg(
                accepted=False, message=f"unknown node: {request.node_id!r}"
            )
        try:
            template = block_from_msg(request.block_template)
            job = self._jobs.start_job(template)
        except (ValueError, RuntimeError) as exc:
            return blockchain_pb2.JobStartedMsg(accepted=False, message=str(exc))

        failures: list[str] = []
        for node_id, assignments in job.assignments.items():
            record = self._registry.get(node_id)
            if record is None:
                failures.append(f"{node_id}: unknown")
                continue
            with NodeClient(record.grpc_address, timeout_s=self._timeout_s) as client:
                for assignment in assignments:
                    ack = client.start_work(assignment)
                    if not ack.ok:
                        failures.append(f"{node_id}: {ack.message}")
        return blockchain_pb2.JobStartedMsg(
            accepted=True,
            job_id=job.job_id,
            message="; ".join(failures),
        )

    def _stop_other_workers(self, job_id: str, winner: str) -> None:
        job = self._jobs.get(job_id)
        for node_id in job.workers():
            if node_id == winner:
                continue
            record = self._registry.get(node_id)
            if record is None:
                continue
            with NodeClient(record.grpc_address, timeout_s=self._timeout_s) as client:
                client.stop_work(job_id)


def build_server(
    service: CoordinatorService,
    *,
    host: str = "0.0.0.0",
    port: int = 50052,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> tuple[grpc.Server, int]:
    server = grpc.server(ThreadPoolExecutor(max_workers=max_workers))
    blockchain_pb2_grpc.add_CoordinatorServiceServicer_to_server(service, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise RuntimeError(f"failed to bind coordinator gRPC to {host}:{port}")
    return server, bound_port
```

Catatan untuk C:
- `PoolJobManager.start_job` memakai semua node di registry. Daftarkan node
  lebih dulu (lewat `Register` atau `NodeRegistry.from_config`) sebelum
  `SubmitTemplate` pertama.
- Worker yang kehabisan rentang nonce melapor `found=False`; dengan
  `nonce_space` default 1e9 dan difficulty demo, ini jarang terjadi. Jika ingin
  menangani kasus itu, tambahkan reassignment di C.
- `CoordinatorService` tidak memvalidasi rantai. Validitas tetap di node
  (AGENTS.md 7.1).
- Di `coordinator/main.py`, jalankan server gRPC di thread/latar dan Flask
  dashboard di thread utama (atau sebaliknya), lalu hentikan keduanya saat
  shutdown.

## Fitur 6: E2 propagation, latency, throughput

`experiments/e2_propagation.py` sudah membaca `logs/*.jsonl` dan menghitung:

- `tx_forward_ms_*` dari `tx_forwarded` ke `tx_received`.
- `block_forward_ms_*` dari `block_forwarded` ke `block_received`.
- `block_propagation_ms_*` dari `block_mined` ke `block_received` tiap node.
- `block_propagation_full_coverage_ms_*` dan `coverage_ratio`.
- `block_throughput_bps`, `tx_throughput_tps`, `blocks_mined`.
- `grpc_rtt_ms_*` hanya jika `--grpc-target host:port` diberikan.

`scripts/tx_generator.py` sudah mengirim transaksi fiktif ke satu target REST
dengan laju yang diminta.

Yang masih dibutuhkan dari C untuk run final E2:

1. **Akses gRPC dari host.** Port gRPC node saat ini tidak dipublikasikan.
   Pilihan yang disarankan: jalankan E2 di dalam network `chainnet` (satu-off
   container atau `docker compose exec`), bukan membuka port ke host.
2. **`experiments/` dan `scripts/` di image.** `docker/Dockerfile.node` hanya
   menyalin `scripts/gen_proto.py`; tambahkan `COPY scripts ./scripts` dan
   `COPY experiments ./experiments` (atau mount repo) agar generator dan E2
   bisa dijalankan di container.
3. **Proxy `POST /tx` coordinator (D2)** atau targetkan REST node langsung
   (`http://node-1:8081/tx`). Default `tx_generator.py` masih ke coordinator.
4. **Event `block_forwarded`** (usul D9) agar `block_forward_ms` terpisah dari
   `block_propagation_ms`.

Contoh urutan run (setelah patch di atas):

```bash
docker compose up -d --build
docker compose exec node-1 python scripts/tx_generator.py \
  --rate 5 --duration 60 --target http://node-1:8081/tx
docker compose exec node-1 python experiments/e2_propagation.py \
  --logs-dir /app/logs \
  --nodes node-1,node-2,node-3,node-4,node-5,node-6 \
  --grpc-target node-1:50051 --grpc-target node-2:50051 \
  --grpc-target node-3:50051 --grpc-target node-4:50051 \
  --grpc-target node-5:50051 --grpc-target node-6:50051 \
  --duration 60
```

Hasil ditulis ke `experiments/results/e2_propagation.csv`; belum ada angka
final sampai run Docker 6 node dijalankan.
