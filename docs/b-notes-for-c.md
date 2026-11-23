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

## Fitur 5 (rencana): CoordinatorService dan mode pool

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
