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
