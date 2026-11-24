# Simulasi Blockchain Terdistribusi (chainsim)

Proyek UAS Komputasi Paralel dan Terdistribusi, Tema 4: simulasi blockchain
terdistribusi dengan Proof-of-Work, konsensus rantai terpanjang, dan pemulihan
setelah node gagal. Dibangun dengan Flask, gRPC, dan Docker.

> Status: implementasi inti selesai. Core blockchain, PoW, gRPC/gossip/sync,
> coordinator, dashboard, dan skrip E1-E4 sudah ada dan saling terhubung.
> Alur multi-node (transaksi, mining, propagasi blok, catch-up setelah restart)
> sudah diuji in-process; verifikasi 6 node lewat Docker Compose menyusul.
> Angka eksperimen resmi baru diisi setelah run nyata di
> `experiments/results/`.

## Prasyarat

- Python 3.11 atau 3.12
- Docker Desktop dengan Docker Compose v2
- Windows PowerShell (contoh di bawah) atau shell POSIX

## Dependensi

| Paket | Dipakai untuk |
|---|---|
| Flask | REST per node dan dashboard coordinator |
| grpcio, grpcio-tools, protobuf | komunikasi antarnode |
| PyYAML | `config/settings.yaml` |
| pytest, ruff | pengujian dan kualitas kode |
| pandas, matplotlib | analisis dan grafik di `experiments/` |

Versi di `requirements.txt` masih rentang; pin dengan `pip freeze` setelah
instalasi pertama berhasil.

## Instalasi lokal

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .
```

## Menjalankan jaringan

```powershell
docker compose up --build -d
docker compose ps
```

Jaringan default: 1 coordinator + 6 node pada network `chainnet`. Setiap node
punya volume sendiri (`node-N-data`) sehingga rantai bertahan setelah restart.
Port node hanya ada di dalam network; host hanya membuka dashboard.

Coordinator membuka gRPC di port `50052` (internal `chainnet`). Node menerima
alamatnya dari `COORDINATOR_GRPC=coordinator:50052` yang sudah diisi di
`docker-compose.yml`; tanpa itu mode pool tidak bisa melapor ke coordinator.
Image Docker membangkitkan kode gRPC dari `proto/` saat build, jadi clone
bersih tidak perlu menjalankan `scripts/gen_proto.ps1` lebih dulu untuk
Docker.

```powershell
docker compose logs -f coordinator
docker compose down            # tambah -v hanya jika ingin menghapus volume rantai
```

## Dashboard

<http://127.0.0.1:8080>

| Endpoint | Isi |
|---|---|
| `GET /` | tabel status node dan kejadian terakhir |
| `GET /api/nodes` | status semua node dalam JSON |
| `GET /api/events?limit=50` | kejadian terbaru dari `logs/*.jsonl` |
| `GET /metrics` | metrik format Prometheus |

Dashboard hanya menampilkan data yang benar-benar dibaca dari node. Node yang
tidak terjangkau ditandai `down` dan metriknya dikosongkan, bukan diisi angka
contoh.

## Transaksi dan injeksi kegagalan

```powershell
# Port REST node tidak dipublikasikan ke host, jadi generator dijalankan
# di dalam network terhadap node-1.
docker compose exec node-1 python scripts/tx_generator.py `
  --rate 5 --duration 60 --target http://node-1:8081/tx

python scripts/fault_inject.py stop --service node-3
python scripts/fault_inject.py start --service node-3
python scripts/fault_inject.py remove-volume --service node-3
python scripts/fault_inject.py --dry-run disconnect --service node-3
```

Setiap aksi injeksi dicatat sebagai event `fault_injected` di
`logs/fault_injector.jsonl` agar waktu deteksi dan pemulihan bisa dihitung.

## Eksperimen

```powershell
python experiments/e3_fault.py --dry-run
python experiments/e4_scalability.py --dry-run
python experiments/compose_gen.py --nodes 16 --mode pool
python experiments/tx_log.py --logs-dir logs
python experiments/plot.py
```

`tx_log.py` menggabungkan `logs/*.jsonl` menjadi
`experiments/results/tx_log.csv` (`ts_ms, tx_id, from_node, to_node, event`)
dan mencetak timeline satu transaksi dari node asal ke peer, ditambah konteks
blok sesudahnya. Gunakan `--tx-id <id>` untuk memilih transaksi tertentu.

`compose_gen.py` menurunkan file compose untuk N node (2/4/8/16) dari
`docker-compose.yml` ke `experiments/.generated/` (tidak di-commit), sehingga
setiap node tetap punya nama dan volume sendiri.

Hasil ringkas disimpan di `experiments/results/*.csv` dan grafiknya `*.png`.
Belum ada hasil eksperimen yang dijalankan; angka di laporan nanti hanya boleh
berasal dari folder itu.

## Struktur repo

```
config/settings.yaml          konfigurasi tunggal (difficulty, port, timeout)
proto/                        kontrak protobuf (anggota B)
src/chainsim/types.py         dataclass kontrak antarmodul
src/chainsim/core/            transaksi, blok, PoW, rantai, mempool, storage (A)
src/chainsim/node/            REST, miner, gRPC, gossip, sync (A dan B)
src/chainsim/coordinator/     registry, heartbeat, scheduler, metrik, dashboard (C)
docker/                       Dockerfile node dan coordinator
docker-compose.yml            1 coordinator + 6 node
scripts/                      pembangkit transaksi, injeksi kegagalan, generate proto
experiments/                  skrip E1-E4, metrik, grafik, hasil
tests/                        unit test
logs/                         log JSONL runtime (tidak di-commit)
evidence/                     tangkapan layar dan potongan log untuk laporan
docs/                         arsitektur, keputusan, lingkungan lab, draf laporan
```

## Konfigurasi

Semua angka ada di `config/settings.yaml` dan dapat ditimpa environment
(`DIFFICULTY`, `MINING_MODE`, `NODE_REST_PORT`, `NODE_GRPC_PORT`, `PEERS`,
`NODES`, `COORDINATOR_GRPC`, `COORDINATOR_GRPC_PORT`, `HEARTBEAT_INTERVAL_S`,
`LOG_DIR`, dan seterusnya). Salin `.env.example` menjadi `.env` untuk
pengaturan lokal; `.env` tidak di-commit.

## Pengujian

```powershell
ruff check .
ruff format --check .
pytest
pytest -m integration         # butuh Docker berjalan
```

## Keamanan

- Hanya dashboard yang dipublikasikan, dan hanya ke `127.0.0.1`.
- Flask berjalan dengan `debug=False` di dalam container.
- Tidak ada kredensial, dompet nyata, atau koneksi ke jaringan blockchain publik.
- Input `POST /tx` wajib divalidasi di sisi server (Bagian 10).

## Dokumen terkait

- `docs/architecture.md` - komponen, topologi, alur konsensus
- `docs/decisions.md` - keputusan teknis dan pertanyaan terbuka
- `docs/lab-environment.md` - spesifikasi mesin eksperimen final
- `docs/video-script.md` - rencana video demo
