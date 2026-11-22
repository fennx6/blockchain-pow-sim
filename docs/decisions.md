# Keputusan teknis

Catatan keputusan yang memengaruhi lebih dari satu anggota. Setiap entri baru
ditambahkan lewat PR yang ditinjau dua anggota lain (AGENTS.md Bagian 3).

## Fase 0 - inisialisasi struktur (2026-11-22)

### D1. Transport log JSONL: bind mount `./logs`

`log_event` menulis `logs/<node_id>.jsonl` di dalam container. Agar log bisa
dibaca coordinator dan dianalisis setelah container dimatikan, `./logs` di host
di-mount ke `/app/logs` pada semua service. Alternatif (menulis ke stdout lalu
`docker compose logs`) ditolak karena tidak menghasilkan JSONL yang bisa
di-parse per kejadian.

### D2. Jalur transaksi dari host

Hanya dashboard coordinator yang dipublikasikan ke host (`127.0.0.1:8080`),
sesuai Bagian 10. Akibatnya `POST /tx` milik node tidak bisa dipanggil langsung
dari laptop. Rencana: coordinator meneruskan `POST /tx` ke node yang dipilih.
**Belum diimplementasikan**; perlu kesepakatan A (pemilik `node/api.py`) dan B
(pemilik gossip) sebelum ditulis.

### D3. Compose default memakai 6 service bernama

`docker-compose.yml` mendefinisikan `node-1` sampai `node-6` dengan volume
bernama per node, karena skenario F2/F3 membutuhkan identitas dan volume yang
stabil. Konsekuensinya `docker compose up --scale node=N` tidak dipakai untuk
E4; skrip E4 akan membangkitkan compose untuk N node dari template. Perubahan
mekanisme ini perlu disetujui C (pemilik) dan ditinjau A/B karena menyangkut
cara pengambilan data E4.

### D4. Heartbeat memakai REST `GET /status`

`CoordinatorService.Heartbeat` (gRPC) belum ada karena `proto/blockchain.proto`
belum ditulis B. Agar deteksi kegagalan bisa diuji lebih awal, heartbeat
memakai endpoint REST yang sudah wajib ada (Bagian 6.4). Saat gRPC siap,
`HeartbeatMonitor` tetap dipertahankan dan RPC cukup memanggil
`NodeRegistry.register` / `mark`.

### D5. Difficulty default 4

Nilai awal di `config/settings.yaml`; belum dikalibrasi. Target Bagian 9.1
(satu blok dengan 1 worker selesai dalam beberapa detik) harus diukur di mesin
final dan hasilnya dicatat di sini sebelum eksperimen dijalankan.

### D6. Identitas node

`NODE_ID` dari environment; bila kosong dipakai hostname container. Ini menjaga
`docker compose up --scale node=N` tetap memberi identitas unik tanpa variabel
per replika.

### D7. AGENTS.md tidak di-commit

`AGENTS.md` masuk `.gitignore` atas permintaan tim agar tidak pernah ter-push.
File tetap ada di mesin lokal sebagai panduan kerja.

### D8. Port

Node: REST `8081` dan gRPC `50051` di dalam network `chainnet`, tidak
dipublikasikan. Coordinator: `8080` dipublikasikan hanya ke `127.0.0.1`.

## Daftar tugas per anggota setelah Fase 0

- **A:** `core/*`, `node/api.py`, `node/miner.py`, `experiments/e1_speedup.py`;
  menyediakan `GET /status` sesuai `NodeStatus` agar dashboard C punya data.
- **B:** `proto/blockchain.proto`, `scripts/gen_proto.*`, `node/main.py`,
  `grpc_server.py`, `grpc_client.py`, `gossip.py`, `sync.py`,
  `scripts/tx_generator.py`, `experiments/e2_propagation.py`; menambah
  `CoordinatorService` di sisi server.
- **C:** melengkapi orkestrasi `experiments/e3_fault.py` dan
  `experiments/e4_scalability.py`, kalibrasi difficulty, `docs/lab-environment.md`,
  serta dashboard bila data dari node sudah tersedia.

## Pertanyaan terbuka

1. Apakah `POST /tx` dilayani coordinator (D2) atau port node dipublikasikan?
2. Apakah E4 boleh memakai compose yang dibangkitkan (D3) atau harus `--scale`?
3. Bentuk penyimpanan cabang samping untuk `AddResult.STORED_SIDE` (milik A).
