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

### D9. Manajemen job mode pool ada di `coordinator/scheduler.py`

`PoolJobManager` membagi satu ruang nonce ke worker, memvalidasi bahwa nonce
yang dilaporkan memang berada di rentang worker tersebut, menetapkan pemenang,
dan membagikan ulang rentang worker yang mati (`reassign`). Ini yang diukur
sebagai waktu reassignment pada F1.

Event tambahan di luar daftar baku Bagian 9.2: `job_reassigned` dan
`job_finished`. Keduanya diperlukan untuk menghitung waktu pemulihan dari log;
daftar baku tidak memuat event reassignment.

### D10. Transport gRPC coordinator menunggu proto milik B

Logika coordinator (registry, heartbeat, penjadwalan job pool, pengumpulan
metrik) sudah terpisah dari transport. Server `CoordinatorService`
(Register/Heartbeat/SubmitWorkResult/GetPeers) belum ditulis karena
`proto/blockchain.proto` belum ada; menulis adapter protobuf sekarang berarti
menebak nama message milik B. Setelah proto disepakati, adapter cukup memanggil
`NodeRegistry` dan `PoolJobManager` yang sudah ada.

### D11. Generator compose untuk E4

`experiments/compose_gen.py` menurunkan compose N node dari template
`docker-compose.yml` sehingga image, environment, `cpus`, dan port dashboard
tetap sama dengan konfigurasi default, sementara setiap node mendapat nama dan
volume sendiri. Hasil generate ditulis ke `experiments/.generated/` (di-ignore
Git) agar tidak mengotori repo.

### D12. Adapter gRPC coordinator dan port 50052

`src/chainsim/coordinator/grpc_server.py` mengimplementasikan
`CoordinatorService` (Register, Heartbeat, GetPeers, SubmitWorkResult,
SubmitTemplate) di atas `NodeRegistry` dan `PoolJobManager`. Server dijalankan
dari `coordinator/main.py` bersama Flask.

Port gRPC coordinator dibaca dari `COORDINATOR_GRPC_PORT` dengan default 50052
lewat `grpc_server.grpc_port_from_env`, bukan dari `config/settings.yaml`,
karena file itu milik bersama dan penambahannya butuh PR terpisah. Node
mendapat alamat coordinator dari `COORDINATOR_GRPC` (mis.
`coordinator:50052`); kedua variabel itu belum ditambahkan ke
`docker-compose.yml` (tugas Docker terpisah).

`heartbeat.on_down` sekarang memanggil `PoolJobManager.reassign_grouped` lalu
`CoordinatorService.dispatch`, sehingga waktu reassignment F1 benar-benar
diukur dari `job_reassigned` ke `StartWork` worker yang hidup.

### D13. `SubmitTemplate` untuk memulai job pool

Mode `pool` tidak memakai miner mandiri: node membangun template dari tip
lokal, mengirim `SubmitTemplate` ke coordinator, lalu menunggu `StartWork`.
Coordinator membagi ruang nonce ke semua worker terdaftar dan menghentikan
worker lain saat pemenang melapor. Validitas blok tetap di node. Pemanggil
`SubmitTemplate` ada di `node/main.py` (milik B), jadi sampai itu ditambahkan
job pool tidak pernah dimulai; lihat `docs/c-notes-for-b.md`.

### D14. Orkestrasi E3/E4 dan baseline scalability

`experiments/e3_fault.py` menjalankan `scripts/fault_inject.py` lalu menghitung
metrik dari `logs/*.jsonl` di dalam jendela waktu aksi. Metrik yang tidak
muncul di log tidak ditulis; skrip keluar dengan kode 2 agar kegagalan terlihat.

`experiments/e4_scalability.py` membangkitkan compose per N (D3), menjalankan
beban transaksi di dalam network, dan memakai jendela pengamatan yang sama
untuk throughput serta delay propagasi. Baseline 1 node dijalankan pada trial
yang sama agar `efficiency = (throughput_N / throughput_1) / N` memakai
pembanding setara. Mode default E4 adalah `compete` sampai pemicu job pool ada
(D13).

## Daftar tugas per anggota setelah Fase 0

- **A:** `core/*`, `node/api.py`, `node/miner.py`, `experiments/e1_speedup.py`;
  menyediakan `GET /status` sesuai `NodeStatus` agar dashboard C punya data.
- **B:** `proto/blockchain.proto`, `scripts/gen_proto.*`, `node/main.py`,
  `grpc_server.py`, `grpc_client.py`, `gossip.py`, `sync.py`,
  `scripts/tx_generator.py`, `experiments/e2_propagation.py`; menambah
  `CoordinatorService` di sisi server.
- **C:** sudah selesai - coordinator, dashboard, logging, Docker, compose,
  generator compose E4, `PoolJobManager`, fault injector, helper metrik/grafik.
  Belum: orkestrasi `e3_fault.py` dan `e4_scalability.py` (butuh Docker + node
  berjalan), kalibrasi difficulty, isi `docs/lab-environment.md`, adapter gRPC
  coordinator setelah proto B ada.

## Pertanyaan terbuka

1. Apakah `POST /tx` dilayani coordinator (D2) atau port node dipublikasikan?
2. Apakah E4 boleh memakai compose yang dibangkitkan (D3) atau harus `--scale`?
3. Bentuk penyimpanan cabang samping untuk `AddResult.STORED_SIDE` (milik A).
