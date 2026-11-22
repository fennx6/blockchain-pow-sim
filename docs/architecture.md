# Arsitektur sistem

Dokumen bersama (AGENTS.md Bagian 7). Diagram final (draw.io) menyusul di
`docs/topology.drawio` dan `docs/topology.png`.

## Komponen

| Komponen soal | Implementasi | Pemilik kode |
|---|---|---|
| Master/Coordinator | container `coordinator`: registry peer, heartbeat, pengumpul metrik, dashboard | C |
| Worker Node | container `node-1..node-6`: rantai, validasi, miner, gossip | A (core, REST, miner) + B (gRPC, main) |
| Communication Layer | gRPC node-to-node dan node-to-coordinator; REST Flask untuk klien dan dashboard | B (gRPC), A (REST node), C (REST dashboard) |
| Shared/Distributed Storage | satu volume Docker per node, rantai disimpan lokal | A (`core/storage.py`), C (volume di compose) |
| Monitoring | dashboard Flask + `/metrics` format Prometheus | C |

## Topologi default

```
                    host: 127.0.0.1:8080
                             |
                       +-----------+
                       |coordinator|
                       |  :8080    |
                       +-----+-----+
                             | chainnet (bridge)
   +--------+--------+-------+-------+--------+--------+
   |        |        |               |        |        |
 node-1   node-2   node-3         node-4   node-5   node-6
 :8081    :8081    :8081          :8081    :8081    :8081
 :50051   :50051   :50051         :50051   :50051   :50051
   |        |        |               |        |        |
 vol-1    vol-2    vol-3           vol-4    vol-5    vol-6
```

Setiap node hanya listen di dalam network; host hanya membuka dashboard.

## Alur utama

1. **Transaksi masuk.** Klien mengirim transaksi fiktif; node asal mencatat
   `tx_received`, memasukkannya ke mempool, lalu meneruskan ke peer lewat gRPC
   (`tx_forwarded`).
2. **Penambangan.** Mode `compete`: setiap node menambang kandidatnya sendiri.
   Mode `pool`: coordinator membagi rentang nonce ke P worker
   (`coordinator/scheduler.py`), worker pertama yang menemukan nonce valid
   melapor, coordinator menghentikan worker lain.
3. **Propagasi blok.** Blok yang ditemukan disiarkan (`block_mined`), diterima
   peer (`block_received`), divalidasi, lalu diterima (`block_accepted`) atau
   ditolak (`block_rejected`).
4. **Konsensus.** Fork choice memakai kerja kumulatif terbesar; bila berganti
   rantai, transaksi blok yang dibuang kembali ke mempool dan dicatat
   `fork_switched`.
5. **Pemulihan.** Node yang hidup lagi menarik blok yang tertinggal
   (`sync_start`, `sync_done`) sampai tinggi rantainya sama dengan jaringan.
6. **Monitoring.** Coordinator memantau `GET /status` tiap node; dashboard
   menampilkan tinggi rantai, tip hash, mempool, hashrate, dan kejadian terakhir.

## Batas tanggung jawab

Coordinator **bukan** otoritas validitas blok. Node memvalidasi sendiri dan
memilih rantainya sendiri. Coordinator hanya membantu penemuan peer, deteksi
kegagalan, penjadwalan mode pool, dan monitoring. Karena itu coordinator adalah
single point of failure untuk fungsi-fungsi tersebut, dan node harus tetap bisa
bergosip dengan daftar peer terakhir bila coordinator mati (skenario F5).

## Kontrak

Sumber kebenaran kontrak ada di AGENTS.md Bagian 6 dan di
`src/chainsim/types.py`. Perubahan kontrak membutuhkan persetujuan ketiga
anggota.
