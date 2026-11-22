# Skrip video demo (draf, maksimum 10 menit)

Draf bersama; diisi setelah sistem berjalan. Setiap anggota tampil dan
menjelaskan bagiannya sendiri (AGENTS.md Bagian 3 dan 11).

| Waktu | Bagian | Penanggung jawab | Isi |
|---|---|---|---|
| 0:00-1:00 | Pembuka | bersama | Judul, tema 4, tujuan, pembagian peran |
| 1:00-3:00 | Arsitektur | B | Diagram topologi, alur gRPC, aturan konsensus |
| 3:00-5:00 | Demo mining | A | Kirim transaksi, blok ditemukan, PoW, isi blok |
| 5:00-6:30 | Propagasi dan fork | B | Log transaksi antar node, delay blok, fork switch |
| 6:30-8:00 | Fault recovery | C | Hentikan node, dashboard mendeteksi down, node pulih dan sync |
| 8:00-9:00 | Kinerja | A, B, C | Grafik speedup, efficiency, throughput, latency, scalability dari CSV nyata |
| 9:00-10:00 | Penutup | bersama | Kesimpulan, keterbatasan, saran |

Aturan:

- Semua angka di video diambil dari `experiments/results/` yang sudah ada.
- Tampilkan dashboard dengan tulisan besar dan kontras.
- Jangan merekam data yang belum benar-benar dijalankan.
