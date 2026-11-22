# Bukti eksperimen dan demo

Aturan penamaan dan isi bukti yang dipakai di laporan dan video.

## Struktur

```
evidence/
  E1/    # speedup dan efficiency
  E2/    # latency, delay propagasi, throughput
  E3/    # fault tolerance (F1-F5)
  E4/    # scalability 2, 4, 8, 16 node
  demo/  # tangkapan layar dashboard dan log untuk video
```

## Penamaan file

`<eksperimen>_<run_id>_<deskripsi>.<ext>`

Contoh:

- `E3_F2_20261122-101500_dashboard-node-3-down.png`
- `E2_20261122-110000_tx-log-satu-transaksi.txt`

## Isi yang boleh dimasukkan

- Tangkapan layar dashboard yang menampilkan data nyata dari node.
- Potongan log JSONL yang benar-benar ada di `logs/` pada saat eksperimen.
- Output CSV yang juga tersimpan di `experiments/results/`.

## Larangan

- Jangan menambahkan angka, grafik, atau tangkapan layar yang tidak berasal dari
  eksekusi nyata (AGENTS.md Bagian 2.2).
- Jangan menyalin tangkapan layar dari kelompok atau tutorial lain.
