# Catatan C untuk A

Dokumen ini berisi hal yang perlu diputuskan/dikerjakan pemilik modul A
(`core/` dan `node/miner.py`, `node/api.py`). C tidak mengubah file tersebut.

## 1. `tx_ids` pada log blok (AGENTS.md 9.3)

Saat ini `block_mined` (di `node/miner.py`) dan `block_accepted` (di
`core/chain.py`) hanya mencatat `tx_count`, bukan daftar `tx_id`. Akibatnya
`experiments/tx_log.py` bisa membuktikan propagasi transaksi antar node, tetapi
tidak bisa membuktikan transaksi mana yang masuk ke blok; timeline hanya
menampilkan blok sesudah transaksi sebagai konteks dan diberi label eksplisit.

Usulan: tambahkan field `tx_ids` (cukup beberapa id pertama, misalnya maksimal
10, agar log tidak membengkak) pada kedua event tersebut. Contoh:

```python
log_event(
    "block_mined",
    self._node_id,
    index=mined.index,
    block_hash=mined.hash,
    tx_ids=[tx.tx_id for tx in mined.transactions[:10]],
    ...
)
```

Tanpa perubahan itu, laporan Bagian 9.3 tetap boleh memakai CSV propagasi,
tetapi klaim "transaksi X masuk ke blok Y" tidak boleh dibuat dari log.
