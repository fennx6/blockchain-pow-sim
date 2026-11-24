# Catatan C untuk B (file milik B)

Dokumen ini catatan koordinasi. C tidak mengubah file milik B; perubahan di
bawah perlu dikerjakan B di `src/chainsim/node/main.py`.

## 1. Pemicu `SubmitTemplate` untuk mode pool

**Masalah.** `CoordinatorService` (C) sudah siap dan `Miner` mode pool hanya
menunggu `StartWork`. Tetapi belum ada kode yang mengirim `SubmitTemplate`,
sehingga `MINING_MODE=pool` membuat semua node idle dan E3/F1 serta E4 mode
pool tidak bisa dijalankan.

**Yang dibutuhkan.** Saat tip berubah, node membangun template dari tip lokal
dan mengirimkannya ke coordinator. Coordinator yang membagi nonce, bukan node.

Kerangka di `NodeRuntime`:

```python
from chainsim.core.block import build_block_template

# __init__: self._pool_tip: str = ""

def submit_pool_template(self) -> None:
    """Minta coordinator membuka job pool untuk tip saat ini (mode pool)."""
    if self.miner.mode != "pool" or self.coordinator_client is None:
        return
    tip = self.chain.tip()
    if tip.hash == self._pool_tip:
        return
    txs = [
        tx
        for tx in self.mempool.select(self.settings.node.block_size)
        if not self.chain.has_transaction(tx.tx_id)
    ]
    template = build_block_template(
        index=tip.index + 1,
        prev_hash=tip.hash,
        transactions=txs,
        difficulty=self.chain.difficulty,
        miner_id=self.node_id,
    )
    result = self.coordinator_client.submit_template(self.node_id, template)
    if result.accepted:
        self._pool_tip = tip.hash
        logger.info("job pool %s dimulai untuk blok %d", result.job_id, template.index)
    else:
        logger.warning("coordinator menolak template: %s", result.message)
```

Panggil di tiga tempat:

1. `start()` setelah `miner.start()` — job pertama.
2. `handle_block()` saat `AddResult.ACCEPTED` atau `FORK_SWITCHED` — tip berubah
   karena blok dari peer.
3. `handle_work_result()` setelah blok pemenang masuk rantai — node pemenang
   juga harus memulai job berikutnya.

Koordinat `coordinator.submit_template` mengembalikan `JobStartResult` dan
menerima template yang belum ditambang (nonce diabaikan). Coordinator menolak
dengan `accepted=False` bila masih ada job aktif; itu wajar saat dua node
mengirim bersamaan, dan akan dicoba lagi pada blok berikutnya.

## 2. Registrasi node (opsional tetapi disarankan)

Registry coordinator sudah diisi dari env `NODES`, jadi pool tetap jalan tanpa
registrasi. Namun `Register` membuat alamat gRPC/peer berasal dari node sendiri:

```python
from chainsim.coordinator.registry import NodeRecord

if self.coordinator_client is not None:
    self.coordinator_client.register(
        NodeRecord(
            node_id=self.node_id,
            rest_url=f"http://{self.node_id}:{self.settings.node.rest_port}",
            grpc_address=f"{self.node_id}:{self.grpc_port}",
            peers=self.peers,
        )
    )
```

## 3. Yang disiapkan C (sudah ada di branch `feat/c-coordinator-grpc`)

- `src/chainsim/coordinator/grpc_server.py`: Register, Heartbeat, GetPeers,
  SubmitWorkResult, SubmitTemplate.
- `coordinator/main.py`: server gRPC berjalan bersama Flask; `peer_down`
  memicu `reassign_grouped` lalu `dispatch` ke worker hidup.
- Port coordinator: env `COORDINATOR_GRPC_PORT` (default 50052).

## 4. Yang belum ada di compose (tugas C berikutnya)

`docker-compose.yml` belum mengekspos 50052 dan belum mengeset
`COORDINATOR_GRPC=coordinator:50052` untuk node. Tanpa itu, `CoordinatorClient`
di node tidak terbentuk (`_coordinator_client_from_env` mengembalikan `None`)
dan mode pool tetap tidak berjalan.

## 5. Bug: satu job pool bisa menghasilkan beberapa blok

Ditemukan saat uji fungsional in-process (3 node, `MINING_MODE=pool`,
difficulty 2, coordinator gRPC asli). Ini bukan kegagalan coordinator, tetapi
race di `NodeRuntime.handle_work_result`.

**Gejala.** Ketiga worker menemukan nonce valid hampir bersamaan (selisih
3 ms). Log dari satu run:

```
pnode-1 block_mined index=1 hash=00dda611...  job-1-1
pnode-2 block_mined index=1 hash=0023a817...  job-1-1
pnode-3 block_mined index=1 hash=000485ac...  job-1-1
coordinator job_done hash=00dda611...          job-1-1
```

Setiap node menambahkan bloknya sendiri (`block_accepted`), lalu menerima blok
lain di index yang sama sebagai side branch. Karena work kumulatif sama, aturan
seri "pertahankan yang lebih dulu" membuat tiap node mempertahankan bloknya
sendiri: tinggi sama (1) tetapi **tip hash berbeda**. Dua worker juga menerima
balasan `job ... sudah selesai` dari coordinator, tetapi bloknya sudah
terlanjur masuk rantai lokal.

**Penyebab.** `handle_work_result` menambahkan dan menyiarkan blok **sebelum**
melapor ke coordinator. Worker yang kalah baru tahu setelah
`SubmitWorkResult` ditolak; pada saat itu bloknya sudah menjadi tip lokal.

**Perbaikan yang disarankan** (kecil, hanya di `handle_work_result`):

```python
# 1. laporkan dulu; coordinator menentukan pemenang
result = WorkResult(
    job_id=assignment.job_id,
    node_id=self.node_id,
    nonce=block.nonce,
    found=True,
    block_hash=block.hash,
)
try:
    ack = self.coordinator_client.submit_work_result(result)
except Exception:
    logger.exception("failed to submit work result for job %s", assignment.job_id)
    return

# 2. hanya pemenang yang boleh menambah dan menyiarkan
if not ack.ok:
    log_event(
        "block_rejected",
        self.node_id,
        index=block.index,
        block_hash=block.hash,
        reason="pool_job_lost",
    )
    return

added = self.chain.add_block(block)
if added not in (AddResult.ACCEPTED, AddResult.FORK_SWITCHED):
    logger.warning("pool block %s was not added (%s)", block.hash, added.value)
    return
self.miner.notify_new_block()
self.gossip.broadcast_block(block)
```

Catatan implementasi:

- `coordinator_client is None` boleh tetap memakai perilaku lama (tambah +
  siarkan) karena tidak ada arbiter.
- Jika RPC gagal karena jaringan, blok sebaiknya dibuang: job belum ditandai
  selesai, jadi worker lain masih bisa menang dan job tidak macet.
- Trade-off: pemenang menunggu satu RTT coordinator (~ms) sebelum menyiarkan.
  Ini menghilangkan fork dan lebih penting daripada latensi sekian milidetik.
- Kasus langka: pemenang sudah tercatat `job_done` tetapi `add_block` gagal
  (mis. rantai sudah maju). Job berakhir tanpa blok; template berikutnya akan
  membuka job baru. Perilaku ini masih lebih baik daripada fork.

**Saran regression test** (di test milik B, tanpa Docker): pakai coordinator
palsu yang mengembalikan `ok=True` untuk panggilan pertama dan `ok=False` untuk
panggilan kedua, panggil `handle_work_result` dua kali dengan dua blok berbeda
pada index yang sama, lalu pastikan hanya blok pertama yang masuk rantai dan
hanya satu `broadcast_block` terjadi.

**Dampak bila dibiarkan:** side branch bertambah, dashboard menampilkan tip
berbeda pada tinggi yang sama, dan throughput/efisiensi pool di E4 bias karena
kerja worker yang kalah terbuang. Status: menunggu keputusan B karena file
`src/chainsim/node/main.py` milik B.
