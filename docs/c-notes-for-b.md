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
