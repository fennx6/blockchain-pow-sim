"""Dataclass kontrak antarmodul (AGENTS.md Bagian 6.1).

Kontrak ini dibekukan di Fase 0. Perubahan membutuhkan persetujuan ketiga
anggota, karena core, komunikasi, dan coordinator saling bergantung padanya.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


@dataclass(frozen=True)
class Transaction:
    """Transaksi fiktif yang belum tentu masuk blok."""

    tx_id: str
    sender: str
    receiver: str
    amount: float
    timestamp_ms: int
    origin_node: str


@dataclass
class Block:
    """Blok rantai; ``hash`` diisi setelah PoW selesai."""

    index: int
    prev_hash: str
    timestamp_ms: int
    transactions: list[Transaction]
    merkle_root: str
    difficulty: int
    nonce: int
    miner_id: str
    hash: str = ""


class AddResult(Enum):
    """Hasil ``Blockchain.add_block``."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    ORPHAN = "orphan"
    FORK_SWITCHED = "fork_switched"
    STORED_SIDE = "stored_side"
    INVALID = "invalid"


@dataclass
class WorkAssignment:
    """Rentang nonce yang diberikan coordinator ke satu worker (mode pool)."""

    job_id: str
    block_template: Block
    nonce_start: int
    nonce_end: int


@dataclass
class NodeStatus:
    """Ringkasan keadaan satu node untuk dashboard dan metrik."""

    node_id: str
    height: int
    tip_hash: str
    peers: list[str]
    mempool_size: int
    hashrate: float
    state: str
    meta: dict = field(default_factory=dict)
