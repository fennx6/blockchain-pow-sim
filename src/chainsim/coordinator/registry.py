"""Daftar node yang dikenal coordinator.

Registry hanya menyimpan alamat dan daftar peer. Coordinator tidak pernah
memutuskan validitas blok (AGENTS.md Bagian 7.1), jadi registry tidak menyimpan
salinan rantai.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class NodeRecord:
    """Alamat satu node beserta peer yang harus digosipkan."""

    node_id: str
    rest_url: str
    grpc_address: str = ""
    peers: tuple[str, ...] = field(default_factory=tuple)


def parse_node_entry(
    entry: str,
    default_rest_port: int = 8081,
    grpc_port: int = 50051,
) -> tuple[str, str, str]:
    """Ubah ``"node-1=node-1:8081"`` menjadi ``(node_id, rest_url, grpc_address)``.

    Bentuk yang diterima: ``node-1``, ``node-1=node-1:8081``, dan
    ``node-1=http://node-1:8081``.
    """
    node_id, separator, address = entry.partition("=")
    node_id = node_id.strip()
    address = address.strip() if separator else ""
    if not node_id:
        raise ValueError(f"entri node tanpa nama: {entry!r}")
    address = address or node_id
    if "://" in address:
        address = address.split("://", 1)[1]
    address = address.rstrip("/")
    host, _, port_text = address.partition(":")
    if not host:
        raise ValueError(f"entri node tanpa host: {entry!r}")
    port = int(port_text) if port_text else default_rest_port
    if not 1 <= port <= 65535:
        raise ValueError(f"port REST di luar rentang 1..65535 pada entri {entry!r}")
    return node_id, f"http://{host}:{port}", f"{host}:{grpc_port}"


class NodeRegistry:
    """Kumpulan ``NodeRecord`` yang aman diakses dari beberapa thread."""

    def __init__(self, records: Iterable[NodeRecord] = ()) -> None:
        self._nodes: dict[str, NodeRecord] = {}
        self._lock = threading.Lock()
        for record in records:
            self.register(record)

    def register(self, record: NodeRecord) -> None:
        """Tambahkan node, atau ganti data node dengan ``node_id`` yang sama."""
        with self._lock:
            self._nodes[record.node_id] = record

    def unregister(self, node_id: str) -> None:
        with self._lock:
            self._nodes.pop(node_id, None)

    def get(self, node_id: str) -> NodeRecord | None:
        with self._lock:
            return self._nodes.get(node_id)

    def all(self) -> list[NodeRecord]:
        with self._lock:
            return [self._nodes[key] for key in sorted(self._nodes)]

    def node_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def peers_for(self, node_id: str) -> list[str]:
        record = self.get(node_id)
        if record is None:
            return []
        if record.peers:
            return list(record.peers)
        return [other for other in self.node_ids() if other != node_id]

    @classmethod
    def from_config(
        cls,
        nodes: Iterable[str],
        peer_map: Mapping[str, Iterable[str]] | None = None,
        default_rest_port: int = 8081,
        grpc_port: int = 50051,
    ) -> NodeRegistry:
        """Bangun registry dari daftar entri konfigurasi.

        Bila ``peer_map`` tidak diberikan, topologi dianggap full mesh: setiap
        node mengenal semua node lain. Ini sesuai mode ``compete`` di Bagian 7.2.
        """
        parsed = [parse_node_entry(entry, default_rest_port, grpc_port) for entry in nodes]
        ids = [node_id for node_id, _, _ in parsed]
        records: list[NodeRecord] = []
        for node_id, rest_url, grpc_address in parsed:
            if peer_map is not None and node_id in peer_map:
                peers = tuple(peer_map[node_id])
            else:
                peers = tuple(other for other in ids if other != node_id)
            records.append(
                NodeRecord(
                    node_id=node_id,
                    rest_url=rest_url,
                    grpc_address=grpc_address,
                    peers=peers,
                )
            )
        return cls(records)
