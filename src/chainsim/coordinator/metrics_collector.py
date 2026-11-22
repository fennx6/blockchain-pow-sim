"""Pengumpul metrik dari endpoint status setiap node.

Data yang tidak terbaca tidak diisi dengan angka karangan: node yang tidak
terjangkau hanya dilaporkan ``state="down"`` dan gauge ``chainsim_node_up 0``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from chainsim.coordinator.registry import NodeRecord, NodeRegistry

logger = logging.getLogger(__name__)

Fetcher = Callable[[NodeRecord], dict[str, Any]]

NUMERIC_FIELDS = ("height", "mempool_size", "hashrate")

# (nama metrik, tipe Prometheus, deskripsi, field payload; None = node_up)
PROMETHEUS_METRICS: tuple[tuple[str, str, str, str | None], ...] = (
    ("chainsim_node_up", "gauge", "1 bila coordinator berhasil membaca status node", None),
    ("chainsim_chain_height", "gauge", "Tinggi rantai lokal node", "height"),
    ("chainsim_mempool_size", "gauge", "Jumlah transaksi di mempool node", "mempool_size"),
    ("chainsim_hashrate", "gauge", "Hash per detik yang dilaporkan node", "hashrate"),
)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def http_get_json(record: NodeRecord, timeout_s: float) -> dict[str, Any]:
    """Baca ``GET /status`` satu node."""
    url = f"{record.rest_url.rstrip('/')}/status"
    with urllib.request.urlopen(url, timeout=timeout_s) as response:  # noqa: S310
        payload: Any = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"status dari {record.node_id} bukan objek JSON")
    return payload


class MetricsCollector:
    """Simpan snapshot status terakhir tiap node untuk dashboard dan /metrics."""

    def __init__(
        self,
        registry: NodeRegistry,
        interval_s: float,
        timeout_s: float,
        fetcher: Fetcher | None = None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s harus lebih besar dari 0")
        if timeout_s <= 0:
            raise ValueError("timeout_s harus lebih besar dari 0")
        self._registry = registry
        self._interval_s = interval_s
        self._fetcher: Fetcher = fetcher or (lambda record: http_get_json(record, timeout_s))
        self._latest: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="metrics", daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout_s)
        self._thread = None

    def latest(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {node_id: dict(payload) for node_id, payload in self._latest.items()}

    def refresh_once(self) -> dict[str, dict[str, Any]]:
        """Ambil status semua node satu kali dan simpan sebagai snapshot terbaru."""
        snapshot: dict[str, dict[str, Any]] = {}
        for record in self._registry.all():
            snapshot[record.node_id] = self._fetch_one(record)
        with self._lock:
            self._latest = snapshot
        return self.latest()

    def prometheus(self) -> str:
        """Eksposisi format Prometheus dari snapshot terakhir."""
        data = self.latest()
        lines: list[str] = []
        for metric, metric_type, description, field in PROMETHEUS_METRICS:
            lines.append(f"# HELP {metric} {description}")
            lines.append(f"# TYPE {metric} {metric_type}")
            for node_id in sorted(data):
                payload = data[node_id]
                down = payload.get("state") == "down"
                if field is None:
                    value: float = 0.0 if down else 1.0
                else:
                    if down:
                        continue
                    raw = payload.get(field)
                    if not isinstance(raw, int | float):
                        continue
                    value = float(raw)
                lines.append(
                    f'{metric}{{node_id="{_escape_label(node_id)}"}} {_format_value(value)}'
                )
        return "\n".join(lines) + "\n"

    def _fetch_one(self, record: NodeRecord) -> dict[str, Any]:
        try:
            payload = self._fetcher(record)
        except (urllib.error.URLError, OSError, ValueError, TimeoutError) as exc:
            logger.warning("status %s tidak terbaca: %s", record.node_id, exc)
            return {
                "node_id": record.node_id,
                "state": "down",
                "error": str(exc),
                "collected_ms": now_ms(),
            }
        snapshot: dict[str, Any] = dict(payload)
        snapshot["node_id"] = record.node_id
        snapshot.setdefault("state", "up")
        snapshot["collected_ms"] = now_ms()
        return snapshot

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh_once()
            except Exception:
                logger.exception("siklus pengumpulan metrik gagal")
            if self._stop.wait(self._interval_s):
                return


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_value(value: float) -> str:
    """Cetak bilangan bulat tanpa desimal agar format Prometheus rapi."""
    return str(int(value)) if float(value).is_integer() else str(value)
