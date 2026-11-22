"""Deteksi node mati lewat polling status REST.

Heartbeat memakai ``GET /status`` milik node (kontrak Bagian 6.4) karena
endpoint itu sudah wajib ada. RPC ``Heartbeat`` di ``CoordinatorService``
(Bagian 6.3) menyusul saat modul gRPC milik B siap; keduanya harus mengisi
``NodeRegistry`` yang sama.
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
from chainsim.logging_utils import log_event

logger = logging.getLogger(__name__)

COORDINATOR_ID = "coordinator"

STATE_UP = "up"
STATE_DOWN = "down"
STATE_UNKNOWN = "unknown"

Probe = Callable[[NodeRecord], bool]
StatusCallback = Callable[[NodeRecord], None]


def http_probe(record: NodeRecord, timeout_s: float) -> bool:
    """True bila node menjawab ``/status`` dengan payload JSON yang masuk akal."""
    url = f"{record.rest_url.rstrip('/')}/status"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as response:  # noqa: S310
            payload: Any = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return False
    return isinstance(payload, dict) and "node_id" in payload


class HeartbeatMonitor:
    """Pantau node secara berkala dan laporkan transisi up/down."""

    def __init__(
        self,
        registry: NodeRegistry,
        interval_s: float,
        timeout_s: float,
        probe: Probe | None = None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s harus lebih besar dari 0")
        if timeout_s <= 0:
            raise ValueError("timeout_s harus lebih besar dari 0")
        self._registry = registry
        self._interval_s = interval_s
        self._probe: Probe = probe or (lambda record: http_probe(record, timeout_s))
        self._states: dict[str, str] = {}
        self._lock = threading.Lock()
        self._down_callbacks: list[StatusCallback] = []
        self._up_callbacks: list[StatusCallback] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def on_down(self, callback: StatusCallback) -> StatusCallback:
        """Daftarkan callback yang dipanggil saat node pertama kali terdeteksi mati."""
        self._down_callbacks.append(callback)
        return callback

    def on_up(self, callback: StatusCallback) -> StatusCallback:
        """Daftarkan callback yang dipanggil saat node kembali hidup."""
        self._up_callbacks.append(callback)
        return callback

    def state(self) -> dict[str, str]:
        with self._lock:
            return dict(self._states)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout_s)
        self._thread = None

    def poll_once(self) -> dict[str, str]:
        """Periksa semua node satu kali; kembalikan keadaan terbaru per node."""
        observed: dict[str, str] = {}
        for record in self._registry.all():
            healthy = self._safe_probe(record)
            new_state = STATE_UP if healthy else STATE_DOWN
            with self._lock:
                previous = self._states.get(record.node_id, STATE_UNKNOWN)
                self._states[record.node_id] = new_state
            observed[record.node_id] = new_state
            if new_state == previous:
                continue
            event = "peer_up" if healthy else "peer_down"
            log_event(event, COORDINATOR_ID, target=record.node_id, previous=previous)
            callbacks = self._up_callbacks if healthy else self._down_callbacks
            for callback in list(callbacks):
                try:
                    callback(record)
                except Exception:
                    logger.exception("callback heartbeat gagal untuk %s", record.node_id)
        return observed

    def _safe_probe(self, record: NodeRecord) -> bool:
        try:
            return bool(self._probe(record))
        except Exception:
            logger.exception("probe ke %s gagal", record.node_id)
            return False

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:
                logger.exception("siklus heartbeat gagal")
            if self._stop.wait(self._interval_s):
                return


def monotonic_ms() -> int:
    """Waktu monotonik dalam milidetik, untuk mengukur durasi di dalam proses."""
    return time.monotonic_ns() // 1_000_000
