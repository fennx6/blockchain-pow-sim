"""Log kejadian JSONL yang dipakai node, coordinator, dan skrip eksperimen.

Satu kejadian = satu baris JSON di ``<LOG_DIR>/<node_id>.jsonl`` (AGENTS.md
Bagian 9.2). Penulisan dikunci dengan lock karena server gRPC, Flask, dan miner
berjalan di thread berbeda dan menulis ke file yang sama.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")
_WRITE_LOCK = threading.Lock()
_LOGGER_LOCK = threading.Lock()
_LOGGERS: dict[str, logging.Logger] = {}

DEFAULT_LOG_DIR = "logs"


def log_dir() -> Path:
    """Direktori log aktif; dibaca ulang tiap panggilan agar test bisa override."""
    return Path(os.environ.get("LOG_DIR", DEFAULT_LOG_DIR))


def _safe_file_stem(node_id: str) -> str:
    stem = _SAFE_NAME.sub("_", node_id.strip())
    if not stem or stem in {".", ".."}:
        raise ValueError(f"node_id tidak bisa dipakai sebagai nama file: {node_id!r}")
    return stem


def log_event(event: str, node_id: str, **fields: Any) -> None:
    """Tulis satu kejadian ke JSONL milik ``node_id``.

    ``ts_ms``, ``node_id``, dan ``event`` selalu ditulis dan tidak bisa ditimpa
    oleh ``fields`` supaya format log tetap konsisten untuk analisis.
    """
    if not event:
        raise ValueError("event tidak boleh kosong")
    directory = log_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{_safe_file_stem(node_id)}.jsonl"

    record: dict[str, Any] = dict(fields)
    record["ts_ms"] = time.time_ns() // 1_000_000
    record["node_id"] = node_id
    record["event"] = event

    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)
    with _WRITE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def _iter_events(directory: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(directory.glob("*.jsonl")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        # Baris setengah tertulis saat container dimatikan: lewati.
                        continue
                    if isinstance(parsed, dict):
                        yield parsed
        except OSError:
            continue


def read_recent_events(limit: int = 50, directory: Path | None = None) -> list[dict[str, Any]]:
    """Ambil ``limit`` kejadian terbaru dari semua file JSONL, terbaru dulu."""
    if limit <= 0:
        return []
    target = directory or log_dir()
    if not target.is_dir():
        return []
    # Indeks urutan dipakai sebagai pemecah seri: banyak kejadian bisa punya
    # ts_ms yang sama karena resolusi milidetik, dan kejadian yang ditulis
    # belakangan harus tetap muncul lebih dulu.
    indexed = list(enumerate(_iter_events(target)))
    indexed.sort(key=lambda pair: (int(pair[1].get("ts_ms", 0)), pair[0]), reverse=True)
    return [event for _, event in indexed][:limit]


def get_logger(name: str) -> logging.Logger:
    """Logger konsol untuk pesan operasional (bukan pengganti log kejadian)."""
    with _LOGGER_LOCK:
        existing = _LOGGERS.get(name)
        if existing is not None:
            return existing
        logger = logging.getLogger(name)
        level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, level_name, logging.INFO))
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
            logger.addHandler(handler)
        logger.propagate = False
        _LOGGERS[name] = logger
        return logger
