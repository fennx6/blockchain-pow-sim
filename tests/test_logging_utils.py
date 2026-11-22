"""Penulisan dan pembacaan log JSONL."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chainsim.logging_utils import log_event, read_recent_events


def read_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_log_event_writes_required_fields(log_dir_env: Path) -> None:
    log_event("tx_received", "node-1", tx_id="abc")
    records = read_lines(log_dir_env / "node-1.jsonl")
    assert len(records) == 1
    record = records[0]
    assert record["event"] == "tx_received"
    assert record["node_id"] == "node-1"
    assert record["tx_id"] == "abc"
    assert isinstance(record["ts_ms"], int)


def test_ts_ms_is_always_generated(log_dir_env: Path) -> None:
    log_event("block_mined", "node-2", ts_ms=1)
    record = read_lines(log_dir_env / "node-2.jsonl")[0]
    assert record["ts_ms"] != 1


def test_duplicate_reserved_argument_is_rejected(log_dir_env: Path) -> None:
    with pytest.raises(TypeError):
        log_event("block_mined", "node-2", node_id="palsu")  # type: ignore[call-arg]


def test_events_are_appended(log_dir_env: Path) -> None:
    log_event("sync_start", "node-1")
    log_event("sync_done", "node-1", pulled=3)
    records = read_lines(log_dir_env / "node-1.jsonl")
    assert [record["event"] for record in records] == ["sync_start", "sync_done"]


def test_node_id_is_sanitised_for_file_name(log_dir_env: Path) -> None:
    log_event("peer_up", "node/../1")
    assert (log_dir_env / "node_.._1.jsonl").is_file()


def test_empty_node_id_is_rejected(log_dir_env: Path) -> None:
    with pytest.raises(ValueError):
        log_event("peer_up", "   ")


def test_read_recent_events_returns_newest_first(log_dir_env: Path) -> None:
    log_event("first", "node-1", marker=1)
    log_event("second", "node-2", marker=2)
    events = read_recent_events(limit=10, directory=log_dir_env)
    assert [event["event"] for event in events] == ["second", "first"]


def test_read_recent_events_respects_limit(log_dir_env: Path) -> None:
    for index in range(5):
        log_event("block_received", "node-1", index=index)
    assert len(read_recent_events(limit=2, directory=log_dir_env)) == 2
    assert read_recent_events(limit=0, directory=log_dir_env) == []


def test_read_recent_events_on_missing_directory(tmp_path: Path) -> None:
    assert read_recent_events(limit=5, directory=tmp_path / "belum-ada") == []
