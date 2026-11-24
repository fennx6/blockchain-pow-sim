"""Tests for the transaction log merger (no Docker required)."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

EXPERIMENTS_DIR = Path(__file__).resolve().parents[1] / "experiments"
if str(EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENTS_DIR))

from e2_propagation import LogEvent  # noqa: E402
from tx_log import (  # noqa: E402
    CSV_FIELDS,
    TxRow,
    collect_tx_rows,
    format_timeline,
    main,
    nodes_reached,
    write_csv,
)


def event(ts_ms: int, node_id: str, name: str, **fields: object) -> LogEvent:
    return LogEvent(ts_ms=ts_ms, node_id=node_id, event=name, fields=dict(fields))


def write_logs(directory: Path, records: dict[str, list[dict[str, object]]]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for node_id, rows in records.items():
        path = directory / f"{node_id}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def test_collect_tx_rows_filters_and_sorts() -> None:
    events = [
        event(2_000, "node-1", "tx_forwarded", tx_id="tx-a", from_node="node-1", to_node="node-2"),
        event(
            1_000,
            "node-1",
            "tx_received",
            tx_id="tx-a",
            from_node="client",
            to_node="node-1",
            origin_node="node-1",
        ),
        event(1_500, "node-1", "block_mined", index=1),
        event(1_200, "node-2", "tx_received", tx_id="tx-b", from_node="client", to_node="node-2"),
    ]

    rows = collect_tx_rows(events)

    assert [row.tx_id for row in rows] == ["tx-a", "tx-b", "tx-a"]
    assert rows[0].ts_ms == 1_000
    assert rows[0].origin_node == "node-1"
    assert collect_tx_rows(events, "tx-b")[0].to_node == "node-2"


def test_collect_tx_rows_skips_missing_tx_id() -> None:
    events = [event(1_000, "node-1", "tx_received", from_node="client", to_node="node-1")]
    assert collect_tx_rows(events) == []


def test_nodes_reached_uses_destination_and_origin() -> None:
    rows = [
        TxRow(1_000, "tx-a", "client", "node-1", "tx_received", origin_node="node-1"),
        TxRow(1_100, "tx-a", "node-1", "node-2", "tx_forwarded"),
        TxRow(1_200, "tx-a", "node-1", "node-3", "tx_forwarded"),
    ]
    assert nodes_reached(rows) == {"tx-a": {"node-1", "node-2", "node-3"}}


def test_write_csv_has_exact_columns(tmp_path: Path) -> None:
    rows = [TxRow(1_000, "tx-a", "node-1", "node-2", "tx_forwarded", origin_node="node-1")]
    path = write_csv(rows, tmp_path / "out" / "tx_log.csv")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == list(CSV_FIELDS)
        assert list(reader) == [
            {
                "ts_ms": "1000",
                "tx_id": "tx-a",
                "from_node": "node-1",
                "to_node": "node-2",
                "event": "tx_forwarded",
            }
        ]


def test_format_timeline_includes_coverage_and_block_context() -> None:
    rows = [
        TxRow(1_000, "tx-a", "client", "node-1", "tx_received", origin_node="node-1"),
        TxRow(1_100, "tx-a", "node-1", "node-2", "tx_forwarded"),
    ]
    events = [
        event(1_050, "node-1", "block_mined", index=3, block_hash="abcdef1234567890"),
        event(1_050, "node-1", "tin_event", tx_id="tx-a"),
    ]
    text = format_timeline(rows, events, "tx-a", context_ms=1_000)

    assert "transaksi tx-a" in text
    assert "node terjangkau: 2 (node-1, node-2)" in text
    assert "node-1 -> node-2" in text
    assert "block_mined" in text
    assert "index=3" in text
    assert "abcdef123456" in text


def test_format_timeline_reports_missing_blocks() -> None:
    rows = [TxRow(1_000, "tx-a", "client", "node-1", "tx_received")]
    text = format_timeline(rows, [], "tx-a", context_ms=500)
    assert "belum ada block_mined" in text


def test_format_timeline_rejects_unknown_tx() -> None:
    with pytest.raises(ValueError):
        format_timeline([], [], "tx-x")


def test_main_writes_csv_and_prints_featured_timeline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    logs = tmp_path / "logs"
    write_logs(
        logs,
        {
            "node-1": [
                {
                    "ts_ms": 1_000,
                    "node_id": "node-1",
                    "event": "tx_received",
                    "tx_id": "tx-a",
                    "from_node": "client",
                    "to_node": "node-1",
                    "origin_node": "node-1",
                },
                {
                    "ts_ms": 1_100,
                    "node_id": "node-1",
                    "event": "tx_forwarded",
                    "tx_id": "tx-a",
                    "from_node": "node-1",
                    "to_node": "node-2",
                },
                {
                    "ts_ms": 2_000,
                    "node_id": "node-1",
                    "event": "tx_received",
                    "tx_id": "tx-b",
                    "from_node": "client",
                    "to_node": "node-1",
                },
            ],
            "node-2": [
                {
                    "ts_ms": 1_200,
                    "node_id": "node-2",
                    "event": "tx_received",
                    "tx_id": "tx-a",
                    "from_node": "node-1",
                    "to_node": "node-2",
                },
                {
                    "ts_ms": 1_300,
                    "node_id": "node-2",
                    "event": "block_mined",
                    "index": 1,
                    "block_hash": "deadbeefcafe",
                },
            ],
        },
    )
    output = tmp_path / "results" / "tx_log.csv"

    code = main(["--logs-dir", str(logs), "--output", str(output)])

    assert code == 0
    with output.open("r", encoding="utf-8", newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 4
    captured = capsys.readouterr()
    assert "transaksi tx-a" in captured.out
    assert "block_mined" in captured.out


def test_main_filters_by_tx_id(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    write_logs(
        logs,
        {
            "node-1": [
                {
                    "ts_ms": 1_000,
                    "node_id": "node-1",
                    "event": "tx_received",
                    "tx_id": "tx-a",
                    "from_node": "client",
                    "to_node": "node-1",
                },
                {
                    "ts_ms": 2_000,
                    "node_id": "node-1",
                    "event": "tx_received",
                    "tx_id": "tx-b",
                    "from_node": "client",
                    "to_node": "node-1",
                },
            ]
        },
    )
    output = tmp_path / "tx_log.csv"

    assert main(["--logs-dir", str(logs), "--output", str(output), "--tx-id", "tx-b"]) == 0
    with output.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["tx_id"] for row in rows] == ["tx-b"]


def test_main_requires_logs(tmp_path: Path) -> None:
    assert (
        main(["--logs-dir", str(tmp_path / "missing"), "--output", str(tmp_path / "out.csv")]) == 1
    )


def test_main_requires_tx_events(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    write_logs(logs, {"node-1": [{"ts_ms": 1_000, "node_id": "node-1", "event": "block_mined"}]})
    assert main(["--logs-dir", str(logs), "--output", str(tmp_path / "out.csv")]) == 1
