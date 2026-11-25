"""Gabungkan log transaksi antar node menjadi CSV dan potongan timeline.

Owner: Member C (AGENTS.md Bagian 9.3). Sumber data hanya ``logs/*.jsonl``
yang ditulis ``logging_utils.log_event`` saat jaringan benar-benar berjalan;
skrip ini tidak mengarang kejadian.

CSV keluaran memakai kolom wajib ``ts_ms, tx_id, from_node, to_node, event``
dan hanya berisi event ``tx_received`` dan ``tx_forwarded``. Event blok membawa
``tx_ids`` (maksimal 10 tx pertama), sehingga timeline dapat menyatakan apakah
transaksi yang ditampilkan benar-benar tercantum di blok, bukan hanya
menampilkan blok sebagai konteks.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
if str(_EXPERIMENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS_DIR))

from e2_propagation import LogEvent, load_events  # noqa: E402

logger = logging.getLogger("chainsim.experiments.tx_log")

TX_EVENTS = ("tx_received", "tx_forwarded")
BLOCK_EVENTS = ("block_mined", "block_accepted")
CSV_FIELDS = ("ts_ms", "tx_id", "from_node", "to_node", "event")
DEFAULT_OUTPUT = _EXPERIMENTS_DIR / "results" / "tx_log.csv"
DEFAULT_CONTEXT_MS = 10_000
DEFAULT_TIMELINE_BLOCKS = 3


@dataclass(frozen=True)
class TxRow:
    """Satu baris CSV untuk perpindahan transaksi antar node."""

    ts_ms: int
    tx_id: str
    from_node: str
    to_node: str
    event: str
    origin_node: str = ""

    def as_csv(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in CSV_FIELDS}


def _text(value: object) -> str:
    return str(value).strip() if value is not None else ""


def collect_tx_rows(
    events: list[LogEvent] | tuple[LogEvent, ...], tx_id: str | None = None
) -> list[TxRow]:
    """Ambil event transaksi, urutkan menurut waktu, dan saring ``tx_id`` bila diberi."""
    wanted = tx_id.strip() if tx_id else None
    rows: list[TxRow] = []
    for event in events:
        if event.event not in TX_EVENTS:
            continue
        current_tx = _text(event.fields.get("tx_id"))
        if not current_tx or (wanted and current_tx != wanted):
            continue
        rows.append(
            TxRow(
                ts_ms=event.ts_ms,
                tx_id=current_tx,
                from_node=_text(event.fields.get("from_node")),
                to_node=_text(event.fields.get("to_node")),
                event=event.event,
                origin_node=_text(event.fields.get("origin_node")),
            )
        )
    rows.sort(key=lambda row: (row.ts_ms, row.tx_id, row.event, row.to_node))
    return rows


def nodes_reached(rows: list[TxRow] | tuple[TxRow, ...]) -> dict[str, set[str]]:
    """Kumpulkan node yang menerima tiap transaksi (tujuan atau node asal)."""
    reached: dict[str, set[str]] = {}
    for row in rows:
        bucket = reached.setdefault(row.tx_id, set())
        if row.to_node and row.to_node != "client":
            bucket.add(row.to_node)
        if row.origin_node:
            bucket.add(row.origin_node)
    return reached


def write_csv(rows: list[TxRow] | tuple[TxRow, ...], path: Path | str) -> Path:
    """Tulis CSV dengan kolom tetap; header tetap ditulis walau ``rows`` kosong."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv())
    return target


def _pick_featured_tx(rows: list[TxRow]) -> str:
    """Pilih transaksi dengan jangkauan node terluas, lalu yang paling awal."""
    coverage = nodes_reached(rows)
    first_ts: dict[str, int] = {}
    for row in rows:
        first_ts.setdefault(row.tx_id, row.ts_ms)
    return min(coverage, key=lambda tx: (-len(coverage[tx]), first_ts[tx], tx))


def format_timeline(
    rows: list[TxRow],
    events: list[LogEvent] | tuple[LogEvent, ...],
    tx_id: str,
    *,
    context_ms: int = DEFAULT_CONTEXT_MS,
    max_blocks: int = DEFAULT_TIMELINE_BLOCKS,
) -> str:
    """Buat potongan timeline yang mudah dibaca untuk satu transaksi."""
    selected = [row for row in rows if row.tx_id == tx_id]
    if not selected:
        raise ValueError(f"transaksi {tx_id!r} tidak punya event tx_received/tx_forwarded")
    selected.sort(key=lambda row: (row.ts_ms, row.event, row.to_node))

    start_ms = selected[0].ts_ms
    reached = sorted(nodes_reached(selected)[tx_id])
    lines = [
        f"transaksi {tx_id}",
        f"  event: {len(selected)} | node terjangkau: {len(reached)} ({', '.join(reached) or '-'})",
        "  ts_ms      +ms     event          from -> to",
    ]
    for row in selected:
        lines.append(
            f"  {row.ts_ms:<10} {row.ts_ms - start_ms:>6}  "
            f"{row.event:<14} {row.from_node or '-'} -> {row.to_node or '-'}"
        )

    last_ts = selected[-1].ts_ms
    blocks = [
        event
        for event in events
        if event.event in BLOCK_EVENTS and start_ms <= event.ts_ms <= last_ts + max(context_ms, 0)
    ]
    blocks.sort(key=lambda event: event.ts_ms)
    lines.append("")
    if blocks:
        lines.append(
            "  blok sesudah transaksi (tx_ids hanya 10 tx pertama; "
            "label menunjukkan keanggotaan):"
        )
        for event in blocks[:max_blocks]:
            index = _text(event.fields.get("index"))
            block_hash = _text(event.fields.get("block_hash"))
            raw_ids = event.fields.get("tx_ids")
            if isinstance(raw_ids, list | tuple):
                membership = (
                    "tx ADA di blok"
                    if tx_id in {_text(item) for item in raw_ids}
                    else "tx tidak tercantum di tx_ids"
                )
            else:
                membership = "tx_ids tidak tersedia"
            lines.append(
                f"  {event.ts_ms:<10} {event.ts_ms - start_ms:>6}  "
                f"{event.event:<14} {event.node_id} "
                f"index={index or '-'} hash={block_hash[:12] or '-'} {membership}"
            )
        if len(blocks) > max_blocks:
            lines.append(f"  ... {len(blocks) - max_blocks} event blok lain")
    else:
        lines.append(
            f"  belum ada block_mined/block_accepted dalam {max(context_ms, 0)} ms "
            "setelah event transaksi terakhir"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gabungkan log transaksi antar node menjadi CSV (AGENTS.md 9.3)"
    )
    parser.add_argument("--logs-dir", default="logs", help="direktori JSONL runtime")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="path CSV")
    parser.add_argument("--tx-id", default=None, help="hanya transaksi ini")
    parser.add_argument(
        "--context-ms",
        type=int,
        default=DEFAULT_CONTEXT_MS,
        help="jendela waktu untuk menampilkan blok sesudah transaksi",
    )
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=DEFAULT_TIMELINE_BLOCKS,
        help="jumlah maksimum event blok di timeline",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    events = load_events(args.logs_dir)
    if not events:
        logger.error("tidak ada event di %s; jalankan jaringan dulu", args.logs_dir)
        return 1

    rows = collect_tx_rows(events, args.tx_id)
    if not rows:
        suffix = f" untuk tx_id={args.tx_id}" if args.tx_id else ""
        logger.error("tidak ada event tx_received/tx_forwarded%s", suffix)
        return 1

    output = write_csv(rows, args.output)
    coverage = nodes_reached(rows)
    logger.info("menulis %d event dari %d transaksi ke %s", len(rows), len(coverage), output)

    featured = args.tx_id.strip() if args.tx_id else _pick_featured_tx(rows)
    try:
        timeline = format_timeline(
            rows,
            events,
            featured,
            context_ms=args.context_ms,
            max_blocks=args.max_blocks,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    sys.stdout.write(timeline + "\n")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
