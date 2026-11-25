"""E2: gRPC latency, block propagation delay, and throughput.

Owner: Member B (AGENTS.md Section 3 and Section 9.1). The script reads the
JSONL event logs produced by the nodes and, optionally, probes ``GetStatus`` on
explicit gRPC targets. It never invents measurements: every row comes from a
logged event or a live RPC.

Metrics written to ``experiments/results/e2_propagation.csv``:

- ``grpc_rtt_ms_*``: live ``GetStatus`` round-trip time (optional).
- ``tx_forward_ms_*``: ``tx_received - tx_forwarded`` per transaction/peer.
- ``block_forward_ms_*``: ``block_received - block_forwarded`` per block/peer.
- ``block_propagation_ms_*``: ``block_received - block_mined`` per block/node.
- ``block_propagation_full_coverage_ms_*``: slowest node per block, only for
  blocks received by every expected node.
- ``block_propagation_coverage_ratio``: average fraction of expected nodes that
  received each block.
- ``block_throughput_bps`` and ``tx_throughput_tps``: mined blocks/transactions
  divided by the observation window.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_EXPERIMENTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _EXPERIMENTS_DIR.parent
_SRC_DIR = _REPO_ROOT / "src"
for _path in (_EXPERIMENTS_DIR, _SRC_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from chainsim.config import load_settings  # noqa: E402
from chainsim.node.grpc_client import NodeClient  # noqa: E402
from metrics import append_result, latency_stats, make_result_row  # noqa: E402
from runner import RESULTS_DIR, new_run_id  # noqa: E402

logger = logging.getLogger("e2_propagation")

EXPERIMENT_NAME = "e2_propagation"
COORDINATOR_ID = "coordinator"
DEFAULT_RTT_SAMPLES = 20
DEFAULT_TIMEOUT_S = 2.0


@dataclass(frozen=True)
class LogEvent:
    """One parsed JSONL event with its bookkeeping fields removed."""

    ts_ms: int
    node_id: str
    event: str
    fields: dict[str, Any]


@dataclass(frozen=True)
class PropagationStats:
    """Per-block propagation delays and coverage."""

    delays_ms: tuple[float, ...]
    full_coverage_ms: tuple[float, ...]
    blocks_total: int
    blocks_full: int
    coverage_ratio: float


@dataclass(frozen=True)
class ThroughputStats:
    """Mined blocks/transactions over the observation window."""

    blocks: int
    txs: int
    duration_s: float
    block_bps: float
    tx_tps: float


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _as_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _normalize_node(value: Any) -> str:
    """Strip URL scheme and port so ``node-2:50051`` matches ``node-2``."""
    text = _as_str(value)
    if not text:
        return ""
    if "://" in text:
        text = text.split("://", 1)[1]
    text = text.rstrip("/")
    if ":" in text and not text.startswith("["):
        text = text.rsplit(":", 1)[0]
    return text


def _event_ts(event: LogEvent, field_name: str | None = None) -> int:
    """Return ``fields[field_name]`` when present and valid, else ``event.ts_ms``."""
    if field_name:
        override = _as_int(event.fields.get(field_name))
        if override is not None:
            return override
    return event.ts_ms


def load_events(logs_dir: Path | str) -> list[LogEvent]:
    """Read every ``*.jsonl`` file, skip malformed lines, and sort by time."""
    directory = Path(logs_dir)
    events: list[LogEvent] = []
    for path in sorted(directory.glob("*.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            logger.warning("cannot read %s", path)
            continue
        for line in lines:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            ts_ms = _as_int(payload.get("ts_ms"))
            node_id = _as_str(payload.get("node_id"))
            event = _as_str(payload.get("event"))
            if ts_ms is None or not node_id or not event:
                continue
            fields = {
                key: value
                for key, value in payload.items()
                if key not in {"ts_ms", "node_id", "event"}
            }
            events.append(LogEvent(ts_ms, node_id, event, fields))
    events.sort(key=lambda item: (item.ts_ms, item.node_id, item.event))
    return events


def _first_times(
    events: Iterable[LogEvent],
    event_name: str,
    key_func: Callable[[LogEvent], tuple[str, str] | None],
    predicate: Callable[[LogEvent], bool] | None = None,
    *,
    ts_field: str | None = None,
) -> dict[tuple[str, str], int]:
    first: dict[tuple[str, str], int] = {}
    for event in events:
        if event.event != event_name:
            continue
        if predicate is not None and not predicate(event):
            continue
        key = key_func(event)
        if key is None:
            continue
        ts = _event_ts(event, ts_field)
        if key not in first or ts < first[key]:
            first[key] = ts
    return first


def _not_failed(event: LogEvent) -> bool:
    return event.fields.get("ok") is not False


def _not_rejected(event: LogEvent) -> bool:
    return event.fields.get("accepted") is not False


def _tx_forwarded_key(event: LogEvent) -> tuple[str, str] | None:
    tx_id = _as_str(event.fields.get("tx_id"))
    to_node = _normalize_node(event.fields.get("to_node"))
    return (tx_id, to_node) if tx_id and to_node else None


def _tx_received_key(event: LogEvent) -> tuple[str, str] | None:
    tx_id = _as_str(event.fields.get("tx_id"))
    receiver = _normalize_node(event.fields.get("to_node")) or _normalize_node(event.node_id)
    return (tx_id, receiver) if tx_id and receiver else None


def _block_forwarded_key(event: LogEvent) -> tuple[str, str] | None:
    block_hash = _as_str(event.fields.get("block_hash"))
    to_node = _normalize_node(event.fields.get("to_node"))
    return (block_hash, to_node) if block_hash and to_node else None


def _block_received_key(event: LogEvent) -> tuple[str, str] | None:
    block_hash = _as_str(event.fields.get("block_hash"))
    receiver = _normalize_node(event.fields.get("to_node")) or _normalize_node(event.node_id)
    return (block_hash, receiver) if block_hash and receiver else None


def _match_delays(
    sent: dict[tuple[str, str], int],
    received: dict[tuple[str, str], int],
) -> list[float]:
    delays: list[float] = []
    for key, sent_ts in sent.items():
        received_ts = received.get(key)
        if received_ts is None or received_ts < sent_ts:
            continue
        delays.append(float(received_ts - sent_ts))
    return delays


def tx_forward_delays(events: Iterable[LogEvent]) -> list[float]:
    """One-way transaction forwarding latency per (tx_id, destination)."""
    materialized = list(events)
    sent = _first_times(
        materialized, "tx_forwarded", _tx_forwarded_key, _not_failed, ts_field="sent_ts_ms"
    )
    received = _first_times(
        materialized, "tx_received", _tx_received_key, _not_rejected, ts_field="received_ts_ms"
    )
    return _match_delays(sent, received)


def block_forward_delays(events: Iterable[LogEvent]) -> list[float]:
    """One-way block forwarding latency per (block_hash, destination)."""
    materialized = list(events)
    sent = _first_times(
        materialized, "block_forwarded", _block_forwarded_key, _not_failed, ts_field="sent_ts_ms"
    )
    received = _first_times(
        materialized,
        "block_received",
        _block_received_key,
        _not_rejected,
        ts_field="received_ts_ms",
    )
    return _match_delays(sent, received)


def propagation_stats(
    events: Iterable[LogEvent],
    expected_nodes: Sequence[str],
) -> PropagationStats:
    """Delay from ``block_mined`` to each node's first accepted ``block_received``."""
    mined: dict[str, tuple[str, int]] = {}
    received: dict[str, dict[str, int]] = defaultdict(dict)

    for event in events:
        if event.event == "block_mined":
            block_hash = _as_str(event.fields.get("block_hash"))
            if block_hash:
                mined.setdefault(block_hash, (event.node_id, event.ts_ms))
        elif event.event == "block_received" and _not_rejected(event):
            block_hash = _as_str(event.fields.get("block_hash"))
            if block_hash:
                recv_ts = _event_ts(event, "received_ts_ms")
                node_map = received[block_hash]
                if event.node_id not in node_map or recv_ts < node_map[event.node_id]:
                    node_map[event.node_id] = recv_ts

    expected = tuple(dict.fromkeys(str(node) for node in expected_nodes if str(node)))
    delays: list[float] = []
    full_coverage: list[float] = []
    coverage_ratios: list[float] = []

    for block_hash, (miner, mined_ts) in mined.items():
        receivers = set(received.get(block_hash, {})) - {miner}
        targets = (set(expected) - {miner}) if expected else receivers
        if not targets:
            continue
        node_delays: list[float] = []
        for node_id in sorted(targets):
            received_ts = received[block_hash].get(node_id)
            if received_ts is None or received_ts < mined_ts:
                continue
            node_delays.append(float(received_ts - mined_ts))
        delays.extend(node_delays)
        coverage_ratios.append(len(node_delays) / len(targets))
        if len(node_delays) == len(targets):
            full_coverage.append(max(node_delays))

    if coverage_ratios:
        coverage_ratio = statistics.fmean(coverage_ratios)
    else:
        coverage_ratio = 1.0 if mined else 0.0

    return PropagationStats(
        delays_ms=tuple(delays),
        full_coverage_ms=tuple(full_coverage),
        blocks_total=len(mined),
        blocks_full=len(full_coverage),
        coverage_ratio=coverage_ratio,
    )


def canonical_block_events(events: Iterable[LogEvent]) -> list[LogEvent]:
    """Return one event per block that is still on the canonical chain.

    ``block_accepted`` is written by every node that adds a block, so events are
    deduplicated by ``block_hash``. Blocks listed in a later ``fork_switched``
    ``discarded_hashes`` field are removed. Logs from before the field existed
    fall back to unique ``block_mined`` events.
    """
    accepted: dict[str, LogEvent] = {}
    mined: dict[str, LogEvent] = {}
    discarded: set[str] = set()

    for event in events:
        if event.event == "block_accepted":
            block_hash = _as_str(event.fields.get("block_hash"))
            if block_hash:
                accepted.setdefault(block_hash, event)
        elif event.event == "block_mined":
            block_hash = _as_str(event.fields.get("block_hash"))
            if block_hash:
                mined.setdefault(block_hash, event)
        elif event.event == "fork_switched":
            raw = event.fields.get("discarded_hashes") or []
            if isinstance(raw, list | tuple):
                discarded.update(str(item).strip() for item in raw if str(item).strip())

    if accepted:
        return [event for block_hash, event in accepted.items() if block_hash not in discarded]
    return list(mined.values())


def throughput_stats(events: Iterable[LogEvent], duration_s: float = 0.0) -> ThroughputStats:
    """Confirmed block/transaction throughput over ``duration_s`` seconds.

    Blocks are deduplicated by hash and fork-discarded blocks are removed. When
    ``duration_s`` is zero, the span between the first and last confirmed block
    is used. A single block with no explicit duration therefore yields zero
    throughput instead of a fabricated number.
    """
    confirmed = canonical_block_events(events)
    blocks = len(confirmed)
    txs = sum(_as_int(event.fields.get("tx_count")) or 0 for event in confirmed)
    window = float(duration_s)
    if window <= 0 and len(confirmed) > 1:
        window = (
            max(event.ts_ms for event in confirmed) - min(event.ts_ms for event in confirmed)
        ) / 1000.0
    if window <= 0:
        return ThroughputStats(blocks, txs, 0.0, 0.0, 0.0)
    return ThroughputStats(
        blocks=blocks,
        txs=txs,
        duration_s=window,
        block_bps=blocks / window,
        tx_tps=txs / window,
    )


def measure_grpc_rtt(
    targets: Sequence[str],
    *,
    samples: int = DEFAULT_RTT_SAMPLES,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[float]:
    """Probe ``GetStatus`` on each target and return round-trip times in ms."""
    if samples < 1:
        raise ValueError(f"samples must be >= 1, got {samples}")
    if timeout_s <= 0:
        raise ValueError(f"timeout_s must be > 0, got {timeout_s}")
    measured: list[float] = []
    for target in targets:
        try:
            with NodeClient(target, timeout_s=timeout_s) as client:
                for _ in range(samples):
                    started = time.perf_counter_ns()
                    client.get_status()
                    measured.append((time.perf_counter_ns() - started) / 1_000_000)
        except Exception as exc:
            logger.warning("GetStatus probe to %s failed: %s", target, exc)
    return measured


def parse_nodes(raw: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))


def infer_nodes(events: Iterable[LogEvent]) -> tuple[str, ...]:
    return tuple(sorted({event.node_id for event in events if event.node_id != COORDINATOR_ID}))


def infer_mode(events: Iterable[LogEvent]) -> str:
    """Read the mode recorded by the nodes instead of the host settings."""
    for event in events:
        if event.event != "block_mined":
            continue
        value = _as_str(event.fields.get("mode"))
        if value in {"compete", "pool"}:
            return value
    return ""


def infer_difficulty(events: Iterable[LogEvent]) -> int | None:
    """Read the difficulty recorded by the nodes, when a block exists."""
    for event in events:
        if event.event != "block_mined":
            continue
        value = _as_int(event.fields.get("difficulty"))
        if value is not None and value >= 1:
            return value
    return None


def _latency_rows(
    prefix: str,
    samples: Sequence[float],
    *,
    run_id: str,
    n_nodes: int,
    mode: str,
    difficulty: int,
) -> list[dict[str, Any]]:
    if not samples:
        logger.warning("no samples for %s", prefix)
        return []
    rows: list[dict[str, Any]] = []
    for suffix, value in latency_stats(samples).items():
        rows.append(
            make_result_row(
                run_id=run_id,
                experiment=EXPERIMENT_NAME,
                metric=f"{prefix}_{suffix}",
                value=value,
                unit="samples" if suffix == "count" else "ms",
                n_nodes=n_nodes,
                mode=mode,
                difficulty=difficulty,
                trial=1,
            )
        )
    return rows


def build_rows(
    *,
    events: Sequence[LogEvent],
    nodes: Sequence[str],
    duration_s: float,
    rtt_samples: Sequence[float],
    run_id: str,
    mode: str,
    difficulty: int,
) -> list[dict[str, Any]]:
    """Turn parsed events into rows matching the Section 9.2 CSV format."""
    propagation = propagation_stats(events, nodes)
    throughput = throughput_stats(events, duration_s)
    rows: list[dict[str, Any]] = []
    rows.extend(
        _latency_rows(
            "grpc_rtt_ms",
            rtt_samples,
            run_id=run_id,
            n_nodes=len(nodes),
            mode=mode,
            difficulty=difficulty,
        )
    )
    rows.extend(
        _latency_rows(
            "tx_forward_ms",
            tx_forward_delays(events),
            run_id=run_id,
            n_nodes=len(nodes),
            mode=mode,
            difficulty=difficulty,
        )
    )
    rows.extend(
        _latency_rows(
            "block_forward_ms",
            block_forward_delays(events),
            run_id=run_id,
            n_nodes=len(nodes),
            mode=mode,
            difficulty=difficulty,
        )
    )
    rows.extend(
        _latency_rows(
            "block_propagation_ms",
            propagation.delays_ms,
            run_id=run_id,
            n_nodes=len(nodes),
            mode=mode,
            difficulty=difficulty,
        )
    )
    rows.extend(
        _latency_rows(
            "block_propagation_full_coverage_ms",
            propagation.full_coverage_ms,
            run_id=run_id,
            n_nodes=len(nodes),
            mode=mode,
            difficulty=difficulty,
        )
    )

    common = {
        "run_id": run_id,
        "experiment": EXPERIMENT_NAME,
        "n_nodes": len(nodes),
        "mode": mode,
        "difficulty": difficulty,
        "trial": 1,
    }
    rows.append(
        make_result_row(
            metric="block_propagation_coverage_ratio",
            value=propagation.coverage_ratio,
            unit="ratio",
            **common,
        )
    )
    rows.append(
        make_result_row(
            metric="blocks_mined",
            value=float(propagation.blocks_total),
            unit="blocks",
            **common,
        )
    )
    if throughput.duration_s > 0:
        rows.append(
            make_result_row(
                metric="observation_duration_s",
                value=throughput.duration_s,
                unit="s",
                **common,
            )
        )
        rows.append(
            make_result_row(
                metric="block_throughput_bps",
                value=throughput.block_bps,
                unit="blocks/s",
                **common,
            )
        )
        rows.append(
            make_result_row(
                metric="tx_throughput_tps",
                value=throughput.tx_tps,
                unit="tx/s",
                **common,
            )
        )
    else:
        logger.warning("throughput skipped: observation window is zero")
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E2: latency, propagation, throughput")
    parser.add_argument("--logs-dir", type=Path, default=_REPO_ROOT / "logs")
    parser.add_argument("--nodes", default="", help="comma-separated expected node IDs")
    parser.add_argument(
        "--mode",
        choices=("compete", "pool"),
        default=None,
        help="override mode metadata; default is inferred from block_mined logs",
    )
    parser.add_argument(
        "--difficulty",
        type=int,
        default=None,
        help="override difficulty metadata; default is inferred from block_mined logs",
    )
    parser.add_argument(
        "--grpc-target",
        action="append",
        default=[],
        help="host:port for a live GetStatus RTT probe; repeatable",
    )
    parser.add_argument("--rtt-samples", type=int, default=DEFAULT_RTT_SAMPLES)
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="observation window in seconds; 0 derives it from block_mined events",
    )
    parser.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.rtt_samples < 1:
        logger.error("--rtt-samples must be >= 1")
        return 2
    if args.duration < 0:
        logger.error("--duration must be >= 0")
        return 2
    if args.difficulty is not None and args.difficulty < 1:
        logger.error("--difficulty must be >= 1")
        return 2

    events = load_events(args.logs_dir)
    if not events:
        logger.error("no events found in %s", args.logs_dir)
        return 2

    nodes = parse_nodes(args.nodes) or infer_nodes(events)
    if not nodes:
        logger.error("no node IDs found; pass --nodes explicitly")
        return 2

    settings = load_settings()
    mode = args.mode or infer_mode(events) or settings.blockchain.mining_mode
    difficulty = args.difficulty if args.difficulty is not None else infer_difficulty(events)
    if difficulty is None:
        difficulty = settings.blockchain.difficulty
    run_id = args.run_id or new_run_id("e2")
    rtt_samples = (
        measure_grpc_rtt(
            args.grpc_target,
            samples=args.rtt_samples,
            timeout_s=settings.network.grpc_timeout_s,
        )
        if args.grpc_target
        else []
    )

    rows = build_rows(
        events=events,
        nodes=nodes,
        duration_s=args.duration,
        rtt_samples=rtt_samples,
        run_id=run_id,
        mode=mode,
        difficulty=difficulty,
    )
    if not rows:
        logger.error("no E2 metrics could be computed from %s", args.logs_dir)
        return 2

    if args.dry_run:
        for row in rows:
            print(f"{row['metric']}={row['value']} {row['unit']}")
        return 0

    output = args.output_dir / f"{EXPERIMENT_NAME}.csv"
    for row in rows:
        append_result(output, row)
    logger.info("wrote %d rows to %s", len(rows), output)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
