"""Pembangkit transaksi fiktif untuk beban uji.

Owner: Member B (AGENTS.md Section 3). The generator posts JSON
``{"sender", "receiver", "amount", "timestamp_ms"}`` objects to one REST target.
For E2 the target is a node's ``POST /tx`` (inside the Docker network) or the
coordinator proxy once C implements it (decision D2). No real wallet or key is
involved.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("tx_generator")

DEFAULT_RATE = 5.0
DEFAULT_DURATION_S = 60.0
DEFAULT_TARGET = "http://127.0.0.1:8080"
DEFAULT_TIMEOUT_S = 5.0
DEFAULT_SEED = 0
MAX_SLEEP_S = 0.05

PostJson = Callable[[str, dict[str, Any], float], None]


@dataclass(frozen=True)
class GeneratorResult:
    """Outcome of one generator run."""

    sent: int
    failed: int
    elapsed_s: float
    actual_rate: float


def build_payload(seq: int, timestamp_ms: int, rng: random.Random) -> dict[str, Any]:
    """Build one unique fake transaction payload."""
    if seq < 0:
        raise ValueError(f"seq must be >= 0, got {seq}")
    return {
        "sender": f"user-{seq:08d}",
        "receiver": f"merchant-{seq % 1000:03d}",
        "amount": round(rng.uniform(0.01, 10.0), 2),
        "timestamp_ms": timestamp_ms,
    }


def post_json(target: str, payload: dict[str, Any], timeout_s: float) -> None:
    """POST ``payload`` as JSON and raise on a non-2xx response."""
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        target,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:  # noqa: S310
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status} from {target}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} from {target}: {exc.reason}") from exc


def run_generator(
    *,
    target: str,
    rate: float,
    duration_s: float,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    seed: int = DEFAULT_SEED,
    post: PostJson = post_json,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> GeneratorResult:
    """Send transactions at ``rate`` per second for ``duration_s`` seconds."""
    if rate <= 0:
        raise ValueError(f"rate must be > 0, got {rate}")
    if duration_s <= 0:
        raise ValueError(f"duration_s must be > 0, got {duration_s}")
    if timeout_s <= 0:
        raise ValueError(f"timeout_s must be > 0, got {timeout_s}")
    if not target.strip():
        raise ValueError("target must not be empty")

    rng = random.Random(seed)
    interval = 1.0 / rate
    started = clock()
    next_send = started
    sent = 0
    failed = 0
    seq = 0

    while True:
        now = clock()
        if now - started >= duration_s:
            break
        if now < next_send:
            sleep(min(next_send - now, MAX_SLEEP_S))
            continue

        payload = build_payload(seq, int(time.time() * 1000), rng)
        try:
            post(target, payload, timeout_s)
            sent += 1
        except Exception as exc:
            failed += 1
            logger.warning("POST to %s failed: %s", target, exc)

        seq += 1
        next_send += interval
        if next_send < now:
            next_send = now + interval

    elapsed = max(clock() - started, 0.0)
    attempts = sent + failed
    return GeneratorResult(
        sent=sent,
        failed=failed,
        elapsed_s=elapsed,
        actual_rate=attempts / elapsed if elapsed > 0 else 0.0,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kirim transaksi fiktif ke jaringan")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="transaksi per detik")
    parser.add_argument(
        "--duration", type=float, default=DEFAULT_DURATION_S, help="durasi dalam detik"
    )
    parser.add_argument("--target", default=DEFAULT_TARGET, help="URL tujuan POST /tx")
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="timeout per request"
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="seed angka acak")
    parser.add_argument("--dry-run", action="store_true", help="cetak rencana tanpa mengirim")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.rate <= 0:
        logger.error("--rate must be > 0")
        return 2
    if args.duration <= 0:
        logger.error("--duration must be > 0")
        return 2
    if args.timeout <= 0:
        logger.error("--timeout must be > 0")
        return 2
    if not args.target.strip():
        logger.error("--target must not be empty")
        return 2

    planned = int(args.rate * args.duration)
    if args.dry_run:
        logger.info(
            "dry-run: target=%s rate=%.2f/s duration=%.1fs planned=%d",
            args.target,
            args.rate,
            args.duration,
            planned,
        )
        return 0

    result = run_generator(
        target=args.target,
        rate=args.rate,
        duration_s=args.duration,
        timeout_s=args.timeout,
        seed=args.seed,
    )
    logger.info(
        "sent=%d failed=%d elapsed=%.3fs actual_rate=%.2f tx/s",
        result.sent,
        result.failed,
        result.elapsed_s,
        result.actual_rate,
    )
    return 0 if result.failed == 0 and result.sent > 0 else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
