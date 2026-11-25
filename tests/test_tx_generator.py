"""Tests for the fake transaction generator."""

from __future__ import annotations

import json
import random
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import tx_generator  # noqa: E402


def test_build_payload_is_unique_and_seeded() -> None:
    first = tx_generator.build_payload(0, 1_700_000_000_000, random.Random(7))
    again = tx_generator.build_payload(0, 1_700_000_000_000, random.Random(7))
    other = tx_generator.build_payload(1, 1_700_000_000_000, random.Random(7))
    assert first == again
    assert first["sender"] != other["sender"]
    assert first["receiver"].startswith("merchant-")
    assert 0.01 <= first["amount"] <= 10.0
    assert first["timestamp_ms"] == 1_700_000_000_000


def test_build_payload_rejects_negative_sequence() -> None:
    with pytest.raises(ValueError):
        tx_generator.build_payload(-1, 1, random.Random(0))


def test_run_generator_sends_at_requested_rate() -> None:
    calls: list[tuple[str, dict]] = []

    def fake_post(target: str, payload: dict, timeout_s: float) -> None:
        calls.append((target, payload))

    result = tx_generator.run_generator(
        target="http://example.invalid/tx",
        rate=1000.0,
        duration_s=0.02,
        post=fake_post,
    )
    assert result.sent > 0
    assert result.failed == 0
    assert result.sent == len(calls)
    assert result.elapsed_s > 0
    assert result.actual_rate > 0
    assert len({payload["sender"] for _, payload in calls}) == len(calls)


def test_run_generator_counts_failures() -> None:
    def failing_post(target: str, payload: dict, timeout_s: float) -> None:
        raise RuntimeError("boom")

    result = tx_generator.run_generator(
        target="http://example.invalid/tx",
        rate=1000.0,
        duration_s=0.01,
        post=failing_post,
    )
    assert result.sent == 0
    assert result.failed > 0


def test_run_generator_validates_arguments() -> None:
    with pytest.raises(ValueError):
        tx_generator.run_generator(target="x", rate=0, duration_s=1)
    with pytest.raises(ValueError):
        tx_generator.run_generator(target="x", rate=1, duration_s=0)
    with pytest.raises(ValueError):
        tx_generator.run_generator(target="x", rate=1, duration_s=1, timeout_s=0)
    with pytest.raises(ValueError):
        tx_generator.run_generator(target=" ", rate=1, duration_s=1)


def test_main_rejects_invalid_arguments() -> None:
    assert tx_generator.main(["--rate", "0"]) == 2
    assert tx_generator.main(["--duration", "0"]) == 2
    assert tx_generator.main(["--timeout", "0"]) == 2
    assert tx_generator.main(["--target", " "]) == 2


def test_main_dry_run_does_not_post() -> None:
    assert tx_generator.main(["--dry-run", "--target", "http://example.invalid/tx"]) == 0


def test_default_target_includes_tx_path() -> None:
    assert tx_generator.DEFAULT_TARGET == "http://127.0.0.1:8080/tx"
    parsed = tx_generator.build_parser().parse_args([])
    assert parsed.target == "http://127.0.0.1:8080/tx"


class _RecordingHandler(BaseHTTPRequestHandler):
    payloads: list[dict] = []
    status = 201

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        _RecordingHandler.payloads.append(json.loads(body))
        self.send_response(_RecordingHandler.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *_: object) -> None:
        return


def _serve() -> tuple[ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_post_json_sends_payload() -> None:
    _RecordingHandler.payloads.clear()
    _RecordingHandler.status = 201
    server, thread = _serve()
    try:
        payload = {"sender": "a", "receiver": "b", "amount": 1.0, "timestamp_ms": 1}
        tx_generator.post_json(f"http://127.0.0.1:{server.server_port}/tx", payload, 2.0)
        assert _RecordingHandler.payloads == [payload]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_post_json_raises_on_server_error() -> None:
    _RecordingHandler.payloads.clear()
    _RecordingHandler.status = 500
    server, thread = _serve()
    try:
        with pytest.raises(RuntimeError, match="HTTP 500"):
            tx_generator.post_json(
                f"http://127.0.0.1:{server.server_port}/tx",
                {"sender": "a", "receiver": "b", "amount": 1.0, "timestamp_ms": 1},
                2.0,
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
