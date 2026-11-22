"""Fixture bersama untuk unit test chainsim."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
for path in (SRC, REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from chainsim.coordinator.registry import NodeRecord  # noqa: E402


@pytest.fixture
def log_dir_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Arahkan log_event ke folder sementara agar repo tetap bersih."""
    target = tmp_path / "logs"
    monkeypatch.setenv("LOG_DIR", str(target))
    return target


@pytest.fixture
def two_nodes() -> list[NodeRecord]:
    return [
        NodeRecord("node-1", "http://node-1:8081"),
        NodeRecord("node-2", "http://node-2:8081"),
    ]
