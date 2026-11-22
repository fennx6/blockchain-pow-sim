"""Unit tests for atomic local chain storage."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from chainsim.core.block import build_block_template, create_genesis_block
from chainsim.core.pow import mine
from chainsim.core.storage import load_chain, save_chain
from chainsim.core.transaction import create_transaction


def _sample_chain() -> list:
    genesis = create_genesis_block(difficulty=1)
    tx = create_transaction("alice", "bob", 3.5, "node-1", timestamp_ms=100)
    b1 = mine(
        build_block_template(
            1, genesis.hash, [tx], difficulty=1, miner_id="node-1", timestamp_ms=200
        ),
        0,
        10_000,
        threading.Event(),
    )
    assert b1 is not None
    return [genesis, b1]


def test_load_chain_returns_empty_list_when_file_missing(tmp_path: Path) -> None:
    assert load_chain(tmp_path / "missing" / "chain.json") == []


def test_save_and_load_chain_round_trip(tmp_path: Path) -> None:
    target = tmp_path / "data" / "chain.json"
    chain = _sample_chain()

    save_chain(target, chain)
    loaded = load_chain(target)

    assert loaded == chain
    # Atomic save must not leave stray temp files behind.
    assert [p.name for p in target.parent.iterdir()] == ["chain.json"]


def test_save_chain_overwrites_existing_file_cleanly(tmp_path: Path) -> None:
    target = tmp_path / "chain.json"
    chain = _sample_chain()

    save_chain(target, [chain[0]])
    assert len(load_chain(target)) == 1

    save_chain(target, chain)
    assert load_chain(target) == chain


def test_load_chain_rejects_corrupt_or_non_list_json(tmp_path: Path) -> None:
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ValueError):
        load_chain(corrupt)

    wrong_type = tmp_path / "object.json"
    wrong_type.write_text('{"blocks": []}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_chain(wrong_type)

    with pytest.raises(ValueError):
        load_chain(tmp_path)
