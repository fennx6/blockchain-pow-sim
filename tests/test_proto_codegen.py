"""Tests for the gRPC proto contract and code generation (Owner: Member B)."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("grpc_tools")

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
PROTO_PATH = REPO_ROOT / "proto" / "blockchain.proto"


def _load_gen_proto():
    spec = importlib.util.spec_from_file_location("gen_proto", SCRIPTS_DIR / "gen_proto.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_proto_declares_required_services() -> None:
    text = PROTO_PATH.read_text(encoding="utf-8")
    assert "service NodeService" in text
    assert "service CoordinatorService" in text
    for rpc in (
        "SubmitTransaction",
        "AnnounceBlock",
        "GetBlocks",
        "GetStatus",
        "StartWork",
        "StopWork",
    ):
        assert f"rpc {rpc}" in text
    for rpc in ("Register", "Heartbeat", "SubmitWorkResult", "GetPeers", "SubmitTemplate"):
        assert f"rpc {rpc}" in text


def test_generate_compiles_and_imports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gen_proto = _load_gen_proto()
    pb2_path, grpc_path = gen_proto.generate(PROTO_PATH, tmp_path, package=None)
    assert pb2_path.is_file()
    assert grpc_path.is_file()

    # Generated modules are named after the proto, so clear any cached copy
    # from a previous test before importing this fresh build.
    for name in ("blockchain_pb2", "blockchain_pb2_grpc"):
        sys.modules.pop(name, None)
    monkeypatch.syspath_prepend(str(tmp_path))

    pb2 = importlib.import_module("blockchain_pb2")
    grpc_mod = importlib.import_module("blockchain_pb2_grpc")
    assert hasattr(pb2, "TransactionMsg")
    assert hasattr(pb2, "BlockMsg")
    assert hasattr(pb2, "WorkAssignmentMsg")
    assert hasattr(grpc_mod, "NodeServiceStub")
    assert hasattr(grpc_mod, "CoordinatorServiceStub")

    tx = pb2.TransactionMsg(
        tx_id="tx-1",
        sender="addr-a",
        receiver="addr-b",
        amount=1.5,
        timestamp_ms=1_700_000_000_000,
        origin_node="node-1",
    )
    assert pb2.TransactionMsg.FromString(tx.SerializeToString()) == tx

    block = pb2.BlockMsg(
        index=1,
        prev_hash="0" * 64,
        timestamp_ms=1_700_000_000_001,
        transactions=[tx],
        merkle_root="merkle",
        difficulty=4,
        nonce=7,
        miner_id="node-1",
        hash="hash",
    )
    assert block.transactions[0].tx_id == "tx-1"
    assert block.nonce == 7


def test_rewrite_grpc_import(tmp_path: Path) -> None:
    gen_proto = _load_gen_proto()
    path = tmp_path / "blockchain_pb2_grpc.py"
    path.write_text("import blockchain_pb2 as blockchain__pb2\n", encoding="utf-8")

    gen_proto.rewrite_grpc_import(path, "chainsim.generated")
    assert "from chainsim.generated import blockchain_pb2 as blockchain__pb2" in path.read_text(
        encoding="utf-8"
    )

    # Running it again must not duplicate the import line.
    gen_proto.rewrite_grpc_import(path, "chainsim.generated")
    text = path.read_text(encoding="utf-8")
    assert text.count("from chainsim.generated import") == 1
    assert text.count("import blockchain_pb2 as") == 1
