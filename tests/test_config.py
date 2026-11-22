"""Pemuatan konfigurasi dan override environment."""

from __future__ import annotations

from pathlib import Path

import pytest

from chainsim.config import load_settings, resolve_node_id

SAMPLE_YAML = """
node:
  rest_port: 9001
  grpc_port: 9002
  peers: [node-2, node-3]
  block_size: 10
  confirmations: 2
coordinator:
  host: 127.0.0.1
  port: 9090
  nodes: ["node-1=node-1:9001"]
blockchain:
  difficulty: 3
  mining_mode: compete
  nonce_space: 5000
network:
  heartbeat_interval_s: 1.5
  heartbeat_timeout_s: 0.5
  grpc_timeout_s: 1.0
  tx_forward_timeout_s: 1.0
logging:
  log_dir: logs-test
  level: debug
"""


@pytest.fixture
def settings_file(tmp_path: Path) -> Path:
    path = tmp_path / "settings.yaml"
    path.write_text(SAMPLE_YAML, encoding="utf-8")
    return path


def test_loads_values_from_yaml(settings_file: Path) -> None:
    settings = load_settings(path=settings_file, env={})
    assert settings.node.rest_port == 9001
    assert settings.node.grpc_port == 9002
    assert settings.node.peers == ("node-2", "node-3")
    assert settings.blockchain.difficulty == 3
    assert settings.coordinator.port == 9090
    assert settings.network.heartbeat_timeout_s == 0.5
    assert settings.logging.level == "DEBUG"
    assert settings.source_path == settings_file


def test_environment_overrides_yaml(settings_file: Path) -> None:
    env = {
        "DIFFICULTY": "6",
        "MINING_MODE": "pool",
        "NODE_ID": "node-7",
        "PEERS": "node-8, node-9",
        "NODES": "node-8=node-8:8081",
        "HEARTBEAT_INTERVAL_S": "4",
        "LOG_DIR": "/tmp/chainsim-logs",
    }
    settings = load_settings(path=settings_file, env=env)
    assert settings.blockchain.difficulty == 6
    assert settings.blockchain.mining_mode == "pool"
    assert settings.node.node_id == "node-7"
    assert settings.node.peers == ("node-8", "node-9")
    assert settings.coordinator.nodes == ("node-8=node-8:8081",)
    assert settings.network.heartbeat_interval_s == 4.0
    assert settings.logging.log_dir == Path("/tmp/chainsim-logs")


def test_invalid_mining_mode_is_rejected(settings_file: Path) -> None:
    with pytest.raises(ValueError, match="MINING_MODE"):
        load_settings(path=settings_file, env={"MINING_MODE": "solo"})


def test_invalid_port_is_rejected(settings_file: Path) -> None:
    with pytest.raises(ValueError, match="NODE_REST_PORT"):
        load_settings(path=settings_file, env={"NODE_REST_PORT": "70000"})


def test_non_numeric_difficulty_is_rejected(settings_file: Path) -> None:
    with pytest.raises(ValueError, match="DIFFICULTY"):
        load_settings(path=settings_file, env={"DIFFICULTY": "sulit"})


def test_missing_file_falls_back_to_defaults(tmp_path: Path) -> None:
    settings = load_settings(path=tmp_path / "tidak-ada.yaml", env={})
    assert settings.blockchain.mining_mode == "compete"
    assert settings.coordinator.port == 8080
    assert settings.source_path is None


def test_resolve_node_id_prefers_environment() -> None:
    assert resolve_node_id({"NODE_ID": "node-3"}) == "node-3"
    assert resolve_node_id({}) != ""
