"""Pemuatan konfigurasi ``config/settings.yaml`` dengan override env.

Nilai numerik tidak boleh ditulis langsung di kode (AGENTS.md Bagian 12), jadi
semua modul membaca konfigurasi lewat modul ini. Override lewat environment
memungkinkan satu image Docker yang sama dipakai semua node.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS_PATH = REPO_ROOT / "config" / "settings.yaml"

MINING_MODES = ("compete", "pool")


@dataclass(frozen=True)
class NodeConfig:
    """Identitas dan port satu node worker."""

    node_id: str
    rest_port: int
    grpc_port: int
    peers: tuple[str, ...]
    block_size: int
    confirmations: int


@dataclass(frozen=True)
class CoordinatorConfig:
    """Alamat dashboard dan daftar node yang dipantau."""

    host: str
    port: int
    nodes: tuple[str, ...]


@dataclass(frozen=True)
class BlockchainConfig:
    """Parameter konsensus dan penambangan."""

    difficulty: int
    mining_mode: str
    nonce_space: int


@dataclass(frozen=True)
class NetworkConfig:
    """Timeout dan interval yang dipakai heartbeat serta gRPC."""

    heartbeat_interval_s: float
    heartbeat_timeout_s: float
    grpc_timeout_s: float
    tx_forward_timeout_s: float


@dataclass(frozen=True)
class LoggingConfig:
    """Lokasi dan level log JSONL."""

    log_dir: Path
    level: str


@dataclass(frozen=True)
class Settings:
    """Konfigurasi lengkap satu proses (node, coordinator, atau skrip)."""

    node: NodeConfig
    coordinator: CoordinatorConfig
    blockchain: BlockchainConfig
    network: NetworkConfig
    logging: LoggingConfig
    source_path: Path | None = None


def resolve_node_id(env: Mapping[str, str] | None = None) -> str:
    """Ambil ``NODE_ID`` dari env, atau pakai hostname container.

    Hostname dipakai agar ``docker compose up --scale node=N`` tetap memberi
    identitas unik tanpa harus mendefinisikan N variabel.
    """
    source = os.environ if env is None else env
    explicit = (source.get("NODE_ID") or "").strip()
    if explicit:
        return explicit
    hostname = socket.gethostname().strip()
    return hostname or "node-local"


def _split_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    return tuple(str(item).strip() for item in items if str(item).strip())


def _pick(env: Mapping[str, str], name: str, fallback: Any) -> Any:
    raw = env.get(name)
    if raw is None or str(raw).strip() == "":
        return fallback
    return raw


def _to_int(value: Any, name: str) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"konfigurasi {name} harus bilangan bulat, bukan {value!r}") from exc


def _to_float(value: Any, name: str) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"konfigurasi {name} harus bilangan, bukan {value!r}") from exc


def _validate_port(port: int, name: str) -> int:
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} harus di rentang 1..65535, bukan {port}")
    return port


def _validate_positive(value: float, name: str) -> float:
    if value <= 0:
        raise ValueError(f"{name} harus lebih besar dari 0, bukan {value}")
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"isi {path} harus mapping YAML, bukan {type(loaded).__name__}")
    return loaded


def load_settings(
    path: Path | str | None = None,
    env: Mapping[str, str] | None = None,
) -> Settings:
    """Baca YAML lalu timpa dengan environment.

    ``env={}`` dipakai di unit test agar tidak terpengaruh environment mesin.
    """
    source_env = os.environ if env is None else env
    settings_path = Path(path or source_env.get("CHAINSIM_SETTINGS") or DEFAULT_SETTINGS_PATH)
    data = _load_yaml(settings_path)

    node_raw: dict[str, Any] = data.get("node") or {}
    coordinator_raw: dict[str, Any] = data.get("coordinator") or {}
    chain_raw: dict[str, Any] = data.get("blockchain") or {}
    network_raw: dict[str, Any] = data.get("network") or {}
    logging_raw: dict[str, Any] = data.get("logging") or {}

    mining_mode = str(
        _pick(source_env, "MINING_MODE", chain_raw.get("mining_mode", "compete"))
    ).strip()
    if mining_mode not in MINING_MODES:
        raise ValueError(f"MINING_MODE harus salah satu dari {MINING_MODES}, bukan {mining_mode!r}")

    difficulty = _to_int(
        _pick(source_env, "DIFFICULTY", chain_raw.get("difficulty", 4)), "DIFFICULTY"
    )
    if difficulty < 1:
        raise ValueError(f"DIFFICULTY minimal 1, bukan {difficulty}")

    node = NodeConfig(
        node_id=resolve_node_id(source_env),
        rest_port=_validate_port(
            _to_int(
                _pick(source_env, "NODE_REST_PORT", node_raw.get("rest_port", 8081)),
                "NODE_REST_PORT",
            ),
            "NODE_REST_PORT",
        ),
        grpc_port=_validate_port(
            _to_int(
                _pick(source_env, "NODE_GRPC_PORT", node_raw.get("grpc_port", 50051)),
                "NODE_GRPC_PORT",
            ),
            "NODE_GRPC_PORT",
        ),
        peers=_split_list(_pick(source_env, "PEERS", node_raw.get("peers", []))),
        block_size=_to_int(
            _pick(source_env, "BLOCK_SIZE", node_raw.get("block_size", 50)), "BLOCK_SIZE"
        ),
        confirmations=_to_int(
            _pick(source_env, "CONFIRMATIONS", node_raw.get("confirmations", 3)), "CONFIRMATIONS"
        ),
    )
    if node.block_size < 1:
        raise ValueError(f"BLOCK_SIZE minimal 1, bukan {node.block_size}")
    if node.confirmations < 0:
        raise ValueError(f"CONFIRMATIONS tidak boleh negatif, bukan {node.confirmations}")

    coordinator = CoordinatorConfig(
        host=str(_pick(source_env, "COORDINATOR_HOST", coordinator_raw.get("host", "0.0.0.0"))),
        port=_validate_port(
            _to_int(
                _pick(source_env, "COORDINATOR_PORT", coordinator_raw.get("port", 8080)),
                "COORDINATOR_PORT",
            ),
            "COORDINATOR_PORT",
        ),
        nodes=_split_list(_pick(source_env, "NODES", coordinator_raw.get("nodes", []))),
    )

    blockchain = BlockchainConfig(
        difficulty=difficulty,
        mining_mode=mining_mode,
        nonce_space=_to_int(
            _pick(source_env, "NONCE_SPACE", chain_raw.get("nonce_space", 1_000_000_000)),
            "NONCE_SPACE",
        ),
    )
    if blockchain.nonce_space < 1:
        raise ValueError(f"NONCE_SPACE minimal 1, bukan {blockchain.nonce_space}")

    network = NetworkConfig(
        heartbeat_interval_s=_validate_positive(
            _to_float(
                _pick(
                    source_env, "HEARTBEAT_INTERVAL_S", network_raw.get("heartbeat_interval_s", 2.0)
                ),
                "HEARTBEAT_INTERVAL_S",
            ),
            "HEARTBEAT_INTERVAL_S",
        ),
        heartbeat_timeout_s=_validate_positive(
            _to_float(
                _pick(
                    source_env, "HEARTBEAT_TIMEOUT_S", network_raw.get("heartbeat_timeout_s", 1.0)
                ),
                "HEARTBEAT_TIMEOUT_S",
            ),
            "HEARTBEAT_TIMEOUT_S",
        ),
        grpc_timeout_s=_validate_positive(
            _to_float(
                _pick(source_env, "GRPC_TIMEOUT_S", network_raw.get("grpc_timeout_s", 2.0)),
                "GRPC_TIMEOUT_S",
            ),
            "GRPC_TIMEOUT_S",
        ),
        tx_forward_timeout_s=_validate_positive(
            _to_float(
                _pick(
                    source_env, "TX_FORWARD_TIMEOUT_S", network_raw.get("tx_forward_timeout_s", 2.0)
                ),
                "TX_FORWARD_TIMEOUT_S",
            ),
            "TX_FORWARD_TIMEOUT_S",
        ),
    )

    logging_config = LoggingConfig(
        log_dir=Path(str(_pick(source_env, "LOG_DIR", logging_raw.get("log_dir", "logs")))),
        level=str(_pick(source_env, "LOG_LEVEL", logging_raw.get("level", "INFO"))).upper(),
    )

    return Settings(
        node=node,
        coordinator=coordinator,
        blockchain=blockchain,
        network=network,
        logging=logging_config,
        source_path=settings_path if settings_path.is_file() else None,
    )
