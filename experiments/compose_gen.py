"""Bangkitkan file compose untuk N node (dipakai E4: 2, 4, 8, 16 node).

Compose default memakai 6 service bernama dengan volume per node (keputusan D3
di docs/decisions.md), sehingga ``--scale`` tidak bisa dipakai tanpa kehilangan
identitas dan volume per node. Generator ini menurunkan compose baru dari
template yang sama agar image, environment, dan batas CPU tetap konsisten.
"""

from __future__ import annotations

import argparse
import copy
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEMPLATE = REPO_ROOT / "docker-compose.yml"
GENERATED_DIR = Path(__file__).resolve().parent / ".generated"
NODE_PREFIX = "node-"
MAX_NODES = 64
DEFAULT_REST_PORT = "8081"

logger = logging.getLogger("compose_gen")


@dataclass(frozen=True)
class ComposeSpec:
    """Parameter compose yang dibangkitkan untuk satu konfigurasi E4."""

    n_nodes: int
    project: str = "chainsim-e4"
    difficulty: int | None = None
    mining_mode: str | None = None
    template: Path = DEFAULT_TEMPLATE


def build_compose(spec: ComposeSpec) -> dict[str, Any]:
    """Kembalikan struktur compose (dict) untuk ``spec.n_nodes`` node."""
    if not 1 <= spec.n_nodes <= MAX_NODES:
        raise ValueError(f"n_nodes harus di rentang 1..{MAX_NODES}, bukan {spec.n_nodes}")

    data = yaml.safe_load(spec.template.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"template {spec.template} bukan mapping YAML")
    services = data.get("services")
    if not isinstance(services, dict) or "coordinator" not in services:
        raise ValueError(f"template {spec.template} tidak punya service coordinator")
    template_service = services.get(f"{NODE_PREFIX}1")
    if not isinstance(template_service, dict):
        raise ValueError(
            f"template {spec.template} tidak punya service {NODE_PREFIX}1 sebagai acuan"
        )

    template_service = copy.deepcopy(template_service)
    rest_port = str(
        (template_service.get("environment") or {}).get("NODE_REST_PORT", DEFAULT_REST_PORT)
    )

    for key in [name for name in services if name.startswith(NODE_PREFIX)]:
        del services[key]
    volumes: dict[str, Any] = data.get("volumes") or {}
    for key in [name for name in volumes if name.startswith(NODE_PREFIX)]:
        del volumes[key]
    for key in [name for name in data if name.startswith("x-")]:
        del data[key]

    node_ids = [f"{NODE_PREFIX}{index}" for index in range(1, spec.n_nodes + 1)]
    node_services: dict[str, Any] = {}
    for node_id in node_ids:
        service = copy.deepcopy(template_service)
        environment = dict(service.get("environment") or {})
        environment["NODE_ID"] = node_id
        environment["PEERS"] = ",".join(other for other in node_ids if other != node_id)
        if spec.difficulty is not None:
            environment["DIFFICULTY"] = str(spec.difficulty)
        if spec.mining_mode is not None:
            environment["MINING_MODE"] = spec.mining_mode
        service["environment"] = environment
        service["volumes"] = ["./logs:/app/logs", f"{node_id}-data:/app/data"]
        node_services[node_id] = service
        volumes[f"{node_id}-data"] = None

    coordinator = services["coordinator"]
    coordinator_environment = dict(coordinator.get("environment") or {})
    coordinator_environment["NODES"] = ",".join(
        f"{node_id}={node_id}:{rest_port}" for node_id in node_ids
    )
    coordinator["environment"] = coordinator_environment

    data["name"] = spec.project
    data["services"] = {"coordinator": coordinator, **node_services}
    data["volumes"] = volumes
    return data


def write_compose(spec: ComposeSpec, path: Path | None = None) -> Path:
    """Tulis compose hasil generate dan kembalikan lokasinya."""
    target = path or GENERATED_DIR / f"docker-compose.n{spec.n_nodes}.yml"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = yaml.safe_dump(
        build_compose(spec),
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )
    target.write_text(payload, encoding="utf-8")
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bangkitkan compose untuk N node")
    parser.add_argument("--nodes", type=int, required=True, help="jumlah node worker")
    parser.add_argument("--project", default="chainsim-e4", help="nama project compose")
    parser.add_argument("--difficulty", type=int, default=None, help="override DIFFICULTY")
    parser.add_argument(
        "--mode",
        choices=("compete", "pool"),
        default=None,
        help="override MINING_MODE",
    )
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    parser.add_argument("--output", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spec = ComposeSpec(
        n_nodes=args.nodes,
        project=args.project,
        difficulty=args.difficulty,
        mining_mode=args.mode,
        template=args.template,
    )
    try:
        target = write_compose(spec, args.output)
    except (OSError, ValueError) as exc:
        logger.error("%s", exc)
        return 2
    logger.info("compose %d node ditulis: %s", spec.n_nodes, target)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
