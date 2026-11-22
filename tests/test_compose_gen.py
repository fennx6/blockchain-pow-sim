"""Generator compose untuk konfigurasi E4."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from experiments.compose_gen import ComposeSpec, build_compose, write_compose


def test_generates_requested_number_of_nodes() -> None:
    data = build_compose(ComposeSpec(n_nodes=4))
    services = data["services"]
    assert list(services) == ["coordinator", "node-1", "node-2", "node-3", "node-4"]
    assert set(data["volumes"]) == {
        "node-1-data",
        "node-2-data",
        "node-3-data",
        "node-4-data",
    }


def test_peers_exclude_self() -> None:
    data = build_compose(ComposeSpec(n_nodes=3))
    peers = data["services"]["node-2"]["environment"]["PEERS"]
    assert peers == "node-1,node-3"


def test_coordinator_knows_all_nodes() -> None:
    data = build_compose(ComposeSpec(n_nodes=2))
    nodes = data["services"]["coordinator"]["environment"]["NODES"]
    assert nodes == "node-1=node-1:8081,node-2=node-2:8081"


def test_template_values_are_preserved() -> None:
    data = build_compose(ComposeSpec(n_nodes=2))
    node = data["services"]["node-1"]
    assert node["cpus"] == "1.0"
    assert node["build"]["dockerfile"] == "docker/Dockerfile.node"
    assert node["environment"]["LOG_DIR"] == "/app/logs"
    assert data["services"]["coordinator"]["ports"] == ["127.0.0.1:8080:8080"]
    assert not [key for key in data if key.startswith("x-")]


def test_difficulty_and_mode_can_be_overridden() -> None:
    data = build_compose(ComposeSpec(n_nodes=2, difficulty=6, mining_mode="pool"))
    environment = data["services"]["node-2"]["environment"]
    assert environment["DIFFICULTY"] == "6"
    assert environment["MINING_MODE"] == "pool"


def test_project_name_is_applied() -> None:
    data = build_compose(ComposeSpec(n_nodes=2, project="chainsim-e4-8"))
    assert data["name"] == "chainsim-e4-8"


@pytest.mark.parametrize("count", [0, -1, 65])
def test_invalid_node_counts_are_rejected(count: int) -> None:
    with pytest.raises(ValueError, match="n_nodes"):
        build_compose(ComposeSpec(n_nodes=count))


def test_missing_template_service_is_rejected(tmp_path: Path) -> None:
    template = tmp_path / "compose.yml"
    template.write_text("services:\n  coordinator:\n    image: x\n", encoding="utf-8")
    with pytest.raises(ValueError, match="node-1"):
        build_compose(ComposeSpec(n_nodes=2, template=template))


def test_write_compose_round_trips(tmp_path: Path) -> None:
    target = write_compose(ComposeSpec(n_nodes=2), tmp_path / "compose-n2.yml")
    assert target.is_file()
    reloaded = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert reloaded == build_compose(ComposeSpec(n_nodes=2))


def test_sixteen_nodes_are_supported() -> None:
    data = build_compose(ComposeSpec(n_nodes=16))
    assert len(data["services"]) == 17
    assert len(data["volumes"]) == 16
    assert data["services"]["node-16"]["environment"]["NODE_ID"] == "node-16"
