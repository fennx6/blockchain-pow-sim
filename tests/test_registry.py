"""Registry node coordinator."""

from __future__ import annotations

import pytest

from chainsim.coordinator.registry import NodeRecord, NodeRegistry, parse_node_entry


def test_parse_entry_with_explicit_port() -> None:
    node_id, rest_url, grpc_address = parse_node_entry("node-1=node-1:9001")
    assert node_id == "node-1"
    assert rest_url == "http://node-1:9001"
    assert grpc_address == "node-1:50051"


def test_parse_entry_with_scheme_and_default_port() -> None:
    node_id, rest_url, _ = parse_node_entry("node-2=http://10.0.0.2:8081")
    assert node_id == "node-2"
    assert rest_url == "http://10.0.0.2:8081"


def test_parse_entry_without_address_uses_default_port() -> None:
    _, rest_url, _ = parse_node_entry("node-3", default_rest_port=8081)
    assert rest_url == "http://node-3:8081"


@pytest.mark.parametrize("entry", ["", "=node-1:8081", "node-1=:8081", "node-1=node-1:70000"])
def test_parse_invalid_entries(entry: str) -> None:
    with pytest.raises(ValueError):
        parse_node_entry(entry)


def test_from_config_builds_full_mesh_peers() -> None:
    registry = NodeRegistry.from_config(
        ["node-1=node-1:8081", "node-2=node-2:8081", "node-3=node-3:8081"]
    )
    assert registry.node_ids() == ["node-1", "node-2", "node-3"]
    assert registry.peers_for("node-2") == ["node-1", "node-3"]


def test_peer_map_overrides_default_peers() -> None:
    registry = NodeRegistry.from_config(
        ["node-1=node-1:8081", "node-2=node-2:8081"],
        peer_map={"node-1": ["node-2"]},
    )
    assert registry.peers_for("node-1") == ["node-2"]
    assert registry.peers_for("node-2") == ["node-1"]


def test_register_replaces_and_unregister_removes(two_nodes: list[NodeRecord]) -> None:
    registry = NodeRegistry(two_nodes)
    registry.register(NodeRecord("node-1", "http://node-1:9999"))
    assert registry.get("node-1").rest_url == "http://node-1:9999"
    registry.unregister("node-2")
    assert registry.get("node-2") is None
    assert registry.peers_for("tidak-ada") == []
