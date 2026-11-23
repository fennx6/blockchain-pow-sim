"""Transaction and block gossip to peers.

Owner: Member B (AGENTS.md Section 3). The network is a full mesh, so the origin
node broadcasts once to every peer; receivers add the payload to their local
mempool/chain and do not re-broadcast. Every send is logged as
``tx_forwarded`` or ``block_forwarded`` with the destination node so the
transaction log required by Section 9.3 can be reconstructed.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from typing import Protocol

from chainsim.logging_utils import log_event
from chainsim.node.grpc_client import DEFAULT_TIMEOUT_S, NodeClient, PeerCallResult
from chainsim.types import Block, Transaction

logger = logging.getLogger(__name__)


class GossipClient(Protocol):
    """Minimal client surface used by ``Gossip`` (satisfied by ``NodeClient``)."""

    def submit_transaction(self, tx: Transaction, *, from_node: str = "") -> PeerCallResult: ...

    def announce_block(self, block: Block, *, from_node: str = "") -> PeerCallResult: ...

    def close(self) -> None: ...


ClientFactory = Callable[[str], GossipClient]


class Gossip:
    """Broadcast transactions and blocks to the configured peers."""

    def __init__(
        self,
        *,
        node_id: str,
        peers: Sequence[str],
        timeout_s: float = DEFAULT_TIMEOUT_S,
        client_factory: ClientFactory | None = None,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s!r}")

        self._node_id = node_id.strip()
        self._peers = self._clean_peers(peers)
        self._client_factory: ClientFactory = client_factory or (
            lambda address: NodeClient(address, timeout_s=timeout_s)
        )
        self._clients: dict[str, GossipClient] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _clean_peers(peers: Sequence[str]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(str(peer).strip() for peer in peers if str(peer).strip()))

    @property
    def node_id(self) -> str:
        return self._node_id

    def peers(self) -> list[str]:
        """Return a snapshot of the current peer list."""
        with self._lock:
            return list(self._peers)

    def set_peers(self, peers: Sequence[str]) -> None:
        """Replace the peer list and close clients that are no longer needed."""
        cleaned = self._clean_peers(peers)
        with self._lock:
            stale = [self._clients.pop(peer) for peer in list(self._clients) if peer not in cleaned]
            self._peers = cleaned
        for client in stale:
            self._close_client(client)

    def close(self) -> None:
        """Close every cached peer client."""
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            self._close_client(client)

    def __enter__(self) -> Gossip:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def broadcast_tx(self, tx: Transaction) -> list[PeerCallResult]:
        """Send ``tx`` to every peer and log one ``tx_forwarded`` event per peer."""
        results: list[PeerCallResult] = []
        for peer in self.peers():
            try:
                client = self._client_for(peer)
                result = client.submit_transaction(tx, from_node=self._node_id)
            except Exception as exc:
                result = PeerCallResult(peer, False, str(exc))
            results.append(result)
            self._safe_log(
                "tx_forwarded",
                tx_id=tx.tx_id,
                origin_node=tx.origin_node,
                from_node=self._node_id,
                to_node=peer,
                ok=result.ok,
                error="" if result.ok else result.message,
            )
        return results

    def broadcast_block(self, block: Block) -> list[PeerCallResult]:
        """Send ``block`` to every peer and log one ``block_forwarded`` event per peer."""
        results: list[PeerCallResult] = []
        for peer in self.peers():
            try:
                client = self._client_for(peer)
                result = client.announce_block(block, from_node=self._node_id)
            except Exception as exc:
                result = PeerCallResult(peer, False, str(exc))
            results.append(result)
            self._safe_log(
                "block_forwarded",
                index=block.index,
                block_hash=block.hash,
                from_node=self._node_id,
                to_node=peer,
                ok=result.ok,
                error="" if result.ok else result.message,
            )
        return results

    def _client_for(self, peer: str) -> GossipClient:
        with self._lock:
            if peer not in self._peers:
                raise ValueError(f"peer {peer!r} is not in the peer list")
            client = self._clients.get(peer)
            if client is None:
                client = self._client_factory(peer)
                self._clients[peer] = client
            return client

    def _safe_log(self, event: str, **fields: object) -> None:
        try:
            log_event(event, self._node_id, **fields)
        except Exception:
            logger.exception("failed to write %s event for %s", event, self._node_id)

    @staticmethod
    def _close_client(client: GossipClient) -> None:
        try:
            client.close()
        except Exception:
            logger.exception("failed to close gossip client")
