"""Catch-up and fork reconciliation against peers.

Owner: Member B (AGENTS.md Section 3). ``catch_up`` is the contract function from
Section 6.2: it pulls missing blocks from peers and returns the number of blocks
fetched. If a peer is on a heavier fork, the local chain is replaced through
``Blockchain.try_replace``, which also returns discarded transactions to the
mempool.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from typing import Protocol

from chainsim.core.chain import Blockchain
from chainsim.logging_utils import log_event
from chainsim.node.grpc_client import DEFAULT_TIMEOUT_S, NodeClient
from chainsim.types import AddResult, Block, NodeStatus

logger = logging.getLogger(__name__)


class SyncClient(Protocol):
    """Minimal peer client surface used by ``catch_up`` (satisfied by ``NodeClient``)."""

    def get_status(self) -> NodeStatus: ...

    def get_blocks(self, from_index: int) -> list[Block]: ...

    def close(self) -> None: ...


ClientFactory = Callable[[str], SyncClient]
PeersProvider = Sequence[str] | Callable[[], Sequence[str]]


def _safe_log(node_id: str, event: str, **fields: object) -> None:
    if not node_id:
        return
    try:
        log_event(event, node_id, **fields)
    except Exception:
        logger.exception("failed to write %s event for %s", event, node_id)


def _apply_blocks(chain: Blockchain, blocks: Sequence[Block]) -> tuple[int, bool]:
    """Add blocks in order; return ``(added, needs_full_sync)``."""
    added = 0
    needs_full_sync = False
    for block in blocks:
        result = chain.add_block(block)
        if result in (AddResult.ACCEPTED, AddResult.FORK_SWITCHED):
            added += 1
        elif result in (AddResult.ORPHAN, AddResult.STORED_SIDE):
            needs_full_sync = True
            break
        elif result == AddResult.INVALID:
            break
    return added, needs_full_sync


def catch_up(
    chain: Blockchain,
    peers: Sequence[str],
    *,
    node_id: str = "",
    timeout_s: float = DEFAULT_TIMEOUT_S,
    client_factory: ClientFactory | None = None,
) -> int:
    """Pull blocks from peers that are ahead of ``chain``.

    Returns the number of blocks fetched from peers (including a full-chain fetch
    used to resolve a fork). The local chain is only changed through
    ``Blockchain.add_block`` and ``Blockchain.try_replace``.
    """
    if timeout_s <= 0:
        raise ValueError(f"timeout_s must be > 0, got {timeout_s!r}")
    factory: ClientFactory = client_factory or (
        lambda address: NodeClient(address, timeout_s=timeout_s)
    )

    total_pulled = 0
    seen_peers: set[str] = set()
    for raw_peer in peers:
        peer = str(raw_peer).strip()
        if not peer or peer in seen_peers:
            continue
        seen_peers.add(peer)
        try:
            client = factory(peer)
        except Exception as exc:
            logger.warning("sync: cannot create client for %s: %s", peer, exc)
            continue
        try:
            total_pulled += _catch_up_from_peer(chain, peer, client, node_id=node_id)
        finally:
            try:
                client.close()
            except Exception:
                logger.exception("sync: failed to close client for %s", peer)
    return total_pulled


def _catch_up_from_peer(
    chain: Blockchain,
    peer: str,
    client: SyncClient,
    *,
    node_id: str,
) -> int:
    local_height = chain.height()
    try:
        status = client.get_status()
    except Exception as exc:
        logger.warning("sync: status from %s failed: %s", peer, exc)
        return 0
    if status.height <= local_height:
        return 0

    _safe_log(
        node_id,
        "sync_start",
        peer=peer,
        local_height=local_height,
        peer_height=status.height,
    )

    try:
        blocks = client.get_blocks(local_height + 1)
    except Exception as exc:
        logger.warning("sync: get_blocks from %s failed: %s", peer, exc)
        _safe_log(
            node_id,
            "sync_done",
            peer=peer,
            from_height=local_height,
            to_height=chain.height(),
            blocks_pulled=0,
            blocks_added=0,
            fork_replaced=False,
            error=str(exc),
        )
        return 0

    pulled = len(blocks)
    _added, needs_full_sync = _apply_blocks(chain, blocks)

    fork_replaced = False
    if needs_full_sync or (blocks and chain.height() == local_height):
        try:
            full_chain = client.get_blocks(0)
        except Exception as exc:
            logger.warning("sync: full get_blocks from %s failed: %s", peer, exc)
            full_chain = []
        if full_chain:
            pulled += len(full_chain)
            fork_replaced = chain.try_replace(full_chain)

    _safe_log(
        node_id,
        "sync_done",
        peer=peer,
        from_height=local_height,
        to_height=chain.height(),
        blocks_pulled=pulled,
        blocks_added=max(0, chain.height() - local_height),
        fork_replaced=fork_replaced,
    )
    return pulled


class SyncService:
    """Periodic catch-up loop that can also be triggered on demand."""

    def __init__(
        self,
        *,
        node_id: str,
        chain: Blockchain,
        peers_provider: PeersProvider,
        interval_s: float,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        client_factory: ClientFactory | None = None,
        on_chain_updated: Callable[[], None] | None = None,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if interval_s <= 0:
            raise ValueError(f"interval_s must be > 0, got {interval_s!r}")
        if timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s!r}")

        self._node_id = node_id.strip()
        self._chain = chain
        self._peers_provider = peers_provider
        self._interval_s = interval_s
        self._timeout_s = timeout_s
        self._client_factory = client_factory
        self._on_chain_updated = on_chain_updated
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def node_id(self) -> str:
        return self._node_id

    def peers(self) -> list[str]:
        raw = self._peers_provider() if callable(self._peers_provider) else self._peers_provider
        return [str(peer).strip() for peer in raw if str(peer).strip()]

    def sync_now(self, peers: Sequence[str] | None = None) -> int:
        """Run one catch-up synchronously (serialized against the background loop)."""
        with self._lock:
            before_tip = self._chain.tip().hash
            target = list(peers) if peers is not None else self.peers()
            pulled = catch_up(
                self._chain,
                target,
                node_id=self._node_id,
                timeout_s=self._timeout_s,
                client_factory=self._client_factory,
            )
            after_tip = self._chain.tip().hash
        if after_tip != before_tip and self._on_chain_updated is not None:
            try:
                self._on_chain_updated()
            except Exception:
                logger.exception("on_chain_updated callback failed for %s", self._node_id)
        return pulled

    def request_sync(self) -> None:
        """Ask the background loop to catch up soon without blocking the caller."""
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._wake.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"sync-{self._node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout_s)
            if thread.is_alive():
                logger.warning(
                    "sync thread for %s did not stop within %.1fs", self._node_id, timeout_s
                )
            else:
                self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.sync_now()
            except Exception:
                logger.exception("periodic sync failed for %s", self._node_id)
            self._wake.wait(self._interval_s)
            self._wake.clear()
