"""Node runtime: wire core, storage, REST, gRPC, gossip, sync, and mining.

Owner: Member B (AGENTS.md Section 3). The container CMD
``python -m chainsim.node.main`` builds one ``NodeRuntime`` per node. The
runtime is deliberately thin: validation and fork choice stay in ``core``,
transport stays in ``grpc_server``/``grpc_client``, and this module only decides
which callback runs when an event arrives.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Sequence
from pathlib import Path

from flask import Flask

from chainsim.config import Settings, load_settings
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import compute_merkle_root
from chainsim.core.chain import Blockchain
from chainsim.core.mempool import Mempool
from chainsim.core.pow import is_valid_pow
from chainsim.logging_utils import log_event
from chainsim.node.api import create_app
from chainsim.node.gossip import ClientFactory as GossipClientFactory
from chainsim.node.gossip import Gossip
from chainsim.node.grpc_client import CoordinatorClient
from chainsim.node.grpc_server import NodeService, build_server
from chainsim.node.miner import Miner
from chainsim.node.sync import ClientFactory as SyncClientFactory
from chainsim.node.sync import SyncService
from chainsim.types import AddResult, Block, NodeStatus, Transaction, WorkAssignment

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = "data"
DEFAULT_BIND_HOST = "0.0.0.0"


def _clean_peers(peers: Sequence[str], node_id: str) -> tuple[str, ...]:
    """Drop empty entries, self references, and duplicates."""
    cleaned: list[str] = []
    for raw in peers:
        text = str(raw).strip()
        if not text:
            continue
        host = text.split("://", 1)[-1].rstrip("/")
        if host == node_id or host.startswith(f"{node_id}:"):
            continue
        cleaned.append(text)
    return tuple(dict.fromkeys(cleaned))


def _storage_path(data_dir: Path | str | None, node_id: str) -> Path:
    base = (
        Path(data_dir)
        if data_dir is not None
        else Path(os.environ.get("DATA_DIR", DEFAULT_DATA_DIR))
    )
    safe_id = re.sub(r"[^A-Za-z0-9._-]", "_", node_id)
    return base / f"{safe_id}.json"


def _coordinator_client_from_env(settings: Settings) -> CoordinatorClient | None:
    address = (os.environ.get("COORDINATOR_GRPC") or "").strip()
    if not address:
        return None
    return CoordinatorClient(address, timeout_s=settings.network.grpc_timeout_s)


class NodeRuntime:
    """One node process: chain + mempool + REST + gRPC + gossip + sync + miner."""

    def __init__(
        self,
        settings: Settings,
        *,
        data_dir: Path | str | None = None,
        grpc_port: int | None = None,
        sync_client_factory: SyncClientFactory | None = None,
        gossip_client_factory: GossipClientFactory | None = None,
        coordinator_client: CoordinatorClient | None = None,
    ) -> None:
        self.settings = settings
        self.node_id = settings.node.node_id.strip()
        if not self.node_id:
            raise ValueError("node_id must be a non-empty string")

        self.peers = _clean_peers(settings.node.peers, self.node_id)
        self.storage_path = _storage_path(data_dir, self.node_id)
        self.mempool = Mempool()
        self.chain = Blockchain(
            settings.blockchain.difficulty,
            mempool=self.mempool,
            storage_path=self.storage_path,
            node_id=self.node_id,
            confirmations=settings.node.confirmations,
        )
        self.gossip = Gossip(
            node_id=self.node_id,
            peers=self.peers,
            timeout_s=settings.network.tx_forward_timeout_s,
            client_factory=gossip_client_factory,
        )
        self.sync = SyncService(
            node_id=self.node_id,
            chain=self.chain,
            peers_provider=self.gossip.peers,
            interval_s=settings.network.heartbeat_interval_s,
            timeout_s=settings.network.grpc_timeout_s,
            client_factory=sync_client_factory,
        )
        self.coordinator_client = coordinator_client or _coordinator_client_from_env(settings)

        self.miner = Miner(
            node_id=self.node_id,
            chain=self.chain,
            mempool=self.mempool,
            mode=settings.blockchain.mining_mode,
            block_size=settings.node.block_size,
            nonce_space=settings.blockchain.nonce_space,
            on_block_mined=self.gossip.broadcast_block,
            on_work_result=self.handle_work_result,
        )
        pool_mode = settings.blockchain.mining_mode == "pool"
        self.service = NodeService(
            node_id=self.node_id,
            on_transaction=self.handle_transaction,
            on_block=self.handle_block,
            get_blocks=self.chain.blocks_from,
            get_status=self.status,
            start_work=self.handle_start_work if pool_mode else None,
            stop_work=self.handle_stop_work if pool_mode else None,
        )
        self.grpc_server, self.grpc_port = build_server(
            self.service,
            host=DEFAULT_BIND_HOST,
            port=settings.node.grpc_port if grpc_port is None else grpc_port,
        )
        self.rest_app: Flask = create_app(
            node_id=self.node_id,
            chain=self.chain,
            mempool=self.mempool,
            miner=self.miner,
            peers=self.gossip.peers,
            on_tx_received=self.gossip.broadcast_tx,
        )
        self._grpc_started = False

    def status(self) -> NodeStatus:
        """Snapshot consumed by REST ``/status``, gRPC ``GetStatus``, and heartbeat."""
        tip = self.chain.tip()
        return NodeStatus(
            node_id=self.node_id,
            height=tip.index,
            tip_hash=tip.hash,
            peers=self.gossip.peers(),
            mempool_size=self.mempool.size(),
            hashrate=self.miner.hashrate(),
            state=self.miner.state(),
            meta={
                "difficulty": self.chain.difficulty,
                "total_work": self.chain.total_work(),
                "side_branch_blocks": self.chain.side_branch_block_count(),
                "mining_mode": self.miner.mode,
            },
        )

    def handle_transaction(self, tx: Transaction) -> tuple[bool, str]:
        """Add an incoming transaction to the mempool without re-broadcasting it."""
        if self.chain.has_transaction(tx.tx_id):
            return True, "already_confirmed"
        try:
            inserted = self.mempool.add(tx)
        except ValueError as exc:
            return False, str(exc)
        return (True, "accepted") if inserted else (True, "duplicate")

    def handle_block(self, block: Block) -> tuple[bool, str]:
        """Validate an incoming block, cancel stale work, and sync on orphans."""
        result = self.chain.add_block(block)
        if result in (AddResult.ACCEPTED, AddResult.FORK_SWITCHED):
            self.miner.notify_new_block()
            return True, result.value
        if result == AddResult.ORPHAN:
            self.miner.notify_new_block()
            try:
                self.sync.sync_now()
            except Exception:
                logger.exception("catch-up after orphan block %s failed", block.hash)
            return True, "orphan_sync_triggered"
        if result in (AddResult.DUPLICATE, AddResult.STORED_SIDE):
            return True, result.value
        return False, "invalid"

    def handle_start_work(self, assignment: WorkAssignment) -> tuple[bool, str]:
        self.miner.submit_work(assignment)
        return True, "assigned"

    def handle_stop_work(self, job_id: str) -> tuple[bool, str]:
        self.miner.stop_work(job_id)
        return True, "stopped"

    def _pool_candidate_problem(self, block: Block) -> str | None:
        """Return why a pool candidate cannot extend the current tip, if any."""
        tip = self.chain.tip()
        if block.index != tip.index + 1:
            return "non_sequential_index"
        if block.prev_hash != tip.hash:
            return "unknown_parent"
        if block.difficulty != self.chain.difficulty:
            return "wrong_difficulty"
        if not is_valid_pow(block):
            return "invalid_pow"
        if block.merkle_root != compute_merkle_root(block.transactions):
            return "invalid_merkle_root"

        seen: set[str] = set()
        for tx in block.transactions:
            if tx.tx_id in seen or self.chain.has_transaction(tx.tx_id):
                return "duplicate_tx"
            seen.add(tx.tx_id)
        return None

    def handle_work_result(self, assignment: WorkAssignment, block: Block | None) -> None:
        """Report a pool candidate before committing it to the local chain.

        The coordinator returns ``ack.ok=True`` only for the first found result
        of a job. Losers must not add or broadcast their candidate, otherwise
        every worker creates a same-index side branch with equal work.
        """
        if block is None:
            return

        problem = self._pool_candidate_problem(block)
        if problem is not None:
            logger.warning(
                "pool candidate %s rejected locally (%s); not reporting",
                block.hash,
                problem,
            )
            return

        if self.coordinator_client is None:
            logger.warning(
                "pool job %s found block %s but COORDINATOR_GRPC is not set",
                assignment.job_id,
                block.hash,
            )
            return

        result = WorkResult(
            job_id=assignment.job_id,
            node_id=self.node_id,
            nonce=block.nonce,
            found=True,
            block_hash=block.hash,
        )
        try:
            ack = self.coordinator_client.submit_work_result(result)
        except Exception:
            logger.exception("failed to submit work result for job %s", assignment.job_id)
            return
        if not ack.ok:
            logger.debug(
                "pool candidate %s was not selected for job %s: %s",
                block.hash,
                assignment.job_id,
                ack.message,
            )
            return

        add_result = self.chain.add_block(block)
        if add_result not in (AddResult.ACCEPTED, AddResult.FORK_SWITCHED):
            logger.warning(
                "winning pool block %s was not added (%s); not broadcasting",
                block.hash,
                add_result.value,
            )
            return

        self.miner.notify_new_block()
        self.gossip.broadcast_block(block)

    def start(self, *, start_miner: bool = True) -> None:
        """Start gRPC, catch up once, then start periodic sync and mining."""
        if self._grpc_started:
            return
        self.grpc_server.start()
        self._grpc_started = True
        try:
            self.sync.sync_now()
        except Exception:
            logger.exception("initial catch-up failed for %s", self.node_id)
        self.sync.start()
        if start_miner:
            self.miner.start()

    def stop(self, *, grpc_grace_s: float = 1.0) -> None:
        """Stop background loops and close peer/coordinator channels."""
        self.sync.stop()
        self.miner.stop()
        if self._grpc_started:
            self.grpc_server.stop(grace=grpc_grace_s)
            self._grpc_started = False
        self.gossip.close()
        if self.coordinator_client is not None:
            self.coordinator_client.close()

    def run_rest(self) -> None:
        """Run Flask in the calling thread (debug and reloader are always off)."""
        self.rest_app.run(
            host=DEFAULT_BIND_HOST,
            port=self.settings.node.rest_port,
            debug=False,
            threaded=True,
            use_reloader=False,
        )


def _safe_log(event: str, node_id: str, **fields: object) -> None:
    try:
        log_event(event, node_id, **fields)
    except Exception:
        logger.exception("failed to write %s event for %s", event, node_id)


def main() -> int:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.logging.level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        runtime = NodeRuntime(settings)
    except Exception:
        logger.exception("failed to build node runtime")
        return 1

    try:
        runtime.start()
    except Exception:
        logger.exception("failed to start node runtime")
        runtime.stop()
        return 1

    _safe_log(
        "node_start",
        runtime.node_id,
        rest_port=settings.node.rest_port,
        grpc_port=runtime.grpc_port,
        peers=runtime.gossip.peers(),
        mining_mode=settings.blockchain.mining_mode,
        difficulty=settings.blockchain.difficulty,
        storage_path=str(runtime.storage_path),
        coordinator_grpc=(
            runtime.coordinator_client.address if runtime.coordinator_client is not None else ""
        ),
    )
    return_code = 0
    try:
        runtime.run_rest()
    except KeyboardInterrupt:
        logger.info("node dihentikan lewat keyboard")
    except Exception:
        logger.exception("REST server failed for %s", runtime.node_id)
        return_code = 1
    finally:
        runtime.stop()
        _safe_log("node_stop", runtime.node_id)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
