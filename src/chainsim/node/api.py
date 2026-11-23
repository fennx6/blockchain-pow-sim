"""Per-node Flask REST API.

Owner: Member A (AGENTS.md Section 3).
Contract (AGENTS.md Section 6.4 & Section 10):
- ``POST /tx``      validate input strictly, add to mempool, forward via gRPC hook
- ``GET /chain``    local active chain (supports ``?from=<index>``)
- ``GET /status``   ``NodeStatus`` snapshot consumed by coordinator heartbeat/metrics
- ``GET /mempool``  pending unconfirmed transactions
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import asdict
from typing import Any

from flask import Flask, Response, jsonify, request

from chainsim.core.block import block_to_dict
from chainsim.core.chain import Blockchain
from chainsim.core.mempool import Mempool
from chainsim.core.transaction import (
    create_transaction,
    transaction_to_dict,
)
from chainsim.logging_utils import log_event
from chainsim.node.miner import Miner
from chainsim.types import NodeStatus, Transaction

logger = logging.getLogger(__name__)

DEFAULT_MAX_TX_AMOUNT = 1_000_000_000.0
DEFAULT_MAX_CONTENT_LENGTH = 64 * 1024

PeersProvider = Sequence[str] | Callable[[], Sequence[str]]
TxForwardCallback = Callable[[Transaction], Any]
StateProvider = Callable[[], str]
MetaProvider = Callable[[], dict[str, Any]]


def _was_forwarded(result: Any) -> bool:
    """Interpret the return value of ``on_tx_received`` as a forwarding status."""
    if result is None:
        return True
    if isinstance(result, bool):
        return result
    if isinstance(result, Sequence) and not isinstance(result, str | bytes):
        if not result:
            return False
        if all(hasattr(item, "ok") for item in result):
            return any(bool(item.ok) for item in result)
        return True
    return bool(result)


def _resolve_peers(peers_provider: PeersProvider) -> list[str]:
    raw = peers_provider() if callable(peers_provider) else peers_provider
    return [str(peer) for peer in raw]


def _resolve_state(miner: Miner | None, state_provider: StateProvider | None) -> str:
    if state_provider is not None:
        custom = state_provider()
        if custom:
            return custom
    if miner is not None:
        return miner.state()
    return "up"


def create_app(
    *,
    node_id: str,
    chain: Blockchain,
    mempool: Mempool,
    miner: Miner | None = None,
    peers: PeersProvider = (),
    on_tx_received: TxForwardCallback | None = None,
    state_provider: StateProvider | None = None,
    meta_provider: MetaProvider | None = None,
    max_tx_amount: float = DEFAULT_MAX_TX_AMOUNT,
    max_content_length: int = DEFAULT_MAX_CONTENT_LENGTH,
) -> Flask:
    """Build the node Flask REST application without starting a network server."""
    if not isinstance(node_id, str) or not node_id.strip():
        raise ValueError("node_id must be a non-empty string")
    if max_tx_amount <= 0:
        raise ValueError(f"max_tx_amount must be > 0, got {max_tx_amount!r}")
    if isinstance(max_content_length, bool) or not isinstance(max_content_length, int):
        raise ValueError(f"max_content_length must be an integer >= 1, got {max_content_length!r}")
    if max_content_length < 1:
        raise ValueError(f"max_content_length must be >= 1, got {max_content_length!r}")

    clean_node_id = node_id.strip()
    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False
    app.config["MAX_CONTENT_LENGTH"] = max_content_length

    @app.errorhandler(413)
    def payload_too_large(_: Exception) -> tuple[Response, int]:
        return jsonify({"error": "request body exceeds maximum allowed size"}), 413

    @app.post("/tx")
    def post_tx() -> tuple[Response, int]:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400

        for required in ("sender", "receiver", "amount"):
            if required not in payload:
                return jsonify({"error": f"missing required field: {required}"}), 400

        raw_ts = payload.get("timestamp_ms")
        try:
            tx = create_transaction(
                sender=payload["sender"],
                receiver=payload["receiver"],
                amount=payload["amount"],
                origin_node=clean_node_id,
                timestamp_ms=raw_ts,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        if tx.amount > max_tx_amount:
            return (
                jsonify({"error": f"amount exceeds maximum allowed value ({max_tx_amount})"}),
                400,
            )

        if chain.has_transaction(tx.tx_id):
            return (
                jsonify({"error": "transaction already confirmed", "tx_id": tx.tx_id}),
                409,
            )

        inserted = mempool.add(tx)
        if not inserted:
            return (
                jsonify(
                    {
                        "status": "duplicate",
                        "transaction": transaction_to_dict(tx),
                        "forwarded": False,
                    }
                ),
                200,
            )

        log_event(
            "tx_received",
            clean_node_id,
            tx_id=tx.tx_id,
            sender=tx.sender,
            receiver=tx.receiver,
            amount=tx.amount,
            origin_node=tx.origin_node,
            from_node="client",
            to_node=clean_node_id,
        )

        forwarded = False
        if on_tx_received is not None:
            try:
                forward_result = on_tx_received(tx)
                forwarded = _was_forwarded(forward_result)
            except Exception:
                logger.exception("failed to forward tx %s to peers", tx.tx_id)

        return (
            jsonify(
                {
                    "status": "accepted",
                    "transaction": transaction_to_dict(tx),
                    "forwarded": forwarded,
                }
            ),
            201,
        )

    @app.get("/chain")
    def get_chain() -> tuple[Response, int]:
        raw_from = request.args.get("from", default="0")
        try:
            start_index = int(raw_from)
        except ValueError:
            return jsonify({"error": "query parameter 'from' must be an integer"}), 400
        if start_index < 0:
            return jsonify({"error": "query parameter 'from' must be >= 0"}), 400

        blocks = chain.blocks_from(start_index)
        tip = chain.tip()
        return (
            jsonify(
                {
                    "node_id": clean_node_id,
                    "height": tip.index,
                    "tip_hash": tip.hash,
                    "total_work": chain.total_work(),
                    "from": start_index,
                    "blocks": [block_to_dict(block) for block in blocks],
                }
            ),
            200,
        )

    @app.get("/status")
    def get_status() -> tuple[Response, int]:
        tip = chain.tip()
        meta: dict[str, Any] = {
            "difficulty": chain.difficulty,
            "total_work": chain.total_work(),
            "side_branch_blocks": chain.side_branch_block_count(),
        }
        if miner is not None:
            meta["mining_mode"] = miner.mode
        if meta_provider is not None:
            meta.update(meta_provider())

        status = NodeStatus(
            node_id=clean_node_id,
            height=tip.index,
            tip_hash=tip.hash,
            peers=_resolve_peers(peers),
            mempool_size=mempool.size(),
            hashrate=miner.hashrate() if miner is not None else 0.0,
            state=_resolve_state(miner, state_provider),
            meta=meta,
        )
        return jsonify(asdict(status)), 200

    @app.get("/mempool")
    def get_mempool() -> tuple[Response, int]:
        txs = mempool.all()
        return (
            jsonify(
                {
                    "node_id": clean_node_id,
                    "size": len(txs),
                    "transactions": [transaction_to_dict(tx) for tx in txs],
                }
            ),
            200,
        )

    @app.after_request
    def no_store(response: Response) -> Response:
        response.headers["Cache-Control"] = "no-store"
        return response

    return app


def run_server(app: Flask, host: str = "0.0.0.0", port: int = 8081) -> None:
    """Run the node REST server with debug mode strictly disabled (AGENTS.md Section 10)."""
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
