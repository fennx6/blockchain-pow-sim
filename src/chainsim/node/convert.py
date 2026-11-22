"""Protobuf conversion layer for node and coordinator messages.

Owner: Member B (AGENTS.md Sections 3 and 6.3). All protobuf <-> dataclass
conversion lives here so gRPC transport code never invents its own field
mapping. Invalid payloads raise ``ValueError`` before they reach the chain.
"""

from __future__ import annotations

from chainsim.coordinator.registry import NodeRecord
from chainsim.coordinator.scheduler import WorkResult
from chainsim.core.block import block_from_dict
from chainsim.core.transaction import is_valid_transaction, transaction_to_dict
from chainsim.generated import blockchain_pb2
from chainsim.types import Block, NodeStatus, Transaction, WorkAssignment

# gRPC metadata key carrying the immediate sender's node id for event logs.
FROM_NODE_METADATA_KEY = "x-from-node"


def tx_to_msg(tx: Transaction) -> blockchain_pb2.TransactionMsg:
    """Convert a ``Transaction`` dataclass into its protobuf message."""
    return blockchain_pb2.TransactionMsg(
        tx_id=tx.tx_id,
        sender=tx.sender,
        receiver=tx.receiver,
        amount=float(tx.amount),
        timestamp_ms=int(tx.timestamp_ms),
        origin_node=tx.origin_node,
    )


def tx_from_msg(msg: blockchain_pb2.TransactionMsg) -> Transaction:
    """Validate and convert a ``TransactionMsg`` into a ``Transaction``.

    Raises ``ValueError`` when fields are malformed or ``tx_id`` does not match
    the canonical transaction hash.
    """
    tx = Transaction(
        tx_id=msg.tx_id,
        sender=msg.sender,
        receiver=msg.receiver,
        amount=float(msg.amount),
        timestamp_ms=int(msg.timestamp_ms),
        origin_node=msg.origin_node,
    )
    if not is_valid_transaction(tx):
        raise ValueError(f"invalid transaction payload: {msg.tx_id!r}")
    return tx


def block_to_msg(block: Block) -> blockchain_pb2.BlockMsg:
    """Convert a ``Block`` dataclass into its protobuf message."""
    return blockchain_pb2.BlockMsg(
        index=int(block.index),
        prev_hash=block.prev_hash,
        timestamp_ms=int(block.timestamp_ms),
        transactions=[tx_to_msg(tx) for tx in block.transactions],
        merkle_root=block.merkle_root,
        difficulty=int(block.difficulty),
        nonce=int(block.nonce),
        miner_id=block.miner_id,
        hash=block.hash,
    )


def block_from_msg(msg: blockchain_pb2.BlockMsg) -> Block:
    """Convert a ``BlockMsg`` into a ``Block``.

    Transaction payloads are validated here; Proof-of-Work and chain linkage are
    validated by ``Blockchain.add_block`` so this layer stays transport-only.
    """
    transactions = [tx_from_msg(tx_msg) for tx_msg in msg.transactions]
    return block_from_dict(
        {
            "index": msg.index,
            "prev_hash": msg.prev_hash,
            "timestamp_ms": msg.timestamp_ms,
            "transactions": [transaction_to_dict(tx) for tx in transactions],
            "merkle_root": msg.merkle_root,
            "difficulty": msg.difficulty,
            "nonce": msg.nonce,
            "miner_id": msg.miner_id,
            "hash": msg.hash,
        }
    )


def status_to_msg(status: NodeStatus) -> blockchain_pb2.NodeStatusMsg:
    """Convert ``NodeStatus`` into a protobuf message.

    ``meta`` values are stringified because the proto map only carries strings.
    """
    return blockchain_pb2.NodeStatusMsg(
        node_id=status.node_id,
        height=int(status.height),
        tip_hash=status.tip_hash,
        peers=list(status.peers),
        mempool_size=int(status.mempool_size),
        hashrate=float(status.hashrate),
        state=status.state,
        meta={str(key): str(value) for key, value in status.meta.items()},
    )


def status_from_msg(msg: blockchain_pb2.NodeStatusMsg) -> NodeStatus:
    """Convert a ``NodeStatusMsg`` into ``NodeStatus`` (meta values stay strings)."""
    return NodeStatus(
        node_id=msg.node_id,
        height=int(msg.height),
        tip_hash=msg.tip_hash,
        peers=list(msg.peers),
        mempool_size=int(msg.mempool_size),
        hashrate=float(msg.hashrate),
        state=msg.state,
        meta=dict(msg.meta),
    )


def work_assignment_to_msg(assignment: WorkAssignment) -> blockchain_pb2.WorkAssignmentMsg:
    """Convert a pool ``WorkAssignment`` into its protobuf message."""
    return blockchain_pb2.WorkAssignmentMsg(
        job_id=assignment.job_id,
        block_template=block_to_msg(assignment.block_template),
        nonce_start=int(assignment.nonce_start),
        nonce_end=int(assignment.nonce_end),
    )


def work_assignment_from_msg(msg: blockchain_pb2.WorkAssignmentMsg) -> WorkAssignment:
    """Validate and convert a ``WorkAssignmentMsg`` into ``WorkAssignment``."""
    if msg.nonce_start < 0:
        raise ValueError(f"nonce_start must be non-negative, got {msg.nonce_start}")
    if msg.nonce_end < msg.nonce_start:
        raise ValueError(f"nonce_end ({msg.nonce_end}) must be >= nonce_start ({msg.nonce_start})")
    return WorkAssignment(
        job_id=msg.job_id,
        block_template=block_from_msg(msg.block_template),
        nonce_start=int(msg.nonce_start),
        nonce_end=int(msg.nonce_end),
    )


def work_result_to_msg(result: WorkResult) -> blockchain_pb2.WorkResultMsg:
    """Convert a coordinator ``WorkResult`` into its protobuf message."""
    return blockchain_pb2.WorkResultMsg(
        job_id=result.job_id,
        node_id=result.node_id,
        nonce=int(result.nonce),
        found=bool(result.found),
        block_hash=result.block_hash,
    )


def work_result_from_msg(msg: blockchain_pb2.WorkResultMsg) -> WorkResult:
    """Convert a ``WorkResultMsg`` into the coordinator ``WorkResult`` dataclass."""
    return WorkResult(
        job_id=msg.job_id,
        node_id=msg.node_id,
        nonce=int(msg.nonce),
        found=bool(msg.found),
        block_hash=msg.block_hash,
    )


def node_info_to_msg(record: NodeRecord) -> blockchain_pb2.NodeInfo:
    """Convert a coordinator ``NodeRecord`` into a ``NodeInfo`` message."""
    return blockchain_pb2.NodeInfo(
        node_id=record.node_id,
        rest_url=record.rest_url,
        grpc_address=record.grpc_address,
        peers=list(record.peers),
    )


def node_info_from_msg(msg: blockchain_pb2.NodeInfo) -> NodeRecord:
    """Convert a ``NodeInfo`` message into a coordinator ``NodeRecord``."""
    if not msg.node_id.strip():
        raise ValueError("NodeInfo.node_id must not be empty")
    return NodeRecord(
        node_id=msg.node_id,
        rest_url=msg.rest_url,
        grpc_address=msg.grpc_address,
        peers=tuple(msg.peers),
    )


def peer_list_to_msg(records: list[NodeRecord]) -> blockchain_pb2.PeerList:
    """Convert a list of ``NodeRecord`` into a ``PeerList`` message."""
    return blockchain_pb2.PeerList(nodes=[node_info_to_msg(record) for record in records])


def peer_list_from_msg(msg: blockchain_pb2.PeerList) -> list[NodeRecord]:
    """Convert a ``PeerList`` message into a list of ``NodeRecord``."""
    return [node_info_from_msg(node) for node in msg.nodes]
