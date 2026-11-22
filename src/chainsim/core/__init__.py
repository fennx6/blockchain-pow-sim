"""Core blockchain package (owner: Member A)."""

from chainsim.core.block import (
    GENESIS_MINER_ID,
    GENESIS_PREV_HASH,
    GENESIS_TIMESTAMP_MS,
    block_from_dict,
    block_to_dict,
    build_block_template,
    compute_hash,
    compute_merkle_root,
    create_genesis_block,
)
from chainsim.core.pow import is_valid_pow, mine, mine_with_stats
from chainsim.core.storage import load_chain, save_chain
from chainsim.core.transaction import (
    compute_tx_id,
    create_transaction,
    is_valid_transaction,
    transaction_from_dict,
    transaction_to_dict,
    validate_transaction_fields,
)

__all__ = [
    "GENESIS_MINER_ID",
    "GENESIS_PREV_HASH",
    "GENESIS_TIMESTAMP_MS",
    "block_from_dict",
    "block_to_dict",
    "build_block_template",
    "compute_hash",
    "compute_merkle_root",
    "compute_tx_id",
    "create_genesis_block",
    "create_transaction",
    "is_valid_pow",
    "is_valid_transaction",
    "load_chain",
    "mine",
    "mine_with_stats",
    "save_chain",
    "transaction_from_dict",
    "transaction_to_dict",
    "validate_transaction_fields",
]
