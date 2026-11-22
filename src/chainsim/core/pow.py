"""Proof-of-Work mining and verification.

Owner: Member A (AGENTS.md Section 3).
Contract (AGENTS.md Section 6.2):
- ``mine(template: Block, nonce_start: int, nonce_end: int, stop: threading.Event) -> Block | None``
- ``is_valid_pow(block: Block) -> bool``
"""

from __future__ import annotations

import hashlib
import threading

from chainsim.core.block import compute_hash, header_nonce_parts
from chainsim.types import Block

# Checking the stop event every 1024 iterations keeps cancellation responsive
# without paying threading.Event overhead on every single SHA-256 call.
_STOP_CHECK_MASK = 0x3FF


def is_valid_pow(block: Block) -> bool:
    """Verify that ``block.hash`` matches its header and meets ``block.difficulty``."""
    if not isinstance(block, Block):
        return False
    if isinstance(block.difficulty, bool) or not isinstance(block.difficulty, int):
        return False
    if block.difficulty < 1:
        return False
    if isinstance(block.nonce, bool) or not isinstance(block.nonce, int) or block.nonce < 0:
        return False
    if not isinstance(block.hash, str) or len(block.hash) != 64:
        return False
    target_prefix = "0" * block.difficulty
    if not block.hash.startswith(target_prefix):
        return False
    return block.hash == compute_hash(block)


def mine_with_stats(
    template: Block,
    nonce_start: int,
    nonce_end: int,
    stop: threading.Event,
) -> tuple[Block | None, int]:
    """Search ``[nonce_start, nonce_end)`` for a valid PoW nonce and count hashes tried.

    Does not mutate ``template``. Returns ``(mined_block, hashes_tried)`` where
    ``mined_block`` is ``None`` if the range is exhausted or ``stop`` is set.
    """
    if isinstance(template.difficulty, bool) or not isinstance(template.difficulty, int):
        raise ValueError(f"difficulty must be an integer >= 1, got {template.difficulty!r}")
    if template.difficulty < 1:
        raise ValueError(f"difficulty must be an integer >= 1, got {template.difficulty!r}")
    if isinstance(nonce_start, bool) or not isinstance(nonce_start, int) or nonce_start < 0:
        raise ValueError(f"nonce_start must be a non-negative integer, got {nonce_start!r}")
    if isinstance(nonce_end, bool) or not isinstance(nonce_end, int) or nonce_end < nonce_start:
        raise ValueError(f"nonce_end ({nonce_end!r}) must be >= nonce_start ({nonce_start!r})")

    if nonce_start == nonce_end or stop.is_set():
        return None, 0

    target_prefix = "0" * template.difficulty
    prefix, suffix = header_nonce_parts(template)
    base_hasher = hashlib.sha256(prefix)
    hashes_tried = 0

    for nonce in range(nonce_start, nonce_end):
        if (hashes_tried & _STOP_CHECK_MASK) == 0 and stop.is_set():
            return None, hashes_tried

        hasher = base_hasher.copy()
        hasher.update(str(nonce).encode("ascii"))
        hasher.update(suffix)
        digest = hasher.hexdigest()
        hashes_tried += 1

        if digest.startswith(target_prefix):
            found = Block(
                index=template.index,
                prev_hash=template.prev_hash,
                timestamp_ms=template.timestamp_ms,
                transactions=list(template.transactions),
                merkle_root=template.merkle_root,
                difficulty=template.difficulty,
                nonce=nonce,
                miner_id=template.miner_id,
                hash=digest,
            )
            return found, hashes_tried

    return None, hashes_tried


def mine(
    template: Block,
    nonce_start: int,
    nonce_end: int,
    stop: threading.Event,
) -> Block | None:
    """Search ``[nonce_start, nonce_end)`` for a valid PoW nonce.

    Returns a new ``Block`` with winning ``nonce`` and ``hash``, or ``None`` if
    cancelled via ``stop`` or if no nonce in the range meets ``template.difficulty``.
    """
    block, _ = mine_with_stats(template, nonce_start, nonce_end, stop)
    return block
