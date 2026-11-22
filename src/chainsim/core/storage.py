"""Atomic local chain persistence on each node's Docker volume.

Owner: Member A (AGENTS.md Section 3).
Contract (AGENTS.md Section 6.2):
- ``save_chain(path, blocks)``
- ``load_chain(path) -> list[Block]``
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path

from chainsim.core.block import block_from_dict, block_to_dict
from chainsim.types import Block


def save_chain(path: Path | str, blocks: Sequence[Block]) -> None:
    """Persist ``blocks`` to ``path`` as JSON using an atomic rename.

    Writing to a sibling temporary file + ``fsync`` + ``os.replace`` prevents
    partial/corrupt files when a container is stopped mid-write (scenario F2).
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    serialized = [block_to_dict(block) for block in blocks]

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)
            json.dump(serialized, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, target)
    except Exception:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise


def load_chain(path: Path | str) -> list[Block]:
    """Load persisted blocks from ``path``.

    Returns an empty list when the file does not exist (e.g., fresh volume in
    scenario F3). Raises ``ValueError`` if the file exists but is malformed.
    """
    target = Path(path)
    if not target.exists():
        return []
    if not target.is_file():
        raise ValueError(f"chain storage path is not a file: {target}")

    try:
        raw_text = target.read_text(encoding="utf-8")
        data = json.loads(raw_text)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"failed to read chain from {target}: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError(f"chain file {target} must contain a JSON list of blocks")

    return [block_from_dict(item) for item in data]
