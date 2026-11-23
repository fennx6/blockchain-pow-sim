"""Kerangka menjalankan satu batch eksperimen secara berulang.

Berisi helper yang dipakai e1-e4: identitas run, pemanggilan perintah Docker,
dan pencatatan metrik ke CSV. Tidak ada angka hasil di modul ini.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metrics import append_result, make_result_row

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "experiments" / "results"
DEFAULT_COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

logger = logging.getLogger("experiments")


@dataclass(frozen=True)
class RunContext:
    """Identitas satu sesi eksperimen."""

    run_id: str
    experiment: str
    results_dir: Path = RESULTS_DIR

    def result_path(self) -> Path:
        return self.results_dir / f"{self.experiment}.csv"

    def record(self, *, metric: str, value: float, unit: str, **extra: Any) -> dict[str, Any]:
        row = make_result_row(
            run_id=self.run_id,
            experiment=self.experiment,
            metric=metric,
            value=value,
            unit=unit,
            **extra,
        )
        append_result(self.result_path(), row)
        return row


def new_run_id(prefix: str, now: float | None = None) -> str:
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
    return f"{prefix}-{stamp}"


def run_command(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    dry_run: bool = False,
    check: bool = True,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Jalankan perintah eksternal dengan log yang bisa direproduksi."""
    printable = " ".join(args)
    logger.info("%s%s", "[dry-run] " if dry_run else "[run] ", printable)
    if dry_run:
        return subprocess.CompletedProcess(list(args), 0, "", "")
    merged_env = None if env is None else {**os.environ, **env}
    return subprocess.run(args, cwd=cwd, text=True, check=check, env=merged_env)


def compose(
    *args: str,
    compose_file: Path = DEFAULT_COMPOSE_FILE,
    dry_run: bool = False,
    check: bool = True,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Panggil ``docker compose -f <file> ...``."""
    return run_command(
        ["docker", "compose", "-f", str(compose_file), *args],
        dry_run=dry_run,
        check=check,
        env=env,
    )
