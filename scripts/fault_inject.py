"""Injeksi kegagalan ke container dan catat waktunya ke log.

Semua aksi menulis event ``fault_injected`` ke ``logs/fault_injector.jsonl``
sehingga waktu deteksi dan pemulihan bisa dihitung dari data, bukan dari catatan
manual (AGENTS.md Bagian 8).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from chainsim.logging_utils import log_event  # noqa: E402

DEFAULT_COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
DEFAULT_NETWORK_SUFFIX = "chainnet"
FAULT_NODE_ID = "fault_injector"

logger = logging.getLogger("fault_inject")


def run(cmd: list[str], dry_run: bool) -> subprocess.CompletedProcess[str]:
    """Jalankan perintah; pada dry-run hanya dicatat, tidak dieksekusi."""
    logger.info("%s%s", "[dry-run] " if dry_run else "[run] ", " ".join(cmd))
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, "", "")
    completed = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"perintah gagal ({completed.returncode}): {' '.join(cmd)}\n{detail}")
    return completed


def compose_command(compose_file: Path, project: str | None) -> list[str]:
    cmd = ["docker", "compose", "-f", str(compose_file)]
    if project:
        cmd.extend(["-p", project])
    return cmd


def compose_project(compose_file: Path, override: str | None, dry_run: bool) -> str:
    """Nama project compose; dipakai untuk nama network dan volume."""
    if override:
        return override
    if dry_run:
        # Tanpa Docker kita tidak bisa memanggil `compose config`, jadi baca
        # field `name:` langsung agar perintah dry-run tetap akurat.
        try:
            content = compose_file.read_text(encoding="utf-8")
        except OSError:
            content = ""
        match = re.search(r"^name:\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*$", content, re.MULTILINE)
        return match.group(1) if match else compose_file.parent.name.lower()
    completed = run(
        ["docker", "compose", "-f", str(compose_file), "config", "--format", "json"], dry_run
    )
    try:
        return str(json.loads(completed.stdout)["name"])
    except (json.JSONDecodeError, KeyError) as exc:
        raise RuntimeError(f"tidak bisa membaca nama project dari compose: {exc}") from exc


def container_id(compose_file: Path, project: str | None, service: str, dry_run: bool) -> str:
    completed = run([*compose_command(compose_file, project), "ps", "-q", service], dry_run)
    if dry_run:
        return f"<container-{service}>"
    value = completed.stdout.strip().splitlines()
    if not value or not value[0]:
        raise RuntimeError(f"container untuk service {service!r} tidak ditemukan")
    return value[0]


def remove_service_volumes(
    compose_file: Path, project: str, service: str, dry_run: bool
) -> list[str]:
    """Hapus volume milik satu service, termasuk data rantainya (skenario F3)."""
    listing = run(
        [
            "docker",
            "volume",
            "ls",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--filter",
            f"label=com.docker.compose.volume={service}-data",
        ],
        dry_run,
    )
    volumes = [line.strip() for line in listing.stdout.splitlines() if line.strip()]
    if not volumes and not dry_run:
        logger.warning("tidak ada volume %s-data pada project %s", service, project)
    for volume in volumes:
        run(["docker", "volume", "rm", volume], dry_run)
    return volumes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Injeksi kegagalan node pada jaringan compose")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument(
        "--project", default=None, help="nama project compose (default: dari compose)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="cetak perintah tanpa menjalankannya"
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    for name, help_text in (
        ("stop", "hentikan container satu service"),
        ("start", "jalankan lagi container satu service"),
        ("restart", "restart container satu service"),
        ("remove-volume", "hentikan service lalu hapus volume datanya"),
        ("disconnect", "lepaskan container dari network (partisi)"),
        ("connect", "sambungkan kembali container ke network"),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("--service", required=True, help="nama service compose, mis. node-3")
        sub.add_argument(
            "--network", default=None, help="nama network (default: <project>_chainnet)"
        )

    subparsers.add_parser("ps", help="tampilkan status container")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    compose_file: Path = args.compose_file
    if not compose_file.is_file():
        logger.error("file compose tidak ditemukan: %s", compose_file)
        return 1

    project = compose_project(compose_file, args.project, args.dry_run)
    compose = compose_command(compose_file, project)

    if args.action == "ps":
        run([*compose, "ps"], args.dry_run)
        return 0

    service: str = args.service
    network = args.network or f"{project}_{DEFAULT_NETWORK_SUFFIX}"
    details: dict[str, object] = {"service": service, "project": project}

    if args.action == "stop":
        run([*compose, "stop", service], args.dry_run)
    elif args.action == "start":
        run([*compose, "start", service], args.dry_run)
    elif args.action == "restart":
        run([*compose, "restart", service], args.dry_run)
    elif args.action == "remove-volume":
        run([*compose, "rm", "-s", "-f", service], args.dry_run)
        details["volumes"] = remove_service_volumes(compose_file, project, service, args.dry_run)
    elif args.action in {"disconnect", "connect"}:
        cid = container_id(compose_file, project, service, args.dry_run)
        details["container"] = cid
        details["network"] = network
        run(["docker", "network", args.action, network, cid], args.dry_run)
    else:  # pragma: no cover - argparse membatasi pilihan
        logger.error("aksi tidak dikenal: %s", args.action)
        return 2

    log_event("fault_injected", FAULT_NODE_ID, action=args.action, **details)
    logger.info("aksi %s pada %s selesai", args.action, service)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc
