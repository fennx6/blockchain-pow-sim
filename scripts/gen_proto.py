"""Generate Python gRPC modules from ``proto/blockchain.proto``.

Owner: Member B (AGENTS.md Section 3). The generated ``*_pb2*.py`` files are
ignored by Git; run this script (directly or through ``gen_proto.ps1`` /
``gen_proto.sh``) before importing ``chainsim.generated``.

``grpc_tools.protoc`` emits ``import blockchain_pb2 as blockchain__pb2`` inside
``blockchain_pb2_grpc.py``. That form only works when the generated directory is
on ``sys.path``. This script rewrites the import to a package-qualified import so
the generated modules work from ``chainsim.generated``.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROTO = REPO_ROOT / "proto" / "blockchain.proto"
DEFAULT_OUT = REPO_ROOT / "src" / "chainsim" / "generated"
DEFAULT_PACKAGE = "chainsim.generated"

_IMPORT_RE = re.compile(r"^import (\w+_pb2) as (\w+)$", re.MULTILINE)


def rewrite_grpc_import(path: Path, package: str) -> None:
    """Rewrite generated sibling imports to ``from <package> import ...``.

    The rewrite is idempotent: a file that already uses the package-qualified
    form is left untouched.
    """
    if not package:
        return
    text = path.read_text(encoding="utf-8")
    replaced = _IMPORT_RE.sub(
        lambda match: f"from {package} import {match.group(1)} as {match.group(2)}",
        text,
    )
    if replaced != text:
        path.write_text(replaced, encoding="utf-8")


def generate(
    proto_path: Path | str,
    out_dir: Path | str,
    *,
    package: str = DEFAULT_PACKAGE,
    python_executable: str | None = None,
) -> tuple[Path, Path]:
    """Compile ``proto_path`` into ``out_dir`` and return the two generated files.

    Raises ``FileNotFoundError`` when the proto is missing and propagates
    ``subprocess.CalledProcessError`` when ``grpc_tools.protoc`` fails.
    """
    proto = Path(proto_path)
    target = Path(out_dir)
    if not proto.is_file():
        raise FileNotFoundError(f"proto file not found: {proto}")
    target.mkdir(parents=True, exist_ok=True)

    command = [
        python_executable or sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto.parent}",
        f"--python_out={target}",
        f"--grpc_python_out={target}",
        str(proto),
    ]
    subprocess.run(command, check=True)

    pb2_path = target / f"{proto.stem}_pb2.py"
    grpc_path = target / f"{proto.stem}_pb2_grpc.py"
    rewrite_grpc_import(grpc_path, package)
    shutil.rmtree(target / "__pycache__", ignore_errors=True)
    return pb2_path, grpc_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Python gRPC modules from the proto contract"
    )
    parser.add_argument(
        "--proto", type=Path, default=DEFAULT_PROTO, help="path to blockchain.proto"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="directory for generated *_pb2*.py files",
    )
    parser.add_argument(
        "--package",
        default=DEFAULT_PACKAGE,
        help="package name used to rewrite sibling imports (empty string disables rewrite)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        pb2_path, grpc_path = generate(args.proto, args.out, package=args.package)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"failed to generate gRPC modules: {exc}", file=sys.stderr)
        return 1
    print(f"generated {pb2_path.name} and {grpc_path.name} in {pb2_path.parent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
