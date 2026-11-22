#!/usr/bin/env bash
# Generate src/chainsim/generated/*_pb2*.py from proto/blockchain.proto.
# Pemilik: anggota B. Logika ada di scripts/gen_proto.py agar sama dengan
# versi PowerShell dan bisa diuji tanpa bergantung pada shell tertentu.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python}"
exec "$PYTHON" "$SCRIPT_DIR/gen_proto.py" "$@"
