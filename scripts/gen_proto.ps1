# Generate src/chainsim/generated/*_pb2*.py from proto/blockchain.proto.
# Pemilik: anggota B. Logika ada di scripts/gen_proto.py agar sama dengan
# versi POSIX dan bisa diuji tanpa bergantung pada PowerShell.
$ErrorActionPreference = "Stop"

$python = if ($env:PYTHON) { $env:PYTHON } else { "python" }
& $python (Join-Path $PSScriptRoot "gen_proto.py") @args
exit $LASTEXITCODE
