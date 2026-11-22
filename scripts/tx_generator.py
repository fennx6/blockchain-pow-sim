"""Pembangkit transaksi fiktif untuk beban uji.

Pemilik: anggota B (AGENTS.md Bagian 3). Placeholder Fase 0; endpoint tujuan dan
jalur pengiriman (lewat proxy coordinator atau REST node) disepakati di
docs/decisions.md.
"""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kirim transaksi fiktif ke jaringan")
    parser.add_argument("--rate", type=float, default=5.0, help="transaksi per detik")
    parser.add_argument("--duration", type=float, default=60.0, help="durasi dalam detik")
    parser.add_argument("--target", default="http://127.0.0.1:8080", help="URL tujuan")
    return parser


def main(argv: list[str] | None = None) -> int:
    build_parser().parse_args(argv)
    raise SystemExit("TODO(B): implementasi pembangkit transaksi sesuai kontrak Bagian 6.4")


if __name__ == "__main__":
    raise SystemExit(main())
