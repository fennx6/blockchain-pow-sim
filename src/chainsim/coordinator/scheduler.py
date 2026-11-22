"""Pembagian ruang nonce untuk mode pool (AGENTS.md Bagian 7.2).

Coordinator memecah satu rentang nonce menjadi bagian yang tidak tumpang tindih
dan menutup seluruh rentang, supaya tidak ada worker yang menambang nonce yang
sama dan tidak ada nonce yang terlewat.
"""

from __future__ import annotations


def split_nonce_space(n_workers: int, start: int, size: int) -> list[tuple[int, int]]:
    """Bagi ``[start, start + size)`` menjadi ``n_workers`` rentang berurutan.

    Nilai kembali adalah daftar ``(nonce_start, nonce_end)`` dengan ``nonce_end``
    eksklusif. Sisa pembagian diberikan satu per satu ke worker pertama. Bila
    ``size < n_workers``, sebagian worker menerima rentang kosong
    (``start == end``) dan harus melapor tanpa menambang.
    """
    if n_workers <= 0:
        raise ValueError(f"n_workers harus lebih besar dari 0, bukan {n_workers}")
    if start < 0:
        raise ValueError(f"start tidak boleh negatif, bukan {start}")
    if size < 0:
        raise ValueError(f"size tidak boleh negatif, bukan {size}")

    base, remainder = divmod(size, n_workers)
    ranges: list[tuple[int, int]] = []
    cursor = start
    for index in range(n_workers):
        span = base + (1 if index < remainder else 0)
        ranges.append((cursor, cursor + span))
        cursor += span
    return ranges
