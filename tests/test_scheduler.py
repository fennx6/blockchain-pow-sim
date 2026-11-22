"""Pembagian rentang nonce untuk mode pool."""

from __future__ import annotations

import pytest

from chainsim.coordinator.scheduler import split_nonce_space


def test_single_worker_gets_whole_range() -> None:
    assert split_nonce_space(1, 0, 1000) == [(0, 1000)]


def test_even_split() -> None:
    assert split_nonce_space(4, 0, 1000) == [
        (0, 250),
        (250, 500),
        (500, 750),
        (750, 1000),
    ]


def test_remainder_goes_to_first_workers() -> None:
    assert split_nonce_space(3, 10, 10) == [(10, 14), (14, 17), (17, 20)]


def test_ranges_are_contiguous_and_cover_space() -> None:
    for workers in range(1, 10):
        for size in (0, 1, 7, 100, 1001):
            ranges = split_nonce_space(workers, 5, size)
            assert ranges[0][0] == 5
            assert ranges[-1][1] == 5 + size
            assert all(low <= high for low, high in ranges)
            assert sum(high - low for low, high in ranges) == size
            for previous, current in zip(ranges, ranges[1:], strict=False):
                assert previous[1] == current[0]


def test_more_workers_than_nonces_yields_empty_ranges() -> None:
    ranges = split_nonce_space(4, 0, 2)
    assert ranges == [(0, 1), (1, 2), (2, 2), (2, 2)]


@pytest.mark.parametrize(
    ("workers", "start", "size"), [(0, 0, 10), (-1, 0, 10), (2, -5, 10), (2, 0, -1)]
)
def test_invalid_arguments_are_rejected(workers: int, start: int, size: int) -> None:
    with pytest.raises(ValueError):
        split_nonce_space(workers, start, size)
