"""Tests for the label-free segmenters ported from NLP (phase 11, step 18b)."""
from __future__ import annotations

import itertools
import math

import numpy as np
import pytest

from archival_structures.stream_analysis.overview.subsequence_detection import (
    _geometric_length_penalty,
    boundaries_to_segments,
    boundaries_to_starts,
    calibrate_length_penalty_weight,
    predicted_start_rate,
    segment_dp,
    starts_to_boundaries,
    textiling_depth_scores,
)


def _centred(sims):
    finite = sims[np.isfinite(sims)]
    base = float(np.median(finite)) if len(finite) else 0.0
    return np.nan_to_num(sims, nan=base) - base


def _score(starts, n, c, mean, weight):
    edges = list(starts) + [n]
    total = 0.0
    for a, b in zip(edges, edges[1:]):
        total += c[a:b - 1].sum() - weight * _geometric_length_penalty(b - a, mean)
    return total


def _brute(sims, mean, weight, max_len):
    n = len(sims) + 1
    c = _centred(sims)
    best, arg = -math.inf, None
    for cuts in itertools.product([0, 1], repeat=n - 1):
        starts = [0] + [i + 1 for i, x in enumerate(cuts) if x]
        edges = starts + [n]
        if any(b - a > max_len for a, b in zip(edges, edges[1:])):
            continue
        s = _score(starts, n, c, mean, weight)
        if s > best + 1e-12:
            best, arg = s, [starts]
        elif abs(s - best) <= 1e-12:
            arg.append(starts)
    return best, arg


def test_geometric_penalty():
    for mean in (2.0, 4.0, 10.0):
        p = 1 / mean
        for L in (1, 2, 5, 9):
            assert math.isclose(_geometric_length_penalty(L, mean), -math.log((1 - p) ** (L - 1) * p))


@pytest.mark.parametrize("seed", range(12))
def test_segment_dp_optimal(seed):
    rng = np.random.default_rng(seed)
    n = int(rng.integers(1, 9))
    sims = rng.uniform(0.0, 1.0, n - 1)
    if n > 2 and seed % 3 == 0:
        sims[0] = np.nan
    mean, w = 3.0, 0.5
    starts = segment_dp(sims, mean, w, max_segment_length=n)
    best, args = _brute(sims, mean, w, n)
    assert math.isclose(_score(starts, n, _centred(sims), mean, w), best, abs_tol=1e-9)
    assert starts in args


def test_segment_dp_max_length():
    rng = np.random.default_rng(99)
    sims = rng.uniform(0.0, 1.0, 7)
    starts = segment_dp(sims, 3.0, 0.5, max_segment_length=2)
    best, args = _brute(sims, 3.0, 0.5, 2)
    assert math.isclose(_score(starts, 8, _centred(sims), 3.0, 0.5), best, abs_tol=1e-9)
    edges = starts + [8]
    assert all(b - a <= 2 for a, b in zip(edges, edges[1:]))


def _three_docs():
    rng = np.random.default_rng(0)
    sims = 0.9 + rng.uniform(-0.02, 0.02, 14)
    sims[4] = 0.1
    sims[9] = 0.1
    return sims


def test_segment_dp_three_documents():
    starts = segment_dp(_three_docs(), 4.0, 0.3)
    assert starts == [0, 5, 10]
    assert starts == sorted(set(starts))


def test_segment_dp_edge_cases():
    assert segment_dp(np.array([])) == [0]
    out = segment_dp(np.full(5, np.nan))
    assert out[0] == 0 and out == sorted(set(out)) and all(0 <= s < 6 for s in out)


def test_textiling_single_valley():
    # window=1 avoids the 3-gap plateau; zero baseline avoids np.convolve's zero-padded edges (kept from NLP)
    sims = np.zeros(20)
    sims[10] = -1.0
    depth, starts = textiling_depth_scores(sims, window=1)
    assert len(depth) == 20
    assert starts == [0, 11]


def test_textiling_window3_plateau_and_edge_quirk():
    sims = np.zeros(20)
    sims[10] = -1.0
    _, starts = textiling_depth_scores(sims)
    assert starts == [0, 10, 11, 12]  # smoothed valley spans three gaps
    _, starts = textiling_depth_scores(np.full(10, 0.5))
    assert starts == [0, 1, 10]  # zero-padded edges create depth: verbatim NLP quirk


def test_textiling_constant_threshold_empty():
    _, starts = textiling_depth_scores(np.zeros(10))
    assert starts == [0]  # std 0 -> threshold max + 1
    sims = np.zeros(20)
    sims[10] = -1.0
    _, s_hi = textiling_depth_scores(sims, depth_threshold=100.0)
    assert s_hi == [0]
    _, s_lo = textiling_depth_scores(sims, window=1, depth_threshold=-1.0)
    assert len(s_lo) == 21
    with pytest.warns(RuntimeWarning):
        d, s = textiling_depth_scores(np.array([]))
    assert len(d) == 0 and s == [0]


def test_calibrate():
    rng = np.random.default_rng(1)
    invs = {}
    for name in "ab":
        s = 0.9 + rng.uniform(-0.05, 0.05, 60)
        s[::5] = 0.2
        invs[name] = s
    cands = (0.02, 0.1, 0.5, 2.0)
    best = None
    for w in cands:
        lengths = []
        for s in invs.values():
            e = segment_dp(s, 4.0, w) + [len(s) + 1]
            lengths += [b - a for a, b in zip(e, e[1:])]
        gap = abs(np.mean(lengths) - 4.0)
        if best is None or gap < best[0]:
            best = (gap, w)
    assert calibrate_length_penalty_weight(invs, candidates=cands) == best[1]
    assert calibrate_length_penalty_weight({}, candidates=cands) == 0.02
    # identical candidates: tie -> first
    assert calibrate_length_penalty_weight(invs, candidates=(0.5, 0.5)) == 0.5


def test_predicted_start_rate():
    assert predicted_start_rate([0, 3], 6) == 1 / 3
    assert math.isnan(predicted_start_rate([0], 0))


def test_converters():
    for s in ([0], [0, 3, 5], [5, 3, 3], [0, 0, 2]):
        assert boundaries_to_starts(starts_to_boundaries(s)) == sorted(set(s) | {0})
    sims = _three_docs()
    starts = segment_dp(sims, 4.0, 0.3)
    segs = boundaries_to_segments(15, starts_to_boundaries(starts))
    assert segs == [(0, 5), (5, 10), (10, 15)]


def test_lazy_exports():
    from archival_structures.stream_analysis import (  # noqa: F401
        segment_dp as f, starts_to_boundaries as g, textiling_depth_scores as h,
    )
    assert f is segment_dp
