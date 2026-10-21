"""Tests for ``archival_structures.analysis.duplicate_pages`` (phase 08, step 17)."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from archival_structures.analysis import duplicate_pages as dp
from archival_structures.analysis.duplicate_pages import (
    CANDIDATE_COLUMNS,
    DEFAULT_DUPLICATE_THRESHOLD,
    duplicate_page_candidates,
    lsh_candidates,
    minhash_coefficients,
    minhash_sketch,
    shingles,
)

_LONG_TEXT = (
    "Den negenentwintighsten December is ter Vergaderinge gelesen een Memorie van wegen den "
    "Heer Resident tot Hamburgh aengaende de Commertie der Ingesetenen van dese Landen."
)
_OTHER = "Een geheel andere, onverwante tekst over de Oost-Indische Compagnie en haer schepen."

_PINNED_SKETCH = (
    4954297, 3391849, 3114384, 6051164, 1349067, 4942196, 7767416, 15515318,
    7897942, 14949686, 12603144, 4396397, 10058569, 35378000, 5054352, 13314217,
    28615607, 14622890, 14720348, 3952674, 30957074, 22778582, 1677760, 11079993,
    510735, 110938645, 11127213, 18813157, 5314792, 25378643, 2982317, 24423053,
    1706085, 11862946, 4081895, 10872992, 10367981, 4566639, 16165038, 1721895,
    16425897, 633336, 1319787, 9062046, 7249946, 6996825, 1441107, 9101702,
    4402223, 6036741, 1997742, 2990591, 19522847, 44974415, 3950044, 15687976,
    4922537, 44306933, 1486186, 8076891, 17928090, 22505518, 10119844, 2048470,
    2517653, 3718454, 50174, 959823, 20698530, 11880453, 892008, 27641874,
    22595844, 15586997, 14332598, 1513069, 32329883, 17096638, 3600216, 1535386,
    7614494, 7196254, 6342742, 6963368, 7281508, 3573091, 16268523, 24002830,
    4603290, 8543973, 8057461, 5373179, 3012147, 2864135, 3444438, 6489406,
    5747213, 7373892, 2466196, 9128085, 2923329, 5881860, 18203237, 42069063,
    9340776, 1135836, 7184448, 17618052, 2580362, 9818826, 12151456, 18206294,
    14886577, 12134429, 6271059, 3977745, 32636996, 701994, 5543631, 12841545,
    10145099, 3299305, 33911405, 1569019, 7442885, 981531, 636322, 43419338,
)


def _perturb(text: str, *, fraction: float, seed: int) -> str:
    rng = random.Random(seed)
    chars = list(text)
    n = max(1, int(len(chars) * fraction))
    for pos in rng.sample(range(len(chars)), n):
        chars[pos] = rng.choice("xqz#@")
    return "".join(chars)


def _random_text(seed: int, n: int = 400) -> str:
    rng = random.Random(seed)
    return "".join(rng.choice("abcdefghij klmnop") for _ in range(n))


def test_reshoot_found_with_perturbed_text():
    pages = [
        ("S_0010", _LONG_TEXT, 10),
        ("S_0011", _perturb(_LONG_TEXT, fraction=0.05, seed=1), 11),
        ("S_0099", _OTHER, 99),
    ]
    df = duplicate_page_candidates(pages, min_shingles=20)
    assert [(r.page_id_a, r.page_id_b) for r in df.itertuples()] == [("S_0010", "S_0011")]
    assert bool(df.adjacent.iloc[0])
    assert "S_0099" not in set(df.page_id_a) | set(df.page_id_b)


def test_near_empty_pages_excluded():
    df = duplicate_page_candidates([("a", "hi", 1), ("b", "hi", 2)])
    assert df.empty
    assert tuple(df.columns) == CANDIDATE_COLUMNS
    assert df.attrs["n_excluded_near_empty"] == 2
    assert df.attrs["n_pages_compared"] == 0


def test_same_side_pairs_across_adjacent_positions():
    t1, t2 = _random_text(1), _random_text(2)
    pages = [
        ("p10_1", t1, 10),
        ("p10_2", t2, 10),
        ("p11_1", _perturb(t1, fraction=0.02, seed=3), 11),
        ("p11_2", _perturb(t2, fraction=0.02, seed=4), 11),
    ]
    df = duplicate_page_candidates(pages)
    got = {(r.page_id_a, r.page_id_b): r.adjacent for r in df.itertuples()}
    assert got == {("p10_1", "p11_1"): True, ("p10_2", "p11_2"): True}


def test_non_adjacent_identical_pages_found_by_lsh():
    t = _random_text(5)
    far = duplicate_page_candidates([("a", t, 1), ("b", t, 5)])
    assert len(far) == 1 and not bool(far.adjacent.iloc[0])
    none = duplicate_page_candidates([("a", t, None), ("b", t, None)])
    assert len(none) == 1 and not bool(none.adjacent.iloc[0])


def _band_free_pair():
    """Texts with Jaccard in [0.6, 0.7) whose sketches share no LSH band (found by trial)."""
    for seed in range(200):
        base = _random_text(100 + seed)
        other = _perturb(base, fraction=0.03 + 0.001 * (seed % 20), seed=seed)
        sa, sb = shingles(base), shingles(other)
        jac = len(sa & sb) / len(sa | sb)
        if not 0.6 <= jac < 0.7:
            continue
        ka, kb = minhash_sketch(sa), minhash_sketch(sb)
        if any(ka[i * 8 : (i + 1) * 8] == kb[i * 8 : (i + 1) * 8] for i in range(16)):
            continue
        return base, other
    raise AssertionError("no band-free pair found")


def test_adjacency_false_skips_forced_pairs():
    base, other = _band_free_pair()
    ka, kb = minhash_sketch(shingles(base)), minhash_sketch(shingles(other))
    assert not any(ka[i * 8 : (i + 1) * 8] == kb[i * 8 : (i + 1) * 8] for i in range(16))
    pages = [("a", base, 1), ("b", other, 2)]
    on = duplicate_page_candidates(pages, adjacency=True)
    off = duplicate_page_candidates(pages, adjacency=False)
    assert len(on) == 1 and bool(on.adjacent.iloc[0])
    assert off.empty


def test_adjacency_false_still_reports_adjacent_column():
    t = _random_text(7)
    df = duplicate_page_candidates([("a", t, 1), ("b", t, 2)], adjacency=False)
    assert len(df) == 1 and bool(df.adjacent.iloc[0])


def test_output_sorted_and_normalised():
    t = _random_text(8)
    pages = [("z", t, 3), ("m", t, 1), ("a", t, 2), ("q", t, 9)]
    df = duplicate_page_candidates(pages)
    keys = list(zip(df.page_id_a, df.page_id_b))
    assert keys == sorted(keys)
    assert all(a < b for a, b in keys)
    assert len(keys) == 6


def test_jaccard_estimated_is_share_of_equal_minhashes():
    t = _random_text(9)
    u = _perturb(t, fraction=0.05, seed=2)
    df = duplicate_page_candidates([("a", t, 1), ("b", u, 2)])
    ka, kb = minhash_sketch(shingles(t)), minhash_sketch(shingles(u))
    expect = sum(x == y for x, y in zip(ka, kb)) / 128
    assert df.jaccard_estimated.iloc[0] == expect
    sa, sb = shingles(t), shingles(u)
    assert df.jaccard_exact.iloc[0] == pytest.approx(len(sa & sb) / len(sa | sb))
    assert df.n_shingles_a.iloc[0] == len(sa)


def test_coefficients_pinned():
    a, b = minhash_coefficients(128, seed=20261004)
    assert [int(x) for x in a[:3]] == [374967567, 116290711, 1620431220]
    assert [int(x) for x in b[:3]] == [128921304, 1415345096, 1405934882]
    assert not a.flags.writeable and not b.flags.writeable
    assert minhash_coefficients(128, seed=20261004)[0] is a
    assert minhash_sketch(shingles(_LONG_TEXT)) == _PINNED_SKETCH


def test_validation_errors():
    ok = [("a", "x" * 200, 1)]
    with pytest.raises(ValueError):
        duplicate_page_candidates(ok + [("a", "y" * 200, 2)])
    with pytest.raises(ValueError):
        duplicate_page_candidates(ok, num_hashes=100, bands=16)
    with pytest.raises(ValueError):
        duplicate_page_candidates(ok, threshold=1.5)
    with pytest.raises(ValueError):
        duplicate_page_candidates(ok, threshold=-0.1)
    with pytest.raises(ValueError):
        duplicate_page_candidates(ok, shingle_length=0)


def test_input_not_mutated_and_no_archive_literals():
    t = _random_text(11)
    pages = [("a", t, 1), ("b", t, 2)]
    snapshot = list(pages)
    duplicate_page_candidates(pages)
    assert pages == snapshot
    src = Path(dp.__file__).read_text()
    for literal in ("1.01.02", "NL-HaNA", "main_text", "margin", "reading_order"):
        assert literal not in src, literal


def test_lsh_candidates_pairs():
    sk = {"a": tuple(range(128)), "b": tuple(range(128)), "c": tuple(range(1000, 1128))}
    assert lsh_candidates(sk) == {("a", "b")}
    assert lsh_candidates({}) == set()


def test_default_threshold():
    assert DEFAULT_DUPLICATE_THRESHOLD == 0.6
