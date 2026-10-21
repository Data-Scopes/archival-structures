"""Duplicate-page (re-shoot) detection over plain ``(page_id, text, position)`` triples.

Ported from LotA step 2.3 (``archival_language/corpus/integrity.py``, decision P2-14). Two
stages: a MinHash sketch over character-shingle sets, banded into LSH buckets to find candidate
pairs cheaply, with every pair of pages at adjacent positions added as a candidate regardless of
what the bucketing found; then every candidate is confirmed with the exact shingle Jaccard.

Adjacency is by **position** (for example the scan number): every kept page at position ``p``
pairs with every kept page at ``p + 1``, cross pairs included. It is never adjacency by sequence
order, because a re-shoot of a two-page scan is a same-side pair across scans N and N+1, and
those pages are not neighbours in sequence. Pages whose position is ``None`` are never adjacent.

The module is archive-agnostic: it knows nothing about page splitting, region types or reading
order. The caller builds the page text. Nothing is stored or modified.

The defaults were calibrated by LotA on States General (SG) HTR and are not validated elsewhere.
``DEFAULT_MIN_SHINGLES`` was raised from 20 to 100 after a first pass on LotA's smoke set: all 755
raw candidates with fewer than 50 shingles (28.9% of them) were short, formulaic session-date
stamps ("den 22e. October 1781.") shared verbatim by distinct stub pages from one session, not
re-shoots. The next value up in the observed distribution was 117 shingles, so 100 sits in the gap
between that cluster and real page text. ``DEFAULT_DUPLICATE_THRESHOLD`` (0.6) sits close to the
re-shoots: the 23 rows of the 13 documented SG re-shoots have exact Jaccard 0.615 and up, and
formulaic but distinct pages reach 0.76 (phase 08, F08-R.2), so the ``adjacent`` column separates
re-shoots better than the Jaccard does.
"""

from __future__ import annotations

import random
import zlib
from collections.abc import Collection, Iterable, Mapping
from functools import lru_cache

import numpy as np
import pandas as pd

DEFAULT_SHINGLE_LENGTH = 5
DEFAULT_MIN_SHINGLES = 100
DEFAULT_NUM_HASHES = 128
DEFAULT_BANDS = 16
MINHASH_PRIME = (1 << 31) - 1  # 31-bit Mersenne prime; keeps a*h+b inside uint64
DEFAULT_MINHASH_SEED = 20261004
DEFAULT_DUPLICATE_THRESHOLD = 0.6

CANDIDATE_COLUMNS = (
    "page_id_a",
    "page_id_b",
    "position_a",
    "position_b",
    "n_shingles_a",
    "n_shingles_b",
    "jaccard_estimated",
    "jaccard_exact",
    "adjacent",
)


def shingles(text: str, k: int = DEFAULT_SHINGLE_LENGTH) -> set[str]:
    """Character k-grams of ``text``; a text shorter than ``k`` is its own single shingle."""
    if len(text) < k:
        return {text} if text else set()
    return {text[i : i + k] for i in range(len(text) - k + 1)}


@lru_cache(maxsize=8)
def _coefficients(n: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = random.Random(seed)
    a = np.array([rng.randrange(1, MINHASH_PRIME) for _ in range(n)], dtype=np.uint64)
    b = np.array([rng.randrange(0, MINHASH_PRIME) for _ in range(n)], dtype=np.uint64)
    a.flags.writeable = False
    b.flags.writeable = False
    return a, b


def minhash_coefficients(n: int, *, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """The ``n`` linear-hash coefficients ``(a, b)`` for ``seed`` (cached, read-only arrays)."""
    return _coefficients(n, seed)


def minhash_sketch(
    shingle_set: Collection[str],
    *,
    num_hashes: int = DEFAULT_NUM_HASHES,
    seed: int = DEFAULT_MINHASH_SEED,
) -> tuple[int, ...]:
    """``num_hashes`` min-hashes: one ``zlib.crc32`` base hash per shingle (deterministic across
    runs, unlike ``hash()``), then ``num_hashes`` linear hashes of it, vectorised."""
    if not shingle_set:
        return tuple(int(MINHASH_PRIME) for _ in range(num_hashes))
    coeff_a, coeff_b = minhash_coefficients(num_hashes, seed=seed)
    base = np.array(
        [zlib.crc32(s.encode("utf-8")) % MINHASH_PRIME for s in shingle_set], dtype=np.uint64
    )
    permuted = (base[:, None] * coeff_a[None, :] + coeff_b[None, :]) % np.uint64(MINHASH_PRIME)
    return tuple(int(v) for v in permuted.min(axis=0))


def lsh_candidates(
    sketches: Mapping[str, tuple[int, ...]], *, bands: int = DEFAULT_BANDS
) -> set[tuple[str, str]]:
    """Page-id pairs (``a < b``) that share at least one LSH band bucket."""
    candidates: set[tuple[str, str]] = set()
    if not sketches:
        return candidates
    n = len(next(iter(sketches.values())))
    rows = n // bands
    for band in range(bands):
        start, end = band * rows, (band + 1) * rows
        buckets: dict[tuple[int, ...], list[str]] = {}
        for page_id, sketch in sketches.items():
            buckets.setdefault(sketch[start:end], []).append(page_id)
        for ids in buckets.values():
            if len(ids) < 2:
                continue
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    a, b = ids[i], ids[j]
                    candidates.add((a, b) if a < b else (b, a))
    return candidates


def duplicate_page_candidates(
    pages: Iterable[tuple[str, str, int | None]],
    *,
    threshold: float = DEFAULT_DUPLICATE_THRESHOLD,
    adjacency: bool = True,
    shingle_length: int = DEFAULT_SHINGLE_LENGTH,
    min_shingles: int = DEFAULT_MIN_SHINGLES,
    num_hashes: int = DEFAULT_NUM_HASHES,
    bands: int = DEFAULT_BANDS,
    seed: int = DEFAULT_MINHASH_SEED,
) -> pd.DataFrame:
    """Suspected duplicate page pairs among ``(page_id, text, position)`` triples.

    Pages with fewer than ``min_shingles`` shingles are excluded first (count in
    ``attrs["n_excluded_near_empty"]``; pages compared in ``attrs["n_pages_compared"]``).
    Candidates are the LSH pairs plus, when ``adjacency`` is true, every pair of kept pages at
    positions ``p`` and ``p + 1``. Every candidate is confirmed by the exact Jaccard and reported
    when it reaches ``threshold``. The ``adjacent`` column always reports position adjacency,
    also when ``adjacency=False``. Rows are sorted by ``(page_id_a, page_id_b)`` with
    ``page_id_a < page_id_b``.
    """
    if not 0.0 <= threshold <= 1.0:
        raise ValueError(f"threshold must be in [0, 1], got {threshold}")
    if shingle_length < 1:
        raise ValueError(f"shingle_length must be >= 1, got {shingle_length}")
    if bands < 1 or num_hashes % bands != 0:
        raise ValueError(f"num_hashes ({num_hashes}) must be divisible by bands ({bands})")

    page_list = list(pages)
    seen: set[str] = set()
    for page_id, _, _ in page_list:
        if page_id in seen:
            raise ValueError(f"repeated page_id: {page_id!r}")
        seen.add(page_id)

    shingle_sets: dict[str, set[str]] = {}
    positions: dict[str, int | None] = {}
    n_excluded = 0
    for page_id, text, position in page_list:
        sset = shingles(text, shingle_length)
        if len(sset) < min_shingles:
            n_excluded += 1
            continue
        shingle_sets[page_id] = sset
        positions[page_id] = position

    sketches = {
        pid: minhash_sketch(s, num_hashes=num_hashes, seed=seed) for pid, s in shingle_sets.items()
    }
    pairs = lsh_candidates(sketches, bands=bands)

    by_position: dict[int, list[str]] = {}
    for page_id, position in positions.items():
        if position is not None:
            by_position.setdefault(position, []).append(page_id)
    adjacent_pairs: set[tuple[str, str]] = set()
    for position, page_ids in by_position.items():
        for next_id in by_position.get(position + 1, []):
            for page_id in page_ids:
                adjacent_pairs.add(
                    (page_id, next_id) if page_id < next_id else (next_id, page_id)
                )

    all_pairs = pairs | adjacent_pairs if adjacency else pairs

    rows = []
    for a, b in sorted(all_pairs):
        sa, sb = shingle_sets[a], shingle_sets[b]
        inter = len(sa & sb)
        union = len(sa | sb)
        jaccard_exact = inter / union if union else 0.0
        jaccard_estimated = sum(x == y for x, y in zip(sketches[a], sketches[b])) / len(
            sketches[a]
        )
        if jaccard_exact < threshold:
            continue
        rows.append(
            {
                "page_id_a": a,
                "page_id_b": b,
                "position_a": positions[a],
                "position_b": positions[b],
                "n_shingles_a": len(sa),
                "n_shingles_b": len(sb),
                "jaccard_estimated": jaccard_estimated,
                "jaccard_exact": jaccard_exact,
                "adjacent": (a, b) in adjacent_pairs,
            }
        )
    frame = pd.DataFrame(rows, columns=list(CANDIDATE_COLUMNS))
    frame.attrs["n_excluded_near_empty"] = n_excluded
    frame.attrs["n_pages_compared"] = len(shingle_sets)
    return frame
