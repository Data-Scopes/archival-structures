"""
Sequence pattern analysis for ordered cluster-label sequences.

Given a sequence of integer cluster labels (one per page or scan), this
module identifies recurring structural patterns: long homogeneous runs,
repeating n-grams, tandem repeats, and between-cluster transitions.

The functions are label-agnostic -- they accept any sequence of integers
and work equally well with labels produced by visual clustering (DINOv2
embeddings via :func:`~archival_structures.stream_analysis.cluster_embeddings`)
or by layout clustering (text-line geometry via
:func:`~archival_structures.analysis.page_layout_clustering.cluster_page_layouts`).

Typical pipeline::

    # After obtaining cluster labels (visual or layout):
    rle = run_length_encode(labels)
    tandem = find_tandem_repeats(labels, min_unit=1, max_unit=4, min_repeats=3)
    ngrams = find_frequent_ngrams(labels, n=3, min_count=5)
    trans = label_transition_matrix(labels)

For visual clusters with many fine-grained HDBSCAN labels, consider
:func:`coarsen_by_hierarchy` to merge nearby clusters before analysing the
sequence -- this can reveal coarser structural patterns that are harder to
see when each small cluster variant is treated as distinct.
"""
from __future__ import annotations

from collections import Counter
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Run-length encoding
# ---------------------------------------------------------------------------

def run_length_encode(
    labels: Sequence[int],
    noise_label: int = -1,
) -> list[dict]:
    """Encode a label sequence as a list of consecutive runs.

    Args:
        labels: sequence of integer cluster labels in page/scan order.
        noise_label: HDBSCAN noise marker (default ``-1``).  Noise runs are
            included in the output (with ``is_noise=True``).

    Returns:
        List of dicts, one per run, with keys:

        * ``cluster`` -- the label value.
        * ``start`` -- index of the first element (inclusive).
        * ``end`` -- index after the last element (exclusive).
        * ``length`` -- run length (``end - start``).
        * ``is_noise`` -- whether this run's label equals *noise_label*.
    """
    labels = list(labels)
    if not labels:
        return []
    runs: list[dict] = []
    start = 0
    for i in range(1, len(labels) + 1):
        if i == len(labels) or labels[i] != labels[i - 1]:
            cluster = labels[start]
            runs.append(
                {
                    "cluster": cluster,
                    "start": start,
                    "end": i,
                    "length": i - start,
                    "is_noise": cluster == noise_label,
                }
            )
            start = i
    return runs


def merge_short_runs(
    rle: list[dict],
    min_length: int,
    noise_label: int = -1,
) -> list[dict]:
    """Merge short runs into their longer neighbours.

    Short isolated runs often represent mis-clustered pages or outliers.
    This function absorbs any run shorter than *min_length* into whichever
    of its two neighbours has the longer run (the left neighbour wins ties).
    The merge is applied iteratively until no short runs remain.

    Args:
        rle: run-length-encoded sequence from :func:`run_length_encode`.
        min_length: runs shorter than this are merged.
        noise_label: noise-label value, passed through to the returned dicts.

    Returns:
        New RLE list with short runs absorbed (may have fewer entries than
        the input).  Start/end/length values are recomputed.
    """
    labels: list[int] = []
    for run in rle:
        labels.extend([run["cluster"]] * run["length"])

    changed = True
    while changed:
        changed = False
        merged_labels: list[int] = []
        i = 0
        while i < len(labels):
            # Find extent of current run
            j = i
            while j < len(labels) and labels[j] == labels[i]:
                j += 1
            run_len = j - i
            if run_len < min_length:
                # Absorb into best neighbour
                before = (
                    merged_labels[-1] if merged_labels else None
                )
                after = labels[j] if j < len(labels) else None
                if before is None and after is None:
                    replace = labels[i]
                elif before is None:
                    replace = after
                elif after is None:
                    replace = before
                else:
                    # Pick the neighbour whose existing run is longer
                    before_run = sum(1 for x in reversed(merged_labels) if x == before)
                    after_run = sum(1 for k in range(j, len(labels)) if labels[k] == after)
                    replace = before if before_run >= after_run else after
                merged_labels.extend([replace] * run_len)
                changed = True
            else:
                merged_labels.extend(labels[i:j])
            i = j
        labels = merged_labels

    return run_length_encode(labels, noise_label=noise_label)


# ---------------------------------------------------------------------------
# N-gram analysis
# ---------------------------------------------------------------------------

def ngram_counts(
    labels: Sequence[int],
    n: Union[int, Sequence[int]],
    skip_noise: bool = True,
    noise_label: int = -1,
) -> Counter:
    """Count every n-gram in *labels*.

    Args:
        labels: integer label sequence.
        n: window size, or a sequence of window sizes.
        skip_noise: if True, skip any n-gram that contains *noise_label*.
        noise_label: HDBSCAN noise marker.

    Returns:
        :class:`collections.Counter` mapping n-gram tuples to frequencies.
    """
    window_sizes = [n] if isinstance(n, int) else list(n)
    lseq = list(labels)
    counts: Counter = Counter()
    for w in window_sizes:
        for i in range(len(lseq) - w + 1):
            gram = tuple(lseq[i : i + w])
            if skip_noise and noise_label in gram:
                continue
            counts[gram] += 1
    return counts


def find_frequent_ngrams(
    labels: Sequence[int],
    n: Union[int, Sequence[int]] = (2, 3),
    min_count: int = 2,
    skip_noise: bool = True,
    noise_label: int = -1,
) -> pd.DataFrame:
    """Find n-grams that appear at least *min_count* times, with positions.

    Args:
        labels: integer label sequence.
        n: window size or collection of window sizes.
        min_count: minimum frequency for inclusion.
        skip_noise: skip windows containing *noise_label*.
        noise_label: HDBSCAN noise marker.

    Returns:
        DataFrame sorted by ``count`` descending, with columns:

        * ``ngram`` -- the n-gram tuple.
        * ``n`` -- window size.
        * ``count`` -- total occurrences.
        * ``positions`` -- list of start indices.
    """
    window_sizes = [n] if isinstance(n, int) else list(n)
    lseq = list(labels)
    # Collect positions per n-gram
    positions: dict[tuple, list[int]] = {}
    for w in window_sizes:
        for i in range(len(lseq) - w + 1):
            gram = tuple(lseq[i : i + w])
            if skip_noise and noise_label in gram:
                continue
            if gram not in positions:
                positions[gram] = []
            positions[gram].append(i)

    rows = []
    for gram, pos in positions.items():
        if len(pos) >= min_count:
            rows.append(
                {
                    "ngram": gram,
                    "n": len(gram),
                    "count": len(pos),
                    "positions": pos,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["ngram", "n", "count", "positions"])
    return (
        pd.DataFrame(rows)
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Tandem repeat detection
# ---------------------------------------------------------------------------

def find_tandem_repeats(
    labels: Sequence[int],
    min_unit: int = 1,
    max_unit: int = 5,
    min_repeats: int = 2,
    noise_label: int = -1,
    skip_noise: bool = True,
) -> pd.DataFrame:
    """Find consecutive repetitions of subsequences (tandem repeats).

    A *tandem repeat* is a sub-sequence of length *k* that appears ≥
    *min_repeats* times in direct succession.  For example, in the sequence
    ``[1, 2, 3, 4, 3, 4, 3, 4, 1]`` the unit ``(3, 4)`` forms a tandem
    repeat of length 2 starting at position 2 with 3 repetitions.

    The search is greedy per unit length: at each position the longest
    tandem with the current unit is recorded, then the cursor jumps to the
    end of that tandem (avoiding double-counting the same repeat span for
    different unit lengths).

    Args:
        labels: integer label sequence.
        min_unit: minimum unit length to search (default 1).
        max_unit: maximum unit length to search (default 5).
        min_repeats: minimum number of consecutive repetitions to report.
        noise_label: HDBSCAN noise marker.
        skip_noise: if True, skip any tandem unit that contains *noise_label*.

    Returns:
        DataFrame sorted by ``start``, with columns:

        * ``unit`` -- the repeating sub-sequence (tuple).
        * ``unit_length`` -- length of the unit.
        * ``start`` -- index of the first occurrence.
        * ``n_repeats`` -- number of consecutive repetitions.
        * ``end`` -- index after the last occurrence.
        * ``total_length`` -- ``unit_length × n_repeats``.
    """
    lseq = list(labels)
    n = len(lseq)
    rows = []

    for unit_len in range(min_unit, max_unit + 1):
        i = 0
        while i <= n - unit_len * min_repeats:
            unit = tuple(lseq[i : i + unit_len])
            if skip_noise and noise_label in unit:
                i += 1
                continue
            # Count consecutive repetitions
            count = 1
            while i + unit_len * (count + 1) <= n:
                nxt = tuple(lseq[i + unit_len * count : i + unit_len * (count + 1)])
                if nxt == unit:
                    count += 1
                else:
                    break
            if count >= min_repeats:
                rows.append(
                    {
                        "unit": unit,
                        "unit_length": unit_len,
                        "start": i,
                        "n_repeats": count,
                        "end": i + unit_len * count,
                        "total_length": unit_len * count,
                    }
                )
                # Jump past this tandem repeat
                i += unit_len * count
            else:
                i += 1

    if not rows:
        return pd.DataFrame(
            columns=["unit", "unit_length", "start", "n_repeats", "end", "total_length"]
        )
    return pd.DataFrame(rows).sort_values("start").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Transition matrix
# ---------------------------------------------------------------------------

def label_transition_matrix(
    labels: Sequence[int],
    noise_label: int = -1,
    normalize: bool = False,
) -> pd.DataFrame:
    """Compute the transition count (or probability) matrix.

    Counts how many times each ordered pair (cluster_i → cluster_j) appears
    as consecutive labels in the sequence (excluding transitions that
    involve *noise_label*).

    Args:
        labels: integer label sequence.
        noise_label: transitions involving this label are excluded.
        normalize: if True, normalise each row to sum to 1.0 (transition
            probabilities); rows that sum to zero remain zero.

    Returns:
        Square DataFrame indexed and columned by the unique non-noise labels
        found in *labels*, with transition counts (or probabilities).
    """
    lseq = [l for l in labels if l != noise_label]
    unique = sorted(set(lseq))
    idx = {c: i for i, c in enumerate(unique)}
    mat = np.zeros((len(unique), len(unique)), dtype=float)
    for a, b in zip(lseq, lseq[1:]):
        mat[idx[a], idx[b]] += 1
    df = pd.DataFrame(mat, index=unique, columns=unique)
    if normalize:
        row_sums = df.sum(axis=1)
        df = df.div(row_sums.replace(0, np.nan), axis=0).fillna(0.0)
    return df


# ---------------------------------------------------------------------------
# Hierarchical coarsening of cluster labels
# ---------------------------------------------------------------------------

def coarsen_by_hierarchy(
    labels: np.ndarray,
    embeddings: np.ndarray,
    n_coarse: int,
    noise_label: int = -1,
) -> np.ndarray:
    """Remap fine-grained HDBSCAN labels to coarser clusters.

    Computes the centroid of each non-noise cluster in embedding space,
    runs scipy agglomerative clustering on those centroids, and maps the
    original labels to the coarser groupings.  Noise labels remain -1.

    This is useful when HDBSCAN has produced many small clusters (e.g.
    separating verso and recto variants of the same layout type) and you
    want a coarser label sequence before doing sequence pattern analysis.

    Args:
        labels: *(N,)* integer array of HDBSCAN cluster labels.
        embeddings: *(N, D)* embedding array (same order as *labels*).
        n_coarse: target number of coarse clusters.
        noise_label: label value for HDBSCAN noise (excluded from centroid
            computation, remapped to -1 in the output).

    Returns:
        *(N,)* integer array of coarse cluster labels (0-based), with
        noise positions set to -1.

    Raises:
        ImportError: if *scipy* is not installed.
    """
    try:
        from scipy.cluster.hierarchy import fcluster, linkage
    except ImportError as exc:
        raise ImportError(
            "scipy is required for coarsen_by_hierarchy. "
            "Install it with:  pip install scipy"
        ) from exc

    labels = np.asarray(labels)
    unique_clusters = sorted(set(labels.tolist()) - {noise_label})
    if len(unique_clusters) <= n_coarse:
        # Already at or below target -- return 0-based remapping
        remap = {c: i for i, c in enumerate(unique_clusters)}
        out = np.where(labels == noise_label, -1, np.vectorize(remap.get)(labels, -1))
        return out.astype(int)

    # Compute cluster centroids
    centroids = np.stack(
        [embeddings[labels == c].mean(axis=0) for c in unique_clusters]
    )
    # Agglomerative clustering on centroids
    Z = linkage(centroids, method="ward")
    coarse = fcluster(Z, t=n_coarse, criterion="maxclust") - 1  # 0-based
    # Build mapping: original cluster → coarse cluster
    fine_to_coarse = {c: int(coarse[i]) for i, c in enumerate(unique_clusters)}
    out = np.where(
        labels == noise_label, -1, np.vectorize(fine_to_coarse.get)(labels, -1)
    )
    return out.astype(int)
