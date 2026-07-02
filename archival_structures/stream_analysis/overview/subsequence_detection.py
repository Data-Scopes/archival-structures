"""
Detect visually homogeneous subsequences in an ordered scan sequence.

A heterogeneous archival folder often contains one or more *book-like*
subsequences -- runs of scans that share the same binding, paper, and layout
style -- embedded in a broader mix of single-sheet documents, covers, and
photographs.  The core signal is the **adjacent cosine similarity** between
consecutive DINOv2 (or CLIP) embeddings: within a book-like run the values
stay high (≥ 0.85 or so); at transitions between document types they drop
sharply.

Typical pipeline::

    embeddings, image_ids = load_cached_embeddings(...)
    similarities = compute_adjacent_similarities(embeddings)
    threshold = suggest_threshold(similarities)          # data-adaptive
    boundaries = detect_boundaries_threshold(similarities, threshold)
    df = score_all_segments(embeddings, image_ids, boundaries, similarities)
    books = df[df['is_book_like']]

Three complementary signals can be passed to :func:`score_all_segments` and
:func:`detect_book_like_subsequences` for richer classification:

* **cluster_labels** -- HDBSCAN labels from the clustering step; a book-like
  run that is dominated by one or two clusters has a lower per-segment entropy.
* **opening_scores** -- per-scan probability that the scan is a two-page
  opening (from :func:`archival_structures.analysis.opening_detection`); a
  physical book produces consistently high values while single sheets don't.
* Alternatively, any per-scan numeric score can be passed in place of
  ``opening_scores`` (e.g. a full-text-page fraction or ink-density score).

Change-point detection via *ruptures* (optional, not a declared dependency)
is available through :func:`detect_boundaries_changepoint` when the library is
installed.  The threshold-based method works well without it and is the
default.
"""
from __future__ import annotations

import logging
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter1d

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core similarity signal
# ---------------------------------------------------------------------------

def compute_adjacent_similarities(embeddings: np.ndarray) -> np.ndarray:
    """Cosine similarity between each consecutive pair of L2-normalised embeddings.

    For an input of shape *(N, D)* where each row is already L2-normalised,
    the dot product equals cosine similarity.

    Args:
        embeddings: *(N, D)* float32 array of L2-normalised embedding vectors
            in scan order.

    Returns:
        Array of shape *(N-1,)* with cosine similarities in [-1, 1].  For
        natural image embeddings the values are almost always positive and
        typically in [0.4, 1.0].  Returns an empty array when *N* < 2.
    """
    if len(embeddings) < 2:
        return np.array([], dtype=np.float32)
    return np.sum(embeddings[:-1] * embeddings[1:], axis=1).astype(np.float32)


def suggest_threshold(
    similarities: np.ndarray,
    percentile: float = 15.0,
) -> float:
    """Suggest a similarity threshold from the empirical distribution.

    The *percentile*-th percentile of the similarity values is a natural
    starting point: in a sequence with a moderate number of cross-document
    transitions, the bottom ~15% of adjacent similarities correspond to those
    transitions, so the 15th percentile sits just below the bulk of
    within-document similarities.

    Inspect the similarity histogram first and adjust if the sequence is
    very heterogeneous (many transitions → raise percentile) or very
    homogeneous (few transitions → lower percentile).

    Args:
        similarities: adjacent similarity array from
            :func:`compute_adjacent_similarities`.
        percentile: which percentile to return (default 15).

    Returns:
        Scalar threshold value.
    """
    if len(similarities) == 0:
        return 0.8
    return float(np.percentile(similarities, percentile))


# ---------------------------------------------------------------------------
# Boundary detection
# ---------------------------------------------------------------------------

def detect_boundaries_threshold(
    similarities: np.ndarray,
    threshold: float = 0.80,
    smooth_sigma: float = 1.5,
    min_gap: int = 3,
) -> list[int]:
    """Find boundary indices via a smoothed similarity threshold.

    A *boundary at index i* signals a split between scan *i* and scan *i+1*.
    The algorithm smooths the similarity sequence with a Gaussian filter to
    suppress single-frame noise, then groups all positions below *threshold*
    and selects the local minimum from each group as the precise boundary.

    Args:
        similarities: *(N-1,)* adjacent similarity array in scan order.
        threshold: similarities below this value are candidate boundaries.
            Use :func:`suggest_threshold` to choose a data-adaptive value.
        smooth_sigma: standard deviation of the Gaussian smoothing kernel
            (in samples).  Set to 0 to disable smoothing.
        min_gap: minimum separation between distinct boundary groups.  Groups
            of below-threshold positions that are closer together than this
            are treated as one boundary (the deepest minimum is kept).

    Returns:
        Sorted list of boundary indices in [0, N-2].  Empty list if no
        position is below *threshold*.
    """
    if len(similarities) == 0:
        return []

    smoothed = (
        gaussian_filter1d(similarities.astype(float), smooth_sigma)
        if smooth_sigma > 0
        else similarities.astype(float)
    )

    below = np.where(smoothed < threshold)[0]
    if len(below) == 0:
        return []

    groups = np.split(below, np.where(np.diff(below) > min_gap)[0] + 1)
    return sorted(int(g[np.argmin(smoothed[g])]) for g in groups if len(g) > 0)


def detect_boundaries_changepoint(
    embeddings: np.ndarray,
    penalty: float = 3.0,
    model: str = "rbf",
    min_size: int = 3,
) -> list[int]:
    """Find boundaries with Pelt change-point detection (requires *ruptures*).

    Uses :class:`ruptures.Pelt` with the specified cost *model*.  The ``rbf``
    (radial-basis-function) kernel is recommended for embedding vectors because
    it is sensitive to distributional shifts without requiring a linear model.

    Args:
        embeddings: *(N, D)* L2-normalised embedding array in scan order.
        penalty: Pelt penalty parameter (larger → fewer breakpoints).  Tune
            this to control granularity; see the ruptures documentation for
            guidance.
        model: Pelt cost model (``"rbf"``, ``"l2"``, ``"l1"``).
        min_size: minimum number of scans between consecutive breakpoints.

    Returns:
        Sorted list of boundary indices (split between scan i and i+1).

    Raises:
        ImportError: if *ruptures* is not installed.
    """
    try:
        import ruptures as rpt
    except ImportError as exc:
        raise ImportError(
            "ruptures is required for change-point boundary detection.  "
            "Install it with:  pip install ruptures"
        ) from exc

    algo = rpt.Pelt(model=model, min_size=min_size, jump=1).fit(
        embeddings.astype(float)
    )
    breakpoints = algo.predict(pen=penalty)
    # ruptures returns 1-based end indices; convert to 0-based split indices
    return sorted(int(b) - 1 for b in breakpoints if b < len(embeddings))


# ---------------------------------------------------------------------------
# Segmentation and scoring
# ---------------------------------------------------------------------------

def boundaries_to_segments(n_scans: int, boundaries: list[int]) -> list[tuple[int, int]]:
    """Convert boundary indices to *(start, end)* segment pairs (end exclusive).

    A boundary at position *i* (in the similarity array, so between scan *i*
    and scan *i+1*) produces a split at embedding index *i+1*:

    * segment before boundary:  ``(start, i+1)``
    * segment after boundary:   ``(i+1, next_boundary+1 or n_scans)``

    Args:
        n_scans: total number of scans in the sequence.
        boundaries: sorted boundary indices from :func:`detect_boundaries_threshold`
            or :func:`detect_boundaries_changepoint`.

    Returns:
        List of *(start, end)* tuples covering [0, n_scans) without gaps.
    """
    if not boundaries:
        return [(0, n_scans)]
    starts = [0] + [b + 1 for b in boundaries]
    ends = [b + 1 for b in boundaries] + [n_scans]
    return list(zip(starts, ends))


def score_segment(
    segment: tuple[int, int],
    similarities: np.ndarray,
    embeddings: np.ndarray,
    cluster_labels: Optional[np.ndarray] = None,
    opening_scores: Optional[np.ndarray] = None,
) -> dict:
    """Compute homogeneity metrics for one segment.

    Args:
        segment: *(start, end)* pair (end exclusive) into the scan sequence.
        similarities: *(N-1,)* adjacent cosine similarity array.
        embeddings: *(N, D)* L2-normalised embedding array.
        cluster_labels: optional *(N,)* integer cluster label array (HDBSCAN
            convention: -1 = outlier/noise).  When provided, ``cluster_entropy``,
            ``dominant_cluster``, and ``dominant_cluster_fraction`` are filled.
        opening_scores: optional *(N,)* per-scan score in [0, 1] (e.g. the
            probability that the scan is a two-page opening).  Mean is returned
            as ``opening_consistency``.

    Returns:
        Dict with keys:

        * ``start``, ``end``, ``length`` -- segment position and size.
        * ``mean_similarity`` -- mean adjacent cosine similarity within the
          segment (1.0 for length-1 segments with no pairs).
        * ``min_similarity`` -- minimum adjacent similarity within the segment.
        * ``first_last_similarity`` -- cosine similarity between first and last
          embedding; captures cumulative visual drift across the segment.
        * ``cluster_entropy`` -- Shannon entropy (bits) of the within-segment
          cluster distribution.  Low (≈ 0) means one cluster dominates.
          ``nan`` if *cluster_labels* is not provided.
        * ``dominant_cluster`` -- most common non-noise cluster label, or -1.
        * ``dominant_cluster_fraction`` -- fraction of non-noise scans in the
          dominant cluster.  ``nan`` if *cluster_labels* is not provided.
        * ``opening_consistency`` -- mean opening score, or ``nan``.
    """
    start, end = segment
    length = end - start

    seg_sims = similarities[start : end - 1] if length > 1 else np.array([])
    mean_sim = float(seg_sims.mean()) if len(seg_sims) > 0 else 1.0
    min_sim = float(seg_sims.min()) if len(seg_sims) > 0 else 1.0

    first_last_sim = (
        float(np.dot(embeddings[start], embeddings[end - 1]))
        if length > 1
        else 1.0
    )

    result: dict = {
        "start": start,
        "end": end,
        "length": length,
        "mean_similarity": mean_sim,
        "min_similarity": min_sim,
        "first_last_similarity": first_last_sim,
        "cluster_entropy": float("nan"),
        "dominant_cluster": -1,
        "dominant_cluster_fraction": float("nan"),
        "opening_consistency": float("nan"),
    }

    if cluster_labels is not None:
        seg_labels = cluster_labels[start:end]
        valid = seg_labels[seg_labels >= 0]
        if len(valid) > 0:
            counts = np.bincount(valid.astype(int))
            probs = counts[counts > 0] / counts.sum()
            entropy = float(-(probs * np.log2(probs + 1e-10)).sum())
            dominant = int(np.argmax(counts))
            result["cluster_entropy"] = entropy
            result["dominant_cluster"] = dominant
            result["dominant_cluster_fraction"] = float(counts[dominant] / len(valid))

    if opening_scores is not None:
        result["opening_consistency"] = float(opening_scores[start:end].mean())

    return result


def score_all_segments(
    embeddings: np.ndarray,
    image_ids: Sequence[str],
    boundaries: list[int],
    similarities: Optional[np.ndarray] = None,
    cluster_labels: Optional[np.ndarray] = None,
    opening_scores: Optional[np.ndarray] = None,
    min_segment_length: int = 4,
    min_mean_similarity: float = 0.82,
    min_opening_consistency: Optional[float] = None,
) -> pd.DataFrame:
    """Score all segments and flag book-like candidates.

    Converts *boundaries* to segments, scores each with :func:`score_segment`,
    and applies a ``is_book_like`` filter.

    Args:
        embeddings: *(N, D)* L2-normalised embedding array in scan order.
        image_ids: sequence of N image path strings in scan order.
        boundaries: sorted boundary indices from a detection function.
        similarities: pre-computed adjacent similarities.  Computed from
            *embeddings* if not provided.
        cluster_labels: forwarded to :func:`score_segment`.
        opening_scores: forwarded to :func:`score_segment`.
        min_segment_length: minimum number of scans for ``is_book_like=True``.
        min_mean_similarity: minimum ``mean_similarity`` for ``is_book_like=True``.
        min_opening_consistency: if set, also require ``opening_consistency >=``
            this value for ``is_book_like=True``.

    Returns:
        DataFrame with one row per segment (sorted by ``start``), columns as
        described in :func:`score_segment` plus ``image_ids`` (list of IDs for
        the scans in that segment) and ``is_book_like`` (bool).
    """
    if similarities is None:
        similarities = compute_adjacent_similarities(embeddings)

    segments = boundaries_to_segments(len(embeddings), boundaries)
    rows = []
    for seg in segments:
        scored = score_segment(seg, similarities, embeddings, cluster_labels, opening_scores)
        s, e = seg
        scored["image_ids"] = list(image_ids)[s:e]
        rows.append(scored)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("start").reset_index(drop=True)

    is_book_like = (df["length"] >= min_segment_length) & (
        df["mean_similarity"] >= min_mean_similarity
    )
    if min_opening_consistency is not None:
        is_book_like &= df["opening_consistency"] >= min_opening_consistency
    df["is_book_like"] = is_book_like

    return df


def detect_book_like_subsequences(
    embeddings: np.ndarray,
    image_ids: Sequence[str],
    cluster_labels: Optional[np.ndarray] = None,
    opening_scores: Optional[np.ndarray] = None,
    method: str = "threshold",
    similarity_threshold: Optional[float] = None,
    smooth_sigma: float = 1.5,
    min_gap: int = 3,
    min_segment_length: int = 4,
    min_mean_similarity: float = 0.82,
    min_opening_consistency: Optional[float] = None,
    penalty: float = 3.0,
    ruptures_model: str = "rbf",
    ruptures_min_size: int = 3,
) -> pd.DataFrame:
    """Detect book-like subsequences in an ordered scan sequence.

    Convenience wrapper that runs boundary detection and scoring in one call.

    Args:
        embeddings: *(N, D)* L2-normalised embedding array in scan order.
        image_ids: sequence of N image path strings in scan order.
        cluster_labels: optional *(N,)* HDBSCAN cluster labels (see
            :func:`score_segment`).
        opening_scores: optional *(N,)* per-scan opening score (see
            :func:`score_segment`).
        method: ``"threshold"`` (default) or ``"changepoint"`` (requires
            *ruptures*).
        similarity_threshold: threshold for the ``"threshold"`` method.
            Defaults to the 15th percentile of adjacent similarities if not
            set (see :func:`suggest_threshold`).
        smooth_sigma: Gaussian smoothing sigma for ``"threshold"`` method.
        min_gap: minimum boundary group separation for ``"threshold"`` method.
        min_segment_length: minimum scans for ``is_book_like=True``.
        min_mean_similarity: minimum mean adjacent similarity for
            ``is_book_like=True``.
        min_opening_consistency: optional additional opening-score filter.
        penalty: Pelt penalty for ``"changepoint"`` method.
        ruptures_model: cost model for ``"changepoint"`` method.
        ruptures_min_size: minimum segment size for ``"changepoint"`` method.

    Returns:
        DataFrame with one row per detected segment; see :func:`score_all_segments`
        for column descriptions.  Segments are ordered by ``start``.
    """
    if len(embeddings) == 0:
        return pd.DataFrame()

    similarities = compute_adjacent_similarities(embeddings)

    if method == "threshold":
        threshold = (
            similarity_threshold
            if similarity_threshold is not None
            else suggest_threshold(similarities)
        )
        logger.info(f"Threshold-based boundary detection (threshold={threshold:.3f})")
        boundaries = detect_boundaries_threshold(
            similarities,
            threshold=threshold,
            smooth_sigma=smooth_sigma,
            min_gap=min_gap,
        )
    elif method == "changepoint":
        logger.info(f"Change-point boundary detection (penalty={penalty}, model={ruptures_model!r})")
        boundaries = detect_boundaries_changepoint(
            embeddings,
            penalty=penalty,
            model=ruptures_model,
            min_size=ruptures_min_size,
        )
    else:
        raise ValueError(f"Unknown method {method!r}. Choose 'threshold' or 'changepoint'.")

    logger.info(f"Found {len(boundaries)} boundaries → {len(boundaries) + 1} segments")

    return score_all_segments(
        embeddings,
        image_ids,
        boundaries,
        similarities=similarities,
        cluster_labels=cluster_labels,
        opening_scores=opening_scores,
        min_segment_length=min_segment_length,
        min_mean_similarity=min_mean_similarity,
        min_opening_consistency=min_opening_consistency,
    )
