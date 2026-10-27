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

Two label-free segmenters ported from NLP (:func:`segment_dp`,
:func:`textiling_depth_scores`) return segment *start* indices rather than the
gap indices of ``detect_boundaries_*``; :func:`starts_to_boundaries` and
:func:`boundaries_to_starts` convert between the two.
"""
from __future__ import annotations

import logging
from math import log
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


# ---------------------------------------------------------------------------
# Label-free segmentation (DP, TextTiling; ported from NLP)
# ---------------------------------------------------------------------------
#
# Ported from NLP ``src/startpage/unsupervised_segmentation.py`` (AS phase 11,
# step 18b).  Function bodies are verbatim.  Convention: ``similarities[i]`` is
# the similarity between pages i and i+1 (length n_pages - 1, NaN allowed).
# Unlike ``detect_boundaries_*`` (gap indices), the two segmenters below return
# segment START indices (always including 0); convert with
# :func:`starts_to_boundaries` / :func:`boundaries_to_starts`.
#
# "Label-free" is precise about one thing and loose about another: no function
# here reads a label at runtime, and nothing is fit per inventory from that
# inventory's own ground truth.  But two global constants were chosen in NLP
# from label-derived corpus statistics -- ``prior_mean_length`` (4.0) and
# ``calibrate_length_penalty_weight``'s ``target_mean_segment_length`` (4.0),
# both the corpus's median true segment length (VOC).  Treat the result as
# "label-free except for two corpus-level constants"; if it is ever used as a
# headline number, re-derive those two from training rows only.  NLP found
# both segmenters score below the majority-class baseline for VOC document
# starts; treat them as section-level tools.

MAX_SEGMENT_LENGTH = 200  # tail cap for the DP's search (VOC: tail to ~150)


def _geometric_length_penalty(length: int, prior_mean_length: float) -> float:
    """-log P(length) under Geometric(p=1/prior_mean_length) on {1, 2, ...}."""
    p = 1.0 / prior_mean_length
    return -((length - 1) * log(1.0 - p) + log(p))


def segment_dp(
    similarities: np.ndarray, prior_mean_length: float = 4.0, length_penalty_weight: float = 1.0,
    max_segment_length: int = MAX_SEGMENT_LENGTH,
) -> list[int]:
    """Returns segment **start** indices (always including 0), not the gap indices of `detect_boundaries_*`; convert with `starts_to_boundaries`.

    Dynamic-programming segmenter under a geometric length prior (the primary
    label-free detector).  NLP found it below the majority-class baseline for
    VOC document starts; treat it as a section-level tool.  Ported from NLP
    `src/startpage/unsupervised_segmentation.py` (AS phase 11, step 18b).

    `similarities[i]` is the cosine similarity between physical pages i and
    i+1 (NaN where unknown, e.g. a gap bridging a missing embedding with no
    embedded neighbor at all -- recentred to exactly the baseline below, so
    it neither encourages nor discourages a cut there).

    Raw cosine similarity isn't centred at 0, so summing it directly within
    a segment scales with segment length and always outscores a length
    penalty that is (for the geometric prior used here) only linear in
    length -- checked on a synthetic 3-document example, and it degenerated
    to never cutting. Centring each inventory's similarities on their own
    median first fixes this: most adjacent-page pairs in any inventory ARE
    within the same document (boundaries are the minority class), so the
    per-inventory median approximates "typical within-document similarity"
    without reading a single label -- a within-document run then nets to ~0
    cohesion (neither rewarding nor penalising length on its own) while
    genuine low-similarity boundaries pull a segment's score down in
    proportion to how anomalous they are.

    DP[j] = best total score for pages [0, j); a candidate segment [i, j)
    scores sum(centred_similarities[i : j-1]) - length_penalty_weight *
    length_penalty(j - i, prior_mean_length). `prior_mean_length` sets the
    penalty's SHAPE (the geometric distribution's mean); because that NLL
    is linear in length, its per-extra-page slope and per-segment intercept
    both scale with `prior_mean_length` together, which over-constrains the
    one real free parameter this method has -- how aggressively it should
    cut at all. `length_penalty_weight` is that second, independent knob
    (see `calibrate_length_penalty_weight`, which sets it without reading
    any label). Search is capped at max_segment_length, trading an
    irrelevant-in-practice bound on segment length for
    O(n * max_segment_length) instead of O(n^2).
    """
    n_pages = len(similarities) + 1
    if n_pages <= 1:
        return [0] if n_pages == 1 else []

    finite = similarities[np.isfinite(similarities)]
    baseline = float(np.median(finite)) if len(finite) else 0.0
    sims = np.nan_to_num(similarities, nan=baseline) - baseline
    cumsum = np.concatenate([[0.0], np.cumsum(sims)])  # cumsum[k] = sum(sims[:k])

    neg_inf = float("-inf")
    best_score = [neg_inf] * (n_pages + 1)
    best_prev = [None] * (n_pages + 1)
    best_score[0] = 0.0

    penalties = {L: length_penalty_weight * _geometric_length_penalty(L, prior_mean_length)
                 for L in range(1, min(max_segment_length, n_pages) + 1)}

    for j in range(1, n_pages + 1):
        for length in range(1, min(max_segment_length, j) + 1):
            i = j - length
            if best_score[i] == neg_inf:
                continue
            segment_cohesion = cumsum[j - 1] - cumsum[i]  # sum(sims[i : j-1])
            score = best_score[i] + segment_cohesion - penalties[length]
            if score > best_score[j]:
                best_score[j] = score
                best_prev[j] = i

    boundaries = []
    j = n_pages
    while j > 0:
        boundaries.append(best_prev[j] if best_prev[j] is not None else 0)
        j = boundaries[-1]
    boundaries.reverse()
    return boundaries


def calibrate_length_penalty_weight(
    similarities_by_inventory: dict[str, np.ndarray], prior_mean_length: float = 4.0,
    target_mean_segment_length: float = 4.0, candidates: Sequence[float] = tuple(np.geomspace(0.02, 2.0, 25)),
) -> float:
    """Pick the `length_penalty_weight` whose mean predicted segment length is closest to the target.

    The mean is pooled across every inventory in `similarities_by_inventory`
    and compared with `target_mean_segment_length` -- a single global
    constant calibrated once against the embedding geometry and a
    corpus-level fact about typical document length (VOC: median true
    segment length 4 pages, mostly 2-6 pages, tail to ~150), never against
    any individual inventory's own labels at segmentation time. The target is
    a project-level prior fixed in advance, not re-fit per run from the data
    being segmented. Ties go to the first candidate. Ported from NLP
    `src/startpage/unsupervised_segmentation.py` (AS phase 11, step 18b).
    """
    best_weight, best_gap = candidates[0], float("inf")
    for weight in candidates:
        lengths = []
        for sims in similarities_by_inventory.values():
            if len(sims) + 1 <= 1:
                continue
            boundaries = segment_dp(sims, prior_mean_length=prior_mean_length, length_penalty_weight=weight)
            n_pages = len(sims) + 1
            edges = boundaries + [n_pages]
            lengths.extend(b - a for a, b in zip(edges, edges[1:]))
        if not lengths:
            continue
        gap = abs(np.mean(lengths) - target_mean_segment_length)
        if gap < best_gap:
            best_gap, best_weight = gap, weight
    return best_weight


def textiling_depth_scores(
    similarities: np.ndarray, window: int = 3, depth_threshold: Optional[float] = None
) -> tuple[np.ndarray, list[int]]:
    """Returns segment **start** indices (always including 0), not the gap indices of `detect_boundaries_*`; convert with `starts_to_boundaries`.

    Hearst (1997)-style depth score, as a cheaper comparator to `segment_dp`
    (NLP found both below the majority-class baseline for VOC document
    starts; treat them as section-level tools).  Ported from NLP
    `src/startpage/unsupervised_segmentation.py` (AS phase 11, step 18b).

    Smooth adjacent-page similarity with a trailing/leading mean of width
    `window`, then at each internal gap i score
    depth[i] = (left_peak - valley) + (right_peak - valley), where valley is
    the smoothed similarity at i and left_peak/right_peak are the nearest
    local maxima walking outward. A gap is a predicted change point if its
    depth exceeds `depth_threshold` (default: mean + 1 sd of the depth
    array itself -- a per-inventory, label-free threshold, not a global
    constant fit on any ground truth).

    Returns (depth_scores, starts) where starts is the list of page indices
    where a new segment starts (always includes 0), matching segment_dp's
    return convention.
    """
    sims = np.nan_to_num(similarities, nan=np.nanmean(similarities) if np.isfinite(np.nanmean(similarities)) else 0.0)
    n_gaps = len(sims)
    n_pages = n_gaps + 1
    if n_gaps == 0:
        return np.array([]), [0] if n_pages == 1 else []

    kernel = np.ones(window) / window
    smoothed = np.convolve(sims, kernel, mode="same")

    depth = np.zeros(n_gaps)
    for i in range(n_gaps):
        left_peak = smoothed[i]
        for k in range(i, -1, -1):
            if smoothed[k] >= left_peak:
                left_peak = smoothed[k]
            else:
                break
        right_peak = smoothed[i]
        for k in range(i, n_gaps):
            if smoothed[k] >= right_peak:
                right_peak = smoothed[k]
            else:
                break
        depth[i] = (left_peak - smoothed[i]) + (right_peak - smoothed[i])

    if depth_threshold is None:
        depth_threshold = depth.mean() + depth.std() if depth.std() > 0 else depth.max() + 1

    boundaries = [0] + [i + 1 for i in range(n_gaps) if depth[i] > depth_threshold]
    return depth, boundaries


def predicted_start_rate(boundaries: list[int], n_pages: int) -> float:
    """Number of predicted starts divided by `n_pages` (NaN when `n_pages` is 0)."""
    return len(boundaries) / n_pages if n_pages else float("nan")


def starts_to_boundaries(starts: Sequence[int]) -> list[int]:
    """Segment start indices (incl. 0) -> gap indices: [s - 1 for s in sorted(set(starts)) if s > 0]."""
    return [s - 1 for s in sorted(set(starts)) if s > 0]


def boundaries_to_starts(boundaries: Sequence[int]) -> list[int]:
    """Gap indices -> segment start indices: [0] + [b + 1 for b in sorted(set(boundaries))]."""
    return [0] + [b + 1 for b in sorted(set(boundaries))]
