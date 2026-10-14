"""Evaluation metrics for document-segmentation detectors, scored against the ground truth a
corruption operator leaves behind.

None of these functions run a detector -- they score whatever predictions you already have (a
real detector's output, a baseline, a hand-written heuristic) against ground truth derived from
an observed stream and, where needed, its provenance. They correspond directly to the two
framings in `docs/challenges/document_segmentation.md#whole-page-segmentation`:

- **Framing A** (start-of-document classification): `observed_start_labels` for ground truth,
  `score_start_classification` (precision/recall/F1) and `window_diff` (boundary-tolerant) to
  score a prediction against it.
- **Framing B** (pairwise same-document adjacency): `true_same_document_pairs` for ground truth,
  `score_same_document_pairs` (precision/recall/F1) to score a prediction against it.
- **Framing-independent**: `grouping_ari` scores a predicted per-position document grouping
  (from either framing, or from clustering, via `pairs_to_groups` for Framing-B predictions)
  against the true grouping directly -- see
  `docs/challenges/page_order_problems.md#case-2-foreign-document-insertion-interleaving` for why
  this catches errors the two framings above can each individually miss.
"""

from __future__ import annotations

from dataclasses import dataclass

from sklearn.metrics import adjusted_rand_score

from archival_structures.stream_analysis.corruption.canonical_stream import (
    StreamUnit,
    document_blocks,
)
from archival_structures.stream_analysis.corruption.operators import ProvenanceEntry
from archival_structures.stream_analysis.evaluation import segmentation as _segmentation
from archival_structures.stream_analysis.evaluation.segmentation import (
    default_k,
    page_level_counts,
)


@dataclass(frozen=True)
class PRF1:
    """Precision, recall, and F1 for one binary classification task."""

    precision: float
    recall: float
    f1: float


def _prf1(tp: int, fp: int, fn: int) -> PRF1:
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return PRF1(precision=precision, recall=recall, f1=f1)


def _score_binary(true_labels: list[bool], predicted_labels: list[bool]) -> PRF1:
    tp, fp, fn = page_level_counts(true_labels, predicted_labels)
    return _prf1(tp, fp, fn)


# ---------------------------------------------------------------------------
# Framing A: start-of-document classification
# ---------------------------------------------------------------------------

def true_start_indices(canonical: list[StreamUnit]) -> set[int]:
    """Canonical indices that are the first page of their document -- ground truth for Framing A
    (see `docs/challenges/document_segmentation.md#framing-a-start-of-document-classification`).
    """
    return {start for _doc_id, start, _end in document_blocks(canonical)}


def observed_start_labels(
    observed: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    canonical: list[StreamUnit],
) -> list[bool]:
    """True Framing-A label for each position in `observed`: was this unit the first page of its
    document *in the canonical stream*, regardless of where a corruption operator moved it to?

    This is the exact distinction worked through in
    `docs/challenges/page_order_problems.md#case-1-intra-document-page-swap`: a unit can be the
    true start page of its document even when it isn't first within that document's own run in
    the observed stream (e.g. under `pi_b` in that worked example) -- which is precisely why an
    oracle classifier using this ground truth can still be right about *which* page is the start
    while the naive "cut before every predicted start" reduction gets the grouping wrong anyway.
    """
    starts = true_start_indices(canonical)
    return [p.canonical_index in starts for p in provenance]


def score_start_classification(
    true_labels: list[bool], predicted_labels: list[bool],
) -> PRF1:
    """Precision/recall/F1 of a Framing-A start-page classifier's per-position predictions
    against `true_labels` (e.g. from `observed_start_labels`). Penalises a boundary placed one
    position off exactly as harshly as a completely wrong one -- see `window_diff` for a
    tolerant alternative, and
    `docs/challenges/simulation_harness.md#5-evaluation-metrics` for why that matters more once
    the stream has been corrupted.
    """
    return _score_binary(true_labels, predicted_labels)


def start_classification_floors(observed, provenance, canonical, **kwargs):
    """Framing-A floors for one corrupted stream: baseline_floors({"stream": observed_start_labels(...)}, **kwargs)."""
    from archival_structures.stream_analysis.evaluation.baselines import baseline_floors

    return baseline_floors({"stream": observed_start_labels(observed, provenance, canonical)},
                           **kwargs)


def window_diff(
    true_labels: list[bool], predicted_labels: list[bool], k: int | None = None,
) -> float:
    """WindowDiff (Pevzner & Hearst, 2002): slides a window of `k` gaps across the sequence and
    scores the fraction of windows where the number of boundaries inside disagrees between
    `true_labels` and `predicted_labels`. Unlike `score_start_classification`, a boundary that's
    off by a few positions counts as a small, partial error rather than one full false positive
    plus one full false negative.

    `k` defaults to half the mean true segment length (the choice used in the original paper),
    rounded to at least 1. This is a strict wrapper: it raises `ValueError` if the two label
    lists differ in length, if there are fewer than 2 positions (no gaps to compare), or if `k`
    is outside 1..number of gaps. Labels must be `bool` (`TypeError` otherwise). See
    `archival_structures.stream_analysis.evaluation.segmentation.window_diff` for NaN and
    window-clipping semantics.
    """
    n = len(true_labels)
    if len(predicted_labels) != n:
        raise ValueError(
            f'true_labels ({n}) and predicted_labels ({len(predicted_labels)}) '
            f'must be the same length'
        )
    if n < 2:
        raise ValueError('need at least 2 positions to define any gaps')

    n_gaps = n - 1
    if k is None:
        k = default_k(true_labels, "gaps")
    if not (1 <= k <= n_gaps):
        raise ValueError(f'window size k={k} must be between 1 and the number of gaps ({n_gaps})')
    return _segmentation.window_diff(true_labels, predicted_labels, k=k)


# ---------------------------------------------------------------------------
# Framing B: pairwise same-document adjacency
# ---------------------------------------------------------------------------

def true_same_document_pairs(observed: list[StreamUnit]) -> list[bool]:
    """Ground truth for Framing B: for each adjacent pair of positions in `observed`, True iff
    they belong to the same document. Needs only `observed` -- no canonical stream or provenance
    required, since `document_id` travels with each unit regardless of corruption (see
    `docs/challenges/page_order_problems.md#case-1-intra-document-page-swap` for why this framing
    doesn't need to know which page is the "true start").
    """
    return [
        observed[i].document_id == observed[i + 1].document_id
        for i in range(len(observed) - 1)
    ]


def score_same_document_pairs(
    true_pairs: list[bool], predicted_pairs: list[bool],
) -> PRF1:
    """Precision/recall/F1 of a Framing-B pairwise same-document classifier's predictions
    against `true_pairs` (e.g. from `true_same_document_pairs`)."""
    return _score_binary(true_pairs, predicted_pairs)


# ---------------------------------------------------------------------------
# Framing-independent: document grouping
# ---------------------------------------------------------------------------

def pairs_to_groups(same_document_pairs: list[bool]) -> list[int]:
    """Turn a Framing-B same-document-pair sequence (length n-1) into per-position group ids
    (length n) by starting a new group id every time a pair is False. Lets a Framing-B
    prediction be scored with `grouping_ari` directly, to check whether locally-correct pairwise
    decisions still add up to the right overall grouping -- see the worked example in
    `docs/challenges/page_order_problems.md#case-2-foreign-document-insertion-interleaving`,
    where every individual pairwise label is correct but the grouping it implies isn't.
    """
    groups = [0]
    for same in same_document_pairs:
        groups.append(groups[-1] if same else groups[-1] + 1)
    return groups


def grouping_ari(observed: list[StreamUnit], predicted_group_ids: list) -> float:
    """Adjusted Rand Index between the true document grouping (`observed[i].document_id`) and a
    predicted per-position grouping -- e.g. `pairs_to_groups` applied to a Framing-B prediction,
    or cluster ids from any other detector. Order-invariant: what matters is which positions end
    up grouped together, not where a group sits in the stream or what its id is called. 1.0 is a
    perfect match; close to 0.0 is no better than a random grouping.
    """
    if len(predicted_group_ids) != len(observed):
        raise ValueError(
            f'predicted_group_ids ({len(predicted_group_ids)}) and observed '
            f'({len(observed)}) must be the same length'
        )
    true_ids = [u.document_id for u in observed]
    return adjusted_rand_score(true_ids, predicted_group_ids)
