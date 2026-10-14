"""Per-sequence segmentation metrics for start-page detection.

Ported from NLP's ``startpage.evaluation`` (behaviour kept verbatim), with AS conventions:
labels are ``bool`` (``True`` = "is a start page"), the default window size follows the AS
convention (see :func:`default_k`), and edge cases are explicit (length mismatch raises,
short input gives NaN, window clipping warns).
"""

from __future__ import annotations

import warnings

import pandas as pd
from sklearn.metrics import precision_recall_fscore_support

__all__ = [
    "WindowClippedWarning",
    "check_labels",
    "page_level_prf",
    "page_level_counts",
    "page_level_prf_with_counts",
    "iter_contiguous_runs",
    "tolerance_prf",
    "tolerance_prf_runs",
    "default_k",
    "pk",
    "window_diff",
    "segment_exact_match",
]


class WindowClippedWarning(UserWarning):
    """A Pk/WindowDiff window size k exceeded the number of gaps and was clipped."""


def check_labels(labels, name: str = "labels") -> list[bool]:
    """Return labels as list[bool]. Raise TypeError if any element is not bool or numpy.bool_
    (strings, ints, None, floats all raise; the message names `name` and the first bad value)."""
    import numpy as np

    out = list(labels)
    for x in out:
        if not isinstance(x, (bool, np.bool_)):
            raise TypeError(
                f"{name} must contain bool values only (True = start page); "
                f"first bad value: {x!r} of type {type(x).__name__}"
            )
    return [bool(x) for x in out]


def _check_pair(y_true, y_pred, positions=None):
    t = check_labels(y_true, "true_labels")
    p = check_labels(y_pred, "pred_labels")
    if len(t) != len(p):
        raise ValueError(f"true labels ({len(t)}) and predicted labels ({len(p)}) differ in length")
    if positions is not None:
        positions = list(positions)
        if len(positions) != len(t):
            raise ValueError(f"positions ({len(positions)}) and labels ({len(t)}) differ in length")
    return t, p, positions


# --------------------------------------------------------------------------
# Page-level precision/recall/F1
# --------------------------------------------------------------------------

def page_level_prf(y_true, y_pred):
    t, p, _ = _check_pair(y_true, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        t, p, labels=[True], average=None, zero_division=0
    )
    return {"precision": float(precision[0]), "recall": float(recall[0]), "f1": float(f1[0])}


def page_level_counts(y_true, y_pred):
    t, p, _ = _check_pair(y_true, y_pred)
    tp = sum(1 for a, b in zip(t, p) if a and b)
    fp = sum(1 for a, b in zip(t, p) if (not a) and b)
    fn = sum(1 for a, b in zip(t, p) if a and (not b))
    return tp, fp, fn


def page_level_prf_with_counts(y_true, y_pred):
    """Same numbers as page_level_prf, plus tp/fp/fn (for a pooled bootstrap CI)."""
    tp, fp, fn = page_level_counts(y_true, y_pred)
    result = dict(page_level_prf(y_true, y_pred))
    result.update({"tp": tp, "fp": fp, "fn": fn})
    return result


# --------------------------------------------------------------------------
# Contiguous runs
# --------------------------------------------------------------------------

def iter_contiguous_runs(true_labels, pred_labels, positions, is_last_page=None):
    """
    Split (true_labels, pred_labels), position-sorted and parallel to `positions`, into maximal
    runs of consecutive `position` values. Yields (run_true, run_pred, is_right_truncated);
    is_right_truncated is False only when the run's last row is the inventory's actual last
    physical page (per `is_last_page`, itself parallel to `positions`) -- unknown
    (`is_last_page=None`) is treated as truncated, the conservative default.
    """
    true_labels = list(true_labels)
    pred_labels = list(pred_labels)
    positions = list(positions)
    if is_last_page is None:
        is_last_page = [False] * len(positions)
    else:
        is_last_page = list(is_last_page)
    n = len(positions)
    start = 0
    for i in range(1, n + 1):
        if i == n or positions[i] != positions[i - 1] + 1:
            yield (true_labels[start:i], pred_labels[start:i], not is_last_page[i - 1])
            start = i


# --------------------------------------------------------------------------
# Tolerance-window precision/recall/F1 (greedy one-to-one matching, per run)
# --------------------------------------------------------------------------

def _tolerance_match_counts(true_labels, pred_labels, tolerance):
    true_positions = [i for i, l in enumerate(true_labels) if l]
    pred_positions = [i for i, l in enumerate(pred_labels) if l]
    used_true = set()
    tp = 0
    for p in pred_positions:
        candidates = [t for t in true_positions
                      if t not in used_true and abs(t - p) <= tolerance]
        if candidates:
            best = min(candidates, key=lambda t: (abs(t - p), t))
            used_true.add(best)
            tp += 1
    fp = len(pred_positions) - tp
    fn = len(true_positions) - len(used_true)
    return tp, fp, fn


def _prf_from_counts(tp, fp, fn):
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def _tolerance_counts_runs(true_labels, pred_labels, positions, is_last_page, tolerance):
    tp = fp = fn = 0
    for run_true, run_pred, _ in iter_contiguous_runs(true_labels, pred_labels, positions, is_last_page):
        t, f, n = _tolerance_match_counts(run_true, run_pred, tolerance)
        tp += t
        fp += f
        fn += n
    return tp, fp, fn


def tolerance_prf_runs(true_labels, pred_labels, positions=None, is_last_page=None, tolerance=1):
    t, p, positions = _check_pair(true_labels, pred_labels, positions)
    tp, fp, fn = _tolerance_counts_runs(t, p, positions, is_last_page, tolerance)
    result = _prf_from_counts(tp, fp, fn)
    result.update({"tp": tp, "fp": fp, "fn": fn})
    return result


_tolerance_prf_runs = tolerance_prf_runs


def tolerance_prf(y_true, y_pred, sequence, tolerance=1, positions=None):
    """
    `sequence` is the eval frame's `inventory` column (parallel to y_true/y_pred) -- grouping is
    required so a tolerance window can never match a start in one inventory to a prediction in
    another. Pass `positions` (parallel, physical position) to also split each inventory into
    contiguous runs, so a window can't cross a run boundary either.
    """
    t, p, positions = _check_pair(y_true, y_pred, positions)
    sequence = list(sequence)
    if len(sequence) != len(t):
        raise ValueError(f"sequence ({len(sequence)}) and labels ({len(t)}) differ in length")
    df = pd.DataFrame({"inventory": sequence, "y_true": t, "y_pred": p})
    if positions is not None:
        df["position"] = positions
    tp = fp = fn = 0
    for _, group in df.groupby("inventory"):
        if positions is not None:
            group = group.sort_values("position")
            a, b, c = _tolerance_counts_runs(
                group["y_true"].tolist(), group["y_pred"].tolist(), group["position"].tolist(), None, tolerance
            )
        else:
            a, b, c = _tolerance_match_counts(group["y_true"].tolist(), group["y_pred"].tolist(), tolerance)
        tp += a
        fp += b
        fn += c
    return _prf_from_counts(tp, fp, fn)


# --------------------------------------------------------------------------
# Pk and WindowDiff
# --------------------------------------------------------------------------

def _boundary_gaps(labels):
    return [1 if labels[i + 1] else 0 for i in range(len(labels) - 1)]


def default_k(true_labels, convention: str = "gaps") -> int:
    """Default window size: half the mean true segment length.

    S = number of true segments (boundaries + 1). "gaps" (AS default) uses the number of gaps,
    ``max(1, round((n - 1) / (2 * S)))``; "pages" (NLP) uses the number of pages,
    ``max(1, round((n / S) / 2))``. Python's banker's ``round`` in both. Computed once over the
    labels passed in; callers that run-split afterwards must not recompute k per run.
    """
    if convention not in ("gaps", "pages"):
        raise ValueError(f"unknown k convention {convention!r}; use 'gaps' or 'pages'")
    labels = check_labels(true_labels, "true_labels")
    n_segments = sum(_boundary_gaps(labels)) + 1
    if convention == "gaps":
        return max(1, round((len(labels) - 1) / (2 * n_segments)))
    mean_segment_length = len(labels) / n_segments
    return max(1, round(mean_segment_length / 2))


def _window_errors(ref_gaps, hyp_gaps, k, compare):
    """Returns (errors, windows, clipped)."""
    n = len(ref_gaps)
    rk = min(k, n)
    windows = n - rk + 1
    if windows <= 0:
        return 0, 0, False
    errors = sum(compare(ref_gaps[i:i + rk], hyp_gaps[i:i + rk]) for i in range(windows))
    return errors, windows, k > n


def _pk_compare(ref_window, hyp_window):
    return (sum(ref_window) > 0) != (sum(hyp_window) > 0)


def _wd_compare(ref_window, hyp_window):
    return sum(ref_window) != sum(hyp_window)


def _windowed_metric(true_labels, pred_labels, k, k_convention, positions, is_last_page, compare):
    if k is None:
        k = default_k(true_labels, k_convention)
    clipped_any = False
    if positions is None:
        ref_gaps = _boundary_gaps(true_labels)
        hyp_gaps = _boundary_gaps(pred_labels)
        errors, windows, clipped_any = _window_errors(ref_gaps, hyp_gaps, k, compare)
        value = errors / windows if windows > 0 else float("nan")
    else:
        total_errors = total_windows = 0
        for run_true, run_pred, _ in iter_contiguous_runs(true_labels, pred_labels, positions, is_last_page):
            if len(run_true) < 2:
                continue
            ref_gaps = _boundary_gaps(run_true)
            hyp_gaps = _boundary_gaps(run_pred)
            errors, windows, clipped = _window_errors(ref_gaps, hyp_gaps, k, compare)
            clipped_any = clipped_any or clipped
            total_errors += errors
            total_windows += windows
        value = total_errors / total_windows if total_windows > 0 else float("nan")
    if clipped_any:
        warnings.warn(
            f"window size k={k} exceeds the number of gaps of at least one sequence/run and was "
            f"clipped", WindowClippedWarning, stacklevel=4,
        )
    return value


def _windowed_entry(true_labels, pred_labels, k, k_convention, positions, is_last_page, compare):
    t, p, positions = _check_pair(true_labels, pred_labels, positions)
    if len(t) < 2:
        return float("nan")
    return _windowed_metric(t, p, k, k_convention, positions, is_last_page, compare)


def pk(true_labels, pred_labels, k=None, positions=None, is_last_page=None, *, k_convention="gaps"):
    return _windowed_entry(true_labels, pred_labels, k, k_convention, positions, is_last_page, _pk_compare)


def window_diff(true_labels, pred_labels, k=None, positions=None, is_last_page=None, *,
                k_convention="gaps"):
    return _windowed_entry(true_labels, pred_labels, k, k_convention, positions, is_last_page, _wd_compare)


# --------------------------------------------------------------------------
# Segment exact match
# --------------------------------------------------------------------------

def _segment_matches_in_run(true_labels, pred_labels, exclude_trailing):
    n = len(true_labels)
    true_starts = [i for i, l in enumerate(true_labels) if l]
    if not true_starts:
        return 0, 0
    pred_starts = sorted(i for i, l in enumerate(pred_labels) if l)

    scoring_count = len(true_starts) - 1 if exclude_trailing else len(true_starts)
    matches = 0
    for idx, start in enumerate(true_starts):
        if exclude_trailing and idx == len(true_starts) - 1:
            continue
        true_end = true_starts[idx + 1] - 1 if idx + 1 < len(true_starts) else n - 1
        if start not in pred_starts:
            continue
        next_pred_start = next((p for p in pred_starts if p > start), None)
        pred_end = next_pred_start - 1 if next_pred_start is not None else n - 1
        if pred_end == true_end:
            matches += 1
    return matches, scoring_count


def segment_exact_match(true_labels, pred_labels, positions=None, is_last_page=None):
    """Fraction of true documents recovered with both boundaries exactly right. Leading pages
    before the first true start are excluded, and a right-truncated run's trailing segment too."""
    t, p, positions = _check_pair(true_labels, pred_labels, positions)
    if positions is None:
        matches, total = _segment_matches_in_run(t, p, exclude_trailing=False)
        return matches / total if total else float("nan")

    total_matches = total_segments = 0
    for run_true, run_pred, is_right_truncated in iter_contiguous_runs(t, p, positions, is_last_page):
        m, n_seg = _segment_matches_in_run(run_true, run_pred, exclude_trailing=is_right_truncated)
        total_matches += m
        total_segments += n_seg
    return total_matches / total_segments if total_segments else float("nan")
