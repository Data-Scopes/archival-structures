"""Aggregation of segmentation metrics over inventories: eval frame, per-inventory tables,
bootstrap CIs and ``evaluate_all``. Ported from NLP's ``startpage.evaluation`` (behaviour
verbatim; bool labels, generic page ids)."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

from archival_structures.stream_analysis.evaluation.segmentation import (
    _prf_from_counts,
    check_labels,
    page_level_prf,
    page_level_prf_with_counts,
    pk,
    segment_exact_match,
    tolerance_prf,
    tolerance_prf_runs,
    window_diff,
)

__all__ = [
    "build_eval_frame",
    "PerInventoryResult",
    "per_inventory",
    "start_rate_calibration",
    "bootstrap_ci",
    "bootstrap_pooled_f1",
    "evaluate_all",
    "flatten_metrics",
    "format_report",
]


def build_eval_frame(y_true, y_pred, scan_ids, sequences: Mapping[str, Sequence[str]],
                     sep: str = "/") -> pd.DataFrame:
    """
    One row per scan id (``f"{inventory}{sep}{page_id}"``), with its physical position within
    its inventory (from `sequences`: inventory -> page ids in physical order), sorted by
    (inventory, position). Columns: inventory, page_id, position, is_last_page, y_true, y_pred.
    Sorting alone does not make the frame physically contiguous; the sequence metrics split
    each inventory into contiguous runs of `position`.
    """
    y_true = check_labels(y_true, "y_true")
    y_pred = check_labels(y_pred, "y_pred")
    scan_ids = list(scan_ids)
    if not (len(y_true) == len(y_pred) == len(scan_ids)):
        raise ValueError(
            f"y_true ({len(y_true)}), y_pred ({len(y_pred)}) and scan_ids ({len(scan_ids)}) "
            f"differ in length")
    if len(set(scan_ids)) != len(scan_ids):
        seen, dup = set(), None
        for s in scan_ids:
            if s in seen:
                dup = s
                break
            seen.add(s)
        raise ValueError(
            f"scan id {dup!r} occurs more than once: a page would be scored twice. Were "
            f"role == 'context' rows dropped before scoring (overlapping chunks put the same "
            f"page in several rows)?")
    position_lookup = {
        inventory: {page_id: idx for idx, page_id in enumerate(sequence)}
        for inventory, sequence in sequences.items()
    }
    last_position = {inventory: len(sequence) - 1 for inventory, sequence in sequences.items()}
    rows = []
    for scan_id, yt, yp in zip(scan_ids, y_true, y_pred):
        inventory, page_id = scan_id.split(sep, 1)
        position = position_lookup[inventory][page_id]
        rows.append({
            "inventory": inventory,
            "page_id": page_id,
            "position": position,
            "is_last_page": position == last_position[inventory],
            "y_true": yt,
            "y_pred": yp,
        })
    return (pd.DataFrame(rows,
                         columns=["inventory", "page_id", "position", "is_last_page", "y_true", "y_pred"])
            .sort_values(["inventory", "position"])
            .reset_index(drop=True))


PerInventoryResult = namedtuple("PerInventoryResult", ["table", "macro", "worst", "worst_inventory"])


def per_inventory(metric_fn, eval_frame, worst_column=None, higher_is_better=True,
                  pass_position_info=False, **kwargs):
    """Run a per-sequence metric_fn(true_labels, pred_labels, **kwargs) once per inventory and
    summarise with macro (mean) and worst-inventory views. With `pass_position_info=True`, also
    pass `positions`/`is_last_page` so run-aware metrics aggregate over contiguous runs."""
    rows = []
    for inventory_number, group in eval_frame.groupby("inventory"):
        group = group.sort_values("position")
        if pass_position_info:
            value = metric_fn(group["y_true"].tolist(), group["y_pred"].tolist(),
                              positions=group["position"].tolist(),
                              is_last_page=group["is_last_page"].tolist(), **kwargs)
        else:
            value = metric_fn(group["y_true"].tolist(), group["y_pred"].tolist(), **kwargs)
        row = {"inventory": inventory_number}
        if isinstance(value, dict):
            row.update(value)
        else:
            row["value"] = value
        rows.append(row)
    table = pd.DataFrame(rows).set_index("inventory")

    score_column = worst_column or ("value" if "value" in table.columns else None)
    macro = table.mean(numeric_only=True).to_dict()
    if score_column is None:
        worst, worst_inventory = None, None
    else:
        scores = table[score_column].dropna()
        if scores.empty:
            worst, worst_inventory = float("nan"), None
        else:
            worst_inventory = scores.idxmax() if not higher_is_better else scores.idxmin()
            worst = scores.loc[worst_inventory]
    return PerInventoryResult(table=table, macro=macro, worst=worst, worst_inventory=worst_inventory)


def start_rate_calibration(eval_frame):
    """Predicted vs. true fraction of start pages, per inventory."""
    check_labels(eval_frame["y_true"], "y_true")
    check_labels(eval_frame["y_pred"], "y_pred")

    def rate(labels):
        labels = list(labels)
        return sum(1 for l in labels if l) / len(labels) if labels else float("nan")

    return (eval_frame.groupby("inventory")
            .agg(true_start_rate=("y_true", rate), predicted_start_rate=("y_pred", rate))
            .assign(diff=lambda d: d["predicted_start_rate"] - d["true_start_rate"]))


def bootstrap_ci(per_inventory_values, n=1000, ci=0.95, unit="inventory", seed=None):
    if unit != "inventory":
        raise ValueError("bootstrap_ci resamples inventories only -- CLAUDE.md's evaluation protocol")
    values = np.asarray(list(per_inventory_values), dtype=float)
    values = values[~np.isnan(values)]
    rng = np.random.RandomState(seed)
    means = np.array([
        rng.choice(values, size=len(values), replace=True).mean()
        for _ in range(n)
    ])
    alpha = (1 - ci) / 2
    lo, hi = np.quantile(means, [alpha, 1 - alpha])
    return {"mean": float(values.mean()), "lo": float(lo), "hi": float(hi), "n": n}


def bootstrap_pooled_f1(per_inventory_counts, n=1000, ci=0.95, seed=None):
    """`per_inventory_counts` is a per_inventory(...).table-shaped frame carrying tp/fp/fn
    columns. Resamples inventories with replacement, sums their tp/fp/fn, and recomputes
    pooled F1 per draw."""
    counts = per_inventory_counts[["tp", "fp", "fn"]].to_numpy(dtype=float)
    n_inventories = len(counts)
    rng = np.random.RandomState(seed)
    draws = np.array([
        _prf_from_counts(*counts[rng.choice(n_inventories, size=n_inventories, replace=True)].sum(axis=0))["f1"]
        for _ in range(n)
    ])
    alpha = (1 - ci) / 2
    lo, hi = np.quantile(draws, [alpha, 1 - alpha])
    overall_tp, overall_fp, overall_fn = counts.sum(axis=0)
    mean_f1 = _prf_from_counts(overall_tp, overall_fp, overall_fn)["f1"]
    return {"mean": float(mean_f1), "lo": float(lo), "hi": float(hi), "n": n}


def evaluate_all(y_true, y_pred, scan_ids, sequences, tolerance=1, n_bootstrap=1000, seed=None,
                 *, k=None, k_convention="gaps", sep="/") -> dict:
    eval_frame = build_eval_frame(y_true, y_pred, scan_ids, sequences, sep=sep)

    page_f1 = per_inventory(page_level_prf_with_counts, eval_frame, worst_column="f1")
    tolerance_overall = tolerance_prf(eval_frame["y_true"], eval_frame["y_pred"], eval_frame["inventory"],
                                      tolerance, positions=eval_frame["position"])
    tolerance_by_inventory = per_inventory(
        tolerance_prf_runs, eval_frame, worst_column="f1", pass_position_info=True, tolerance=tolerance
    )

    pk_result = per_inventory(pk, eval_frame, higher_is_better=False, pass_position_info=True,
                              k=k, k_convention=k_convention)
    wd_result = per_inventory(window_diff, eval_frame, higher_is_better=False, pass_position_info=True,
                              k=k, k_convention=k_convention)
    sem_result = per_inventory(segment_exact_match, eval_frame, pass_position_info=True)

    calibration = start_rate_calibration(eval_frame)

    return {
        "page_level": {
            "pooled": page_level_prf(eval_frame["y_true"], eval_frame["y_pred"]),
            "per_inventory": page_f1.table,
            "macro": page_f1.macro,
            "worst": page_f1.worst,
            "worst_inventory": page_f1.worst_inventory,
            "bootstrap_f1": bootstrap_ci(page_f1.table["f1"], n=n_bootstrap, seed=seed),
            "bootstrap_pooled_f1": bootstrap_pooled_f1(page_f1.table, n=n_bootstrap, seed=seed),
        },
        "tolerance": {
            "tolerance": tolerance,
            "pooled": tolerance_overall,
            "per_inventory": tolerance_by_inventory.table,
            "macro": tolerance_by_inventory.macro,
            "worst": tolerance_by_inventory.worst,
            "worst_inventory": tolerance_by_inventory.worst_inventory,
            "bootstrap_f1": bootstrap_ci(tolerance_by_inventory.table["f1"], n=n_bootstrap, seed=seed),
            "bootstrap_pooled_f1": bootstrap_pooled_f1(tolerance_by_inventory.table, n=n_bootstrap, seed=seed),
        },
        "pk": {"per_inventory": pk_result.table, "macro": pk_result.macro["value"],
               "worst": pk_result.worst, "worst_inventory": pk_result.worst_inventory},
        "window_diff": {"per_inventory": wd_result.table, "macro": wd_result.macro["value"],
                        "worst": wd_result.worst, "worst_inventory": wd_result.worst_inventory},
        "segment_exact_match": {"per_inventory": sem_result.table, "macro": sem_result.macro["value"],
                                "worst": sem_result.worst, "worst_inventory": sem_result.worst_inventory},
        "start_rate_calibration": calibration,
    }


def flatten_metrics(results) -> dict:
    """Flatten an `evaluate_all` result into NLP's 23 scalar metric columns (verbatim keys and
    expressions of NLP's `run_trivial_baselines.flatten_metrics`)."""
    pl, tol = results["page_level"], results["tolerance"]
    pk_r, wd_r, sem_r = results["pk"], results["window_diff"], results["segment_exact_match"]
    calib = results["start_rate_calibration"]
    return {
        "f1_pooled": pl["pooled"]["f1"], "precision_pooled": pl["pooled"]["precision"],
        "recall_pooled": pl["pooled"]["recall"],
        "f1_macro": pl["macro"]["f1"], "f1_worst": pl["worst"], "f1_worst_inventory": pl["worst_inventory"],
        "f1_macro_ci_lo": pl["bootstrap_f1"]["lo"], "f1_macro_ci_hi": pl["bootstrap_f1"]["hi"],
        "f1_pooled_ci_lo": pl["bootstrap_pooled_f1"]["lo"], "f1_pooled_ci_hi": pl["bootstrap_pooled_f1"]["hi"],
        "tol1_f1_pooled": tol["pooled"]["f1"], "tol1_f1_macro": tol["macro"]["f1"],
        "tol1_f1_worst": tol["worst"], "tol1_f1_worst_inventory": tol["worst_inventory"],
        "pk_macro": pk_r["macro"], "pk_worst": pk_r["worst"], "pk_worst_inventory": pk_r["worst_inventory"],
        "window_diff_macro": wd_r["macro"], "window_diff_worst": wd_r["worst"],
        "segment_exact_match_macro": sem_r["macro"], "segment_exact_match_worst": sem_r["worst"],
        "start_rate_diff_mean": float(calib["diff"].mean()), "start_rate_diff_sd": float(calib["diff"].std()),
    }


def format_report(results):
    lines = []
    pl = results["page_level"]
    lines.append(f"Page F1        : pooled {pl['pooled']['f1']:.3f} "
                 f"(95% CI [{pl['bootstrap_pooled_f1']['lo']:.3f}, {pl['bootstrap_pooled_f1']['hi']:.3f}])  "
                 f"macro {pl['macro']['f1']:.3f} "
                 f"(95% CI [{pl['bootstrap_f1']['lo']:.3f}, {pl['bootstrap_f1']['hi']:.3f}])  "
                 f"worst {pl['worst']:.3f} ({pl['worst_inventory']})")
    tol = results["tolerance"]
    lines.append(f"+/-{tol['tolerance']} tolerance F1: pooled {tol['pooled']['f1']:.3f} "
                 f"(95% CI [{tol['bootstrap_pooled_f1']['lo']:.3f}, {tol['bootstrap_pooled_f1']['hi']:.3f}])  "
                 f"macro {tol['macro']['f1']:.3f} "
                 f"(95% CI [{tol['bootstrap_f1']['lo']:.3f}, {tol['bootstrap_f1']['hi']:.3f}])  "
                 f"worst {tol['worst']:.3f} ({tol['worst_inventory']})")
    pk_r = results["pk"]
    lines.append(f"Pk             : macro {pk_r['macro']:.3f}  worst {pk_r['worst']:.3f} ({pk_r['worst_inventory']})")
    wd_r = results["window_diff"]
    lines.append(f"WindowDiff     : macro {wd_r['macro']:.3f}  worst {wd_r['worst']:.3f} ({wd_r['worst_inventory']})")
    sem_r = results["segment_exact_match"]
    lines.append(f"Segment exact match: macro {sem_r['macro']:.3f}  "
                 f"worst {sem_r['worst']:.3f} ({sem_r['worst_inventory']})")
    calib = results["start_rate_calibration"]
    lines.append(f"Start-rate calibration (mean predicted - true, per inventory): "
                 f"{calib['diff'].mean():+.4f} (sd {calib['diff'].std():.4f})")
    return "\n".join(lines)
