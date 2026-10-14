"""Trivial start-page baselines (C8): the floors every segmentation number is read against.

None of them reads text. ``oracle_every_k_own_rate`` is an upper bound on "rate-only
knowledge", not a legitimate predictor (D9). Ported from NLP's ``run_trivial_baselines.py``
with bool predictions.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

from archival_structures.stream_analysis.evaluation.aggregate import evaluate_all, flatten_metrics
from archival_structures.stream_analysis.evaluation.segmentation import check_labels
from archival_structures.stream_analysis.evaluation.splits import core_rows

__all__ = [
    "majority",
    "every_k",
    "length_prior",
    "oracle_every_k_own_rate",
    "train_start_rate",
    "BaselineRun",
    "baseline_suite",
    "baseline_floors",
]


def majority(n: int) -> list[bool]:
    """Predict no start anywhere."""
    return [False] * n


def every_k(n: int, k: int, offset: int = 0) -> list[bool]:
    """Start at every k-th position: ``(i - offset) % k == 0``. ``k < 1`` raises ValueError."""
    if k < 1:
        raise ValueError(f"k must be >= 1, got {k}")
    return [(i - offset) % k == 0 for i in range(n)]


def length_prior(n: int, mean_length: float, rng: np.random.Generator) -> list[bool]:
    """Segment lengths drawn i.i.d. from Geometric(1 / mean_length) (min 1); a start at each
    segment's first position. The caller shares `rng` across sequences, visited in order, to
    reproduce NLP seed for seed. ``mean_length < 1`` raises ValueError."""
    if mean_length < 1:
        raise ValueError(f"mean_length must be >= 1, got {mean_length}")
    p = 1.0 / mean_length
    starts = set()
    pos = 0
    while pos < n:
        starts.add(pos)
        length = max(1, int(rng.geometric(p)))
        pos += length
    return [i in starts for i in range(n)]


def oracle_every_k_own_rate(true_labels: Sequence[bool]) -> list[bool]:
    """ORACLE: uses this sequence's own true labels, so it is not a legitimate predictor (D9).

    Period = round(1 / own start rate) (the sequence length if there are no starts), then
    `every_k`. An upper bound on what rate-only knowledge buys, never a baseline to beat."""
    labels = check_labels(true_labels, "true_labels")
    n = len(labels)
    rate = sum(labels) / n if n else 0.0
    period = round(1 / rate) if rate > 0 else n
    return every_k(n, max(1, period))


def train_start_rate(split_df: pd.DataFrame, is_start: Mapping[tuple[str, str], bool],
                     split: str = "train") -> float:
    """Start rate over the core rows (``role != "context"``) of partition `split`, looked up in
    `is_start[(inventory_number, page_id)]`.

    Leakage rule: fit on the train partition only, never on the partition being evaluated.
    No rows raises ValueError."""
    rows = core_rows(split_df)
    rows = rows[rows["split"] == split]
    if len(rows) == 0:
        raise ValueError(f"no core rows with split == {split!r}")
    flags = [bool(is_start[(r.inventory_number, r.page_id)]) for r in rows.itertuples()]
    return sum(flags) / len(flags)


@dataclass(frozen=True)
class BaselineRun:
    method: str            # "majority" | "every_k_train_rate" | "every_k_own_rate_ORACLE" | "length_prior"
    seed: int | None
    param: float           # k for every_k, mean_length for length_prior, nan otherwise
    is_oracle: bool
    predictions: dict[str, list[bool]]   # sequence id -> predictions


def baseline_suite(sequences: Mapping[str, Sequence[bool]], train_start_rate: float,
                   seeds: Sequence[int] = (42, 43, 44), include_oracle: bool = False
                   ) -> list[BaselineRun]:
    """Majority, every-k at the train rate, [oracle], and one length prior per seed.

    `sequences` maps sequence id -> true labels (validated; only the oracle reads them, and
    then it is flagged). The length prior's rng is shared over `sequences` in mapping order."""
    if not (0 < train_start_rate <= 1):
        raise ValueError(f"train_start_rate must be in (0, 1], got {train_start_rate}")
    seqs = {sid: check_labels(lab, f"sequences[{sid!r}]") for sid, lab in sequences.items()}
    k = max(1, round(1 / train_start_rate))
    mean_length = 1 / train_start_rate
    runs = [
        BaselineRun("majority", None, math.nan, False,
                    {sid: majority(len(l)) for sid, l in seqs.items()}),
        BaselineRun("every_k_train_rate", None, float(k), False,
                    {sid: every_k(len(l), k) for sid, l in seqs.items()}),
    ]
    if include_oracle:
        runs.append(BaselineRun("every_k_own_rate_ORACLE", None, math.nan, True,
                                {sid: oracle_every_k_own_rate(l) for sid, l in seqs.items()}))
    for seed in seeds:
        rng = np.random.default_rng(seed)
        runs.append(BaselineRun("length_prior", seed, mean_length, False,
                                {sid: length_prior(len(l), mean_length, rng)
                                 for sid, l in seqs.items()}))
    return runs


def baseline_floors(sequences: Mapping[str, Sequence[bool]], *,
                    train_start_rate: float | None = None, seeds=(42, 43, 44),
                    include_oracle=True, tolerance=1, k=None, k_convention="gaps",
                    n_bootstrap=200, bootstrap_seed=0) -> pd.DataFrame:
    """One row per baseline run, scored with `evaluate_all` on all positions.

    If `train_start_rate` is None it is computed from `sequences` themselves and
    `rate_from_eval_labels` is True: the every-k and length-prior rows are then rate-oracles
    too. Columns: method, seed, param, is_oracle, rate_from_eval_labels, then the
    `flatten_metrics` keys."""
    seqs = {sid: check_labels(lab, f"sequences[{sid!r}]") for sid, lab in sequences.items()}
    from_eval = train_start_rate is None
    if from_eval:
        total = sum(len(l) for l in seqs.values())
        train_start_rate = sum(sum(l) for l in seqs.values()) / total if total else 0.0
    page_ids = {sid: [f"{i:06d}" for i in range(len(l))] for sid, l in seqs.items()}
    scan_ids = [f"{sid}/{pid}" for sid, ids in page_ids.items() for pid in ids]
    y_true = [x for l in seqs.values() for x in l]
    rows = []
    for run in baseline_suite(seqs, train_start_rate, seeds=seeds, include_oracle=include_oracle):
        y_pred = [x for sid in seqs for x in run.predictions[sid]]
        res = evaluate_all(y_true, y_pred, scan_ids, page_ids, tolerance=tolerance,
                           n_bootstrap=n_bootstrap, seed=bootstrap_seed, k=k,
                           k_convention=k_convention)
        rows.append({"method": run.method, "seed": run.seed, "param": run.param,
                     "is_oracle": run.is_oracle, "rate_from_eval_labels": from_eval,
                     **flatten_metrics(res)})
    return pd.DataFrame(rows)
