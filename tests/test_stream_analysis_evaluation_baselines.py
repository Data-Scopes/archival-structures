import numpy as np
import pandas as pd
import pytest

from archival_structures.stream_analysis.corruption import (
    StreamUnit, initial_provenance, start_classification_floors)
from archival_structures.stream_analysis.evaluation import baselines as bl
from archival_structures.stream_analysis.evaluation.aggregate import flatten_metrics

T, F = True, False

NLP_KEYS = [
    "f1_pooled", "precision_pooled", "recall_pooled", "f1_macro", "f1_worst", "f1_worst_inventory",
    "f1_macro_ci_lo", "f1_macro_ci_hi", "f1_pooled_ci_lo", "f1_pooled_ci_hi",
    "tol1_f1_pooled", "tol1_f1_macro", "tol1_f1_worst", "tol1_f1_worst_inventory",
    "pk_macro", "pk_worst", "pk_worst_inventory", "window_diff_macro", "window_diff_worst",
    "segment_exact_match_macro", "segment_exact_match_worst",
    "start_rate_diff_mean", "start_rate_diff_sd",
]


def test_simple_predictors():
    assert bl.majority(4) == [F, F, F, F]
    assert bl.every_k(7, 3) == [T, F, F, T, F, F, T]
    assert bl.every_k(5, 2, offset=1) == [F, T, F, T, F]
    with pytest.raises(ValueError):
        bl.every_k(3, 0)


def _nlp_loop(n_pages, rng, mean):
    p = 1.0 / mean
    starts = set()
    pos = 0
    while pos < n_pages:
        starts.add(pos)
        length = max(1, int(rng.geometric(p)))
        pos += length
    return [i in starts for i in range(n_pages)]


@pytest.mark.parametrize("seed", [42, 43, 44])
def test_length_prior_matches_nlp_loop(seed):
    r1, r2 = np.random.default_rng(seed), np.random.default_rng(seed)
    for n in (30, 1, 17):
        assert bl.length_prior(n, 8.64, r1) == _nlp_loop(n, r2, 8.64)


def test_length_prior_rejects_short_mean():
    with pytest.raises(ValueError):
        bl.length_prior(5, 0.5, np.random.default_rng(0))


def test_oracle():
    assert bl.oracle_every_k_own_rate([T, F, F, F, T, F, F, F]) == [T, F, F, F, T, F, F, F]
    assert bl.oracle_every_k_own_rate([F] * 5) == [T, F, F, F, F]
    assert "oracle" in bl.oracle_every_k_own_rate.__doc__.lower()
    runs = bl.baseline_suite({"a": [T, F, F, F]}, 0.25, include_oracle=True)
    oracle = [r for r in runs if r.is_oracle]
    assert len(oracle) == 1 and "ORACLE" in oracle[0].method


def test_train_start_rate_leakage_and_empty():
    df = pd.DataFrame({
        "inventory_number": ["i"] * 4, "page_id": list("abcd"),
        "split": ["train", "train", "dev", "train"],
        "role": ["core", "core", "core", "context"],
    })
    is_start = {("i", "a"): False, ("i", "b"): False, ("i", "c"): True, ("i", "d"): True}
    assert bl.train_start_rate(df, is_start) == 0.0
    assert bl.train_start_rate(df, is_start, split="dev") == 1.0
    with pytest.raises(ValueError):
        bl.train_start_rate(df[df["split"] != "train"], is_start)


def test_baseline_suite_order_and_lengths():
    seqs = {"a": [T, F, F, T, F], "b": [T, F, T]}
    for oracle, n in ((False, 5), (True, 6)):
        runs = bl.baseline_suite(seqs, 0.4, seeds=(1, 2, 3), include_oracle=oracle)
        assert len(runs) == n
        assert [r.method for r in runs][:2] == ["majority", "every_k_train_rate"]
        assert runs[1].param == 2.0
        for r in runs:
            assert {k: len(v) for k, v in r.predictions.items()} == {"a": 5, "b": 3}
        assert [r.seed for r in runs if r.method == "length_prior"] == [1, 2, 3]
    with pytest.raises(ValueError):
        bl.baseline_suite(seqs, 0.0)


def test_baseline_floors_toy():
    seqs = {"a": [T, F, F, T, F, F], "b": [T, F, T, F]}
    df = bl.baseline_floors(seqs, n_bootstrap=20)
    assert len(df) == 1 + 1 + 1 + 3
    assert {"pk_macro", "rate_from_eval_labels", "method", "is_oracle"} <= set(df.columns)
    assert df["rate_from_eval_labels"].all()
    assert df.loc[df["method"] == "majority", "recall_pooled"].iloc[0] == 0
    df2 = bl.baseline_floors(seqs, train_start_rate=0.3, n_bootstrap=20)
    assert not df2["rate_from_eval_labels"].any()


def test_start_classification_floors():
    canonical = [StreamUnit(f"s{i}", f"d{i // 3}") for i in range(9)]
    df = start_classification_floors(canonical, initial_provenance(canonical), canonical,
                                     n_bootstrap=20)
    f1 = df.set_index("method")["f1_pooled"]
    assert f1["every_k_own_rate_ORACLE"] >= f1["majority"]


def test_flatten_metrics_keys():
    seqs = {"a": [T, F, F, T, F, F]}
    ids = {"a": [f"{i}" for i in range(6)]}
    from archival_structures.stream_analysis.evaluation import evaluate_all
    res = evaluate_all(seqs["a"], bl.majority(6), [f"a/{i}" for i in range(6)], ids, n_bootstrap=5)
    assert list(flatten_metrics(res)) == NLP_KEYS
