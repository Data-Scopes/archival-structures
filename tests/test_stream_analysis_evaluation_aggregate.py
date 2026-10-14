import numpy as np
import pandas as pd
import pytest

from archival_structures.stream_analysis.evaluation import aggregate as ag
from archival_structures.stream_analysis.evaluation import segmentation as ev

START = True
NOT = False


def make_frame(labels, positions=None, inventory="inv1"):
    n = len(labels)
    positions = positions or list(range(n))
    return pd.DataFrame({
        "inventory": [inventory] * n,
        "page_id": [f"p{i}" for i in range(n)],
        "position": positions,
        "y_true": labels,
        "y_pred": labels,
    })


def test_per_inventory_macro_and_worst():
    frame = pd.concat([
        make_frame([START, NOT, NOT, START], inventory="good"),
        make_frame([START, NOT, NOT, START], inventory="bad"),
    ], ignore_index=True)
    frame.loc[frame["inventory"] == "bad", "y_pred"] = NOT
    result = ag.per_inventory(ev.page_level_prf, frame, worst_column="f1")
    assert result.table.loc["good", "f1"] == 1.0
    assert result.table.loc["bad", "f1"] == 0.0
    assert result.worst_inventory == "bad"
    assert result.macro["f1"] == pytest.approx(0.5)


def test_bootstrap_ci_resamples_inventories_not_pages():
    result = ag.bootstrap_ci([1.0, 1.0, 1.0, 0.0], n=500, seed=0)
    assert result["mean"] == pytest.approx(0.75)
    assert 0.0 <= result["lo"] <= result["mean"] <= result["hi"] <= 1.0


def test_build_eval_frame_position_and_sort_order():
    sequences = {"inv1": ["p0", "p1", "p2"]}
    frame = ag.build_eval_frame([NOT, NOT, START], [NOT, NOT, START],
                                ["inv1/p2", "inv1/p0", "inv1/p1"], sequences)
    assert frame["position"].tolist() == [0, 1, 2]
    assert frame["page_id"].tolist() == ["p0", "p1", "p2"]
    assert frame["is_last_page"].tolist() == [False, False, True]
    assert list(frame.columns) == ["inventory", "page_id", "position", "is_last_page", "y_true", "y_pred"]


def test_build_eval_frame_unknown_page_keyerror():
    with pytest.raises(KeyError):
        ag.build_eval_frame([NOT], [NOT], ["inv1/zzz"], {"inv1": ["p0"]})


def test_build_eval_frame_custom_sep():
    frame = ag.build_eval_frame([NOT], [NOT], ["inv1|a/b"], {"inv1": ["a/b"]}, sep="|")
    assert frame["page_id"].tolist() == ["a/b"]


def test_build_eval_frame_duplicate_scan_id_context_guard():
    with pytest.raises(ValueError, match="context"):
        ag.build_eval_frame([NOT, NOT], [NOT, NOT], ["inv1/p0", "inv1/p0"], {"inv1": ["p0", "p1"]})


def test_evaluate_all_handles_single_page_and_zero_start_inventories():
    sequences = {"single": ["p0"], "zero_starts": ["p0", "p1"]}
    scan_ids = ["single/p0", "zero_starts/p0", "zero_starts/p1"]
    results = ag.evaluate_all([NOT] * 3, [NOT] * 3, scan_ids, sequences, n_bootstrap=10)
    for key in ("page_level", "tolerance", "pk", "window_diff", "segment_exact_match",
                "start_rate_calibration"):
        assert key in results
    assert isinstance(ag.format_report(results), str)


def test_start_rate_calibration_matches_true_and_predicted_fractions():
    frame = pd.concat([
        make_frame([START, NOT, NOT, NOT], inventory="inv1"),
        make_frame([NOT, NOT], inventory="inv2"),
    ], ignore_index=True)
    frame.loc[frame["inventory"] == "inv2", "y_pred"] = START
    calib = ag.start_rate_calibration(frame)
    assert calib.loc["inv1", "true_start_rate"] == pytest.approx(0.25)
    assert calib.loc["inv1", "diff"] == pytest.approx(0.0)
    assert calib.loc["inv2", "true_start_rate"] == pytest.approx(0.0)
    assert calib.loc["inv2", "predicted_start_rate"] == pytest.approx(1.0)
    assert calib.loc["inv2", "diff"] == pytest.approx(1.0)


def test_evaluate_all_rejects_string_labels():
    s = ["start_page", "not_start_page"]
    with pytest.raises(TypeError):
        ag.evaluate_all(s, s, ["a/p0", "a/p1"], {"a": ["p0", "p1"]}, n_bootstrap=5)


def _toy():
    # inv_a: n=6, S=1 (k differs: gaps 2, pages 3); inv_b: n=4, S=2
    seqs = {"a": [f"p{i}" for i in range(6)], "b": [f"p{i}" for i in range(4)]}
    t_a, p_a = tf6("TFFFFF"), tf6("TFFFTF")
    t_b, p_b = tf6("TFTF"), tf6("TTFF")
    ids = [f"a/p{i}" for i in range(6)] + [f"b/p{i}" for i in range(4)]
    return seqs, ids, t_a + t_b, p_a + p_b, (t_a, p_a), (t_b, p_b)


def tf6(s):
    return [c == "T" for c in s]


def test_evaluate_all_passes_k_convention_through():
    seqs, ids, yt, yp, (ta, pa), (tb, pb) = _toy()
    res = {c: ag.evaluate_all(yt, yp, ids, seqs, n_bootstrap=5, seed=0, k_convention=c)
           for c in ("gaps", "pages")}
    pk_g = res["gaps"]["pk"]["per_inventory"]["value"]
    pk_p = res["pages"]["pk"]["per_inventory"]["value"]
    assert pk_g["a"] != pk_p["a"]
    assert pk_g["b"] == pk_p["b"]
    for c in ("gaps", "pages"):
        got = res[c]["pk"]["per_inventory"]["value"]
        assert got["a"] == ev.pk(ta, pa, k_convention=c)
        assert got["b"] == ev.pk(tb, pb, k_convention=c)
        gotw = res[c]["window_diff"]["per_inventory"]["value"]
        assert gotw["a"] == ev.window_diff(ta, pa, k_convention=c)


def test_evaluate_all_explicit_k():
    seqs, ids, yt, yp, (ta, pa), _ = _toy()
    r = ag.evaluate_all(yt, yp, ids, seqs, n_bootstrap=5, seed=0, k=1)
    assert r["pk"]["per_inventory"]["value"]["a"] == ev.pk(ta, pa, k=1)
