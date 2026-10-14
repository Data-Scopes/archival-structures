import warnings

import numpy as np
import pytest

from archival_structures.stream_analysis.corruption import metrics as cm
from archival_structures.stream_analysis.evaluation import segmentation as ev
from archival_structures.stream_analysis.evaluation.segmentation import WindowClippedWarning

START = True
NOT = False


def tf(s):
    return [c == "T" for c in s]


# --- ported NLP tests (labels as bool) -------------------------------------

def test_perfect_prediction():
    true = [START, NOT, NOT, START, NOT, NOT, START]
    pred = list(true)
    assert ev.page_level_prf(true, pred) == {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    tol = ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1)
    assert tol == {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    assert ev.window_diff(true, pred) == 0.0
    assert ev.pk(true, pred) == 0.0
    assert ev.segment_exact_match(true, pred) == 1.0


def test_all_negative_prediction():
    true = [START, NOT, NOT, START, NOT, NOT, START]
    pred = [NOT] * len(true)
    prf = ev.page_level_prf(true, pred)
    assert prf["precision"] == 0.0
    assert prf["recall"] == 0.0
    assert prf["f1"] == 0.0
    tol = ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1)
    assert tol == {"precision": 0.0, "recall": 0.0, "f1": 0.0}
    assert ev.segment_exact_match(true, pred) == 0.0
    assert ev.window_diff(true, pred) > 0.0


def test_all_positive_prediction():
    true = [START, NOT, NOT, START, NOT, NOT, START]
    pred = [START] * len(true)
    prf = ev.page_level_prf(true, pred)
    assert prf["recall"] == 1.0
    assert prf["precision"] == pytest.approx(3 / 7)
    assert prf["f1"] == pytest.approx(2 * prf["precision"] * 1.0 / (prf["precision"] + 1.0))


def test_everything_off_by_one():
    true = [NOT, START, NOT, NOT, START, NOT, NOT, START, NOT]
    pred = [START, NOT, NOT, START, NOT, NOT, START, NOT, NOT]
    assert ev.page_level_prf(true, pred)["f1"] == 0.0
    assert ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1)["f1"] == 1.0
    assert ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=0)["f1"] == 0.0
    wd = ev.window_diff(true, pred, k=2)
    assert 0.0 < wd < 1.0


def test_tolerance_greedy_one_to_one_no_double_credit():
    true = [NOT, START, NOT, NOT, NOT]
    pred = [START, NOT, START, NOT, NOT]
    tol = ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1)
    assert tol["precision"] == pytest.approx(0.5)
    assert tol["recall"] == pytest.approx(1.0)


def test_tolerance_never_crosses_inventory_boundary():
    true = [NOT, START, NOT, NOT]
    pred = [NOT, NOT, START, NOT]
    tol = ev.tolerance_prf(true, pred, ["inv1", "inv1", "inv2", "inv2"], tolerance=1)
    assert tol["recall"] == 0.0
    assert tol["precision"] == 0.0


def test_segment_exact_match_partial():
    true = [NOT, START, NOT, NOT, START, NOT, NOT]
    pred = [NOT, START, NOT, NOT, START, NOT, START]
    assert ev.segment_exact_match(true, pred) == pytest.approx(0.5)


def test_tolerance_does_not_cross_a_run_boundary():
    true = [NOT, NOT, NOT, START, NOT, NOT]
    pred = [NOT, NOT, START, NOT, NOT, NOT]
    positions = [0, 1, 2, 50, 51, 52]
    tol = ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1, positions=positions)
    assert tol["precision"] == 0.0
    assert tol["recall"] == 0.0
    tol_no_positions = ev.tolerance_prf(true, pred, ["inv"] * len(true), tolerance=1)
    assert tol_no_positions["recall"] == 1.0


def test_pk_and_window_diff_ignore_the_junction():
    true = [NOT, NOT, NOT, START, NOT, NOT]
    pred = [NOT, NOT, START, NOT, NOT, NOT]
    positions = [0, 1, 2, 50, 51, 52]
    assert ev.pk(true, pred, k=1, positions=positions) != ev.pk(true, pred, k=1)
    assert ev.window_diff(true, pred, k=1, positions=positions) != ev.window_diff(true, pred, k=1)


def test_segment_exact_match_excludes_truncated_trailing_segment():
    true = [NOT, START, NOT, NOT]
    pred = [NOT, START, NOT, NOT]
    positions = [0, 1, 2, 3]
    full = ev.segment_exact_match(true, pred, positions=positions, is_last_page=[False, False, False, True])
    assert full == 1.0
    truncated = ev.segment_exact_match(true, pred, positions=positions, is_last_page=[False] * 4)
    assert np.isnan(truncated)


def test_default_k_comes_from_pooled_inventory_not_a_run():
    true_run1 = [NOT, START] + [NOT] * 8
    true_run2 = [NOT, NOT, NOT]
    assert ev.default_k(true_run1 + true_run2) != ev.default_k(true_run2)


# --- shared WindowDiff fixture ---------------------------------------------

WD_CASES = [
    ("one_off", "TFTF", "TTFF", 1, 2 / 3),
    ("one_off_k2", "TFTF", "TTFF", 2, 0.5),
    ("identical", "TFFTFF", "TFFTFF", 2, 0.0),
    ("all_missed", "TFFTFFTF", "TFFFFFFF", 2, 2 / 3),
    ("extra", "TFFFFFFF", "TFFFTFFF", 3, 0.6),
    ("mixed", "TFTFFFTFFT", "TTFFFTFFFT", 2, 0.375),
    ("mixed_k3", "TFTFFFTFFT", "TTFFFTFFFT", 3, 3 / 7),
]


@pytest.mark.parametrize("name,true,pred,k,expected", WD_CASES, ids=[c[0] for c in WD_CASES])
def test_window_diff_shared_fixture(name, true, pred, k, expected):
    assert ev.window_diff(tf(true), tf(pred), k=k) == pytest.approx(expected, abs=1e-12)
    assert cm.window_diff(tf(true), tf(pred), k=k) == pytest.approx(expected, abs=1e-12)


def test_pk_mixed():
    assert ev.pk(tf("TFTFFFTFFT"), tf("TTFFFTFFFT"), k=2) == 0.375


# --- D3: default k ----------------------------------------------------------

@pytest.mark.parametrize("labels,gaps,pages", [
    (tf("TFFFFF"), 2, 3),
    (tf("TFTF"), 1, 1),
    (tf("TFFTFFTFFF"), 2, 2),
    (tf("FFTFFFF"), 2, 2),
])
def test_default_k_conventions(labels, gaps, pages):
    assert ev.default_k(labels) == gaps
    assert ev.default_k(labels, "gaps") == gaps
    assert ev.default_k(labels, "pages") == pages


def test_default_k_unknown_convention():
    with pytest.raises(ValueError):
        ev.default_k(tf("TFTF"), "bogus")


def test_window_diff_default_k():
    assert ev.window_diff([True, False, True, False], [True, True, False, False]) == pytest.approx(2 / 3)
    assert cm.window_diff([True, False, True, False], [True, True, False, False]) == pytest.approx(2 / 3)


# --- D3b: edge cases --------------------------------------------------------

def test_short_input_is_nan():
    assert np.isnan(ev.pk([True], [True]))
    assert np.isnan(ev.window_diff([True], [True]))
    assert np.isnan(ev.pk([], []))


def test_length_mismatch_raises():
    t, p = [True, False, True], [True, False]
    with pytest.raises(ValueError):
        ev.page_level_counts(t, p)
    with pytest.raises(ValueError):
        ev.page_level_prf(t, p)
    with pytest.raises(ValueError):
        ev.page_level_prf_with_counts(t, p)
    with pytest.raises(ValueError):
        ev.tolerance_prf(t, p, ["a"] * 3)
    with pytest.raises(ValueError):
        ev.tolerance_prf_runs(t, p, positions=[0, 1, 2])
    with pytest.raises(ValueError):
        ev.pk(t, p)
    with pytest.raises(ValueError):
        ev.window_diff(t, p)
    with pytest.raises(ValueError):
        ev.segment_exact_match(t, p)
    with pytest.raises(ValueError):
        ev.pk(t, t, positions=[0, 1])
    with pytest.raises(ValueError):
        ev.tolerance_prf(t, t, ["a"] * 3, positions=[0, 1])


def test_window_clipping_warns_once():
    with pytest.warns(WindowClippedWarning) as rec:
        v = ev.window_diff([True, False, True], [True, False, False], k=5)
    assert v == 1.0
    assert len(rec) == 1
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert ev.window_diff([True, False, True], [True, False, False], k=2) == 1.0


def test_clipping_warns_once_across_runs():
    true = [True, False, True, False, True, False]
    with pytest.warns(WindowClippedWarning) as rec:
        ev.pk(true, true, k=4, positions=[0, 1, 2, 10, 11, 12])
    assert len(rec) == 1


# --- P1: label validation ---------------------------------------------------

STR = ["start_page", "not_start_page", "start_page"]


@pytest.mark.parametrize("fn", [
    lambda t, p: ev.pk(t, p),
    lambda t, p: ev.window_diff(t, p),
    lambda t, p: ev.page_level_prf(t, p),
    lambda t, p: ev.tolerance_prf(t, p, ["a"] * len(t)),
    lambda t, p: ev.segment_exact_match(t, p),
    lambda t, p: cm.window_diff(t, p, k=1),
    lambda t, p: cm.score_start_classification(t, p),
])
def test_non_bool_labels_raise(fn):
    with pytest.raises(TypeError):
        fn(STR, STR)
    with pytest.raises(TypeError):
        fn([0, 1, 1], [0, 1, 1])
    with pytest.raises(TypeError):
        fn([True, False, True], [True, None, True])


def test_check_labels_message_and_numpy():
    with pytest.raises(TypeError, match="y_x.*'start_page'"):
        ev.check_labels(["start_page"], "y_x")
    assert ev.check_labels(np.array([True, False])) == [True, False]
    assert ev.pk(np.array([True, False, True]), np.array([True, False, True]), k=1) == 0.0
