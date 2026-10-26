import random

import pytest

from archival_structures.analysis.hand_print import (
    VolumeModeParams, fit_thresholds, region_features, region_geometry, scan_label, scan_value,
    volume_mode)


def L(y, h=80, text="abcdef", rid="r1", sid="s1"):
    bl = "" if y is None else f"0,{y} 100,{y}"
    return {"region_id": rid, "scan_id": sid, "h": h, "baseline": bl, "text_raw": text}


def test_region_geometry():
    g = region_geometry([L(100), L(150), L(200)])
    assert g["hrel"] == pytest.approx(1.6) and g["h_med"] == 80 and g["pitch_med"] == 50
    assert g["n_lines"] == 3 and g["n_chars"] == 18
    assert region_geometry([L(100), L(150)]) is None
    # empty baseline skipped: 3 lines but only 2 ys -> 1 pitch -> None
    assert region_geometry([L(100), L(150), L(None)]) is None
    assert region_geometry([L(100), L(150), L(200), L(None)])["hrel"] == pytest.approx(1.6)
    # equal ys give no pitch
    assert region_geometry([L(100), L(100), L(100)]) is None
    # h == "" skipped
    g = region_geometry([L(100, h=""), L(150), L(200, h=60), L(250, h=100)])
    assert g["h_med"] == 80
    assert region_geometry([L(100, h=""), L(150, h=""), L(200, h="")]) is None
    # empty text lines do not count
    assert region_geometry([L(100), L(150), L(200, text="")]) is None


def test_scan_value_weighted_median():
    f = lambda *a: [{"hrel": h, "n_chars": c} for h, c in a]
    assert scan_value(f((1.2, 100), (1.6, 300))) == (1.6, 400)
    assert scan_value(f((1.2, 300), (1.6, 100))) == (1.2, 400)
    assert scan_value(f((1.2, 200), (1.6, 200)))[0] == 1.2   # bisect_left: c=[200,400], 200 -> idx 0
    assert scan_value(f((1.2, 100), (None, 500))) == (1.2, 100)
    assert scan_value([]) == (None, 0)


def test_scan_label_bounds():
    p = VolumeModeParams(1.5, 1.6)
    assert scan_label(1.5, 300, p) == "print"
    assert scan_label(1.6, 300, p) == "hand"
    assert scan_label(1.55, 300, p) == "uncertain"
    assert scan_label(1.0, 199, p) == "unlabelled"
    assert scan_label(None, 500, p) == "unlabelled"


def test_fit_thresholds_swap_and_round():
    assert fit_thresholds([2.0] * 10, [1.0] * 10) == (1.0, 2.0)
    assert fit_thresholds([1.0] * 10, [2.0] * 10) == (1.0, 2.0)   # crossing: swapped
    lo, hi = fit_thresholds([1.23456] * 5, [1.0, 1.0, 1.0, 1.0, 1.9])
    assert lo == 1.235 or hi == 1.235
    assert fit_thresholds(list(range(101)), list(range(101))) == (5.0, 95.0)


def scan_rows(sid, y_step, n_regions=2, text="x" * 40):
    rows = []
    for r in range(n_regions):
        for i in range(4):
            rows.append(L(100 + i * y_step, rid=f"{sid}-{r}", sid=sid, text=text))
    return rows


def test_volume_mode():
    p = VolumeModeParams(1.5, 1.6)
    # y_step 50, h 80 -> 1.6 (hand); y_step 60 -> 1.333 (print); y_step 52 -> 1.538 uncertain
    rows = scan_rows("a", 50) + scan_rows("b", 60)
    r = volume_mode(rows, p)
    assert r["print_scan_share"] == 0.5 and r["mode"] == "print" and r["method"] == "geometry"
    assert r["n_scans"] == 2 and r["n_scans_print"] == 1 and r["n_scans_hand"] == 1
    r = volume_mode(scan_rows("a", 52) + scan_rows("b", 52, text="x"), p)
    assert (r["mode"], r["method"], r["print_scan_share"]) == ("hand", "default", None)
    assert r["n_scans_uncertain"] == 1 and r["n_scans_labelled"] == 1
    assert r["n_scans"] == 2
    r = volume_mode(scan_rows("a", 50) + scan_rows("b", 60) + scan_rows("c", 52) + scan_rows("d", 52, text="x"), p)
    assert (r["n_scans_print"] + r["n_scans_hand"] + r["n_scans_uncertain"]
            + sum(s["label"] == "unlabelled" for s in r["scans"])) == r["n_scans"] == 4
    assert len(r["regions"]) == 8


def test_params_round_trip_and_validation():
    p = VolumeModeParams(1.529, 1.534, print_share=0.4)
    assert VolumeModeParams.from_dict(p.to_dict()) == p
    with pytest.raises(ValueError):
        VolumeModeParams.from_dict({"t_lo": 1, "t_hi": 2, "bogus": 3})
    with pytest.raises(ValueError):
        VolumeModeParams.from_dict({"t_lo": 2, "t_hi": 1})
    with pytest.raises(TypeError):
        VolumeModeParams.from_dict({"t_hi": 2})


def test_deterministic():
    p = VolumeModeParams(1.5, 1.6)
    rows = scan_rows("a", 50) + scan_rows("b", 60) + scan_rows("c", 52)
    base = volume_mode(rows, p)
    shuffled = rows[:]
    random.Random(1).shuffle(shuffled)
    r = volume_mode(shuffled, p)
    key = lambda s: s["scan_id"]
    assert (r["mode"], r["print_scan_share"]) == (base["mode"], base["print_scan_share"])
    assert sorted(r["scans"], key=key) == sorted(base["scans"], key=key)


# ---------------------------------------------------------------- box-height rule (phase 12, step 15u)
from archival_structures.analysis.hand_print import (  # noqa: E402
    BoxHeightParams, box_height_mode, fit_box_height_cut, scan_box_heights, volume_box_height)


def F(h, hrel, chars, sid="s1", rid=None):
    return {"region_id": rid or f"r{h}_{chars}_{sid}", "scan_id": sid, "n_chars": chars, "h_med": h, "hrel": hrel}


def test_volume_box_height():
    assert volume_box_height([F(60, 1.4, 300), F(140, 1.7, 100)]) == (60, 400, 2)
    # 99 chars excluded, hrel None excluded
    assert volume_box_height([F(60, 1.4, 99), F(140, 1.7, 100)]) == (140, 100, 1)
    assert volume_box_height([F(60, None, 500), F(140, 1.7, 100)]) == (140, 100, 1)
    assert volume_box_height([F(60, None, 500), F(140, 1.7, 99)]) == (None, 0, 0)
    assert volume_box_height([]) == (None, 0, 0)
    # tie at half: cum [100, 200], half 100 -> bisect_left -> index 0
    assert volume_box_height([F(50, 1, 100), F(90, 1, 100)])[0] == 50


def test_fit_box_height_cut():
    assert fit_box_height_cut([89.5, 150], [65, 86]) == 87.75
    # overlapping: print 60,70,100 ; hand 90,95,120 -> best cut between 95 and 100? evaluate
    # cut 80: print>80:1/3, hand<=80:0 -> 1/6 ; cut 92.5: print>:1/3, hand<=:1/3 -> 1/3 ; cut 97.5: print>:0, hand<=:2/3 -> 1/3
    assert fit_box_height_cut([90, 95, 120], [60, 70, 100]) == 80.0
    # equal errors -> lowest cut: print [10, 30], hand [20, 40]: cut 15 err (1/2+0)/2=.25 ; cut 25: (.5+.5)/2=.5 ; cut 35: (0+.5)/2=.25
    assert fit_box_height_cut([20, 40], [10, 30]) == 15.0
    assert fit_box_height_cut([10.004], [3.0]) == 6.5   # (3+10.004)/2 = 6.502 -> 6.5
    assert fit_box_height_cut([1.0], [0.0]) == 0.5


def test_box_height_mode():
    p = BoxHeightParams(cut=87.75)
    assert box_height_mode([F(87.75, 1.5, 200)], p)["mode"] == "print"
    r = box_height_mode([F(90.0, 1.5, 200)], p)
    assert r["mode"] == "hand" and r["method"] == "box_height" and r["margin"] == pytest.approx(2.25)
    r = box_height_mode([F(90.0, None, 200), F(50, 1.5, 10)], p)
    assert (r["mode"], r["method"], r["margin"], r["value"]) == ("hand", "default", None, None)
    assert r["params"] == {"cut": 87.75, "min_region_chars": 100}


def test_box_height_params():
    p = BoxHeightParams(cut=80.0, min_region_chars=50)
    assert BoxHeightParams.from_dict(p.to_dict()) == p
    with pytest.raises(ValueError):
        BoxHeightParams.from_dict({"cut": 80, "bogus": 1})
    with pytest.raises(ValueError):
        BoxHeightParams.from_dict({"cut": 0})
    with pytest.raises(ValueError):
        BoxHeightParams.from_dict({"cut": -1})
    with pytest.raises(TypeError):
        BoxHeightParams.from_dict({"min_region_chars": 100})


def test_scan_box_heights():
    feats = [F(80, 1.5, 300, "b"), F(100, 1.5, 120, "b"), F(70, 1.5, 150, "a"), F(70, None, 500, "c")]
    out = scan_box_heights(feats)
    assert [o["scan_id"] for o in out] == ["b", "a", "c"]
    assert out[0] == {"scan_id": "b", "value": 80, "n_chars": 420, "n_regions": 2}
    assert out[1]["value"] == 70
    assert out[2] == {"scan_id": "c", "value": None, "n_chars": 0, "n_regions": 0}


def test_box_height_deterministic():
    feats = [F(60 + i * 3, 1.5, 100 + 7 * i, rid=f"r{i}") for i in range(20)]
    base = box_height_mode(feats, BoxHeightParams(cut=80))
    rng = random.Random(1)
    for _ in range(5):
        sh = feats[:]
        rng.shuffle(sh)
        assert box_height_mode(sh, BoxHeightParams(cut=80)) == base
