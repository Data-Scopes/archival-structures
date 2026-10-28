"""Tests for analysis.index_pages (step 19c)."""
import pytest

from archival_structures.analysis.index_pages import (
    INDEX_SIDES_DEFAULTS, fill_runs, side_features, side_rule)
from archival_structures.analysis.page_structure import LayoutParams


def _line(x, text, w="400"):
    return {"x": str(x), "w": w, "text": text}


def test_side_features_two_columns():
    lines = [_line(50, "lemma entry text 12") for _ in range(10)] + \
            [_line(550, "lemma entry text 34") for _ in range(10)]
    f = side_features(lines, {})
    assert f["n_lines"] == 20 and f["n_text8"] == 20 and f["n_text20"] == 0   # 19 chars < 20
    lines = [_line(50, "lemma entry text 12 x") for _ in range(10)] + \
            [_line(550, "lemma entry text 34 x") for _ in range(10)]
    f = side_features(lines, {})
    assert f["n_text20"] == 20 and f["col_share"] == pytest.approx(0.5)
    assert f["end_num"] == 0.0


def test_side_features_few_long_lines_none():
    lines = [_line(50, "x" * 30) for _ in range(14)]
    assert side_features(lines, {})["col_share"] is None
    assert side_features(lines + [_line(50, "x" * 30)], {})["col_share"] == 0.0


def test_side_features_end_and_date():
    lines = [_line(0, "resolutie 12"), _line(0, "12 Jan. 1700 iets"), _line(0, "geen cijfers hier"),
             _line(0, "kort"), _line(0, "   ")]
    f = side_features(lines, {"date_pattern": r"\d{1,2}\s*jan"})
    assert f["n_lines"] == 4 and f["n_text8"] == 3
    assert f["end_num"] == pytest.approx(1 / 3) and f["date_start"] == pytest.approx(1 / 3)
    assert side_features(lines, {})["date_start"] == 0.0
    assert side_features([], {}) == {"n_lines": 0, "n_text8": 0, "n_text20": 0, "col_share": None,
                                     "end_num": 0.0, "date_start": 0.0}


def test_col_min_chars_uses_unstripped_text():
    lines = [_line(50, " " * 10 + "abc") for _ in range(15)]
    f = side_features(lines, {"col_min_chars": 13})
    assert f["n_text20"] == 15 and f["n_text8"] == 0 and f["col_share"] == 0.0


def test_side_rule_arms():
    base = {"n_lines": 30, "n_text8": 30, "n_text20": 30, "col_share": 0.4, "end_num": 0.2, "date_start": 0.0}
    assert side_rule(base, {})
    assert not side_rule({**base, "col_share": 0.1}, {})
    assert not side_rule({**base, "col_share": None}, {})
    assert not side_rule({**base, "end_num": 0.1}, {})
    assert side_rule({**base, "end_num": 0.0, "date_start": 0.3}, {})
    shape_only = {**base, "col_share": None, "end_num": 0.5}
    assert not side_rule(shape_only, {})
    assert side_rule(shape_only, {"shape_only_min": 0.3})
    assert not side_rule({**shape_only, "n_text8": 4}, {"shape_only_min": 0.3})


def test_fill_runs():
    assert fill_runs([], 4, 3) == []
    f = [True, False, False, True, False]
    assert fill_runs(f, 0, 1) == f
    assert fill_runs(f, 2, 1) == [True, True, True, True, False]
    assert fill_runs(f, 1, 1) == f
    assert fill_runs([True, False, True, False, False, False, True], 1, 3) == [True, True, True, False, False, False, False]
    assert fill_runs([True, True, False, True, False], 0, 2) == [True, True, False, False, False]


def test_layout_params_index_sides():
    assert "index_sides" not in LayoutParams().to_dict()
    p = LayoutParams(index_sides={"gap": 8})
    assert LayoutParams.from_dict(p.to_dict()) == p
    assert p.to_dict()["index_sides"] == {"gap": 8}
    with pytest.raises(ValueError):
        LayoutParams(index_sides={"bogus": 1})
    assert set(INDEX_SIDES_DEFAULTS) >= {"gap", "min_run"}
