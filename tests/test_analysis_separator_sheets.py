import re

import numpy as np
import pandas as pd
import pytest

from archival_structures.analysis.separator_sheets import (
    apply_rule,
    detect_sheets,
    is_separator_sheet,
    next_scan_id,
)

P = "NL-HaNA_1.04.02_2555_%04d"


@pytest.mark.parametrize("t", ["No: 7.", "No. 63", "N=o 6.", "Nr 12", "No 4.\n"])
def test_positive(t):
    assert is_separator_sheet(t)


@pytest.mark.parametrize("t", ["", None, "No. 63 den 4 Maart", "Nota 7", "7", 5])
def test_negative(t):
    assert not is_separator_sheet(t)


def test_custom_pattern_and_max_chars():
    rx = re.compile(r"^Folio \d+$")
    assert is_separator_sheet("Folio 12", pattern=rx)
    assert not is_separator_sheet("No. 7", pattern=rx)
    assert not is_separator_sheet("Folio 12", pattern=rx, max_chars=5)
    assert not is_separator_sheet("No. 12345", max_chars=4)


def test_detect_sheets_custom_columns_and_str_inventory():
    df = pd.DataFrame({"inv": [2555, 2555], "pid": [P % 1, P % 2], "txt": ["No. 3", "hello"]})
    got = detect_sheets(df, inventory_col="inv", page_col="pid", text_col="txt")
    assert got == {("2555", P % 1)}


@pytest.mark.parametrize(
    "pid,exp",
    [
        ("NL-HaNA_1.04.02_2555_0664", "NL-HaNA_1.04.02_2555_0665"),
        ("NL-HaNA_1.04.02_2555_0999", "NL-HaNA_1.04.02_2555_1000"),
        ("NL-HaNA_1.04.02_2555_00999", "NL-HaNA_1.04.02_2555_01000"),
        ("NL-HaNA_1.04.02_2555_0664.jpg", "NL-HaNA_1.04.02_2555_0665.jpg"),
        ("NL-HaNA_1.04.02_4848A_0012", "NL-HaNA_1.04.02_4848A_0013"),
        ("garbage", None),
        ("", None),
    ],
)
def test_next_scan_id(pid, exp):
    assert next_scan_id(pid) == exp


def toy(probs):
    return pd.DataFrame(
        {"inventory_number": "2555", "page_id": [P % i for i in range(1, 5)], "pred_prob": probs}
    )


SHEETS = {("2555", P % 2)}


def test_none():
    df = toy([0.1, 0.9, 0.2, 0.6])
    assert apply_rule(df, SHEETS, "none").tolist() == [False, True, False, True]


def test_sheet_and_next():
    df = toy([0.1, 0.2, 0.9, 0.6])
    assert apply_rule(df, SHEETS, "sheet").tolist() == [False, True, False, True]
    assert apply_rule(df, SHEETS, "next").tolist() == [False, False, True, True]
    low = toy([0.1, 0.2, 0.3, 0.1])
    assert not apply_rule(low, SHEETS, "sheet").any()
    assert not apply_rule(low, SHEETS, "next").any()


def test_dict_convention_missing_inventory():
    df = toy([0.1, 0.2, 0.9, 0.1])
    assert apply_rule(df, SHEETS, {"other": "sheet"}).tolist() == [False, False, True, False]
    assert apply_rule(df, SHEETS, {"2555": "sheet"}).tolist() == [False, True, False, False]


def test_absent_next_left_alone():
    df = toy([0.1, 0.9, 0.2, 0.1]).drop(index=2).reset_index(drop=True)
    assert apply_rule(df, SHEETS, "next").tolist() == [False, True, False]


def test_shuffle_invariant():
    df = toy([0.1, 0.2, 0.9, 0.6])
    base = pd.Series(apply_rule(df, SHEETS, "sheet"), index=df["page_id"])
    sh = df.sample(frac=1, random_state=3)
    got = pd.Series(apply_rule(sh, SHEETS, "sheet"), index=sh["page_id"])
    assert (got.loc[base.index] == base).all()


def test_unknown_convention():
    with pytest.raises(ValueError):
        apply_rule(toy([0.1, 0.9, 0.2, 0.1]), SHEETS, "bogus")


def test_custom_next_page_id_and_columns():
    df = toy([0.1, 0.9, 0.2, 0.1]).rename(columns={"pred_prob": "p"})
    out = apply_rule(df, SHEETS, "next", prob_col="p", next_page_id=lambda pid: P % 4)
    assert out.tolist() == [False, False, False, True]


def test_consecutive_sheets_order_independent():
    # phase-11 review: sheets on pages 2 and 3; the result must not depend on iteration order
    df = toy([0.1, 0.2, 0.9, 0.6])
    sheets = [("2555", P % 2), ("2555", P % 3)]
    a = apply_rule(df, set(sheets), "sheet").tolist()
    assert apply_rule(df, sheets, "sheet").tolist() == a
    assert apply_rule(df, list(reversed(sheets)), "sheet").tolist() == a
    assert a == [False, True, True, False]  # sorted: page 2, then page 3 (its unit 3+4)
