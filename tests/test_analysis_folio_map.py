
import numpy as np

from archival_structures.analysis.folio_map import FolioMapParams, build_folio_map


def _vol(n, numbered, blank=(), verso_numbers=()):
    """n scans; scan i (1-based) has verso+recto except blanks; numbered: {scan position: number} on rectos."""
    scans = [f"S{i:03d}" for i in range(1, n + 1)]
    sides, nums = [], {}
    for i, s in enumerate(scans, 1):
        if i in blank:
            continue
        sides += [(s, "verso"), (s, "recto")]
        if i in numbered:
            nums[(s, "recto")] = [str(numbered[i])]
        if i in verso_numbers:
            nums[(s, "verso")] = [str(verso_numbers[i])]
    return scans, sides, nums


def test_printed_hit_and_back():
    scans, sides, nums = _vol(20, {i: i - 1 for i in range(2, 21)})
    m = build_folio_map(scans, sides, nums)
    assert m.mode == "foliation"
    t = m.locate(5)
    assert t.front == ("S006", "recto") and t.back == ("S007", "verso")
    assert t.method == "printed" and t.anchor_dist == 0 and t.confidence == "high"


def test_interpolation_across_gap_of_three():
    numbered = {i: i for i in range(1, 21) if i not in (8, 9, 10)}
    scans, sides, nums = _vol(20, numbered)
    t = build_folio_map(scans, sides, nums).locate(9)
    assert t.method == "interpolated" and t.front[0] == "S009"


def test_blank_scan_shifts_scan_positions():
    # scan 6 is blank (no sides); numbers skip it by position
    numbered = {i: i - (i > 6) for i in range(1, 21) if i != 6}
    scans, sides, nums = _vol(20, numbered, blank={6})
    m = build_folio_map(scans, sides, nums)
    assert m.locate(5).front[0] == "S005"
    assert m.locate(6).front[0] == "S007"
    t = build_folio_map(scans, sides, {k: v for k, v in nums.items() if k[0] != "S007"}).locate(6)
    assert t.front[0] == "S007"  # interpolated over scan positions (blank counts as a position)
    # the blank scan itself gets front (scan, "recto") when asked via a map with that position
    only = {i: i for i in (1, 2, 3, 4, 5, 8, 9, 10)}
    sc, sd, nm = _vol(10, only, blank={6})
    tt = build_folio_map(sc, sd, nm).locate(6)
    assert tt.front == ("S006", "recto") and tt.back == ("S007", "verso")


def test_out_of_order_anchor_dropped():
    numbered = {i: i for i in range(1, 21)}
    numbered[10] = 3
    scans, sides, nums = _vol(20, numbered)
    m = build_folio_map(scans, sides, nums)
    assert 10 not in m.chain[:, 0] and m.n_anchors == 20
    assert m.locate(10).front[0] == "S010" and m.locate(10).method == "interpolated"


def test_verso_anchors_dropped_in_foliation():
    scans, sides, nums = _vol(30, {i: i for i in range(1, 30)}, verso_numbers={5: 99})
    m = build_folio_map(scans, sides, nums)
    assert m.mode == "foliation" and 99 not in m.chain[:, 1] and m.recto_share == 29 / 30


def test_pagination_mode():
    scans, sides, _ = _vol(10, {})
    nums = {s: [str(i + 1)] for i, s in enumerate(sides)}
    m = build_folio_map(scans, sides, nums)
    assert m.mode == "pagination" and m.recto_share == 0.5
    t = m.locate(4)
    assert t.front == sides[3] and t.back is None and t.position == 4


def test_exclude_ignored():
    scans, sides, nums = _vol(20, {i: i for i in range(1, 21)})
    bad = {(scans[i], "recto") for i in range(3)}
    m = build_folio_map(scans, sides, nums, exclude=bad)
    assert m.n_anchors == 17 and m.chain[0, 1] == 4


def test_extrapolation_limit():
    scans, sides, nums = _vol(40, {i: i for i in range(1, 21)})
    m = build_folio_map(scans, sides, nums)
    t = m.locate(26)
    assert t.method == "extrapolated" and t.front[0] == "S026" and t.confidence == "low"
    assert m.locate(27) is None
    assert build_folio_map(scans, sides, nums).locate(0) is None


def test_min_chain_none():
    scans, sides, nums = _vol(10, {1: 1, 2: 2})
    m = build_folio_map(scans, sides, nums)
    assert m.mode == "none" and m.locate(1) is None and m.chain.shape == (0, 2) and m.table() == []


def test_round_half_up():
    # anchors at positions 4 (number 4) and 5 (number 5)... build chain with s = 4.5
    scans, sides, nums = _vol(10, {1: 1, 4: 4, 5: 6, 9: 10})
    t = build_folio_map(scans, sides, nums).locate(5)
    assert t.position == 5


def test_back_none_without_next_verso():
    scans = ["A", "B", "C"]
    sides = [("A", "verso"), ("A", "recto"), ("B", "recto"), ("C", "verso"), ("C", "recto")]
    nums = {("A", "recto"): ["1"], ("B", "recto"): ["2"], ("C", "recto"): ["3"]}
    m = build_folio_map(scans, sides, nums)
    assert m.locate(1).back is None  # B has no verso
    assert m.locate(2).back == ("C", "verso")
    assert m.locate(3).back is None  # last scan


def test_table_rows_and_keys():
    scans, sides, nums = _vol(20, {i: i + 5 for i in range(1, 16)})
    rows = build_folio_map(scans, sides, nums).table()
    assert [r["number"] for r in rows] == list(range(1, 27))
    assert set(rows[0]) == {"number", "front_scan_id", "front_page", "back_scan_id", "back_page", "position",
                            "method", "anchor_dist", "confidence"}
    assert rows[0]["method"] == "extrapolated"


def test_number_pattern_and_first_text():
    scans, sides, nums = _vol(10, {i: i for i in range(1, 11)})
    nums[("S003", "recto")] = ["x-y-z-w", " 3. "]
    m = build_folio_map(scans, sides, nums, params=FolioMapParams())
    assert 3 in m.chain[:, 1]
