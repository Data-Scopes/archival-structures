"""Tests for archival_structures.analysis.page_structure (step 15fp), on synthetic regions."""

import random

import pagexml.model.physical_document_model as pdm
from pagexml.model.coords import Baseline

from archival_structures.analysis.page_structure import (
    get_overlapping_region_sets,
    LEGACY_15FP, LayoutParams, assign_column_roles, choose_gutter_rule, find_main_edge_x,
    find_separation_x, fit_gutter, furniture_region_ids, group_regions_into_columns, hull_coords,
    inventory_line_width_rel, merge_overlapping_text_regions, region_box_id,
    merge_regions, sort_lines, sort_lines_by_row, _line_sort_key,
    split_regions_on_line_field, split_scan_into_pages)

SCAN = "NL-HaNA_1.01.02_9999_0001"


def line(lid, x, y, w=400, h=40, text="tekst", column=""):
    return pdm.PageXMLTextLine(doc_id=lid, metadata={"scan_id": SCAN, "column": column},
                               coords=pdm.Coords.coords_from_box_params(x, y, w, h),
                               baseline=Baseline([(x, y + h - 5), (x + w, y + h - 5)]), text=text)


def region(rid, lines):
    r = pdm.PageXMLTextRegion(doc_id=rid, metadata={"scan_id": SCAN}, coords=hull_coords(lines),
                              lines=lines)
    r.set_as_parent(lines)
    return r


def column_region(rid, x, y0, n, w=400, step=50, prefix=None):
    prefix = prefix or rid
    return region(rid, [line(f"{prefix}-l{i}", x, y0 + i * step, w) for i in range(n)])


def test_nested_region_merges():
    main = column_region("main", 100, 100, 10)                  # y 100..590
    odd = region("odd", [line("odd-l0", 150, 322, 200, 30)])     # inside the column, between lines
    out, passes = merge_overlapping_text_regions([main, odd])
    assert len(out) == 1 and passes == 1
    merged = out[0]
    ys = [l.coords.y for l in merged.lines]
    assert ys == sorted(ys) and [l.id for l in merged.lines].index("odd-l0") == 5
    assert merged.metadata["merged_from"] == ["main", "odd"]
    assert merged.id == region_box_id(SCAN, merged.coords)


def test_side_by_side_not_merged():
    main = column_region("main", 300, 100, 10)
    note = column_region("note", 50, 200, 3, w=200)              # margin note left of the column
    out, passes = merge_overlapping_text_regions([main, note])
    assert [r.id for r in out] == ["main", "note"] and passes == 0


def test_transitive_merge_deterministic():
    a = column_region("A", 100, 100, 4)                          # y 100..290
    b = column_region("B", 100, 210, 2, step=30)                 # y 210..280: inside A
    c = column_region("C", 100, 240, 10)                         # y 240..730: overlaps B, not A
    d = column_region("D", 900, 100, 4)
    from pagexml.helper.pagexml_helper import regions_overlap
    assert regions_overlap(a, b) and regions_overlap(b, c) and not regions_overlap(a, c)
    results = set()
    for seed in range(5):
        regions = [a, b, c, d]
        random.Random(seed).shuffle(regions)
        out, _ = merge_overlapping_text_regions(regions)
        merged = [r for r in out if r.metadata.get("merged_from")]
        assert len(out) == 2 and len(merged) == 1
        assert sorted(merged[0].metadata["merged_from"]) == ["A", "B", "C"]
        results.add((merged[0].id, tuple(l.id for l in merged[0].lines)))
    assert len(results) == 1


def _col_region(rid, x, y0, n, column, step=50):
    return region(rid, [line(f"{rid}-l{i}", x, y0 + i * step, column=column) for i in range(n)])


def test_column_guard_keeps_split_pieces_apart():
    a = _col_region("A", 100, 100, 4, "1")
    b = _col_region("B", 100, 150, 4, "2")
    blocked = {}
    out, _ = merge_overlapping_text_regions([a, b], column_blocked=blocked)
    assert len(out) == 2 and list(blocked) == [("A", "B")]
    out, _ = merge_overlapping_text_regions([a, b], LayoutParams(column_merge_guard=False))
    assert len(out) == 1


def test_column_guard_empty_value_joins():
    a = _col_region("A", 100, 100, 4, "1")
    b = _col_region("B", 100, 150, 4, "")
    out, _ = merge_overlapping_text_regions([a, b])
    assert len(out) == 1


def test_column_guard_chain():
    a = _col_region("A", 100, 100, 4, "1")
    b = region("B", [line(f"B-l{i}", 100, 210 + i * 30, column="") for i in range(2)])
    c = _col_region("C", 100, 240, 10, "2")
    sets = get_overlapping_region_sets([a, b, c], column_field="column")
    assert [[r.id for r in s] for s in sets] == [["A", "B"], ["C"]]


def test_column_guard_off_without_field():
    a = _col_region("A", 100, 100, 4, "1")
    b = _col_region("B", 100, 150, 4, "2")
    out, _ = merge_overlapping_text_regions([a, b], LayoutParams(split_on_line_field=""))
    assert len(out) == 1


def test_column_guard_order_unchanged():
    a = column_region("A", 100, 100, 4)
    b = column_region("B", 100, 210, 2, step=30)
    c = column_region("C", 100, 240, 10)
    d = column_region("D", 900, 100, 4)
    regions = [d, c, a, b]
    on = get_overlapping_region_sets(regions, column_field="column")
    off = get_overlapping_region_sets(regions)
    assert [[r.id for r in s] for s in on] == [[r.id for r in s] for s in off]


def test_unmerged_id_unchanged():
    rid = f"{SCAN}-text_region-1-2-3-4"
    r = column_region(rid, 100, 100, 3, prefix="r")
    other = column_region("other", 900, 100, 3)
    out, _ = merge_overlapping_text_regions([r, other])
    assert out[0] is r and out[0].id == rid


def test_gutter_split_region():
    straddle = region("s", [line("v0", 100, 100), line("r0", 1100, 150), line("v1", 100, 200),
                            line("r1", 1100, 250)])
    verso_only = column_region("v", 100, 400, 3)
    pages, n_split = split_scan_into_pages([straddle, verso_only], 1000.0)
    assert n_split == 1
    assert [l.id for r in pages["verso"] for l in r.lines] == ["v0", "v1", "v-l0", "v-l1", "v-l2"]
    assert [l.id for r in pages["recto"] for l in r.lines] == ["r0", "r1"]
    assert pages["verso"][0].metadata["split_from"] == ["s"]
    assert pages["verso"][1] is verso_only


def test_portrait_single():
    params = LayoutParams()
    from archival_structures.analysis.page_structure import is_opening
    assert not is_opening(1000, 1400, params) and is_opening(2000, 1400, params)
    pages, n_split = split_scan_into_pages([column_region("a", 100, 100, 3)], None)
    assert list(pages) == ["single"] and n_split == 0


def test_find_separation_x():
    boxes = [(100, 700), (1100, 700), (1050, 30)]       # text up to 800 and from 1050
    gutter_rel, n = fit_gutter([(2000, boxes), (2000, [(100, 760), (1000, 800)])])
    assert n == 2 and 0.43 < gutter_rel < 0.47
    x, method = find_separation_x(boxes, 2000, gutter_rel)
    assert method == "gap" and 800 <= x <= 1050
    x, method = find_separation_x([(100, 1800)], 2000, 0.5)
    assert method == "min_coverage"


def test_wide_header_not_chaining_columns():
    header = region("h", [line("h0", 100, 50, 1700, 40)])
    left = column_region("L", 100, 150, 10, w=800)
    right = column_region("R", 1000, 150, 10, w=800)
    columns = group_regions_into_columns([header, left, right])
    assert len(columns) == 3
    assert [[r.id for r in c.text_regions] for c in columns] == [["L"], ["h"], ["R"]] or \
        [[r.id for r in c.text_regions] for c in columns] == [["h"], ["L"], ["R"]]
    assert all(len(c.text_regions) == 1 for c in columns)


def test_roles():
    main = column_region("main", 300, 100, 20)
    lower = column_region("lower", 300, 1200, 5)            # same column, further down
    note = column_region("note", 50, 300, 2, w=150)
    columns = group_regions_into_columns([main, note, lower])
    page = assign_column_roles(columns)
    assert [r.id for c in page.columns for r in c.text_regions] == ["main", "lower"]
    assert [r.id for r in page.extra] == ["note"]
    assert note.metadata["layout_role"] == "extra" and main.metadata["layout_role"] == "main"
    assert main.metadata["layout_column"] == lower.metadata["layout_column"] == 1


def test_page_parentage():
    main = column_region("main", 300, 100, 20)
    note = column_region("note", 50, 300, 2, w=150)
    page = assign_column_roles(group_regions_into_columns([main, note]), page_id="p")
    assert page.extra and all(r.parent is page for r in page.extra)
    assert all(l.parent is note for l in note.lines)
    assert all(c.parent is page for c in page.columns)
    assert all(r.parent is page.columns[0] for r in page.columns[0].text_regions)


def test_empty_page_builds():
    page = assign_column_roles([], page_id="p")
    assert page.columns == [] and page.extra == []


# --- step 15fq ------------------------------------------------------------------------------

def _opening_boxes():
    """Page width 2000: verso lines x 200-1000, recto margin notes x 1005-1200, recto text from
    1300 (gap 1210-1290)."""
    verso = [(200, 800)] * 8
    notes = [(1005, 195)] * 3
    recto = [(1300, 600)] * 8
    return verso + notes + recto


def test_main_edge_moves_recto_margin_notes():
    boxes = _opening_boxes()
    med = inventory_line_width_rel([(2000, boxes)])
    x, method = find_separation_x(boxes, 2000, 0.5)
    assert method == "gap" and 1210 <= x <= 1290        # notes (centre 1100) go to the verso
    x, method = find_separation_x(boxes, 2000, 0.5, rule="main_edge", med_w_rel=med)
    assert method == "main_edge" and x == 1000          # notes go to the recto


def test_main_edge_fallback_few_long_lines():
    boxes = [(200, 800)] * 3 + [(1300, 600)] * 8
    med = 0.3
    assert find_main_edge_x(boxes, 2000, 0.5, med) is None
    x, method = find_separation_x(boxes, 2000, 0.5, rule="main_edge", med_w_rel=med)
    assert method == "gap"


def test_main_edge_fallback_window():
    boxes = [(200, 400)] * 8 + [(1700, 250)] * 8        # edges 600 and 1700: 1000 away from c0
    assert find_main_edge_x(boxes, 2000, 0.5, 0.2) is None


def test_choose_gutter_rule():
    p = LayoutParams()
    assert choose_gutter_rule(984, 1000, p) == "gap"
    assert choose_gutter_rule(874, 1000, p) == "main_edge"
    assert choose_gutter_rule(0, 0, p) == "gap"
    assert choose_gutter_rule(1, 100, LayoutParams(gutter_rule="gap")) == "gap"
    assert choose_gutter_rule(100, 100, LayoutParams(gutter_rule="main_edge")) == "main_edge"


def test_split_on_line_field():
    lines = [line("a", 100, 100, column="1"), line("b", 600, 100, column="2"),
             line("c", 100, 150, column="1"), line("d", 100, 400, column="")]
    r = region("R", lines)
    out, n = split_regions_on_line_field([r], "column")
    assert n == 1 and len(out) == 3
    assert [[l.id for l in o.lines] for o in out] == [["a", "c"], ["b"], ["d"]]
    assert all(o.metadata["column_split_from"] == ["R"] for o in out)
    assert out[0].id == region_box_id(SCAN, out[0].coords)
    same = region("S", [line("e", 100, 100), line("f", 100, 150)])
    out, n = split_regions_on_line_field([same], "column")
    assert n == 0 and out[0] is same


PATTERNS = (r"\W*(fol\.?|pag\.?|p\.)?\s*\d{1,4}\s*\W*",)


def test_furniture_not_merged():
    # interleave guard off: only ``exclude`` keeps the page number "( 93" out of the merge
    params = LayoutParams(furniture_patterns=PATTERNS, interleave_guard=False)
    main = column_region("main", 100, 100, 10)
    number = region("num", [line("n0", 150, 322, 100, 30, text="( 93")])
    ids = furniture_region_ids([main, number], 600, 1000, params)
    assert ids == {"num"}
    out, _ = merge_overlapping_text_regions([main, number], params, exclude=ids)
    assert [r.id for r in out] == ["main", "num"]
    out, _ = merge_overlapping_text_regions([main, number], params, exclude=())
    assert len(out) == 1


def test_split_sorts_lines():
    lines = [line("a", 100, 300, column="1"), line("b", 600, 100, column="2"),
             line("c", 100, 100, column="1"), line("d", 600, 200, column="2")]
    r = region("R", lines)
    out, n = split_regions_on_line_field([r], "column", sort_lines=True)
    assert n == 1
    assert [[l.id for l in o.lines] for o in out] == [["c", "a"], ["b", "d"]]
    out, _ = split_regions_on_line_field([r], "column")
    assert [[l.id for l in o.lines] for o in out] == [["a", "c"], ["b", "d"]]
    out, _ = split_regions_on_line_field([r], "column", sort_lines=False)
    assert [[l.id for l in o.lines] for o in out] == [["a", "c"], ["b", "d"]]
    one = region("S", [line("e", 100, 300, column="1"), line("f", 100, 100, column="1")])
    out, n = split_regions_on_line_field([one], "column", sort_lines=True)
    assert n == 0 and out[0] is one and [l.id for l in one.lines] == ["e", "f"]
    assert LayoutParams().sort_split_lines and not LEGACY_15FP.sort_split_lines
    # step 15j: row-tolerant sort of the pieces
    rl = [line("a", 500, 930, h=75, column="1"), line("b", 100, 936, h=75, column="1"),
          line("c", 600, 100, h=75, column="2"), line("d", 100, 980, h=75, column="1")]
    out, _ = split_regions_on_line_field([region("R2", rl)], "column", sort_lines=True,
                                         row_tol_frac=0.2)
    assert [l.id for l in out[0].lines] == ["b", "a", "d"]
    out, _ = split_regions_on_line_field([region("R3", rl)], "column", sort_lines=True)
    assert [l.id for l in out[0].lines] == ["a", "b", "d"]


def test_blocked_counted_once():
    a1 = _col_region("A1", 100, 100, 4, "1")
    a2 = _col_region("A2", 100, 150, 4, "1")
    b = _col_region("B", 100, 150, 4, "2")
    blocked, col_blocked = {}, {}
    out, passes = merge_overlapping_text_regions([a1, a2, b], blocked=blocked,
                                                 column_blocked=col_blocked)
    assert passes == 1 and len(out) == 2
    assert blocked == {}
    assert list(col_blocked) == [(out[0].id, "B")]      # one pair, of final regions
    assert out[0].metadata["merged_from"] == ["A1", "A2"]


def test_gutter_rule_validated():
    import pytest
    for rule in ("gap", "main_edge", "auto"):
        LayoutParams(gutter_rule=rule)
    with pytest.raises(ValueError):
        LayoutParams(gutter_rule="edge")


def test_catchword_band():
    page_h = 1000
    body = column_region("body", 100, 100, 5)
    catch = region("catch", [line("c0", 100, 940, 120, 30, text="Gesien")])
    mid = region("mid", [line("m0", 600, 500, 120, 30, text="Gesien")])
    two = region("two", [line("t0", 100, 960, 200, 30, text="twee woorden")])
    params = LayoutParams()
    assert furniture_region_ids([body, catch, mid, two], 970, page_h, params) == {"catch"}
    assert furniture_region_ids([body, catch], 970, page_h, LayoutParams(catchword_band=0.0)) == set()


def test_interleave_guard_blocks_margin_item():
    main = column_region("main", 100, 100, 10)                  # lines w 400, y 100..590
    note = region("note", [line("note-l0", 150, 322, 100, 30, text="1. Maart")])
    blocked = {}
    out, passes = merge_overlapping_text_regions([main, note], blocked=blocked)
    assert [r.id for r in out] == ["main", "note"] and passes == 0
    assert list(blocked) == [("main", "note")]
    out, _ = merge_overlapping_text_regions([main, note], LayoutParams(interleave_guard=False))
    assert len(out) == 1


def test_nested_full_width_still_merges():
    main = column_region("main", 100, 100, 10)
    odd = region("odd", [line("odd-l0", 100, 322, 400, 30)])
    out, _ = merge_overlapping_text_regions([main, odd])
    assert len(out) == 1
    # sticking out above the column's first line: not strictly inside, so not blocked
    top = region("top", [line("top-l0", 150, 90, 100, 30)])
    out, _ = merge_overlapping_text_regions([main, top])
    assert len(out) == 1


def test_legacy_params_match_15fp_defaults():
    d = LEGACY_15FP.to_dict()
    assert d["gutter_rule"] == "gap" and d["interleave_guard"] is False
    assert LayoutParams.from_dict(d) == LEGACY_15FP
    assert LayoutParams.from_dict({"furniture_patterns": ["a"]}).furniture_patterns == ("a",)
    main = column_region("main", 100, 100, 10)
    note = region("note", [line("note-l0", 150, 322, 100, 30)])
    out, passes = merge_overlapping_text_regions([main, note], LEGACY_15FP)
    assert len(out) == 1 and passes == 1                        # 15fp behaviour: merged
    assert find_separation_x(_opening_boxes(), 2000, 0.5, LEGACY_15FP)[1] == "gap"
    assert LEGACY_15FP.line_sort == "baseline" and LayoutParams().line_sort == "row"


def _same_row_lines():
    # baselines (y + 70): A 1000, B 1006, C 1050; h 75
    return [line("A", 500, 930, h=75), line("B", 100, 936, h=75), line("C", 100, 980, h=75)]


def test_row_sort_same_row_fragments():
    lines = _same_row_lines()
    assert [l.id for l in sort_lines_by_row(lines, 0.2)] == ["B", "A", "C"]
    assert [l.id for l in sorted(lines, key=_line_sort_key)] == ["A", "B", "C"]
    assert sort_lines_by_row([], 0.2) == []
    assert [l.id for l in sort_lines(lines, LayoutParams())] == ["B", "A", "C"]
    assert [l.id for l in sort_lines(lines, LEGACY_15FP)] == ["A", "B", "C"]


def test_row_sort_keeps_separate_rows():
    lines = [line(f"L{i}", 900 - 100 * i, 100 + 48 * i, h=75) for i in range(5)]
    assert ([l.id for l in sort_lines_by_row(lines, 0.2)]
            == [l.id for l in sorted(lines, key=_line_sort_key)])


def test_line_sort_validated():
    import pytest
    for kw in ({"line_sort": "x"}, {"row_tol_frac": 0}, {"row_tol_frac": 1}):
        with pytest.raises(ValueError):
            LayoutParams(**kw)
    p = LayoutParams(line_sort="baseline", row_tol_frac=0.3, sort_unchanged_lines=True)
    assert LayoutParams.from_dict(p.to_dict()) == p


def test_merge_regions_row_key():
    lines = _same_row_lines()
    r1, r2 = region("R1", lines[:1]), region("R2", lines[1:])
    assert [l.id for l in merge_regions([r1, r2], row_tol_frac=0.2).lines] == ["B", "A", "C"]
    assert [l.id for l in merge_regions([r1, r2], row_tol_frac=None).lines] == ["A", "B", "C"]
