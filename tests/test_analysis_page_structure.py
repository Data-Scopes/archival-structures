"""Tests for archival_structures.analysis.page_structure (step 15fp), on synthetic regions."""

import random

import pagexml.model.physical_document_model as pdm
from pagexml.model.coords import Baseline

from archival_structures.analysis.page_structure import (
    LayoutParams, assign_column_roles, find_separation_x, fit_gutter, group_regions_into_columns,
    hull_coords, merge_overlapping_text_regions, region_box_id, split_scan_into_pages)

SCAN = "NL-HaNA_1.01.02_9999_0001"


def line(lid, x, y, w=400, h=40, text="tekst"):
    return pdm.PageXMLTextLine(doc_id=lid, metadata={"scan_id": SCAN},
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
