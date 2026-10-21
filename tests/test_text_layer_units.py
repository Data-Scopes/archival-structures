"""Tests for the v2 text-layer units stage (step 15h). Synthetic rows plus the sg/9999 fixture."""

import json
from pathlib import Path

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.units import (
    DOC_ID_MAP_COLUMNS, UNIT_COLUMNS, UNIT_SEGMENT_COLUMNS, UnitParams, build_units_page,
    doc_id_map_rows, republic_crosswalk, split_region)


def seg(i, ltype, x=300, y=None, w=500, h=30, chars=20, idx=0):
    y = 200 + 60 * i if y is None else y
    return {"scan_id": "S", "line_id": f"l{i}", "seg_idx": idx, "char_start": 0, "char_end": chars,
            "x": x, "y": y, "w": w, "h": h, "line_type": ltype, "_pos": i}


def region(rid="R", role="main", col=0):
    return {"layout_region_id": rid, "layout_role": role, "layout_column": col}


def test_hand_furniture_pulled_out_main_keeps_id():
    segs = [seg(0, "date_header", chars=8), seg(1, "main_text"), seg(2, "main_text"),
            seg(3, "page_number", chars=3)]
    units, nc = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False, {"R"})
    assert [u["unit_type"] for u in units] == ["date_header", "main_text", "page_number"]
    main = [u for u in units if u["unit_type"] == "main_text"][0]
    assert main["unit_id"] == "R" and len(main["segs"]) == 2 and nc == 0
    assert len({u["unit_id"] for u in units}) == 3


def test_hand_main_cut_at_date_header():
    segs = [seg(0, "main_text"), seg(1, "main_text"), seg(2, "date_header", chars=8),
            seg(3, "main_text"), seg(4, "main_text"), seg(5, "main_text")]
    parts = split_region(segs, UnitParams(), False)
    mains = [p for p in parts if p["type"] == "main_text"]
    assert [len(p["segs"]) for p in mains] == [2, 3]
    assert {p["method"] for p in mains} == {"hand:region_cut_at_header"}
    units, _ = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False, {"R"})
    assert [u["unit_id"] for u in units if u["unit_type"] == "main_text"][1] == "R"   # more chars


def test_marginalia_inside_region_does_not_cut_main():
    segs = [seg(0, "main_text"), seg(1, "main_text"), seg(2, "marginalia", x=50, w=100, chars=5),
            seg(3, "main_text"), seg(4, "main_text")]
    parts = split_region(segs, UnitParams(), False)
    assert [(p["type"], len(p["segs"])) for p in parts] == [("main_text", 4), ("marginalia", 1)]
    assert parts[0]["method"] == "hand:region"


def _para_segs(override=None):
    """Six lines, left edge 300, right edge 800, h 30, tight spacing."""
    segs = [seg(i, "main_text", y=200 + 35 * i) for i in range(6)]
    for i, v in (override or {}).items():
        segs[i].update(v)
    return segs


def _cuts(segs, **kw):
    parts = split_region(segs, UnitParams(**kw), True)
    return [len(p["segs"]) for p in parts if p["type"] == "main_text"], parts


def test_print_paragraph_cues():
    # each cue alone; the other cues are off
    assert _cuts(_para_segs())[0] == [6]
    indent = _para_segs({3: {"x": 360, "w": 440}})
    sizes, parts = _cuts(indent, indent_frac=1.0)
    assert sizes == [3, 3] and parts[1]["method"] == "print:indent"
    assert _cuts(indent, indent_frac=3.0)[0] == [6]
    short = _para_segs({2: {"w": 200}})
    sizes, parts = _cuts(short, short_frac=0.2)
    assert sizes == [3, 3] and parts[1]["method"] == "print:short_prev"
    assert _cuts(short, short_frac=0.9)[0] == [6]
    gap = _para_segs({3: {"y": 200 + 35 * 3 + 40}, 4: {"y": 200 + 35 * 4 + 40},
                        5: {"y": 200 + 35 * 5 + 40}})
    sizes, parts = _cuts(gap, gap_factor=1.0)
    assert sizes == [3, 3] and parts[1]["method"] == "print:gap"
    assert _cuts(gap, gap_factor=2.0)[0] == [6]
    header = _para_segs()
    header[3]["line_type"] = "running_header"
    header[3]["x"] = 500
    sizes, parts = _cuts(header)
    assert sizes == [3, 2] and parts[2]["method"] == "print:header"
    both = _para_segs({3: {"x": 360, "w": 440}, 2: {"w": 200}})
    assert _cuts(both, indent_frac=1.0, short_frac=0.2)[1][1]["method"] == "print:indent+short_prev"


def test_new_id_collision_bumps_w():
    segs = [seg(0, "date_header", x=100, y=10, w=50, h=20, chars=4), seg(1, "main_text")]
    taken = {"R", "S-text_region-100-10-50-20", "S-text_region-100-10-51-20"}
    units, nc = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False, taken)
    ids = {u["unit_type"]: u["unit_id"] for u in units}
    assert ids["main_text"] == "R"
    assert ids["date_header"] == "S-text_region-100-10-52-20" and nc == 2


def test_reading_order():
    verso = [seg(0, "main_text", y=500)]
    recto_c0 = [seg(1, "main_text", y=900)]
    recto_c1 = [seg(2, "main_text", y=100), seg(3, "date_header", y=60, chars=4)]
    uv, _ = build_units_page("S", "verso", [(region("V"), verso)], UnitParams(), False, {"V"})
    ur, _ = build_units_page("S", "recto", [(region("C1", col=1), recto_c1),
                                           (region("C0", col=0), recto_c0)],
                             UnitParams(), False, {"C0", "C1"})
    ordered = sorted(ur + uv, key=lambda u: u["_key"])
    assert [u["layout_region_id"] for u in ordered] == ["V", "C0", "C1", "C1"]
    assert [u["unit_type"] for u in ordered][2:] == ["date_header", "main_text"]


def test_doc_id_map_status():
    line_region = {("S", "a"): "OLD1", ("S", "b"): "OLD2", ("S", "c"): "OLD2", ("S", "d"): "OLD3",
                   ("S", "e"): "OLD4"}
    seg_unit = {("S", "a", 0): "OLD1",
                ("S", "b", 0): "OLD2", ("S", "c", 0): "NEW",
                ("S", "d", 0): "NEW2", ("S", "e", 0): "OLD1"}   # e (OLD4) is swallowed by OLD1
    seg_chars = {k: 10 for k in seg_unit}
    unit_sizes = {"OLD1": 2, "OLD2": 1, "NEW": 1, "NEW2": 1}
    rows = doc_id_map_rows(["OLD1", "OLD2", "OLD3", "OLD4", "OLD5"], line_region, seg_unit,
                           seg_chars, unit_sizes)
    status = {r[0]: r[3] for r in rows}
    assert status == {"OLD1": "trimmed", "OLD2": "trimmed", "OLD3": "changed", "OLD4": "changed",
                      "OLD5": "empty"}
    assert [r[2] for r in rows if r[0] == "OLD2"] == ["same_id", "part"]
    assert [r for r in rows if r[0] == "OLD5"] == [["OLD5", "", "none", "empty", 0, 0]]
    unit_sizes["OLD1"] = 1
    seg_unit = {("S", "a", 0): "OLD1"}
    rows = doc_id_map_rows(["OLD1"], {("S", "a"): "OLD1"}, seg_unit, {("S", "a", 0): 5}, unit_sizes)
    assert rows == [["OLD1", "OLD1", "same_id", "identical", 1, 5]]


def test_republic_crosswalk_exact_near_split_original():
    lines = {"S1": [("a", (10, 20, 100, 30), ""), ("b_L", (10, 60, 40, 30), "b"),
                     ("b_R", (60, 62, 50, 28), "b"), ("c", (10, 200, 100, 30), "")]}
    ids = ["S1-line-10-20-100-30", "S1-line-12-18-103-30", "S1-line-10-58-102-34",
           "S1-line-500-500-10-10", "S9-line-1-1-1-1",
           "S1-line-60-62-35-28"]
    rows = republic_crosswalk(ids, lines)
    assert [(r[2], r[3]) for r in rows] == [("a", "exact"), ("a", "near"), ("b_L", "split_original"),
                                            ("", "unmatched"), ("", "unmatched"),
                                            ("b_R", "split_half")]


def test_build_stage_units(tmp_path):
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.presets import PRESETS
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    root = tmp_path / preset.name
    extract_inventory_v2("9999", fixture, root, preset)
    base = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2"]
    assert build.main(base + ["--stage", "layout"]) == 0
    assert build.main(base + ["--stage", "types"]) == 0
    segs_before = (root / "types" / "segments" / "9999.tsv.gz").read_bytes()
    assert build.main(base + ["--stage", "units"]) == 0
    assert (root / "types" / "segments" / "9999.tsv.gz").read_bytes() == segs_before
    units = list(read_tsv_gz(root / "units" / "units" / "9999.tsv.gz"))
    usegs = list(read_tsv_gz(root / "units" / "segments" / "9999.tsv.gz"))
    dmap = list(read_tsv_gz(root / "units" / "doc_id_map" / "9999.tsv.gz"))
    assert list(units[0]) == UNIT_COLUMNS and list(usegs[0]) == UNIT_SEGMENT_COLUMNS
    assert list(dmap[0]) == DOC_ID_MAP_COLUMNS
    tsegs = list(read_tsv_gz(root / "types" / "segments" / "9999.tsv.gz"))
    assert sorted((s["scan_id"], s["line_id"], s["seg_idx"]) for s in usegs) == \
        sorted((s["scan_id"], s["line_id"], s["seg_idx"]) for s in tsegs)
    assert len({u["unit_id"] for u in units}) == len(units)
    meta = list(read_tsv_gz(root / "regions_meta" / "9999.tsv.gz"))
    assert {r["region_id"] for r in meta} == {r["old_doc_id"] for r in dmap}
    stats = json.loads((root / "units" / "stats.json").read_text())["9999"]
    assert stats["n_units"] == len(units) and stats["n_id_collisions"] == 0
    assert "units" in json.loads((root / "manifest.json").read_text())["stages"]
    first = (root / "units" / "units" / "9999.tsv.gz").read_bytes()
    assert build.main(base + ["--stage", "units"]) == 0
    assert (root / "units" / "units" / "9999.tsv.gz").read_bytes() == first
