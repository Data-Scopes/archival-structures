"""Tests for the v2 text-layer units stage (step 15h). Synthetic rows plus the sg/9999 fixture."""

import json
from pathlib import Path

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.units import (
    DOC_ID_MAP_COLUMNS, REPUBLIC_COLUMNS, UNIT_COLUMNS, UNIT_SEGMENT_COLUMNS, UnitParams, build_units_page,
    doc_id_map_rows, republic_crosswalk, split_region)
from archival_structures.text_layer.units import INDEX_CUES, INDEX_ENTRY_DEFAULTS, entry_cues


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


def test_print_drop_cap_lines():
    ind = {"x": 360, "w": 440}
    two = _para_segs({1: ind, 2: ind})
    assert _cuts(two, indent_frac=1.0)[0] == [1, 1, 4]
    assert _cuts(two, indent_frac=1.0, drop_cap_lines=2)[0] == [6]
    three = _para_segs({1: ind, 2: ind, 3: ind})
    assert _cuts(three, indent_frac=1.0, drop_cap_lines=2)[0] == [3, 3]
    # indent + short_prev always cuts
    both = _para_segs({0: {"w": 200}, 1: ind})
    sizes, parts = _cuts(both, indent_frac=1.0, short_frac=0.2, drop_cap_lines=2)
    assert sizes == [1, 5] and parts[1]["method"] == "print:indent+short_prev"
    # an indent at line 4 after a cut at line 3 (short_prev) is dropped
    after = _para_segs({2: {"w": 200}, 4: ind})
    sizes, _ = _cuts(after, indent_frac=1.0, short_frac=0.2, drop_cap_lines=2)
    assert sizes == [3, 3]
    assert _cuts(after, indent_frac=1.0, short_frac=0.2)[0] == [3, 1, 2]


def test_unit_params_to_dict_omits_none_drop_cap():
    from archival_structures.text_layer.presets import SG_PRINT_UNIT_PARAMS
    assert "drop_cap_lines" not in UnitParams(**SG_PRINT_UNIT_PARAMS).to_dict()
    d = {**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}
    assert UnitParams.from_dict(d).to_dict() == d


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
    assert all(r[4] == "" for r in rows)
    rows = republic_crosswalk(ids, lines, roles={ids[0]: "start", ids[2]: "end", ids[3]: "both"})
    assert [r[4] for r in rows] == ["start", "", "end", "both", "", ""]


def test_crosswalk_role_end_maps_last_half():
    lines = {"S1": [("b_L", (10, 60, 40, 30), "b"), ("b_R", (60, 62, 50, 28), "b")]}
    rid = "S1-line-10-58-102-34"
    assert republic_crosswalk([rid], lines, roles={rid: "end"})[0][2:] == ["b_R", "split_original", "end"]
    assert republic_crosswalk([rid], lines, roles={rid: "start"})[0][2:] == ["b_L", "split_original", "start"]
    assert REPUBLIC_COLUMNS[-1] == "role"


def test_read_republic_roles(tmp_path):
    import gzip
    p = tmp_path / "p.tsv.gz"
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write("line_start\tline_end\nA\tB\nB\tC\nD\tD\n")
    from archival_structures.text_layer.units import read_republic_ids, read_republic_roles
    roles = read_republic_roles(p)
    assert roles == {"A": "start", "B": "both", "C": "end", "D": "both"}
    assert list(roles) == read_republic_ids(p)


def test_no_republic_dir_no_crosswalk(tmp_path, monkeypatch, capsys):
    import dataclasses
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.presets import PRESETS
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    root = tmp_path / preset.name
    extract_inventory_v2("9999", fixture, root, preset)
    base = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2"]
    assert build.main(base + ["--stage", "layout"]) == 0
    assert build.main(base + ["--stage", "types"]) == 0
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v2", dataclasses.replace(preset, merged_line_split="on"))
    assert build.main(base + ["--stage", "units"]) == 0
    assert "WARNING: no --republic-dir given" in capsys.readouterr().out
    assert json.loads((root / "units" / "params.json").read_text())["republic_dir"] is None
    assert not (root / "units" / "republic_lines").exists()


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


# ---- step 15ir ---------------------------------------------------------------------------------

def test_legacy_twin_keeps_id():
    twin = "S-text_region-100-10-50-20"
    segs = [seg(0, "date_header", x=100, y=10, w=50, h=20, chars=4), seg(1, "main_text")]
    taken = {"R", twin}
    claimable = {twin: frozenset({"l0"})}
    units, nc = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False, taken,
                                 claimable)
    ids = {u["unit_type"]: u["unit_id"] for u in units}
    assert ids == {"main_text": "R", "date_header": twin} and nc == 0 and claimable == {}
    # the legacy region held another line too: not a twin, the id is bumped
    taken = {"R", twin}
    claimable = {twin: frozenset({"l0", "lX"})}
    units, nc = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False, taken,
                                 claimable)
    ids = {u["unit_type"]: u["unit_id"] for u in units}
    assert ids["date_header"] == "S-text_region-100-10-51-20" and nc == 1 and twin in claimable
    # without ``claimable`` the behaviour is unchanged
    units, nc = build_units_page("S", "recto", [(region("R"), segs)], UnitParams(), False,
                                 {"R", twin})
    assert nc == 1


def test_missing_republic_file_logged(tmp_path, monkeypatch, capsys):
    import dataclasses
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.presets import PRESETS
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    root = tmp_path / preset.name
    extract_inventory_v2("9999", fixture, root, preset)
    base = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2"]
    assert build.main(base + ["--stage", "layout"]) == 0
    assert build.main(base + ["--stage", "types"]) == 0
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v2", dataclasses.replace(preset, merged_line_split="on"))
    nowhere = tmp_path / "no-republic"
    assert build.main(base + ["--stage", "units", "--republic-dir", str(nowhere)]) == 0
    assert "WARNING: no paragraphs-<inv>.tsv.gz" in capsys.readouterr().out
    assert json.loads((root / "units" / "params.json").read_text())["republic_dir"] is None
    nowhere.mkdir()
    (nowhere / "paragraphs-9999.tsv.gz").write_bytes(b"")
    assert build.main(base + ["--stage", "units", "--republic-dir", str(nowhere)]) in (0, 1)
    assert json.loads((root / "units" / "params.json").read_text())["republic_dir"] == str(nowhere)


def test_claimable_per_scan():
    # two scans with legacy twins: each scan claims only its own legacy regions
    twin1, twin2 = "S1-text_region-100-10-50-20", "S2-text_region-100-10-50-20"
    for sid, twin in (("S1", twin1), ("S2", twin2)):
        segs = [seg(0, "date_header", x=100, y=10, w=50, h=20, chars=4), seg(1, "main_text")]
        claimable = {twin: frozenset({"l0"})}
        units, nc = build_units_page(sid, "recto", [(region("R"), segs)], UnitParams(), False,
                                     {"R", twin}, claimable)
        assert {u["unit_type"]: u["unit_id"] for u in units} == {"main_text": "R", "date_header": twin}
        assert nc == 0


# ---------------------------------------------------------------- step 19d: index entry units

def tseg(i, text, x=300, w=500, **kw):
    d = seg(i, "main_text", x=x, w=w, chars=len(text), **kw)
    d["text"] = text
    return d


def test_entry_cue_outdent_threshold():
    cfg = {"cues": ["outdent"]}   # h = 30, so 0.5 h = 15 px
    mains = [tseg(0, "aaa", x=300), tseg(1, "bbb", x=285), tseg(2, "ccc", x=265), tseg(3, "ddd", x=300)]
    assert entry_cues(mains, [False] * 4, cfg) == [["outdent"], ["outdent"], []]
    mains = [tseg(0, "aaa", x=300), tseg(1, "bbb", x=288)]   # 12 px = 0.4 h
    assert entry_cues(mains, [False] * 2, cfg) == [[]]


def test_entry_cue_date_and_ref_and_order():
    from archival_structures.text_layer.presets import SG_INDEX_DATE_START
    cfg = {"cues": ["outdent", "date_start", "end_ref_prev"], "date_pattern": SG_INDEX_DATE_START}
    mains = [tseg(0, "Miss: Heer 42."), tseg(1, "17. April. Miss: aan"), tseg(2, "en nog iets")]
    assert entry_cues(mains, [False, False, True], cfg) == [["date_start", "end_ref_prev"], ["header"]]
    # no date pattern: date_start never fires
    assert entry_cues(mains[:2], [False] * 2, {"cues": ["date_start"]}) == [[]]
    # cues not listed do not fire
    assert entry_cues(mains[:2], [False] * 2, {"cues": ["outdent"]}) == [[]]


def test_entry_cue_short_prev_and_indent():
    mains = [tseg(0, "x", w=500), tseg(1, "y", x=330, w=300), tseg(2, "z", w=500)]
    out = entry_cues(mains, [False] * 3, {"cues": ["indent", "short_prev"], "short_frac": 0.1})
    assert out[0] == ["indent"]          # line 0 is full width: no short_prev; line 1 is indented
    assert out[1] == ["short_prev"]      # line 1 ends well left of the column's right edge


def test_split_region_index_side():
    segs = [tseg(0, "Aaa 3.", x=300), tseg(1, "cont", x=330), tseg(2, "Bbb 4.", x=300), tseg(3, "cont", x=330),
            tseg(4, "Ccc", x=300), tseg(5, "cont", x=330)]
    # the continuation lines are indented, so an entry starts where x falls back (outdent 30 px = 1 h)
    params = UnitParams(index_entry={"cues": ["outdent"]})
    parts = split_region(segs, params, False, index_side=True)
    assert [p["method"] for p in parts] == ["hand:index_start", "hand:index:outdent", "hand:index:outdent"]
    assert [len(p["segs"]) for p in parts] == [2, 2, 2]
    for kwargs in ({"index_side": False}, {}):
        parts = split_region(segs, params, False, **kwargs)
        assert [p["method"] for p in parts] == ["hand:region"]
    parts = split_region(segs, UnitParams(), False, index_side=True)
    assert [p["method"] for p in parts] == ["hand:region"]
    # print mode ignores index_entry
    pparams = UnitParams(indent_frac=0.5, index_entry={"cues": ["outdent"]})
    a = split_region(segs, pparams, True, index_side=True)
    b = split_region(segs, UnitParams(indent_frac=0.5), True)
    assert [(p["method"], len(p["segs"])) for p in a] == [(p["method"], len(p["segs"])) for p in b]


def test_split_region_index_side_header_and_single():
    segs = [tseg(0, "Aaa"), seg(1, "date_header", chars=8), tseg(2, "Bbb")]
    segs[1]["text"] = "1 Mei"
    parts = split_region(segs, UnitParams(index_entry={"cues": ["outdent"]}), False, index_side=True)
    mains = [p for p in parts if p["type"] == "main_text"]
    assert [p["method"] for p in mains] == ["hand:index_start", "hand:index:header"]


def test_unit_params_index_entry_roundtrip():
    assert "index_entry" not in UnitParams().to_dict()
    assert "index_entry" not in UnitParams(indent_frac=0.5).to_dict()
    cfg = {"cues": ["outdent", "date_start"], "outdent_frac": 0.5, "date_pattern": "x"}
    d = UnitParams(index_entry=cfg).to_dict()
    assert d["index_entry"] == cfg and UnitParams.from_dict(d) == UnitParams(index_entry=cfg)
    hash(UnitParams(index_entry=cfg))
    import pytest
    with pytest.raises(ValueError):
        UnitParams(index_entry={"cues": ["nonsense"]})
    with pytest.raises(ValueError):
        UnitParams(index_entry={"bogus": 1})
    assert set(INDEX_ENTRY_DEFAULTS["cues"]) <= set(INDEX_CUES)


def test_entry_cue_no_cut_pattern():
    from archival_structures.text_layer.presets import SG_INDEX_NO_CUT
    cfg = {"cues": ["end_ref_prev"], "no_cut_pattern": SG_INDEX_NO_CUT}
    mains = [tseg(0, "Miss: Heer 42."), tseg(1, "358, 540, 546."), tseg(2, "Nieuwe post")]
    # line 1 follows a line ending in a number, but it is a reference list that ran over: no cut
    assert entry_cues(mains, [False] * 3, cfg) == [[], ["end_ref_prev"]]
    # a header still cuts a reference-list line
    assert entry_cues(mains, [False, True, False], cfg) == [["header"], ["end_ref_prev"]]
    # default (None): old behaviour
    assert entry_cues(mains, [False] * 3, {"cues": ["end_ref_prev"]}) == [["end_ref_prev"], ["end_ref_prev"]]


def test_split_region_index_main_types():
    segs = [tseg(0, "Aaa 3.", x=300), tseg(1, "cont", x=330), tseg(2, "Bbb 4.", x=300), tseg(3, "cont", x=330)]
    for s in segs:
        s["line_type"] = "index"
    base = {"cues": ["outdent"]}
    # default main_types: index lines are no mains, they stay one run
    parts = split_region(segs, UnitParams(index_entry=base), False, index_side=True)
    assert [(p["type"], p["method"]) for p in parts] == [("index", "hand:run")]
    cfg = {**base, "main_types": ["main_text", "index"]}
    parts = split_region(segs, UnitParams(index_entry=cfg), False, index_side=True)
    assert [(p["type"], p["method"]) for p in parts] == [("main_text", "hand:index_start"),
                                                         ("main_text", "hand:index:outdent")]
    # not an index side: unchanged
    parts = split_region(segs, UnitParams(index_entry=cfg), False, index_side=False)
    assert [(p["type"], p["method"]) for p in parts] == [("index", "hand:run")]
    # the input segments are not modified
    assert all(s["line_type"] == "index" for s in segs)


def test_unit_params_index_entry_new_keys_roundtrip():
    cfg = {"cues": ["outdent"], "no_cut_pattern": r"[\d\s.,:;]+", "main_types": ["main_text", "index"]}
    d = UnitParams(index_entry=cfg).to_dict()
    assert d["index_entry"] == cfg and UnitParams.from_dict(d) == UnitParams(index_entry=cfg)
    assert INDEX_ENTRY_DEFAULTS["no_cut_pattern"] is None and INDEX_ENTRY_DEFAULTS["main_types"] == ["main_text"]


def test_unit_inventory_index_entry_needs_layout_pages(tmp_path):
    import pytest
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    from archival_structures.text_layer.presets import PRESETS
    from archival_structures.text_layer.units import unit_inventory
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    root = tmp_path / preset.name
    extract_inventory_v2("9999", fixture, root, preset)
    base = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2"]
    assert build.main(base + ["--stage", "layout"]) == 0
    assert build.main(base + ["--stage", "types"]) == 0
    with pytest.raises(FileNotFoundError, match="index_sides"):
        unit_inventory("9999", root, UnitParams(index_entry={"cues": ["outdent"]}))
    unit_inventory("9999", root, UnitParams())   # without index_entry the pages table is not needed
