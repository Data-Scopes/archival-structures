from archival_structures.analysis.index_links import front_index_run, read_volume_inputs
from archival_structures.text_layer.io import write_tsv_gz
import pytest


def _flags(first=0, n_flag=12, gap_len=25, after=320, holes=()):
    f = [False] * first + [True] * n_flag + [False] * (gap_len + after)
    for h in holes:
        f[h] = False
    return f


def test_run_ok():
    assert front_index_run(_flags(first=2)) == (2, 13, 14)


def test_each_condition_fails_once():
    assert front_index_run([]) is None
    assert front_index_run(_flags(n_flag=9)) is None            # min_flagged
    assert front_index_run(_flags(first=10)) is None            # max_first
    assert front_index_run(_flags(after=270)) is None           # min_after (n - end = 295)
    assert front_index_run([True] * 12 + [False] * 5 + [True] * 300) is None  # no gap
    assert front_index_run([True] * 700 + [False] * 30 + [True] * 100) is None  # share
    assert front_index_run([False] * 400 + [True] * 12) is None  # gap from start: no flagged before end


def test_gap_19_does_not_end_run():
    flags = [True] * 12 + [False] * 19 + [True] * 3 + [False] * 25 + [False] * 320
    assert front_index_run(flags) == (0, 33, 34)
    short = [True] * 12 + [False] * 20 + [False] * 320
    assert front_index_run(short) == (0, 11, 12)


def test_read_volume_inputs(tmp_path):
    root = tmp_path
    write_tsv_gz(root / "scans" / "9.scans.tsv.gz", ["scan_id", "status"], [["A", "ok"], ["B", "no_regions"], ["C", "ok"]])
    write_tsv_gz(root / "layout" / "pages" / "9.tsv.gz", ["scan_id", "page", "index_side"],
                 [["A", "verso", "1"], ["A", "recto", ""], ["C", "single", "0"]])
    write_tsv_gz(root / "lines" / "9.lines.tsv.gz", ["scan_id", "line_id", "text"],
                 [["A", "l1", "ab 12. cd"], ["A", "l2", "text"], ["C", "l1", "7"]])
    write_tsv_gz(root / "types" / "segments" / "9.tsv.gz", ["scan_id", "line_id", "char_start", "char_end", "page", "line_type"],
                 [["A", "l1", "3", "6", "recto", "page_number"], ["A", "l2", "0", "4", "recto", "main_text"],
                  ["C", "l1", "0", "1", "single", "page_number"]])
    v = read_volume_inputs(root, "9")
    assert v.scan_ids == ["A", "B", "C"]
    assert v.sides == [("A", "verso"), ("A", "recto"), ("C", "single")]
    assert v.flags == [True, False, False]
    assert v.numbers == {("A", "recto"): ["12."], ("C", "single"): ["7"]}
    with pytest.raises(FileNotFoundError):
        read_volume_inputs(root, "10")


# --- step 20e: locators, links, builder -------------------------------------------------------------------------

import gzip
import importlib.util
import json
import sys
from pathlib import Path

from archival_structures.analysis.index_links import (
    DEFAULT_LOCATOR_VARIANT, SG_LOCATOR_PARAMS, VolumeInputs, extract_locators, volume_links)

TABLE = [
    ("Vriesche Compen. op de fleuijtschepen, 252: 568: 581: 594: 616: 623:",
     [[252, 568, 581, 594, 616, 623]] * 3 + [[252]]),
    ("Geiden 170, 375, 483.", [[170, 375, 483]] * 3 + [[170, 375]]),
    ("Uts: openstellen der Vaart. 703, 752. Uto: de Scheepvaart aan de gang geraakt, 858.",
     [[858], [703, 752, 858], [703, 752, 858], []]),
    ("Missr. van Wartensleben, rak de zaak van Erechiels 6e: 147.", [[147]] * 4),
    ("In de continuatie van 6 Fregat, ter bewaringe 328.", [[328], [6, 328], [6, 328], [6, 328]]),
    ("Levantschen handel vide 2e. L. Hanicart 342.", [[342]] * 4),
    ("Ontfangen 17. April Requeste van Jan 412.", [[412], [17, 412], [412], []]),
    ("Betalinge van 6 maanden Tractement 96.", [[96]] * 4),
    ("Vereeringe f 150. Aen de Weduwe 77.", [[77], [150, 77], [77], [77]]),
]


def test_locator_table():
    for text, expected in TABLE:
        for v, exp in zip(("L1", "L2", "L3", "L4"), expected):
            got = [l.number for l in extract_locators(text, SG_LOCATOR_PARAMS[v], folio_range=(1, 400))]
            assert got == exp, (text, v)


def test_ordinal_upper_case_only():
    t = "Regiment 6E. Capiteyn 12."
    assert [l.number for l in extract_locators(t, SG_LOCATOR_PARAMS["L2"])] == [6, 12]
    assert [l.number for l in extract_locators(t, SG_LOCATOR_PARAMS["L3"])] == [12]


def test_l4_without_range_is_l3_and_offsets():
    t = "Geiden 170, 375, 483."
    l3 = extract_locators(t, SG_LOCATOR_PARAMS["L3"])
    assert extract_locators(t, SG_LOCATOR_PARAMS["L4"]) == l3
    assert [t[l.char_start:l.char_end] for l in l3] == ["170", "375", "483"]
    assert [l.number for l in extract_locators("12345 en 7", SG_LOCATOR_PARAMS["L2"])] == [7]  # 5 digits: no candidate
    assert DEFAULT_LOCATOR_VARIANT in SG_LOCATOR_PARAMS


def _synthetic_inputs(n=340, n_flag=12):
    scan_ids = [f"S{i:04d}" for i in range(1, n + 1)]
    sides = [(s, "recto") for s in scan_ids]
    flags = [i < n_flag for i in range(n)]
    numbers = {(s, "recto"): [str(i - 39)] for i, s in enumerate(scan_ids) if i >= 40}
    return VolumeInputs("9", scan_ids, sides, flags, numbers)


def test_volume_links_synthetic():
    inputs = _synthetic_inputs()
    units = [dict(scan_id="S0001", page="recto", unit_id="u1", unit_type="main_text"),
             dict(scan_id="S0002", page="recto", unit_id="u2", unit_type="main_text"),
             dict(scan_id="S0002", page="recto", unit_id="u3", unit_type="page_number"),
             dict(scan_id="S0200", page="recto", unit_id="u4", unit_type="main_text")]   # not a front-index side
    texts = {("S0001", "u1"): "Geiden 5, 7.", ("S0002", "u2"): "Nergens", ("S0002", "u3"): "9", ("S0200", "u4"): "x 3."}
    res = volume_links(inputs, units, texts, SG_LOCATOR_PARAMS["L2"])
    assert res["front"] == (0, 11, 12)
    assert [u["unit_id"] for u in res["entries"]] == ["u1", "u2"]
    rows = res["links"]
    assert [(r["number"], r["front_scan_id"], r["method"]) for r in rows] == [(5, "S0045", "printed"), (7, "S0047", "printed")]
    assert rows[0]["back_scan_id"] == "" and rows[1]["loc_idx"] == 1
    texts[("S0001", "u1")] = "Geiden 5, 800."
    far = volume_links(inputs, units, texts, SG_LOCATOR_PARAMS["L2"])["links"]
    assert far[1]["method"] == "none" and far[1]["front_scan_id"] == "" and far[1]["number"] == 800
    assert volume_links(_synthetic_inputs(n_flag=5), units, texts, SG_LOCATOR_PARAMS["L2"]) is None


def _write_layer(root, inv, front):
    inputs = _synthetic_inputs(n_flag=12 if front else 0)
    write_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz", ["scan_id", "status"], [[s, "ok"] for s in inputs.scan_ids])
    write_tsv_gz(root / "layout" / "pages" / f"{inv}.tsv.gz", ["scan_id", "page", "index_side"],
                 [[s, p, "1" if f else "0"] for (s, p), f in zip(inputs.sides, inputs.flags)])
    lines, segs = [], []
    for (s, p), nums in inputs.numbers.items():
        lines.append([s, "l1", nums[0]])
        segs.append([s, "l1", "0", str(len(nums[0])), p, "page_number"])
    write_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz", ["scan_id", "line_id", "text"], lines)
    write_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz",
                 ["scan_id", "line_id", "char_start", "char_end", "page", "line_type"], segs)
    units = [[f"S{i:04d}", "recto", f"u{i}", "main_text"] for i in (1, 2, 3)]
    write_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz", ["scan_id", "page", "unit_id", "unit_type"], units)
    write_tsv_gz(root / "join" / "units" / f"{inv}.tsv.gz", ["scan_id", "unit_id", "text"],
                 [["S0001", "u1", "Geiden 5, 7."], ["S0002", "u2", "Sonder"], ["S0003", "u3", "Vlooten 12."]])


def _load_builder():
    path = Path(__file__).resolve().parent.parent / "scripts" / "build_index_links.py"
    spec = importlib.util.spec_from_file_location("build_index_links", path)
    mod = importlib.util.module_from_spec(spec)
    # a process pool pickles module-level functions by module name: make the module importable
    sys.path.insert(0, str(path.parent))
    sys.modules["build_index_links"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_builder_two_volumes_and_resume(tmp_path):
    layer, out = tmp_path / "layer", tmp_path / "out"
    out.mkdir()
    _write_layer(layer, "1", True)
    _write_layer(layer, "2", False)
    (layer / "manifest.json").write_text(json.dumps({"layer": "synthetic_v5"}))
    b = _load_builder()
    assert b.main(["--layer", str(layer), "--out-dir", str(out)]) == 0
    root = out / "NL-HaNA_1.01.02_index_links_v1"
    assert (root / "links" / "1.tsv.gz").exists() and (root / "folio_map" / "1.tsv.gz").exists()
    assert not (root / "links" / "2.tsv.gz").exists()
    vol = (root / "volumes.tsv").read_text().splitlines()
    assert len(vol) == 2 and vol[1].split("\t")[:2] == ["1", "foliation"]
    f = vol[1].split("\t")
    assert f[10:] == ["3", "3", "3"]
    man = json.loads((root / "manifest.json").read_text())
    assert man["source_layer"] == "synthetic_v5" and man["counts"] == dict(volumes=1, n_entries=3, n_locators=3, n_linked=3)
    mtime = (root / "links" / "1.tsv.gz").stat().st_mtime_ns
    assert b.main(["--layer", str(layer), "--out-dir", str(out)]) == 0
    assert (root / "links" / "1.tsv.gz").stat().st_mtime_ns == mtime
    assert json.loads((root / "manifest.json").read_text())["last_run"]["outcomes"] == {"skipped": 1, "no_front_index": 1}


def test_builder_failed_volume_is_recorded(tmp_path):
    layer, out = tmp_path / "layer", tmp_path / "out"
    out.mkdir()
    _write_layer(layer, "1", True)
    _write_layer(layer, "2", True)
    (layer / "manifest.json").write_text(json.dumps({"layer": "synthetic_v5"}))
    (layer / "units" / "units" / "2.tsv.gz").unlink()
    b = _load_builder()
    args = ["--layer", str(layer), "--out-dir", str(out)]
    assert b.main(args) == 1
    root = out / "NL-HaNA_1.01.02_index_links_v1"
    assert (root / "links" / "1.tsv.gz").exists() and not (root / "links" / "2.tsv.gz").exists()
    last = json.loads((root / "manifest.json").read_text())["last_run"]
    assert last["outcomes"] == {"done": 1, "failed": 1} and "2" in last["failed"]
    assert "units" in last["failed"]["2"]
    assert len((root / "progress.log").read_text().splitlines()) == 2
    _write_layer(layer, "2", True)
    assert b.main(args) == 0
    last = json.loads((root / "manifest.json").read_text())["last_run"]
    assert last["outcomes"] == {"skipped": 1, "done": 1} and last["failed"] == {}
    assert len((root / "progress.log").read_text().splitlines()) == 4
    assert b.main(args + ["--workers", "2"]) == 0
    assert json.loads((root / "manifest.json").read_text())["last_run"]["outcomes"] == {"skipped": 2}


# --- step 21c: locators L5-L7, lines of a side, entry assembly ---------------------------------------------------

from types import SimpleNamespace

from archival_structures.analysis.index_links import (
    Entry, LineBox, assemble_entries, entry_locators, read_side_lines, side_reading_order)
from archival_structures.text_layer.presets import SG_HAND_COLUMN_ORDER_V5


def _nums(text, variant, rng=None):
    return [l.number for l in extract_locators(text, SG_LOCATOR_PARAMS[variant], folio_range=rng)]


def test_l5_needs_punct_before_capital():
    assert _nums("Verlof 14 Aug", "L5") == []
    assert _nums("Verlof 14 Aug", "L2") == [14]
    assert _nums("Hof 252. Misse 300.", "L5") == [252, 300] == _nums("Hof 252. Misse 300.", "L2")


def test_l6_left_boundary():
    assert _nums("Denhof 354.", "L6") == [354]
    assert _nums("Denhof 354.", "L3") == []
    assert _nums("Thesing7.", "L6") == []
    assert _nums("Oorlog1789 en U1 Cunes94", "L6") == []
    assert _nums("X f 150. Y 77.", "L2") == [150, 77]
    assert _nums("X f 150. Y 77.", "L6") == [77]


def test_l7_is_l6_with_range():
    t = "Geiden 170, 375, 483."
    assert _nums(t, "L7") == _nums(t, "L6") == [170, 375, 483]
    assert _nums(t, "L7", (100, 400)) == [170, 375]       # 483 > 400 + 6
    assert _nums(t, "L6", (100, 400)) == [170, 375, 483]
    assert [l.number for l in extract_locators(t, SG_LOCATOR_PARAMS["L4"], folio_range=(100, 400))] == [170, 375]


def _write_side_layer(root, inv="7"):
    sc = "SC1"
    rows = [  # line_id, text, x, y, w, h
        ("a", "Geiden 5,", 100, 10, 500, 40), ("b", "7.", 100, 60, 100, 40), ("c", "12", 100, 110, 50, 40),
        ("d", "Marg 9.", 1300, 10, 400, 40), ("e", "Tekst", 100, 160, 300, 40), ("f", "99", 900, 900, 30, 30)]
    write_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz", ["scan_id", "line_id", "x", "y", "w", "h", "text"],
                 [[sc, i, x, y, w, h, t] for i, t, x, y, w, h in rows])
    write_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz", ["scan_id", "page", "unit_id", "unit_order", "unit_type"],
                 [[sc, "recto", "u2", "1", "marginalia"], [sc, "recto", "u1", "0", "main_text"],
                  [sc, "recto", "u3", "2", "page_number"], [sc, "recto", "u4", "3", "main_text"],
                  [sc, "verso", "u5", "0", "main_text"]])
    write_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz", ["scan_id", "unit_id", "seg_order", "line_id", "seg_idx"],
                 [[sc, "u1", "1", "b", "0"], [sc, "u1", "0", "a", "0"], [sc, "u1", "2", "c", "0"],
                  [sc, "u2", "0", "d", "0"], [sc, "u3", "0", "f", "0"], [sc, "u4", "0", "e", "0"],
                  [sc, "u4", "1", "c", "0"], [sc, "u5", "0", "e", "0"]])


def test_read_side_lines_base_order_and_types(tmp_path):
    _write_side_layer(tmp_path)
    got = read_side_lines(tmp_path, "7", [("SC1", "recto"), ("SC1", "verso"), ("SC1", "single")])
    assert [l.line_id for l in got[("SC1", "recto")]] == ["a", "b", "c", "d", "e"]   # no page_number "f"; "c" once
    assert got[("SC1", "recto")][3].unit_type == "marginalia" and got[("SC1", "recto")][0].w == 500
    assert [l.line_id for l in got[("SC1", "verso")]] == ["e"] and got[("SC1", "single")] == []


def _lb(i, text, x=100, y=0, w=300, h=40):
    return LineBox(i, text, x, y, w, h, "u", "main_text", 0, 0)


def test_assemble_a1_line_rule():
    lines = [_lb("1", "Alpha beta"), _lb("2", "gamma 12,"), _lb("3", "14."), _lb("4", ""), _lb("5", "Delta 3"),
             _lb("6", "Open entry")]
    es = assemble_entries(lines, "A1")
    assert [(e.kind, e.line_ids) for e in es] == [("entry", ("1", "2", "3")), ("entry", ("5",)), ("entry", ("6",))]
    assert es[0].text == "Alpha beta gamma 12, 14." and es[2].text == "Open entry"
    assert [l.number for l, _ in entry_locators(es[0], SG_LOCATOR_PARAMS["L2"])] == [12, 14]


def test_assemble_a2_reorders_two_columns():
    left = [_lb(f"L{i}", f"links {i}", x=100, y=100 * i, w=700) for i in range(4)]
    right = [_lb(f"R{i}", f"rechts {i}", x=1300, y=100 * i + 5, w=700) for i in range(4)]
    inter = [lb for pair in zip(left, right) for lb in pair]
    got, moved = side_reading_order(inter, SG_HAND_COLUMN_ORDER_V5)
    assert moved and [l.line_id for l in got] == ["L0", "L1", "L2", "L3", "R0", "R1", "R2", "R3"]
    one, moved1 = side_reading_order(left, SG_HAND_COLUMN_ORDER_V5)
    assert not moved1 and one == left
    a1 = assemble_entries([_lb(l.line_id, l.text + " 1", l.x, l.y, l.w) for l in inter], "A1")
    a2 = assemble_entries([_lb(l.line_id, l.text + " 1", l.x, l.y, l.w) for l in inter], "A2",
                          column_cfg=SG_HAND_COLUMN_ORDER_V5)
    assert len(a1) == len(a2) == 8 and [e.line_ids[0] for e in a2][:4] == ["L0", "L1", "L2", "L3"]


def test_assemble_a3_heading():
    lines = [_lb("1", "Commissien."), _lb("2", "Aen den Heer 37."), _lb("3", "Voor"), _lb("4", "de Heer van Hof 38.")]
    assert [e.kind for e in assemble_entries(lines, "A2", column_cfg=SG_HAND_COLUMN_ORDER_V5)] == ["entry", "entry"]
    es = assemble_entries(lines, "A3", column_cfg=SG_HAND_COLUMN_ORDER_V5)
    assert [e.kind for e in es] == ["heading", "entry", "heading", "entry"]
    # known cost of the rule: the one-word first line "Voor" of a real entry is taken as a heading as well
    assert es[2].text == "Voor" and entry_locators(es[0], SG_LOCATOR_PARAMS["L2"]) == []


def test_wbc_join_and_spans():
    es = assemble_entries([_lb("1", "ver„"), _lb("2", "nietigen 580.")], "A1")
    assert es[0].text == "vernietigen 580." and es[0].spans == (("1", 0, 3), ("2", 3, 16))
    got = entry_locators(es[0], SG_LOCATOR_PARAMS["L2"])
    assert [(l.number, lid) for l, lid in got] == [(580, "2")]


def test_entry_locators_line_of_locator():
    es = assemble_entries([_lb("1", "Geiden"), _lb("2", "Hof, 9 Mey."), _lb("3", "Kerk 12.")], "A1")
    assert [e.line_ids for e in es] == [("1", "2", "3")]
    got = entry_locators(es[0], SG_LOCATOR_PARAMS["L2"])
    assert [(l.number, lid) for l, lid in got] == [(9, "2"), (12, "3")]
    e2 = Entry("entry", ("a", "b"), "Geiden 5, 7, 9.", (("a", 0, 9), ("b", 10, 15)))
    assert [(l.number, lid) for l, lid in entry_locators(e2, SG_LOCATOR_PARAMS["L2"])] == [(5, "a"), (7, "b"), (9, "b")]
    assert entry_locators(Entry("heading", ("a",), "Kerk 5", (("a", 0, 6),)), SG_LOCATOR_PARAMS["L2"]) == []


# --- step 21d: volume_links_v2 and the v2 builder ------------------------------------------------------------------

from archival_structures.analysis.index_links import (
    ENTRY_COLUMNS, LINK_COLUMNS_V2, V2_CONFIG, volume_links_v2)
from archival_structures.analysis.folio_map import FOLIO_MAP_V2


def _inputs_with_verso():
    inp = _synthetic_inputs()
    inp.sides.append(("S0045", "verso"))      # the facing page of folio 5
    inp.flags.append(False)
    return inp


def test_volume_links_v2_synthetic():
    inputs = _inputs_with_verso()
    left = [LineBox(f"L{i}", f"links {5 + i}", 100, 100 * i, 700, 40, "u", "main_text", 0, i) for i in range(4)]
    right = [LineBox(f"R{i}", f"rechts {20 + i}", 1300, 100 * i + 5, 700, 40, "u", "main_text", 0, 4 + i) for i in range(4)]
    inter = [lb for pair in zip(left, right) for lb in pair]
    cfg = dict(V2_CONFIG, method="A2")
    res = volume_links_v2(inputs, {("S0001", "recto"): inter}, [], {}, config=cfg)
    assert res["front"] == (0, 11, 12)
    assert [e["entry_id"] for e in res["entries"]] == [f"S0001:recto:e{n:03d}" for n in range(1, 9)]
    assert [e["first_line_id"] for e in res["entries"]] == ["L0", "L1", "L2", "L3", "R0", "R1", "R2", "R3"]
    assert set(res["entries"][0]) == set(ENTRY_COLUMNS) and res["entries"][0]["line_ids"] == "L0"
    rows = res["links"]
    assert set(rows[0]) == set(LINK_COLUMNS_V2)
    assert [(r["entry_id"], r["line_id"], r["number"]) for r in rows[:2]] == [("S0001:recto:e001", "L0", 5), ("S0001:recto:e002", "L1", 6)]
    assert [r["number"] for r in rows] == [5, 6, 7, 8, 20, 21, 22, 23]
    assert rows[0]["front_scan_id"] == "S0045" and rows[0]["method"] == "printed"
    assert (rows[0]["opening_scan_id"], rows[0]["opening_page"]) == ("S0045", "verso")
    assert rows[1]["opening_scan_id"] == ""      # S0046 has no verso side
    assert volume_links_v2(_synthetic_inputs(n_flag=5), {}, [], {}, config=cfg) is None
    assert V2_CONFIG["map_params"] is FOLIO_MAP_V2 and V2_CONFIG["method"] == "A0" and V2_CONFIG["variant"] == "L7"


def test_volume_links_v2_default_a0():
    inputs = _inputs_with_verso()
    units = [dict(scan_id="S0001", page="recto", unit_id="u2", unit_type="main_text", unit_order="1"),
             dict(scan_id="S0001", page="recto", unit_id="u1", unit_type="main_text", unit_order="0"),
             dict(scan_id="S0001", page="recto", unit_id="u3", unit_type="marginalia", unit_order="2"),
             dict(scan_id="S0200", page="recto", unit_id="u4", unit_type="main_text", unit_order="0")]
    texts = {("S0001", "u1"): "Geiden 5, 7.", ("S0001", "u2"): "Hof 400.", ("S0001", "u3"): "Marg 9."}
    segs = {("S0001", "u1"): [("a", 0, 7), ("b", 8, 12)], ("S0001", "u2"): [("c", 0, 8)]}
    res = volume_links_v2(inputs, {}, units, texts, unit_segments=segs)
    assert [e["entry_id"] for e in res["entries"]] == ["u1", "u2"]            # unit_order, main_text, front sides
    assert res["entries"][0]["line_ids"] == "a|b" and res["entries"][0]["n_lines"] == 2 and res["entries"][0]["text"] == "Geiden 5, 7."
    got = [(r["entry_id"], r["number"], r["line_id"]) for r in res["links"]]
    assert got == [("u1", 5, "a"), ("u1", 7, "b")]        # 400 is outside the chain range + 6 (L7): no locator
    assert res["links"][0]["front_scan_id"] == "S0045"


def _write_layer_v2(root, inv, front):
    _write_layer(root, inv, front)
    write_tsv_gz(root / "join" / "segments" / f"{inv}.tsv.gz",
                 ["scan_id", "unit_id", "seg_order", "seg_idx", "char_start", "char_end", "line_id"],
                 [["S0001", "u1", "0", "0", "0", "12", "t1"], ["S0002", "u2", "0", "0", "0", "6", "t2"],
                  ["S0003", "u3", "0", "0", "0", "11", "t3"]])


def test_builder_v2_tables_and_resume(tmp_path):
    layer, out = tmp_path / "layer", tmp_path / "out"
    out.mkdir()
    _write_layer_v2(layer, "1", True)
    _write_layer_v2(layer, "2", False)
    (layer / "manifest.json").write_text(json.dumps({"layer": "synthetic_v5"}))
    b = _load_builder()
    assert b.main(["--layer", str(layer), "--out-dir", str(out), "--version", "v2"]) == 0
    root = out / "NL-HaNA_1.01.02_index_links_v2"
    for t in ("folio_map", "entries", "links"):
        assert (root / t / "1.tsv.gz").exists() and not (root / t / "2.tsv.gz").exists()
    with gzip.open(root / "links" / "1.tsv.gz", "rt") as f:
        assert f.readline().rstrip("\n").split("\t") == LINK_COLUMNS_V2
    with gzip.open(root / "folio_map" / "1.tsv.gz", "rt") as f:
        assert "opening_scan_id" in f.readline()
    vol = (root / "volumes.tsv").read_text().splitlines()
    assert vol[0].split("\t")[-3:] == ["n_headings", "n_dup_dropped", "chain_cut"] and len(vol) == 2
    assert vol[1].split("\t")[10:] == ["3", "3", "3", "0", "0", "0"]
    man = json.loads((root / "manifest.json").read_text())
    assert man["version"] == "v2" and man["config"]["method"] == "A0" and man["config"]["variant"] == "L7"
    assert man["bars"]["L_recall_21c"]["result"] == "fail" and man["counts"]["n_linked"] == 3
    mtime = (root / "links" / "1.tsv.gz").stat().st_mtime_ns
    assert b.main(["--layer", str(layer), "--out-dir", str(out), "--version", "v2"]) == 0
    assert (root / "links" / "1.tsv.gz").stat().st_mtime_ns == mtime
    assert json.loads((root / "manifest.json").read_text())["last_run"]["outcomes"] == {"skipped": 1, "no_front_index": 1}


def test_builder_v1_unchanged_default(tmp_path):
    layer, out = tmp_path / "layer", tmp_path / "out"
    out.mkdir()
    _write_layer(layer, "1", True)
    (layer / "manifest.json").write_text(json.dumps({"layer": "synthetic_v5"}))
    b = _load_builder()
    assert b.main(["--layer", str(layer), "--out-dir", str(out)]) == 0
    root = out / "NL-HaNA_1.01.02_index_links_v1"
    assert not (out / "NL-HaNA_1.01.02_index_links_v2").exists() and not (root / "entries").exists()
    with gzip.open(root / "links" / "1.tsv.gz", "rt") as f:
        assert f.readline().rstrip("\n").split("\t") == b.LINK_COLUMNS
    assert (root / "volumes.tsv").read_text().splitlines()[0].split("\t") == b.VOLUME_COLUMNS
    assert "version" not in json.loads((root / "manifest.json").read_text())


def test_builder_v2_rejects_variant(tmp_path):
    layer, out = tmp_path / "layer", tmp_path / "out"
    out.mkdir()
    b = _load_builder()
    with pytest.raises(SystemExit):
        b.main(["--layer", str(layer), "--out-dir", str(out), "--version", "v2", "--variant", "L2"])
