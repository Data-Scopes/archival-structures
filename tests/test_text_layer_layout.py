"""Tests for the v2 text-layer layout stage (step 15fp). Fixture: tests/fixtures/text_layer/sg/9999."""

import gzip
import json
from pathlib import Path

from archival_structures.text_layer import build
from archival_structures.text_layer.extract_v2 import extract_inventory_v2
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.layout import (
    LAYOUT_LINE_COLUMNS, LAYOUT_REGION_COLUMNS, LAYOUT_SCAN_COLUMNS, layout_inventory)
from archival_structures.text_layer.presets import PRESETS

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
PRESET = PRESETS["sg_1.01.02_v2"]
INPUTS = ("scans/9999.scans.tsv.gz", "regions_meta/9999.tsv.gz", "lines/9999.lines.tsv.gz")


def _layer(tmp_path):
    root = tmp_path / PRESET.name
    extract_inventory_v2("9999", FIXTURE, root, PRESET)
    return root


def _header(path):
    import gzip
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return handle.readline().rstrip("\n").split("\t")


def test_layout_inventory_conserves_lines(tmp_path):
    root = _layer(tmp_path)
    before = {name: (root / name).read_bytes() for name in INPUTS}
    stats = layout_inventory("9999", root)
    assert {name: (root / name).read_bytes() for name in INPUTS} == before
    out = root / "layout"
    assert _header(out / "scans" / "9999.tsv.gz") == LAYOUT_SCAN_COLUMNS
    assert _header(out / "regions" / "9999.tsv.gz") == LAYOUT_REGION_COLUMNS
    assert _header(out / "lines" / "9999.tsv.gz") == LAYOUT_LINE_COLUMNS
    lines_in = [(r["scan_id"], r["line_id"]) for r in read_tsv_gz(root / INPUTS[2])]
    lines_out = [(r["scan_id"], r["line_id"]) for r in read_tsv_gz(out / "lines" / "9999.tsv.gz")]
    assert sorted(lines_out) == sorted(lines_in) and len(set(lines_out)) == len(lines_out)
    regions = list(read_tsv_gz(out / "regions" / "9999.tsv.gz"))
    assert len({r["layout_region_id"] for r in regions}) == len(regions)
    assert all(r["layout_region_id"] == r["source_region_ids"]
               for r in regions if r["change"] == "unchanged")
    meta_ids = {r["region_id"] for r in read_tsv_gz(root / INPUTS[1])}
    assert {s for r in regions for s in r["source_region_ids"].split()} == meta_ids
    scans = {r["scan_id"]: r for r in read_tsv_gz(out / "scans" / "9999.tsv.gz")}
    assert scans["NL-HaNA_1.01.02_9999_0001"]["page_mode"] == "opening"
    assert scans["NL-HaNA_1.01.02_9999_0004_deelopname1"]["page_mode"] == "single"
    pages = {r["layout_region_id"]: r["page"] for r in regions}
    assert pages["NL-HaNA_1.01.02_9999_0001-text_region-1100-100-800-300"] == "recto"
    assert stats["n_lines"] == len(lines_in)


def test_build_stage_layout(tmp_path):
    root = _layer(tmp_path)
    before = {name: (root / name).read_bytes() for name in INPUTS}
    args = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2", "--stage", "layout"]
    assert build.main(args) == 0
    assert {name: (root / name).read_bytes() for name in INPUTS} == before
    stats = json.loads((root / "layout" / "stats.json").read_text())
    params = json.loads((root / "layout" / "params.json").read_text())
    assert set(stats) == {"9999"} and params["overlap_threshold"] == 0.5
    assert params["column_merge_guard"] is True and stats["9999"]["n_column_blocked"] == 0
    header = gzip.open(root / "layout" / "scans" / "9999.tsv.gz", "rt").readline().split()
    assert header[-1] == "n_regions_resorted"
    manifest = json.loads((root / "manifest.json").read_text())
    assert "layout" in manifest["stages"]
    first = (root / "layout" / "regions" / "9999.tsv.gz").read_bytes()
    assert build.main(args) == 0
    assert (root / "layout" / "regions" / "9999.tsv.gz").read_bytes() == first


def test_preset_layout_params_and_cli_override(tmp_path):
    root = _layer(tmp_path)
    args = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2", "--stage", "layout"]
    assert build.main(args) == 0
    params = json.loads((root / "layout" / "params.json").read_text())
    assert params["furniture_patterns"] == list(PRESET.layout_params["furniture_patterns"])
    assert params["merge"] is False and params["interleave_guard"] is True   # hand: merges off (15fq)
    assert PRESETS["sg_1.10.94_v2"].layout_params.get("merge", True) is True
    assert build.main(args + ["--layout-params", '{"merge": true, "furniture_patterns": []}']) == 0
    params = json.loads((root / "layout" / "params.json").read_text())
    assert params["merge"] is True and params["furniture_patterns"] == []


def test_legacy_matches_default_on_fixture(tmp_path):
    from archival_structures.analysis.page_structure import LEGACY_15FP, LayoutParams
    root = _layer(tmp_path)
    tables = {}
    for name, params in (("default", LayoutParams()), ("legacy", LEGACY_15FP)):
        layout_inventory("9999", root, params)
        tables[name] = {t: list(read_tsv_gz(root / "layout" / t / "9999.tsv.gz"))
                        for t in ("scans", "regions", "lines")}
    assert tables["default"] == tables["legacy"]       # the fixture has no 15fq-specific case
    assert all(r["furniture"] == "0" for r in tables["legacy"]["regions"])


# ---- synthetic print layer (step 15ir) -------------------------------------------------------

SCAN = "NL-HaNA_1.10.94_9998_0001"


def _synthetic_layer(tmp_path, regions):
    """``regions``: {region_id: [(line_id, x, y, w, text, column)]}; one portrait scan 1000x1600."""
    from archival_structures.text_layer.extract_v2 import (
        LINE_COLUMNS_V2, REGION_COLUMNS_V2, SCAN_COLUMNS_V2)
    from archival_structures.text_layer.io import write_tsv_gz
    root = tmp_path / "synthetic"
    scan = {c: "" for c in SCAN_COLUMNS_V2}
    scan.update(scan_id=SCAN, page_w="1000", page_h="1600", status="ok")
    write_tsv_gz(root / "scans" / "9998.scans.tsv.gz", SCAN_COLUMNS_V2,
                 [[scan[c] for c in SCAN_COLUMNS_V2]])
    reg_rows, line_rows = [], []
    for rid, lines in regions.items():
        reg = {c: "" for c in REGION_COLUMNS_V2}
        reg.update(scan_id=SCAN, region_id=rid, region_tag="paragraph")
        reg_rows.append([reg[c] for c in REGION_COLUMNS_V2])
        for idx, (lid, x, y, w, text, column) in enumerate(lines):
            row = {c: "" for c in LINE_COLUMNS_V2}
            row.update(scan_id=SCAN, column=column, region_id=rid, line_idx=str(idx), line_id=lid,
                       x=str(x), y=str(y), w=str(w), h="40",
                       baseline=f"{x},{y + 35} {x + w},{y + 35}", text=text, text_raw=text)
            line_rows.append([row[c] for c in LINE_COLUMNS_V2])
    write_tsv_gz(root / "regions_meta" / "9998.tsv.gz", REGION_COLUMNS_V2, reg_rows)
    write_tsv_gz(root / "lines" / "9998.lines.tsv.gz", LINE_COLUMNS_V2, line_rows)
    return root


def test_merge_off(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    regions = {"a": [(f"a{i}", 100, 100 + 50 * i, 800, "tekst tekst", "") for i in range(6)],
               "b": [("b0", 100, 175, 800, "ertussen", "")]}
    root = _synthetic_layer(tmp_path, regions)
    on = layout_inventory("9998", root, LayoutParams(merge=True))
    assert on["n_merges"] == 1
    assert [r["change"] for r in read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz")] == ["merged"]
    off = layout_inventory("9998", root, LayoutParams(merge=False))
    rows = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert off["n_merges"] == 0 and len(rows) == 2
    assert all("merged" not in r["change"] for r in rows)


def test_print_column_split_and_furniture(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    interleaved = ([("c1a", 100, 300, 350, "links een", "1"), ("c2a", 550, 100, 350, "rechts een", "2"),
                    ("c1b", 100, 100, 350, "links twee", "1"), ("c2b", 550, 200, 350, "rechts twee", "2")])
    regions = {"page": interleaved, "catch": [("k0", 100, 1500, 120, "Gesien", "")]}
    root = _synthetic_layer(tmp_path, regions)
    layout_inventory("9998", root, LayoutParams(furniture_patterns=()))
    rows = {r["layout_region_id"]: r for r in read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz")}
    split = [r for r in rows.values() if r["change"] == "split_at_column"]
    assert sorted(r["column_value"] for r in split) == ["1", "2"]
    assert rows["catch"]["furniture"] == "1" and rows["catch"]["change"] == "unchanged"
    assert all(r["furniture"] == "0" for r in split)
    lines = list(read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz"))
    by_region = {}
    for l in lines:
        by_region.setdefault(l["layout_region_id"], []).append((int(l["layout_line_idx"]), l["line_id"]))
    left = next(r["layout_region_id"] for r in split if r["column_value"] == "1")
    assert [lid for _, lid in sorted(by_region[left])] == ["c1b", "c1a"]      # y order, not XML order
    # legacy parameters keep the source order
    from archival_structures.analysis.page_structure import LEGACY_15FP
    legacy = LayoutParams(**{**LEGACY_15FP.to_dict(), "split_on_line_field": "column",
                             "furniture_patterns": ()})
    assert legacy.sort_split_lines is False
    layout_inventory("9998", root, legacy)
    lines = list(read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz"))
    first = {l["layout_region_id"]: l["line_id"] for l in lines if l["layout_line_idx"] == "0"}
    assert "c1a" in first.values()


def test_sort_unchanged_lines(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    # one region whose XML order is out of y order; the last two lines share a row
    regions = {"a": [("a0", 100, 300, 800, "drie", ""), ("a1", 500, 100, 300, "twee", ""),
                     ("a2", 100, 104, 300, "een", "")]}
    root = _synthetic_layer(tmp_path, regions)
    st = layout_inventory("9998", root, LayoutParams(sort_unchanged_lines=True))
    reg = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert len(reg) == 1 and reg[0]["change"] == "unchanged" and reg[0]["layout_region_id"] == "a"
    assert reg[0]["lines_resorted"] == "1" and st["n_regions_resorted"] == 1
    idx = {l["line_id"]: int(l["layout_line_idx"])
           for l in read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz")}
    assert idx == {"a2": 0, "a1": 1, "a0": 2}
    scans = list(read_tsv_gz(root / "layout" / "scans" / "9998.tsv.gz"))
    assert scans[0]["n_regions_resorted"] == "1"
    st = layout_inventory("9998", root, LayoutParams(sort_unchanged_lines=False))
    reg = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert reg[0]["lines_resorted"] == "0" and st["n_regions_resorted"] == 0
    idx = {l["line_id"]: int(l["layout_line_idx"])
           for l in read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz")}
    assert idx == {"a0": 0, "a1": 1, "a2": 2}


def test_print_preset_sorts_unchanged():
    assert PRESETS["sg_1.10.94_v2"].layout_params["sort_unchanged_lines"] is True
    assert "sort_unchanged_lines" not in PRESETS["sg_1.01.02_v2"].layout_params


def test_hand_column_split_layout_scan(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    lines = []
    for i in range(6):   # XML order alternates between the two columns
        lines.append((f"l{i}", 50, 100 + 50 * i, 400, "links", ""))
        lines.append((f"r{i}", 550, 100 + 50 * i, 400, "rechts", ""))
    root = _synthetic_layer(tmp_path, {"a": lines})
    layout_inventory("9998", root, LayoutParams())
    rows = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert [r["change"] for r in rows] == ["unchanged"]
    st = layout_inventory("9998", root, LayoutParams(hand_column_split={}))
    rows = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert len(rows) == 2 and all(r["change"] == "split_at_column" for r in rows)
    assert len({r["layout_column"] for r in rows}) == 2
    assert len({r["layout_region_id"] for r in rows}) == 2
    by_region = {}
    for l in read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz"):
        by_region.setdefault(l["layout_region_id"], []).append((int(l["layout_line_idx"]), l["line_id"]))
    got = sorted([lid for _, lid in sorted(v)] for v in by_region.values())
    assert got == [[f"l{i}" for i in range(6)], [f"r{i}" for i in range(6)]]


def test_hand_column_reorder_layout_scan(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    lines = []
    for i in range(6):   # XML order alternates between the two columns
        lines.append((f"l{i}", 50, 100 + 50 * i, 400, "links", ""))
        lines.append((f"r{i}", 550, 100 + 50 * i, 400, "rechts", ""))
    root = _synthetic_layer(tmp_path, {"a": lines})
    layout_inventory("9998", root, LayoutParams(hand_column_split={"mode": "reorder"}))
    rows = list(read_tsv_gz(root / "layout" / "regions" / "9998.tsv.gz"))
    assert len(rows) == 1 and rows[0]["change"] == "column_reordered"
    got = [l["line_id"] for l in sorted(read_tsv_gz(root / "layout" / "lines" / "9998.tsv.gz"),
                                        key=lambda l: int(l["layout_line_idx"]))]
    assert got == [f"l{i}" for i in range(6)] + [f"r{i}" for i in range(6)]


def test_index_sides_pages_table(tmp_path):
    from archival_structures.analysis.page_structure import LayoutParams
    from archival_structures.text_layer.layout import PAGE_COLUMNS
    lines = []
    for i in range(20):   # two columns of long lines ending in numbers
        lines.append((f"l{i}", 50, 100 + 50 * i, 400, "lemma entry some text 12", ""))
        lines.append((f"r{i}", 550, 100 + 50 * i, 400, "lemma entry more text 34", ""))
    root = _synthetic_layer(tmp_path, {"a": lines})
    st = layout_inventory("9998", root, LayoutParams())
    assert not (root / "layout" / "pages").exists() and "n_index_sides" not in st
    st = layout_inventory("9998", root, LayoutParams(index_sides={}))
    assert st["n_index_sides"] == 1
    path = root / "layout" / "pages" / "9998.tsv.gz"
    with gzip.open(path, "rt") as fh:
        assert fh.readline().rstrip("\n").split("\t") == PAGE_COLUMNS
    rows = list(read_tsv_gz(path))
    assert len(rows) == 1
    r = rows[0]
    assert r["scan_id"] == SCAN and r["n_lines"] == "40" and r["n_text20"] == "40"
    assert r["col_share"] == "0.5" and r["end_num"] == "1.0" and r["rule"] == "1" and r["index_side"] == "1"


def test_index_sides_pages_table_scan_order(tmp_path):
    """Sides follow the scans table, not a parsed scan number: suffixed, lettered and multi-part scan ids
    (3548_0003A, 7364A_R, 5751B_P001, 5745A/B) crashed or interleaved the parts (v5 full run, 2026-10-29)."""
    from archival_structures.analysis.page_structure import LayoutParams
    from archival_structures.text_layer.extract_v2 import (
        LINE_COLUMNS_V2, REGION_COLUMNS_V2, SCAN_COLUMNS_V2)
    from archival_structures.text_layer.io import write_tsv_gz
    scan_ids = [f"NL-HaNA_1.01.02_9998{s}" for s in
                ("A_R", "A_V", "A_0001", "A_0002", "B_0001", "B_P001", "_0003", "_0003A", "_0004_deelopname1")]
    root = tmp_path / "multi"
    scan_rows, reg_rows, line_rows = [], [], []
    for sid in scan_ids:
        scan = {c: "" for c in SCAN_COLUMNS_V2}
        scan.update(scan_id=sid, page_w="1000", page_h="1600", status="ok")
        scan_rows.append([scan[c] for c in SCAN_COLUMNS_V2])
        reg = {c: "" for c in REGION_COLUMNS_V2}
        reg.update(scan_id=sid, region_id=f"{sid}-a", region_tag="paragraph")
        reg_rows.append([reg[c] for c in REGION_COLUMNS_V2])
        for i in range(3):
            row = {c: "" for c in LINE_COLUMNS_V2}
            row.update(scan_id=sid, region_id=f"{sid}-a", line_idx=str(i), line_id=f"{sid}-l{i}",
                       x="100", y=str(100 + 50 * i), w="800", h="40",
                       baseline=f"100,{135 + 50 * i} 900,{135 + 50 * i}", text="tekst tekst", text_raw="tekst tekst")
            line_rows.append([row[c] for c in LINE_COLUMNS_V2])
    write_tsv_gz(root / "scans" / "9998.scans.tsv.gz", SCAN_COLUMNS_V2, scan_rows)
    write_tsv_gz(root / "regions_meta" / "9998.tsv.gz", REGION_COLUMNS_V2, reg_rows)
    write_tsv_gz(root / "lines" / "9998.lines.tsv.gz", LINE_COLUMNS_V2, line_rows)
    layout_inventory("9998", root, LayoutParams(index_sides={}))
    rows = list(read_tsv_gz(root / "layout" / "pages" / "9998.tsv.gz"))
    assert [r["scan_id"] for r in rows] == scan_ids
