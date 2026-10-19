"""Tests for the v2 text-layer layout stage (step 15fp). Fixture: tests/fixtures/text_layer/sg/9999."""

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
    manifest = json.loads((root / "manifest.json").read_text())
    assert "layout" in manifest["stages"]
    first = (root / "layout" / "regions" / "9999.tsv.gz").read_bytes()
    assert build.main(args) == 0
    assert (root / "layout" / "regions" / "9999.tsv.gz").read_bytes() == first
