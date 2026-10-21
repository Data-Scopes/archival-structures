"""Tests for the v2 text-layer extract (step 15d). Fixture: tests/fixtures/text_layer/sg/9999."""

import io
import shutil
import tarfile
import zipfile
from pathlib import Path

import pytest

from archival_structures.text_layer import build
from archival_structures.text_layer.extract_v2 import (
    LINE_COLUMNS_V2, REGION_COLUMNS_V2, SCAN_COLUMNS_V2, extract_inventory_v2, extract_scan_v2,
    legacy_doc_id, legacy_region_text)
from archival_structures.text_layer.io import (
    discover_inventories_v2, iter_pagexml, read_tsv_gz)
from archival_structures.text_layer.presets import PRESETS, SG_HAND_V2, SG_PRINT_V2, VOC_V1

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
NAME = "NL-HaNA_1.01.02_9999"


@pytest.fixture
def sources(tmp_path):
    """dir, tar.gz and zip versions of the fixture, each with resource-fork junk."""
    root = tmp_path / "src"
    root.mkdir()
    dir_ = root / f"{NAME}"
    shutil.copytree(FIXTURE, dir_)
    tgz = root / f"{NAME}-pagexml.tar.gz"
    with tarfile.open(tgz, "w:gz") as tf:
        for p in sorted(FIXTURE.glob("*.xml")):
            tf.add(p, arcname=f"9999/page/{p.name}")
        junk = b"\x00\x05\x16\x07binary"
        info = tarfile.TarInfo("9999/page/._NL-HaNA_1.01.02_9999_0001.xml")
        info.size = len(junk)
        tf.addfile(info, io.BytesIO(junk))
    zp = root / f"{NAME}.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        for p in sorted(FIXTURE.glob("*.xml")):
            zf.write(p, arcname=f"9999/{p.name}")
        zf.writestr("__MACOSX/9999/._NL-HaNA_1.01.02_9999_0001.xml", b"junk")
        zf.writestr("9999/._NL-HaNA_1.01.02_9999_0002.xml", b"junk")
    return root, dir_, tgz, zp


def _names(source, **kw):
    return [m.rsplit("/", 1)[-1] for m, _ in iter_pagexml(source, **kw)]


def test_iter_pagexml_targz_matches_dir(sources):
    _, dir_, tgz, zp = sources
    expected = sorted(p.name for p in FIXTURE.glob("*.xml"))
    assert _names(dir_) == expected
    assert _names(tgz) == expected
    assert _names(zp) == expected
    assert [d for _, d in iter_pagexml(tgz)] == [d for _, d in iter_pagexml(dir_)]


def test_skip_resource_forks(sources):
    _, dir_, tgz, zp = sources
    for src in (tgz, zp):
        assert _names(src, skip_resource_forks=True) == _names(src)
        assert not any(n.startswith("._") for n in _names(src, skip_resource_forks=True))
    assert len(_names(tgz, skip_resource_forks=True)) == 4


def test_discover_inventories_v2(sources, tmp_path):
    root, _, _, _ = sources
    (root / "README.txt").write_text("x")
    (root / "NL-HaNA_1.10.94_455.zip").write_bytes(b"")
    (root / ".hidden").write_text("")
    found = discover_inventories_v2(root, SG_HAND_V2.source_pattern)
    assert list(found) == ["9999"]
    assert found["9999"].is_file()  # an archive wins over the directory
    extra = tmp_path / "multi"
    extra.mkdir()
    for n in ("NL-HaNA_1.01.02_11182", "NL-HaNA_1.01.02_3349-pagexml.tar.gz",
              "NL-HaNA_1.01.02_3771.tgz", "NL-HaNA_1.01.02_9626A", "other_3349"):
        (extra / n).mkdir() if n.endswith(("11182", "9626A")) else (extra / n).write_bytes(b"")
    assert list(discover_inventories_v2(extra, SG_HAND_V2.source_pattern)) == [
        "3349", "3771", "9626A", "11182"]
    assert list(discover_inventories_v2(root, SG_PRINT_V2.source_pattern)) == ["455"]


def test_legacy_doc_id():
    box = {"x": 1, "y": 2, "w": 3, "h": 4}
    assert legacy_doc_id("NL-HaNA_1.01.02_3349_0004", box) == \
        "NL-HaNA_1.01.02_3349_0004-text_region-1-2-3-4"


def test_legacy_region_text():
    assert legacy_region_text(["abc„", "def"]) == "abcdef"
    assert legacy_region_text(["abc", "", "def"]) == "abc def"
    # a WBC at the end of the region has nothing to join to and stays
    assert legacy_region_text(["Eerste regel", "tweede„"]) == "Eerste regel tweede„"
    assert legacy_region_text(["x-"]) == "x-"
    assert legacy_region_text(["", ""]) == ""
    assert legacy_region_text([]) == ""


def _parse(name):
    from pagexml.parser import parse_pagexml_file
    return parse_pagexml_file(str(FIXTURE / name))


def _scan(name="NL-HaNA_1.01.02_9999_0001.xml"):
    return extract_scan_v2(_parse(name), inv="9999", inv_dir="9999", member=name, preset=SG_HAND_V2)


def test_extract_scan_v2_rows():
    scan, regions, lines = _scan()
    assert scan["scan_id"] == "NL-HaNA_1.01.02_9999_0001"
    assert (scan["page_w"], scan["page_h"], scan["inv_num"], scan["scan_num"]) == (2000, 1000, "9999", 1)
    assert scan["archive_id"] == "NL-HaNA_1.01.02" and scan["scan_id_ok"] == 1
    assert (scan["n_regions"], scan["n_regions_text"], scan["n_lines"], scan["n_lines_text"],
            scan["n_words"], scan["status"]) == (3, 2, 4, 4, 5, "ok")
    assert [set(r) for r in regions] == [set(REGION_COLUMNS_V2)] * 3
    assert [set(r) for r in lines] == [set(LINE_COLUMNS_V2)] * 4
    assert set(scan) == set(SCAN_COLUMNS_V2)
    r1, r2, r3 = regions
    assert r1["region_id"] == "NL-HaNA_1.01.02_9999_0001-text_region-100-100-800-300"
    assert (r1["region_ro_index"], r1["region_tag"], r1["page_side"]) == (1, "Text", "left")
    assert (r2["region_ro_index"], r2["region_tag"], r2["page_side"]) == ("", "untagged", "right")
    assert (r3["n_lines"], r3["n_lines_text"]) == (0, 0)  # the empty region is kept
    l1, l2, l3, l4 = lines
    assert (l1["x"], l1["y"], l1["w"], l1["h"]) == (100, 100, 800, 100)
    assert l1["baseline"] == "100,190 500,185 900,190"
    assert (l1["conf"], l2["conf"], l3["conf"], l4["conf"]) == ("0.987654", "0.5", "1", "")
    assert l4["baseline"] == "" and l4["page_side"] == "right"
    assert [l["line_idx"] for l in lines] == [0, 1, 2, 0]
    assert l2["text_raw"] == l2["text"] == "tweede„"
    assert (l1["column"], l1["edit_flags"], l1["split_from"]) == ("", "", "")


def test_scan_status_values(tmp_path):
    st = lambda n: _scan(n)[0]["status"]
    assert st("NL-HaNA_1.01.02_9999_0001.xml") == "ok"
    assert st("NL-HaNA_1.01.02_9999_0002.xml") == "no_regions"
    assert st("NL-HaNA_1.01.02_9999_0003.xml") == "no_words"
    row, regions, lines = _scan("NL-HaNA_1.01.02_9999_0004_deelopname1.xml")
    assert row["status"] == "partial_shot_dropped" and not regions and not lines
    assert row["scan_variant"] == "deelopname1" and row["scan_id_ok"] == 0
    assert row["inv_num"] == "9999" and row["scan_num"] == ""
    bad = tmp_path / "NL-HaNA_1.01.02_9999_0005.xml"
    bad.write_text("<PcGts><broken")
    row, regions, lines = extract_scan_v2(None, inv="9999", inv_dir="9999", member=str(bad),
                                          preset=SG_HAND_V2)
    assert row["status"] == "parse_error" and row["scan_id"] == "NL-HaNA_1.01.02_9999_0005"
    assert not regions and not lines
    # the same file through extract_inventory_v2
    d = tmp_path / "inv"
    d.mkdir()
    shutil.copy(bad, d / bad.name)
    extract_inventory_v2("9999", d, tmp_path / "o", SG_HAND_V2)
    rows = list(read_tsv_gz(tmp_path / "o" / "scans" / "9999.scans.tsv.gz"))
    assert [r["status"] for r in rows] == ["parse_error"]


def _tree(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_extract_inventory_v2_deterministic(sources, tmp_path):
    _, _, tgz, _ = sources
    extract_inventory_v2("9999", tgz, tmp_path / "a", SG_HAND_V2)
    extract_inventory_v2("9999", tgz, tmp_path / "b", SG_HAND_V2)
    ta, tb = _tree(tmp_path / "a"), _tree(tmp_path / "b")
    assert ta == tb and len(ta) == 4
    scans = list(read_tsv_gz(tmp_path / "a" / "scans" / "9999.scans.tsv.gz"))
    assert [r["status"] for r in scans] == ["ok", "no_regions", "no_words", "partial_shot_dropped"]
    lines = list(read_tsv_gz(tmp_path / "a" / "lines" / "9999.lines.tsv.gz"))
    assert len(lines) == 6  # 4 + 2 empty-text lines; the deelopname scan has none
    import json
    stats = json.loads((tmp_path / "a" / "extract_stats" / "9999.json").read_text())
    assert stats["status_counts"]["partial_shot_dropped"] == 1
    assert len(stats["source_sha256"]) == 64
    assert stats["n_duplicate_region_ids"] == 0
    extract_inventory_v2("9999", FIXTURE, tmp_path / "c", SG_HAND_V2)
    assert json.loads((tmp_path / "c" / "extract_stats" / "9999.json").read_text())["source_sha256"] is None


def test_scan_hook_metadata_propagates(tmp_path):
    seen = []

    def hook(scan):
        seen.append(scan.id)
        for region in scan.text_regions:
            for line in region.lines:
                line.metadata.update({"column": "2", "edit_flags": "split", "split_from": "x1"})
        return scan

    extract_inventory_v2("9999", FIXTURE, tmp_path, SG_HAND_V2, scan_hook=hook)
    assert not any("deelopname" in s for s in seen)  # dropped scans are not hooked
    assert len(seen) == 3
    lines = list(read_tsv_gz(tmp_path / "lines" / "9999.lines.tsv.gz"))
    assert {(r["column"], r["edit_flags"], r["split_from"]) for r in lines} == {("2", "split", "x1")}


def test_presets_voc_unchanged():
    assert (VOC_V1.name, VOC_V1.untagged_tag, VOC_V1.scan_id_prefix) == (
        "voc_text_layer_v1", "unclassified", "NL-HaNA_1.04.02")
    assert (VOC_V1.format_version, VOC_V1.archive_id, VOC_V1.source_pattern,
            VOC_V1.skip_resource_forks, VOC_V1.merged_line_split) == (1, None, None, False, "off")
    assert VOC_V1.output_bytes_per_xml_byte == 0.0233
    assert PRESETS["sg_1.01.02_v2"] is SG_HAND_V2 and PRESETS["sg_1.10.94_v2"] is SG_PRINT_V2
    assert (SG_HAND_V2.format_version, SG_HAND_V2.merged_line_split, SG_HAND_V2.name) == (
        2, "off", "NL-HaNA_1.01.02_text_layer_v2")
    assert (SG_PRINT_V2.merged_line_split, SG_PRINT_V2.scan_id_prefix) == ("on", "NL-HaNA_1.10.94")


def test_build_v2_cli_all_runs_the_chain(sources, tmp_path):
    root, _, _, _ = sources
    out = tmp_path / "out"
    args = ["--pagexml-dir", str(root), "--out-dir", str(out), "--preset", "sg_1.01.02_v2"]
    assert build.main(args + ["--stage", "all", "--workers", "1"]) == 0
    layer = out / "NL-HaNA_1.01.02_text_layer_v2"
    assert (layer / "scans" / "9999.scans.tsv.gz").exists()
    assert not (layer / "regions").exists()   # v1 output; v2 has join/ instead
    assert (layer / "join" / "units" / "9999.tsv.gz").exists()
    import json
    manifest = json.loads((layer / "manifest.json").read_text())
    assert manifest["layer"] == "NL-HaNA_1.01.02_text_layer_v2" and manifest["format_version"] == 2
    assert {"extract", "layout", "types", "units", "wbc", "join"} <= set(manifest["stages"])
