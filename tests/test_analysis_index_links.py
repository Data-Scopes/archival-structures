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
