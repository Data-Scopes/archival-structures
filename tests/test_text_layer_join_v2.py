"""Tests for v2 word breaks and unit text with offsets (step 15hj)."""

import json
import shutil
from pathlib import Path

import pytest

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.join import _make_line, join_region, library_join
from archival_structures.text_layer.join_v2 import JOIN_SEGMENT_COLUMNS, join_unit
from archival_structures.text_layer.word_breaks import robust_counts, word_break_stats

WBC = ["-", "„"]


def test_line_ranges_semantics():
    """What make_text_region_text's ranges cover: a merged line stops before the dropped break char
    and the next line starts right there; a plain line includes its separating space."""
    from pagexml.helper.pagexml_helper import make_text_region_text
    text, ranges = make_text_region_text([_make_line(t, i) for i, t in enumerate(["Cor-", "respondentie"])],
                                         word_break_chars={"-"})
    assert text == "Correspondentie"
    assert [(r["start"], r["end"]) for r in ranges] == [(0, 3), (3, 15)]
    assert text[0:3] == "Cor"
    text, ranges = make_text_region_text([_make_line(t, i) for i, t in enumerate(["aap", "noot"])],
                                         word_break_chars={"-"})
    assert text == "aap noot"
    assert [(r["start"], r["end"]) for r in ranges] == [(0, 4), (4, 8)]
    assert text[0:4] == "aap "   # the plain range includes the space; join_unit trims it


CASES = [
    ["aap", "noot", "mies"],
    ["Cor-", "respondentie"],
    ["Cor-", "respondentie", "van", "den-", "haag"],
    ["vande„", "„selve", "meer"],
    ["vande„", "„selve"],
    ["8835„", "5„"],                      # g_digit: money notation stays apart
    ["8835„", "5„", "tesamen"],
    ["Heer -", "lijk"],                   # g_standalone
    ["-", "en dus"],
    ["woord-", "12 stuks"],               # g_digit_right
    ["f 12„", "tot"],                     # g_digit_left
    ["(wel)-", "gemeend", "hoe-", "wel"],  # g_punct_left then merge
    ["Cor-", "-respondentie"],            # leading break char is kept without the fix? (fix removes it)
    ["een", "twee-", "„drie", "vier"],
    ["x"],
    ["Eerste regel-", "tweede„", "„derde-", "vierde"],
]


@pytest.mark.parametrize("texts", CASES)
def test_join_unit_equals_join_region(texts):
    text, spans, bounds = join_unit(texts, WBC)
    assert text == join_region(texts, WBC)
    assert len(spans) == len(bounds) == len(texts)
    for i, (cs, ce, lead) in enumerate(spans):
        seg = texts[i][lead:]
        piece = text[cs:ce]
        assert seg.startswith(piece), (texts, i, piece)
        removed = seg[len(piece):]
        if bounds[i][0] == "merge":
            assert removed and set(removed) <= set(WBC)
            assert text[ce:ce + 1] != " " or ce == len(text)
            assert spans[i + 1][0] == ce
        else:
            assert removed == ""
            if i + 1 < len(texts):
                assert text[ce:spans[i + 1][0]] == " "
    assert spans[-1][1] == len(text) or texts[-1] == ""


def test_join_unit_known_values():
    text, spans, bounds = join_unit(["vande„", "„selve", "meer"], WBC)
    assert text == "vandeselve meer"
    assert spans == [(0, 5, 0), (5, 10, 1), (11, 15, 0)]
    assert bounds == [("merge", "„"), ("plain", ""), ("", "")]
    text, spans, bounds = join_unit(["8835„", "5„"], WBC)
    assert text == "8835„ 5„" and [b[0] for b in bounds] == ["g_digit", ""]
    assert spans == [(0, 5, 0), (6, 8, 0)]


@pytest.mark.parametrize("texts", [c for c in CASES if len(c) > 1])
def test_join_unit_no_guards_equals_library_join(texts):
    text, spans, bounds = join_unit(texts, WBC, guards=(), both_ends_fix=False)
    assert text == library_join(texts, WBC)


def test_robust_counts():
    lines = ["„„selve", "8835„5„", "vande„", "a„b", "5 „ 5", "1„„2"]
    got = robust_counts(lines, {"„"})
    # "„„selve": run of 2 at start (2); "8835„5„": the first „ is between digits (1);
    # "vande„", "a„b", "5 „ 5": none; "1„„2": both between digits ignoring adjacent „ (2)
    assert got == {"„": 5}


def test_word_break_stats_v1_columns_unchanged_by_robust():
    regions = [["abc-", "def"] * 20, ["f 8835„5„", "zeven"] * 20]
    rows, _ = word_break_stats(regions, "X")
    rows_r, _ = word_break_stats(regions, "X", robust=True)
    assert all("all_freq_robust" not in r for r in rows)
    for a, b in zip(rows, rows_r):
        assert {k: v for k, v in b.items() if k in a} == a
    assert any(r["all_freq_robust"] < r["all_freq"] for r in rows_r if r["char"] == "„")


# ---------------------------------------------------------------- the v2 chain

SG_FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"


def _v2_pagexml(tmp_path):
    root = tmp_path / "pagexml"
    shutil.copytree(SG_FIXTURE, root / "NL-HaNA_1.01.02_9999")
    return root


def test_v2_all_chain(tmp_path):
    out = tmp_path / "out"
    base = ["--pagexml-dir", str(_v2_pagexml(tmp_path)), "--out-dir", str(out),
            "--preset", "sg_1.01.02_v2", "--workers", "1"]
    assert build.main(base + ["--stage", "all"]) == 0
    root = out / "NL-HaNA_1.01.02_text_layer_v2"
    for d in ("lines", "layout", "types", "units", "wbc", "join"):
        assert (root / d).is_dir(), d
    assert (root / "wbc" / "wbc_per_inventory.tsv").exists()
    units = list(read_tsv_gz(root / "join" / "units" / "9999.tsv.gz"))
    segs = list(read_tsv_gz(root / "join" / "segments" / "9999.tsv.gz"))
    assert units and segs and list(segs[0]) == JOIN_SEGMENT_COLUMNS
    text_of = {(u["scan_id"], u["unit_id"]): u["text"] for u in units}
    lines = {(r["scan_id"], r["line_id"]): r["text"]
             for r in read_tsv_gz(root / "lines" / "9999.lines.tsv.gz")}
    cuts = {(r["scan_id"], r["line_id"], r["seg_idx"]): int(r["char_start"])
            for r in read_tsv_gz(root / "types" / "segments" / "9999.tsv.gz")}
    for s in segs:
        cs, ce, off = int(s["char_start"]), int(s["char_end"]), int(s["line_offset"])
        unit_text = text_of[(s["scan_id"], s["unit_id"])]
        line = lines[(s["scan_id"], s["line_id"])]
        assert unit_text[cs:ce] == line[off:off + (ce - cs)]
        assert off >= cuts[(s["scan_id"], s["line_id"], s["seg_idx"])]
    stats = json.loads((root / "join" / "stats" / "9999.json").read_text())
    assert stats["n_offset_violations"] == 0
    manifest = json.loads((root / "manifest.json").read_text())
    assert {"wbc", "join", "units"} <= set(manifest["stages"]) and "history" not in manifest
