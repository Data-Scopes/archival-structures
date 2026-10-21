"""Tests for the v2 text-layer types stage (step 15g). Synthetic rows plus the sg/9999 fixture."""

import json
from pathlib import Path

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import PRESETS
from archival_structures.text_layer.types import (
    TYPE_REGION_COLUMNS, TYPE_SEGMENT_COLUMNS, TYPES, Line, TypeParams, _compile,
    _page_context, segment_page)

PW = 1000
MED = 0.5


def _params(**kw):
    return TypeParams.from_dict({**PRESETS["sg_1.01.02_v2"].types_params, **kw})


def _line(i, text, x, y, w, role="main", col="0"):
    row = {"scan_id": "S", "line_id": f"l{i}", "text": text, "x": x, "y": y, "w": w, "h": 30}
    lay = {"page": "recto", "layout_region_id": "R", "layout_role": role, "layout_column": col}
    return Line(row, lay, i)


def _body(start=1, n=8, y0=200):
    """n full-width main lines with left edge 300 (main left 300, centre 550)."""
    return [_line(start + k, "tekst van de regel nummer", 300, y0 + 60 * k, 500) for k in range(n)]


def _segment(lines, params=None, chain_ok=False, index_page=False):
    params = params or _params()
    ordered, ref, cols = _page_context(lines, PW, MED, params)
    segs, splits = segment_page(ordered, PW, MED, ref, cols, params, _compile(params), chain_ok,
                                index_page)
    return segs, splits


def _of(segs, line_id):
    return [s for s in segs if s["line_id"] == line_id]


def test_date_prefix_and_pn_suffix_split():
    text = "28. Junij verthoonen, als het sullen requireren. 909."
    top = _line(0, text, 300, 100, 600)
    segs, splits = _segment([top] + _body(), chain_ok=True)
    got = _of(segs, "l0")
    assert [(s["line_type"], text[s["char_start"]:s["char_end"]]) for s in got] == [
        ("date_header", "28. Junij"), ("main_text", "verthoonen, als het sullen requireren."),
        ("page_number", "909.")]
    assert [s["type_method"] for s in got] == ["rule:date_prefix", "rule:default", "rule:pn_suffix"]
    assert got[0]["x"] == 300 and got[0]["geom_method"] == "char_proportion"
    assert got[1]["x"] == 300 + round(600 * 10 / len(text))
    assert splits["rule:date_prefix"] == 1 and splits["rule:pn_suffix"] == 1


def test_pn_suffix_not_in_chain_not_split():
    top = _line(0, "28. Junij verthoonen, als het sullen requireren. 909.", 300, 100, 600)
    segs, _ = _segment([top] + _body(), chain_ok=False)
    got = _of(segs, "l0")
    assert [s["line_type"] for s in got] == ["date_header", "main_text"]
    assert got[1]["char_end"] == len(top.text)


def test_margin_prefix_month_only():
    # line starts left of main left (300); "Augt." ends near x=300, the text starts there
    line = _line(0, "Augt. haer Ho:Mog: geliefden", 150, 300, 840)
    segs, splits = _segment(_body() + [line])
    got = _of(segs, "l0")
    assert [s["line_type"] for s in got][:2] == ["date_header", "main_text"]
    assert got[0]["type_method"] == "rule:margin_prefix" and splits["rule:margin_prefix"] == 1
    assert line.text[got[0]["char_start"]:got[0]["char_end"]] == "Augt."


def test_margin_prefix_other_not_cut():
    line = _line(0, "Diesbagh en dan de rest van de regel hier", 100, 300, 700)
    segs, splits = _segment(_body() + [line])
    got = _of(segs, "l0")
    assert len(got) == 1 and splits["rule:margin_prefix"] == 0
    assert (got[0]["char_start"], got[0]["char_end"]) == (0, len(line.text))
    segs, _ = _segment(_body() + [line], _params(margin_cut="all"))
    got = _of(segs, "l0")
    assert got[0]["line_type"] == "marginalia" and len(got) == 2


def test_margin_cut_off():
    line = _line(0, "Augt. haer Ho:Mog: geliefden", 150, 300, 840)
    segs, splits = _segment(_body() + [line], _params(margin_cut="off"))
    assert len(_of(segs, "l0")) == 1 and splits["rule:margin_prefix"] == 0


def test_date_line_with_weekday():
    segs, _ = _segment(_body() + [_line(0, "Lunæ den 3. Januarii", 300, 330, 300)])
    assert _of(segs, "l0")[0]["type_method"] == "rule:date_line"
    segs, _ = _segment(_body() + [_line(0, "21. April", 300, 330, 100)])
    assert _of(segs, "l0")[0]["line_type"] == "date_header"


def test_page_margin_marginalia():
    segs, _ = _segment(_body() + [_line(0, "Nieuwe jaren", 80, 330, 150)])
    got = _of(segs, "l0")
    assert (got[0]["line_type"], got[0]["type_method"]) == ("marginalia", "rule:page_margin")
    segs, _ = _segment(_body() + [_line(0, "noot", 600, 330, 150, role="extra", col="1")])
    assert _of(segs, "l0")[0]["type_method"] == "rule:extra_role"


def test_catch_word_and_page_number_line():
    body = _body()
    segs, _ = _segment(body + [_line(0, "Heeren", 700, 800, 90)])
    assert _of(segs, "l0")[0]["line_type"] == "catch_word"
    segs, _ = _segment([_line(0, "( 152 )", 450, 100, 80)] + body)
    got = _of(segs, "l0")[0]
    assert (got["line_type"], got["type_method"]) == ("page_number", "rule:pn_line")
    segs, _ = _segment([_line(0, "1717.", 450, 100, 80)] + body)
    assert _of(segs, "l0")[0]["line_type"] == "running_header"


def test_print_column_override():
    top = _line(0, "( 152 )", 450, 100, 80)
    params = TypeParams.from_dict({**PRESETS["sg_1.10.94_v2"].types_params})
    assert params.column_override
    segs, _ = _segment([top] + _body(), params)
    assert _of(segs, "l0")[0]["column_override"] == "x"
    assert all(s["column_override"] == "" for s in segs if s["line_type"] == "main_text")
    segs, _ = _segment([top] + _body(), _params())
    assert _of(segs, "l0")[0]["column_override"] == ""


def test_every_text_line_has_segments_and_offsets_cover_text():
    lines = [_line(0, "21. April Is ter vergaderinge gelesen. 565.", 300, 100, 600),
             _line(1, "Augt. haer Ho:Mog: geliefden", 150, 160, 600),
             _line(2, "  ", 300, 190, 10)] + _body(3)
    lines = [l for l in lines if l.text.strip()]
    segs, _ = _segment(lines, chain_ok=True)
    for line in lines:
        got = _of(segs, line.line_id)
        assert got
        pieces = [line.text[s["char_start"]:s["char_end"]] for s in got]
        assert " ".join(" ".join(p.split()) for p in pieces) == " ".join(line.text.split())
        assert all(a["char_end"] <= b["char_start"] for a, b in zip(got, got[1:]))
    assert {s["line_type"] for s in segs} <= set(TYPES)


def test_build_stage_types(tmp_path):
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    root = tmp_path / preset.name
    extract_inventory_v2("9999", fixture, root, preset)
    base = ["--out-dir", str(tmp_path), "--preset", "sg_1.01.02_v2"]
    assert build.main(base + ["--stage", "layout"]) == 0
    lines_before = (root / "lines" / "9999.lines.tsv.gz").read_bytes()
    assert build.main(base + ["--stage", "types"]) == 0
    assert (root / "lines" / "9999.lines.tsv.gz").read_bytes() == lines_before
    segs = list(read_tsv_gz(root / "types" / "segments" / "9999.tsv.gz"))
    regions = list(read_tsv_gz(root / "types" / "regions" / "9999.tsv.gz"))
    assert list(segs[0]) == TYPE_SEGMENT_COLUMNS and list(regions[0]) == TYPE_REGION_COLUMNS
    texts = {(r["scan_id"], r["line_id"]): r["text"] for r in read_tsv_gz(root / "lines" / "9999.lines.tsv.gz")}
    nonblank = {k for k, t in texts.items() if t.strip()}
    assert {(s["scan_id"], s["line_id"]) for s in segs} == nonblank
    stats = json.loads((root / "types" / "stats.json").read_text())
    assert stats["9999"]["n_segments"] == len(segs)
    assert "types" in json.loads((root / "manifest.json").read_text())["stages"]
    assert json.loads((root / "types" / "params.json").read_text())["margin_cut"] == "month_only"
    first = (root / "types" / "segments" / "9999.tsv.gz").read_bytes()
    assert build.main(base + ["--stage", "types"]) == 0
    assert (root / "types" / "segments" / "9999.tsv.gz").read_bytes() == first
