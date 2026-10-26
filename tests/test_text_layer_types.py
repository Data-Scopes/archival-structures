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


# ---- step 15ir ---------------------------------------------------------------------------------

def _page_layer(tmp_path, numbers, bottom_extra=None):
    """A synthetic v2 layer: one portrait scan per entry of ``numbers`` with a body of six lines,
    the number as top line and, for scans in ``bottom_extra``, a second number-like bottom line."""
    from archival_structures.analysis.page_structure import LayoutParams
    from archival_structures.text_layer.extract_v2 import (
        LINE_COLUMNS_V2, REGION_COLUMNS_V2, SCAN_COLUMNS_V2)
    from archival_structures.text_layer.io import write_tsv_gz
    from archival_structures.text_layer.layout import layout_inventory
    root = tmp_path / "chain"
    scans, regs, lines = [], [], []
    for n, number in enumerate(numbers):
        sid = f"NL-HaNA_1.01.02_9997_{n + 1:04d}"
        scan = {c: "" for c in SCAN_COLUMNS_V2}
        scan.update(scan_id=sid, page_w="1000", page_h="1600", status="ok")
        scans.append([scan[c] for c in SCAN_COLUMNS_V2])
        specs = [("num", 450, 60, 80, str(number))]
        specs += [(f"b{k}", 300, 200 + 60 * k, 500, "tekst van de regel nummer") for k in range(6)]
        if bottom_extra and n in bottom_extra:
            specs.append(("bot", 450, 1400, 80, bottom_extra[n]))
        for k, (name, x, y, w, text) in enumerate(specs):
            rid = f"{sid}-text_region-{x}-{y}-{w}-40"
            reg = {c: "" for c in REGION_COLUMNS_V2}
            reg.update(scan_id=sid, region_id=rid, region_tag="paragraph")
            regs.append([reg[c] for c in REGION_COLUMNS_V2])
            row = {c: "" for c in LINE_COLUMNS_V2}
            row.update(scan_id=sid, region_id=rid, line_idx="0", line_id=f"{sid}-{name}", x=str(x),
                       y=str(y), w=str(w), h="40", baseline=f"{x},{y + 35} {x + w},{y + 35}",
                       text=text, text_raw=text)
            lines.append([row[c] for c in LINE_COLUMNS_V2])
    write_tsv_gz(root / "scans" / "9997.scans.tsv.gz", SCAN_COLUMNS_V2, scans)
    write_tsv_gz(root / "regions_meta" / "9997.tsv.gz", REGION_COLUMNS_V2, regs)
    write_tsv_gz(root / "lines" / "9997.lines.tsv.gz", LINE_COLUMNS_V2, lines)
    layout_inventory("9997", root, LayoutParams(furniture_patterns=(), merge=False))
    return root


def test_chain_relabel(tmp_path):
    from archival_structures.text_layer.types import type_inventory
    root = _page_layer(tmp_path, [10, 99, 11, 12], bottom_extra={0: "7."})
    stats = type_inventory("9997", root, _params())
    assert stats["chain_length"] == 3 and stats["chain_candidates"] == 4
    got = {(s["scan_id"][-4:], s["line_id"].rsplit("-", 1)[1]): s["type_method"]
           for s in read_tsv_gz(root / "types" / "segments" / "9997.tsv.gz")
           if s["line_type"] == "page_number"}
    assert got == {("0001", "num"): "rule:pn_line", ("0001", "bot"): "rule:pn_line_offchain",
                   ("0002", "num"): "rule:pn_line_offchain", ("0003", "num"): "rule:pn_line",
                   ("0004", "num"): "rule:pn_line"}


def test_date_line_with_year_not_split():
    segs, splits = _segment([_line(0, "28. Junij 1650.", 300, 100, 300)] + _body())
    got = _of(segs, "l0")
    assert len(got) == 1 and splits["rule:date_prefix"] == 0
    assert (got[0]["line_type"], got[0]["type_method"]) == ("date_header", "rule:date_line")
    assert (got[0]["char_start"], got[0]["char_end"]) == (0, len("28. Junij 1650."))
    # a date followed by running text is still split by rule 1
    segs, splits = _segment([_line(0, "28. Junij verthoonen, als het sullen", 300, 100, 600)] + _body())
    assert splits["rule:date_prefix"] == 1


def test_date_line_colon_form():
    # row 55: "20: Decemb:" is one date_header segment
    segs, splits = _segment([_line(0, "20: Decemb:", 300, 100, 200)] + _body())
    got = _of(segs, "l0")
    assert len(got) == 1 and got[0]["line_type"] == "date_header"
    assert splits["rule:date_prefix"] == 0


def test_types_params_validated():
    import pytest
    for kw in ({"margin_cut": "some"}, {"top_n": 0}, {"bottom_n": 0},
               {"offchain_other_bands": ("middle",)}):
        with pytest.raises(ValueError):
            TypeParams(**kw)
    for mode in ("month_only", "all", "off"):
        TypeParams(margin_cut=mode)


# ---- step 15k ----------------------------------------------------------------------------------

def _pn_methods(tmp_path, numbers, bottom_extra, **kw):
    from archival_structures.text_layer.types import type_inventory
    root = _page_layer(tmp_path, numbers, bottom_extra=bottom_extra)
    type_inventory("9997", root, _params(**kw))
    return {(s["scan_id"][-4:], s["line_id"].rsplit("-", 1)[1]): (s["line_type"], s["type_method"])
            for s in read_tsv_gz(root / "types" / "segments" / "9997.tsv.gz")
            if s["type_method"].startswith("rule:pn_line") or s["type_method"] == "rule:year_offchain"}


def test_offchain_gap_kept(tmp_path):
    got = _pn_methods(tmp_path, [10, 11, 99, 13, 14], {2: "( 12 )"}, offchain_retype=True)
    assert got[("0003", "bot")] == ("page_number", "rule:pn_line_gap")
    assert got[("0003", "num")] == ("page_number", "rule:pn_line_offchain")   # 99: not in the gap
    assert got[("0002", "num")] == ("page_number", "rule:pn_line")


def test_offchain_band_retyped(tmp_path):
    numbers, extra = [10, 99, 11, 12], {0: "41"}
    got = _pn_methods(tmp_path, numbers, extra, offchain_retype=True, offchain_other_bands=("bottom",))
    assert got[("0001", "bot")] == ("other", "rule:pn_line_offchain")
    assert got[("0002", "num")] == ("page_number", "rule:pn_line_offchain")
    got = _pn_methods(tmp_path / "off", numbers, extra, offchain_retype=False,
                      offchain_other_bands=("bottom",))
    assert got[("0001", "bot")] == ("page_number", "rule:pn_line_offchain")
    assert got[("0002", "num")] == ("page_number", "rule:pn_line_offchain")


def test_bottom_year_offchain(tmp_path):
    got = _pn_methods(tmp_path, [10, 11, 12], {0: "1717."}, offchain_retype=True,
                      offchain_years_other=True)
    assert got[("0001", "bot")] == ("other", "rule:year_offchain")
    got = _pn_methods(tmp_path / "off", [10, 11, 12], {0: "1717."}, offchain_retype=True,
                      offchain_years_other=False)
    assert got[("0001", "bot")] == ("page_number", "rule:pn_line_offchain")


# ---- full-run fix: parse_error scans -----------------------------------------------------------

def test_stages_survive_parse_error_scan(tmp_path):
    import shutil
    from archival_structures.text_layer.extract_v2 import extract_inventory_v2
    fixture = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
    preset = PRESETS["sg_1.01.02_v2"]
    base_args = ["--preset", "sg_1.01.02_v2"]

    ref_root = tmp_path / "ref" / preset.name
    extract_inventory_v2("9999", fixture, ref_root, preset)
    assert build.main(["--out-dir", str(tmp_path / "ref")] + base_args + ["--stage", "layout"]) == 0
    assert build.main(["--out-dir", str(tmp_path / "ref")] + base_args + ["--stage", "types"]) == 0
    ref_segs = [dict(r) for r in read_tsv_gz(ref_root / "types" / "segments" / "9999.tsv.gz")]

    bad = tmp_path / "bad_src"
    shutil.copytree(fixture, bad)
    (bad / "doc.xml").write_text(
        '<?xml version="1.0"?><mets xmlns="http://www.loc.gov/METS/"><x/></mets>')
    out = tmp_path / "bad"
    root = out / preset.name
    result = extract_inventory_v2("9999", bad, root, preset)
    assert result["n_errors"] == 1
    scans = list(read_tsv_gz(root / "scans" / "9999.scans.tsv.gz"))
    errs = [r for r in scans if r["status"] == "parse_error"]
    assert len(errs) == 1 and errs[0]["page_w"] == ""
    base = ["--out-dir", str(out)] + base_args
    for stage in ("layout", "types", "units", "wbc", "join"):
        assert build.main(base + ["--stage", stage]) == 0, stage
    segs = [dict(r) for r in read_tsv_gz(root / "types" / "segments" / "9999.tsv.gz")]
    assert segs == ref_segs
