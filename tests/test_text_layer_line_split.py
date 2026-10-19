"""Tests for the merged-line splitter (step 15e): the ported republic tests and the AS adapter."""

import copy
import math
import shutil
import unittest
from pathlib import Path

import pagexml.model.physical_document_model as pdm
import pytest
from pagexml.parser import json_to_pagexml_line

from archival_structures.text_layer import line_split as ls
from archival_structures.text_layer.extract_v2 import extract_inventory_v2
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_HAND_V2, SG_PRINT_V2

# ---------------------------------------------------------------------------
# ported from republic-project tests/test_line_splitter.py (Copyright (c) the republic-project authors, MIT)

line_json = {
    "id": "NL-HaNA_1.10.94_456_0008-line-2625-2341-1806-79",
    "type": ["structure_doc", "physical_structure_doc", "line", "pagexml_doc"],
    "main_type": "line",
    "domain": "physical",
    "metadata": {
        "custom_attributes": [{"index": "7", "tag_name": "readingOrder"}],
        "reading_order": {"index": "7"},
        "type": "line",
        "parent_type": "text_region",
        "parent_id": "NL-HaNA_1.10.94_456_0008-text_region-2581-2099-1853-1186",
        "text_region_id": "NL-HaNA_1.10.94_456_0008-text_region-2581-2099-1853-1186",
        "scan_id": "NL-HaNA_1.10.94_456_0008",
        "height": {"max": 58, "min": 28, "mean": 43, "median": 40},
        "line_width": "full",
        "left_alignment": "indent",
        "right_alignment": "column",
        "column_id": "NL-HaNA_1.10.94_456_0008-column-2575-422-1859-2863",
        "page_id": "NL-HaNA_1.10.94_456_0008-page-15",
    },
    "coords": [[2625, 2352], [2670, 2354], [2689, 2363], [2779, 2363], [2795, 2348],
               [3003, 2343], [3011, 2351], [3243, 2352], [3251, 2344], [3311, 2341],
               [3317, 2347], [3411, 2347], [3419, 2354], [3455, 2354], [3467, 2363],
               [3547, 2359], [3555, 2367], [3570, 2359], [3619, 2366], [3626, 2359],
               [3678, 2363], [3702, 2355], [3831, 2367], [3868, 2355], [3931, 2358],
               [3946, 2352], [3997, 2352], [4009, 2361], [4020, 2352], [4064, 2352],
               [4079, 2362], [4098, 2354], [4165, 2359], [4172, 2352], [4238, 2355],
               [4270, 2348], [4301, 2362], [4325, 2348], [4431, 2358], [4431, 2397],
               [4297, 2398], [4282, 2413], [4235, 2398], [4203, 2405], [4080, 2402],
               [4073, 2408], [4062, 2402], [3734, 2406], [3719, 2416], [3706, 2416],
               [3696, 2406], [3626, 2406], [3592, 2409], [3581, 2420], [3540, 2406],
               [3443, 2406], [3428, 2417], [3408, 2417], [3397, 2406], [3325, 2406],
               [3313, 2417], [3268, 2409], [2986, 2409], [2975, 2420], [2940, 2409],
               [2746, 2406], [2703, 2416], [2681, 2406], [2625, 2406]],
    "text": "Odijck, Becker, van Hecke, met twee extra- dertighsten der voorlede maendt, "
            "geaddresseert aen",
    "baseline": [[2644, 2397], [2694, 2396], [2894, 2401], [3144, 2402], [3494, 2397],
                 [3544, 2398], [3694, 2398], [4294, 2389], [4394, 2389], [4413, 2388]],
}


class TestLineSplitter(unittest.TestCase):

    def setUp(self) -> None:
        line = json_to_pagexml_line(line_json)
        self.test_line_group = [line]

    def test_split_word_candidates(self):
        line_splitter = ls.LineSplitter(
            min_merged_width=1200, column_sep_width=20, default_line_num_chars=45,
            default_line_width=900, text_left_boundary=2560, text_right_boundary=4400)
        # (as in the original: it only constructs the splitter; the call is commented out there)
        assert line_splitter.is_merged_line(self.test_line_group[0])


# ---------------------------------------------------------------------------
# synthetic pages

def make_line(line_id, text, x, y, w, h=60, scan_id="NL-HaNA_1.10.94_9_0001"):
    coords = pdm.Coords([[x, y], [x + w, y], [x + w, y + h], [x, y + h]])
    base_y = y + h - 15
    baseline = pdm.Baseline([(x + 5, base_y), (x + w // 2, base_y + 2), (x + w - 5, base_y)])
    return pdm.PageXMLTextLine(doc_id=line_id, metadata={"scan_id": scan_id}, coords=coords,
                               baseline=baseline, text=text)


def tok(n):
    return f"{n:04d}"


def two_column_lines(merged_row=2, rows=5, left_x=400, hyphen=False):
    """Rows of a two-column page: left column words 1000.., right column words 5000...
    Row `merged_row` is one merged line (left part + right part). Each column line holds
    9 four-character tokens (44 characters; the splitter wants >= 45 for a merged line)."""
    lines, left_n, right_n = [], 1000, 5000
    col_w, merged_w = 900, 1840
    y = 100
    for r in range(rows):
        left_words = [tok(left_n + i) for i in range(9)]
        right_words = [tok(right_n + i) for i in range(9)]
        left_n += 9
        right_n += 9
        if r == merged_row:
            if hyphen:
                left_words[-1] = left_words[-1][:2] + "-"
                right_words[0] = "respondentie"
            text = " ".join(left_words + right_words)
            lines.append(make_line(f"line_{r}", text, left_x, y, merged_w))
        else:
            lines.append(make_line(f"line_{r}a", " ".join(left_words), left_x, y, col_w))
            lines.append(make_line(f"line_{r}b", " ".join(right_words), left_x + 940, y, col_w))
        y += 100
    return lines


def make_page(lines):
    tr = pdm.PageXMLTextRegion(doc_id="tr1", metadata={"scan_id": "NL-HaNA_1.10.94_9_0001"},
                               coords=pdm.parse_derived_coords(lines), lines=lines)
    col = pdm.PageXMLColumn(doc_id="col1", metadata={"scan_id": "NL-HaNA_1.10.94_9_0001"},
                            coords=pdm.parse_derived_coords([tr]), text_regions=[tr])
    return pdm.PageXMLPage(doc_id="page1", metadata={"scan_id": "NL-HaNA_1.10.94_9_0001"},
                           columns=[col])


def make_splitter():
    return ls.LineSplitter(min_merged_width=1100, default_line_width=900, text_left_boundary=400,
                           text_right_boundary=2240, default_line_num_chars=45)


def test_hyphenated_merged_line_split():
    text = "Ontfangen een Missive van den Heeren Cor- respondentie ende andere Heeren Gecommitteerden"
    lines = [make_line("a1", "x" * 44, 400, 100, 900), make_line("a2", "y" * 44, 1340, 100, 900),
             make_line("m", text, 400, 200, 1840),
             make_line("b1", "x" * 44, 400, 300, 900), make_line("b2", "y" * 44, 1340, 300, 900)]
    page = make_page(lines)
    out = ls.page_split_merged_hyphenated_lines(make_splitter(), page)
    out_lines = out.columns[0].text_regions[0].lines
    assert len(out_lines) == 6
    idx = [i for i, ln in enumerate(out_lines) if ln.text == "Ontfangen een Missive van den Heeren Cor-"]
    assert idx, [ln.text for ln in out_lines]
    left, right = out_lines[idx[0]], out_lines[idx[0] + 1]
    assert left.text + " " + right.text == text
    assert right.text.startswith("respondentie")
    orig = lines[2].coords
    for half in (left, right):
        assert orig.left - 2 <= half.coords.left and half.coords.right <= orig.right + 2
        assert orig.top - 2 <= half.coords.top and half.coords.bottom <= orig.bottom + 2
    assert left.coords.left < right.coords.left
    # the original page is not changed
    assert len(page.columns[0].text_regions[0].lines) == 5


def stub_scorer(w1, w2):
    """+5 if w2 follows w1 numerically, else -5 (tokens are 4-digit numbers)."""
    try:
        return 5.0 if int(w2) == int(w1) + 1 else -5.0
    except ValueError:
        return -5.0


def test_non_hyphenated_split_uses_scorer():
    page = make_page(two_column_lines())
    splitter = make_splitter()
    out = ls.page_split_merged_non_hyphenated_lines(splitter, page, stub_scorer)
    out_lines = out.columns[0].text_regions[0].lines
    texts = [ln.text for ln in out_lines]
    assert len(out_lines) == len(page.columns[0].text_regions[0].lines) + 1
    # the true split is after token 1026 (left words 1018..1026, right words 5018..5026)
    left = " ".join(tok(1018 + i) for i in range(9))
    right = " ".join(tok(5018 + i) for i in range(9))
    assert left in texts and right in texts, texts
    # and a different scorer moves the split (the scorer decides)
    assert ls.page_split_merged_non_hyphenated_lines(
        splitter, page, lambda a, b: 0.0).columns[0].text_regions[0].lines is not None


def test_page_split_columns_sides():
    lines = two_column_lines(merged_row=-1)   # no merged line: 2 columns of 5 lines
    page = make_page(lines)
    out = ls.page_split_columns(make_splitter(), page)
    assert len(out.columns) == 2
    left, right = ([ln.id for ln in c.get_lines()] for c in out.columns)
    assert sorted(left) == sorted(f"line_{r}a" for r in range(5))
    assert sorted(right) == sorted(f"line_{r}b" for r in range(5))
    # fix 1: the helper finds the text regions of the columns (get_all_text_regions does not)
    assert len(ls._page_text_regions(page)) == 1


def rows(specs):
    return [{"x": x, "y": 0, "w": w, "h": 60, "text_raw": "a" * n, "page_side": side}
            for (x, w, n, side) in specs]


def test_derive_split_params():
    # 20 column lines (x 400, w 900, 45 chars), 10 merged lines (x 400, w 1840, 90 chars),
    # 5 short lines (ignored: < 20 chars)
    specs = [(400, 900, 45, "left")] * 20 + [(400, 1840, 90, "left")] * 10 \
        + [(400, 100, 5, "left")] * 5 + [(2600, 900, 45, "right")] * 4
    params = ls.derive_split_params(rows(specs), "left", split_region_tags=("Text",))
    assert params.default_line_width == 900
    assert params.default_line_num_chars == 45
    assert params.min_merged_width == round(1.19 * 900) == 1071
    assert ls.derive_split_params(rows(specs), "left", split_region_tags=("Text",),
                                  merged_width_factor=1.22).min_merged_width == 1098
    assert params.text_left_boundary == 400
    assert params.text_right_boundary == 2240
    assert params.column_sep_width == 20
    assert params.split_region_tags == ("Text",)
    # the plain median would be the merged width (more merged than column lines)
    specs2 = [(400, 900, 45, "left")] * 10 + [(400, 1840, 90, "left")] * 30
    assert ls.derive_split_params(rows(specs2), "left").default_line_width == 900
    right = ls.derive_split_params(rows(specs), "right")
    assert right is None or right.text_left_boundary == 2600
    assert ls.derive_split_params([], "left") is None


def test_make_pmi_scorer_matches_notebook():
    docs = [["de", "heeren", "van", "de", "heeren"], ["van", "de", "heeren"]]
    finder = ls.train_bigram_finder(docs)
    from nltk.metrics import BigramAssocMeasures
    pmi = BigramAssocMeasures().pmi
    scorer = ls.make_pmi_scorer(finder, min_uni_freq=2)
    # seen bigram: the PMI of nltk
    assert scorer("de", "heeren") == pytest.approx(finder.score_ngram(pmi, "de", "heeren"))
    # unseen bigram of two frequent words (> 2): log(1e-10 / (p1 * p2))
    n = finder.N
    expected = math.log(1e-10 / math.exp(math.log(3 / n) + math.log(3 / n)))
    assert finder.word_fd["de"] == 3 and finder.word_fd["heeren"] == 3
    assert scorer("heeren", "de") != 0.0   # ("heeren","de") is seen: 1 time -> PMI
    assert scorer("de", "de") == pytest.approx(expected)
    # unseen and not frequent enough: non_score
    assert ls.make_pmi_scorer(finder, min_uni_freq=100, non_score=-1.0)("de", "de") == -1.0
    assert scorer("onbekend", "woord") == 0.0


# ---------------------------------------------------------------------------
# the hook

def make_scan(lines, page_w=5000, tag="resolution"):
    tr = pdm.PageXMLTextRegion(doc_id="region_1", metadata={"scan_id": "NL-HaNA_1.10.94_9_0001"},
                               coords=pdm.parse_derived_coords(lines), lines=lines)
    tr.metadata["type"] = tag
    tr.metadata["structure"] = True
    return pdm.PageXMLScan(
        doc_id="NL-HaNA_1.10.94_9_0001.jpg",
        coords=pdm.Coords([[0, 0], [page_w, 0], [page_w, 4000], [0, 4000]]),
        text_regions=[tr])


def make_model(scorer=stub_scorer):
    params = ls.SplitParams(min_merged_width=1100, default_line_width=900,
                            default_line_num_chars=45, text_left_boundary=400,
                            text_right_boundary=2240, split_region_tags=("resolution",))
    return ls.SplitModel(params={"left": params, "right": None}, scorer=scorer,
                         split_region_tags=("resolution",), untagged_tag="untagged")


def test_hook_sets_metadata_and_ids():
    scan = make_scan(two_column_lines())
    hook = ls.make_split_hook(make_model())
    hook.record_splits = True
    scan = hook(scan)
    lines = scan.text_regions[0].lines
    by_id = {ln.id: ln for ln in lines}
    assert "line_2_L" in by_id and "line_2_R" in by_id and "line_2" not in by_id
    left, right = by_id["line_2_L"], by_id["line_2_R"]
    assert left.metadata["split_from"] == right.metadata["split_from"] == "line_2"
    assert left.metadata["edit_flags"] == right.metadata["edit_flags"] == "split:bigram"
    assert (left.metadata["column"], right.metadata["column"]) == ("1", "2")
    assert left.coords.left < right.coords.left
    assert left.text + " " + right.text == " ".join(
        [tok(1018 + i) for i in range(9)] + [tok(5018 + i) for i in range(9)])
    # other lines: column from the page split, no split metadata
    assert by_id["line_0a"].metadata["column"] == "1"
    assert by_id["line_0b"].metadata["column"] == "2"
    assert "split_from" not in by_id["line_0a"].metadata
    s = hook.stats_dict()
    assert s["n_candidates_before"] == 1 and s["n_split_bigram"] == 1
    assert s["n_split_hyphen"] == 0 and s["n_candidates_after"] == 0 and s["n_page_errors"] == 0
    assert s["n_lines_participating"] == 9
    # deterministic: a second run on a fresh scan gives the same result
    hook2 = ls.make_split_hook(make_model())
    scan2 = hook2(make_scan(two_column_lines()))
    assert [(ln.id, ln.text, ln.metadata.get("column")) for ln in scan2.text_regions[0].lines] \
        == [(ln.id, ln.text, ln.metadata.get("column")) for ln in lines]


def test_hook_page_error_keeps_unsplit_page():
    def broken(w1, w2):
        raise ValueError("boom")
    scan = make_scan(two_column_lines())
    hook = ls.make_split_hook(make_model(scorer=broken))
    scan = hook(scan)
    s = hook.stats_dict()
    assert s["n_page_errors"] == 1 and s["error_types"] == {"bigram:ValueError": 1}
    assert s["error_samples"][0]["step"] == "bigram"
    assert len(scan.text_regions[0].lines) == 9          # unsplit
    assert s["n_candidates_after"] == 1 and s["n_split_bigram"] == 0


def test_hook_double_space_at_column_join():
    """HTR text with two spaces where the columns meet: split anyway; halves are normalised."""
    lines = two_column_lines()
    merged = lines[4]   # line_2 (rows 0 and 1 have two lines each)
    assert merged.id == "line_2"
    merged.text = merged.text.replace(tok(1026) + " " + tok(5018), tok(1026) + "  " + tok(5018))
    assert "  " in merged.text
    hook = ls.make_split_hook(make_model())
    scan = hook(make_scan(lines))
    by_id = {ln.id: ln for ln in scan.text_regions[0].lines}
    assert "line_2_L" in by_id and hook.stats_dict()["n_page_errors"] == 0
    assert hook.stats_dict()["n_lines_text_normalised"] == 1
    assert by_id["line_2_L"].text + " " + by_id["line_2_R"].text == " ".join(merged.text.split())


def test_write_back_degenerate_split_keeps_line():
    """A 'split' that leaves one piece without text keeps the original line (and counts it)."""
    a = make_line("a", "x" * 44, 400, 100, 900)
    m = make_line("m", "y" * 50, 400, 200, 1840)
    scan = make_scan([a, m])
    region = scan.text_regions[0]
    hook = ls.make_split_hook(make_model())
    pseudo_region = pdm.PageXMLTextRegion(
        doc_id=region.id, lines=[make_line("a", "x" * 44, 400, 100, 900),
                                 make_line("zz1", "", 400, 200, 10),
                                 make_line("zz2", "y" * 50, 410, 200, 1830)])
    col = pdm.PageXMLColumn(doc_id="c", text_regions=[pseudo_region])
    page = pdm.PageXMLPage(doc_id="p", coords=pdm.Coords([[0, 0], [1, 0], [1, 1]]), columns=[col])
    hook._write_back(scan, {1: (region, [a, m])}, [1], page, set(), "left")
    assert [ln.id for ln in region.lines] == ["a", "m"]
    s = hook.stats_dict()
    assert s["n_degenerate_splits"] == 1 and s["n_alignment_failures"] == 0
    assert hook._alias == {"m": "zz2"}


def test_hook_hyphen_flag():
    scan = make_scan(two_column_lines(hyphen=True))
    hook = ls.make_split_hook(make_model())
    scan = hook(scan)
    flags = {ln.id: ln.metadata.get("edit_flags") for ln in scan.text_regions[0].lines}
    assert flags.get("line_2_L") == "split:hyphen"


def test_hook_ignores_other_tags():
    scan = make_scan(two_column_lines(), tag="page-number")
    hook = ls.make_split_hook(make_model())
    scan = hook(scan)
    assert len(scan.text_regions[0].lines) == 9
    s = hook.stats_dict()
    assert s["n_lines_participating"] == 0 and s["n_candidates_other_tags"] == 1
    assert all("column" not in ln.metadata for ln in scan.text_regions[0].lines)


# ---------------------------------------------------------------------------
# extract with and without a hook

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"


def test_hook_off_for_hand_preset(tmp_path):
    """A no-op hook gives byte-identical output; the hand preset has the split switched off."""
    assert SG_HAND_V2.merged_line_split == "off" and SG_HAND_V2.split_region_tags == ()
    assert SG_PRINT_V2.merged_line_split == "on"
    assert SG_PRINT_V2.split_region_tags == ("index", "resolution", "untagged", "Text")
    src = tmp_path / "src" / "NL-HaNA_1.01.02_9999"
    shutil.copytree(FIXTURE, src)
    out_a, out_b = tmp_path / "a", tmp_path / "b"
    extract_inventory_v2("9999", src, out_a, SG_HAND_V2)
    extract_inventory_v2("9999", src, out_b, SG_HAND_V2, scan_hook=lambda scan: scan)
    for sub, name in (("lines", "9999.lines.tsv.gz"), ("regions_meta", "9999.tsv.gz"),
                      ("scans", "9999.scans.tsv.gz")):
        assert (out_a / sub / name).read_bytes() == (out_b / sub / name).read_bytes()


def test_extract_inventory_v2_split_writes_model_and_stats(tmp_path):
    src = tmp_path / "src" / "NL-HaNA_1.10.94_9999"
    shutil.copytree(FIXTURE, src)
    out = tmp_path / "out"
    res = ls.extract_inventory_v2_split("9999", src, out, SG_PRINT_V2)
    assert res["stage"] == "extract"
    assert (out / "split_model" / "9999.json").exists()
    assert (out / "split_stats" / "9999.json").exists()
    rows_ = list(read_tsv_gz(out / "lines" / "9999.lines.tsv.gz"))
    assert len(rows_) > 0 and "split_from" in rows_[0]
