"""Fixture tests for text_layer.extract and text_layer.join (step 15).

The fixture (tests/fixtures/text_layer/pagexml/9999A) pins pagexml-tools 0.9.1 behaviour.
"""

import json
import shutil
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from archival_structures.text_layer.extract import (
    extract_inventory, region_tag_of, scan_id_matches)
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.join import join_inventory

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "pagexml" / "9999A"
PREFIX = "NL-HaNA_1.04.02"


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("tl")
    src = root / "pagexml" / "9999A"
    shutil.copytree(FIXTURE, src)
    out = root / "out"
    status = extract_inventory("9999A", src, out, scan_id_prefix=PREFIX)
    join_status = join_inventory("9999A", out, {"¬", "„"}, write_events=True)
    return SimpleNamespace(out=out, status=status, join_status=join_status)


def _rows(built, layer, name="9999A.tsv.gz"):
    return list(read_tsv_gz(built.out / layer / name))


def test_status(built):
    s = built.status
    assert (s["n_rows"], s["n_lines"], s["n_scans"], s["n_errors"]) == (6, 7, 4, 0)


def test_regions_meta(built):
    rows = [(r["region_id"], r["region_box"], r["region_tag"], r["n_lines"])
            for r in _rows(built, "regions_meta")]
    assert rows == [("r1", "10-10-980-90", "header", "2"),
                    ("r2", "10-200-980-100", "paragraph", "0"),
                    ("r3", "10-400-980-150", "unclassified", "3"),
                    ("n1", "10-10-980-290", "paragraph", "0"),
                    ("n1a", "20-20-180-40", "unclassified", "1"),
                    ("m1", "10-10-290-90", "marginalia", "1")]


def test_lines(built):
    lines = _rows(built, "lines", "9999A.lines.tsv.gz")
    assert len(lines) == 7
    assert "„schrijven met tab" in [r["line_text"] for r in lines]
    r3 = [r for r in lines if r["region_id"] == "r3"]
    assert r3[1]["line_text"] == ""


def test_extract_stats(built):
    stats = json.loads((built.out / "extract_stats" / "9999A.json").read_text())
    assert stats["counts"] == {"lines_text_none": 1, "n_scans": 4, "nested_text_regions": 1,
                               "table_regions": 0, "text_regions_with_children": 1,
                               "untagged_no_custom": 2, "zero_line_regions": 2}
    assert stats["sanitised"] == {"tab": 1}
    assert stats["parse_errors"] == []
    assert stats["n_scan_id_mismatch"] == 1
    assert "_0004.xml -> NL-HaNA_1.04.02_9998_0004.jpg" in stats["scan_id_mismatch"][0]
    assert stats["scans_without_text_regions"] == ["NL-HaNA_1.04.02_9999A_0002.jpg"]


def test_regions_text(built):
    texts = {r["region_id"]: r["region_text"] for r in _rows(built, "regions")}
    assert texts == {"r1": "Copia van een briefschrijven met tab", "r2": "",
                     "r3": "f 2500„ tesamen", "n1": "", "n1a": "nested", "m1": "misfiled scan"}


def test_join_events(built):
    events = _rows(built, "join_events")
    assert [e["event"] for e in events] == ["both_ends_fix_last_line", "merge", "g_digit_left"]
    assert events[1]["joined_word"] == "briefschrijven"


def test_join_stats(built):
    stats = json.loads((built.out / "join_stats" / "9999A.json").read_text())
    assert stats["join"] == {"both_ends_fix": {"last_line": {"„": 1}}, "g_digit_left": {"„": 1},
                             "lines_empty_dropped": 1, "merge": {"¬": 1},
                             "merge_context": {"letter>lower": 1}, "regions": 6,
                             "regions_empty_text": 2}


def test_nested_regions_parse(built):
    # pagexml-tools 0.8.1 and 0.9.0 failed on nested TextRegions (TypeError, set += set in
    # get_all_text_regions); 0.9.1 parses them (step 20a), so scan _0003 yields n1 and n1a.
    stats = json.loads((built.out / "extract_stats" / "9999A.json").read_text())
    assert stats["parse_errors"] == []
    assert stats["counts"]["nested_text_regions"] == 1
    assert "n1a" in {r["region_id"] for r in _rows(built, "regions")}


@pytest.mark.parametrize("scan_id,inv,prefix,expected", [
    ("NL-HaNA_1.04.02_9999A_0001", "9999A", None, True),
    ("NL-HaNA_1.04.02_9999A_0001.jpg", "9999A", PREFIX, True),
    ("NL-HaNA_1.04.02_9999A_0001", "9998", None, False),
    ("NL-HaNA_1.04.02_9999A_0001", "9999A", "NL-HaNA_1.04.03", False),
    ("garbage", "9999A", None, False),
    ("", "9999A", PREFIX, False),
    ("NL-HaNA_1.04.02_10090_64.jpg", "10090", PREFIX, True),
    ("NL-HaNA_1.04.02_1186_0571A.jpg", "1186", PREFIX, True),
    ("NL-HaNA_1.04.02_9817_P01.jpg", "9817", PREFIX, True),
    ("NL-HaNA_1.04.03_9999A_0001.jpg", "9999A", PREFIX, False),
    ("NL-HaNA_1.04.02_9998_0004.jpg", "9999A", PREFIX, False),
    ("NL-HaNA_1.04.02_9998_0004.jpg", "9999A", None, False),
])
def test_scan_id_matches(scan_id, inv, prefix, expected):
    assert scan_id_matches(scan_id, inv, prefix) is expected


def test_region_tag_of_custom_untagged_tag():
    region = SimpleNamespace(attrs={}, metadata={})
    counts = Counter()
    assert region_tag_of(region, counts) == "unclassified"
    assert region_tag_of(region, counts, "untyped") == "untyped"
    assert counts["untagged_no_custom"] == 2
    typed = SimpleNamespace(attrs={}, metadata={"type": "header", "structure": "x"})
    assert region_tag_of(typed, counts, "untyped") == "header"
