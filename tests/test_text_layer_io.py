"""Tests for archival_structures.text_layer.io (ported from NLP's test_voc_text_layer.py)."""

import gzip
from collections import Counter
from pathlib import Path

import pytest

from archival_structures.text_layer import io as tio
from pagexml.model.coords import Coords


def test_sanitisation_counts_each_replacement():
    counter = Counter()
    assert tio.sanitise("a\tb\r\nc", counter) == "a b  c"
    assert counter == Counter({"tab": 1, "cr": 1, "lf": 1})
    assert tio.sanitise(None, counter) == ""


def test_written_fields_reject_tabs_and_newlines(tmp_path):
    with pytest.raises(AssertionError):
        tio.write_tsv_gz(tmp_path / "x.tsv.gz", ["a"], [["bad\tvalue"]])


def test_tsv_gz_is_deterministic_and_round_trips(tmp_path):
    a, b = tmp_path / "a.tsv.gz", tmp_path / "b.tsv.gz"
    rows = [["NL-HaNA_1.04.02_9626A_0001.jpg", "r1", "", 'x "q" ¬„']]
    tio.write_tsv_gz(a, ["scan_id", "region_id", "empty", "text"], rows)
    tio.write_tsv_gz(b, ["scan_id", "region_id", "empty", "text"], rows)
    assert a.read_bytes() == b.read_bytes()
    assert list(tio.read_tsv_gz(a)) == [
        {"scan_id": "NL-HaNA_1.04.02_9626A_0001.jpg", "region_id": "r1", "empty": "",
         "text": 'x "q" ¬„'}]
    with gzip.open(a, "rt") as handle:
        assert handle.readline() == "scan_id\tregion_id\tempty\ttext\n"


def test_region_box_format():
    coords = Coords([(2751, 696), (2867, 696), (2867, 747), (2751, 747)])
    assert tio.format_box(coords) == "2751-696-116-51"
    assert tio.format_box(None) == ""


def test_inventory_numbers_with_letter_suffix_stay_str(tmp_path):
    invs = ["9626A", "11013B", "9626", "1539", "10000"]
    assert sorted(invs, key=tio.inventory_sort_key) == ["1539", "9626", "9626A", "10000", "11013B"]
    for name in ("11013B.zip", "9626A.zip"):
        (tmp_path / name).write_bytes(b"")
    (tmp_path / "8834").mkdir()
    found = tio.discover_inventories(tmp_path)
    assert list(found) == ["8834", "9626A", "11013B"]
    assert all(isinstance(k, str) for k in found)


def test_inventory_sort_key_natural_order_and_non_numbers_last():
    names = ["9627", "10", "9626A", "9626", "x"]
    assert sorted(names, key=tio.inventory_sort_key) == ["10", "9626", "9626A", "9627", "x"]


def test_discover_inventories_zip_wins_and_skips_hidden(tmp_path):
    (tmp_path / "10").mkdir()
    (tmp_path / "9").mkdir()
    (tmp_path / "9.zip").write_bytes(b"")
    (tmp_path / ".hidden").mkdir()
    found = tio.discover_inventories(tmp_path)
    assert list(found) == ["9", "10"]
    assert found["9"].suffix == ".zip"
    assert found["10"].is_dir()


def _layer(tmp_path, n_lines_override=None):
    header = ["scan_id", "region_id", "line_idx", "line_id", "line_text"]
    tio.write_tsv_gz(tmp_path / "lines" / "7.lines.tsv.gz", header, [
        ["s1", "r1", "0", "l0", "eerste"], ["s1", "r1", "1", "l1", "tweede"]])
    meta = ["scan_id", "region_id", "region_box", "region_tag", "n_lines"]
    tio.write_tsv_gz(tmp_path / "regions_meta" / "7.tsv.gz", meta, [
        ["s1", "r1", "", "paragraph", n_lines_override or 2],
        ["s1", "r2", "", "unclassified", 0]])


def test_read_region_lines_skeleton_order_with_empty_region(tmp_path):
    _layer(tmp_path)
    regions = tio.read_region_lines(tmp_path, "7")
    assert [(row["region_id"], texts) for row, texts in regions] == [
        ("r1", ["eerste", "tweede"]), ("r2", [])]


def test_read_region_lines_asserts_on_line_count_mismatch(tmp_path):
    _layer(tmp_path, n_lines_override=3)
    with pytest.raises(AssertionError):
        tio.read_region_lines(tmp_path, "7")
