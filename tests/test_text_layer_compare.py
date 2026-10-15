"""Tests for the layer comparison (step 15c)."""

import json

from archival_structures.text_layer import compare
from archival_structures.text_layer.io import write_tsv_gz

HEADER = ["scan_id", "region_id", "region_text"]
ROWS = [["s1", "r1", "alpha"], ["s1", "r2", "beta"], ["s2", "r1", "gamma"]]


def make_layer(root, rows=ROWS, inv="1", stats=None):
    write_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz", HEADER, rows)
    write_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz", HEADER, rows)
    write_tsv_gz(root / "regions" / f"{inv}.tsv.gz", HEADER, rows)
    for kind in ("extract_stats", "join_stats"):
        path = root / kind / f"{inv}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(stats or {"n": 1, "source": "/a"}), encoding="utf-8")
    return root


def pair(tmp_path, right_rows=ROWS, right_stats=None):
    return make_layer(tmp_path / "L"), make_layer(tmp_path / "R", right_rows, stats=right_stats)


def test_identical(tmp_path):
    left, right = pair(tmp_path)
    frame = compare.compare_layers(left, right)
    assert len(frame) == 5
    tsv = frame[~frame["kind"].isin(compare.JSON_KINDS)]
    assert (tsv["byte_equal"] == True).all()  # noqa: E712
    assert (tsv["rows_left"] == 3).all()
    assert compare.summarise(frame)["lines"]["byte_equal"] == 1
    assert compare.main([str(left), str(right)]) == 0


def test_rows_swapped_is_order_only(tmp_path):
    left, right = pair(tmp_path, right_rows=[ROWS[1], ROWS[0], ROWS[2]])
    frame = compare.compare_layers(left, right, ["1"])
    row = frame[frame["kind"] == "regions"].iloc[0]
    assert not row["content_equal"] and row["multiset_equal"] and row["order_only"]
    assert compare.main([str(left), str(right)]) == 1


def test_text_changed(tmp_path):
    left, right = pair(tmp_path, right_rows=[ROWS[0], ["s1", "r2", "BETA"], ROWS[2]])
    row = compare.compare_layers(left, right).query("kind == 'lines'").iloc[0]
    assert not row["multiset_equal"] and not row["order_only"]


def test_missing_file(tmp_path):
    left, right = pair(tmp_path)
    (right / "regions" / "1.tsv.gz").unlink()
    row = compare.compare_layers(left, right, ["1"]).query("kind == 'regions'").iloc[0]
    assert row["exists_left"] and not row["exists_right"]
    assert compare.main([str(left), str(right)]) == 1


def test_json_ignores_source(tmp_path):
    left, right = pair(tmp_path, right_stats={"n": 1, "source": "/elsewhere"})
    frame = compare.compare_layers(left, right)
    js = frame[frame["kind"].isin(compare.JSON_KINDS)]
    assert (js["json_equal"] == True).all()  # noqa: E712
    assert compare.main([str(left), str(right)]) == 0


def test_json_difference_fails(tmp_path):
    left, right = pair(tmp_path, right_stats={"n": 2, "source": "/a"})
    assert compare.main([str(left), str(right)]) == 1


def test_run_files(tmp_path):
    left, right = pair(tmp_path)
    for root in (left, right):
        (root / "wbc").mkdir()
        (root / "wbc" / "corpus_default.json").write_text('{"a": 1}')
    frame = compare.compare_run_files(left, right, ("wbc/corpus_default.json", "wbc/wbc_stats.tsv"))
    assert list(frame["json_equal"].map(lambda v: isinstance(v, bool) and v)) == [True, False]
    assert list(frame["exists_left"]) == [True, False]
