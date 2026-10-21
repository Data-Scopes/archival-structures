"""Tests for the text-layer build driver (step 15c), on the tiny fixture inventory 9999A."""

import json
import shutil
from pathlib import Path

import pytest

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "pagexml" / "9999A"


@pytest.fixture
def pagexml_dir(tmp_path):
    root = tmp_path / "pagexml"
    shutil.copytree(FIXTURE, root / "9999A")
    shutil.copytree(FIXTURE, root / "9999B")
    return root


def run(pagexml_dir, out_dir, *extra):
    return build.main(["--pagexml-dir", str(pagexml_dir), "--out-dir", str(out_dir), *extra])


def status_rows(out_dir):
    path = Path(out_dir) / "voc_text_layer_v1" / "status.tsv"
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:]]


def test_all_without_ignore_sanity_stops_at_join(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    with pytest.raises(SystemExit) as exc:
        run(pagexml_dir, out, "--stage", "all", "--inventories", "9999A")
    assert exc.value.code not in (0, None)
    assert "sanity" in str(exc.value.code)
    assert not (out / "voc_text_layer_v1" / "regions" / "9999A.tsv.gz").exists()


def test_all_with_ignore_sanity(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    assert run(pagexml_dir, out, "--inventories", "9999A", "--ignore-sanity") == 0
    assert (out / "voc_text_layer_v1" / "regions" / "9999A.tsv.gz").exists()
    rows = status_rows(out)
    assert [r["stage"] for r in rows] == ["extract", "wbc", "join"]
    assert all(r["status"] == "done" for r in rows)
    data = list(read_tsv_gz(out / "voc_text_layer_v1" / "regions" / "9999A.tsv.gz"))
    assert data and list(data[0]) == build.REGION_COLUMNS


def test_rerun_is_resumable_and_force_redoes(pagexml_dir, tmp_path, capsys):
    out = tmp_path / "out"
    args = ("--inventories", "9999A", "--ignore-sanity")
    run(pagexml_dir, out, *args)
    n_before = len([r for r in status_rows(out) if r["stage"] == "extract"])
    capsys.readouterr()
    assert run(pagexml_dir, out, *args) == 0
    assert "[extract] 1 already done, 0 to do" in capsys.readouterr().out
    assert len([r for r in status_rows(out) if r["stage"] == "extract"]) == n_before
    assert run(pagexml_dir, out, *args, "--force") == 0
    assert len([r for r in status_rows(out) if r["stage"] == "extract"]) == n_before + 1


def test_wbc_on_fresh_out_dir_fails(pagexml_dir, tmp_path):
    with pytest.raises(SystemExit) as exc:
        run(pagexml_dir, tmp_path / "out", "--stage", "wbc", "--inventories", "9999A")
    assert "extract) is not done" in str(exc.value.code)


def test_estimate_creates_no_layer(pagexml_dir, tmp_path, capsys):
    out = tmp_path / "out"
    assert run(pagexml_dir, out, "--estimate", "--workers", "8") == 0
    assert "all three stages" in capsys.readouterr().out
    assert not (out / "voc_text_layer_v1").exists()


def test_limit_processes_first_inventory_only(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    assert run(pagexml_dir, out, "--stage", "extract", "--limit", "1") == 0
    assert {r["inventory_number"] for r in status_rows(out)} == {"9999A"}
    assert not (out / "voc_text_layer_v1" / "lines" / "9999B.lines.tsv.gz").exists()


def test_two_inventories_with_workers(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    assert run(pagexml_dir, out, "--ignore-sanity", "--workers", "2") == 0
    assert len([r for r in status_rows(out) if r["status"] == "done"]) == 6


def test_manifest_keys(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    run(pagexml_dir, out, "--inventories", "9999A", "--ignore-sanity")
    manifest = json.loads((out / "voc_text_layer_v1" / "manifest.json").read_text())
    assert {"layer", "row_order", "columns", "format", "untagged_region_tag",
            "stages"} <= set(manifest)
    assert manifest["layer"] == "voc_text_layer_v1"
    assert manifest["untagged_region_tag"] == "unclassified"
    assert set(manifest["stages"]) == {"extract", "wbc", "join"}
    for stage in manifest["stages"].values():
        assert {"finished_utc", "git_head", "archival_structures", "preset", "pagexml_tools",
                "python", "platform", "argv", "params"} <= set(stage)
        assert stage["preset"] == "voc_v1"
        assert stage["archival_structures"]["version"]


def test_help_lists_preset():
    with pytest.raises(SystemExit) as exc:
        build.main(["--help"])
    assert exc.value.code == 0


def test_subset_guard(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    run(pagexml_dir, out, "--stage", "extract")           # layer with 9999A and 9999B
    with pytest.raises(SystemExit) as exc:
        run(pagexml_dir, out, "--stage", "wbc", "--inventories", "9999A")
    assert "--allow-subset-wbc" in str(exc.value.code)
    assert not (out / "voc_text_layer_v1" / "wbc" / "wbc_per_inventory.tsv").exists()
    assert run(pagexml_dir, out, "--stage", "wbc", "--inventories", "9999A", "--allow-subset-wbc") == 0
    assert (out / "voc_text_layer_v1" / "wbc" / "wbc_per_inventory.tsv").exists()
    assert run(pagexml_dir, out, "--stage", "wbc") == 0    # a full run is unchanged


def test_manifest_history(pagexml_dir, tmp_path):
    out = tmp_path / "out"
    path = out / "voc_text_layer_v1" / "manifest.json"
    run(pagexml_dir, out, "--inventories", "9999A", "--ignore-sanity")
    first = json.loads(path.read_text(encoding="utf-8"))
    assert "history" not in first
    run(pagexml_dir, out, "--inventories", "9999A", "--ignore-sanity", "--force")
    second = json.loads(path.read_text(encoding="utf-8"))
    assert set(second["history"]) == {"extract", "wbc", "join"}
    assert second["history"]["extract"] == [first["stages"]["extract"]]
    run(pagexml_dir, out, "--inventories", "9999A", "--ignore-sanity", "--force")
    third = json.loads(path.read_text(encoding="utf-8"))
    assert [h["finished_utc"] for h in third["history"]["wbc"]][0] == first["stages"]["wbc"]["finished_utc"]
    assert len(third["history"]["wbc"]) == 2
