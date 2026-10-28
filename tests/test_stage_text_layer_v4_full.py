"""Tests of scripts/stage_text_layer_v4_full.py (step 15yr): fake files only."""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("stage_v4_full", ROOT / "scripts" / "stage_text_layer_v4_full.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

H, P = "1.01.02", "1.10.94"
LH, LP = f"NL-HaNA_{H}_text_layer_v4", f"NL-HaNA_{P}_text_layer_v4"


def touch(path, text="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.fixture
def paths(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    for arch, layer in ((H, LH), (P, LP)):
        root = src / arch / layer
        for name in ("status.tsv", "manifest.json", "progress.log"):
            touch(root / name, arch)
        touch(root / "mode" / "455.json", arch)
        touch(root / "split_model" / "455.json", arch)
        touch(root / "units" / "params.json", "{}")
        touch(root / "units" / "units" / "455.tsv.gz", arch)
        touch(root / "units" / "units" / "9.tsv.gz", arch)
        touch(root / "wbc" / "parts" / "455.tsv", arch)
    touch(src / "audit.json", json.dumps({"invariants": [], "n_pass": 0, "n_fail": 0, "n_fail_judged": 0,
                                          "n_inventories": 0}))
    return src, dst


def run(src, dst, *extra):
    return mod.main(["--from", str(src), "--to", str(dst), *extra])


def test_copy_and_skip(paths, capsys):
    src, dst = paths
    assert run(src, dst, "--inventories", "455") == 0
    assert (dst / H / LH / "mode" / "455.json").read_text() == H
    assert (dst / P / LP / "units" / "units" / "455.tsv.gz").is_file()
    assert not (dst / H / LH / "units" / "units" / "9.tsv.gz").exists()   # not wanted
    capsys.readouterr()
    assert run(src, dst, "--inventories", "455") == 0
    assert "copied 0 files" in capsys.readouterr().out


def test_mirror_deletion_in_split_model(paths, capsys):
    src, dst = paths
    run(src, dst, "--inventories", "455")
    stale = touch(dst / H / LH / "split_model" / "1.json")
    touch(dst / H / LH / "wbc" / "parts" / "keep.tsv")   # skipped dir: kept
    assert run(src, dst, "--inventories", "455") == 0
    assert not stale.exists() and (dst / H / LH / "wbc" / "parts" / "keep.tsv").exists()
    assert "deleted 1" in capsys.readouterr().out


def test_wanted_inventory_stale_file_deleted_other_kept(paths):
    src, dst = paths
    run(src, dst, "--inventories", "455")
    stale = touch(dst / H / LH / "units" / "units" / "455.tsv.gz.old")
    other = touch(dst / H / LH / "units" / "units" / "777.tsv.gz")
    keep = [touch(dst / H / LH / "units" / "params.json"), touch(dst / "run_v4.log")]
    assert run(src, dst, "--inventories", "455") == 0
    assert not stale.exists() and other.exists() and all(p.exists() for p in keep)
    assert (dst / H / LH / "status.tsv").exists()


def test_dry_run_deletes_nothing(paths, capsys):
    src, dst = paths
    run(src, dst, "--inventories", "455")
    stale = touch(dst / H / LH / "mode" / "gone.json")
    assert run(src, dst, "--inventories", "455", "--dry-run") == 0
    out = capsys.readouterr().out
    assert stale.exists() and "would delete" in out


def test_deletion_cap(paths, capsys):
    src, dst = paths
    run(src, dst, "--inventories", "455")
    stale = [touch(dst / H / LH / "split_stats" / f"{i}.json") for i in range(101)]
    assert run(src, dst, "--inventories", "455") == 1
    assert all(p.exists() for p in stale)
    assert "101 stale files" in capsys.readouterr().out
    assert run(src, dst, "--inventories", "455", "--max-deletions", "101") == 0
    assert not any(p.exists() for p in stale)


def test_tags_parsed():
    assert mod.TAG_RE.findall("[455] x [1.01.02/455] y [12B]") == [("", "455"), ("1.01.02", "455"), ("", "12B")]
    audit = {"n_pass": 0, "n_fail": 2, "n_fail_judged": 2, "n_inventories": 2,
             "invariants": [{"pass": False, "reported": False, "invariant": "[455] a", "expected": 1, "actual": 2, "note": ""},
                            {"pass": False, "reported": False, "invariant": "[1.10.94/455] b", "expected": 1, "actual": 2, "note": ""}]}
    assert mod.failing_inventories(audit) == [(None, "455"), ("1.10.94", "455")]


def test_archive_inventory_item_copies_from_that_archive_only(paths):
    src, dst = paths
    assert run(src, dst, "--inventories", f"{P}/455") == 0
    assert (dst / P / LP / "units" / "units" / "455.tsv.gz").is_file()
    assert not (dst / H / LH / "units" / "units" / "455.tsv.gz").exists()
