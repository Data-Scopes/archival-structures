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


# ---- step 15y: done-skip and staleness of the v2 stages (fixture: the SG inventory 9999, v4 preset)

SG_FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
V4_LAYER = "NL-HaNA_1.01.02_text_layer_v4"


@pytest.fixture
def v4_out(tmp_path, capsys):
    px = tmp_path / "px"
    shutil.copytree(SG_FIXTURE, px / "NL-HaNA_1.01.02_9999")
    out = tmp_path / "out"
    assert v4_run(px, out, "--stage", "all") == 0
    capsys.readouterr()
    return px, out


def v4_run(px, out, *extra):
    return build.main(["--pagexml-dir", str(px), "--out-dir", str(out), "--preset", "sg_1.01.02_v4",
                       "--workers", "1", *extra])


def v4_rows(out):
    lines = (Path(out) / V4_LAYER / "status.tsv").read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:]]


def layer_mtimes(out):
    skip = {"status.tsv", "manifest.json", "progress.log", "stats.json", "params.json"}
    return {str(p): p.stat().st_mtime_ns for p in (Path(out) / V4_LAYER).rglob("*")
            if p.is_file() and p.name not in skip and p.parent.name != "wbc"}


def test_v2_rerun_skips_done_stages(v4_out, capsys):
    px, out = v4_out
    n_rows, mtimes = len(v4_rows(out)), layer_mtimes(out)
    assert v4_run(px, out, "--stage", "all") == 0
    log = capsys.readouterr().out
    for stage in ("extract", "layout", "types", "units"):
        assert f"[{stage}] 1 already done, 0 to do" in log
    assert "[wbc] 1 stats parts already done, 0 to do" in log and "[join] 1 already done" in log
    assert len(v4_rows(out)) == n_rows
    assert layer_mtimes(out) == mtimes


def test_v2_params_change_redoes(v4_out, capsys):
    px, out = v4_out
    n_rows = len(v4_rows(out))
    assert v4_run(px, out, "--stage", "layout", "--layout-params", '{"merge": true}') == 0
    log = capsys.readouterr().out
    assert "[layout] parameters changed: redoing all" in log and "[layout] 0 already done, 1 to do" in log
    assert len(v4_rows(out)) == n_rows + 1 and v4_rows(out)[-1]["stage"] == "layout"


def test_v2_forced_extract_makes_later_stages_stale(v4_out, capsys):
    px, out = v4_out
    n_rows = len(v4_rows(out))
    assert v4_run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 0
    assert v4_run(px, out, "--stage", "all") == 0
    log = capsys.readouterr().out
    for stage in ("layout", "types", "units"):
        assert f"[{stage}] 0 already done, 1 to do" in log
    assert "[wbc] 0 stats parts already done, 1 to do" in log and "[join] 0 already done" in log
    assert [r["stage"] for r in v4_rows(out)[n_rows:]] == ["extract", "layout", "types", "units", "wbc", "join"]


def test_units_removes_stale_crosswalk(v4_out):
    px, out = v4_out
    stale = out / V4_LAYER / "units" / "republic_lines" / "9999.tsv.gz"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"stale")
    assert v4_run(px, out, "--stage", "units", "--force") == 0
    assert not stale.exists()


# ---- step 15yr: failed upstream blocks later v2 stages; recorded sizes

def _gate_preset(pins):
    from dataclasses import replace
    from archival_structures.text_layer.presets import PRESETS, SPLIT_GATE_V4
    base = PRESETS["sg_1.01.02_v4"]
    gate = {**SPLIT_GATE_V4, "min_rows": 1, "min_chars": 1, "min_share": 0.0}
    return replace(base, volume_mode={**base.volume_mode, "split_gate": gate, "pins": pins})


def _layer_content(root, inv):
    import gzip
    res = {}
    for p in Path(root).rglob(f"{inv}.*"):
        raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
        if p.name.endswith(".json"):
            raw = json.dumps({k: v for k, v in json.loads(raw).items()
                              if k not in ("seconds", "source") and not k.startswith("elapsed")},
                             sort_keys=True).encode()
        res[str(p.relative_to(root))] = raw
    return res


def test_v2_failed_extract_blocks_later_stages(tmp_path, monkeypatch, capsys):
    from archival_structures.text_layer import extract_v2
    from archival_structures.text_layer.presets import PRESETS
    px = tmp_path / "px"
    shutil.copytree(SG_FIXTURE, px / "NL-HaNA_1.01.02_9999")
    out = tmp_path / "out"
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", _gate_preset({"9999": "print"}))
    assert v4_run(px, out, "--stage", "all") == 0
    layer = out / V4_LAYER
    before = _layer_content(layer, "9999")
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", _gate_preset({"9999": "hand"}))
    real = extract_v2.extract_inventory_v2

    def boom(*a, **k):
        raise OSError("simulated read error after the pre-pass")
    monkeypatch.setattr(extract_v2, "extract_inventory_v2", boom)
    assert v4_run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 1
    n_rows = len(v4_rows(out))
    capsys.readouterr()
    assert v4_run(px, out, "--stage", "all") == 1
    log = capsys.readouterr().out
    new = [(r["stage"], r["status"]) for r in v4_rows(out)[n_rows:]]
    assert new == [("extract", "failed")]      # nothing after the failed extract
    assert "blocked" in log
    after = _layer_content(layer, "9999")
    assert {k for k in after if after[k] != before.get(k)} <= {"mode/9999.json"}
    monkeypatch.setattr(extract_v2, "extract_inventory_v2", real)
    assert v4_run(px, out, "--stage", "all") == 0
    fresh = tmp_path / "fresh"
    assert v4_run(px, fresh, "--stage", "all") == 0
    assert _layer_content(layer, "9999") == _layer_content(fresh / V4_LAYER, "9999")


def _write_tar(path, members):
    import tarfile
    with tarfile.open(path, "w:gz") as tf:
        for name in members:
            tf.add(SG_FIXTURE / name, arcname=name)


def test_v2_recorded_sizes(tmp_path, monkeypatch, capsys):
    names = sorted(p.name for p in SG_FIXTURE.glob("*.xml"))
    assert len(names) >= 2
    px = tmp_path / "px"
    px.mkdir()
    tar = px / "NL-HaNA_1.01.02_9999-pagexml.tar.gz"
    _write_tar(tar, names[:-1])
    out = tmp_path / "out"
    assert v4_run(px, out, "--stage", "all") == 0
    n_rows = len(v4_rows(out))
    calls = []
    real = build.input_bytes
    monkeypatch.setattr(build, "input_bytes", lambda s: calls.append(s) or real(s))
    capsys.readouterr()
    assert v4_run(px, out, "--stage", "all") == 0
    assert calls == [] and "[sizes] 1 from status.tsv, 0 measured" in capsys.readouterr().out
    assert len(v4_rows(out)) == n_rows
    _write_tar(tar, names)
    assert v4_run(px, out, "--stage", "all") == 0
    assert len(calls) == 1
