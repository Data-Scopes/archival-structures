"""Review of 15y (Opus 5.5, 2026-10-27): the v2 staleness logic on cases the step's tests do not cover.

Run from the AS root: `.venv/bin/python -m pytest -q -p no:cacheprovider -s
reports/data/phase-12-hand-print-box-height/review/stale_cases_15yz.py`. Builds into pytest's tmp dirs only.

  A  the forced extract fails after the mode file is written; then `--stage all`
  B  the reverse pin: hand-built, then pinned `print` with the splitter on (15y tested print -> hand)
  C  a crash after layout in the rebuild: forced extract, `--stage layout`, then `--stage all`
  D  a two-volume layer: rebuild one, the other keeps every file and row
"""

import gzip
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from archival_structures.text_layer import build
from archival_structures.text_layer import extract_v2
from archival_structures.text_layer.presets import PRESETS, SPLIT_GATE_V4

FIX = Path("tests/fixtures/text_layer/sg/9999")
LAYER = "NL-HaNA_1.01.02_text_layer_v4"
BASE = PRESETS["sg_1.01.02_v4"]
GATE_OPEN = {**SPLIT_GATE_V4, "min_rows": 1, "min_chars": 1, "min_share": 0.0}


def preset(pins):
    return replace(BASE, volume_mode={**BASE.volume_mode, "split_gate": GATE_OPEN, "pins": pins})


def run(px, out, *extra):
    return build.main(["--pagexml-dir", str(px), "--out-dir", str(out), "--preset", "sg_1.01.02_v4",
                       "--workers", "1", *extra])


def rows(out):
    lines = (Path(out) / LAYER / "status.tsv").read_text().splitlines()
    head = lines[0].split("\t")
    return [dict(zip(head, ln.split("\t"))) for ln in lines[1:]]


def last(out):
    d = {}
    for r in rows(out):
        d[(r["inventory_number"], r["stage"])] = r["status"]
    return d


def content(root, inv):
    res = {}
    for p in Path(root).rglob(f"{inv}.*"):
        raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
        if p.name.endswith(".json"):
            raw = json.dumps({k: v for k, v in json.loads(raw).items()
                              if k not in ("seconds", "source") and not k.startswith("elapsed")},
                             sort_keys=True).encode()
        res[str(p.relative_to(root))] = raw
    return res


@pytest.fixture
def px(tmp_path):
    d = tmp_path / "px" / "NL-HaNA_1.01.02_9999"
    shutil.copytree(FIX, d)
    return d.parent


def test_A_failed_forced_extract(px, tmp_path, monkeypatch, capsys):
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "print"}))
    out = tmp_path / "out"
    assert run(px, out, "--stage", "all") == 0
    layer = out / LAYER
    before = content(layer, "9999")
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "hand"}))
    real = extract_v2.extract_inventory_v2

    def boom(*a, **k):
        raise OSError("simulated read error after the pre-pass")
    monkeypatch.setattr(extract_v2, "extract_inventory_v2", boom)
    assert run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 1
    mode = json.loads((layer / "mode" / "9999.json").read_text())
    print("\nA: after the failed forced extract: mode file", mode["mode"], mode["method"],
          "| lines from the print build still there:", (layer / "lines" / "9999.lines.tsv.gz").exists(),
          "| split_model:", (layer / "split_model" / "9999.json").exists())
    n = len(rows(out))
    capsys.readouterr()
    rc = run(px, out, "--stage", "all")          # extract still fails
    log = capsys.readouterr().out
    new = [(r["stage"], r["status"]) for r in rows(out)[n:]]
    print("A: --stage all rc", rc, "new rows", new)
    print("A: log lines:", [ln.split(" ", 1)[1] for ln in log.splitlines() if "to do" in ln or "FAILED" in ln])
    after = content(layer, "9999")
    changed = sorted(k for k in after if after.get(k) != before.get(k))
    print("A: files of 9999 that changed:", changed)
    print("A: last rows:", {s: st for (i, s), st in last(out).items()})
    monkeypatch.setattr(extract_v2, "extract_inventory_v2", real)
    rc = run(px, out, "--stage", "all")
    fresh = tmp_path / "fresh"
    assert run(px, fresh, "--stage", "all") == 0
    a, b = content(layer, "9999"), content(fresh / LAYER, "9999")
    print("A: after the fix, rc", rc, "equal to a fresh hand build:", a == b,
          [k for k in set(a) | set(b) if a.get(k) != b.get(k)])


def test_B_hand_to_split_print(px, tmp_path, monkeypatch):
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "hand"}))
    out = tmp_path / "out"
    assert run(px, out, "--stage", "all") == 0
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "print"}))
    assert run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 0
    assert run(px, out, "--stage", "all") == 0
    fresh = tmp_path / "fresh"
    assert run(px, fresh, "--stage", "all") == 0
    a, b = content(out / LAYER, "9999"), content(fresh / LAYER, "9999")
    diff = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
    print("\nB: hand -> pinned print (split on): files", len(a), len(b), "differ:", diff,
          "| split_model:", (out / LAYER / "split_model" / "9999.json").exists())
    assert not diff


def test_C_crash_after_layout(px, tmp_path, monkeypatch, capsys):
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "print"}))
    out = tmp_path / "out"
    assert run(px, out, "--stage", "all") == 0
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "hand"}))
    assert run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 0
    assert run(px, out, "--stage", "layout") == 0       # the "crash" comes after layout
    capsys.readouterr()
    assert run(px, out, "--stage", "all") == 0
    log = capsys.readouterr().out
    fresh = tmp_path / "fresh"
    assert run(px, fresh, "--stage", "all") == 0
    a, b = content(out / LAYER, "9999"), content(fresh / LAYER, "9999")
    diff = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
    print("\nC:", [ln.split(" ", 1)[1] for ln in log.splitlines() if "to do" in ln], "differ:", diff)
    assert not diff


def test_D_two_volumes(px, tmp_path, monkeypatch, capsys):
    # a second inventory: the fixture copied under 9998 (file names renamed)
    d = px / "NL-HaNA_1.01.02_9998"
    d.mkdir()
    for p in (px / "NL-HaNA_1.01.02_9999").iterdir():
        (d / p.name.replace("_9999_", "_9998_")).write_text(p.read_text().replace("_9999_", "_9998_"))
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "print", "9998": "print"}))
    out = tmp_path / "out"
    assert run(px, out, "--stage", "all") == 0
    layer = out / LAYER
    keep = {str(p): p.stat().st_mtime_ns for p in layer.rglob("9998.*")}
    n = len(rows(out))
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset({"9999": "hand", "9998": "print"}))
    assert run(px, out, "--stage", "extract", "--force", "--inventories", "9999") == 0
    capsys.readouterr()
    assert run(px, out, "--stage", "all") == 0
    log = capsys.readouterr().out
    new = [(r["inventory_number"], r["stage"]) for r in rows(out)[n:]]
    now = {str(p): p.stat().st_mtime_ns for p in layer.rglob("9998.*")}
    print("\nD:", [ln.split(" ", 1)[1] for ln in log.splitlines() if "to do" in ln])
    print("D: new rows", new, "| 9998 files untouched:", keep == now, len(keep))
    assert all(i == "9999" for i, _ in new) and keep == now
