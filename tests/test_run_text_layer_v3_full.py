"""Tests of scripts/run_text_layer_v3_full.py (step 15n): fake entries only, no PageXML, no build."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("run_v3_full", ROOT / "scripts" / "run_text_layer_v3_full.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def touch(path, size=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


@pytest.fixture
def tree(tmp_path, monkeypatch):
    monkeypatch.chdir(ROOT)
    a, b = tmp_path / "a", tmp_path / "b"
    touch(a / "NL-HaNA_1.01.02_3349-pagexml.tar.gz", 100)
    touch(a / "NL-HaNA_1.01.02_3350-pagexml.tar.gz", 100)
    touch(a / ".NL-HaNA_1.01.02_3351-pagexml.tar.gz")
    touch(a / "notes.txt")
    touch(a / "NL-HaNA_1.10.94_455.zip")           # wrong archive for the hand pattern
    touch(b / "NL-HaNA_1.01.02_3350-pagexml.tar.gz", 5)
    (b / "NL-HaNA_1.01.02_11182").mkdir()
    touch(b / "NL-HaNA_1.01.02_11182" / "x.xml", 7)
    (tmp_path / "out").mkdir()
    return tmp_path


def args_for(tree, *extra):
    return ["--hand-src", str(tree / "a"), "--hand-src", str(tree / "b"), "--only", "hand",
            "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), *extra]


def test_link_first_wins_and_ignores(tree):
    msgs = []
    links = mod.link_side("hand", [tree / "a", tree / "b"], tree / "work", msgs.append)
    assert list(links) == ["3349", "3350", "11182"]
    assert links["3350"].resolve() == (tree / "a" / "NL-HaNA_1.01.02_3350-pagexml.tar.gz").resolve()
    assert links["3350"].is_symlink() and links["3350"].readlink().is_absolute()
    assert len(msgs) == 1 and "3350" in msgs[0] and str(tree / "a") in msgs[0]
    assert not any(p.is_symlink() for p in (tree / "a").iterdir())   # nothing written into a source


def test_relink_removes_stale(tree):
    mod.link_side("hand", [tree / "a", tree / "b"], tree / "work", print)
    links = mod.link_side("hand", [tree / "b"], tree / "work", print)
    assert sorted(p.name for p in (tree / "work" / "hand").iterdir()) == sorted(p.name for p in links.values())
    assert list(links) == ["3350", "11182"]


def test_subset_missing_reports_and_exits(tree, capsys):
    (tree / "subset.txt").write_text("3349\n9999\n")
    rc = mod.main(args_for(tree, "--hand-inventories-file", str(tree / "subset.txt"), "--check-only"))
    assert rc == 2
    assert "9999" in capsys.readouterr().out


def test_republic_warning(tree, capsys):
    touch(tree / "p" / "NL-HaNA_1.10.94_455.zip")
    touch(tree / "p" / "NL-HaNA_1.10.94_456.zip")
    touch(tree / "rep" / "paragraphs-455.tsv.gz")
    rc = mod.main(["--print-src", str(tree / "p"), "--only", "print", "--republic-dir", str(tree / "rep"),
                   "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), "--check-only"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "1/2 inventories" in out and "no crosswalk for 456" in out


def test_space_check(tree, monkeypatch, capsys):
    monkeypatch.setattr(mod, "OUTPUT_PER_XML_BYTE", {"hand": 1.0, "print": 1.0})
    monkeypatch.setattr(mod, "ARCHIVE_XML_RATIO", 1.0)
    big = 10 ** 6
    touch(tree / "a" / "NL-HaNA_1.01.02_3349-pagexml.tar.gz", big)
    usage = type("U", (), {"free": int(1.1 * (big + 100 + 7))})()
    monkeypatch.setattr(mod.shutil, "disk_usage", lambda p: usage)
    assert mod.main(args_for(tree, "--check-only")) == 2
    assert "TOO LOW" in capsys.readouterr().out
    assert mod.main(args_for(tree, "--check-only", "--ignore-space")) == 0


def test_check_only_builds_nothing(tree, monkeypatch):
    calls = []
    monkeypatch.setattr(mod.subprocess, "Popen", lambda *a, **k: calls.append(a))
    monkeypatch.setattr(mod, "run_cmd", lambda *a, **k: calls.append(a) or 0)
    assert mod.main(args_for(tree, "--check-only")) == 0
    assert calls == []
    assert (tree / "out" / "run_v3.log").is_file()


def test_refuses_outside_root(tree, monkeypatch, capsys):
    monkeypatch.chdir(tree)
    assert mod.main(args_for(tree, "--check-only")) == 2
    assert "root" in capsys.readouterr().err


def test_predict_range():
    out, low, high = mod.predict("hand", [100, 10_000], want=8)
    rate = mod.WORKER_SECONDS_PER_XML_BYTE["hand"]
    assert low == 10_000 * rate                      # the largest inventory is the floor
    assert high == mod.SLOW_FACTOR * low
    assert out == 10_100 * mod.OUTPUT_PER_XML_BYTE["hand"]
    _, low_many, _ = mod.predict("hand", [100] * 1000, want=4)
    assert low_many == 100_000 * rate / 4            # many small inventories: throughput bound
