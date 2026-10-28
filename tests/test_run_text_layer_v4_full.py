"""Tests of scripts/run_text_layer_v4_full.py (step 15w): fake entries only, no PageXML, no build."""
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("run_v4_full", ROOT / "scripts" / "run_text_layer_v4_full.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

H, P = "1.01.02", "1.10.94"


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
    touch(a / "NL-HaNA_1.10.94_455.zip")           # wrong archive for the 1.01.02 pattern
    touch(b / "NL-HaNA_1.01.02_3350-pagexml.tar.gz", 5)
    (b / "NL-HaNA_1.01.02_11182").mkdir()
    touch(b / "NL-HaNA_1.01.02_11182" / "x.xml", 7)
    (tmp_path / "out").mkdir()
    return tmp_path


def args_for(tree, *extra):
    return ["--src", f"{H}={tree / 'a'}", "--src", f"{H}={tree / 'b'}", "--only", H,
            "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), *extra]


def print_tree(tree):
    touch(tree / "p" / "NL-HaNA_1.10.94_455.zip")
    touch(tree / "p" / "NL-HaNA_1.10.94_456.zip")
    touch(tree / "rep" / "paragraphs-455.tsv.gz")


def test_link_first_wins_and_ignores(tree):
    msgs = []
    links = mod.link_archive(H, [tree / "a", tree / "b"], tree / "work", msgs.append)
    assert list(links) == ["3349", "3350", "11182"]
    assert links["3350"].resolve() == (tree / "a" / "NL-HaNA_1.01.02_3350-pagexml.tar.gz").resolve()
    assert links["3350"].is_symlink() and links["3350"].readlink().is_absolute()
    assert len(msgs) == 1 and "3350" in msgs[0] and str(tree / "a") in msgs[0]
    assert not any(p.is_symlink() for p in (tree / "a").iterdir())   # nothing written into a source


def test_relink_removes_stale(tree):
    mod.link_archive(H, [tree / "a", tree / "b"], tree / "work", print)
    links = mod.link_archive(H, [tree / "b"], tree / "work", print)
    assert sorted(p.name for p in (tree / "work" / H).iterdir()) == sorted(p.name for p in links.values())
    assert list(links) == ["3350", "11182"]


def test_subset_missing_reports_and_exits(tree, capsys):
    (tree / "subset.txt").write_text("3349\n9999\n")
    rc = mod.main(args_for(tree, "--inventories-file", f"{H}={tree / 'subset.txt'}", "--check-only"))
    assert rc == 2
    assert "9999" in capsys.readouterr().out


def test_republic_warning(tree, capsys):
    print_tree(tree)
    rc = mod.main(["--src", f"{P}={tree / 'p'}", "--only", P, "--republic-dir", f"{P}={tree / 'rep'}",
                   "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), "--check-only"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "1/2 inventories" in out and "no crosswalk for 456" in out


def test_space_check(tree, monkeypatch, capsys):
    monkeypatch.setattr(mod, "OUTPUT_PER_XML_BYTE", {H: 1.0, P: 1.0})
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
    assert (tree / "out" / "run_v4.log").is_file()


def test_refuses_outside_root(tree, monkeypatch, capsys):
    monkeypatch.chdir(tree)
    assert mod.main(args_for(tree, "--check-only")) == 2
    assert "root" in capsys.readouterr().err


def test_predict_model():
    from archival_structures.text_layer.presets import predict_seconds
    fit = mod.THROUGHPUT_V4[H]
    sizes = [100e6] * 1000                       # many equal small volumes: the pool is throughput bound
    out, low, high = mod.predict(H, sizes, 8, 2)
    serial = sum(predict_seconds(b, st, fit) for st in mod.SERIAL_STAGES for b in sizes)
    pooled = sum(predict_seconds(b, st, fit) for st in mod.POOL_STAGES for b in sizes)
    assert low == pytest.approx(serial + pooled / 8)
    assert high == pytest.approx(serial + pooled / 2) and high >= low
    assert out == sum(sizes) * mod.OUTPUT_PER_XML_BYTE[H]
    _, low_big, high_big = mod.predict(H, [1e6, 5e9], 8, 2)   # one big volume: it is the pool floor
    assert low_big >= predict_seconds(5e9, "extract", fit) and high_big >= low_big
    assert mod.predict(P, [1e9], 4, 2)[1] > 0
    assert set(mod.THROUGHPUT_V4) == {H, P}
    assert all(set(v) == {*mod.POOL_STAGES, *mod.SERIAL_STAGES} for v in mod.THROUGHPUT_V4.values())


def test_build_cmds_rebuild(tree):
    args = mod.parse_args(["--src", f"{H}=x", "--out-dir", "o", "--want", "6", "--min", "3",
                           "--rebuild", f"{H}=pins.txt"])
    assert args.rebuild == {H: Path("pins.txt")}
    cmds = mod.build_cmds(H, args, "w", "o", None, "w/1.01.02_rebuild.txt")
    assert len(cmds) == 2
    first, second = cmds
    assert first[first.index("--stage") + 1] == "extract" and "--force" in first
    assert first[first.index("--inventories-file") + 1] == "w/1.01.02_rebuild.txt"
    assert first[-2:] == ["--workers", "{workers}"] and "v4-1.01.02" in first
    assert second[second.index("--stage") + 1] == "all" and "--inventories-file" not in second
    assert "--force" not in second
    assert len(mod.build_cmds(H, args, "w", "o", None)) == 1
    assert not any("--estimate" in c for c in cmds)


def test_rebuild_errors(tree, capsys):
    layer = tree / "out" / H / mod.PRESETS[mod.ARCHIVES[H]].name
    (tree / "pins.txt").write_text("# pins\n3349\n")
    (tree / "other.txt").write_text("3350\n")
    base = args_for(tree, "--rebuild", f"{H}={tree / 'pins.txt'}", "--check-only")
    assert mod.main(base) == 2                                    # no status.tsv
    assert "status.tsv" in capsys.readouterr().err
    touch(layer / "status.tsv")
    assert mod.main(base) == 0                                    # comments ignored, one inventory
    assert f"rebuild: 1 of 3 inventories" in capsys.readouterr().out
    assert mod.main(base + ["--inventories-file", f"{H}={tree / 'other.txt'}"]) == 2
    assert "--inventories-file" in capsys.readouterr().err
    assert mod.main(base + ["--limit", "1"]) == 2
    assert "--limit" in capsys.readouterr().err
    (tree / "pins.txt").write_text("3349\n9999\n")
    assert mod.main(base) == 2                                    # 9999 is not among the links
    assert "9999" in capsys.readouterr().out


def test_archive_summary_last_row(tmp_path):
    layer = tmp_path / H / "NL-HaNA_1.01.02_text_layer_v4"
    layer.mkdir(parents=True)
    head = "inventory_number\tstage\tstatus\tn_rows\n"
    rows = ["1\textract\tdone\t1", "1\ttypes\tfailed\t0", "1\ttypes\tdone\t1",
            "2\textract\tdone\t1", "2\ttypes\tdone\t1"]
    (layer / "status.tsv").write_text(head + "\n".join(rows) + "\n")
    assert mod.archive_summary(tmp_path, H) == (2, 0)
    (layer / "status.tsv").write_text(head + "\n".join(rows + ["2\ttypes\tfailed\t0"]) + "\n")
    assert mod.archive_summary(tmp_path, H) == (1, 1)


def test_two_archives_one_run(tree, capsys):
    print_tree(tree)
    rc = mod.main(["--src", f"{H}={tree / 'a'}", "--src", f"{P}={tree / 'p'}",
                   "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), "--check-only"])
    out = capsys.readouterr().out
    assert rc == 0
    assert (tree / "work" / H).is_dir() and (tree / "work" / P).is_dir()
    assert f"[{H}] 2 inventories found" in out and f"[{P}] 2 inventories found" in out
    log = (tree / "out" / "run_v4.log").read_text()
    assert log.count("start:") == 1 and f"archives={H},{P}" in log
    # an archive without --src is not built
    (tree / "out" / "run_v4.log").unlink()
    assert mod.main(["--src", f"{P}={tree / 'p'}", "--out-dir", str(tree / "out"),
                     "--work-dir", str(tree / "work2"), "--check-only"]) == 0
    assert f"archives={P} " in (tree / "out" / "run_v4.log").read_text()


def test_build_cmds_per_archive(tree):
    args = mod.parse_args(["--src", f"{H}=x", "--src", f"{P}=y", "--republic-dir", f"{P}=rep",
                           "--out-dir", "o", "--want", "6", "--min", "3"])
    real_h, = mod.build_cmds(H, args, "w", "o", None)
    real_p, = mod.build_cmds(P, args, "w", "o", "subset.txt")
    assert "--republic-dir" not in real_h and real_h[-2:] == ["--workers", "{workers}"]
    assert "sg_1.01.02_v4" in real_h and "v4-1.01.02" in real_h
    assert real_p[real_p.index("--republic-dir") + 1] == "rep" and "subset.txt" in real_p
    assert "sg_1.10.94_v4" in real_p and real_p[real_p.index("--want") + 1] == "6"


def test_republic_per_archive(tree, capsys):
    print_tree(tree)
    # 1.10.94 lacks paragraphs for 456: warned. 1.01.02 given a republic dir: no warning (print-mode only).
    rc = mod.main(["--src", f"{H}={tree / 'a'}", "--src", f"{P}={tree / 'p'}",
                   "--republic-dir", f"{P}={tree / 'rep'}", "--republic-dir", f"{H}={tree / 'rep'}",
                   "--out-dir", str(tree / "out"), "--work-dir", str(tree / "work"), "--check-only"])
    out = capsys.readouterr().out
    assert rc == 0
    warn = [ln for ln in out.splitlines() if "WARNING" in ln]
    assert len(warn) == 1 and f"[{P}]" in warn[0] and "456" in warn[0]
    # 1.01.02 without --republic-dir (the 15w setting): no warning at all
    rc = mod.main(["--src", f"{H}={tree / 'a'}", "--out-dir", str(tree / "out"),
                   "--work-dir", str(tree / "work"), "--check-only"])
    assert rc == 0 and "WARNING" not in capsys.readouterr().out


def test_bad_archive_argument(tree, capsys):
    with pytest.raises(SystemExit):
        mod.parse_args(["--src", "9.9.9=x", "--out-dir", "o"])


def test_audit_self_test():
    audit = ROOT / "reports/data/phase-12-hand-print-box-height/step-15w/audit.py"
    res = subprocess.run([sys.executable, "-I", str(audit), "--self-test"], cwd=ROOT, capture_output=True, text=True)
    assert res.returncode == 0, res.stdout[-800:] + res.stderr[-800:]
    assert "self-test ok" in res.stdout


def test_whole_layer_terms():
    assert mod.whole_layer_seconds(0, 0) == mod.WBC_DECISION_S
    base = mod.WBC_DECISION_S
    assert mod.whole_layer_seconds(1000, 0) - base == pytest.approx(2 * (mod.whole_layer_seconds(500, 0) - base))
    assert mod.whole_layer_seconds(0, 2e9) - base == pytest.approx(2 * (mod.whole_layer_seconds(0, 1e9) - base))
    assert mod.whole_layer_seconds(10, 3e9) == pytest.approx(
        base + 10 * mod.AUDIT_S_PER_INVENTORY + 3 * mod.SIZING_S_PER_STORED_GB)


def test_check_only_logs_whole_layer(tree):
    assert mod.main(args_for(tree, "--check-only")) == 0
    assert "over the whole layer" in (tree / "out" / "run_v4.log").read_text()


def test_version_v5(tree):
    args = mod.parse_args(["--src", f"{H}=x", "--out-dir", "o", "--version", "v5"])
    real, = mod.build_cmds(H, args, "w", "o", None)
    assert real[real.index("--preset") + 1] == "sg_1.01.02_v5" and "v5-1.01.02" in real
    assert mod.parse_args(["--src", f"{H}=x", "--out-dir", "o"]).version == "v4"
    assert mod.ARCHIVES == mod.ARCHIVES_BY_VERSION["v4"]


def test_version_v5_rejects_1_10_94(tree):
    with pytest.raises(SystemExit):
        mod.parse_args(["--only", P, "--out-dir", "o", "--version", "v5"])
    with pytest.raises(SystemExit):
        mod.parse_args(["--src", f"{P}=y", "--out-dir", "o", "--version", "v5"])
