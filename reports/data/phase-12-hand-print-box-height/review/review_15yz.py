"""Review of steps 15y and 15z (Opus 5.5, 2026-10-27). Written apart from the steps' own scripts.

Run from the AS root with `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15yz.py CMD`:

  throughput   own OLS fit on the staged full run; in-sample, held-out-volume and 15x-a errors
  compare15y   15x-a vs 15y-b (all files), 15y-a vs 15y-b (3463)
  repro15z     37 values vs the full run; the pin rule re-applied to verdicts.tsv; 15z-a vs 15z-b; staged 16 vs 15z-b
  summary      archive_summary of the staged full run
  units IDX    main_text units of index volumes, print build next to hand build (reads, seed 20261105)
"""

import gzip
import json
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path.cwd()))
sys.path.insert(0, str(Path.cwd() / "scripts"))

from archival_structures.text_layer.presets import predict_wall  # noqa: E402

OUT = Path("reports/data/phase-12-hand-print-box-height/review")
L4 = "NL-HaNA_1.01.02_text_layer_v4"
FULL = Path("data/text_layer_v4_full")
DEV = Path("data/text_layer_v2_dev")
STAGES = ("extract", "layout", "types", "units", "wbc", "join")
POOL = {"extract", "wbc", "join"}


def last_done(status_path):
    rows = {}
    with open(status_path, encoding="utf-8") as fh:
        head = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            r = dict(zip(head, line.rstrip("\n").split("\t")))
            if r["status"] == "done" and r["seconds"] and r["input_xml_bytes"]:
                rows[(r["inventory_number"], r["stage"])] = r
    return rows


def ts(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").timestamp()


def fit_ols(rows, invs, stage):
    x = np.array([float(rows[(i, stage)]["input_xml_bytes"]) / 1e6 for i in invs if (i, stage) in rows])
    y = np.array([float(rows[(i, stage)]["seconds"]) for i in invs if (i, stage) in rows])
    A = np.vstack([np.ones_like(x), x]).T
    (b0, b1), *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = b0 + b1 * x
    r2 = 1 - ((y - pred) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    return {"intercept_s": float(b0), "s_per_mb": float(b1), "r2": float(r2)}


def span_h(rows, invs, stage):
    sel = [rows[(i, stage)] for i in invs if (i, stage) in rows]
    end = max(ts(r["finished_utc"]) for r in sel)
    start = min(ts(r["finished_utc"]) - float(r["seconds"]) for r in sel)
    return (end - start) / 3600, sum(float(r["seconds"]) for r in sel) / 3600


def pred_h(fit, rows, invs, stage, workers):
    sizes = [float(rows[(i, stage)]["input_xml_bytes"]) for i in invs if (i, stage) in rows]
    if stage in POOL:
        return predict_wall(sizes, workers, {stage: fit}, stages=(stage,)) / 3600
    return sum(max(0.0, fit["intercept_s"] + fit["s_per_mb"] * b / 1e6) for b in sizes) / 3600


def cmd_throughput():
    full = last_done(FULL / "1.01.02" / L4 / "status.tsv")
    dev = last_done(DEV / "15x-a" / "1.01.02" / L4 / "status.tsv")
    invs_full = sorted({i for i, _ in full})
    invs_dev = sorted({i for i, _ in dev})
    # rows rebuilt after the full run (the 16 pins) carry the rebuild's times: leave them out of the fit
    rebuilt = {i for (i, s), r in full.items() if r["finished_utc"] >= "2026-10-27T17:00:00Z"}
    workers = {"extract": 7, "wbc": 4, "join": 4}
    workers_dev = {"extract": 4, "wbc": 4, "join": 4}   # 15x-a: 4 granted for 1.01.02 (run_v4.log)
    out = {"n_full": len(invs_full), "n_dev": len(invs_dev), "rebuilt_rows_excluded": sorted(rebuilt), "stages": {}}
    print(f"full run: {len(invs_full)} inventories with done rows; rebuilt after the run: {len(rebuilt)}")
    fit_set = [i for i in invs_full if i not in rebuilt]
    holdout = sorted(set(invs_dev) & set(fit_set))
    no_dev = [i for i in fit_set if i not in set(invs_dev)]
    rng = random.Random(20261105)
    shuffled = fit_set[:]
    rng.shuffle(shuffled)
    half_a, half_b = sorted(shuffled[: len(shuffled) // 2]), sorted(shuffled[len(shuffled) // 2:])
    print(f"{'stage':8s} {'fit':>32s} | per-volume MAPE in | split-half: sum err | 196 held out: sum err (full-run secs)"
          f" | 15x-a run (fit without the 196): span meas/pred err")
    for st in STAGES:
        f_all = fit_ols(full, fit_set, st)
        f_nodev = fit_ols(full, no_dev, st)
        f_a = fit_ols(full, half_a, st)
        # per-volume relative error in sample
        x = np.array([float(full[(i, st)]["input_xml_bytes"]) / 1e6 for i in fit_set])
        y = np.array([float(full[(i, st)]["seconds"]) for i in fit_set])
        p = np.maximum(0, f_all["intercept_s"] + f_all["s_per_mb"] * x)
        mape = float(np.median(np.abs(p - y) / np.maximum(y, 1e-3)))
        sum_in = (p.sum() - y.sum()) / y.sum()
        # split half: fit on A, predict the summed seconds of B
        yb = np.array([float(full[(i, st)]["seconds"]) for i in half_b])
        xb = np.array([float(full[(i, st)]["input_xml_bytes"]) / 1e6 for i in half_b])
        pb = np.maximum(0, f_a["intercept_s"] + f_a["s_per_mb"] * xb).sum()
        err_half = (pb - yb.sum()) / yb.sum()
        # the 196 dev volumes held out of the fit, predicted against their own full-run seconds
        yh = np.array([float(full[(i, st)]["seconds"]) for i in holdout])
        xh = np.array([float(full[(i, st)]["input_xml_bytes"]) / 1e6 for i in holdout])
        ph = np.maximum(0, f_nodev["intercept_s"] + f_nodev["s_per_mb"] * xh).sum()
        err_hold = (ph - yh.sum()) / yh.sum()
        # the 15x-a run, span against the prediction with the granted workers (fit without the 196)
        m_dev, sum_dev = span_h(dev, invs_dev, st)
        p_dev = pred_h(f_nodev, dev, invs_dev, st, workers_dev.get(st, 1))
        m_full, sum_full = span_h(full, fit_set, st)
        p_full = pred_h(f_all, full, fit_set, st, workers.get(st, 1))
        out["stages"][st] = {"fit": f_all, "fit_without_196": f_nodev, "per_volume_median_ape": mape,
                             "sum_err_in_sample": sum_in, "sum_err_split_half": err_half,
                             "sum_err_196_held_out": err_hold,
                             "full_span_h": m_full, "full_worker_h": sum_full, "full_pred_h": p_full,
                             "dev_span_h": m_dev, "dev_worker_h": sum_dev, "dev_pred_h": p_dev,
                             "dev_ratio_seconds": float(sum(float(dev[(i, st)]["seconds"]) for i in holdout if (i, st) in dev)
                                                       / max(1e-9, yh.sum()))}
        print(f"{st:8s} {f_all['intercept_s']:9.4f} + {f_all['s_per_mb']:.5f}/MB R2 {f_all['r2']:.3f} | {mape:6.1%} "
              f"sum {sum_in:+.4%} | {err_half:+.2%} | {err_hold:+.2%} | full span {m_full:.3f} h pred {p_full:.3f} "
              f"({(p_full - m_full) / m_full:+.1%}), eff. workers {sum_full / m_full:.2f} | "
              f"15x-a {m_dev:.3f}/{p_dev:.3f} ({(p_dev - m_dev) / m_dev:+.1%}), 15x-a secs / full-run secs of the "
              f"same volumes {out['stages'][st]['dev_ratio_seconds']:.2f}")
    (OUT / "throughput_15yz_review.json").write_text(json.dumps(out, indent=1))


def read_any(p):
    raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
    if p.suffix == ".json":
        def strip(o):
            if isinstance(o, dict):
                return {k: strip(v) for k, v in o.items()
                        if k not in ("seconds", "source") and not k.startswith("elapsed")}
            return o
        try:
            return json.dumps(strip(json.loads(raw)), sort_keys=True).encode()
        except json.JSONDecodeError:
            return raw
    return raw


SKIP = {"manifest.json", "progress.log", "status.tsv"}


def compare_trees(a, b, focus=()):
    """{'equal': n, 'differ': [...], 'only_a': [...], 'only_b': [...]} over every file, decompressed."""
    fa = {str(p.relative_to(a)) for p in a.rglob("*") if p.is_file() and p.name not in SKIP}
    fb = {str(p.relative_to(b)) for p in b.rglob("*") if p.is_file() and p.name not in SKIP}
    res = {"equal": 0, "differ": [], "only_a": sorted(fa - fb), "only_b": sorted(fb - fa)}
    for rel in sorted(fa & fb):
        pa, pb = a / rel, b / rel
        if pa.read_bytes() == pb.read_bytes() or read_any(pa) == read_any(pb):
            res["equal"] += 1
        else:
            res["differ"].append(rel)
    return res


def inv_of(rel):
    name = Path(rel).name
    return name.split(".")[0]


def cmd_compare15y():
    a, b = DEV / "15x-a" / "1.01.02" / L4, DEV / "15y-b" / "1.01.02" / L4
    r = compare_trees(a, b)
    other = [x for x in r["differ"] + r["only_a"] + r["only_b"] if inv_of(x) != "3463"]
    print(f"15x-a vs 15y-b 1.01.02: equal {r['equal']}, differ {len(r['differ'])}, only 15x-a {r['only_a']}, "
          f"only 15y-b {r['only_b']}")
    print("  not 3463:", other)
    r2 = compare_trees(DEV / "15x-a" / "1.10.94", DEV / "15y-b" / "1.10.94")
    print(f"1.10.94: equal {r2['equal']}, differ {r2['differ']}, only {r2['only_a']} {r2['only_b']}")
    fa, fb = DEV / "15y-a" / "1.01.02" / L4, b
    files = sorted(str(p.relative_to(fa)) for p in fa.rglob("3463.*") if p.is_file())
    filesb = sorted(str(p.relative_to(fb)) for p in fb.rglob("3463.*") if p.is_file())
    eq = sum(read_any(fa / f) == read_any(fb / f) for f in files if f in filesb)
    print(f"15y-a vs 15y-b, 3463: {len(files)} / {len(filesb)} files, {eq} equal; sets equal {files == filesb}")
    m = json.loads((fb / "mode" / "3463.json").read_text())
    print("  mode/3463.json:", {k: m.get(k) for k in ("mode", "method", "box_height_mode", "value", "split_gate")})


def cmd_repro15z():
    R = Path("reports/data/phase-12-hand-print-box-height")
    vols = {}
    for line in (R / "step-15z/volumes.tsv").read_text().splitlines()[1:]:
        c = line.split("\t")
        vols[c[0]] = {"built": c[1], "decision": c[10]}
    bad = []
    for inv in vols:
        a = json.loads((DEV / "15z-a/1.01.02" / L4 / "mode" / f"{inv}.json").read_text())
        f = json.loads((FULL / "1.01.02" / L4 / "mode" / f"{inv}.json").read_text())
        fm = f.get("box_height_mode", f["mode"]) if f["method"] == "pinned" else f["mode"]   # staged copy now pinned
        am = a.get("box_height_mode", a["mode"]) if a["method"] == "pinned" else a["mode"]
        if a["value"] != f["value"] or (a.get("split_gate") or {}).get("share") != (f.get("split_gate") or {}).get("share") \
                and f["method"] != "pinned" or am != fm:
            bad.append((inv, a["value"], f["value"], am, fm))
    print(f"37 volumes: {len(vols)}; value/gate/measured-mode mismatches vs staged full run: {bad}")
    # the pin rule from verdicts.tsv
    counts = defaultdict(Counter)
    for line in (R / "step-15z/verdicts.tsv").read_text().splitlines()[1:]:
        c = line.split("\t")
        counts[c[1]][c[3]] += 1
    print("verdict rows:", sum(sum(c.values()) for c in counts.values()),
          Counter(v for c in counts.values() for v in c.elements()))
    pins = {}
    for inv, c in counts.items():
        m = vols[inv]["built"]
        opp = c["handwritten"] if m == "print" else c["printed"]
        same = c["printed"] if m == "print" else c["handwritten"]
        readable = opp + same
        pin = opp >= 4 or (readable < 5 and same == 0 and opp >= 2)
        dec = ("pin " + ("hand" if m == "print" else "print")) if pin else ("mixed" if opp in (2, 3) else "as built")
        if dec != vols[inv]["decision"]:
            print("  decision differs:", inv, dec, vols[inv]["decision"], dict(c))
        if pin:
            pins[inv] = "hand" if m == "print" else "print"
    from archival_structures.text_layer.presets import MODE_PINS_V4
    print(f"rule gives {len(pins)} pins (+3463 from 15y); MODE_PINS_V4 == rule + 3463: "
          f"{MODE_PINS_V4 == {**pins, '3463': 'hand'}}; decisions {Counter(v['decision'] for v in vols.values())}")
    a, b = DEV / "15z-a/1.01.02" / L4, DEV / "15z-b/1.01.02" / L4
    r = compare_trees(a, b)
    pinned = set(pins)
    other = [x for x in r["differ"] + r["only_a"] + r["only_b"] if inv_of(x) not in pinned]
    print(f"15z-a vs 15z-b: equal {r['equal']}, differ {len(r['differ'])}, only a {len(r['only_a'])}, "
          f"only b {len(r['only_b'])}; not a pinned volume: {other}")
    # staged drive rebuild: the 16 volumes against 15z-b
    s = FULL / "1.01.02" / L4
    n_eq, diff, only = 0, [], []
    for inv in pins:   # the rule's 16 include 3463
        fb = {str(p.relative_to(b)) for p in b.rglob(f"{inv}.*") if p.is_file()}
        fs = {str(p.relative_to(s)) for p in s.rglob(f"{inv}.*") if p.is_file()}
        only += sorted((fs ^ fb))
        for rel in fb & fs:
            if read_any(b / rel) == read_any(s / rel):
                n_eq += 1
            else:
                diff.append(rel)
    print(f"staged 16 vs 15z-b: equal {n_eq}, differ {diff}, in one only {only}")


def cmd_summary():
    import run_text_layer_v4_full as rs
    print("archive_summary:", rs.archive_summary(FULL, "1.01.02"), rs.archive_summary(FULL, "1.10.94"))


def units_of(root, inv):
    from archival_structures.text_layer.io import read_tsv_gz
    lines = {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
    units = [u for u in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz")]
    segs = defaultdict(list)
    for s in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
        segs[s["unit_id"]].append(s)
    return lines, units, segs


def cmd_units(inv):
    """The main_text units (method, column, lines, joined text) of one random scan of an index volume: print build,
    then hand build. Seed f"20261105-{inv}"; volume-wide counts of the unit methods first."""
    from archival_structures.text_layer.io import read_tsv_gz
    if inv == "3463":
        a, b = DEV / "15x-a/1.01.02" / L4, DEV / "15y-b/1.01.02" / L4
    else:
        a, b = DEV / "15z-a/1.01.02" / L4, DEV / "15z-b/1.01.02" / L4

    def units(root, scan=None):
        meta = {u["unit_id"]: u for u in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz")
                if scan is None or u["scan_id"] == scan}
        text = {u["unit_id"]: u["text"] for u in read_tsv_gz(root / "join" / "units" / f"{inv}.tsv.gz")
                if u["unit_id"] in meta}
        return [(meta[k], text.get(k, "")) for k in meta]
    ua = units(a)
    scans = sorted({m["scan_id"] for m, _ in ua if m["unit_type"] == "main_text"})
    scan = random.Random(f"20261105-{inv}").choice(scans)
    for tag, root in (("PRINT", a), ("HAND", b)):
        c = Counter(m["unit_method"] for m, _ in units(root) if m["unit_type"] == "main_text")
        print(f"## {inv} {tag} volume main_text {sum(c.values())} by method {dict(c.most_common(6))}")
    print(f"== {inv} scan {scan}")
    for tag, root in (("PRINT", a), ("HAND", b)):
        us = [(m, t) for m, t in units(root, scan) if m["unit_type"] == "main_text"]
        print(f"-- {tag}: {len(us)} main_text units")
        for m, t in us:
            print(f"  [{m['unit_method']} col{m['layout_column']} n={m['n_lines']}] {t[:300]}")


if __name__ == "__main__":
    cmd = sys.argv[1]
    {"throughput": cmd_throughput, "compare15y": cmd_compare15y, "repro15z": cmd_repro15z,
     "summary": cmd_summary, "interleave": lambda: None, "interleave_hand": lambda: None}.get(cmd, lambda: cmd_units(sys.argv[2]))()


def cmd_interleave():
    """Main-text units whose consecutive lines jump left and right by > 800 px at least 3 times
    (lines >= 500 px wide only) (two columns
    interleaved in one unit), print build against hand build, for the 14 hand-pinned volumes."""
    from archival_structures.text_layer.io import read_tsv_gz
    from archival_structures.text_layer.presets import MODE_PINS_V4
    rows = []
    for inv in sorted(i for i, m in MODE_PINS_V4.items() if m == "hand"):
        pr = DEV / ("15x-a" if inv == "3463" else "15z-a") / "1.01.02" / L4
        hd = DEV / ("15y-b" if inv == "3463" else "15z-b") / "1.01.02" / L4
        res = [inv]
        for root in (pr, hd):
            x = {r["line_id"]: int(r["x"]) for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")
                 if int(r["w"]) >= 500}   # review: only lines >= 500 px wide
            main = {u["unit_id"] for u in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz")
                    if u["unit_type"] == "main_text"}
            segs = defaultdict(list)
            for s in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
                if s["unit_id"] in main:
                    segs[s["unit_id"]].append((int(s["seg_order"]), s["line_id"]))
            bad = 0
            for uid, ss in segs.items():
                xs = [x[l] for _, l in sorted(ss) if l in x]
                jumps = sum(abs(b - a) > 800 for a, b in zip(xs, xs[1:]))
                bad += jumps >= 3
            res += [len(main), bad]
        rows.append(res)
        print(f"{inv}: print {res[1]} units, {res[2]} interleaved | hand {res[3]} units, {res[4]} interleaved")
    tot = [sum(r[i] for r in rows) for i in range(1, 5)]
    print(f"total: print {tot[0]} units, {tot[1]} interleaved ({tot[1] / tot[0]:.2%}) | hand {tot[2]} units, "
          f"{tot[3]} interleaved ({tot[3] / tot[2]:.2%})")
    (OUT / "interleave_15yz_review.tsv").write_text(
        "inv\tprint_units\tprint_interleaved\thand_units\thand_interleaved\n"
        + "".join("\t".join(map(str, r)) + "\n" for r in rows))


if __name__ == "__main__" and sys.argv[1] == "interleave":
    cmd_interleave()


def cmd_interleave_hand():
    """The same count over every `hand` volume of 15x-a (box-height hand, not pinned)."""
    from archival_structures.text_layer.io import read_tsv_gz
    root = DEV / "15x-a" / "1.01.02" / L4
    out = []
    for mp in sorted((root / "mode").glob("*.json")):
        m = json.loads(mp.read_text())
        if m["mode"] != "hand":
            continue
        inv = mp.stem
        x = {r["line_id"]: int(r["x"]) for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")
                 if int(r["w"]) >= 500}   # review: only lines >= 500 px wide
        main = {u["unit_id"] for u in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz")
                if u["unit_type"] == "main_text"}
        segs = defaultdict(list)
        for s in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
            if s["unit_id"] in main:
                segs[s["unit_id"]].append((int(s["seg_order"]), s["line_id"]))
        bad = sum(sum(abs(b - a) > 800 for a, b in zip(xs, xs[1:])) >= 3
                  for xs in ([x[l] for _, l in sorted(ss) if l in x] for ss in segs.values()))
        out.append((inv, len(main), bad))
    n, b = sum(r[1] for r in out), sum(r[2] for r in out)
    top = sorted(out, key=lambda r: -r[2] / max(r[1], 1))[:8]
    print(f"15x-a hand volumes: {len(out)}, main_text units {n}, interleaved {b} ({b / n:.2%}); "
          f"volumes with >= 1%: {sum(r[2] / max(r[1], 1) >= 0.01 for r in out)}; top {top}")
    (OUT / "interleave_hand_15x-a_review.tsv").write_text(
        "inv\tunits\tinterleaved\n" + "".join(f"{a}\t{c}\t{d}\n" for a, c, d in out))


if __name__ == "__main__" and sys.argv[1] == "interleave_hand":
    cmd_interleave_hand()
