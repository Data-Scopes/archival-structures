"""Review of step 15u (phase 12): independent recomputation and two exploratory checks.

1. Recompute every test and diagnostic volume value from the 15u region caches without the library, compare with
   `<set>_volumes.tsv`, and recompute the stop rule (P5) from the volume table.
2. Exploratory (F12-R.1): box height divided by median page width, per class. Not a rule.
3. Exploratory (F12-R.2): per-stratum normal-tail estimate of volumes that would fall on the wrong side of the cut,
   pooled over fit and test. Not a rule; crude (n = 20 per stratum, normality assumed).

Run from the repo root: .venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15u.py
"""
import bisect
import csv
import gzip
import json
import math
from pathlib import Path

import pandas as pd

ROOT = Path.cwd()
R = ROOT / "data/hand_print_geom/15u-a/1.01.02/regions"
D = ROOT / "reports/data/phase-12-hand-print-box-height/step-15u"
SAMPLE = ROOT / "reports/data/phase-12-hand-print-box-height/planning/test_sample.tsv"
SILVER_15Q = ROOT / "reports/data/phase-10-hand-print-per-region/planning-15q/silver_sample.tsv"
WEIGHTS = {"eerste_serie": 123, "tweede_serie": 421, "gedrukte_resoluties": 99, "nouvelles": 64, "plakkaten": 76}


def value(inv):
    regs = []
    with gzip.open(R / f"{inv}.tsv.gz", "rt") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["hrel"] == "" or r["h_med"] == "" or int(r["n_chars"]) < 100:
                continue
            regs.append((float(r["h_med"]), int(r["n_chars"])))
    if not regs:
        return None
    regs.sort()
    cum, t = [], 0
    for _, n in regs:
        t += n
        cum.append(t)
    return regs[bisect.bisect_left(cum, cum[-1] / 2)][0]


def wilson(k, n, z=1.959964):
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return round((c - h) / d, 4), round((c + h) / d, 4)


def main():
    cut = json.load(open(D / "cut.json"))["cut"]
    s = pd.read_csv(SAMPLE, sep="\t", dtype=str)
    cols = ["inv", "stratum", "mode", "value", "median_page_w"]
    print("1. recomputation")
    tabs = {}
    for setn in ("test", "diagnostic"):
        v = pd.read_csv(D / f"{setn}_volumes.tsv", sep="\t", dtype={"inv": str}, usecols=cols)
        want = set(s[(s.set == setn) & (s.role == "sample")].inv)
        mine = {i: value(i) for i in v.inv}
        bad = sum(1 for _, r in v.iterrows() if mine[r.inv] is None or abs(mine[r.inv] - r.value) > 1e-9
                  or ("print" if mine[r.inv] <= cut else "hand") != r["mode"])
        print(f"  {setn}: n={len(v)} same volumes as sample rows={want == set(v.inv)} mismatches={bad}")
        tabs[setn] = v
    t = tabs["test"].merge(s[["inv", "silver"]], on="inv")
    acc = {}
    for st, g in t.groupby("stratum"):
        k = int((g["mode"] == g.silver).sum())
        acc[st] = k / len(g)
        print(f"  {st:20s} {g.silver.iloc[0]:5s} n={len(g)} agree={k} wilson95={wilson(k, len(g))}")
    for cls, sts in (("print", ["gedrukte_resoluties", "nouvelles", "plakkaten"]), ("hand", ["eerste_serie", "tweede_serie"])):
        w = sum(WEIGHTS[x] * acc[x] for x in sts) / sum(WEIGHTS[x] for x in sts)
        print(f"  weighted {cls}: {w:.3f}")

    print("2. box height per 1000 px of page width (exploratory)")
    for name, v in (("test", t), ("diagnostic", tabs["diagnostic"].assign(silver="non-silver"))):
        v = v.assign(h_per_kw=v.value / v.median_page_w * 1000)
        for cls, g in v.groupby("silver"):
            print(f"  {name:10s} {cls:10s} n={len(g):2d} value {g.value.min():.1f}-{g.value.max():.1f} px; "
                  f"per 1000 px width {g.h_per_kw.min():.2f}-{g.h_per_kw.max():.2f}; "
                  f"page_w {g.median_page_w.min():.0f}-{g.median_page_w.max():.0f}")

    print("3. normal-tail estimate of wrong-side volumes per stratum, fit + test pooled (exploratory)")
    f = pd.read_csv(D / "fit_volumes.tsv", sep="\t", dtype={"inv": str})
    ss = pd.read_csv(SILVER_15Q, sep="\t", dtype={"inv": str}, usecols=["inv", "stratum"])
    f = f.merge(ss, on="inv", how="left")
    f["stratum"] = f.stratum.fillna("dev/455-456")
    pooled = pd.concat([f[["stratum", "cls", "value"]],
                        t.rename(columns={"silver": "cls"})[["stratum", "cls", "value"]]])
    for (cls, st), g in pooled.groupby(["cls", "stratum"]):
        m, sd = g.value.mean(), g.value.std()
        z = (cut - m) / sd if cls == "print" else (m - cut) / sd
        p = 0.5 * math.erfc(z / math.sqrt(2))
        size = WEIGHTS.get(st)
        exp = f" ~{p * size:.1f} of {size} volumes" if size else ""
        print(f"  {cls:5s} {st:20s} n={len(g):2d} mean={m:.1f} sd={sd:.1f} "
              f"range={g.value.min()}-{g.value.max()} P(wrong side)={p:.4f}{exp}")


if __name__ == "__main__":
    main()
