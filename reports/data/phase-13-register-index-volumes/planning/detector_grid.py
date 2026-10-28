"""Phase-13 planning: the detector grid V01-V12 (PLAN P8) on the GT **fit** volumes, from side_features.tsv.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/planning/detector_grid.py [fit|test]

Planning ran it with `fit` only (2026-10-28): V05 selected, lines P 0.9808, R 0.9297. Step 19c runs `test` once, for the
selected variant only (bar D1); the planner did not run `test`.
Side order for the contiguity fill: per inventory by (scan number, page verso 0 / recto 1 / single 0, page name). Gap fill: every
run of <= g undetected sides between two detected sides becomes detected; then detected runs shorter than m are
dropped. Positive label: gt_label in {index_page, respect_page}; sides without a GT label are left out of P and R.
Line weight = n_lines (lines with text) of the side.
"""
import sys
from pathlib import Path

import pandas as pd

P = Path(__file__).resolve().parent
GRID = [(c3, g, m) for c3 in (None, 0.3) for g in (0, 4, 8) for m in (1, 3)]   # V01..V12


def fill(flags, g, m):
    q = list(flags)
    idx = [i for i, x in enumerate(q) if x]
    for a, b in zip(idx, idx[1:]):
        if b - a - 1 <= g:
            for k in range(a + 1, b):
                q[k] = True
    i = 0
    while i < len(q):
        if q[i]:
            j = i
            while j < len(q) and q[j]:
                j += 1
            if j - i < m:
                for k in range(i, j):
                    q[k] = False
            i = j
        else:
            i += 1
    return q


def evaluate(f, c3, g, m, c1=0.25, c2=0.15):
    out = []
    for _, v in f.groupby("inv", sort=False):
        a = (v.cs >= c1) & (v.shp >= c2)
        if c3 is not None:
            a = a | ((v.n_text8 >= 5) & (v.shp >= c3))
        v = v.copy()
        v["pred"] = fill(list(a), g, m)
        out.append(v)
    d = pd.concat(out)
    d = d[d.gt_label != ""]
    pos = d.gt_label.isin(["index_page", "respect_page"])
    tp = (d.n_lines * (pos & d.pred)).sum()
    fp = (d.n_lines * (~pos & d.pred)).sum()
    fn = (d.n_lines * (pos & ~d.pred)).sum()
    return tp / max(tp + fp, 1), tp / max(tp + fn, 1), int((pos & d.pred).sum()), int((~pos & d.pred).sum()), \
        int((pos & ~d.pred).sum())


def main(which):
    f = pd.read_csv(P / "side_features.tsv", sep="\t", dtype={"inv": str}, keep_default_na=False)
    sp = pd.read_csv(P / "gt_split.tsv", sep="\t", dtype={"inv": str}).set_index("inv")["split"]
    f = f[f.inv.isin(sp.index[sp == which])].copy()
    f["cs"] = pd.to_numeric(f.col_share.replace("", None))
    f["shp"] = f[["end_num", "date_start"]].max(axis=1)
    f["scan"] = f.scan_id.str.rsplit("_", n=1).str[1].astype(int)
    f["po"] = f.page.map({"verso": 0, "recto": 1, "single": 0})
    f = f.sort_values(["inv", "scan", "po", "page"], key=lambda s: s.astype(int) if s.name == "inv" else s,
                      kind="stable")
    for k, (c3, g, m) in enumerate(GRID, start=1):
        p, r, tp, fp, fn = evaluate(f, c3, g, m)
        print(f"V{k:02d} c3={c3} gap={g} min_run={m}: lines P={p:.4f} R={r:.4f} | sides tp={tp} fp={fp} fn={fn}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "fit")
