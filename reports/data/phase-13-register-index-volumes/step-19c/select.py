"""Step 19c: detector grid V01-V12 on the GT fit volumes (`fit`), then the selected variant once on the GT test
volumes (`test`). Features from the clones' layout/pages tables, labels (gt_label) from planning/side_features.tsv.

    .venv/bin/python -I select.py fit     # prints the 12 lines, writes selection.json
    .venv/bin/python -I select.py test    # ONCE: refuses to run again (test_done.json)

Selection: among variants with lines P >= 0.95 the highest R (ties: lower V); none -> highest P.
Positive label: gt_label in {index_page, respect_page}; sides without a GT label are left out; line weight = n_lines.
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import PLAN, load_pages  # noqa: E402

from archival_structures.analysis.index_pages import fill_runs, side_rule  # noqa: E402

HERE = Path(__file__).resolve().parent
GRID = [(c3, g, m) for c3 in (None, 0.3) for g in (0, 4, 8) for m in (1, 3)]   # V01..V12


def variant_cfg(k):
    c3, g, m = GRID[k - 1]
    return {"col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": c3, "shape_only_lines": 5, "gap": g,
            "min_run": m}


def load(which):
    p = load_pages()
    lab = pd.read_csv(PLAN / "side_features.tsv", sep="\t", dtype=str, keep_default_na=False)[
        ["inv", "scan_id", "page", "gt_label"]]
    p = p.merge(lab, on=["inv", "scan_id", "page"], how="left")
    sp = pd.read_csv(PLAN / "gt_split.tsv", sep="\t", dtype=str).set_index("inv")["split"]
    return p[p.inv.map(sp) == which].copy()


def predict(p, cfg):
    out = []
    for _, v in p.groupby("inv", sort=False):
        rules = [side_rule({"n_text8": int(r.n_text8), "col_share": None if r.col_share == "" else float(r.col_share),
                            "end_num": float(r.end_num), "date_start": float(r.date_start)}, cfg)
                 for r in v.itertuples()]
        v = v.copy()
        v["pred"] = fill_runs(rules, cfg["gap"], cfg["min_run"])
        out.append(v)
    return pd.concat(out)


def score(d):
    d = d[d.gt_label != ""]
    w = d.n_lines.astype(int)
    pos = d.gt_label.isin(["index_page", "respect_page"])
    tp, fp, fn = (w * (pos & d.pred)).sum(), (w * (~pos & d.pred)).sum(), (w * (pos & ~d.pred)).sum()
    return tp / max(tp + fp, 1), tp / max(tp + fn, 1), int((pos & d.pred).sum()), int((~pos & d.pred).sum()), \
        int((pos & ~d.pred).sum())


def main(which):
    p = load(which)
    if which == "fit":
        res = []
        for k in range(1, 13):
            P, R, tp, fp, fn = score(predict(p, variant_cfg(k)))
            c3, g, m = GRID[k - 1]
            print(f"V{k:02d} c3={c3} gap={g} min_run={m}: lines P={P:.4f} R={R:.4f} | sides tp={tp} fp={fp} fn={fn}")
            res.append((k, P, R))
        ok = [r for r in res if r[1] >= 0.95]
        best = max(ok, key=lambda r: (r[2], -r[0])) if ok else max(res, key=lambda r: (r[1], -r[0]))
        sel = {"variant": f"V{best[0]:02d}", "k": best[0], "fit_P": round(best[1], 4), "fit_R": round(best[2], 4),
               "rule": "among P>=0.95 highest R (ties lower V); none -> highest P", "cfg": variant_cfg(best[0])}
        (HERE / "selection.json").write_text(json.dumps(sel, indent=1) + "\n")
        print("selected", sel["variant"], sel["fit_P"], sel["fit_R"])
    else:
        done = HERE / "test_done.json"
        if done.exists():
            raise SystemExit("test already run once: " + done.read_text())
        sel = json.loads((HERE / "selection.json").read_text())
        d = predict(p, sel["cfg"])
        P, R, tp, fp, fn = score(d)
        out = {"variant": sel["variant"], "lines_P": round(P, 4), "lines_R": round(R, 4), "sides_tp": tp,
               "sides_fp": fp, "sides_fn": fn, "n_volumes": int(d.inv.nunique())}
        done.write_text(json.dumps(out) + "\n")
        d.to_csv(HERE / "test_sides.tsv", sep="\t", index=False)
        print(out)
        print("D1:", "PASS" if P >= 0.95 and R >= 0.90 else "FAIL")


if __name__ == "__main__":
    main(sys.argv[1])
