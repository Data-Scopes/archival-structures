"""Step 19c error-analysis sample: all FP sides of the test volumes (<= 20), 20 FN sides drawn with probability
proportional to n_lines (sequential draws without replacement, random.Random(20261120) over the sides sorted by
inv, scan, page), the D2 disagreements. Writes error_sample.tsv (kind, inv, scan_id, page, n_lines, features, gt_label).

    .venv/bin/python -I errors_sample.py
"""
import random
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
d = pd.read_csv(HERE / "test_sides.tsv", sep="\t", dtype=str, keep_default_na=False)
d = d[d.gt_label != ""].copy()
d["n"] = d.n_lines.astype(int)
d["pos"] = d.gt_label.isin(["index_page", "respect_page"])
d["pred"] = d.pred == "True"
d["scan"] = d.scan_id.str.rsplit("_", n=1).str[1].astype(int)
d["inv_i"] = d.inv.astype(int)
d = d.sort_values(["inv_i", "scan", "page"])
fp = d[~d.pos & d.pred].assign(kind="FP")
fnp = d[d.pos & ~d.pred & (d.n > 0)]
rng = random.Random(20261120)
pool = fnp.index.tolist()
w = [int(fnp.loc[i, "n"]) for i in pool]
pick = []
while len(pick) < 20 and pool:
    k = rng.choices(range(len(pool)), weights=w)[0]
    pick.append(pool.pop(k))
    w.pop(k)
fn = fnp.loc[pick].assign(kind="FN")
out = pd.concat([fp, fn])
out[["kind", "inv", "scan_id", "page", "n_lines", "col_share", "end_num", "date_start", "rule", "gt_label"]].to_csv(
    HERE / "error_sample.tsv", sep="\t", index=False)
print(len(fp), "FP,", len(fn), "FN sampled of", len(fnp), "FN sides with text (", int((d.pos & ~d.pred).sum()), "FN total)")
print(out[["kind", "inv", "scan_id", "page", "n_lines", "col_share", "end_num", "date_start", "rule", "gt_label"]]
      .to_string(index=False))
