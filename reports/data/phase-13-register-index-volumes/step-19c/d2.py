"""Step 19c, bar D2: the 19c clones' index_side for the 60 read sides against 19a's verdicts
(index = 1, not index = 0; unclear / unreadable input left out). Writes d2_sides.tsv.

    .venv/bin/python -I d2.py
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import PLAN, load_pages  # noqa: E402

HERE = Path(__file__).resolve().parent
reads = pd.read_csv(PLAN / "reads_sides.tsv", sep="\t", dtype=str)
ver = pd.read_csv(HERE.parent / "step-19a" / "side_verdicts.tsv", sep="\t", dtype=str)
d = reads.merge(ver[["read_id", "verdict", "kind"]], on="read_id", how="left")
pages = load_pages()[["inv", "scan_id", "page", "n_lines", "col_share", "end_num", "date_start", "rule", "index_side"]]
d = d.merge(pages, on=["inv", "scan_id", "page"], how="left", suffixes=("_read", ""))
print("verdicts:", d.verdict.value_counts().to_dict(), " missing page rows:", int(d.index_side.isna().sum()))
j = d[d.verdict.isin(["index", "not index"])].copy()
j["gold"] = (j.verdict == "index").astype(int)
j["pred"] = j.index_side.astype(int)
print("judged", len(j), "left out", len(d) - len(j))
acc = (j.gold == j.pred).mean()
print(f"accuracy {acc:.4f} ({int((j.gold == j.pred).sum())}/{len(j)})")
fp = j[(j.gold == 0) & (j.pred == 1)]
print("false positives:", len(fp), " in S3h+S3a:", int(fp.stratum.isin(["S3h", "S3a"]).sum()))
print("false negatives:", int(((j.gold == 1) & (j.pred == 0)).sum()))
print(j.groupby("stratum").apply(lambda g: f"n={len(g)} correct={(g.gold == g.pred).sum()}", include_groups=False))
j.to_csv(HERE / "d2_sides.tsv", sep="\t", index=False)
dis = j[j.gold != j.pred]
print(dis[["read_id", "stratum", "inv", "scan_id", "page", "gold", "pred", "rule", "col_share", "end_num", "date_start"]]
      .to_string(index=False))
print("D2:", "PASS" if acc >= 0.90 and int(fp.stratum.isin(["S3h", "S3a"]).sum()) <= 2 and len(d) - len(j) <= 10
      else "FAIL")
