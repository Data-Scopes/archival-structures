"""Write step-15z/reads.md: per read the inventory, EAD title, scan id and the text of the 3 largest regions.
No mode, value or box height. Run: .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/build_reads.py
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
L = Path("data/text_layer_v2_dev/15z-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/lines")
spec = importlib.util.spec_from_file_location("r15w", D / "review/review_15w.py")
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
reads = pd.read_csv(D / "planning/reads_15z.tsv", sep="\t", dtype={"inv": str})
titles = r.ead_titles(set(reads.inv))
out = []
cache = {}
for row in reads.itertuples():
    if row.inv not in cache:
        cache[row.inv] = pd.read_csv(L / f"{row.inv}.lines.tsv.gz", sep="\t", usecols=["scan_id", "region_id", "line_idx", "y", "text"], dtype={"text": str})
    d = cache[row.inv]
    d = d[d.scan_id == row.scan_id].copy()
    d["text"] = d["text"].fillna("")
    out.append(f"## Read {row.read_id}: inventory {row.inv}\n\nEAD title: {titles.get(row.inv, '(none)')}\nScan: {row.scan_id}\n")
    if d.empty:
        out.append("(no lines)\n"); continue
    size = d.groupby("region_id")["text"].apply(lambda s: s.str.len().sum()).sort_values(ascending=False)
    top = list(size.index[:3])
    top = sorted(top, key=lambda g: d[d.region_id == g].y.min())
    for i, g in enumerate(top, 1):
        g_d = d[d.region_id == g].sort_values("line_idx")
        out.append(f"Region {i} ({int(size[g])} chars):\n```\n" + "\n".join(g_d.text) + "\n```\n")
Path(D / "step-15z/reads.md").write_text("\n".join(out))
