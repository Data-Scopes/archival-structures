"""Step 15z: page size (F12-R.1). Per volume the median page_w, page_h of mode/scans/<inv>.tsv.gz of the full run
(data/text_layer_v4_full, read only); then the median over volumes for the 37 read volumes, all volumes, and the EAD series
(ancestor_3 of step-15p/inventories.tsv) the 37 belong to. Writes resolution.tsv.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/resolution.py"""
from pathlib import Path
import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
FULL = Path("data/text_layer_v4_full")
rows = []
for arch_dir in sorted(FULL.glob("*/NL-HaNA_*_text_layer_v4/mode/scans")):
    arch = arch_dir.parts[-4]
    for p in arch_dir.glob("*.tsv.gz"):
        s = pd.read_csv(p, sep="\t", usecols=["page_w", "page_h"])
        rows.append((arch, p.name.split(".")[0], s.page_w.median(), s.page_h.median(), len(s)))
v = pd.DataFrame(rows, columns=["archive", "inv", "page_w", "page_h", "n_scans"])
inv = pd.read_csv(D.parent / "phase-10-hand-print-per-region/step-15p/inventories.tsv", sep="\t", dtype={"inv": str},
                  usecols=["inv", "ancestor_3"])
v = v.merge(inv, on="inv", how="left")
read = set(pd.read_csv(D / "step-15z/volumes.tsv", sep="\t", dtype={"inv": str}).inv)
v["read"] = v.inv.isin(read) & (v.archive == "1.01.02")
v.to_csv(D / "step-15z/resolution_per_volume.tsv", sep="\t", index=False)
print("volumes", len(v), v.archive.value_counts().to_dict())
tab = []
def row(name, df):
    tab.append((name, len(df), df.page_w.median(), df.page_h.median()))
row("all volumes", v)
row("1.01.02 volumes", v[v.archive == "1.01.02"])
row("37 read volumes", v[v.read])
vol = pd.read_csv(D / "step-15z/volumes.tsv", sep="\t", dtype={"inv": str})
def grp(i):
    n = int(i)
    if 3433 <= n <= 3489 or n in (3726, 4792, 12297): return "index volumes (3433-3489, 3726, 4792, 12297)"
    if n in (2586, 2600, 4016): return "minutes (2586, 2600, 4016)"
    if n in (12163, 12165): return "plakkaten silver print (12163, 12165)"
    return "pamphlets, extracts, verbals, other"
vv = v[v.read].copy(); vv["group"] = vv.inv.map(grp)
for g, df in vv.groupby("group"): row(g, df)
for a in sorted(set(vv.ancestor_3.dropna())):
    row("series: " + a[:60], v[(v.ancestor_3 == a)])
t = pd.DataFrame(tab, columns=["set", "n_volumes", "median_page_w", "median_page_h"])
allw, allh = t.iloc[0].median_page_w, t.iloc[0].median_page_h
t["ratio_w_vs_all"] = (t.median_page_w / allw).round(3); t["ratio_h_vs_all"] = (t.median_page_h / allh).round(3)
t.to_csv(D / "step-15z/resolution.tsv", sep="\t", index=False)
print(t.to_string())
