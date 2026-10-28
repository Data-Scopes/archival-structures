"""Step 19c: detected index sides of the 125 hand volumes, per volume group; for half-year registers >= 3349 the first and
last flagged scan. Writes flagged_by_volume.tsv.

    .venv/bin/python -I flagged.py
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import PLAN, load_pages  # noqa: E402

HERE = Path(__file__).resolve().parent
p = load_pages()
p["n_lines"] = p.n_lines.astype(int)
p["f"] = p.index_side.astype(int)
p["scan"] = p.scan_id.str.rsplit("_", n=1).str[1].astype(int)
sp = pd.read_csv(PLAN / "gt_split.tsv", sep="\t", dtype=str).set_index("inv")["split"]
p["n"] = p.inv.astype(int)


def group(n, inv):
    if inv in sp.index:
        return "GT volumes"
    if n >= 3349 and n <= 3600:
        return "half-year registers >= 3349"
    if 4600 <= n <= 4800:
        return "tweede serie index volumes (46xx-47xx)"
    if n < 3349:
        return "other (< 3349)"
    return "other (> 3600)"


rows = []
for inv, v in p.groupby("inv", sort=False):
    fl = v[v.f == 1]
    rows.append(dict(inv=inv, n=int(inv), group=group(int(inv), inv), sides=len(v), flagged=len(fl),
                     lines=int(v.n_lines.sum()), flagged_lines=int(fl.n_lines.sum()),
                     first_scan=int(fl.scan.min()) if len(fl) else "", last_scan=int(fl.scan.max()) if len(fl) else ""))
t = pd.DataFrame(rows).sort_values("n")
t.drop(columns="n").to_csv(HERE / "flagged_by_volume.tsv", sep="\t", index=False)
g = t.groupby("group")[["sides", "flagged", "lines", "flagged_lines"]].sum()
g["volumes"] = t.groupby("group").size()
g["vols_with_flags"] = t[t.flagged > 0].groupby("group").size()
print(g.to_string())
print(t[t.flagged > 0].drop(columns="n").to_string(index=False))

# front section of the half-year registers: first contiguous run of flagged sides (in side order) and flags outside it
print("\nvolumes with >= 800 sides and < 50% of sides flagged (front index section of registers):")
fr = []
for inv, v in p.groupby("inv", sort=False):
    if len(v) < 800 or v.f.mean() >= 0.5 or int(inv) < 3349 or int(inv) in sp.index.astype(int).tolist() and False:
        continue
    f = v.f.tolist()
    i = f.index(1) if 1 in f else None
    if i is None:
        fr.append((inv, "", "", 0, 0))
        continue
    j = i
    while j < len(f) and f[j]:
        j += 1
    fr.append((inv, int(v.scan.iloc[i]), int(v.scan.iloc[j - 1]), j - i, int(sum(f)) - (j - i)))
fr = pd.DataFrame(fr, columns=["inv", "front_first_scan", "front_last_scan", "front_sides", "flagged_outside_front"])
fr.to_csv(HERE / "front_sections.tsv", sep="\t", index=False)
print(fr.describe(include="all").loc[["count", "min", "max", "mean"]].to_string())
print("volumes with flags outside the front run:", int((fr.flagged_outside_front > 0).sum()), "of", len(fr),
      "; total outside sides:", int(fr.flagged_outside_front.sum()))
print(fr[fr.flagged_outside_front > 0].to_string(index=False))
