"""Step 15p error-analysis samples (seed 15): writes samples/<name>.tsv with text snippets and an
empty verdict column. Run from the AS root: .venv/bin/python -I <this file>
Needs data/hand_print_census/15p-a and inventories.tsv/mixed_scans.tsv.gz from analyze.py.
"""
import random
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
C = Path("data/hand_print_census/15p-a")
TSV = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/paragraphs/source")
PREFIX = "NL-HaNA_1.01.02_"
SNIP = 220
(HERE / "samples").mkdir(exist_ok=True)
inv_df = pd.read_csv(HERE / "inventories.tsv", sep="\t", dtype={"inv": str}, keep_default_na=False)
_cache = {}


def regions(inv):
    return pd.read_csv(C / "regions" / f"{inv}.tsv.gz", sep="\t", dtype={"scan_id": str})


def text_of(inv, df):
    """df: regions rows -> list of text (None if not found)."""
    p = TSV / f"paragraphs-{PREFIX}{inv}.tsv.gz"
    if not p.exists():  # suffixed inventories (audit D3) live in the bare-number file
        import re
        p = TSV / f"paragraphs-{PREFIX}{re.match(r'\d+', inv).group(0)}.tsv.gz"
    if inv not in _cache:
        _cache.clear()
        _cache[inv] = (pd.read_csv(p, sep="\t", quoting=3, dtype=str, keep_default_na=False,
                                   usecols=["doc_id", "text"]).set_index("doc_id")["text"]
                       if p.exists() else pd.Series(dtype=str))
    t = _cache[inv]
    out = []
    for r in df.itertuples():
        k = f"{r.scan_id}-text_region-{int(r.x)}-{int(r.y)}-{int(r.w)}-{int(r.h)}"
        v = t.get(k)
        out.append(None if v is None or not isinstance(v, str) else v)
    return out


def finish(rows, name):
    d = pd.DataFrame(rows)
    d["snippet"] = d["text"].map(lambda s: "" if not isinstance(s, str) else (s if len(s) <= SNIP else s[:SNIP] + "…"))
    d["verdict"] = ""
    d = d.drop(columns="text")
    d.insert(0, "idx", range(1, len(d) + 1))
    d.to_csv(HERE / "samples" / f"{name}.tsv", sep="\t", index=False, float_format="%.4g")
    print(name, len(d))
    return d


def reg_rows(inv, g, **extra):
    txt = text_of(inv, g)
    return [dict(inv=inv, scan_id=r.scan_id, w=int(r.w), h=int(r.h), n_chars=int(r.n_chars), density=r.density,
                 label=r.label, text=t, **extra) for r, t in zip(g.itertuples(), txt)]


# 1. 3350 dense regions
rg = regions("3350")
rg = rg[(rg.density > 0.55) & (rg.n_chars >= 20)]
print("3350 regions > 0.55:", len(rg))
if len(rg) > 30:
    rg = rg.sample(30, random_state=15)
finish(reg_rows("3350", rg), "s1_3350_dense")

# 2. uncertain regions, non-dev inventories, >= 100 chars
rng = random.Random(15)
cand = inv_df[(~inv_df.dev.astype(bool)) & (inv_df.uncertain_chars.astype(float) > 0)]["inv"].tolist()
rng.shuffle(cand)
rows = []
for inv in cand:
    g = regions(inv)
    g = g[(g.label == "uncertain") & (g.n_chars >= 100)]
    if len(g) and (TSV / f"paragraphs-{PREFIX}{inv}.tsv.gz").exists():
        rows += reg_rows(inv, g.sample(1, random_state=15))
    if len(rows) == 20:
        break
finish(rows, "s2_uncertain")

# 3. mixed scans: two densest and two sparsest regions each
ms = pd.read_csv(HERE / "mixed_scans.tsv.gz", sep="\t", dtype={"inv": str})
ms = ms[~ms.inv.isin(inv_df[inv_df.dev.astype(bool)].inv)]
rng = random.Random(15)
pick = rng.sample(sorted(ms.scan_id), 20)
rows = []
for sid in pick:
    inv = sid[len(PREFIX):].rsplit("_", 1)[0]
    g = regions(inv)
    g = g[(g.scan_id == sid) & (g.n_chars >= 20)].sort_values("density")
    sel = pd.concat([g.head(2), g.tail(2)]).drop_duplicates()
    rows += reg_rows(inv, sel)
finish(rows, "s3_mixed_scans")

# 4. silver contradictions: 10 volumes from each of three groups, 3 most-chars scans, top region of each
sc = pd.read_csv(C / "scans.tsv.gz", sep="\t", dtype={"inv": str})
nd = inv_df[(~inv_df.dev.astype(bool)) & inv_df.silver.isin(["hand", "print"])]
groups = {"print->mixed": nd[(nd.silver == "print") & (nd.label == "mixed")],
          "hand->print": nd[(nd.silver == "hand") & (nd.label == "print")],
          "hand->mixed": nd[(nd.silver == "hand") & (nd.label == "mixed")]}
rows = []
vol_rows = []
for gname, gd in groups.items():
    invs = sorted(gd.inv)
    random.Random(15).shuffle(invs)
    for inv in invs[:10]:
        top = sc[sc.inv == inv].sort_values("n_chars", ascending=False).head(3)
        rg = regions(inv)
        for sid in top.scan_id:
            g = rg[rg.scan_id == sid].sort_values("n_chars", ascending=False).head(1)
            rows += reg_rows(inv, g, group=gname)
finish(rows, "s4_silver_contradictions")

# 5. odd material: 7 dense (>1.5), 7 sparse (<0.05), 6 layout outliers; seed 15
odd = pd.read_csv(HERE / "odd_regions.tsv", sep="\t", dtype={"inv": str})
rows = []
for kind, sub, n in [("dense>1.5", odd[odd.density > 1.5], 7), ("sparse<0.05", odd[odd.density < 0.05], 7)]:
    sub = sub[~sub.inv.isin(inv_df[inv_df.dev.astype(bool)].inv)]
    s = sub.sample(n, random_state=15)
    for r in s.itertuples():
        g = pd.DataFrame([r])
        g["label"] = "?"
        rows += reg_rows(r.inv, g, kind=kind)
lo = pd.read_csv(HERE / "layout_outliers.tsv", sep="\t", dtype={"inv": str})
lo = lo.sample(6, random_state=15)
for r in lo.itertuples():
    g = regions(r.inv)
    g = g.sort_values("n_chars", ascending=False).head(1)
    rows += reg_rows(r.inv, g, kind=f"layout w={r.median_w:.0f} h={r.median_h:.0f}")
finish(rows, "s5_odd")
