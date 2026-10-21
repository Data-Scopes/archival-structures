"""Step 15fq2: residual column-blocked pairs in 15fq-c print (sizes, sample of 20, seed 21)."""
import random
from itertools import combinations
import pandas as pd
import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import regions_overlap

P = "data/text_layer_v2_dev/15fq-c/print/NL-HaNA_1.10.94_text_layer_v2/"
rows = []
for inv in ("455", "456"):
    src = pd.read_csv(P + f"lines/{inv}.lines.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    col = dict(zip(zip(src.scan_id, src.line_id), src["column"]))
    txt = dict(zip(zip(src.scan_id, src.line_id), src["text"]))
    reg = pd.read_csv(P + f"layout/regions/{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    ln = pd.read_csv(P + f"layout/lines/{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    cols = {}
    first = {}
    for s, l, r in zip(ln.scan_id, ln.line_id, ln.layout_region_id):
        if col[(s, l)]:
            cols.setdefault((s, r), set()).add(col[(s, l)])
        first.setdefault((s, r), txt[(s, l)])
    reg = reg[(reg.layout_role != "empty") & (reg.furniture != "1")]
    for (sid, page), g in reg.groupby(["scan_id", "page"]):
        rs = list(g.itertuples())
        box = [pdm.Coords.coords_from_box_params(int(r.x), int(r.y), int(r.w), int(r.h)) for r in rs]
        objs = [type("B", (), {"coords": b})() for b in box]
        for i, j in combinations(range(len(rs)), 2):
            a, b = rs[i], rs[j]
            va, vb = cols.get((sid, a.layout_region_id), set()), cols.get((sid, b.layout_region_id), set())
            if len(va | vb) > 1 and regions_overlap(objs[i], objs[j], 0.5):
                rows.append((inv, sid, "".join(sorted(va)), int(a.n_lines), "".join(sorted(vb)), int(b.n_lines),
                             a.layout_role + "/" + b.layout_role, first[(sid, a.layout_region_id)][:30],
                             first[(sid, b.layout_region_id)][:30]))
df = pd.DataFrame(rows, columns=["inv", "scan", "colA", "nA", "colB", "nB", "roles", "firstA", "firstB"])
print(len(df), df.groupby("inv").size().to_dict())
df["small"] = df[["nA", "nB"]].min(axis=1)
print("min(n_lines) distribution:", df.small.describe().to_dict())
print("pairs with a side of <=3 lines:", int((df.small <= 3).sum()), "; both >=20 lines:", int((df.small >= 20).sum()))
print(df.roles.value_counts().to_dict())
rng = random.Random(21)
for _, r in df.iloc[sorted(rng.sample(range(len(df)), 20))].iterrows():
    print(r.inv, r.scan[-4:], f"{r.colA}:{r.nA} vs {r.colB}:{r.nB}", r.roles, "|", r.firstA, "|", r.firstB)

import re
sm = df[df.small <= 3].copy()
sm["t"] = [a if na <= nb else b for a, na, b, nb in zip(sm.firstA, sm.nA, sm.firstB, sm.nB)]
pn = sm.t.str.fullmatch(r"\(? ?\d[\d ]*\)?\s*|\d+ \d+ \)|\( \d+ \)")
print("small-side pairs", len(sm), "page-number-like:", int(pn.sum()))
print("other small-side first texts:", sm[~pn].t.tolist())
