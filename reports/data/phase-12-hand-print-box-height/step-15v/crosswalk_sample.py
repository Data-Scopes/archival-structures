"""Crosswalk bar sample of step 15v: 20 matched rows (seed 17) and 10 unmatched rows (seed 17) of the six
printed 1.01.02 volumes, with the unit text next to the REPUBLIC paragraph text.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/crosswalk_sample.py
"""
import csv
import gzip
import random
from pathlib import Path

import pandas as pd

R = Path("data/text_layer_v2_dev/15v-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4")
REP = Path("/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_paragraphs_v1")
OUT = Path("reports/data/phase-12-hand-print-box-height/step-15v")
INVS = "3771 3772 3773 3821 3822 3823".split()
rd = lambda p, **k: pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False, quoting=3, **k)

X = []
for inv in INVS:
    x = rd(R / "units" / "republic_lines" / f"{inv}.tsv.gz")
    x["inv"] = inv
    X.append(x)
X = pd.concat(X, ignore_index=True)
print(X.groupby(["inv", "match"]).size().unstack(fill_value=0))
rng = random.Random(17)
m = X[X.match != "unmatched"].to_dict("records")
u = X[X.match == "unmatched"].to_dict("records")
rng.shuffle(m)
rng.shuffle(u)
matched, unmatched = m[:20], u[:10]

cache = {}


def load(inv):
    if inv not in cache:
        L = rd(R / "lines" / f"{inv}.lines.tsv.gz", usecols=["scan_id", "line_id", "x", "y", "w", "h", "text_raw"])
        S = rd(R / "units" / "segments" / f"{inv}.tsv.gz")
        P = rd(REP / f"paragraphs-{inv}.tsv.gz")
        cache[inv] = (L, S, P)
    return cache[inv]


def unit_text(inv, line_id):
    L, S, _ = load(inv)
    s = S[S.line_id == line_id]
    if s.empty:
        return "", ""
    uid = s.unit_id.iloc[0]
    ids = S[S.unit_id == uid].sort_values("seg_order", key=lambda c: c.astype(int)).line_id
    t = dict(zip(L.line_id, L.text_raw))
    return uid, " ".join(t.get(i, "") for i in ids)


def para(inv, rid):
    P = load(inv)[2]
    r = P[(P.line_start == rid) | (P.line_end == rid)]
    return r.iloc[0] if len(r) else None


rows = []
for r in matched:
    uid, ut = unit_text(r["inv"], r["line_id"])
    p = para(r["inv"], r["republic_line_id"])
    rows.append({"kind": "matched", "inv": r["inv"], "match": r["match"], "role": r["role"], "republic_line_id": r["republic_line_id"],
                 "unit_id": uid, "unit_start": ut[:160], "unit_end": ut[-120:],
                 "para_start": p["text"][:160] if p is not None else "", "para_end": p["text"][-120:] if p is not None else "",
                 "verdict": ""})
for r in unmatched:
    inv = r["inv"]
    L = load(inv)[0]
    from archival_structures.text_layer.units import parse_republic_id
    pr = parse_republic_id(r["republic_line_id"])
    p = para(inv, r["republic_line_id"])
    near = ""
    if pr:
        sc, (x, y, w, h) = pr
        sub = L[L.scan_id == sc]
        if len(sub):
            d = (sub.x.astype(int) - x).abs() + (sub.y.astype(int) - y).abs() + (sub.w.astype(int) - w).abs() + (sub.h.astype(int) - h).abs()
            k = d.idxmin()
            near = f"{sub.loc[k, 'x']},{sub.loc[k, 'y']},{sub.loc[k, 'w']},{sub.loc[k, 'h']} d={d[k]} '{sub.loc[k, 'text_raw'][:60]}'"
        else:
            near = "scan not in AS layer"
    rows.append({"kind": "unmatched", "inv": inv, "match": "unmatched", "role": r["role"], "republic_line_id": r["republic_line_id"],
                 "unit_id": "", "unit_start": near, "unit_end": "", "para_start": p["text"][:160] if p is not None else "",
                 "para_end": p["text"][-120:] if p is not None else "", "verdict": ""})
pd.DataFrame(rows).to_csv(OUT / "crosswalk_sample.tsv", sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar="\\")
