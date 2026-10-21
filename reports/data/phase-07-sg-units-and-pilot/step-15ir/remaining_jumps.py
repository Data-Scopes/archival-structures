"""Step 15ir criterion 3: the split_at_column regions of 15ir-a that still jump back > 50 px in box y, with baseline y."""
import gzip, json
from collections import defaultdict
from pathlib import Path
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
r = next(Path("data/text_layer_v2_dev/15ir-a/print").glob("*_text_layer_v2"))
res = []
for inv in ("455", "456"):
    lines = {(x["scan_id"], x["line_id"]): x for x in rd(r/"lines"/f"{inv}.lines.tsv.gz")}
    by = defaultdict(list)
    for l in rd(r/"layout"/"lines"/f"{inv}.tsv.gz"): by[(l["scan_id"], l["layout_region_id"])].append(l)
    for g in rd(r/"layout"/"regions"/f"{inv}.tsv.gz"):
        if "split_at_column" not in g["change"]: continue
        ls = sorted(by[(g["scan_id"], g["layout_region_id"])], key=lambda l: int(l["layout_line_idx"]))
        ls = [lines[(l["scan_id"], l["line_id"])] for l in ls if lines[(l["scan_id"], l["line_id"])]["text"].strip()]
        ys = [int(l["y"]) for l in ls]
        if any(b < a - 50 for a, b in zip(ys, ys[1:])):
            def bl(l):
                pts = [tuple(map(int, p.split(","))) for p in l["baseline"].split()] if l["baseline"] else []
                return round(sum(p[1] for p in pts)/len(pts)) if pts else None
            res.append({"inv": inv, "region": g["layout_region_id"][-40:], "change": g["change"], "n": len(ls),
                        "rows": [(int(l["y"]), int(l["h"]), bl(l), l["text"][:25]) for l in ls
                                 ]})
for x in res:
    ys = [t[0] for t in x["rows"]]
    j = next(i for i in range(len(ys)-1) if ys[i+1] < ys[i]-50)
    print(x["inv"], x["region"], x["change"], x["n"], "jump at", j, x["rows"][max(0,j-1):j+3])
