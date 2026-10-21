"""Step 15ir criterion 2 (print): what differs between 15i-b and 15ir-a in layout/regions, layout/lines, layout/scans."""
import gzip, json
from collections import Counter
from pathlib import Path
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
ra = next(Path("data/text_layer_v2_dev/15ir-a/print").glob("*_text_layer_v2")); rb = next(Path("data/text_layer_v2_dev/15i-b/print").glob("*_text_layer_v2"))
for inv in ("455", "456"):
    reg_a, reg_b = rd(ra/"layout"/"regions"/f"{inv}.tsv.gz"), rd(rb/"layout"/"regions"/f"{inv}.tsv.gz")
    print(inv, "layout/regions identical:", reg_a == reg_b, len(reg_a))
    change = {(r["scan_id"], r["layout_region_id"]): r["change"] for r in reg_a}
    la, lb = rd(ra/"layout"/"lines"/f"{inv}.tsv.gz"), rd(rb/"layout"/"lines"/f"{inv}.tsv.gz")
    assert [(x["scan_id"], x["line_id"]) for x in la] == [(x["scan_id"], x["line_id"]) for x in lb]
    cols = Counter(); kinds = Counter()
    for x, y in zip(la, lb):
        if x != y:
            for c in x:
                if x[c] != y[c]: cols[c] += 1
            kinds[change[(x["scan_id"], x["layout_region_id"])]] += 1
    print(inv, "layout/lines differing rows", sum(kinds.values()), "columns", dict(cols), "by region change", dict(kinds))
    sa, sb = rd(ra/"layout"/"scans"/f"{inv}.tsv.gz"), rd(rb/"layout"/"scans"/f"{inv}.tsv.gz")
    cc = Counter()
    for x, y in zip(sa, sb):
        for c in x:
            if x[c] != y[c]: cc[c] += 1
    print(inv, "layout/scans differing columns", dict(cc))
    st_a = json.load(open(ra/"layout"/"stats.json"))[inv]; st_b = json.load(open(rb/"layout"/"stats.json"))[inv]
    print(inv, {k: (st_b[k], st_a[k]) for k in st_a if st_a[k] != st_b[k] and k != "seconds"})
