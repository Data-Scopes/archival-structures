"""Step 15ir: which hand tables differ between 15i-b and 15ir-a, and how (decompressed)."""
import collections
import gzip
import json
from pathlib import Path

A = Path("data/text_layer_v2_dev/15ir-a/hand"); B = Path("data/text_layer_v2_dev/15i-b/hand")
ra = next(A.glob("*_text_layer_v2")); rb = next(B.glob("*_text_layer_v2"))
INV = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
TABLES = ["scans", "regions_meta", "lines", "layout/scans", "layout/regions", "layout/lines", "types/segments",
          "types/regions", "units/units", "units/segments", "units/doc_id_map", "join/units", "join/segments"]


def path(t, inv):
    if t in ("scans",): return f"scans/{inv}.scans.tsv.gz"
    if t == "lines": return f"lines/{inv}.lines.tsv.gz"
    return f"{t}/{inv}.tsv.gz"


def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return [l.rstrip("\n").split("\t") for l in fh]


out = {}
for t in TABLES:
    for inv in INV:
        a, b = rd(ra / path(t, inv)), rd(rb / path(t, inv))
        if a == b:
            continue
        hdr = a[0]
        diff_cols = collections.Counter()
        n = 0
        if len(a) == len(b) and a[0] == b[0]:
            for ra_, rb_ in zip(a[1:], b[1:]):
                if ra_ != rb_:
                    n += 1
                    for c, x, y in zip(hdr, ra_, rb_):
                        if x != y:
                            diff_cols[c] += 1
        out.setdefault(t, {})[inv] = {"rows_a": len(a) - 1, "rows_b": len(b) - 1, "n_rows_differing": n,
                                      "columns": dict(diff_cols)}
json.dump(out, open(Path(__file__).parent / "hand_diff.json", "w"), indent=1)
for t, d in out.items():
    print(t, {i: (v["rows_a"], v["rows_b"], v["n_rows_differing"], v["columns"]) for i, v in d.items()})
for inv in INV:
    for f in (f"layout/stats", f"units/stats", f"join/stats/{inv}"):
        pass
sa = json.load(open(ra / "layout/stats.json")); sb = json.load(open(rb / "layout/stats.json"))
print("layout stats equal except counters:", {i: {k: (sb[i][k], sa[i][k]) for k in sa[i] if sa[i][k] != sb[i].get(k) and k != "seconds"} for i in sa})
ua = json.load(open(ra / "units/stats.json")); ub = json.load(open(rb / "units/stats.json"))
print("units stats:", {i: {k: (ub[i][k], ua[i][k]) for k in ua[i] if ua[i][k] != ub[i].get(k) and k != "seconds"} for i in ua})
