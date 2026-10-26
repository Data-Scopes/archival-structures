"""Split counts and units per scan per series, 15w-a vs 15wr-a; 5 random lines (seed 20261030); 3 lines of 3771."""
import csv, gzip, random, collections, json, sys
csv.field_size_limit(10**9)
R = "NL-HaNA_1.01.02_text_layer_v4"
def rd(layer, sub, inv, suffix=".tsv.gz"):
    p = f"data/text_layer_v2_dev/{layer}/1.01.02/{R}/{sub}/{inv}{suffix}"
    return csv.DictReader(gzip.open(p, "rt", encoding="utf-8"), delimiter="\t", quoting=csv.QUOTE_NONE)
gate = {r["inv"]: r for r in csv.DictReader(open("reports/data/phase-12-hand-print-box-height/step-15wr/gate.tsv"), delimiter="\t") if r["split"] in ("True", "False")}
agg = collections.defaultdict(lambda: collections.Counter())
for inv, g in gate.items():
    s = g["series"][:12]
    for layer in ("15w-a", "15wr-a"):
        n = sum(1 for r in rd(layer, "lines", inv, ".lines.tsv.gz") if r["split_from"])
        us = list(rd(layer, "units/units", inv))
        agg[(s, g["split"])][layer + " splits"] += n
        agg[(s, g["split"])][layer + " units"] += len(us)
        agg[(s, g["split"])][layer + " scans"] += len({u["scan_id"] for u in us})
        agg[(s, g["split"])][layer + " vols"] += 0
    agg[(s, g["split"])]["vols"] += 1
for k, c in sorted(agg.items()):
    print(k, "vols", c["vols"], "| splits", c["15w-a splits"], "->", c["15wr-a splits"],
          "| units/scan %.2f -> %.2f" % (c["15w-a units"] / c["15w-a scans"], c["15wr-a units"] / c["15wr-a scans"]))
# 5 random lines
random.seed(20261030)
gated = [i for i, g in gate.items() if g["split"] == "False"]
cands = []
for inv in gated:
    old = {r["line_id"]: r for r in rd("15w-a", "lines", inv, ".lines.tsv.gz")}
    for r in rd("15wr-a", "lines", inv, ".lines.tsv.gz"):
        if len(r["text"]) > 30: cands.append((inv, r["line_id"], r["text"]))
for inv, lid, text in random.sample(cands, 5):
    old = {r["line_id"]: r for r in rd("15w-a", "lines", inv, ".lines.tsv.gz")}
    halves = [r["text"] for k, r in old.items() if k in (lid + "_L", lid + "_R")]
    print(inv, lid[:30]); print("   15wr-a:", text); print("   15w-a :", halves or ["(unsplit) " + old.get(lid, {}).get("text", "?")])
print("3771:")
a = {r["line_id"]: r for r in rd("15w-a", "lines", "3771", ".lines.tsv.gz")}
b = [r for r in rd("15wr-a", "lines", "3771", ".lines.tsv.gz") if r["split_from"]]
random.seed(1)
for r in random.sample(b, 3):
    print("   15wr-a:", r["line_id"][-12:], r["text"], "| 15w-a same id:", a.get(r["line_id"], {}).get("text") == r["text"])
