"""Gate table: inv, series, share, n_rows, split, review pooled_mid_start (15w-a print volumes, 15wr-a mode files)."""
import csv, json, sys
from pathlib import Path
new = Path("data/text_layer_v2_dev/15wr-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode")
old = Path("data/text_layer_v2_dev/15w-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode")
pooled = {r["inv"]: r for r in csv.DictReader(open("reports/data/phase-12-hand-print-box-height/review/column_share_pooled.tsv"), delimiter="\t")}
out = csv.writer(open(sys.argv[1], "w"), delimiter="\t")
out.writerow(["inv", "series", "share", "n_rows", "split", "pooled_mid_start", "diff"])
bad = 0
for inv, r in sorted(pooled.items(), key=lambda kv: kv[0]):
    if not (new / f"{inv}.json").exists():
        out.writerow([inv, r["series"], "", "", "", r["pooled_mid_start"], "1.10.94, no gate"]); continue
    m = json.load(open(new / f"{inv}.json"))
    g = m.get("split_gate")
    if g is None:
        out.writerow([inv, r["series"], "", "", "", r["pooled_mid_start"], "NO GATE (mode %s)" % m["mode"]]); continue
    d = abs(g["share"] - float(r["pooled_mid_start"]))
    bad += d > 0.03
    out.writerow([inv, r["series"], g["share"], g["n_rows"], g["split"], r["pooled_mid_start"], round(d, 4)])
print("rows", len(pooled), "share gap > 0.03:", bad)
