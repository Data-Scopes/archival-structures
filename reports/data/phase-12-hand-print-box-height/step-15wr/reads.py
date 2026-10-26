"""Print, per scan of reads_15wr.tsv, the largest main_text unit (15wr-a) and the largest unit in 15w-a.
usage: reads.py OUT.txt"""
import csv, gzip, sys
from pathlib import Path
R = "NL-HaNA_1.01.02_text_layer_v4"
def load(layer, inv):
    root = Path(f"data/text_layer_v2_dev/{layer}/1.01.02/{R}")
    rd = lambda p: list(csv.DictReader(gzip.open(p, "rt", encoding="utf-8"), delimiter="\t", quoting=csv.QUOTE_NONE))
    return (rd(root / "units/units" / f"{inv}.tsv.gz"), rd(root / "units/segments" / f"{inv}.tsv.gz"),
            {r["line_id"]: r for r in rd(root / "lines" / f"{inv}.lines.tsv.gz")})
def best(units, scan):
    c = [u for u in units if u["scan_id"] == scan and u["unit_type"] == "main_text"] or [u for u in units if u["scan_id"] == scan]
    return min(c, key=lambda u: (-int(u["n_lines"]), int(u["unit_order"]))) if c else None
def show(layer, inv, scan, f):
    units, segs, lines = load(layer, inv)
    u = best(units, scan)
    if not u:
        f.write(f"  [{layer}] no unit\n"); return
    f.write(f"  [{layer}] unit_order {u['unit_order']} {u['unit_method']} n_lines {u['n_lines']} col {u['layout_column']}\n")
    ss = sorted((s for s in segs if s["unit_id"] == u["unit_id"]), key=lambda s: (int(s["seg_order"]), int(s["seg_idx"])))
    for s in ss:
        l = lines.get(s["line_id"])
        f.write(f"    {l['text'] if l else '?'+s['line_id']}\n")
with open(sys.argv[1], "w") as f:
    for r in csv.DictReader(open("reports/data/phase-12-hand-print-box-height/review/reads_15wr.tsv"), delimiter="\t"):
        f.write(f"=== {r['series']} {r['inv']} {r['scan_id']}\n")
        show("15wr-a", r["inv"], r["scan_id"], f)
        show("15w-a", r["inv"], r["scan_id"], f)
