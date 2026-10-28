"""Step 19e error analysis: writes error_reads.txt with (1) 10 v5 units on flagged sides of volumes outside both gold sets
(random.Random(20261122) over sorted ids "inv:unit_id"), (2) 5 such units on gap-filled sides (rule == 0; seed 20261123),
(3) one flagged side each of the 3 volumes with the largest relative change in main_text units on flagged sides (side = first
flagged one by sorted scan; units in order). Text = `text` of each line (whole line)."""
import csv
import json
import random
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
V5 = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
gold = {r["inv"] for r in csv.DictReader(open(A / "review/fresh_entry_sides.tsv"), delimiter="\t")}
for r in csv.DictReader(open(A / "planning/reads_entries.tsv"), delimiter="\t"):
    gold.add(r["inv"])
mc = json.loads((A / "step-19e/metrics_c.json").read_text())["per_inv"]
hand = sorted(mc, key=int)
cache = {}


def inv_data(inv):
    if inv not in cache:
        pages = {(r["scan_id"], r["page"]): r for r in read_tsv_gz(V5 / "layout/pages" / f"{inv}.tsv.gz")}
        units = list(read_tsv_gz(V5 / "units/units" / f"{inv}.tsv.gz"))
        segs = defaultdict(list)
        for r in read_tsv_gz(V5 / "units/segments" / f"{inv}.tsv.gz"):
            segs[(r["scan_id"], r["unit_id"])].append(r)
        txt = {(r["scan_id"], r["line_id"]): r["text"] for r in read_tsv_gz(V5 / "lines" / f"{inv}.lines.tsv.gz")}
        cache[inv] = (pages, units, segs, txt)
    return cache[inv]


def show(out, inv, u):
    pages, units, segs, txt = inv_data(inv)
    out.write(f"--- {inv} {u['scan_id']} {u['page']} {u['unit_id']} type={u['unit_type']} method={u['unit_method']} "
              f"col={u['layout_column']} rule={pages[(u['scan_id'], u['page'])]['rule']}\n")
    for s in sorted(segs[(u["scan_id"], u["unit_id"])], key=lambda r: int(r["seg_order"])):
        out.write("    " + txt.get((s["scan_id"], s["line_id"]), "") + "\n")


def pool(rule0=False):
    res = []
    for inv in hand:
        if inv in gold or not mc[inv]["flagged_sides"]:
            continue
        pages, units, _, _ = inv_data(inv)
        for u in units:
            p = pages[(u["scan_id"], u["page"])]
            if p["index_side"] == "1" and u["unit_type"] == "main_text" and (not rule0 or p["rule"] == "0"):
                res.append((f"{inv}:{u['unit_id']}", inv, u))
    return sorted(res, key=lambda t: t[0])


with open(A / "step-19e/error_reads.txt", "w") as out:
    for title, seed, k, r0 in (("10 units on flagged sides outside gold volumes", 20261122, 10, False),
                               ("5 units on gap-filled sides (rule 0)", 20261123, 5, True)):
        out.write(f"##### {title}\n")
        p = pool(r0)
        out.write(f"pool {len(p)}\n")
        for _, inv, u in random.Random(seed).sample(p, k):
            show(out, inv, u)
    big = sorted(((v["v5"] / max(v["v4"], 1), inv) for inv, v in mc.items() if v["flagged_sides"]), reverse=True)[:3]
    for _, inv in big:
        pages, units, _, _ = inv_data(inv)
        fl = sorted(k for k, r in pages.items() if r["index_side"] == "1")[0]
        out.write(f"##### largest change: {inv} {mc[inv]} side {fl}\n")
        for u in sorted(units, key=lambda r: int(r["unit_order"])):
            if (u["scan_id"], u["page"]) == fl:
                show(out, inv, u)
