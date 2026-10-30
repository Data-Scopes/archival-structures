"""Attach read_id/inv/scan_id/page/line_id to hand-decided labels and append them to line_gold.tsv.

Usage: write_side.py READ_ID LABELS_FILE
LABELS_FILE: one row per k of the side (all k, in order), fields separated by '|':
    k|col|label|entry|locators|note
The labels are my own decisions; this script only joins them to the line ids of show_side.py.
"""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import read_row, side_lines

D = os.path.dirname(os.path.abspath(__file__))
HDR = ["read_id", "inv", "scan_id", "page", "k", "line_id", "col", "label", "entry", "locators", "note"]
rid, lab = sys.argv[1], sys.argv[2]
r = read_row(rid)
sl = side_lines(r["inv"], r["scan_id"], r["page"])
given = [ln.rstrip("\n").split("|") for ln in open(lab) if ln.strip()]
assert [int(g[0]) for g in given] == list(range(1, len(sl) + 1)), f"k mismatch: {len(given)} vs {len(sl)}"
p = f"{D}/line_gold.tsv"
if os.path.exists(p):
    assert rid not in {x["read_id"] for x in csv.DictReader(open(p), delimiter="\t")}, "side already written"
fresh = not os.path.exists(p)
with open(p, "a", newline="") as f:
    w = csv.writer(f, delimiter="\t", lineterminator="\n")
    if fresh:
        w.writerow(HDR)
    for g, (_, lid, _) in zip(given, sl):
        g += [""] * (6 - len(g))
        w.writerow([rid, r["inv"], r["scan_id"], r["page"], g[0], lid, g[1], g[2], g[3], g[4], g[5]])
os.chmod(p, 0o664)
print(f"wrote {len(sl)} rows for {rid}")
