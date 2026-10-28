"""Step 15y error analysis: 5 scans of 3463, units of 15x-a (print-built, split) next to 15y-b (hand pin).
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15y/reads_3463.py > reads_3463.txt"""
import csv
import gzip
import random
from collections import defaultdict

csv.field_size_limit(10 ** 9)
P = "data/text_layer_v2_dev/{}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/join/units/3463.tsv.gz"


def load(layer):
    by = defaultdict(list)
    for r in csv.DictReader(gzip.open(P.format(layer), "rt"), delimiter="\t"):
        by[r["scan_id"]].append(r)
    return by


a, b = load("15x-a"), load("15y-b")
cand = sorted(s for s, rows in a.items() if sum(r["unit_type"] == "main_text" for r in rows) >= 3)
print(f"# {len(cand)} scans with >= 3 main_text units in 15x-a (of {len(a)})")
for scan in random.Random(20261103).sample(cand, 5):
    print(f"\n=== {scan}: 15x-a {len(a[scan])} units ({sum(r['unit_type']=='main_text' for r in a[scan])} main_text), "
          f"15y-b {len(b[scan])} units ({sum(r['unit_type']=='main_text' for r in b[scan])} main_text)")
    for label, by in (("15x-a (print)", a), ("15y-b (hand pin)", b)):
        print(f"--- {label}")
        for r in by[scan]:
            print(f"[{r['unit_type']}] {r['text']}")
