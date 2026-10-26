"""Step 15w: for each inventory whose lower-case merge share is < 0.95 (reported failures), 5 offending merges
(right side upper-case, digit or other; seed 19) with context. Writes audit_failure_rows.tsv and prints category counts.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15w/audit_failure_rows.py
"""
import collections
import csv
import json
import random
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = json.loads(Path("reports/data/phase-12-hand-print-box-height/step-15w/audit_15w-a.json").read_text())
bad = [i["invariant"].split("]")[0][1:] for i in A["invariants"] if not i["pass"]]
roots = {inv: next(Path(f"data/text_layer_v2_dev/15w-a/{'1.10.94' if inv in ('455', '456') else '1.01.02'}").glob("NL-HaNA_*_v4"))
         for inv in bad}
rows, cats = [], collections.Counter()
for inv in bad:
    r = roots[inv]
    jun = {(x["scan_id"], x["unit_id"]): x for x in read_tsv_gz(r / "join" / "units" / f"{inv}.tsv.gz")}
    by = collections.defaultdict(list)
    for s in read_tsv_gz(r / "join" / "segments" / f"{inv}.tsv.gz"):
        by[(s["scan_id"], s["unit_id"])].append(s)
    wbc = next(iter(jun.values()))["word_break_chars"]
    off = []
    for k, segs in by.items():
        segs.sort(key=lambda s: int(s["seg_order"]))
        t = jun[k]["text"]
        for a, b in zip(segs, segs[1:]):
            if a["boundary_kind"] == "merge":
                rest = t[int(b["char_start"]):]
                ch = rest.lstrip(wbc)[:1]
                if not ch.islower():
                    off.append((k, t[max(0, int(b["char_start"]) - 30):int(b["char_start"])], rest[:30], ch))
    for k, left, right, ch in random.Random(19).sample(off, min(5, len(off))):
        cat = "upper" if ch.isupper() else "digit" if ch.isdigit() else "other"
        cats[cat] += 1
        rows.append({"inv": inv, "unit": k[1][-30:], "left": left, "right": right, "right_char": ch, "cat": cat})
with open(Path("reports/data/phase-12-hand-print-box-height/step-15w/audit_failure_rows.tsv"), "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t")
    w.writeheader()
    w.writerows(rows)
print(len(bad), "failing inventories,", len(rows), "rows;", dict(cats))
