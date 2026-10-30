"""Draw link targets for every judged `index` side that has no rows in targets.tsv yet.

Deterministic and idempotent: candidates = entries with >= 1 locator (entry-id order); draw
2 per dev side / 4 per test side (all if fewer) with random.Random(f"21a-{read_id}").sample;
append to targets.tsv, numbering T001... in read_id order (continuing after existing rows).
Run it only after the sides' lines are in line_gold.tsv and before any target scan is opened.
Usage: draw_targets.py [--split dev|test]    (run from the AS root; --split limits the sides drawn)
"""
import csv
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import read_row

D = os.path.dirname(os.path.abspath(__file__))
HDR = ["target_id", "read_id", "split", "inv", "entry", "first_line_id", "target_locator"]


def rows(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


SPLIT = sys.argv[sys.argv.index('--split') + 1] if '--split' in sys.argv else None
verd = rows(f"{D}/side_verdicts.tsv")
gold = rows(f"{D}/line_gold.tsv")
have = rows(f"{D}/targets.tsv")
done = {r["read_id"] for r in have}
nxt = max([int(r["target_id"][1:]) for r in have] + [0]) + 1
judged = sorted({r["read_id"] for r in verd if r["side_verdict"] == "index"} - done,
                key=lambda x: int(x[1:]))
new = []
for rid in judged:
    if SPLIT and read_row(rid)['split'] != SPLIT:
        continue
    ls = sorted((r for r in gold if r["read_id"] == rid), key=lambda r: int(r["k"]))
    ent = {}      # entry -> list of locators (text order)
    first = {}    # entry -> first line id (the S line)
    for r in ls:
        if r["label"] == "S" and r["entry"]:
            first[int(r["entry"])] = r["line_id"]
        for tok in filter(None, r["locators"].split(";")):
            if "@" in tok:
                n, e = tok.split("@")
                ent.setdefault(int(e), []).append(n)
            elif r["entry"]:
                ent.setdefault(int(r["entry"]), []).append(tok)
    cands = sorted(e for e in ent if e in first)
    split = read_row(rid)["split"]
    n = 2 if split == "dev" else 4
    pick = sorted(random.Random(f"21a-{rid}").sample(cands, min(n, len(cands))))
    for e in pick:
        new.append({"target_id": f"T{nxt:03d}", "read_id": rid, "split": split,
                    "inv": read_row(rid)["inv"], "entry": e, "first_line_id": first[e],
                    "target_locator": ent[e][0]})
        nxt += 1
if new:
    p = f"{D}/targets.tsv"
    fresh = not os.path.exists(p)
    with open(p, "a", newline="") as f:
        w = csv.DictWriter(f, HDR, delimiter="\t", lineterminator="\n")
        if fresh:
            w.writeheader()
        w.writerows(new)
    os.chmod(p, 0o664)
print(f"drew {len(new)} targets for {len(judged)} new sides")
