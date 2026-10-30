"""Phase-15 planning: draw the fresh gold sides for step 21a (seed 20261030). Reads no label.

Pool: the 29 front-index volumes of the 19e-a layer that no phase-14 gold touched (pool_volumes.tsv).
Split by volume, stratified by the v1 share of front-index units without an L2 locator (>= 0.25 =
"high": mostly two-column interleaved indexes, planner peek 3444): high 13 -> 6 dev / 7 test,
low 16 -> 7 dev / 9 test. Per volume one `read` side and one `reserve` side, drawn from its front-index
sides (flagged sides up to the end of the front run) with >= 15 layout lines, excluding sides read
in phase 13 (19a entry_gold, 19g fresh_gold) and sides the planner printed (planner_seen.tsv).
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/planning/draw_sides.py
Writes reads_sides.tsv next to this file.
"""
import csv
import random
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
HERE = Path(__file__).parent
P13 = Path("reports/data/phase-13-register-index-volumes")
SEED = 20261030

pool = list(csv.DictReader(open(HERE / "pool_volumes.tsv"), delimiter="\t"))
seen = set()
for f in (P13 / "step-19a/entry_gold.tsv", P13 / "step-19g/fresh_gold.tsv"):
    for r in csv.DictReader(open(f), delimiter="\t"):
        seen.add((r["scan_id"], r["page"]))
for line in open(HERE / "planner_seen.tsv"):
    inv, scan, page = line.rstrip("\n").split("\t")
    seen.add((scan, page))

rng = random.Random(SEED)
high = sorted(r["inv"] for r in pool if float(r["no_loc_share"]) >= 0.25)
low = sorted(r["inv"] for r in pool if float(r["no_loc_share"]) < 0.25)
split = {}
for stratum, n_dev in ((high, 6), (low, 7)):
    order = stratum[:]
    rng.shuffle(order)
    for i, inv in enumerate(order):
        split[inv] = "dev" if i < n_dev else "test"

rows = []
for inv in sorted(split, key=int):
    vi = read_volume_inputs(L, inv)
    first, last, end = front_index_run(vi.flags)
    front = [vi.sides[i] for i in range(last + 1) if vi.flags[i]]
    n_lines = Counter((r["scan_id"], r["page"]) for r in read_tsv_gz(L / "layout/lines" / f"{inv}.tsv.gz"))
    cand = [s for s in front if s not in seen and n_lines[s] >= 15]
    pick = rng.sample(cand, 2)
    for role, side in zip(("read", "reserve"), pick):
        rows.append(dict(split=split[inv], stratum="high" if inv in high else "low", role=role, inv=inv,
                         scan_id=side[0], page=side[1], n_layout_lines=n_lines[side],
                         front_pos=front.index(side) + 1, n_front=len(front)))
rows.sort(key=lambda r: (r["split"] != "dev", r["role"] != "read", int(r["inv"])))
for i, r in enumerate(rows, 1):
    r["read_id"] = f"S{i:02d}"
cols = ["read_id", "split", "stratum", "role", "inv", "scan_id", "page", "n_layout_lines", "front_pos", "n_front"]
with open(HERE / "reads_sides.tsv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
    w.writeheader()
    w.writerows(rows)
c = Counter((r["split"], r["role"]) for r in rows)
print(dict(c))
for sp in ("dev", "test"):
    rd = [r for r in rows if r["split"] == sp and r["role"] == "read"]
    print(sp, "volumes", sorted((r["inv"] for r in rd), key=int), "read lines", sum(r["n_layout_lines"] for r in rd))
