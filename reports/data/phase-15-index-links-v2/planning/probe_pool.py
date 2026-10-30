"""Phase-15 planning probe: the 29 untouched front-index volumes (no gold, no labels read).

Per volume: front-index sides, entries (main_text units on front sides), lines per side, L2
locators, units without a locator, unit_method counts. Also the chain ends of the 41 dev volumes
against their scan counts (20r-a build). Run from the AS root:
    .venv/bin/python -I reports/data/phase-15-index-links-v2/planning/probe_pool.py
Writes pool_volumes.tsv next to this file; prints a summary.
"""
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.analysis.index_links import (  # noqa: E402
    SG_LOCATOR_PARAMS, LocatorParams, extract_locators, front_index_run, read_volume_inputs)
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
HERE = Path(__file__).parent
SPENT = {"3254", "3308", "3394", "3471", "3256", "3320", "3352", "3401", "3456", "3590", "3611", "3712"}
ALL41 = ("3126 3254 3256 3265 3274 3280 3283 3290 3303 3307 3308 3315 3320 3349 3350 3352 3353 3368 3394 3401 "
         "3421 3428 3438 3444 3455 3456 3459 3471 3487 3493 3507 3522 3539 3547 3572 3587 3590 3611 3623 3630 "
         "3712").split()
POOL = [i for i in ALL41 if i not in SPENT]
L2 = SG_LOCATOR_PARAMS["L2"]

rows = []
meth_all = Counter()
for inv in POOL:
    vi = read_volume_inputs(L, inv)
    run = front_index_run(vi.flags)
    first, last, end = run
    front = {vi.sides[k] for k in range(last + 1) if vi.flags[k]}
    units = [u for u in read_tsv_gz(L / "units/units" / f"{inv}.tsv.gz")
             if (u["scan_id"], u["page"]) in front and u["unit_type"] == "main_text"]
    texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L / "join/units" / f"{inv}.tsv.gz")}
    nloc = [len(extract_locators(texts[(u["scan_id"], u["unit_id"])], L2)) for u in units]
    meth = Counter(u["unit_method"].split(":")[0] + ":" + u["unit_method"].split(":")[1] for u in units)
    meth_all.update(meth)
    nlines = Counter()
    for r in read_tsv_gz(L / "layout/lines" / f"{inv}.tsv.gz"):
        if (r["scan_id"], r["page"]) in front:
            nlines[(r["scan_id"], r["page"])] += 1
    rows.append(dict(inv=inv, n_scans=len(vi.scan_ids), n_sides=len(vi.sides), front_sides=len(front),
                     entries=len(units), entries_per_side=round(len(units) / len(front), 1),
                     lines_per_side=round(sum(nlines.values()) / len(front), 1),
                     no_loc_share=round(sum(1 for n in nloc if n == 0) / len(units), 3),
                     locators=sum(nloc)))
with open(HERE / "pool_volumes.tsv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t")
    w.writeheader()
    w.writerows(rows)
print("pool", len(POOL))
for r in rows:
    print("\t".join(str(v) for v in r.values()))
print("unit methods", meth_all.most_common(8))
print("totals: front sides", sum(r["front_sides"] for r in rows), "entries", sum(r["entries"] for r in rows),
      "locators", sum(r["locators"] for r in rows))

# chain ends vs scan counts (dev build 20r-a, 41 volumes) and full run (412)
for name, p, scans_root in [
        ("dev41", "data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1/volumes.tsv", L / "scans"),
        ("full412", "data/index_links_full/volumes.tsv",
         Path("data/text_layer_v5_full/1.01.02/NL-HaNA_1.01.02_text_layer_v5/scans"))]:
    vol = list(csv.DictReader(open(p), delimiter="\t"))
    over = []
    big = 0
    for v in vol:
        n = sum(1 for _ in read_tsv_gz(scans_root / f"{v['inv']}.scans.tsv.gz"))
        if n >= 1500:
            big += 1
        if v["chain_last"] and float(v["chain_last"]) > n:
            over.append((v["inv"], v["mode"], v["chain_last"], n))
    print(name, "volumes", len(vol), "chain_last > n_scans:", len(over), "n_scans>=1500:", big)
    print("  ", over)
