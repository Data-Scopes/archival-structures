"""Phase-13 review (2026-10-28): draw fresh entry-gold sides for the conditional step 19g.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/draw_fresh.py

Population: page sides flagged ``index_side == 1`` by the 19c detector (V05) in the clones ``19c-a``/``19c-z``, with
``n_lines >= 20`` (planning ``side_features.tsv``), minus the 38 sides of ``planning/reads_entries.tsv``. Groups as in
``planning/draw_reads.py`` (G1 GT register index sections, G2 late register fronts, G3 tweede serie index volumes, G4
other index books). Quota G1 5, G2 5, G3 6, G4 4 read sides plus 2 reserves per group. Volumes: those without a
19a read side first (all volumes of the group if fewer than 2 such volumes have candidates); round robin over the
shuffled volumes, one random side per turn. Seed 20261124. Writes ``fresh_entry_sides.tsv`` (read order shuffled so
the groups mix; reserves last within their group). No label or prediction is read except the detector flag.
"""
import csv
import random
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

A = Path("reports/data/phase-13-register-index-volumes")
sys.path.insert(0, str(A / "planning"))
import draw_reads as dr  # noqa: E402  (group constants only)

DEV = Path("data/text_layer_v2_dev")
L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
CLONE = {"15y-b": "19c-a", "15z-b": "19c-z"}
QUOTA = {"G1": 5, "G2": 5, "G3": 6, "G4": 4}
N_RESERVE = 2


def main():
    feats = pd.read_csv(A / "planning/side_features.tsv", sep="\t", dtype=str, keep_default_na=False,
                        usecols=["inv", "layer", "scan_id", "page", "n_lines", "gt_label"])
    gt_index_vols = sorted(set(feats.loc[feats.gt_label == "index_page", "inv"]), key=int)
    gt_vols = set(feats.loc[feats.gt_label != "", "inv"])
    old = list(csv.DictReader(open(A / "planning/reads_entries.tsv"), delimiter="\t"))
    old_sides = {(r["inv"], r["scan_id"], r["page"]) for r in old}
    old_read_vols = {r["inv"] for r in old if r["role"] == "read"}
    flags = []
    for layer, clone in CLONE.items():
        lst = "hand_15y-b.txt" if layer == "15y-b" else "hand_15z-b.txt"
        for inv in (A / "planning" / lst).read_text().split():
            p = pd.read_csv(DEV / clone / L4 / "layout" / "pages" / f"{inv}.tsv.gz", sep="\t", dtype=str,
                            keep_default_na=False, usecols=["scan_id", "page", "index_side"])
            p = p[p.index_side == "1"]
            p.insert(0, "inv", inv)
            flags.append(p)
    flags = pd.concat(flags, ignore_index=True)
    cand = flags.merge(feats, on=["inv", "scan_id", "page"], how="left")
    cand = cand[cand.n_lines.astype(int) >= 20]
    cand = cand[[(i, s, p) not in old_sides for i, s, p in zip(cand.inv, cand.scan_id, cand.page)]]
    by_inv = defaultdict(list)
    for r in cand.sort_values(["inv", "scan_id", "page"]).to_dict("records"):
        by_inv[r["inv"]].append(r)
    groups = {
        "G1": [i for i in gt_index_vols if i not in dr.GT_OTHER_INDEX],
        "G2": [i for i in dr.LATE_REGISTERS if i not in gt_vols],
        "G3": list(dr.INDEX_VOLS),
        "G4": dr.GT_OTHER_INDEX + dr.OTHER_INDEX_VOLS,
    }
    rng = random.Random(20261124)
    rows = []
    for g, vols in groups.items():
        vols = [v for v in vols if by_inv[v]]
        fresh = [v for v in vols if v not in old_read_vols]
        pool_vols = fresh if len(fresh) >= 2 else vols
        pool_vols = sorted(pool_vols, key=int)
        rng.shuffle(pool_vols)
        need = QUOTA[g] + N_RESERVE
        chosen, k = [], 0
        while len(chosen) < need and any(r not in chosen for v in pool_vols for r in by_inv[v]):
            v = pool_vols[k % len(pool_vols)]
            pool = [r for r in by_inv[v] if r not in chosen]
            if pool:
                chosen.append(rng.choice(pool))
            k += 1
        for j, r in enumerate(chosen):
            rows.append(dict(group=g, role="read" if j < QUOTA[g] else "reserve", inv=r["inv"], layer=r["layer"],
                             scan_id=r["scan_id"], page=r["page"], n_lines=r["n_lines"],
                             pool_volumes=" ".join(sorted(pool_vols, key=int)),
                             fresh_volumes=int(pool_vols == sorted(fresh, key=int) or set(pool_vols) <= set(fresh))))
    reads = [r for r in rows if r["role"] == "read"]
    rng.shuffle(reads)
    reserves = [r for r in rows if r["role"] == "reserve"]
    out = reads + reserves
    for k, r in enumerate(out, 1):
        r["read_id"] = 100 + k
    cols = ["read_id", "group", "role", "inv", "layer", "scan_id", "page", "n_lines", "fresh_volumes", "pool_volumes"]
    with open(A / "review/fresh_entry_sides.tsv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(out)
    print(pd.DataFrame(out).groupby(["group", "role"]).size().to_string())
    print("flagged candidate sides:", len(cand), "volumes:", cand.inv.nunique())


if __name__ == "__main__":
    main()
