"""Bar B2 sample (step 19f): 20 flagged + 20 other reordered regions, random.Random(20261125), writes b2_reads.txt.
Cases = layout/regions rows with change containing column_reordered, keyed by source_region_ids; the 40 source
regions of step-19b/b1_verdicts.tsv are dropped. Sorted by (int(inv), scan_id, source region id); flagged sampled
first, then other, from one Random. Also writes b2_cases.tsv (all cases, picked or not).
usage: show_order.py OUT.txt"""
import csv, random, sys
from collections import defaultdict
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *

def main(out):
    b1 = {r["region_id"] for r in csv.DictReader(open("reports/data/phase-13-register-index-volumes/step-19b/b1_verdicts.tsv"), delimiter="\t")}
    cases = {True: [], False: []}
    nb1 = 0
    for inv, clone, root in volumes():
        fl = flagged_sides(inv, root)
        for r in reorder_rows(inv, root):
            if r["source_region_ids"] in b1:
                nb1 += 1
                continue
            cases[(r["scan_id"], r["page"]) in fl].append((int(inv), r["scan_id"], r["source_region_ids"], inv, root, r["layout_region_id"]))
    print("dropped (in B1):", nb1)
    rng = random.Random(20261125)
    picked = []
    with open(out.replace("b2_reads.txt", "b2_cases.tsv"), "w") as f:
        f.write("side_class\tinv\tscan_id\tregion_id\n")
        for cls in (True, False):
            lst = sorted(cases[cls], key=lambda c: c[:3])
            print("flagged" if cls else "other", "reordered regions (excl. B1):", len(lst))
            for c in lst: f.write(f"{'flagged' if cls else 'other'}\t{c[0]}\t{c[1]}\t{c[2]}\n")
            picked += [("flagged" if cls else "other", c) for c in (rng.sample(lst, 20) if len(lst) > 20 else lst)]
    cache = {}
    with open(out, "w") as f:
        for n, (cls, (_, scan, src, inv, root, lrid)) in enumerate(picked, 1):
            if inv not in cache:
                cache = {inv: {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}}
            L = cache[inv]
            orig = sorted((r for r in L.values() if r["region_id"] == src), key=lambda r: int(r["line_idx"]))
            f.write(f"\n#### case {n:02d} [{cls}] inv {inv} {scan} region {src}\n-- original (XML order), {len(orig)} lines\n")
            for r in orig:
                f.write(f"  {r['x']:>5} {r['y']:>5} {r['w']:>5} | {r['text']}\n")
            ls = sorted((r for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz")
                         if r["scan_id"] == scan and r["layout_region_id"] == lrid), key=lambda r: int(r["layout_line_idx"]))
            reg = next(r for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz") if r["scan_id"] == scan and r["layout_region_id"] == lrid)
            sx = None
            f.write(f"-- new order ({len(ls)} lines)  [tag L/W/R; derived from x only for display]\n")
            # tags: recomputed from the geometry the rule used
            from archival_structures.analysis.page_structure import HAND_COLUMN_SPLIT_DEFAULTS as D
            import statistics
            rows = [L[r["line_id"]] for r in ls]
            T = [l for l in rows if l["text"].strip()]
            W = max(int(l["x"]) + int(l["w"]) for l in T) - min(int(l["x"]) for l in T)
            mh = statistics.median(int(l["h"]) for l in T)
            N = sorted((l for l in T if int(l["w"]) <= D["max_col_frac"] * W), key=lambda l: (int(l["x"]), int(l["y"]), l["line_id"]))
            gap, gi = max(((int(N[i + 1]["x"]) - int(N[i]["x"]), -i) for i in range(len(N) - 1)))
            sx = int(N[-gi + 1]["x"]); tol = D["tol_h"] * mh
            R = [l for l in T if int(l["x"]) >= sx - tol]
            wR = statistics.median(int(l["w"]) for l in R)
            for l in rows:
                if int(l["x"]) >= sx - tol: tag = "R"
                elif l["text"].strip() and int(l["x"]) + int(l["w"]) > sx + D["span_reach_frac"] * wR: tag = "W"
                else: tag = "L"
                f.write(f"  {tag} {l['x']:>5} {l['y']:>5} {l['w']:>5} | {l['text']}\n")

if __name__ == "__main__":
    main(sys.argv[1])
