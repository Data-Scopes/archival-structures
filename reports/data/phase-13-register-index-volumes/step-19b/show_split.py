"""Bar B1 sample (step 19b): 20 index-like + 20 other split regions, random.Random(20261117), writes b1_reads.txt.
Sorted by (int(inv), scan_id, source region id); index-like sampled first, then other, from one Random.
usage: show_split.py OUT.txt"""
import random, sys
from collections import defaultdict
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *

def main(out):
    il = index_like_sides()
    cases = {True: [], False: []}
    for inv, clone, root in volumes():
        for (scan, src), pcs in split_groups(inv, root).items():
            cases[(inv, scan, pcs[0]["page"]) in il].append((int(inv), scan, src, inv, root))
    rng = random.Random(20261117)
    picked = []
    for cls in (True, False):
        lst = sorted(cases[cls], key=lambda c: c[:3])
        print("index-like" if cls else "other", "split regions:", len(lst))
        picked += [("index-like" if cls else "other", c) for c in (rng.sample(lst, 20) if len(lst) > 20 else lst)]
    cache = {}
    with open(out, "w") as f:
        for n, (cls, (_, scan, src, inv, root)) in enumerate(picked, 1):
            if inv not in cache:
                cache = {inv: {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}}
            L = cache[inv]
            orig = sorted((r for r in L.values() if r["region_id"] == src), key=lambda r: int(r["line_idx"]))
            f.write(f"\n#### case {n:02d} [{cls}] inv {inv} {scan} region {src}\n-- original (XML order), {len(orig)} lines\n")
            for r in orig:
                f.write(f"  {r['x']:>5} {r['y']:>5} {r['w']:>5} | {r['text']}\n")
            pcs = split_groups(inv, root)[(scan, src)]
            lay = defaultdict(list)
            for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"):
                if r["scan_id"] == scan:
                    lay[r["layout_region_id"]].append(r)
            for pi, p in enumerate(pcs):
                ls = sorted(lay[p["layout_region_id"]], key=lambda r: int(r["layout_line_idx"]))
                f.write(f"-- piece {pi} layout_column {p['layout_column']} role {p['layout_role']} ({len(ls)} lines)\n")
                for r in ls:
                    l = L[r["line_id"]]
                    f.write(f"  {l['x']:>5} {l['y']:>5} {l['w']:>5} | {l['text']}\n")

if __name__ == "__main__":
    main(sys.argv[1])
