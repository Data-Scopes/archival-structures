"""Step 19b: 10 interleaved units that remain after the split (random.Random(20261118) over sorted unit ids).
usage: show_remaining.py IDS.txt OUT.txt   (also prints, per unit, the guard that kept its source region whole,
recomputed on the lines of the region(s) the unit's lines came from)"""
import random, sys, statistics
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *
from archival_structures.analysis.page_structure import HAND_COLUMN_SPLIT_DEFAULTS as C

def guard(lines):
    T = [l for l in lines if l["text"].strip()]
    k = C["min_lines"]
    if len(T) < 2 * k: return f"min_lines (only {len(T)} non-empty lines)"
    x0 = min(l["x"] for l in T); W = max(l["x"] + l["w"] for l in T) - x0
    mh = statistics.median(l["h"] for l in T)
    N = sorted((l for l in T if l["w"] <= C["max_col_frac"] * W), key=lambda l: (l["x"], l["y"]))
    if len(N) < 2 * k: return f"narrow candidates < {2*k} ({len(N)} of {len(T)}, W={W})"
    g, i = max(((N[j+1]["x"] - N[j]["x"], -j) for j in range(len(N) - 1))); i = -i
    if g < C["min_gap_frac"] * W: return f"gap {g} < 0.25*W ({W})"
    sx = N[i+1]["x"]; tol = C["tol_h"] * mh
    L = [l for l in T if l["x"] + l["w"] <= sx + tol]
    R = [l for l in T if l["x"] >= sx - tol and not l["x"] + l["w"] <= sx + tol]
    S = [l for l in T if l not in L and l not in R]
    if len(L) < k or len(R) < k: return f"side count L{len(L)} R{len(R)}"
    if len(S) > C["max_span_frac"] * len(T): return f"span {len(S)}/{len(T)} > 0.34"
    for nm, p in (("L", L), ("R", R)):
        if statistics.median(l["w"] for l in p) < C["min_col_w_frac"] * W: return f"median width {nm}"
    ov = min(max(l["y"]+l["h"] for l in L), max(l["y"]+l["h"] for l in R)) - max(min(l["y"] for l in L), min(l["y"] for l in R))
    if ov < C["min_overlap_h"] * mh: return f"overlap {ov:.0f} < 2h"
    return "none (region would split; unit spans several regions or pieces)"

ids = sorted(l.split("\t") for l in open(sys.argv[1]).read().splitlines())
pick = random.Random(20261118).sample(ids, 10) if len(ids) > 10 else ids
vol = {inv: root for inv, _, root in volumes()}
with open(sys.argv[2], "w") as f:
    for n, (inv, uid) in enumerate(pick, 1):
        root = vol[inv]
        lines = {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        seg = sorted((int(r["seg_order"]), r["line_id"]) for r in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz") if r["unit_id"] == uid)
        ls = [lines[l] for _, l in seg]
        regs = {l["region_id"] for l in ls}
        allr = []
        for l in lines.values():
            if l["region_id"] in regs: allr.append({**l, "x": int(l["x"]), "y": int(l["y"]), "w": int(l["w"]), "h": int(l["h"])})
        f.write(f"\n#### unit {n:02d} inv {inv} {uid}\n-- source regions {len(regs)}, their lines {len(allr)}; guard: ")
        f.write("; ".join(guard([l for l in allr if l["region_id"] == r]) for r in sorted(regs)) + "\n")
        for l in ls:
            f.write(f"  {l['x']:>5} {l['y']:>5} {l['w']:>5} | {l['text']}\n")
