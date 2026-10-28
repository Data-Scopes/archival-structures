"""Step 19f: 10 interleaved units that remain after the reorder (random.Random(20261126) over sorted unit ids).
usage: show_remaining.py IDS.txt OUT.txt   (prints, per unit, which reorder guard kept its source region(s) whole,
recomputed on the lines of the region(s) the unit's lines came from, and whether the region was reordered)"""
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
    N = sorted((l for l in T if l["w"] <= C["max_col_frac"] * W), key=lambda l: (l["x"], l["y"], l["line_id"]))
    if len(N) < 2 * k: return f"narrow candidates < {2*k} ({len(N)} of {len(T)}, W={W})"
    g, i = max(((N[j+1]["x"] - N[j]["x"], -j) for j in range(len(N) - 1))); i = -i
    if g < C["min_gap_frac"] * W: return f"gap {g} < 0.25*W ({W})"
    sx = N[i+1]["x"]; tol = C["tol_h"] * mh
    R = [l for l in T if l["x"] >= sx - tol]
    if len(R) < k: return f"R lines {len(R)} < {k}"
    wR = statistics.median(l["w"] for l in R)
    LT = [l for l in T if l["x"] < sx - tol]
    wide = [l for l in LT if l["x"] + l["w"] > sx + C["span_reach_frac"] * wR]
    Ln = [l for l in LT if l not in wide]
    if len(Ln) < k: return f"Ln {len(Ln)} < {k}"
    if len(wide) > C["max_span_frac"] * len(T): return f"wide {len(wide)}/{len(T)} > 0.34"
    for nm, p in (("Ln", Ln), ("R", R)):
        if statistics.median(l["w"] for l in p) < C["min_col_w_frac"] * W: return f"median width {nm}"
    ov = min(max(l["y"]+l["h"] for l in Ln), max(l["y"]+l["h"] for l in R)) - max(min(l["y"] for l in Ln), min(l["y"] for l in R))
    if ov < C["min_overlap_h"] * mh: return f"overlap {ov:.0f} < 2h"
    return "none (all guards pass)"

ids = sorted((l.split("\t") for l in open(sys.argv[1]).read().splitlines()), key=lambda t: (int(t[0]), t[1]))
pick = random.Random(20261126).sample(ids, 10) if len(ids) > 10 else ids
vol = {inv: root for inv, _, root in volumes()}
with open(sys.argv[2], "w") as f:
    for n, (inv, uid) in enumerate(pick, 1):
        root = vol[inv]
        lines = {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        seg = sorted((int(r["seg_order"]), r["line_id"]) for r in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz") if r["unit_id"] == uid)
        ls = [lines[l] for _, l in seg]
        regs = {l["region_id"] for l in ls}
        reord = {r["source_region_ids"] for r in reorder_rows(inv, root)}
        allr = []
        for l in lines.values():
            if l["region_id"] in regs: allr.append({**l, "x": int(l["x"]), "y": int(l["y"]), "w": int(l["w"]), "h": int(l["h"])})
        f.write(f"\n#### unit {n:02d} inv {inv} {uid}\n-- source regions {len(regs)} (reordered: {sum(r in reord for r in regs)}), their lines {len(allr)}; guard: ")
        f.write("; ".join(guard([l for l in allr if l["region_id"] == r]) for r in sorted(regs)) + "\n")
        for l in ls:
            f.write(f"  {l['x']:>5} {l['y']:>5} {l['w']:>5} | {l['text']}\n")

# second part: guard recomputed on the LAYOUT regions the unit's lines sit in (what the code saw)
with open(sys.argv[2].replace(".txt", "_layout.txt"), "w") as f:
    for n, (inv, uid) in enumerate(pick, 1):
        root = vol[inv]
        lines = {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        seg = [r["line_id"] for r in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz") if r["unit_id"] == uid]
        lay = {}
        for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"):
            lay[r["line_id"]] = (r["scan_id"], r["layout_region_id"])
        keys = {lay[l] for l in seg}
        reg = {(r["scan_id"], r["layout_region_id"]): r for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz")}
        by = {}
        for lid, k in lay.items():
            if k in keys: by.setdefault(k, []).append({**lines[lid], "x": int(lines[lid]["x"]), "y": int(lines[lid]["y"]), "w": int(lines[lid]["w"]), "h": int(lines[lid]["h"])})
        for k in sorted(keys):
            f.write(f"unit {n:02d} layout region {k[1][-40:]} change={reg[k]['change']} lines={len(by[k])} unit_lines_in_it={sum(lay[l]==k for l in seg)} guard: {guard(by[k])}\n")
