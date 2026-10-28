"""Prototype of the hand column split on PageXML regions (raw lines). args: inv [show]"""
import sys, json, random
from collections import defaultdict, Counter
from pathlib import Path
from statistics import median
from archival_structures.text_layer.io import read_tsv_gz
DEV = Path("data/text_layer_v2_dev"); L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
P = dict(min_lines=3, min_gap_frac=0.25, max_col_frac=0.65, min_col_w_frac=0.3, tol_h=0.5, min_overlap_h=2.0)
def root(inv):
    for t in ("15y-b", "15z-b"):
        p = DEV / t / L4 / "mode" / f"{inv}.json"
        if p.exists() and json.loads(p.read_text())["mode"] == "hand":
            return DEV / t / L4
def split(lines):
    L = [l for l in lines if l["text"].strip()]
    if len(L) < 2 * P["min_lines"]: return None
    x0 = min(l["x"] for l in L); x1 = max(l["x"] + l["w"] for l in L); W = x1 - x0
    mh = median(l["h"] for l in L)
    N = sorted((l for l in L if l["w"] <= P["max_col_frac"] * W), key=lambda l: l["x"])
    if len(N) < 2 * P["min_lines"]: return None
    gaps = [(N[i+1]["x"] - N[i]["x"], i) for i in range(len(N) - 1)]
    g, i = max(gaps)
    if g < P["min_gap_frac"] * W: return None
    sx = N[i+1]["x"]; tol = P["tol_h"] * mh
    left = [l for l in L if l["x"] + l["w"] <= sx + tol]
    right = [l for l in L if l["x"] >= sx - tol and l not in left]
    span = [l for l in L if l not in left and l not in right]
    if len(left) < P["min_lines"] or len(right) < P["min_lines"]: return None
    if len(span) > 0.34 * len(L): return None
    if median(l["w"] for l in left) < P["min_col_w_frac"] * W or median(l["w"] for l in right) < P["min_col_w_frac"] * W: return None
    ov = min(max(l["y"] + l["h"] for l in left), max(l["y"] + l["h"] for l in right)) - max(min(l["y"] for l in left), min(l["y"] for l in right))
    if ov < P["min_overlap_h"] * mh: return None
    return left, right, span
inv = sys.argv[1]; show = len(sys.argv) > 2
regs = defaultdict(list)
for l in read_tsv_gz(root(inv) / "lines" / f"{inv}.lines.tsv.gz"):
    for k in ("x", "y", "w", "h"): l[k] = int(l[k])
    regs[(l["scan_id"], l["region_id"])].append(l)
res = {k: split(v) for k, v in regs.items()}
hit = [k for k, v in res.items() if v]
nl = sum(len(regs[k]) for k in hit); ns = sum(len(res[k][2]) for k in hit)
scans = Counter(k[0][-4:] for k in hit)
print(f"{inv}: regions {len(regs)}, split {len(hit)}, lines in split regions {nl}, span lines {ns}; scans {len(scans)} first {sorted(scans)[:3]} last {sorted(scans)[-3:]}")
if show:
    hit2 = [k for k in hit if int(k[0][-4:]) > int(sys.argv[3])] if len(sys.argv) > 3 else hit
    for k in random.Random(sys.argv[2]).sample(hit2, min(3, len(hit2))):
        left, right, span = res[k]
        print("==", k[0][-9:], f"L{len(left)} R{len(right)} S{len(span)}")
        for nm, part in (("L", left), ("R", right), ("S", span)):
            for l in sorted(part, key=lambda l: l["y"])[:4]: print(f"   {nm} x={l['x']} y={l['y']} w={l['w']} | {l['text'][:60]}")
