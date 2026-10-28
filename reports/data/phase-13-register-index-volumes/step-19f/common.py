"""Shared helpers for step 19f scripts."""
import csv
from collections import defaultdict
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
PLAN = Path("reports/data/phase-13-register-index-volumes/planning")
DEV = Path("data/text_layer_v2_dev")
CLONES = (("19f-a", "hand_15y-b.txt"), ("19f-z", "hand_15z-b.txt"))

def volumes():
    for clone, lst in CLONES:
        for inv in sorted((PLAN / lst).read_text().split(), key=int):
            yield inv, clone, DEV / clone / L4

def flagged_sides(inv, root):
    """{(scan_id, page)} with index_side == '1' in layout/pages."""
    return {(r["scan_id"], r["page"]) for r in read_tsv_gz(root / "layout" / "pages" / f"{inv}.tsv.gz")
            if r.get("index_side") == "1"}

def reorder_rows(inv, root):
    """layout/regions rows whose change contains column_reordered."""
    return [r for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz") if "column_reordered" in r["change"]]

def tag_lines(rows):
    """Tags 'L'/'W'/'R' for line rows (dicts with x, y, w, h, text) of a reordered region, recomputed with the rule's geometry."""
    import statistics
    from archival_structures.analysis.page_structure import HAND_COLUMN_SPLIT_DEFAULTS as D
    T = [l for l in rows if l["text"].strip()]
    W = max(int(l["x"]) + int(l["w"]) for l in T) - min(int(l["x"]) for l in T)
    mh = statistics.median(int(l["h"]) for l in T)
    N = sorted((l for l in T if int(l["w"]) <= D["max_col_frac"] * W), key=lambda l: (int(l["x"]), int(l["y"]), l["line_id"]))
    gap, gi = max(((int(N[i + 1]["x"]) - int(N[i]["x"]), -i) for i in range(len(N) - 1)))
    sx = int(N[-gi + 1]["x"]); tol = D["tol_h"] * mh
    R = [l for l in T if int(l["x"]) >= sx - tol]
    wR = statistics.median(int(l["w"]) for l in R)
    out = []
    for l in rows:
        if int(l["x"]) >= sx - tol: out.append("R")
        elif l["text"].strip() and int(l["x"]) + int(l["w"]) > sx + D["span_reach_frac"] * wR: out.append("W")
        else: out.append("L")
    return out
