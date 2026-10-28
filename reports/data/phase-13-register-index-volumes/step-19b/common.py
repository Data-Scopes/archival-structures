"""Shared helpers for step 19b scripts."""
import csv
from collections import defaultdict
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
PLAN = Path("reports/data/phase-13-register-index-volumes/planning")
DEV = Path("data/text_layer_v2_dev")
CLONES = (("19b-a", "hand_15y-b.txt"), ("19b-z", "hand_15z-b.txt"))

def volumes():
    for clone, lst in CLONES:
        for inv in sorted((PLAN / lst).read_text().split(), key=int):
            yield inv, clone, DEV / clone / L4

def index_like_sides():
    """{(inv, scan_id, page)} of index-like sides: col_share >= 0.25 and max(end_num, date_start) >= 0.15."""
    out = set()
    with open(PLAN / "side_features.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["col_share"] != "" and float(r["col_share"]) >= 0.25 and \
                    max(float(r["end_num"] or 0), float(r["date_start"] or 0)) >= 0.15:
                out.add((r["inv"], r["scan_id"], r["page"]))
    return out

def split_groups(inv, root):
    """{source region id: [layout region rows]} for regions with a split_at_column piece."""
    g = defaultdict(list)
    for r in read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"):
        if "split_at_column" in r["change"]:
            g[(r["scan_id"], r["source_region_ids"])].append(r)
    return g
