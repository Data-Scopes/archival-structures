"""Planner peek: print one front-index side with unit boundaries and L2 locators.
Sides printed here are excluded from the 21a draw (planner_seen.tsv). Usage: peek_side.py INV K (K-th front side)."""
import sys
from pathlib import Path
sys.path.insert(0, ".")
from archival_structures.analysis.index_links import SG_LOCATOR_PARAMS, extract_locators, front_index_run, read_volume_inputs
from archival_structures.text_layer.io import read_tsv_gz
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
inv, k = sys.argv[1], int(sys.argv[2])
vi = read_volume_inputs(L, inv)
first, last, end = front_index_run(vi.flags)
front = [vi.sides[i] for i in range(last + 1) if vi.flags[i]]
side = front[k]
with open(Path(__file__).parent / "planner_seen.tsv", "a") as f:
    f.write(f"{inv}\t{side[0]}\t{side[1]}\n")
texts = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(L / "join/units" / f"{inv}.tsv.gz")}
print(side)
for u in read_tsv_gz(L / "units/units" / f"{inv}.tsv.gz"):
    if (u["scan_id"], u["page"]) == side:
        t = texts.get((u["scan_id"], u["unit_id"]), "")
        locs = [l.number for l in extract_locators(t, SG_LOCATOR_PARAMS["L2"])] if u["unit_type"] == "main_text" else ""
        print(f"[{u['unit_type'][:4]} {u['unit_method'][:16]:16} col{u['layout_column']}] {t[:110]!r} {locs}")
