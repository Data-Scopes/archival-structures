"""Step 15ir acceptance measurements (criteria 2-5, 7) on a layer base: `accept.py LAYER_BASE_NAME` (15i-b or 15ir-a).
Reads data/text_layer_v2_dev/<name>/{hand,print}. Prints and writes accept_<name>.json."""
import gzip
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

name = sys.argv[1]
HERE = Path(__file__).parent
BASE = Path("data/text_layer_v2_dev") / name
HAND = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
PRINT = ["455", "456"]


def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]


def by_(l):
    """mean baseline y (the sort key of the layout), else the box centre."""
    pts = [tuple(map(int, p.split(","))) for p in l["baseline"].split()] if l["baseline"] else []
    return sum(p[1] for p in pts) / len(pts) if pts else int(l["y"]) + int(l["h"]) / 2


def root(kind):
    return next((BASE / kind).glob("*_text_layer_v2"))


out = {}
for kind, invs in (("print", PRINT), ("hand", HAND)):
    r = root(kind)
    for inv in invs:
        lines = {(x["scan_id"], x["line_id"]): x for x in rd(r / "lines" / f"{inv}.lines.tsv.gz")}
        regs = rd(r / "layout" / "regions" / f"{inv}.tsv.gz")
        llines = rd(r / "layout" / "lines" / f"{inv}.tsv.gz")
        by_reg = defaultdict(list)
        for l in llines:
            by_reg[(l["scan_id"], l["layout_region_id"])].append(l)
        jump_by_change = Counter(); jump_bl = Counter()
        for g in regs:
            ls = sorted(by_reg.get((g["scan_id"], g["layout_region_id"]), []), key=lambda l: int(l["layout_line_idx"]))
            ys = [int(lines[(l["scan_id"], l["line_id"])]["y"]) for l in ls if lines[(l["scan_id"], l["line_id"])]["text"].strip()]
            if any(b < a - 50 for a, b in zip(ys, ys[1:])):
                jump_by_change[g["change"]] += 1
            yb = [by_(lines[(l["scan_id"], l["line_id"])]) for l in ls
                  if lines[(l["scan_id"], l["line_id"])]["text"].strip()]
            if any(b < a - 50 for a, b in zip(yb, yb[1:])):
                jump_bl[g["change"]] += 1
        # units with a jump (segment order inside the unit)
        units = rd(r / "units" / "units" / f"{inv}.tsv.gz")
        useg = defaultdict(list)
        for s in rd(r / "units" / "segments" / f"{inv}.tsv.gz"):
            useg[(s["scan_id"], s["unit_id"])].append(s)
        n_unit_jump = n_unit_jump_bl = 0
        for u in units:
            ss = sorted(useg[(u["scan_id"], u["unit_id"])], key=lambda s: int(s["seg_order"]))
            seen = []
            for s in ss:
                if not seen or seen[-1] != s["line_id"]:
                    seen.append(s["line_id"])
            ys = [int(lines[(u["scan_id"], lid)]["y"]) for lid in seen]
            if any(b < a - 50 for a, b in zip(ys, ys[1:])):
                n_unit_jump += 1
            yb = [by_(lines[(u["scan_id"], lid)]) for lid in seen]
            if any(b < a - 50 for a, b in zip(yb, yb[1:])):
                n_unit_jump_bl += 1
        segs = rd(r / "types" / "segments" / f"{inv}.tsv.gz")
        tl = {(x["scan_id"], x["line_id"]): x["text"] for x in segs[:0]}
        date_line = re.compile(r"\W*(" + "|".join([]) + r")")  # placeholder, replaced below
        methods = Counter(s["type_method"] for s in segs)
        lst = json.load(open(r / "layout" / "stats.json"))[inv]
        ust = json.load(open(r / "units" / "stats.json"))[inv]
        jst = json.load(open(r / "join" / "stats" / f"{inv}.json"))
        out[inv] = {
            "regions_with_backward_jump": dict(jump_by_change), "n_regions_with_jump": sum(jump_by_change.values()),
            "regions_with_backward_jump_baseline": dict(jump_bl), "n_units": len(units),
            "n_units_with_jump": n_unit_jump, "n_units_with_jump_baseline": n_unit_jump_bl,
            "pn_line": methods.get("rule:pn_line", 0), "pn_line_offchain": methods.get("rule:pn_line_offchain", 0),
            "date_prefix": methods.get("rule:date_prefix", 0),
            "n_guard_blocked": lst["n_guard_blocked"], "n_column_blocked": lst["n_column_blocked"],
            "n_id_collisions": ust["n_id_collisions"], "old_status": ust["old_status"],
            "n_offset_violations": jst["n_offset_violations"]}
json.dump(out, open(HERE / f"accept_{name}.json", "w"), indent=1)
for inv, v in out.items():
    print(inv, v)
