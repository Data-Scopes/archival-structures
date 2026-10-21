"""Step 15fq2 acceptance numbers (criteria 1-6) over the 16 inventories.

Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq2/acceptance.py
Compares data/text_layer_v2_dev/15fq-c with 15fq-b and 15fq2-noguard. Writes acceptance.json
(next to this script) and prints the tables.
"""

import json
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import regions_overlap

from archival_structures.analysis.page_structure import _guard_blocks

PARENT = Path("data/text_layer_v2_dev")
NEW, OLD, NOGUARD = PARENT / "15fq-c", PARENT / "15fq-b", PARENT / "15fq2-noguard"
HERE = Path(__file__).parent
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
COLS = {
    "scans": ["scan_id", "page_mode", "separation_x", "split_method", "n_lines_crossing",
              "n_regions_in", "n_regions_out", "n_merges", "n_gutter_splits", "gutter_rule",
              "n_column_splits", "n_furniture", "n_guard_blocked"],
    "regions": ["scan_id", "page", "layout_region_id", "x", "y", "w", "h", "layout_column",
                "layout_role", "n_lines", "source_region_ids", "change", "furniture",
                "column_value"],
    "lines": ["scan_id", "line_id", "region_id", "page", "layout_region_id", "layout_line_idx",
              "layout_column", "layout_role"],
}


def table(base, layer, name, kind, inv):
    root = base / layer / name
    path = root / "lines" / f"{inv}.lines.tsv.gz" if kind == "src_lines" \
        else root / "layout" / kind / f"{inv}.tsv.gz"
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)


class Box:
    def __init__(self, x, y, w, h):
        self.coords = pdm.Coords.coords_from_box_params(int(x), int(y), int(w), int(h))


def mixed_regions(regs, lines, src):
    col = dict(zip(zip(src.scan_id, src.line_id), src["column"]))
    vals = defaultdict(set)
    for s, l, r in zip(lines.scan_id, lines.line_id, lines.layout_region_id):
        if col[(s, l)]:
            vals[(s, r)].add(col[(s, l)])
    return vals, col


def inventory(layer, name, inv):
    res = {}
    src = table(NEW, layer, name, "src_lines", inv)
    scans, regs, lines = (table(NEW, layer, name, k, inv) for k in ("scans", "regions", "lines"))
    # 1 conservation and ids
    res["c1_conservation_ok"] = (Counter(zip(src.scan_id, src.line_id)) ==
                                 Counter(zip(lines.scan_id, lines.line_id)) and
                                 not lines.duplicated(["scan_id", "line_id"]).any())
    res["c1_dup_layout_region_ids"] = int(regs.duplicated(["scan_id", "layout_region_id"]).sum())
    un = regs[regs.change == "unchanged"]
    res["c1_unchanged_id_mismatch"] = int((un.layout_region_id != un.source_region_ids).sum())
    res["n_column_blocked"] = int(pd.to_numeric(scans.n_column_blocked).sum())
    res["n_guard_blocked"] = int(pd.to_numeric(scans.n_guard_blocked).sum())
    # 2 hand unchanged / 3 noguard regression
    ref = NOGUARD if layer == "print" else OLD
    eq = {}
    for kind, cols in COLS.items():
        a = table(NEW if layer != "print" else NOGUARD, layer, name, kind, inv)[cols]
        b = table(OLD, layer, name, kind, inv)[cols] if True else None
        eq[kind] = bool(a.equals(b))
    res["c23_equal_to_15fq-b" + ("_noguard" if layer == "print" else "")] = eq
    # 4 column purity
    vals, col = mixed_regions(regs, lines, src)
    res["c4_regions_with_both_columns"] = sum(len(v) > 1 for v in vals.values())
    nonempty = regs[regs.layout_role != "empty"]
    # 5 furniture and residuals
    res["c5_furniture_regions"] = int((nonempty.furniture == "1").sum())
    res["c5_furniture_merged"] = int(((nonempty.furniture == "1") &
                                      nonempty.change.str.contains("merged")).sum())
    srcg = src.set_index(["scan_id", "line_id"])
    geo = defaultdict(list)
    for s, l, r in zip(lines.scan_id, lines.line_id, lines.layout_region_id):
        geo[(s, r)].append((s, l))
    fur = set(zip(*[nonempty[nonempty.furniture == "1"][c] for c in ("scan_id", "layout_region_id")]))
    by_page = defaultdict(list)
    for r in nonempty.itertuples():
        by_page[(r.scan_id, r.page)].append(r)
    resid, detail = Counter(), []
    for (sid, page), rs in by_page.items():
        boxes = [Box(r.x, r.y, r.w, r.h) for r in rs]
        for i, j in combinations(range(len(rs)), 2):
            if not regions_overlap(boxes[i], boxes[j], 0.5):
                continue
            a, b = rs[i], rs[j]
            kind = "other"
            if (sid, a.layout_region_id) in fur or (sid, b.layout_region_id) in fur:
                kind = "furniture"
            else:
                objs = []
                for r in (a, b):
                    ls = [srcg.loc[k] for k in geo[(sid, r.layout_region_id)]]
                    objs.append(SimpleNamespace(lines=[SimpleNamespace(coords=SimpleNamespace(
                        w=int(x["w"]), y=int(x["y"]), h=int(x["h"]))) for x in ls]))
                if _guard_blocks(objs[0], objs[1], 0.5):
                    kind = "guard"
                elif len(vals.get((sid, a.layout_region_id), set()) |
                         vals.get((sid, b.layout_region_id), set())) > 1:
                    kind = "column"
            resid[kind] += 1
            if kind == "other":
                detail.append((sid, page, a.layout_region_id, b.layout_region_id))
    res["c5_residual_pairs"] = dict(resid)
    res["c5_residual_other"] = detail[:20]
    # 6 locality (print only)
    if layer == "print":
        oreg = table(OLD, layer, name, "regions", inv)
        olines = table(OLD, layer, name, "lines", inv)
        ovals, _ = mixed_regions(oreg, olines, src)
        mixed = {k for k, v in ovals.items() if len(v) > 1}
        res["c6_mixed_regions_15fq-b"] = len(mixed)
        mixed_sources = {}
        for r in oreg.itertuples():
            if (r.scan_id, r.layout_region_id) in mixed:
                mixed_sources[(r.scan_id, r.layout_region_id)] = set(r.source_region_ids.split())
        okeys = set(zip(oreg.scan_id, oreg.layout_region_id))
        nkeys = set(zip(regs.scan_id, regs.layout_region_id))
        new = regs[[k not in okeys for k in zip(regs.scan_id, regs.layout_region_id)]]
        res["c6_new_regions"] = len(new)
        res["c6_lost_regions"] = len(okeys - nkeys)
        exc = []
        for r in new.itertuples():
            srcs = set(r.source_region_ids.split())
            if not any(k[0] == r.scan_id and srcs <= v for k, v in mixed_sources.items()):
                exc.append((r.scan_id, r.layout_region_id, r.source_region_ids))
        res["c6_locality_exceptions"] = len(exc)
        res["c6_locality_exception_list"] = exc[:20]
        lost = okeys - nkeys
        res["c6_lost_not_mixed"] = sum(k not in mixed for k in lost)
    return res


def main():
    out = {}
    for layer, (name, invs) in LAYERS.items():
        for inv in invs:
            out[inv] = inventory(layer, name, inv)
    (HERE / "acceptance.json").write_text(json.dumps(out, indent=1, sort_keys=True, default=list) + "\n")
    for inv, r in out.items():
        print(inv, {k: v for k, v in r.items() if k not in ("c5_residual_other", "c6_locality_exception_list")})


if __name__ == "__main__":
    main()
