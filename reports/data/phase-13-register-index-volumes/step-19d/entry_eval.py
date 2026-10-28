"""Step 19d: score index-entry unit variants E1-E5 and the baselines on the 19a gold sides.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/entry_eval.py dev
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/entry_eval.py test --variant E3

Needs (read only): ``step-19a/entry_sides.tsv`` + ``entry_gold.tsv``, ``planning/reads_entries.tsv``, the 19c clones
``data/text_layer_v2_dev/19c-{a,z}`` (v4 hand layout + ``layout/pages``), the v4 hand layers ``15y-b``, ``15z-b`` and the
v4 print builds ``15z-a`` (``15x-a`` for 3463). Writes ``eval_<split>.json`` and, for the scored variant and the
baselines, ``eval_<split>_lines.tsv`` (one row per gold line with the predicted start per system).
Predicted starts: units in ``_key`` (hand clone: ``unit_order``) order, types main_text and date_header only; a main_text
unit directly preceded by a date_header unit in the same layout_column is not a start. TP = start on S, FP = start on C,
FN = S line that is no start (``fn_nounit``: its line is in no main_text/date_header unit); H and ? ignored."""
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_INDEX_DATE_START
from archival_structures.text_layer.units import UnitParams, build_units_page, load_segments

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
SUB = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
CLONE = {"15y-b": "19c-a", "15z-b": "19c-z"}
PRINT_LAYER = {"3463": "15x-a"}
PRINT_INVS = {"3433", "3436", "3439", "3442", "3445", "3466", "3469", "3472", "3475", "3489", "3463"}
OUT = A / "step-19d"


def root(layer):
    return DEV / layer / SUB


def variants():
    base = {"outdent_frac": 0.5, "indent_frac": 0.5, "short_frac": 0.1, "date_pattern": SG_INDEX_DATE_START}
    cues = {"E1": ["outdent"], "E2": ["outdent", "date_start"], "E3": ["outdent", "date_start", "end_ref_prev"],
            "E4": ["outdent", "date_start", "end_ref_prev", "short_prev"], "E5": ["indent", "short_prev"]}
    return {k: {"cues": v, **base} for k, v in cues.items()}


def load_sides(split):
    reads = {r["read_id"]: r for r in csv_rows(A / "planning/reads_entries.tsv")}
    sides = []
    for r in csv_rows(A / "step-19a/entry_sides.tsv"):
        if r["side_verdict"] == "index" and r["split"] == split:
            m = reads[r["read_id"]]
            sides.append({"read_id": r["read_id"], "group": r["group"], "inv": m["inv"], "layer": m["layer"],
                          "scan_id": m["scan_id"], "page": m["page"]})
    gold = defaultdict(dict)
    for r in csv_rows(A / "step-19a/entry_gold.tsv"):
        gold[r["read_id"]][r["line_id"]] = r["label"]
    return sides, gold


def csv_rows(path):
    import csv
    with open(path, encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


class Cache:
    """Per (layer, inv) tables of the 19c clones, loaded once."""
    def __init__(self):
        self.d = {}

    def get(self, layer, inv):
        k = (layer, inv)
        if k not in self.d:
            rt = root(layer)
            regions = defaultdict(list)
            for r in read_tsv_gz(rt / "layout" / "regions" / f"{inv}.tsv.gz"):
                regions[(r["scan_id"], r["page"])].append(r)
            segs = load_segments(rt, inv)
            text = {(r["scan_id"], r["line_id"]): r["text"]
                    for r in read_tsv_gz(rt / "lines" / f"{inv}.lines.tsv.gz")}
            for ss in segs.values():
                for s in ss:
                    s["text"] = text.get((s["scan_id"], s["line_id"]), "")[s["char_start"]:s["char_end"]]
            self.d[k] = (regions, segs)
        return self.d[k]


def starts_from_units(units):
    """units: dicts with unit_type, unit_method, layout_column, first_line_id in order.
    Returns ({line_id: method}, set of line ids in a main_text/date_header unit)."""
    use = [u for u in units if u["unit_type"] in ("main_text", "date_header")]
    starts, covered = {}, set()
    for j, u in enumerate(use):
        prev = use[j - 1] if j else None
        covered |= u["lines"]
        if (u["unit_type"] == "main_text" and prev is not None and prev["unit_type"] == "date_header"
                and prev["layout_column"] == u["layout_column"]):
            continue
        starts[u["first_line_id"]] = u["unit_method"]
    return starts, covered


def predict_rule(cache, side, cfg):
    layer = CLONE[side["layer"]]
    regions_by, segs_by = cache.get(layer, side["inv"])
    sid, page = side["scan_id"], side["page"]
    regions = []
    for r in regions_by[(sid, page)]:
        reg = {"layout_region_id": r["layout_region_id"], "layout_role": r["layout_role"],
               "layout_column": int(r["layout_column"]) if r["layout_column"] else 0}
        regions.append((reg, segs_by.get((sid, page, r["layout_region_id"]), [])))
    units, _ = build_units_page(sid, page, regions, UnitParams(index_entry=cfg), False, taken=set(),
                                claimable=None, index_side=True)
    units.sort(key=lambda u: u["_key"])
    return starts_from_units([{"unit_type": u["unit_type"], "unit_method": u["unit_method"],
                               "layout_column": u["layout_column"], "first_line_id": u["segs"][0]["line_id"],
                               "lines": {s["line_id"] for s in u["segs"]}} for u in units])


_tab = {}


def predict_table(layer, side):
    rt = root(layer)
    inv = side["inv"]
    if (layer, inv) not in _tab:
        units = defaultdict(list)
        for r in read_tsv_gz(rt / "units" / "units" / f"{inv}.tsv.gz"):
            units[(r["scan_id"], r["page"])].append(r)
        segs = defaultdict(set)
        for r in read_tsv_gz(rt / "units" / "segments" / f"{inv}.tsv.gz"):
            segs[(r["scan_id"], r["unit_id"])].add(r["line_id"])
        _tab[(layer, inv)] = (units, segs)
    units, segs = _tab[(layer, inv)]
    rows = sorted(units[(side["scan_id"], side["page"])], key=lambda r: int(r["unit_order"]))
    return starts_from_units([{"unit_type": r["unit_type"], "unit_method": r["unit_method"],
                               "layout_column": r["layout_column"], "first_line_id": r["first_line_id"],
                               "lines": segs[(r["scan_id"], r["unit_id"])]} for r in rows])


def score(side_preds, gold):
    """side_preds: list of (side, starts, covered). Returns micro and per-group counts."""
    def zero():
        return {"tp": 0, "fp": 0, "fn": 0, "fn_nounit": 0, "n_sides": 0}
    tot, groups = zero(), defaultdict(zero)
    for side, starts, covered in side_preds:
        for g in (tot, groups[side["group"]]):
            g["n_sides"] += 1
            for lid, lab in gold[side["read_id"]].items():
                st = lid in starts
                if lab == "S":
                    if st:
                        g["tp"] += 1
                    else:
                        g["fn"] += 1
                        g["fn_nounit"] += lid not in covered
                elif lab == "C" and st:
                    g["fp"] += 1
    return {"micro": prf(tot), "groups": {k: prf(v) for k, v in sorted(groups.items())}}


def prf(c):
    p = c["tp"] / (c["tp"] + c["fp"]) if c["tp"] + c["fp"] else 0.0
    r = c["tp"] / (c["tp"] + c["fn"]) if c["tp"] + c["fn"] else 0.0
    f = 2 * p * r / (p + r) if p + r else 0.0
    return {**c, "P": round(p, 4), "R": round(r, 4), "F1": round(f, 4)}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("split", choices=["dev", "test"])
    ap.add_argument("--variant", default=None, help="test: the chosen variant (E1..E5)")
    a = ap.parse_args(argv)
    if a.split == "test" and not a.variant:
        sys.exit("test needs --variant (choose on dev first)")
    sides, gold = load_sides(a.split)
    cache = Cache()
    vs = variants()
    names = list(vs) if a.split == "dev" else [a.variant]
    systems = {}
    for k in names:
        systems[k] = [(s, *predict_rule(cache, s, vs[k])) for s in sides]
    systems["v4_hand"] = [(s, *predict_table(s["layer"], s)) for s in sides]
    systems["split_only"] = [(s, *predict_table(CLONE[s["layer"]], s)) for s in sides]
    psides = [s for s in sides if s["inv"] in PRINT_INVS]
    systems["v4_print"] = [(s, *predict_table("15x-a" if s["inv"] in PRINT_LAYER else "15z-a", s)) for s in psides]
    res = {"split": a.split, "n_sides": len(sides), "sides": [s["read_id"] for s in sides],
           "print_sides": [s["read_id"] for s in psides],
           "variants": {k: vs[k] for k in names}, "scores": {}}
    for k, sp in systems.items():
        res["scores"][k] = score(sp, gold)
    # print baseline vs v4 hand on the print sides only (same sides)
    pset = {s["read_id"] for s in psides}
    for k in names + ["v4_hand", "split_only"]:
        res["scores"][k + "@print_sides"] = score([x for x in systems[k] if x[0]["read_id"] in pset], gold)
    if a.split == "dev":
        best = max(names, key=lambda k: (res["scores"][k]["micro"]["F1"], -names.index(k)))
        res["chosen"] = best
    (OUT / f"eval_{a.split}.json").write_text(json.dumps(res, indent=1))
    with open(OUT / f"eval_{a.split}_lines.tsv", "w") as fh:
        keys = list(systems)
        by_sys = {k: {x[0]["read_id"]: x for x in sp} for k, sp in systems.items()}
        fh.write("\t".join(["read_id", "group", "inv", "scan_id", "page", "line_id", "label"] + keys) + "\n")
        for side in sides:
            for lid, lab in gold[side["read_id"]].items():
                row = [side["read_id"], side["group"], side["inv"], side["scan_id"], side["page"], lid, lab]
                for k in keys:
                    x = by_sys[k].get(side["read_id"])
                    # start method / "-" (in a unit, no start) / "nounit" (in no main_text or date_header unit)
                    row.append("" if x is None else x[1][lid] if lid in x[1] else "-" if lid in x[2] else "nounit")
                fh.write("\t".join(row) + "\n")
    for k, v in res["scores"].items():
        m = v["micro"]
        print(f"{k:22s} P {m['P']:.4f} R {m['R']:.4f} F1 {m['F1']:.4f} tp {m['tp']} fp {m['fp']} fn {m['fn']} "
              f"(no unit {m['fn_nounit']})")
    if "chosen" in res:
        print("chosen:", res["chosen"])


if __name__ == "__main__":
    main()
