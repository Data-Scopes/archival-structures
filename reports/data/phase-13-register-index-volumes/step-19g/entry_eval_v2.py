"""Step 19g: index-entry rule v2. Selection of X1-X4 on the 30 spent 19a sides, one fresh test.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/entry_eval_v2.py select
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/entry_eval_v2.py fresh --variant X2

BASE = the v4-layout clones 19c-a / 19c-z (STATUS.md, "19g base"). Prediction and scoring are 19d's (imported from
``step-19d/entry_eval.py``): units in ``_key`` order, main_text/date_header only, a main_text right after a date_header in
the same layout_column is no start; TP start on S, FP start on C, FN S without start; H and ? ignored.
``fresh`` refuses a second run (``test_done.json``). Writes ``select.json`` / ``fresh.json`` and ``*_lines.tsv``."""
import argparse
import importlib.util
import json
import sys
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.presets import SG_INDEX_DATE_START, SG_INDEX_ENTRY_V5, SG_INDEX_NO_CUT

A = Path("reports/data/phase-13-register-index-volumes")
OUT = A / "step-19g"
spec = importlib.util.spec_from_file_location("entry_eval19d", A / "step-19d/entry_eval.py")
E = importlib.util.module_from_spec(spec)
spec.loader.exec_module(E)


def variants():
    base = {"outdent_frac": 0.5, "short_frac": 0.1, "date_pattern": SG_INDEX_DATE_START,
            "no_cut_pattern": SG_INDEX_NO_CUT}
    x1 = ["outdent", "date_start", "end_ref_prev"]
    mt = ["main_text"]
    mi = ["main_text", "index"]
    return {"X1": {"cues": x1, "main_types": mt, **base}, "X2": {"cues": x1, "main_types": mi, **base},
            "X3": {"cues": x1 + ["short_prev"], "main_types": mt, **base},
            "X4": {"cues": x1 + ["short_prev"], "main_types": mi, **base}}


def sides_select():
    sides, gold = E.load_sides("dev")
    s2, g2 = E.load_sides("test")
    gold.update(g2)
    return sides + s2, gold


def sides_fresh():
    meta = {r["read_id"]: r for r in E.csv_rows(A / "review/fresh_entry_sides.tsv")}
    sides, gold = [], defaultdict(dict)
    for r in E.csv_rows(OUT / "fresh_sides.tsv"):
        if r["side_verdict"] == "index":
            m = meta[r["read_id"]]
            sides.append({"read_id": r["read_id"], "group": m["group"], "inv": m["inv"], "layer": m["layer"],
                          "scan_id": m["scan_id"], "page": m["page"]})
    for r in E.csv_rows(OUT / "fresh_gold.tsv"):
        gold[r["read_id"]][r["line_id"]] = r["label"]
    return sides, gold


def flagged(side):
    layer = E.CLONE[side["layer"]]
    for r in read_tsv_gz(E.root(layer) / "layout" / "pages" / f"{side['inv']}.tsv.gz"):
        if r["scan_id"] == side["scan_id"] and r["page"] == side["page"]:
            return r["index_side"] == "1"
    return None


def run(name, sides, gold, names, vs):
    cache = E.Cache()
    systems = {k: [(s, *E.predict_rule(cache, s, vs[k])) for s in sides] for k in names}
    systems["E5"] = [(s, *E.predict_rule(cache, s, SG_INDEX_ENTRY_V5)) for s in sides]
    systems["v4_hand"] = [(s, *E.predict_table(s["layer"], s)) for s in sides]
    systems["base_units"] = [(s, *E.predict_table(E.CLONE[s["layer"]], s)) for s in sides]
    psides = [s for s in sides if s["inv"] in E.PRINT_INVS]
    systems["v4_print"] = [(s, *E.predict_table("15x-a" if s["inv"] in E.PRINT_LAYER else "15z-a", s))
                           for s in psides]
    res = {"n_sides": len(sides), "sides": [s["read_id"] for s in sides], "print_sides": [s["read_id"] for s in psides],
           "variants": {k: vs[k] for k in names}, "scores": {}}
    for k, sp in systems.items():
        res["scores"][k] = E.score(sp, gold)
    pset = {s["read_id"] for s in psides}
    for k in names + ["E5", "v4_hand", "base_units"]:
        res["scores"][k + "@print_sides"] = E.score([x for x in systems[k] if x[0]["read_id"] in pset], gold)
    with open(OUT / f"{name}_lines.tsv", "w") as fh:
        keys = list(systems)
        by = {k: {x[0]["read_id"]: x for x in sp} for k, sp in systems.items()}
        fh.write("\t".join(["read_id", "group", "inv", "scan_id", "page", "line_id", "label"] + keys) + "\n")
        for side in sides:
            for lid, lab in gold[side["read_id"]].items():
                row = [side["read_id"], side["group"], side["inv"], side["scan_id"], side["page"], lid, lab]
                for k in keys:
                    x = by[k].get(side["read_id"])
                    row.append("" if x is None else x[1][lid] if lid in x[1] else "-" if lid in x[2] else "nounit")
                fh.write("\t".join(row) + "\n")
    return res


def show(res):
    for k, v in res["scores"].items():
        m = v["micro"]
        print(f"{k:18s} P {m['P']:.4f} R {m['R']:.4f} F1 {m['F1']:.4f} tp {m['tp']} fp {m['fp']} fn {m['fn']} "
              f"(no unit {m['fn_nounit']})")
        if "@" not in k:
            print("   groups " + "; ".join(f"{g} P {x['P']:.3f} R {x['R']:.3f}" for g, x in v["groups"].items()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["select", "fresh"])
    ap.add_argument("--variant")
    a = ap.parse_args()
    vs = variants()
    if a.mode == "select":
        sides, gold = sides_select()
        res = run("select", sides, gold, list(vs), vs)
        names = list(vs)
        res["chosen"] = max(names, key=lambda k: (res["scores"][k]["micro"]["F1"], -names.index(k)))
        (OUT / "select.json").write_text(json.dumps(res, indent=1))
        show(res)
        print("chosen:", res["chosen"])
        return
    guard = OUT / "test_done.json"
    if guard.exists():
        sys.exit("fresh test already run: " + guard.read_text())
    if a.variant not in vs:
        sys.exit("fresh needs --variant X1..X4")
    sel = json.loads((OUT / "select.json").read_text())
    if sel["chosen"] != a.variant:
        sys.exit(f"variant must be the selected one ({sel['chosen']})")
    sides, gold = sides_fresh()
    fl = {s["read_id"]: flagged(s) for s in sides}
    print("flagged in BASE layout/pages:", fl)
    if not all(fl.values()):
        sys.exit("not all fresh sides are flagged; no test run")
    guard.write_text(json.dumps({"variant": a.variant}))
    res = run("fresh", sides, gold, [a.variant], vs)
    res["flagged"] = fl
    (OUT / "fresh.json").write_text(json.dumps(res, indent=1))
    show(res)


if __name__ == "__main__":
    main()
