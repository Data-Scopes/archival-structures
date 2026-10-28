"""Review of steps 19f and 19g (phase 13, Opus 5.5, 2026-10-28). Read-only on every layer; writes only to OUTDIR.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/review_19fg.py OUTDIR CMD

CMD:
  f_counts      19f: layout region rows, reordered regions (flagged / other), duplicate region ids, main_text units,
                19f-a/z against 19c-a/z.
  g_select      19g selection X1-X4 + baselines on the 30 spent 19a sides (19g's own functions, OUTDIR for *_lines.tsv).
  g_fresh       19g fresh test recomputed WITHOUT entry_eval_v2.py fresh (its guard stays): (1) from the production units
                of 19g-a/19g-z, (2) from build_units_page on BASE (19c) with X4; both compared line by line with
                step-19g/fresh_lines.tsv.
  g_reorder     DIAGNOSTIC ONLY (no bar; the fresh gold is spent): X4 on the 19f reorder layout (19f-a/19f-z) and the
                19f clones' own (v4 hand rule) units, same fresh sides and scoring.
  g_fn          FN of X4 on the fresh sides: line_type of each FN line in BASE types, by side and volume.
  g_unflagged   units on unflagged pages: 19g-a/z against 19c-a/z (unit rows without unit_order, segment rows).
"""
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
SUB = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
LISTS = {"a": A / "planning/hand_15y-b.txt", "z": A / "planning/hand_15z-b.txt"}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def invs(side):
    return sorted(LISTS[side].read_text().split(), key=int)


def root(layer):
    return DEV / layer / SUB


def flagged(layer, inv):
    return {(r["scan_id"], r["page"]) for r in read_tsv_gz(root(layer) / "layout" / "pages" / f"{inv}.tsv.gz")
            if r["index_side"] == "1"}


def f_counts(out):
    tot = Counter()
    for side in "az":
        for inv in invs(side):
            fl = flagged(f"19f-{side}", inv)
            rows_f = list(read_tsv_gz(root(f"19f-{side}") / "layout" / "regions" / f"{inv}.tsv.gz"))
            rows_c = list(read_tsv_gz(root(f"19c-{side}") / "layout" / "regions" / f"{inv}.tsv.gz"))
            tot["regions_19f"] += len(rows_f)
            tot["regions_19c"] += len(rows_c)
            ids = Counter((r["scan_id"], r["layout_region_id"]) for r in rows_f)
            tot["dup_ids"] += sum(n - 1 for n in ids.values() if n > 1)
            for r in rows_f:
                if "column_reordered" in r["change"]:
                    tot["reordered"] += 1
                    tot["reordered_flagged" if (r["scan_id"], r["page"]) in fl else "reordered_other"] += 1
            if fl != flagged(f"19c-{side}", inv):
                tot["pages_flag_differs_inv"] += 1
            for lay in (f"19f-{side}", f"19c-{side}"):
                n = sum(1 for r in read_tsv_gz(root(lay) / "units" / "units" / f"{inv}.tsv.gz")
                        if r["unit_type"] == "main_text")
                tot["main_" + lay[:3]] += n
    print(json.dumps(tot, indent=1))
    (out / "f_counts.json").write_text(json.dumps(tot, indent=1))


def g_select(out):
    G = load("ev2", A / "step-19g/entry_eval_v2.py")
    G.OUT = out
    vs = G.variants()
    sides, gold = G.sides_select()
    res = G.run("select", sides, gold, list(vs), vs)
    names = list(vs)
    res["chosen"] = max(names, key=lambda k: (res["scores"][k]["micro"]["F1"], -names.index(k)))
    (out / "select.json").write_text(json.dumps(res, indent=1))
    G.show(res)
    print("chosen", res["chosen"], "| 19a sides", len(sides), "| inv", sorted({s["inv"] for s in sides}, key=int))


def fresh_sides():
    G = load("ev2", A / "step-19g/entry_eval_v2.py")
    return G, *G.sides_fresh()


def score_lines(gold, preds):
    """preds: {read_id: (starts, covered)} -> tp fp fn fn_nounit P R F1."""
    tp = fp = fn = fnu = 0
    for rid, lab_by in gold.items():
        if rid not in preds:
            continue
        starts, covered = preds[rid]
        for lid, lab in lab_by.items():
            st = lid in starts
            if lab == "S":
                tp += st
                if not st:
                    fn += 1
                    fnu += lid not in covered
            elif lab == "C":
                fp += st
    P = tp / (tp + fp) if tp + fp else 0
    R = tp / (tp + fn) if tp + fn else 0
    return dict(tp=tp, fp=fp, fn=fn, fn_nounit=fnu, P=round(P, 4), R=round(R, 4),
                F1=round(2 * P * R / (P + R), 4) if P + R else 0)


def logged_x4():
    rows = {}
    with open(A / "step-19g/fresh_lines.tsv") as f:
        head = f.readline().rstrip("\n").split("\t")
        for line in f:
            r = dict(zip(head, line.rstrip("\n").split("\t")))
            rows[(r["read_id"], r["line_id"])] = r["X4"]
    return rows


def as_cell(starts, covered, lid):
    return starts[lid] if lid in starts else "-" if lid in covered else "nounit"


def g_fresh(out):
    G, sides, gold = fresh_sides()
    E = G.E
    vs = G.variants()
    logged = logged_x4()
    built = {s["read_id"]: E.predict_table({"15y-b": "19g-a", "15z-b": "19g-z"}[s["layer"]], s) for s in sides}
    cache = E.Cache()
    rule = {s["read_id"]: E.predict_rule(cache, s, vs["X4"]) for s in sides}
    for name, preds in (("production units 19g-a/z", built), ("build_units_page on 19c, X4", rule)):
        diff = sum(1 for rid, lab in gold.items() for lid in lab
                   if rid in preds and as_cell(*preds[rid], lid) != logged[(rid, lid)])
        print(name, score_lines(gold, preds), "| cells differing from fresh_lines.tsv X4:", diff)


def g_reorder(out):
    G, sides, gold = fresh_sides()
    E = G.E
    vs = G.variants()
    E.CLONE = {"15y-b": "19f-a", "15z-b": "19f-z"}
    cache = E.Cache()
    fl = {s["read_id"]: (s["scan_id"], s["page"]) in flagged(E.CLONE[s["layer"]], s["inv"]) for s in sides}
    print("fresh sides flagged in 19f layout/pages:", sum(fl.values()), "of", len(fl))
    res = {}
    rule = {s["read_id"]: E.predict_rule(cache, s, vs["X4"]) for s in sides}
    own = {s["read_id"]: E.predict_table(E.CLONE[s["layer"]], s) for s in sides}
    res["X4 on 19f reorder layout"] = score_lines(gold, rule)
    res["19f units (v4 hand rule on reorder)"] = score_lines(gold, own)
    # per side, against the logged X4 on the v4 layout
    logged = logged_x4()
    v4 = defaultdict(dict)
    for (rid, lid), cell in logged.items():
        v4[rid][lid] = cell
    per = []
    for s in sides:
        rid = s["read_id"]
        st_v4 = {lid for lid, c in v4[rid].items() if c not in ("-", "nounit")}
        cov_v4 = {lid for lid, c in v4[rid].items() if c != "nounit"}
        a = score_lines({rid: gold[rid]}, {rid: (dict.fromkeys(st_v4, "x"), cov_v4)})
        b = score_lines({rid: gold[rid]}, {rid: rule[rid]})
        if (a["tp"], a["fp"]) != (b["tp"], b["fp"]):
            per.append((rid, s["inv"], (a["tp"], a["fp"], a["fn"]), (b["tp"], b["fp"], b["fn"])))
    res["sides that change (rid, inv, v4 tp/fp/fn, reorder tp/fp/fn)"] = per
    print(json.dumps(res, indent=1))
    (out / "g_reorder.json").write_text(json.dumps(res, indent=1))


def g_fn(out):
    G, sides, gold = fresh_sides()
    logged = logged_x4()
    meta = {s["read_id"]: s for s in sides}
    types = {}
    for s in sides:
        layer = {"15y-b": "19c-a", "15z-b": "19c-z"}[s["layer"]]
        for r in read_tsv_gz(root(layer) / "types" / "segments" / f"{s['inv']}.tsv.gz"):
            if r["scan_id"] == s["scan_id"]:
                types[(s["read_id"], r["line_id"])] = r["line_type"]
    fn = [(rid, lid, logged[(rid, lid)], types.get((rid, lid), "?")) for rid, lab in gold.items()
          for lid, l in lab.items() if l == "S" and logged[(rid, lid)] in ("-", "nounit")]
    print("FN", len(fn), "no unit", sum(c == "nounit" for _, _, c, _ in fn))
    print("FN by (cell, line_type):", Counter((c, t) for _, _, c, t in fn))
    nu = [x for x in fn if x[2] == "nounit"]
    print("no-unit FN by side:", Counter(f"{r} ({meta[r]['inv']}, {meta[r]['group']})" for r, *_ in nu))
    print("no-unit FN by volume:", Counter(meta[r]["inv"] for r, *_ in nu))
    # all S lines typed marginalia on the fresh sides (reached or not)
    s_marg = Counter(rid for rid, lab in gold.items() for lid, l in lab.items()
                     if l == "S" and types.get((rid, lid)) == "marginalia")
    print("S lines typed marginalia by side:", s_marg)
    lt = Counter(types.get((rid, lid), "?") for rid, lab in gold.items() for lid, l in lab.items())
    print("all gold lines by line_type:", lt)


def g_marg(out):
    """DIAGNOSTIC ONLY (both gold sets are spent): X4 with `marginalia` added to main_types, on the 30 selection
    sides and the 20 fresh sides (v4 layout, 19c)."""
    G = load("ev2", A / "step-19g/entry_eval_v2.py")
    E = G.E
    vs = G.variants()
    x4m = {**vs["X4"], "main_types": ["main_text", "index", "marginalia"]}
    for name, (sides, gold) in (("selection 30", G.sides_select()), ("fresh 20", G.sides_fresh()[0:2])):
        cache = E.Cache()
        for vname, cfg in (("X4", vs["X4"]), ("X4+marginalia", x4m)):
            preds = {s["read_id"]: E.predict_rule(cache, s, cfg) for s in sides}
            print(name, vname, score_lines(gold, preds))


def g_unflagged(out):
    tot = Counter()
    for side in "az":
        for inv in invs(side):
            fl = flagged(f"19g-{side}", inv)
            if fl != flagged(f"19c-{side}", inv):
                tot["flag_differs"] += 1

            def units(layer):
                rows = [r for r in read_tsv_gz(root(layer) / "units" / "units" / f"{inv}.tsv.gz")
                        if (r["scan_id"], r["page"]) not in fl]
                ids = {r["unit_id"] for r in rows}
                segs = [tuple(r.values()) for r in read_tsv_gz(root(layer) / "units" / "segments" / f"{inv}.tsv.gz")
                        if r["unit_id"] in ids]
                return [tuple(v for k, v in r.items() if k != "unit_order") for r in rows], segs
            u1, s1 = units(f"19g-{side}")
            u0, s0 = units(f"19c-{side}")
            tot["units_unflagged"] += len(u1)
            if u1 != u0 or s1 != s0:
                tot["inv_differs"] += 1
    print(json.dumps(tot, indent=1))


if __name__ == "__main__":
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    globals()[sys.argv[2]](out)
