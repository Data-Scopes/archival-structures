"""Step 19e metrics on the v5 dev layer 19e-a. One part per call, one output prefix per part.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/metrics.py {a|b|c|d}

a  interleave (step-19f/interleave.py logic) over the 125 hand volumes of 19e-a -> metrics_a.tsv (+ .ids.txt), metrics_a.json
b  flagged sides and their lines per volume and group (19c's grouping) -> metrics_b.tsv, metrics_b.json; also: 19e-a
   layout/pages equal to the 19c clones' (same detector)
c  main_text units on flagged sides, v4 (15y-b/15z-b) vs v5, and unit_method counts -> metrics_c.json
d  end to end on the 50 spent gold sides with the v5 units (19d starts_from_units/score; 19a 30 sides, 19g 20 sides,
   scored separately, no bar) -> metrics_d.json, metrics_d_lines.tsv
"""
import csv
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
OUT = A / "step-19e"
DEV = Path("data/text_layer_v2_dev")
SUB4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
SUB5 = "1.01.02/NL-HaNA_1.01.02_text_layer_v5"
V5 = DEV / "19e-a" / SUB5


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def hand_invs():
    return sorted((p.stem for p in (V5 / "mode").glob("*.json")
                   if json.loads(p.read_text())["mode"] == "hand"), key=int)


def ref4(inv):
    return DEV / ("15z-b" if (DEV / "15z-b" / SUB4 / "mode" / f"{inv}.json").is_file() else "15y-b") / SUB4


def part_a():
    il = load(A / "step-19f/interleave.py", "il")
    il.L4 = SUB5
    tu = ti = 0
    rows = []
    ids = open(OUT / "metrics_a.ids.txt", "w")
    for inv, layer, n, bad in il.run(str(DEV / "19e-a"), str(OUT / "hand_invs.txt")):
        rows.append((inv, n, len(bad)))
        tu += n
        ti += len(bad)
        for u in bad:
            ids.write(f"{inv}\t{u}\n")
    ids.close()
    with open(OUT / "metrics_a.tsv", "w") as f:
        f.write("inv\tunits\tinterleaved\n")
        for r in rows:
            f.write("\t".join(map(str, r)) + "\n")
    res = {"volumes": len(rows), "main_text_units": tu, "interleaved": ti, "share": ti / tu}
    (OUT / "metrics_a.json").write_text(json.dumps(res, indent=1))
    print(res)


def group(inv, gt):
    n = int(inv)
    if inv in gt:
        return "GT volumes"
    if 3349 <= n <= 3600:
        return "half-year registers >= 3349"
    if 4600 <= n <= 4800:
        return "tweede serie index volumes (46xx-47xx)"
    return "other (< 3349)" if n < 3349 else "other (> 3600)"


def part_b():
    gt = {r["inv"] for r in csv.DictReader(open(A / "planning/gt_split.tsv"), delimiter="\t")}
    rows, tot = [], defaultdict(Counter)
    same = {"equal": 0, "diff": []}
    hz = set((A / "planning/hand_15z-b.txt").read_text().split())
    for inv in hand_invs():
        pages = list(read_tsv_gz(V5 / "layout/pages" / f"{inv}.tsv.gz"))
        clone = DEV / ("19c-z" if inv in hz else "19c-a") / SUB4 / "layout/pages" / f"{inv}.tsv.gz"
        if clone.is_file() and list(read_tsv_gz(clone)) == pages:
            same["equal"] += 1
        else:
            same["diff"].append(inv)
        fl = [r for r in pages if r["index_side"] == "1"]
        g = group(inv, gt)
        row = {"inv": inv, "group": g, "sides": len(pages), "flagged": len(fl),
               "lines": sum(int(r["n_lines"]) for r in pages), "flagged_lines": sum(int(r["n_lines"]) for r in fl),
               "rule1": sum(r["rule"] == "1" for r in fl), "gapfill0": sum(r["rule"] == "0" for r in fl)}
        rows.append(row)
        for k in ("sides", "flagged", "lines", "flagged_lines", "rule1", "gapfill0"):
            tot[g][k] += row[k]
        tot[g]["volumes"] += 1
        tot[g]["vols_with_flags"] += row["flagged"] > 0
    with open(OUT / "metrics_b.tsv", "w") as f:
        f.write("\t".join(rows[0]) + "\n")
        for r in rows:
            f.write("\t".join(map(str, r.values())) + "\n")
    res = {"groups": {k: dict(v) for k, v in tot.items()},
           "total": {k: sum(v[k] for v in tot.values()) for k in ("sides", "flagged", "lines", "flagged_lines", "rule1", "gapfill0")},
           "pages_equal_19c_clones": same["equal"], "pages_diff_19c_clones": same["diff"]}
    (OUT / "metrics_b.json").write_text(json.dumps(res, indent=1))
    print(json.dumps(res, indent=1))


def flagged_set(inv):
    return {(r["scan_id"], r["page"]) for r in read_tsv_gz(V5 / "layout/pages" / f"{inv}.tsv.gz")
            if r["index_side"] == "1"}


def part_c():
    main = Counter()
    methods = {"v4_flagged": Counter(), "v5_flagged": Counter(), "v4_other": Counter(), "v5_other": Counter()}
    types = {"v4_flagged": Counter(), "v5_flagged": Counter()}
    per_inv = {}
    for inv in hand_invs():
        fl = flagged_set(inv)
        for tag, root in (("v4", ref4(inv)), ("v5", V5)):
            n = 0
            for u in read_tsv_gz(root / "units/units" / f"{inv}.tsv.gz"):
                on = (u["scan_id"], u["page"]) in fl
                methods[f"{tag}_{'flagged' if on else 'other'}"][u["unit_method"]] += 1
                if on:
                    types[f"{tag}_flagged"][u["unit_type"]] += 1
                    if u["unit_type"] == "main_text":
                        main[tag] += 1
                        n += 1
                elif u["unit_type"] == "main_text":
                    main[f"{tag}_other"] += 1
            per_inv.setdefault(inv, {})[tag] = n
        per_inv[inv]["flagged_sides"] = len(fl)
    res = {"main_text_on_flagged": {"v4": main["v4"], "v5": main["v5"]},
           "main_text_elsewhere": {"v4": main["v4_other"], "v5": main["v5_other"]},
           "types_on_flagged": {k: dict(v) for k, v in types.items()},
           "unit_methods": {k: dict(sorted(v.items())) for k, v in methods.items()}, "per_inv": per_inv}
    (OUT / "metrics_c.json").write_text(json.dumps(res, indent=1))
    print({k: v for k, v in res.items() if k != "per_inv"})
    big = sorted(((v["v5"] / max(v["v4"], 1), inv, v) for inv, v in per_inv.items() if v["flagged_sides"]), reverse=True)
    print("largest relative change (v5/v4 main_text on flagged sides):", [(i, v) for _, i, v in big[:5]])


def part_d():
    E = load(A / "step-19d/entry_eval.py", "e19d")
    X = load(A / "step-19g/entry_eval_v2.py", "e19g")
    E.SUB = SUB5
    sets = {"19a_sides": E.load_sides("dev"), "19a_sides_test": E.load_sides("test"), "19g_sides": X.sides_fresh()}
    s19a, g19a = sets["19a_sides"]
    s2, g2 = sets["19a_sides_test"]
    g19a.update(g2)
    s19a = s19a + s2
    s19g, g19g = sets["19g_sides"]
    res, lines = {}, []
    for name, sides, gold in (("19a_sides", s19a, g19a), ("19g_sides", s19g, g19g)):
        preds = [(s, *E.predict_table("19e-a", s)) for s in sides]
        sc = E.score(preds, gold)
        res[name] = {"n_sides": len(sides), **sc}
        for s, starts, covered in preds:
            for lid, lab in gold[s["read_id"]].items():
                lines.append([name, s["read_id"], s["inv"], s["scan_id"], s["page"], lid, lab,
                              starts[lid] if lid in starts else "-" if lid in covered else "nounit"])
        m = sc["micro"]
        print(f"{name}: sides {len(sides)} P {m['P']} R {m['R']} F1 {m['F1']} tp {m['tp']} fp {m['fp']} fn {m['fn']}")
    (OUT / "metrics_d.json").write_text(json.dumps(res, indent=1))
    with open(OUT / "metrics_d_lines.tsv", "w") as f:
        f.write("set\tread_id\tinv\tscan_id\tpage\tline_id\tlabel\tstart\n")
        for r in lines:
            f.write("\t".join(map(str, r)) + "\n")


if __name__ == "__main__":
    part = sys.argv[1]
    if part == "a" and not (OUT / "hand_invs.txt").exists():
        (OUT / "hand_invs.txt").write_text("\n".join(hand_invs()) + "\n")
    {"a": part_a, "b": part_b, "c": part_c, "d": part_d}[part]()
