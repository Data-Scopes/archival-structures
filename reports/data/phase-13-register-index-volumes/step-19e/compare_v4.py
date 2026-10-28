"""Step 19e: compare the v5 dev layer (19e-a) with the v4 references, per inventory and per-inventory file.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/compare_v4.py

v4 reference of an inventory: 15z-b if it has the inventory (37 volumes), else 15y-b. Hand inventories (mode/<inv>.json of
v5) are also compared with 19g-a (hand_15y-b.txt) / 19g-z (hand_15z-b.txt) for units/ (branch A expectation).
Files are compared decompressed; JSON without the keys `seconds`, `elapsed*`, `source` (any depth). Per-inventory file =
file whose stem (name up to the first dot) is an inventory of the layer; everything else is layer-level (listed, not judged).
Writes compare_v4.json."""
import gzip
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
SUB4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
SUB5 = "1.01.02/NL-HaNA_1.01.02_text_layer_v5"
V5 = DEV / "19e-a" / SUB5
UNITS_EXPECTED = {"units/units", "units/segments"}


def strip(o):
    if isinstance(o, dict):
        return {k: strip(v) for k, v in o.items() if k != "seconds" and k != "source" and not k.startswith("elapsed")}
    if isinstance(o, list):
        return [strip(v) for v in o]
    return o


def content(p):
    raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
    name = p.name[:-3] if p.suffix == ".gz" else p.name
    if name.endswith(".json"):
        return json.dumps(strip(json.loads(raw)), sort_keys=True).encode()
    return raw


def stem(p):
    return p.name.split(".")[0]


def per_inv_files(root, invs):
    out = {}
    for p in root.rglob("*"):
        if p.is_file() and stem(p) in invs:
            out[(str(p.parent.relative_to(root)), stem(p))] = p
    return out


def main():
    invs = sorted((p.stem for p in (V5 / "mode").glob("*.json")), key=lambda s: (len(s.rstrip("ABC")), s))
    invset = set(invs)
    mode = {i: json.loads((V5 / "mode" / f"{i}.json").read_text())["mode"] for i in invs}
    in_z = {p.stem for p in (DEV / "15z-b" / SUB4 / "mode").glob("*.json")}
    ref4 = {i: DEV / ("15z-b" if i in in_z else "15y-b") / SUB4 for i in invs}
    hz = set((A / "planning/hand_15z-b.txt").read_text().split())
    hy = set((A / "planning/hand_15y-b.txt").read_text().split())
    ref19g = {i: DEV / ("19g-z" if i in hz else "19g-a") / SUB4 for i in invs if i in hz | hy}
    v5files = per_inv_files(V5, invset)
    cache4 = {}
    res = defaultdict(lambda: {"n": 0, "equal": 0, "diff": [], "new": 0})
    res19g = defaultdict(lambda: {"n": 0, "equal": 0, "diff": []})
    for (cat, inv), p in sorted(v5files.items()):
        for label, refroot, bucket in (("v4", ref4[inv], res), ("19g", ref19g.get(inv), res19g)):
            if refroot is None or (label == "19g" and cat not in UNITS_EXPECTED):
                continue
            key = f"{mode[inv]}|{cat}" if label == "v4" else f"hand|{cat}"
            b = bucket[key]
            q = refroot / p.relative_to(V5)
            b["n"] += 1
            if not q.is_file():
                if label == "v4":
                    b["new"] += 1
                else:
                    b["diff"].append(inv)
                continue
            if content(p) == content(q):
                b["equal"] += 1
            else:
                b["diff"].append(inv)
    # files in the references that v5 lacks
    missing = defaultdict(list)
    for inv in invs:
        for (cat, i), q in per_inv_files(ref4[inv], {inv}).items():
            if (cat, i) not in v5files:
                missing[f"{mode[inv]}|{cat}"].append(inv)
    layer = {}
    ref_for_layer = DEV / "15y-b" / SUB4
    for p in sorted(V5.rglob("*")):
        if p.is_file() and stem(p) not in invset:
            rel = p.relative_to(V5)
            q = ref_for_layer / rel
            layer[str(rel)] = ("no v4 counterpart" if not q.is_file() else
                               "equal" if content(p) == content(q) else "differs (15y-b)")
    out = {"n_inventories": len(invs), "modes": dict(Counter(mode.values())),
           "v4": {k: {**v, "diff": v["diff"][:25], "n_diff": len(v["diff"])} for k, v in sorted(res.items())},
           "units_vs_19g": {k: {**v, "diff": v["diff"][:25], "n_diff": len(v["diff"])} for k, v in sorted(res19g.items())},
           "missing_in_v5": {k: v[:10] for k, v in missing.items()},
           "layer_level": layer}
    (A / "step-19e" / "compare_v4.json").write_text(json.dumps(out, indent=1))
    print(out["n_inventories"], out["modes"])
    for k, v in out["v4"].items():
        print(f"{k:34s} n {v['n']:4d} equal {v['equal']:4d} new {v['new']:4d} diff {v['n_diff']:4d}")
    for k, v in out["units_vs_19g"].items():
        print(f"vs19g {k:28s} n {v['n']:4d} equal {v['equal']:4d} diff {v['n_diff']:4d} {v['diff'][:5]}")
    print("missing in v5:", out["missing_in_v5"])
    print("layer-level:", Counter(layer.values()))


if __name__ == "__main__":
    main()
