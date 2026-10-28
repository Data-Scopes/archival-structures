"""Step 15yr (G): how much a v4 extract row's `seconds` undercounted, by timing whole calls.

Run from the AS root: .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15yr/prepass_factor.py

Takes the volumes of 15x-a (1.01.02) with input_xml_bytes <= 300e6 whose source tar is among the 182 staged
tars, groups them by mode/<inv>.json (hand, print_split, print_gated), draws 4 per group (seed 20261109),
times extract_inventory_v2_modes one volume at a time (after step 15yr `seconds` includes the pre-pass and
`prepass_seconds` is its part) and writes prepass_factor.json. Factor per group = sum(seconds) / sum(seconds -
prepass_seconds); the 1.01.02 factor weights the groups by their share of input_xml_bytes among the full run's
last done extract rows (the 16 volumes rebuilt after 2026-10-27T17:00:00Z left out); 1.10.94 = print_split.
"""
import json
import random
import tempfile
from pathlib import Path

from archival_structures.text_layer.modes import extract_inventory_v2_modes
from archival_structures.text_layer.presets import PRESETS

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
DEV = ROOT / "data/text_layer_v2_dev/15x-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4"
FULL = ROOT / "data/text_layer_v4_full/1.01.02/NL-HaNA_1.01.02_text_layer_v4"
TARS = ROOT / "data/text_layer_v4_staged/NL-HaNA_1.01.02"
REBUILT_AFTER = "2026-10-27T17:00:00Z"
GROUPS = ("hand", "print_split", "print_gated")


def last_done_extract(path):
    rows = {}
    lines = path.read_text().splitlines()
    head = lines[0].split("\t")
    for ln in lines[1:]:
        r = dict(zip(head, ln.split("\t")))
        if r["stage"] == "extract" and r["status"] == "done":
            rows[r["inventory_number"]] = r
    return rows


def group_of(layer, inv):
    m = json.loads((layer / "mode" / f"{inv}.json").read_text())
    if m["mode"] == "hand":
        return "hand"
    sg = m.get("split_gate")
    return "print_gated" if sg is not None and sg.get("split") is False else "print_split"


def main():
    dev = last_done_extract(DEV / "status.tsv")
    groups = {g: [] for g in GROUPS}
    for inv, r in dev.items():
        tar = TARS / f"NL-HaNA_1.01.02_{inv}-pagexml.tar.gz"
        if tar.is_file() and int(r["input_xml_bytes"]) <= 300e6:
            groups[group_of(DEV, inv)].append(inv)
    pool = {g: len(v) for g, v in groups.items()}
    rng = random.Random(20261109)
    drawn = {g: rng.sample(sorted(v), 4) for g, v in groups.items()}
    preset = PRESETS["sg_1.01.02_v4"]
    rows = []
    for g in GROUPS:
        for inv in drawn[g]:
            with tempfile.TemporaryDirectory() as td:
                res = extract_inventory_v2_modes(inv, TARS / f"NL-HaNA_1.01.02_{inv}-pagexml.tar.gz", td, preset)
            rows.append({"group": g, "inventory": inv, "xml_bytes": int(dev[inv]["input_xml_bytes"]),
                         "seconds": res["seconds"], "prepass_seconds": res["prepass_seconds"]})
            print(rows[-1], flush=True)
    factors = {}
    for g in GROUPS:
        rs = [r for r in rows if r["group"] == g]
        factors[g] = sum(r["seconds"] for r in rs) / sum(r["seconds"] - r["prepass_seconds"] for r in rs)
    full = last_done_extract(FULL / "status.tsv")
    weight = {g: 0 for g in GROUPS}
    for inv, r in full.items():
        if r["finished_utc"] > REBUILT_AFTER:
            continue
        weight[group_of(FULL, inv)] += int(r["input_xml_bytes"])
    total = sum(weight.values())
    weights = {g: weight[g] / total for g in GROUPS}
    f_h = sum(weights[g] * factors[g] for g in GROUPS)
    out = {"seed": 20261109, "pool_sizes": pool, "rows": rows, "group_factors": factors, "weights": weights,
           "factor_1.01.02": f_h, "factor_1.10.94": factors["print_split"]}
    (HERE / "prepass_factor.json").write_text(json.dumps(out, indent=1))
    print(json.dumps({k: v for k, v in out.items() if k != "rows"}, indent=1))


if __name__ == "__main__":
    main()
