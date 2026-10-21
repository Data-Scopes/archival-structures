"""Step 15h acceptance criteria 1-6. Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15h/acceptance.py
Writes acceptance.json next to this file."""
import json
import statistics
from collections import Counter, defaultdict

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from common15h import ALL_INVS, HERE, LOTA, PRINT, root
from archival_structures.text_layer.io import read_tsv_gz


def main():
    res = {"crit1": {}, "crit2": {}, "crit3": {}, "crit4": {}, "crit5": {}, "crit6": {}}
    old_ids, old_status = {}, {}
    per_type, med_main = {}, {}
    for inv in ALL_INVS:
        r = root(inv)
        units = list(read_tsv_gz(r / "units" / "units" / f"{inv}.tsv.gz"))
        usegs = list(read_tsv_gz(r / "units" / "segments" / f"{inv}.tsv.gz"))
        tsegs = list(read_tsv_gz(r / "types" / "segments" / f"{inv}.tsv.gz"))
        dmap = list(read_tsv_gz(r / "units" / "doc_id_map" / f"{inv}.tsv.gz"))
        meta = list(read_tsv_gz(r / "regions_meta" / f"{inv}.tsv.gz"))
        lay = list(read_tsv_gz(r / "layout" / "regions" / f"{inv}.tsv.gz"))
        # 1 coverage
        keys = Counter((s["scan_id"], s["line_id"], s["seg_idx"]) for s in usegs)
        tkeys = {(s["scan_id"], s["line_id"], s["seg_idx"]): s["line_type"] for s in tsegs}
        utype = {u["unit_id"]: u["unit_type"] for u in units}
        seg_types = defaultdict(set)
        for s in usegs:
            seg_types[s["unit_id"]].add(tkeys[(s["scan_id"], s["line_id"], s["seg_idx"])])
        res["crit1"][inv] = {
            "every_segment_once": set(keys) == set(tkeys) and max(keys.values()) == 1,
            "n_segments": len(tkeys), "unit_id_unique": len({u["unit_id"] for u in units}) == len(units),
            "one_type_per_unit": all(seg_types[u] == {t} for u, t in utype.items()),
            "n_units": len(units)}
        # 2 R5
        same = {r_["old_doc_id"] for r_ in dmap if r_["link"] == "same_id"}
        seg_regions = {u["layout_region_id"] for u in units}
        unchanged = [x for x in lay if x["change"] == "unchanged" and x["layout_region_id"] in seg_regions]
        miss = [x["layout_region_id"] for x in unchanged if x["layout_region_id"] not in same]
        status = {}
        for r_ in dmap:
            status[r_["old_doc_id"]] = r_["old_status"]
        res["crit2"][inv] = {"n_unchanged_nonempty": len(unchanged), "n_not_same_id": len(miss),
                             "examples": miss[:3],
                             "all_meta_in_map": {m["region_id"] for m in meta} == set(status),
                             "old_status": dict(Counter(status.values())),
                             "n_id_collisions": json.loads((r.parent.joinpath(r.name) / "units" / "stats.json").read_text())[inv]["n_id_collisions"]}
        old_ids.update(status)
        old_status[inv] = status
        pt = Counter(u["unit_type"] for u in units)
        per_type[inv] = dict(pt)
        lens = [int(u["n_lines"]) for u in units if u["unit_type"] == "main_text"]
        med_main[inv] = statistics.median(lens)
        # 5 crosswalk
        if inv in PRINT:
            cw = list(read_tsv_gz(r / "units" / "republic_lines" / f"{inv}.tsv.gz"))
            res["crit5"][inv] = dict(Counter(x["match"] for x in cw))
    # 3 stable keys
    du = pd.read_csv(LOTA / "output/parallel_text/1.10.94-1.01.02/difference_units.tsv.gz", sep="\t", dtype=str,
                     usecols=["unit_id", "para_id", "hand_doc_id"])
    gold = pd.read_csv(LOTA / "gold/02d-hand-print-differences-partial-extended-rescaled.tsv", sep="\t", dtype=str,
                       usecols=["unit_id"])
    j = gold.merge(du, on="unit_id", how="left")
    ids = j.hand_doc_id.tolist()
    present = [i for i in ids if i in old_ids]
    res["crit3"] = {"n_gold": len(ids), "n_distinct": len(set(ids)), "n_missing_hand_doc_id": int(j.hand_doc_id.isna().sum()),
                    "n_in_map": len(present), "missing": [i for i in ids if i not in old_ids][:5],
                    "old_status": dict(Counter(old_ids[i] for i in present))}
    # 4 row 80
    r = root("3353")
    lines = {(x["scan_id"], x["line_id"]): x for x in read_tsv_gz(r / "lines" / "3353.lines.tsv.gz")}
    usegs = list(read_tsv_gz(r / "units" / "segments" / "3353.tsv.gz"))
    units = {u["unit_id"]: u for u in read_tsv_gz(r / "units" / "units" / "3353.tsv.gz")}
    found = {}
    for s in usegs:
        ln = lines[(s["scan_id"], s["line_id"])]
        if "3353_0754" not in s["scan_id"]:
            continue
        if ln["region_id"].endswith("3604-3785-1254-132") and "alhier gebragt" in ln["text"]:
            found["noch"] = s["unit_id"]
        if ln["region_id"].endswith("3617-1633-1232-221") and ln["text"].strip().startswith("in te huuren"):
            found["huuren"] = s["unit_id"]
    if len(found) == 2:
        a, b = units[found["noch"]], units[found["huuren"]]
        res["crit4"] = {"noch": [a["unit_id"], a["unit_order"], a["unit_type"], a["unit_method"]],
                        "huuren": [b["unit_id"], b["unit_order"], b["unit_type"], b["unit_method"]],
                        "same_scan": a["scan_id"] == b["scan_id"],
                        "adjacent": abs(int(a["unit_order"]) - int(b["unit_order"])) <= 1}
    else:
        res["crit4"] = {"found": found}
    res["crit6"] = {"units_per_type": per_type, "median_main_unit_lines": med_main,
                    "fit": json.loads((HERE / "fit_paragraphs.json").read_text())}
    (HERE / "acceptance.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({k: v for k, v in res.items() if k != "crit6"}, indent=1)[:6000])
    print(json.dumps(res["crit6"]["units_per_type"]), json.dumps(res["crit6"]["median_main_unit_lines"]))


if __name__ == "__main__":
    main()
