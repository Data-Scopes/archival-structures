"""Step 19d step 4: main_text units on flagged index sides and elsewhere, 19c-* vs 19d-*; unit_method counts; units on
unflagged pages identical (all columns but ``unit_order``, which is a scan-level counter and shifts after a flagged page;
plus the unit segment rows of those pages and the order of their unit ids).

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/units_stats.py
Reads data/text_layer_v2_dev/{19c,19d}-{a,z} (hand lists planning/hand_15{y,z}-b.txt). Writes units_stats.json."""
import json
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
SUB = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"


def main():
    out = {}
    for side, lst in (("a", "hand_15y-b"), ("z", "hand_15z-b")):
        invs = [l.strip() for l in open(A / "planning" / f"{lst}.txt") if l.strip()]
        r0, r1 = DEV / f"19c-{side}" / SUB, DEV / f"19d-{side}" / SUB
        s = {"n_inv": len(invs), "main_flagged": Counter(), "main_other": Counter(), "methods_flagged": Counter(),
             "methods_other": Counter(), "n_flagged_sides": 0, "n_other_pages_diff_rows": 0, "n_other_units": 0,
             "n_other_seg_rows_diff": 0, "n_other_order_diff": 0, "inv_flagged_main": {}}
        for inv in invs:
            flagged = {(r["scan_id"], r["page"]) for r in read_tsv_gz(r0 / "layout" / "pages" / f"{inv}.tsv.gz")
                       if r["index_side"] == "1"}
            s["n_flagged_sides"] += len(flagged)
            tabs = {}
            for tag, rt in (("c", r0), ("d", r1)):
                tabs[tag] = (list(read_tsv_gz(rt / "units" / "units" / f"{inv}.tsv.gz")),
                             list(read_tsv_gz(rt / "units" / "segments" / f"{inv}.tsv.gz")))
            page_of = {(r["scan_id"], r["unit_id"]): (r["scan_id"], r["page"]) for r in tabs["c"][0]}
            page_of.update({(r["scan_id"], r["unit_id"]): (r["scan_id"], r["page"]) for r in tabs["d"][0]})
            for tag, (units, segs) in tabs.items():
                for u in units:
                    key = (u["scan_id"], u["page"])
                    is_f = key in flagged
                    if u["unit_type"] == "main_text":
                        (s["main_flagged"] if is_f else s["main_other"])[tag] += 1
                    if is_f:
                        s["methods_flagged"][f"{tag}:{u['unit_method']}"] += 1
                    else:
                        s["methods_other"][f"{tag}:{u['unit_method']}"] += 1
            if flagged:
                s["inv_flagged_main"][inv] = [sum(1 for u in tabs[t][0] if u["unit_type"] == "main_text"
                                                  and (u["scan_id"], u["page"]) in flagged) for t in "cd"]
            # unflagged pages must be identical
            def by_page(units, segs):
                rows, srows, order = defaultdict(list), defaultdict(list), defaultdict(list)
                for u in units:
                    k = (u["scan_id"], u["page"])
                    if k not in flagged:
                        rows[k].append([v for kk, v in u.items() if kk != "unit_order"])
                        order[k].append(u["unit_id"])
                for g in segs:
                    k = page_of[(g["scan_id"], g["unit_id"])]
                    if k not in flagged:
                        srows[k].append(list(g.values()))
                return rows, srows, order
            c, d = by_page(*tabs["c"]), by_page(*tabs["d"])
            s["n_other_units"] += sum(len(v) for v in c[0].values())
            s["n_other_pages_diff_rows"] += sum(1 for k in set(c[0]) | set(d[0]) if c[0].get(k) != d[0].get(k))
            s["n_other_seg_rows_diff"] += sum(1 for k in set(c[1]) | set(d[1]) if c[1].get(k) != d[1].get(k))
            s["n_other_order_diff"] += sum(1 for k in set(c[2]) | set(d[2]) if c[2].get(k) != d[2].get(k))
        for k in ("main_flagged", "main_other", "methods_flagged", "methods_other"):
            s[k] = dict(sorted(s[k].items()))
        out[side] = s
    (A / "step-19d" / "units_stats.json").write_text(json.dumps(out, indent=1))
    for side, s in out.items():
        print(side, {k: v for k, v in s.items() if k not in ("inv_flagged_main", "methods_other")})
        print("  methods_other", s["methods_other"])


if __name__ == "__main__":
    main()
