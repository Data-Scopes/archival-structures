"""Step 15g2 acceptance criteria 1-3 and the per-type shift. Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g2/acceptance.py
Writes acceptance.json next to this file."""

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "step-15g"))
from common import ALL_INVS, PARENT, layer_root  # noqa: E402

from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
from archival_structures.text_layer.presets import PRESETS  # noqa: E402

OUT = Path(__file__).parent / "acceptance.json"
PRESET_OF = lambda inv: PRESETS["sg_1.10.94_v2" if inv in ("455", "456") else "sg_1.01.02_v2"]  # noqa: E731


def rows(layer, inv, kind):
    return list(read_tsv_gz(layer_root(PARENT / layer, inv) / "types" / kind / f"{inv}.tsv.gz"))


def group(segs):
    g = defaultdict(list)
    for s in segs:
        g[(s["scan_id"], s["line_id"])].append(s)
    return g


def main():
    res = {"crit1": {}, "crit2": {}, "crit3": {}, "types": {}}
    tot_types = {k: Counter() for k in ("15g-b", "15g-c")}
    for inv in ALL_INVS:
        # 1 regression
        eq = {k: rows("15g2-all", inv, k) == rows("15g-b", inv, k) for k in ("segments", "regions")}
        res["crit1"][inv] = eq
        b, c = rows("15g-b", inv, "segments"), rows("15g-c", inv, "segments")
        lines = {(r["scan_id"], r["line_id"]): r["text"]
                 for r in read_tsv_gz(layer_root(PARENT / "15g-c", inv) / "lines" / f"{inv}.lines.tsv.gz")}
        month = re.compile(PRESET_OF(inv).types_params["month_only"], re.IGNORECASE)
        # 2 cut set
        cuts_c = [s for s in c if s["type_method"] == "rule:margin_prefix"]
        bad = [s for s in cuts_c if s["line_type"] != "date_header"
               or not month.fullmatch(lines[(s["scan_id"], s["line_id"])][int(s["char_start"]):int(s["char_end"])])]
        cuts_b_month = [s for s in b if s["type_method"] == "rule:margin_prefix" and s["line_type"] == "date_header"]
        cuts_b_other = [s for s in b if s["type_method"] == "rule:margin_prefix" and s["line_type"] != "date_header"]
        res["crit2"][inv] = {"c_cuts": len(cuts_c), "c_not_month": len(bad), "b_month_cuts": len(cuts_b_month),
                             "b_other_cuts": len(cuts_b_other), "equal": len(cuts_c) == len(cuts_b_month)}
        # 3 locality
        gb, gc = group(b), group(c)
        changed = {k for k, v in gb.items() if any(s["type_method"] == "rule:margin_prefix" and s["line_type"] != "date_header" for s in v)}
        unchanged_bad = [k for k in gb if k not in changed and gb[k] != gc.get(k)]
        changed_bad = []
        n_new_sfx = 0
        for k in changed:
            got = gc.get(k, [])
            text = lines[k]
            sig = lambda s: (s["char_start"], s["char_end"], s["line_type"], s["x"], s["w"])  # noqa: E731
            sfx_b = [sig(s) for s in gb[k] if s["type_method"] == "rule:pn_suffix"]
            sfx_c = [sig(s) for s in got if s["type_method"] == "rule:pn_suffix"]
            new_sfx = bool(sfx_c) and not sfx_b
            ok = (not any(s["type_method"] == "rule:margin_prefix" for s in got)
                  and (sfx_b == sfx_c or new_sfx) and len(got) == 1 + len(sfx_c)
                  and int(got[0]["char_start"]) == len(text) - len(text.lstrip()))
            n_new_sfx += new_sfx
            if not ok:
                changed_bad.append(k)
        res["crit3"][inv] = {"changed_lines": len(changed), "unchanged_differ": len(unchanged_bad),
                             "changed_not_as_expected": len(changed_bad), "new_pn_suffix": n_new_sfx,
                             "keys_bad": [list(k) for k in (unchanged_bad + changed_bad)[:5]],
                             "region_rows_differ": sum(x != y for x, y in zip(rows("15g-b", inv, "regions"), rows("15g-c", inv, "regions"))),
                             "n_regions": len(rows("15g-c", inv, "regions"))}
        tot_types["15g-b"].update(s["line_type"] for s in b)
        tot_types["15g-c"].update(s["line_type"] for s in c)
        res["types"][inv] = {"15g-b": dict(Counter(s["line_type"] for s in b)),
                             "15g-c": dict(Counter(s["line_type"] for s in c))}
    res["types_total"] = {k: dict(v) for k, v in tot_types.items()}
    OUT.write_text(json.dumps(res, indent=1, ensure_ascii=False), encoding="utf-8")
    print("crit1 all equal:", all(all(v.values()) for v in res["crit1"].values()), "(%d/32 tables)" % sum(sum(v.values()) for v in res["crit1"].values()))
    for inv in ALL_INVS:
        print(inv, res["crit2"][inv], {k: v for k, v in res["crit3"][inv].items() if k != "keys_bad"})
    for key in ("c_cuts", "b_month_cuts", "b_other_cuts", "c_not_month"):
        print(key, sum(v[key] for v in res["crit2"].values()))
    print("new pn_suffix", sum(v["new_pn_suffix"] for v in res["crit3"].values()))
    print("changed lines", sum(v["changed_lines"] for v in res["crit3"].values()),
          "bad", sum(v["unchanged_differ"] + v["changed_not_as_expected"] for v in res["crit3"].values()))
    print(res["types_total"])


if __name__ == "__main__":
    main()
