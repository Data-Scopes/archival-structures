"""Step 15ir criteria 2 and 4 (types): hand/print type tables 15i-b vs 15ir-a. Lines whose segmentation changed (fix 3a) and the
type_method relabels (fix 3b); date_prefix segments on lines that fullmatch date_line."""
import gzip, json, re
from collections import Counter, defaultdict
from pathlib import Path
from archival_structures.text_layer.presets import PRESETS
HERE = Path(__file__).parent
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
HAND = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
res = {}; relabels = []; split_lines = []
for kind, invs, preset in (("hand", HAND, "sg_1.01.02_v2"), ("print", ["455", "456"], "sg_1.10.94_v2")):
    date_line = re.compile(PRESETS[preset].types_params["date_line"], re.IGNORECASE) if False else None
    tp = PRESETS[preset].types_params
    from archival_structures.text_layer.types import TypeParams, _compile
    pats = _compile(TypeParams.from_dict(tp))
    ra = next(Path(f"data/text_layer_v2_dev/15ir-a/{kind}").glob("*_text_layer_v2")); rb = next(Path(f"data/text_layer_v2_dev/15i-b/{kind}").glob("*_text_layer_v2"))
    for inv in invs:
        lines = {(x["scan_id"], x["line_id"]): x["text"] for x in rd(ra/"lines"/f"{inv}.lines.tsv.gz")}
        a, b = rd(ra/"types"/"segments"/f"{inv}.tsv.gz"), rd(rb/"types"/"segments"/f"{inv}.tsv.gz")
        ga, gb = defaultdict(list), defaultdict(list)
        for s in a: ga[(s["scan_id"], s["line_id"])].append(s)
        for s in b: gb[(s["scan_id"], s["line_id"])].append(s)
        assert set(ga) == set(gb)
        c = Counter(); other_cols = Counter()
        for k in ga:
            sa, sb = ga[k], gb[k]
            if len(sa) != len(sb):
                c["lines_resegmented"] += 1; split_lines.append((inv, k, lines[k], [(s["line_type"], s["type_method"]) for s in sb], [(s["line_type"], s["type_method"]) for s in sa]))
                continue
            for x, y in zip(sa, sb):
                for col in x:
                    if x[col] != y[col]:
                        other_cols[col] += 1
                if x["type_method"] != y["type_method"]:
                    c[f'{y["type_method"]}->{x["type_method"]}'] += 1
                    relabels.append((inv, kind, x["scan_id"], x["line_id"], y["type_method"], x["type_method"], x["line_type"], lines[k][:60], x["y"], x["page"]))
        dl_dp = sum(1 for s in a if s["type_method"] == "rule:date_prefix"
                    and pats["date_line"].fullmatch(lines[(s["scan_id"], s["line_id"])].strip()))
        res[inv] = {"changes": dict(c), "differing_columns_other_than_type_method": dict(other_cols),
                    "date_prefix_on_date_line_lines_new": dl_dp,
                    "date_prefix_on_date_line_lines_old": sum(1 for s in b if s["type_method"] == "rule:date_prefix" and pats["date_line"].fullmatch(lines[(s["scan_id"], s["line_id"])].strip())),
                    "pn_line_new": sum(s["type_method"] == "rule:pn_line" for s in a), "pn_line_old": sum(s["type_method"] == "rule:pn_line" for s in b),
                    "offchain_new": sum(s["type_method"] == "rule:pn_line_offchain" for s in a), "offchain_old": sum(s["type_method"] == "rule:pn_line_offchain" for s in b),
                    "page_number_segments_new": sum(s["line_type"] == "page_number" for s in a), "page_number_segments_old": sum(s["line_type"] == "page_number" for s in b)}
        print(inv, res[inv])
tot = Counter()
for v in res.values():
    for k in ("date_prefix_on_date_line_lines_new", "date_prefix_on_date_line_lines_old", "pn_line_new", "pn_line_old", "offchain_new", "offchain_old"):
        tot[k] += v[k]
    tot["lines_resegmented"] += v["changes"].get("lines_resegmented", 0)
print("TOTAL", dict(tot))
json.dump({"per_inv": res, "total": dict(tot)}, open(HERE / "types_diff.json", "w"), indent=1)
with open(HERE / "types_resegmented.tsv", "w", encoding="utf-8") as fh:
    for r in split_lines: fh.write("\t".join(map(str, (r[0], r[1][1], r[2], r[3], r[4]))) + "\n")
import pickle
pickle.dump(relabels, open(HERE / "relabels.pkl", "wb"))
