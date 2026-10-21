"""Step 15ir fix 4 error analysis: units that now take a legacy region id (twins); 10 read (Random(34))."""
import gzip, json, random
from collections import Counter, defaultdict
from pathlib import Path
HERE = Path(__file__).parent
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
ra = next(Path("data/text_layer_v2_dev/15ir-a/print").glob("*_text_layer_v2")); rb = next(Path("data/text_layer_v2_dev/15i-b/print").glob("*_text_layer_v2"))
res = {}; allt = []
for inv in ("455", "456"):
    legacy = {(r["scan_id"], r["region_id"]) for r in rd(ra/"regions_meta"/f"{inv}.tsv.gz")}
    layout_ids = {(r["scan_id"], r["layout_region_id"]) for r in rd(ra/"layout"/"regions"/f"{inv}.tsv.gz")}
    lines = rd(ra/"lines"/f"{inv}.lines.tsv.gz")
    reg_lines = defaultdict(list)
    for l in lines:
        if l["text"].strip(): reg_lines[(l["scan_id"], l["region_id"])].append(l)
    useg = defaultdict(list)
    for s in rd(ra/"units"/"segments"/f"{inv}.tsv.gz"): useg[(s["scan_id"], s["unit_id"])].append(s)
    types = {(s["scan_id"], s["line_id"], s["seg_idx"]): s for s in rd(ra/"types"/"segments"/f"{inv}.tsv.gz")}
    ltext = {(l["scan_id"], l["line_id"]): l["text"] for l in lines}
    jt = {(u["scan_id"], u["unit_id"]): u["text"] for u in rd(ra/"join"/"units"/f"{inv}.tsv.gz")}
    old_ids = {(u["scan_id"], u["unit_id"]) for u in rd(rb/"units"/"units"/f"{inv}.tsv.gz")}
    dmap = {r["old_doc_id"]: r for r in rd(ra/"units"/"doc_id_map"/f"{inv}.tsv.gz") if r["relation"] == "same_id"} if False else None
    c = Counter()
    for u in rd(ra/"units"/"units"/f"{inv}.tsv.gz"):
        k = (u["scan_id"], u["unit_id"])
        if k in legacy and k not in layout_ids:
            ul = [s["line_id"] for s in useg[k]]
            rl = [l["line_id"] for l in reg_lines[k]]
            same_lines = set(ul) == set(rl)
            covers = all((int(types[(s["scan_id"], s["line_id"], s["seg_idx"])]["char_start"]), int(types[(s["scan_id"], s["line_id"], s["seg_idx"])]["char_end"])) == (0, len(ltext[(s["scan_id"], s["line_id"])].rstrip())) or True for s in useg[k])
            full = all(len(useg[k]) == len(set(ul)) for _ in [0]) and same_lines
            c["twin_units"] += 1; c["same_line_set"] += same_lines; c["one_segment_per_line"] += full
            c["was_in_15i-b"] += (k in old_ids)
            allt.append((inv, k, u, ul, rl, jt[k]))
    res[inv] = dict(c)
print(res)
json.dump(res, open(HERE / "twins_summary.json", "w"), indent=1)
pick = random.Random(34).sample(allt, 10)
out = []
for inv, k, u, ul, rl, text in pick:
    out.append(f"{inv} {k[0][-9:]} id {k[1][-30:]}: unit type {u['unit_type']}, {len(ul)} lines, legacy region has {len(rl)} lines, same set {set(ul)==set(rl)}, text {text[:90]!r}")
open(HERE / "twins_sample.txt", "w").write("\n".join(out) + "\n"); print("\n".join(out))
