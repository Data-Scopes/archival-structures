"""Step 15k error analysis: 10 retyped segments per stratum (Random(81)) with page context."""
import gzip, csv, glob, random, sys
from collections import defaultdict
def rd(p): return list(csv.DictReader(gzip.open(p, "rt"), delimiter="\t", quoting=csv.QUOTE_NONE))
rnd = random.Random(81)
pools = defaultdict(list)
for sub in ("hand", "print"):
    root = glob.glob(f"data/text_layer_v2_dev/15k-a/{sub}/*_text_layer_v2")[0]
    for f in sorted(glob.glob(f"{root}/types/segments/*.tsv.gz")):
        inv = f.split("/")[-1].split(".")[0]
        L = {(r["scan_id"], r["line_id"]): r["text"] for r in rd(f"{root}/lines/{inv}.lines.tsv.gz")}
        S = rd(f)
        conf = defaultdict(list)
        for s in S:
            if s["type_method"] in ("rule:pn_line", "rule:pn_line_gap"):
                conf[(s["scan_id"], s["page"])].append(L[(s["scan_id"], s["line_id"])][int(s["char_start"]):int(s["char_end"])].strip())
        for s in S:
            m = s["type_method"]
            if s["line_type"] == "other" and m in ("rule:year_offchain", "rule:pn_line_offchain"):
                t = L[(s["scan_id"], s["line_id"])][int(s["char_start"]):int(s["char_end"])].strip()
                key = "year" if m == "rule:year_offchain" else f"{sub}-band"
                pools[key].append((sub, inv, s["scan_id"][-9:], s["page"], t, " ".join(conf[(s["scan_id"], s["page"])])))
for k, v in pools.items():
    print("##", k, len(v))
    for row in rnd.sample(v, 10): print("  ", *row, sep="\t")
