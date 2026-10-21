"""Step 15ir error analysis fix 3b: 20 relabelled pn_line segments (Random(33)) with the page's top/bottom band."""
import gzip, pickle, random
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).parent
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
rel = pickle.load(open(HERE / "relabels.pkl", "rb"))
pick = random.Random(33).sample(rel, 20)
cache = {}
out = []
for inv, kind, scan, lid, old, new, lt, text, y, page in pick:
    r = next(Path(f"data/text_layer_v2_dev/15ir-a/{kind}").glob("*_text_layer_v2"))
    if inv not in cache:
        segs = rd(r/"types"/"segments"/f"{inv}.tsv.gz"); lines = {(x["scan_id"], x["line_id"]): x for x in rd(r/"lines"/f"{inv}.lines.tsv.gz")}
        bypage = defaultdict(list)
        for s in segs:
            l = lines[(s["scan_id"], s["line_id"])]
            bypage[(s["scan_id"], s["page"])].append((int(l["y"]), l["text"][:30], s["line_type"], s["type_method"].replace("rule:", ""), s["line_id"]))
        cache[inv] = bypage
    pg = sorted(cache[inv][(scan, page)])
    band = pg[:3] + pg[-2:]
    out.append(f"{inv} {scan[-10:]} {page}: relabelled {text!r} y={y}\n   top/bottom band: " + " | ".join(f"{b[0]}:{b[1]!r}:{b[2]}/{b[3]}" for b in band))
open(HERE / "relabel_context.txt", "w").write("\n".join(out) + "\n")
print("\n".join(out))
