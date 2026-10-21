"""Step 15ir error analysis fix 1: print units whose text changed between 15i-b and 15ir-a; sample 20 (Random(31))."""
import gzip, json, random
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).parent
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
ra = next(Path("data/text_layer_v2_dev/15ir-a/print").glob("*_text_layer_v2")); rb = next(Path("data/text_layer_v2_dev/15i-b/print").glob("*_text_layer_v2"))
changed = []; summary = {}
for inv in ("455", "456"):
    ta = {(u["scan_id"], u["unit_id"]): u for u in rd(ra/"join"/"units"/f"{inv}.tsv.gz")}
    tb = {(u["scan_id"], u["unit_id"]): u for u in rd(rb/"join"/"units"/f"{inv}.tsv.gz")}
    both = set(ta) & set(tb)
    ch = [k for k in sorted(both) if ta[k]["text"] != tb[k]["text"]]
    summary[inv] = {"units_old": len(tb), "units_new": len(ta), "in_both": len(both), "text_changed_among_both": len(ch),
                    "only_old": len(set(tb) - set(ta)), "only_new": len(set(ta) - set(tb))}
    changed += [(inv, k, ta[k], tb[k]) for k in ch]
print(summary)
json.dump(summary, open(HERE / "units_changed_summary.json", "w"), indent=1)
pick = random.Random(31).sample(changed, 20)
lines_cache = {}; useg_cache = {}
out = []
for inv, k, ua, ub in pick:
    if inv not in lines_cache:
        lines_cache[inv] = {(x["scan_id"], x["line_id"]): x for x in rd(ra/"lines"/f"{inv}.lines.tsv.gz")}
        d = defaultdict(list)
        for s in rd(ra/"units"/"segments"/f"{inv}.tsv.gz"): d[(s["scan_id"], s["unit_id"])].append(s)
        useg_cache[inv] = d
    ss = sorted(useg_cache[inv][k], key=lambda s: int(s["seg_order"]))
    seq = []
    for s in ss:
        l = lines_cache[inv][(s["scan_id"], s["line_id"])]
        seq.append((int(l["y"]), int(l["x"]), l["text"][:28]))
    jumps = sum(1 for a, b in zip(seq, seq[1:]) if b[0] < a[0] - 50)
    out.append(f"== {inv} {k[0][-9:]} unit {k[1][-26:]} lines={len(seq)} new_text_jumps={jumps}\n  NEW: {ua['text'][:200]!r}\n  OLD: {ub['text'][:200]!r}\n  y/x/text (new order): " + " | ".join(f"{y},{x}:{t!r}" for y, x, t in seq[:8]))
open(HERE / "units_changed_sample.txt", "w").write("\n".join(out) + "\n")
print("\n".join(out))
