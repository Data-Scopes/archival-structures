"""Step 15ir fix 5: merges (right side start) and removed lead commas, 15i-b vs 15ir-a (hand). Writes commas.json and
commas_sample.tsv (Random(32), 30 removed commas, >= 10 from 11182)."""
import gzip
import json
import random
from collections import Counter, defaultdict
from pathlib import Path

INV = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
HERE = Path(__file__).parent


def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]


def stats(base, inv, lines):
    r = next(Path(base, "hand").glob("*_text_layer_v2"))
    segs = rd(r / "join" / "segments" / f"{inv}.tsv.gz")
    n_merge = n_lower = n_comma_letter = 0
    first_chars = Counter()
    by_unit = defaultdict(list)
    for s in segs:
        by_unit[(s["scan_id"], s["unit_id"])].append(s)
    removed = []
    for u, ss in by_unit.items():
        ss.sort(key=lambda s: int(s["seg_order"]))
        for a, b in zip(ss, ss[1:]):
            if a["merged_with_next"] != "1":
                continue
            n_merge += 1
            ltxt = lines[(b["scan_id"], b["line_id"])]
            off = int(b["line_offset"])
            # first char of the right side as it is in the source line (after the types cut), before any dropped mark
            ts = types_start[(inv, b["scan_id"], b["line_id"], b["seg_idx"])]
            raw = ltxt[ts:]
            ch = ltxt[off:off + 1]
            first_chars[ch if not ch.isalpha() else ("lower" if ch.islower() else "upper")] += 1
            n_lower += ch.islower()
            if raw[:1] == "," and raw[1:2].isalpha():
                n_comma_letter += 1
            if off > ts and ltxt[ts:off].strip("„-=") == "," or (off > ts and "," in ltxt[ts:off]):
                removed.append((inv, a, b, ltxt))
    return {"n_merge": n_merge, "n_right_lower": n_lower, "lower_share": round(n_lower / max(n_merge, 1), 4),
            "n_raw_comma_letter": n_comma_letter, "n_removed": len(removed), "first": dict(first_chars.most_common(8))}, removed


types_start = {}
res = {}
all_removed = {}
for label, base in (("15i-b", "data/text_layer_v2_dev/15i-b"), ("15ir-a", "data/text_layer_v2_dev/15ir-a")):
    res[label] = {}
    for inv in INV:
        r = next(Path(base, "hand").glob("*_text_layer_v2"))
        lines = {(x["scan_id"], x["line_id"]): x["text"] for x in rd(r / "lines" / f"{inv}.lines.tsv.gz")}
        types_start.update({(inv, x["scan_id"], x["line_id"], x["seg_idx"]): int(x["char_start"])
                            for x in rd(r / "types" / "segments" / f"{inv}.tsv.gz")})
        res[label][inv], rem = stats(base, inv, lines)
        if label == "15ir-a":
            all_removed[inv] = [(a, b, lines) for _, a, b, _ in rem]
tot = {l: {k: sum(res[l][i][k] for i in INV) for k in ("n_merge", "n_right_lower", "n_raw_comma_letter", "n_removed")} for l in res}
json.dump({"per_inv": res, "total": tot}, open(HERE / "commas.json", "w"), indent=1, ensure_ascii=False)
for l in res:
    print(l, tot[l])
    print({i: (res[l][i]["n_merge"], res[l][i]["lower_share"], res[l][i]["n_raw_comma_letter"], res[l][i]["n_removed"]) for i in INV})
# sample
rng = random.Random(32)
pool = [(inv, a, b, lines) for inv, v in all_removed.items() for a, b, lines in v]
p11182 = [x for x in pool if x[0] == "11182"]
other = [x for x in pool if x[0] != "11182"]
pick = rng.sample(p11182, 12) + rng.sample(other, 18)
with open(HERE / "commas_sample.tsv", "w", encoding="utf-8") as fh:
    fh.write("inv\tscan_id\tprev_line_id\tline_id\tprev_line_tail\tline_head\n")
    for inv, a, b, lines in pick:
        pl = lines[(a["scan_id"], a["line_id"])]
        l = lines[(b["scan_id"], b["line_id"])]
        fh.write(f"{inv}\t{a['scan_id']}\t{a['line_id']}\t{b['line_id']}\t{pl[-25:]}\t{l[:25]}\n")
print(len(p11182), len(other))
