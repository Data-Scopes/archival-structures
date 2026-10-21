"""Step 15ir criterion 6: after the fix, merges whose right side (as joined) starts with ',' + letter; and, in 15i-b, right
sides starting with ',' by next-char class (why the brief's 3772/3822/3823 counts are higher than the letter-only counts)."""
import gzip
from collections import Counter, defaultdict
from pathlib import Path
INV = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
def rd(p):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh]
    return [dict(zip(rows[0], r)) for r in rows[1:]]
for label in ("15i-b", "15ir-a"):
    tot = Counter(); per = {}
    for inv in INV:
        r = next(Path(f"data/text_layer_v2_dev/{label}/hand").glob("*_text_layer_v2"))
        lines = {(x["scan_id"], x["line_id"]): x["text"] for x in rd(r / "lines" / f"{inv}.lines.tsv.gz")}
        segs = rd(r / "join" / "segments" / f"{inv}.tsv.gz")
        by = defaultdict(list)
        for s in segs: by[(s["scan_id"], s["unit_id"])].append(s)
        c = Counter()
        for ss in by.values():
            ss.sort(key=lambda s: int(s["seg_order"]))
            for a, b in zip(ss, ss[1:]):
                if a["merged_with_next"] != "1": continue
                t = lines[(b["scan_id"], b["line_id"])]; off = int(b["line_offset"])
                if t[off:off+1] == ",":
                    c["comma_letter" if t[off+1:off+2].isalpha() else "comma_other"] += 1
        per[inv] = dict(c); tot.update(c)
    print(label, dict(tot)); print(per)
