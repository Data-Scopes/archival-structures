"""Step 15k: tally changed types/segments rows between two layer bases (15j-a vs 15k-a)
by (sub, inventory, old type|method -> new type|method, band by line position in nothing)."""
import gzip, csv, sys
from collections import Counter
from pathlib import Path
a, b = Path(sys.argv[1]), Path(sys.argv[2])
def root(base, sub):
    return next((base / sub).glob("*_text_layer_v*"))
tot = Counter(); other_changes = 0
for sub in ("hand", "print"):
    ra, rb = root(a, sub), root(b, sub)
    for fa in sorted((ra / "types" / "segments").glob("*.tsv.gz")):
        inv = fa.name.split(".")[0]
        def rd(p):
            with gzip.open(p, "rt", encoding="utf-8", newline="") as fh:
                return list(csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE))
        A, B = rd(fa), rd(rb / "types" / "segments" / fa.name)
        assert len(A) == len(B)
        for x, y in zip(A, B):
            assert (x["scan_id"], x["line_id"], x["seg_idx"]) == (y["scan_id"], y["line_id"], y["seg_idx"])
            if (x["line_type"], x["type_method"]) != (y["line_type"], y["type_method"]):
                tot[(sub, inv if sub == "print" else "*", x["line_type"] + "|" + x["type_method"],
                     y["line_type"] + "|" + y["type_method"])] += 1
            for k in x:
                if k not in ("line_type", "type_method") and x[k] != y[k]:
                    other_changes += 1
for k, v in sorted(tot.items()):
    print(v, *k)
print("changes in other columns:", other_changes)
