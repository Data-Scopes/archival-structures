"""Step 20d error analysis: LOO misses of dev volumes + 3712 (30 at random.Random(20261131) if more than 30),
with the chain neighbours and the printed texts around the anchor. Run from the AS root with .venv/bin/python -I."""
import csv, random, sys
from pathlib import Path
sys.path.insert(0, ".")
from archival_structures.analysis.index_links import read_volume_inputs
L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
D = Path("reports/data/phase-14-index-links/step-20d")
rows = [r for r in csv.DictReader(open(D / "loo.tsv"), delimiter="\t")
        if r["inv"] in {"3254", "3471", "3394", "3308", "3712"} and r["position"] != r["predicted"]]
print(len(rows), "misses")
if len(rows) > 30:
    rows = random.Random(20261131).sample(rows, 30)
allr = list(csv.DictReader(open(D / "loo.tsv"), delimiter="\t"))
vi = {}
for r in sorted(rows, key=lambda r: (r["inv"], int(r["position"]))):
    inv, n, p, q = r["inv"], int(r["number"]), int(r["position"]), int(r["predicted"])
    v = vi.setdefault(inv, read_volume_inputs(L, inv))
    ch = [(int(x["position"]), int(x["number"])) for x in allr if x["inv"] == inv]
    k = [i for i, c in enumerate(ch) if c == (p, n)][0]
    nb = ch[max(0, k - 2):k + 3]
    if inv == "3712":  # pagination mode: positions are side positions
        txt = [(pp, v.sides[pp - 1][1][0], v.numbers.get(v.sides[pp - 1])) for pp in range(p - 2, p + 3) if 0 < pp <= len(v.sides)]
    else:  # foliation: positions are scan positions, texts of the recto
        txt = [(pp, v.numbers.get((v.scan_ids[pp - 1], "recto"))) for pp in range(p - 2, p + 3) if 0 < pp <= len(v.scan_ids)]
    print(inv, f"N={n} pos={p} pred={q} diff={q-p}", "chain(inner only)", nb, "texts", txt)
