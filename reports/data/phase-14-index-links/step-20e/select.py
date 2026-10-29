"""Step 20e: choose the locator variant on the 20 dev gold rows (L1-L4; max F1, ties -> lower variant).
Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/step-20e/select.py
Writes select.tsv and select_diffs.tsv in this directory."""
import sys
from collections import Counter
sys.path.insert(0, "."); sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *
from archival_structures.analysis.index_links import SG_LOCATOR_PARAMS, extract_locators

rows = [g for g in gold() if g["split"] == "dev"]
texts = {}
for inv in {g["inv"] for g in rows}:
    texts.update({(inv,) + k: v for k, v in unit_texts(inv).items()})
print(len(rows), "dev rows")
tot = {v: [0, 0, 0] for v in SG_LOCATOR_PARAMS}
diffs = []
for g in rows:
    t = texts[(g["inv"], g["scan_id"], g["unit_id"])]
    gn = gold_numbers(g)
    for v, p in SG_LOCATOR_PARAMS.items():
        pred = [l.number for l in extract_locators(t, p)]
        tp, npred, ng = compare(pred, gn)
        for i, x in enumerate((tp, npred, ng)):
            tot[v][i] += x
        if Counter(pred) != Counter(gn):
            diffs.append([g["read_id"], v, g["inv"], g["entry_verdict"], gn, pred,
                          sorted((Counter(pred) - Counter(gn)).elements()), sorted((Counter(gn) - Counter(pred)).elements()),
                          t[-90:].replace("\t", " ")])
with open(OUT / "select.tsv", "w") as f:
    f.write("variant\tTP\tn_pred\tn_gold\tP\tR\tF1\n")
    best = None
    for v, (tp, n, ng) in tot.items():
        p, r, f1 = prf(tp, n, ng)
        f.write(f"{v}\t{tp}\t{n}\t{ng}\t{p:.4f}\t{r:.4f}\t{f1:.4f}\n")
        print(v, tp, n, ng, f"P={p:.4f} R={r:.4f} F1={f1:.4f}")
        if best is None or f1 > best[1] + 1e-12:
            best = (v, f1)
print("CHOICE", best)
with open(OUT / "select_diffs.tsv", "w") as f:
    f.write("read_id\tvariant\tinv\tverdict\tgold\tpred\tfp\tfn\ttext_tail\n")
    for d in diffs:
        f.write("\t".join(map(str, d)) + "\n")
