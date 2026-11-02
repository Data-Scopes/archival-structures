"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): join with ¬ in the pinned break set, on 870 ¬-ended lines of 6 volumes (F17-P.2).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/wbc_probe.py
"""
import sys, random, collections
sys.path.insert(0, ".")
import pandas as pd
from archival_structures.text_layer.join_v2 import join_unit
L = "data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/lines/"
random.seed(1)
pairs = []
for inv in ["1120", "1274", "1557", "2448", "1917", "3826"]:
    d = pd.read_csv(L + f"{inv}.lines.tsv.gz", sep="\t", usecols=["region_id", "line_idx", "text"], dtype=str).fillna("")
    for rid, g in d.groupby("region_id", sort=False):
        t = [x for x in g.text.tolist() if x.strip()]
        for i in range(len(t) - 1):
            if t[i].rstrip().endswith("¬"):
                pairs.append((inv, t[i], t[i + 1]))
print("pairs", len(pairs))
kinds = collections.Counter(); lead = collections.Counter()
for inv, a, b in pairs:
    txt, spans, bnd = join_unit([a, b], "-=„¬", lead_marks=",")
    kinds[bnd[0][0]] += 1
    lead[b.lstrip()[:1] if b.lstrip()[:1] in "„,-¬" else "letter/other"] += 1
print(kinds, lead)
for inv, a, b in random.sample(pairs, 12):
    txt, spans, bnd = join_unit([a, b], "-=„¬", lead_marks=",")
    print(inv, bnd[0][0], "|", a[-25:], "||", b[:20], "=>", txt[max(0, len(a) - 25): len(a) + 15])
