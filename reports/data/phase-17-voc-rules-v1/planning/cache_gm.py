"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): cache read_volume_inputs (TL tags) and the 22b exclude set for the 545 GM volumes into a pickle (about 50 MB, 2 min).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/cache_gm.py <scratch>/gm_inputs.pkl
"""
import sys, pickle, csv, time
sys.path.insert(0, ".")
from pathlib import Path
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs
LAYER = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
rows = list(csv.DictReader(open("reports/data/phase-16-voc-generalisation/planning/gm_anchors.tsv"), delimiter="\t"))
invs = sorted({r["inv"] for r in rows})
out = {}
t = time.time()
for k, inv in enumerate(invs):
    try:
        v = read_volume_inputs(LAYER, inv, number_tags=("page-number",))
    except FileNotFoundError:
        continue
    run = front_index_run(v.flags)
    excl = {s for i, s in enumerate(v.sides) if (run and i <= run[1]) or v.flags[i]}
    out[inv] = (v.scan_ids, v.sides, v.numbers, excl)
    if k % 50 == 0:
        print(k, inv, round(time.time() - t), flush=True)
pickle.dump(out, open(sys.argv[1], "wb"))
print("done", len(out), round(time.time() - t))
