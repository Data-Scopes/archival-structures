"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): offsets gold scan minus map front scan, by layout, anchor kind and method (F17-P.4).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/offsets.py <scratch>/gm_inputs.pkl
"""
import sys, csv, collections
sys.argv = [sys.argv[0], sys.argv[1]]
exec(open(sys.argv[0].replace("offsets.py", "map_proto.py")).read().split("OPTS = ")[0])
lay = {}
for r in csv.DictReader(open("reports/data/phase-16-voc-generalisation/step-22a/sides.tsv"), delimiter="\t"):
    s, v, rc = int(r["single"]), int(r["verso"]), int(r["recto"])
    lay[r["inv"]] = "openings" if (v + rc) >= 0.5 * (s + v + rc) else "single"
off = collections.defaultdict(collections.Counter)
for inv in sorted({r["inv"] for r in rows}):
    if inv not in VOLS: continue
    scan_ids, sides, numbers, excl = VOLS[inv]
    fm, pcs = build(scan_ids, sides, numbers, excl, {})
    pos = {s: i for i, s in enumerate(scan_ids)}
    for r in rows:
        if r["inv"] != inv: continue
        g = f"NL-HaNA_1.04.02_{inv}_{int(r['scan']):04d}"
        if g not in pos: continue
        t = fm.locate(int(r["folio"]))
        if t is None: off[(lay[inv], r["kind"])]["none"] += 1; continue
        d = pos[g] - pos[t.front[0]]
        off[(lay[inv], r["kind"], t.method)][d if abs(d) <= 3 else ("<-3" if d < 0 else ">3")] += 1
for k in sorted(off, key=str):
    print(k, sorted(off[k].items(), key=lambda x: str(x[0])))
