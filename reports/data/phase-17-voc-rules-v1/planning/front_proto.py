"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): front_index_run with SG defaults vs scaled thresholds on the prototype TOC flags (F17-P.6).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: P17_CACHE_DIR=<scratch> .venv/bin/python reports/data/phase-17-voc-rules-v1/planning/front_proto.py reports/data/phase-17-voc-rules-v1/planning/toc_proto.py
"""
import sys, math
sys.path.insert(0, ".")
src = open(sys.argv[1]).read().split("invs = E.ALL")[0]
exec(src)
from archival_structures.analysis.index_links import front_index_run
def side_flags(inv, W, t, a, g, ut):
    keys, _, scans = vols[inv]
    order = {s: i for i, s in enumerate(scans)}
    fl = [order[k[0]] < W and f["amt"] < a and ((f["n"] >= 3 and f["fol"] + f["date"] >= t) or (ut and f["title"]))
          for k, f in zip(keys, F[inv])]
    idx = [i for i, x in enumerate(fl) if x]
    for i, j in zip(idx, idx[1:]):
        if j - i - 1 <= g:
            for k in range(i + 1, j): fl[k] = True
    return fl
cfg = (60, 0.2, 0.15, 4, True)
sg = sc = 0; rows = []
for inv in E.ALL:
    fl = side_flags(inv, *cfg); n = len(fl)
    a = front_index_run(fl)
    b = front_index_run(fl, min_flagged=1, max_first=60, gap=20, min_after=min(300, math.ceil(0.5 * n)))
    sg += a is not None; sc += b is not None
    rows.append((inv, n, sum(fl), a, b))
print("SG defaults", sg, "scaled", sc, "of", len(E.ALL))
for r in rows: print(r)
