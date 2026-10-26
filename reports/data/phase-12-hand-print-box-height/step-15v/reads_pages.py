"""Step 15v: 10 pages (seed 17) of 3771, print-mode build (15v-a) next to the hand-mode build (15m-a/hand).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/reads_pages.py [scan_suffix ...]
"""
import random
import sys
from pathlib import Path

import pandas as pd

A = Path("data/text_layer_v2_dev/15m-a/hand/NL-HaNA_1.01.02_text_layer_v3")   # hand rules
B = Path("data/text_layer_v2_dev/15v-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4")  # print rules
rd = lambda p, **k: pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False, quoting=3, **k)


def units(root, scan):
    U = rd(root / "units" / "units" / "3771.tsv.gz")
    U = U[U.scan_id == scan]
    S = rd(root / "units" / "segments" / "3771.tsv.gz")
    S = S[S.scan_id == scan]
    L = rd(root / "lines" / "3771.lines.tsv.gz", usecols=["scan_id", "line_id", "text_raw"])
    lt = dict(zip(L.line_id[L.scan_id == scan], L.text_raw[L.scan_id == scan]))
    out = []
    for u in U.sort_values("unit_order", key=lambda c: c.astype(int)).itertuples():
        segs = S[S.unit_id == u.unit_id].sort_values("seg_order", key=lambda c: c.astype(int))
        out.append((u.unit_type, u.unit_method, int(u.n_lines), " ".join(lt.get(i, "") for i in segs.line_id)))
    return out


sc = rd(B / "scans" / "3771.scans.tsv.gz", usecols=["scan_id"]).scan_id.tolist()
pick = [f"NL-HaNA_1.01.02_3771_{s}" for s in sys.argv[1:]] or random.Random(17).sample(sc, 10)
for scan in pick:
    ua, ub = units(A, scan), units(B, scan)
    print(f"=== {scan[-4:]}: hand {len(ua)} units, print {len(ub)} units")
    for name, u in (("HAND ", ua), ("PRINT", ub)):
        print(f"  {name} types:", {t: sum(1 for x in u if x[0] == t) for t in sorted({x[0] for x in u})})
        for x in u[:int(sys.argv[0] and 9)]:
            print(f"    {x[0][:9]:9s} {x[1][:18]:18s} n={x[2]:2d} {x[3][:110]}")
