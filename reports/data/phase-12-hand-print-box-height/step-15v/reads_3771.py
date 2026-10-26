"""Step 15v reads on 3771 (print mode): 20 split lines + 10 merged regions (seed 17), 15 units (seed 18).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/reads_3771.py {splits|merges|units}
"""
import random
import sys
from pathlib import Path

import pandas as pd

R = Path("data/text_layer_v2_dev/15v-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4")
rd = lambda p, **k: pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False, quoting=3, **k)
what = sys.argv[1]
L = rd(R / "lines" / "3771.lines.tsv.gz", usecols=["scan_id", "page_side", "column", "region_id", "line_idx", "line_id", "x", "y", "w", "h", "text_raw", "edit_flags", "split_from"])
if what == "splits":
    sp = L[L.split_from != ""]
    groups = sorted(sp.split_from.unique())
    for g in random.Random(17).sample(groups, 20):
        h = sp[sp.split_from == g]
        print(h.scan_id.iloc[0][-4:], h.edit_flags.iloc[0], " || ".join(f"[col {r.column} x{r.x} w{r.w}] {r.text_raw}" for r in h.itertuples()))
elif what == "merges":
    reg = rd(R / "layout" / "regions" / "3771.tsv.gz")
    mg = reg[reg.change.str.contains("merged")]
    print(len(mg), "merged regions")
    lay = rd(R / "layout" / "lines" / "3771.tsv.gz")
    lt = dict(zip(L.line_id, L.text_raw))
    for r in mg.sample(10, random_state=17).itertuples():
        ids = lay[lay.layout_region_id == r.layout_region_id].sort_values("layout_line_idx", key=lambda c: c.astype(int)).line_id
        txt = [lt.get(i, "") for i in ids]
        print(r.scan_id[-4:], r.page, "col", r.layout_column, r.layout_role, f"src={len(r.source_region_ids.split())} n_lines={r.n_lines} box={r.x},{r.y},{r.w},{r.h}")
        print("   ", " / ".join(txt[:6]), "...", " / ".join(txt[-2:]))
else:
    U = rd(R / "units" / "units" / "3771.tsv.gz")
    S = rd(R / "units" / "segments" / "3771.tsv.gz")
    lt = dict(zip(L.line_id, L.text_raw))
    ids = random.Random(18).sample(sorted(U.unit_id), 15)
    for uid in ids:
        u = U[U.unit_id == uid].iloc[0]
        segs = S[S.unit_id == uid].sort_values("seg_order", key=lambda c: c.astype(int))
        print(u.scan_id[-4:], u.unit_type, u.unit_method, "n_lines", u.n_lines)
        for r in segs.itertuples():
            print("      |", lt.get(r.line_id, ""))
