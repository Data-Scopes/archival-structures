"""Phase-10 review, exploratory (F10-R.3): char-weighted median line-box height per evaluated volume.

Reads the 15q region caches (regions with >= 100 chars) and the 15q volume tables; writes
h_by_volume.tsv beside this script. Post hoc on the spent 15q sample: a lead, not a result.

    .venv/bin/python -I reports/data/phase-10-hand-print-per-region/review/h_by_volume.py
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
D = ROOT / "reports/data/phase-10-hand-print-per-region/step-15q"
GEOM = ROOT / "data/hand_print_geom/15q-a"
DEV_PRINT = {"3771", "3772", "3773", "3821", "3822", "3823"}

rows = []
for set_, arch in [("dev_eval", "1.01.02"), ("transfer", "1.10.94"), ("sample", "1.01.02"), ("staged", "1.01.02")]:
    v = pd.read_csv(D / f"{set_}_volumes.tsv", sep="\t", dtype={"inv": str})
    for _, r in v.iterrows():
        d = pd.read_csv(GEOM / arch / "regions" / f"{r.inv}.tsv.gz", sep="\t", usecols=["h_med", "n_chars"]).dropna()
        d = d[d.n_chars >= 100].sort_values("h_med")
        c = d.n_chars.cumsum()
        h_w = d.h_med[c >= c.iloc[-1] / 2].iloc[0]
        if set_ == "dev_eval":
            silver = "print" if r.inv in DEV_PRINT else "hand"
        elif set_ == "transfer":
            silver = "print"
        else:
            silver = r.get("silver") if isinstance(r.get("silver"), str) else "-"
        rows.append((set_, r.inv, silver, r["mode"], round(r.print_scan_share, 3), h_w))
t = pd.DataFrame(rows, columns=["set", "inv", "silver", "mode", "share", "h_w"])
t.to_csv(Path(__file__).with_name("h_by_volume.tsv"), sep="\t", index=False)
print(t.groupby("silver").h_w.describe().round(1))
