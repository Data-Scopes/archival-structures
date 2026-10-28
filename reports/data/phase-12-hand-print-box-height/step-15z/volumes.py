"""Volume table of step 15z: counts per verdict and the pre-registered pin rule.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/volumes.py
Writes volumes.tsv and pins.txt (volumes the rule pins whose mode in 15z-a differs from the pin) and pins_drive.txt.
"""
import json
from pathlib import Path

import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
OUT = D / "step-15z"
A = Path("data/text_layer_v2_dev/15z-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode")
v = pd.read_csv(OUT / "verdicts.tsv", sep="\t", dtype={"inv": str, "read_id": int})
cnt = v.groupby(["inv", "verdict"]).size().unstack(fill_value=0)
for c in ("printed", "handwritten", "mixed page", "table/numbers", "ocr_junk", "unreadable input", "unclear"):
    if c not in cnt:
        cnt[c] = 0
titles = pd.read_csv(OUT / "titles.tsv", sep="\t", dtype=str).set_index("inv")["title"].to_dict() if (OUT / "titles.tsv").exists() else {}
full = Path("data/text_layer_v4_full/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode")
rows = []
for inv, r in cnt.iterrows():
    m = json.loads((A / f"{inv}.json").read_text())
    f = json.loads((full / f"{inv}.json").read_text())
    built = f["mode"]  # box-height mode of the full run (3463 is pinned in 15z-a)
    same_cls, opp_cls, opp_mode = ("printed", "handwritten", "hand") if built == "print" else ("handwritten", "printed", "print")
    same, opp = int(r[same_cls]), int(r[opp_cls])
    readable = int(r["printed"] + r["handwritten"])
    n_read = int(r.sum())
    if opp >= 4 or (readable < 5 and same == 0 and opp >= 2):
        dec = f"pin {opp_mode}"
    elif opp in (2, 3):
        dec = "mixed"
    else:
        dec = "as built"
    rows.append(dict(inv=inv, built_mode=built, value=f["value"], split=(f.get("split_gate") or {}).get("split"), n_read=n_read,
                     printed=int(r["printed"]), handwritten=int(r["handwritten"]), table=int(r["table/numbers"]),
                     unclear=int(r["unclear"]), mixed_page=int(r["mixed page"]), decision=dec, title=titles.get(inv, "")))
t = pd.DataFrame(rows)
t.to_csv(OUT / "volumes.tsv", sep="\t", index=False)
print(t.drop(columns=["title"]).to_string())
print(t.decision.value_counts())
pins = t[t.decision.str.startswith("pin")]
(OUT / "pins.txt").write_text("".join(f"{r.inv}\n" for r in pins.itertuples() if r.decision.split()[1] != json.loads((A / f"{r.inv}.json").read_text())["mode"]))
