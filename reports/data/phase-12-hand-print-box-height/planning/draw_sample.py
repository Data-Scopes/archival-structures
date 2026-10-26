"""Phase-12 planning: draw the fresh test sample for the box-height rule (seed 20261212).

Silver strata = EAD `ancestor_3` series of the 15p inventory table (same strata and sizes as the phase-10
sample, decision P10 there). Excluded: the 14 dev volumes, the 60 volumes of
`phase-10-hand-print-per-region/planning-15q/silver_sample.tsv` (spent, F10-R.5), the 12 UA-017 volumes of
`step-15p/staging.tsv`. Per stratum the shuffled order is kept: the first k are the sample, the next 5 are
reserves, used in order only for a tar that is missing (decided before any geometry is computed).
Plus a diagnostic set: 20 (+5 reserves) volumes outside every silver group, plain numeric ids, chars > 0.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/planning/draw_sample.py
"""
import random
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
P10 = ROOT / "reports/data/phase-10-hand-print-per-region"
OUT = Path(__file__).with_name("test_sample.tsv")
SEED = 20261212
STRATA = [  # (name, silver, ancestor_3 prefix, k)
    ("eerste_serie", "hand", "De eerste serie registers van ordinaris resoluties", 10),
    ("tweede_serie", "hand", "De tweede serie registers van ordinaris net-resoluties", 20),
    ("gedrukte_resoluties", "print", "De gedrukte resoluties van de Staten-Generaal", 10),
    ("nouvelles", "print", "Gedrukte nouvelles van Nederlandse gezanten", 10),
    ("plakkaten", "print", "Gedrukte plakkaten", 10),
]
N_RESERVE = 5

inv = pd.read_csv(P10 / "step-15p/inventories.tsv", sep="\t", dtype=str,
                  usecols=["inv", "silver", "ancestor_3", "dev", "chars", "label"])
seen = set(pd.read_csv(P10 / "planning-15q/silver_sample.tsv", sep="\t", dtype=str).inv)
staged = set(pd.read_csv(P10 / "step-15p/staging.tsv", sep="\t", dtype=str).inv)
excl = seen | staged | set(inv.inv[inv.dev == "True"])
inv = inv[~inv.inv.isin(excl)]
rng = random.Random(SEED)
rows = []
for name, silver, prefix, k in STRATA:
    pool = inv[(inv.silver == silver) & inv.ancestor_3.fillna("").str.startswith(prefix)]
    ids = sorted(pool.inv, key=lambda s: (len(s), s))
    rng.shuffle(ids)
    for i, x in enumerate(ids[:k + N_RESERVE]):
        rows.append({"inv": x, "set": "test", "stratum": name, "silver": silver, "role": "sample" if i < k else f"reserve{i - k + 1}"})
pool = inv[inv.silver.isna() & inv.inv.str.fullmatch(r"\d+") & (inv.chars.astype(float) > 0)]
ids = sorted(pool.inv, key=lambda s: (len(s), s))
rng.shuffle(ids)
for i, x in enumerate(ids[:20 + N_RESERVE]):
    rows.append({"inv": x, "set": "diagnostic", "stratum": "non_silver", "silver": "", "role": "sample" if i < 20 else f"reserve{i - 19}"})
out = pd.DataFrame(rows)
# stratum_size = the phase-10 sizes (silver volumes outside dev), so the weights are the same as in 15q
sizes = pd.read_csv(P10 / "planning-15q/silver_sample.tsv", sep="\t", dtype=str).groupby("stratum").stratum_size.first()
out["stratum_size"] = out.stratum.map(sizes).fillna("")
lab = inv.set_index("inv")
out["chars"] = out.inv.map(lab.chars)
out["label15p"] = out.inv.map(lab.label)
out.to_csv(OUT, sep="\t", index=False)
print(out.groupby(["set", "stratum", "role"]).size().unstack(fill_value=0).iloc[:, :2])
s = out[out.role == "sample"]
print("sample volumes", len(s), "chars M", round(s.chars.astype(float).sum() / 1e6, 1),
      "est GB (15 MB per M chars)", round(s.chars.astype(float).sum() / 1e6 * 15 / 1000, 2))
