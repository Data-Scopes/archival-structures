"""Step 15w, acceptance 2: every mode/<inv>.json `value` of 15w-a against the 168 reference volumes (15u / phase 10).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15w/values_check.py
Writes modes.tsv (all volumes: inv, archive, mode, method, value, margin, silver, series, set) and prints the counts.
"""
import json
from pathlib import Path

import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height")
L = Path("data/text_layer_v2_dev/15w-a")
refs = []
for name in ("fit", "test", "diagnostic"):
    t = pd.read_csv(D / f"step-15u/{name}_volumes.tsv", sep="\t", dtype={"inv": str})
    t = t.assign(ref_set=name + (":" + t["set"] if "set" in t else ""))[["inv", "value", "ref_set"]]
    refs.append(t)
h = pd.read_csv("reports/data/phase-10-hand-print-per-region/review/h_by_volume.tsv", sep="\t", dtype={"inv": str})
h = h[h["set"] == "staged"].rename(columns={"h_w": "value"}).assign(ref_set="ua017")[["inv", "value", "ref_set"]]
refs.append(h)
ref = pd.concat(refs, ignore_index=True)
inv = pd.read_csv("reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv", sep="\t", dtype=str,
                  usecols=["inv", "silver", "ancestor_3"])
rows = []
for archive in ("1.01.02", "1.10.94"):
    for f in sorted((L / archive).glob("NL-HaNA_*/mode/*.json")):
        m = json.loads(f.read_text())
        rows.append({"inv": m["inv"], "archive": archive, "mode": m["mode"], "method": m["method"],
                     "value": m["value"], "margin": m["margin"], "n_scans": m["n_scans"],
                     "scan_minority_share": round(m["scan_minority_share"], 4)})
modes = pd.DataFrame(rows)
s = inv.rename(columns={"ancestor_3": "series"})
modes = modes.merge(s, on="inv", how="left").where(lambda d: d.notna(), "")
modes.loc[modes.archive == "1.10.94", ["silver", "series"]] = ""
ref1 = ref.rename(columns={"value": "ref_value"})
cmp_ = modes.merge(ref1, on="inv", how="left")
# 455/456 are in the 1.10.94 archive; the 1.01.02 inv numbers 455/456 do not exist in the staged set
cmp_ = cmp_[~((cmp_.archive == "1.01.02") & cmp_.inv.isin(["455", "456"]))]
modes = cmp_.drop_duplicates("inv")
have = modes[modes.ref_set.notna()]
eq = (have.value.astype(float) == have.ref_value.astype(float))
print("volumes built:", len(modes), "| with reference:", len(have), "| equal:", int(eq.sum()))
print("ref_set counts:", have.ref_set.value_counts().to_dict())
print("unequal:", have[~eq][["inv", "value", "ref_value"]].to_dict("records"))
print("missing refs (reference volumes not built):", sorted(set(ref.inv) - set(modes.inv)))
print("mode counts (all):", modes.groupby(["archive", "mode", "method"]).size().to_dict())
print("mode counts (reference 168):", have.groupby(["archive", "mode"]).size().to_dict())
modes.to_csv(D / "step-15w/modes.tsv", sep="\t", index=False)
res = modes[modes.ref_set.isna()]
print("reserves (no reference):", len(res))
print(res[["inv", "mode", "value", "margin", "silver", "series"]].sort_values("value").to_string(index=False))
