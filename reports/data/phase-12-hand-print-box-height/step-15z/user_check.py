"""UA-035 part 1: compare the user's check-page verdicts (check_verdicts.json) with the agent's blind reads (verdicts.tsv).

The user's convention (UA-035 response): `Printed`/`Handwritten` only when all five scans agree; otherwise `Mixed`, with the
five scans' scripts in the note in page order ("print hand print print print"; "had" read as "hand"). Page order = read_id
order. Writes user_check.tsv (one row per read scan) and prints the per-scan agreement and the pin rule applied to the
user's labels. Run from the AS root: .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/user_check.py
"""
import json
from collections import Counter
from pathlib import Path

import pandas as pd

D = Path("reports/data/phase-12-hand-print-box-height/step-15z")
ALL = {"Handwritten": "handwritten", "Printed": "printed"}
WORD = {"print": "printed", "hand": "handwritten", "had": "handwritten"}

v = pd.read_csv(D / "verdicts.tsv", sep="\t", dtype=str).sort_values("read_id", key=lambda s: s.astype(int))
vol = pd.read_csv(D / "volumes.tsv", sep="\t", dtype=str).set_index("inv")
user = {it["inv"]: it for it in json.loads((D / "check_verdicts.json").read_text())["items"]}

rows = []
for inv, g in v.groupby("inv", sort=False):
    it = user[inv]
    if it["choice"] in ALL:
        labels = [ALL[it["choice"]]] * len(g)
    else:
        labels = [WORD[w] for w in it["note"].split()]
        assert len(labels) == len(g), (inv, it["note"])
    for (_, r), u in zip(g.iterrows(), labels):
        rows.append({"read_id": r.read_id, "inv": inv, "scan_id": r.scan_id, "agent": r.verdict, "user": u,
                     "user_note": it.get("note", "")})
t = pd.DataFrame(rows)
t.to_csv(D / "user_check.tsv", sep="\t", index=False)

print("agent x user, per scan:")
print(pd.crosstab(t.agent, t.user, margins=True).to_string())
print("\nscans the agent read printed/handwritten and the user read otherwise:")
print(t[t.agent.isin(["printed", "handwritten"]) & (t.agent != t.user)].to_string(index=False))

print("\npin rule on the user's labels (opp >= 4 of 5 pins; opp 2-3 is a mixed volume):")
for inv, g in t.groupby("inv", sort=False):
    built = vol.loc[inv, "built_mode"]
    c = Counter(g.user)
    opp = c["handwritten" if built == "print" else "printed"]
    dec = "pin " + ("hand" if built == "print" else "print") if opp >= 4 else ("mixed" if opp >= 2 else "as built")
    if dec != vol.loc[inv, "decision"]:
        print(f"  {inv}: built {built}, user {dict(c)} -> {dec} (agent: {vol.loc[inv, 'decision']})")
