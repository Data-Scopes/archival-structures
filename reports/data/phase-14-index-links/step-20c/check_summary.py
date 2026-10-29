"""Summarise the user's UA-050 verdicts on the 20 confirm-page links.  Run from the AS root:
    .venv/bin/python reports/data/phase-14-index-links/step-20c/check_summary.py
Reads check_verdicts.json (exported by check_page.html) and check_items.json; prints counts, Wilson 95% intervals,
and every item that is not Yes/Yes or carries a note."""
import json
from collections import Counter
from math import sqrt

D = "reports/data/phase-14-index-links/step-20c"
V = json.load(open(f"{D}/check_verdicts.json"))["items"]
I = {i["id"]: i for i in json.load(open(f"{D}/check_items.json"))}
split = {"3254": "dev", "3471": "dev", "3394": "dev", "3308": "dev"}  # P7; other volumes are test


def wilson(k, n, z=1.96):
    p = k / n; d = 1 + z * z / n; c = p + z * z / (2 * n); h = z * sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - h) / d, (c + h) / d


assert len(V) == 20 and {v["id"] for v in V} == set(I)
for q in ("q1", "q2"):
    c = Counter(v.get(q) for v in V); lo, hi = wilson(c["Yes"], len(V))
    print(f"{q}: {dict(c)}  Yes {c['Yes']}/{len(V)} = {c['Yes']/len(V):.2f} (Wilson {lo:.2f}-{hi:.2f})")
print("by split:", dict(Counter(split.get(v["inv"], "test") for v in V)))
for v in V:
    if v.get("q1") != "Yes" or v.get("q2") != "Yes" or v.get("note"):
        it = I[v["id"]]
        print(f"- {v['id']} {split.get(v['inv'], 'test')} {v['inv']} loc {v['locator']}: q1 {v.get('q1')} q2 {v.get('q2')} | "
              f"entry {it['text'][:70]!r} | agent {it['agent_content']} | note: {v.get('note', '')}")
