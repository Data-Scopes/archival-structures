"""Summarise the user's UA-055 verdicts against the agent's 21a gold.  Run from the AS root:
    .venv/bin/python reports/data/phase-15-index-links-v2/step-21a/check_summary.py > reports/data/phase-15-index-links-v2/step-21a/check_summary.txt
Reads check_verdicts.json and check_items.json; prints only (no gold file is changed)."""
import collections, json

D = "reports/data/phase-15-index-links-v2/step-21a"
v = json.load(open(f"{D}/check_verdicts.json"))
items = {it["id"]: it for it in json.load(open(f"{D}/check_items.json"))}
print("export", v["page"], v["exported"], "items", len(v["items"]))
c = collections.Counter((q, x.get(q, "")) for x in v["items"] for q in ("q1", "q2", "q3"))
for q in ("q1", "q2", "q3"):
    print(q, {k: c[(q, k)] for k in ("Yes", "No", "Unsure", "")  if c[(q, k)]})
print("\nq2 by the agent's content judgement:")
for a in ("leaf", "unclear", "absent"):
    print(" ", a, dict(collections.Counter(x.get("q2") for x in v["items"] if items[x["id"]]["agent_content"] == a)))
print("\nitems with any non-Yes answer or a note:")
for x in v["items"]:
    it = items[x["id"]]
    if any(x.get(q) != "Yes" for q in ("q1", "q2", "q3")) or x.get("note"):
        print(f"  {x['id']} inv {it['inv']} N={it['locator']} q1={x.get('q1')} q2={x.get('q2')} q3={x.get('q3')} "
              f"agent={it['folio_evidence']}/{it['agent_content']} | {x.get('note', '')}")
