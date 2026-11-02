"""UA-059 result: join the user's verdicts (check_verdicts.json, exported from check_page.html on 2026-11-01) with the
items and write check_result.tsv, one row per checked GM anchor. Run from the AS root:
    .venv/bin/python -I reports/data/phase-16-voc-generalisation/step-22b/summarise_check.py
`correct_scan` is the scan the user says carries the folio (empty when they could not tell). The gold is not edited."""
import collections, csv, json
from pathlib import Path

D = Path("reports/data/phase-16-voc-generalisation/step-22b")
items = {it["id"]: it for it in json.load(open(D / "check_items.json"))}
verdicts = json.load(open(D / "check_verdicts.json"))["items"]
rows = []
for v in verdicts:
    it = items[v["id"]]
    scan = v.get("scan", "")
    correct = {"gold scan": it["gold_scan"], "predicted scan": it["pred_scan"], "another scan": scan}.get(v.get("v", ""), "")
    rows.append(dict(id=v["id"], split=v["id"].rstrip("0123456789"), inv=it["inv"], folio=it["folio"], gold_scan=it["gold_scan"],
                     pred_scan=it["pred_scan"], verdict=v.get("v", ""), correct_scan=correct,
                     user_note=" / ".join(v.get("note", "").split("\n"))))
with open(D / "check_result.tsv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t")
    w.writeheader(); w.writerows(rows)
for split in ("dev", "test"):
    print(split, dict(collections.Counter(r["verdict"] for r in rows if r["split"] == split)))
