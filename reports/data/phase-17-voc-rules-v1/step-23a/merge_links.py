"""Step 23a: merge locator/target reads into gold/link_lines.tsv and gold/link_targets.tsv.  Run from the AS root:
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/merge_links.py [--raw]
Inputs: reads/lines_<inv>.tsv (line_id gold_folios status note), reads/targets_<inv>.tsv (never edited), the link-line
sample reads/link_lines_sample.tsv (copy of the 168-row draw, empty gold columns), and unless --raw the consistency
edits reads/links_consistency.tsv (inv line_id field old new rule reason; field gold_folios or note) and the targets
the 23a session added reads/extra_targets.tsv (same columns as targets_<inv>.tsv plus inv). Targets whose folio is no
longer in the line's gold_folios are dropped. Writes link_lines.tsv (sample order, columns of the sample) and
link_targets.tsv (inv line_id folio target_scan status confidence evidence reader). Refuses to overwrite frozen gold."""
import csv, os, sys
from pathlib import Path
D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
raw = "--raw" in sys.argv
if (D / "gold/agent_gold.done").exists():
    sys.exit("gold is frozen")
READER = {}
for r, invs in {"t01": "2047 1723 1556", "t02": "1631 3260 3793", "t03": "2379 2538 8466", "t04": "8813 1081 7694",
                "t05": "7978 2167 8840", "t06": "8875 1184 2954", "t07": "2282 8029 4325"}.items():
    for i in invs.split():
        READER[i] = r
def rd(p): return list(csv.DictReader(open(p), delimiter="\t"))
sample = rd(D / "reads/link_lines_sample.tsv")
lines = {}
for inv in READER:
    for r in rd(D / f"reads/lines_{inv}.tsv"):
        lines[r["line_id"]] = r
edits = {}
if not raw and (D / "reads/links_consistency.tsv").exists():
    for r in rd(D / "reads/links_consistency.tsv"):
        edits.setdefault((r["line_id"], r["field"]), []).append(r)
out = []
for s in sample:
    r = lines[s["line_id"]]
    row = dict(s, gold_folios=(r["gold_folios"] or "").strip(), status=r["status"], note=(r["note"] or "").strip())
    for f in ("gold_folios", "note"):
        for e in edits.get((s["line_id"], f), []):
            row[f] = e["new"]
    out.append(row)
assert len(out) == 168 and all(r["status"] in ("ok", "unreadable") for r in out)
fol = {r["line_id"]: [x.strip() for x in r["gold_folios"].split(";") if x.strip()] for r in out}
inv_of = {r["line_id"]: r["inv"] for r in out}
order = {r["line_id"]: i for i, r in enumerate(out)}
tg = []
for inv in READER:
    for r in rd(D / f"reads/targets_{inv}.tsv"):
        tg.append(dict(r, inv=inv, reader=READER[inv]))
if not raw and (D / "reads/extra_targets.tsv").exists():
    for r in rd(D / "reads/extra_targets.tsv"):
        tg.append(dict(r, reader="23a"))
canon = {l[5:] if l.startswith("line_") else l: l for l in order}  # a target reader prefixed one id with "line_"
for t in tg:
    t["line_id"] = canon.get(t["line_id"][5:] if t["line_id"].startswith("line_") else t["line_id"], t["line_id"])
tg = [t for t in tg if t["folio"] in fol.get(t["line_id"], [])]
tg.sort(key=lambda t: (order[t["line_id"]], int(t["folio"])))
keys = [(t["line_id"], t["folio"]) for t in tg]
want = [(l, f) for l in order for f in fol[l]]
assert sorted(keys) == sorted(want) and len(set(keys)) == len(keys), (set(want) ^ set(keys))
cols = ["inv", "scan_id", "page", "line_id", "line_idx", "text", "gold_folios", "status", "note"]
with open(D / "gold/link_lines.tsv", "w") as f:
    f.write("\t".join(cols) + "\n")
    for r in out:
        f.write("\t".join(r[c].replace("\t", " ") for c in cols) + "\n")
tc = ["inv", "line_id", "folio", "target_scan", "status", "confidence", "evidence", "reader"]
with open(D / "gold/link_targets.tsv", "w") as f:
    f.write("\t".join(tc) + "\n")
    for t in tg:
        f.write("\t".join((t[c] or "").replace("\t", " ") for c in tc) + "\n")
for p in (D / "gold/link_lines.tsv", D / "gold/link_targets.tsv"):
    os.chmod(p, os.stat(p).st_mode | 0o020)
from collections import Counter
print("lines", len(out), "with folios", sum(1 for r in out if fol[r["line_id"]]), "empty ok", sum(1 for r in out if not fol[r["line_id"]] and r["status"] == "ok"),
      "unreadable", sum(1 for r in out if r["status"] == "unreadable"))
print("targets", len(tg), dict(Counter((t["status"], t["confidence"]) for t in tg)))
