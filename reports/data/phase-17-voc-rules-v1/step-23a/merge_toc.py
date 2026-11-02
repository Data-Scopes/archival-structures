"""Step 23a: merge reads/toc_<inv>.tsv (raw, never edited) plus reads/extra_labels.tsv (gap rows) into gold/toc_labels.tsv, applying reads/harmonise.tsv
(old -> new label/list_type per scan) and the `loc=yes;`/`loc=no;` note prefixes from reads/loc.tsv (inv scan_id loc).
Run from the AS root:  .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/merge_toc.py [--raw-only]
Columns: inv scan_id scan_num label list_type note reader. Sorted by inventory rank, then scan_num.
Checks: coverage 1..min(100, max(60, last toc + 10)) per volume; unclear <= 5%. Prints label / list_type counts."""
import csv, os, sys
from collections import Counter
from pathlib import Path

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
READER = {}
for r, invs in {"r01": "2487 1081", "r02": "2047 1631", "r03": "7694 2222", "r04": "3793 8813", "r05": "3260 8029",
                "r06": "2282 7978", "r07": "8466 4325", "r08": "1723 1390", "r09": "2538 2379", "r10": "2954 1556",
                "r11": "8875 1184", "r12": "2167 8840"}.items():
    for i in invs.split():
        READER[i] = r
raw_only = "--raw-only" in sys.argv
harm, loc = {}, {}
if not raw_only:
    if (D / "reads/harmonise.tsv").exists():
        for r in csv.DictReader(open(D / "reads/harmonise.tsv"), delimiter="\t"):
            harm[(r["inv"], r["scan_id"])] = r
    if (D / "reads/loc.tsv").exists():
        for r in csv.DictReader(open(D / "reads/loc.tsv"), delimiter="\t"):
            loc[(r["inv"], r["scan_id"])] = r["loc"]
rank = {r["inv"]: int(r["rank"]) for r in csv.DictReader(open("reports/data/phase-17-voc-rules-v1/planning/fresh_volumes.tsv"), delimiter="\t")}
rows = []
for inv in READER:
    for r in csv.DictReader(open(D / f"reads/toc_{inv}.tsv"), delimiter="\t"):
        label, lt, note = r["label"], r["list_type"] or "", r["note"] or ""
        h = harm.get((inv, r["scan_id"]))
        if h:
            assert (h["old_label"], h["old_list_type"]) == (label, lt), (inv, r["scan_id"])
            label, lt = h["new_label"], h["new_list_type"]
        if label == "toc":
            l = loc.get((inv, r["scan_id"]))
            assert l in ("yes", "no") or raw_only, (inv, r["scan_id"], "loc missing")
            if l:
                note = f"loc={l}; " + note
        rows.append((inv, r["scan_id"], int(r["scan_num"]), label, lt, note.replace("\t", " "), READER[inv]))
if not raw_only and (D / "reads/extra_labels.tsv").exists():
    for r in csv.DictReader(open(D / "reads/extra_labels.tsv"), delimiter="\t"):
        rows.append((r["inv"], r["scan_id"], int(r["scan_num"]), r["label"], r["list_type"], r["note"], r["reader"]))
rows.sort(key=lambda x: (rank[x[0]], x[2]))
bad = []
for inv in READER:
    nums = [x[2] for x in rows if x[0] == inv]
    toc = [x[2] for x in rows if x[0] == inv and x[3] == "toc"]
    need = min(100, max(60, (max(toc) + 10) if toc else 60))
    if nums != list(range(1, need + 1)):
        bad.append((inv, len(nums), need))
print("coverage problems:", bad)
cnt = Counter(x[3] for x in rows)
print("rows", len(rows), dict(cnt), "unclear share %.3f" % (cnt["unclear"] / len(rows)))
print("list_type (toc rows)", dict(Counter(x[4] for x in rows if x[3] == "toc")))
assert not bad and cnt["unclear"] / len(rows) <= 0.05, 'coverage or unclear check failed'
out = D / ("reads/toc_labels_raw_merge.tsv" if raw_only else "gold/toc_labels.tsv")
if not raw_only and out.exists():
    sys.exit("gold/toc_labels.tsv exists; not overwriting")
with open(out, "w") as f:
    f.write("inv\tscan_id\tscan_num\tlabel\tlist_type\tnote\treader\n")
    for x in rows:
        f.write("\t".join(map(str, x)) + "\n")
os.chmod(out, os.stat(out).st_mode | 0o020)
