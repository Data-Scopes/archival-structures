"""Step 23a: draw the 240 folio-crop items: 10 scans per sample volume, uniform over all its scans, without
replacement, random.Random(f"20261101-{inv}").  Run from the AS root:
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/folio_sample.py
Writes folio_sample.tsv (item_id inv scan_id scan_num side_layout).  side_layout = `openings` if verso+recto sides are
>= 0.5 of the volume's sides in layout/lines (distinct (scan_id, page)), else `single`."""
import csv, gzip, os, random
from pathlib import Path

D = Path("reports/data/phase-17-voc-rules-v1/step-23a")
LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
csv.field_size_limit(10**9)


def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


vols = [r for r in csv.DictReader(open("reports/data/phase-17-voc-rules-v1/planning/fresh_volumes.tsv"), delimiter="\t")
        if r["role"] == "sample"]
vols.sort(key=lambda r: int(r["rank"]))
assert len(vols) == 24
rows = []
for v in vols:
    inv = v["inv"]
    scans = sorted(tsv(LAYER / "scans" / f"{inv}.scans.tsv.gz"), key=lambda r: int(r["scan_num"]))
    sides = {(r["scan_id"], r["page"]) for r in tsv(LAYER / "layout" / "lines" / f"{inv}.tsv.gz")}
    two = sum(1 for _, p in sides if p in ("verso", "recto"))
    layout = "openings" if sides and two / len(sides) >= 0.5 else "single"
    pick = random.Random(f"20261101-{inv}").sample(scans, 10)
    for s in sorted(pick, key=lambda r: int(r["scan_num"])):
        rows.append((f"{inv}-{int(s['scan_num']):04d}", inv, s["scan_id"], int(s["scan_num"]), layout))
assert len(rows) == 240 and len({r[0] for r in rows}) == 240
out = D / "folio_sample.tsv"
with open(out, "w") as f:
    f.write("item_id\tinv\tscan_id\tscan_num\tside_layout\n")
    for r in rows:
        f.write("\t".join(map(str, r)) + "\n")
os.chmod(out, os.stat(out).st_mode | 0o020)
from collections import Counter
print(len(rows), "items;", Counter(r[4] for r in rows))
