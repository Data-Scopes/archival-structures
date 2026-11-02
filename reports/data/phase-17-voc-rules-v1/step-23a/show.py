"""Step 23a: print scans A..B (scan numbers) of a volume as in dumps/<inv>_front.txt, but from the whole volume.
    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23a/show.py --inv 2047 --from 3 --to 5
Reads only lines/, layout/lines/ and scans/ of the fresh layer (no layout/pages)."""
import argparse, csv, gzip
from pathlib import Path

LAYER = Path("data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
csv.field_size_limit(10**9)


def tsv(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


ap = argparse.ArgumentParser()
ap.add_argument("--inv", required=True)
ap.add_argument("--from", dest="a", type=int, required=True)
ap.add_argument("--to", dest="b", type=int, required=True)
a = ap.parse_args()
scans = {r["scan_id"]: r for r in tsv(LAYER / "scans" / f"{a.inv}.scans.tsv.gz")}
page = {(r["scan_id"], r["line_id"]): r["page"] for r in tsv(LAYER / "layout" / "lines" / f"{a.inv}.tsv.gz")}
by = {}
for r in tsv(LAYER / "lines" / f"{a.inv}.lines.tsv.gz"):
    by.setdefault(r["scan_id"], []).append(r)
for sid, s in sorted(scans.items(), key=lambda kv: int(kv[1]["scan_num"])):
    n = int(s["scan_num"])
    if not a.a <= n <= a.b:
        continue
    pages = sorted({page.get((sid, r["line_id"]), "single") for r in by.get(sid, [])})
    print(f"== {sid} | #{n} | sides: {','.join(pages) or '-'} | lines: {s['n_lines']}")
    for r in by.get(sid, []):
        t = r["text"].strip()
        if t:
            print(f"[{page.get((sid, r['line_id']), 'single')}] {t}")
