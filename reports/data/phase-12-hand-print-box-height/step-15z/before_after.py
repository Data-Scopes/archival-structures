"""Step 15z error analysis: for each pinned volume, the units of its first read scan (lowest read_id) in 15z-a (as built)
next to 15z-b (pinned), full text. Writes before_after.txt.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/before_after.py"""
import csv
import gzip
from pathlib import Path

csv.field_size_limit(10 ** 9)
D = Path("reports/data/phase-12-hand-print-box-height")
P = "data/text_layer_v2_dev/{}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/join/units/{}.tsv.gz"
pins = [l.strip() for l in open(D / "step-15z/pins.txt")] 
reads = [l.rstrip("\n").split("\t") for l in open(D / "planning/reads_15z.tsv")][1:]
out = []
for inv in pins:
    rid, _, scan = sorted((r for r in reads if r[1] == inv), key=lambda r: int(r[0]))[0][:3]
    out.append(f"\n######## {inv}  first read {rid}  {scan}")
    for layer in ("15z-a", "15z-b"):
        rows = [r for r in csv.DictReader(gzip.open(P.format(layer, inv), "rt"), delimiter="\t") if r["scan_id"] == scan]
        out.append(f"--- {layer}: {len(rows)} units ({sum(r['unit_type'] == 'main_text' for r in rows)} main_text)")
        for r in rows:
            out.append(f"[{r['unit_type']}] {r['text']}")
(D / "step-15z/before_after.txt").write_text("\n".join(out) + "\n")
