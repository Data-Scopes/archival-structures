"""Step 15z: per first-read scan of each pinned volume, main_text unit counts in 15z-a and 15z-b: units starting with a
date (number + month name), units with >= 2 dates inside (merged entries), units starting lower-case (cut phrases).
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15z/ba_metrics.py"""
import csv, gzip, re
from pathlib import Path
csv.field_size_limit(10 ** 9)
D = Path("reports/data/phase-12-hand-print-box-height")
P = "data/text_layer_v2_dev/{}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/join/units/{}.tsv.gz"
DATE = re.compile(r"(?<![\w])(\d{1,2})\.? ?(jan|feb|maert|maart|march|april|apr|may|mey|maij|meij|mei|jun|jul|aug|sep|oct|nov|dec)\w*", re.I)
pins = [l.strip() for l in open(D / "step-15z/pins.txt")]
reads = [l.rstrip("\n").split("\t") for l in open(D / "planning/reads_15z.tsv")][1:]
print("inv scan | layer: main starts_date multi(>=2 dates) starts_lower")
for inv in pins:
    scan = sorted((r for r in reads if r[1] == inv), key=lambda r: int(r[0]))[0][2]
    line = f"{inv} {scan[-9:]} |"
    for layer in ("15z-a", "15z-b"):
        rows = [r for r in csv.DictReader(gzip.open(P.format(layer, inv), "rt"), delimiter="\t") if r["scan_id"] == scan and r["unit_type"] == "main_text"]
        t = [r["text"] for r in rows]
        line += f" {layer}: {len(t)} {sum(bool(DATE.match(x)) for x in t)} {sum(len(DATE.findall(x)) >= 2 for x in t)} {sum(x[:1].islower() for x in t)} |"
    print(line)
