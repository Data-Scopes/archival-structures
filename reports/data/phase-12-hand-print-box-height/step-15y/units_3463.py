"""Step 15y: volume-wide counts for 3463, 15x-a (print-built) vs 15y-b (hand pin), main_text units of join/units.
Starts-with-date = first token is a number or a month name; starts-lower = first character is lower case
(a cut phrase or a continuation). Dated-entry starts inside a unit = date-like tokens ("12 January", "3 Maij") after the first word.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15y/units_3463.py"""
import csv
import gzip
import re

csv.field_size_limit(10 ** 9)
P = "data/text_layer_v2_dev/{}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/join/units/3463.tsv.gz"
DATE = re.compile(r"(?<![\w])(\d{1,2})\.? ?(jan|feb|maert|maart|april|may|mey|maij|jun|jul|aug|sep|oct|nov|dec)\w*", re.I)
for layer in ("15x-a", "15y-b"):
    rows = [r for r in csv.DictReader(gzip.open(P.format(layer), "rt"), delimiter="\t")]
    main = [r for r in rows if r["unit_type"] == "main_text"]
    lower = sum(r["text"][:1].islower() for r in main)
    dated_first = sum(bool(DATE.match(r["text"])) for r in main)
    entries = [len(DATE.findall(r["text"])) for r in main]
    multi = sum(n >= 2 for n in entries)
    chars = sum(len(r["text"]) for r in main)
    print(f"{layer}: units {len(rows)}, main_text {len(main)}, starting lower-case {lower} ({lower / len(main):.1%}), "
          f"starting with a date {dated_first}, with >= 2 dates inside {multi}, dates total {sum(entries)}, "
          f"median chars {sorted(len(r['text']) for r in main)[len(main)//2]}, text chars {chars}")
