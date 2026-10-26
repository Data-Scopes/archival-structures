"""Phase-12 review: pre-registered read lists for steps 15wr and 15x (drawn before either step runs).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/draw_reads.py

Reads only the 15w-a scan and segment tables (the scans are the same in later builds) and
`step-15w/modes.tsv`; writes `review/reads_15wr.tsv` and `review/reads_15x.tsv`.

* 15wr (seed 20261027): scans of the print-mode 1.01.02 volumes outside the gedrukte resoluties
  that the split gate is expected to switch off: 10 *nouvelles* volumes x 1 scan, 8 *plakkaten*
  volumes x 1 scan, 9486 x 2 scans; volumes drawn at random, then one scan each among scans
  with >= 10 main_text segments.
* 15x (seed 20261026): 10 of the 31 *gedrukte resoluties* volumes x 2 scans among scans with
  >= 10 main_text segments, excluding every scan already read in 15v and in the phase review.
"""
import csv
import gzip
import random
from collections import Counter
from pathlib import Path

D = Path("reports/data/phase-12-hand-print-box-height")
L = Path("data/text_layer_v2_dev/15w-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4")
READ_BEFORE = {"3771": {"0028", "0113", "0179", "0285", "0297", "0311", "0375", "0425", "0535", "0554",
                        "0394", "0418"},
               "3822": {"0080", "0168"}, "12088": {"0064", "0428", "0630"}}


def main_counts(inv):
    c = Counter()
    with gzip.open(L / "types" / "segments" / f"{inv}.tsv.gz", "rt", encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE):
            if r["line_type"] == "main_text":
                c[r["scan_id"]] += 1
    return c


def eligible(inv):
    c = main_counts(inv)
    skip = READ_BEFORE.get(inv, set())
    return sorted(s for s, n in c.items() if n >= 10 and s.rsplit("_", 1)[1] not in skip)


def main():
    with open(D / "step-15w" / "modes.tsv", encoding="utf-8") as fh:
        modes = [r for r in csv.DictReader(fh, delimiter="\t")
                 if r["archive"] == "1.01.02" and r["mode"] == "print"]
    by_series = {}
    for r in modes:
        by_series.setdefault(r["series"], []).append(r["inv"])
    key = {s.split()[1] if s.startswith("Gedrukte") else s.split()[0]: s for s in by_series if s}
    nouv = sorted(by_series[key["nouvelles"]])
    plak = sorted(by_series[key["plakkaten"]])
    gedr = sorted(by_series[[s for s in by_series if s.startswith("De gedrukte")][0]])

    rnd = random.Random(20261027)
    rows = []
    for series, invs, k in (("nouvelles", nouv, 10), ("plakkaten", plak, 8)):
        for inv in sorted(rnd.sample(invs, k), key=int):
            rows.append((series, inv, rnd.choice(eligible(inv))))
    for scan in sorted(rnd.sample(eligible("9486"), 2)):
        rows.append(("processtukken", "9486", scan))
    with open(D / "review" / "reads_15wr.tsv", "w", encoding="utf-8") as fh:
        fh.write("series\tinv\tscan_id\n")
        fh.writelines(f"{a}\t{b}\t{c}\n" for a, b, c in rows)

    rnd = random.Random(20261026)
    rows = []
    for inv in sorted(rnd.sample(gedr, 10), key=int):
        for scan in sorted(rnd.sample(eligible(inv), 2)):
            rows.append(("gedrukte_resoluties", inv, scan))
    with open(D / "review" / "reads_15x.tsv", "w", encoding="utf-8") as fh:
        fh.write("series\tinv\tscan_id\n")
        fh.writelines(f"{a}\t{b}\t{c}\n" for a, b, c in rows)


if __name__ == "__main__":
    main()
