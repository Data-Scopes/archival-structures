"""Step 15g2 error analysis: 30 formerly cut lines (non-month prefix in 15g-b), seed 22.
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g2/ea.py OUT.txt
Strata: 10 from 3349-3354, 10 from 3771-3823, 10 from 11182/11281/455/456. Also the two 15g right cuts."""

import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "step-15g"))
from common import PARENT, layer_root  # noqa: E402

from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

STRATA = [["3349", "3350", "3351", "3352", "3353", "3354"],
          ["3771", "3772", "3773", "3821", "3822", "3823"], ["11182", "11281", "455", "456"]]


def load(layer, inv):
    d = defaultdict(list)
    for s in read_tsv_gz(layer_root(PARENT / layer, inv) / "types" / "segments" / f"{inv}.tsv.gz"):
        d[(s["scan_id"], s["line_id"])].append(s)
    return d


def main():
    out = open(sys.argv[1], "w", encoding="utf-8")
    rng = random.Random(22)
    for si, invs in enumerate(STRATA):
        cands = []
        for inv in invs:
            gb, gc = load("15g-b", inv), load("15g-c", inv)
            lines = {(r["scan_id"], r["line_id"]): r for r in read_tsv_gz(layer_root(PARENT / "15g-c", inv) / "lines" / f"{inv}.lines.tsv.gz")}
            for k, v in gb.items():
                if any(s["type_method"] == "rule:margin_prefix" and s["line_type"] != "date_header" for s in v):
                    cands.append((inv, k, lines[k], v, gc[k]))
        pick = rng.sample(cands, min(10, len(cands)))
        out.write(f"=== stratum {si} ({len(cands)} candidates)\n")
        for inv, k, ln, v, c in pick:
            t = ln["text"]
            fmt = lambda segs: " | ".join(f"[{s['line_type']}/{s['type_method'].split(':')[1]}: {t[int(s['char_start']):int(s['char_end'])]}]" for s in segs)  # noqa: E731
            out.write(f"{inv} {k[0]} {k[1]} x={ln['x']} w={ln['w']} y={ln['y']}\n  b: {fmt(v)}\n  c: {fmt(c)}\n")
    for inv, scan, needle in (("3353", "3353_0318", "Praside"), ("3349", "3349_0380", "Herstal")):
        gb, gc = load("15g-b", inv), load("15g-c", inv)
        for r in read_tsv_gz(layer_root(PARENT / "15g-c", inv) / "lines" / f"{inv}.lines.tsv.gz"):
            if r["scan_id"].endswith(scan) and any(x["type_method"] == "rule:margin_prefix" for x in gb.get((r["scan_id"], r["line_id"]), [])):
                k = (r["scan_id"], r["line_id"]); t = r["text"]
                fmt = lambda segs: " | ".join(f"[{s['line_type']}/{s['type_method']}: {t[int(s['char_start']):int(s['char_end'])]}]" for s in segs)  # noqa: E731
                out.write(f"=== 15g right cut {scan} x={r['x']} w={r['w']}\n  b: {fmt(gb[k])}\n  c: {fmt(gc[k])}\n")
    out.close()


main()
