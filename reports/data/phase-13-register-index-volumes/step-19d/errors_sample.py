"""Step 19d error reads: list test errors of the chosen variant (E5) and write ``error_reads.txt`` for reading.
Sample: all errors if <= 40, else 40 with random.Random(20261121) over the sorted (read_id, k) list. Each error is shown
with the two lines before and after (gold order k), their x, w, gold label, and the start method of E5 / v4 print.
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/errors_sample.py"""
import csv
import random
from collections import defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
SUB = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
CLONE = {"15y-b": "19c-a", "15z-b": "19c-z"}


def main():
    ev = list(csv.DictReader(open(A / "step-19d/eval_test_lines.tsv"), delimiter="\t"))
    meta = {r["read_id"]: r for r in csv.DictReader(open(A / "planning/reads_entries.tsv"), delimiter="\t")}
    gold = {r["line_id"]: r for r in csv.DictReader(open(A / "step-19a/entry_gold.tsv"), delimiter="\t")}
    gold_rows = list(csv.DictReader(open(A / "step-19a/entry_gold.tsv"), delimiter="\t"))
    by_read = defaultdict(list)
    for r in gold_rows:
        by_read[r["read_id"]].append(r)
    by_line = {(r["read_id"], r["line_id"]): r for r in ev}
    errs = []
    for r in ev:
        st = r["E5"] not in ("-", "nounit")
        if r["label"] == "C" and st:
            errs.append((r, "FP"))
        elif r["label"] == "S" and not st:
            errs.append((r, "FN" if r["E5"] == "-" else "FN-nounit"))
    errs.sort(key=lambda e: (int(e[0]["read_id"]), by_read[e[0]["read_id"]].index(
        next(g for g in by_read[e[0]["read_id"]] if g["line_id"] == e[0]["line_id"]))))
    sample = errs if len(errs) <= 40 else sorted(random.Random(20261121).sample(range(len(errs)), 40))
    sample = [errs[i] for i in sample] if len(errs) > 40 else errs
    lines_cache = {}
    out = [f"# {len(errs)} errors of E5 on the test sides; {len(sample)} shown", ""]
    for r, kind in sample:
        m = meta[r["read_id"]]
        key = (m["layer"], m["inv"])
        if key not in lines_cache:
            rt = DEV / CLONE[m["layer"]] / SUB
            lines_cache[key] = {(x["scan_id"], x["line_id"]): x for x in read_tsv_gz(rt / "lines" / f"{m['inv']}.lines.tsv.gz")}
        lc = lines_cache[key]
        rows = by_read[r["read_id"]]
        i = next(j for j, g in enumerate(rows) if g["line_id"] == r["line_id"])
        out.append(f"== {kind} read {r['read_id']} ({r['group']}, inv {r['inv']}, {r['scan_id'][-9:]} {r['page']}) k={rows[i]['k']}")
        for j in range(max(0, i - 2), min(len(rows), i + 3)):
            g = rows[j]
            ln = lc.get((g["scan_id"], g["line_id"]))
            e = by_line[(r["read_id"], g["line_id"])]
            out.append(f"  {'>>' if j == i else '  '} k{g['k']:>3} gold={g['label']} E5={e['E5']:<22} x={ln['x']:>5} w={ln['w']:>5} | {ln['text'][:90]}")
        out.append("")
    (A / "step-19d/error_reads.txt").write_text("\n".join(out))
    print(len(errs), {k: sum(1 for _, kk in errs if kk == k) for k in ("FP", "FN", "FN-nounit")}, len(sample))


if __name__ == "__main__":
    main()
