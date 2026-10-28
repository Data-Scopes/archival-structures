"""Step 19g error reads on the fresh sides (variant X4 as scored in fresh_lines.tsv). All FP; FN: all if <= 40 else 40 with
random.Random(20261127) over the sorted (read_id, k) list. Each error is shown with 2 lines before/after in REGION order
(BASE layout/segments), the region id, gold label, X4 start method, x, w, line_type.
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/errors_sample.py"""
import csv
import importlib.util
import random
from collections import defaultdict
from pathlib import Path

A = Path("reports/data/phase-13-register-index-volumes")
spec = importlib.util.spec_from_file_location("e19d", A / "step-19d/entry_eval.py")
E = importlib.util.module_from_spec(spec); spec.loader.exec_module(E)


def main():
    rows = list(csv.DictReader(open(A / "step-19g/fresh_lines.tsv"), delimiter="\t"))
    gold = {(r["read_id"], r["line_id"]): r for r in csv.DictReader(open(A / "step-19g/fresh_gold.tsv"), delimiter="\t")}
    meta = {r["read_id"]: r for r in csv.DictReader(open(A / "review/fresh_entry_sides.tsv"), delimiter="\t")}
    errs = []
    for r in rows:
        st = r["X4"] not in ("-", "nounit")
        k = int(gold[(r["read_id"], r["line_id"])]["k"])
        if r["label"] == "C" and st:
            errs.append((int(r["read_id"]), k, r, "FP"))
        elif r["label"] == "S" and not st:
            errs.append((int(r["read_id"]), k, r, "FN-nounit" if r["X4"] == "nounit" else "FN"))
    errs.sort(key=lambda e: (e[0], e[1]))
    fp = [e for e in errs if e[3] == "FP"]
    fn = [e for e in errs if e[3] != "FP"]
    fn_s = fn if len(fn) <= 40 else [fn[i] for i in sorted(random.Random(20261127).sample(range(len(fn)), 40))]
    sample = fp + fn_s
    cache = E.Cache()
    out = [f"# X4 on the fresh sides: {len(fp)} FP, {len(fn)} FN ({sum(1 for e in fn if e[3]=='FN-nounit')} nounit); "
           f"shown {len(fp)} FP + {len(fn_s)} FN", ""]
    for rid, k, r, kind in sorted(sample, key=lambda e: (e[3] != "FP", e[0], e[1])):
        m = meta[r["read_id"]]
        regions_by, segs_by = cache.get(E.CLONE[m["layer"]], m["inv"])
        sid, page = r["scan_id"], r["page"]
        target = None
        for reg in regions_by[(sid, page)]:
            ss = segs_by.get((sid, page, reg["layout_region_id"]), [])
            for i, s in enumerate(ss):
                if s["line_id"] == r["line_id"]:
                    target = (reg, ss, i)
        reg, ss, i = target
        xs = sorted(s["x"] for s in ss if s["line_type"] in ("main_text", "index"))
        out.append(f"== {kind} read {rid} ({r['group']}, inv {r['inv']}, {sid[-9:]} {page}) k={k} region {reg['layout_region_id'][-40:]} "
                   f"n_seg={len(ss)} col={reg['layout_column']} role={reg['layout_role']} x[{xs[0] if xs else '-'}..{xs[-1] if xs else '-'}]")
        for j in range(max(0, i - 2), min(len(ss), i + 3)):
            s = ss[j]
            g = gold.get((r["read_id"], s["line_id"]))
            lab = g["label"] if g else "."
            gk = g["k"] if g else "."
            cur = next((x for x in rows if x["read_id"] == r["read_id"] and x["line_id"] == s["line_id"]), None)
            meth = cur["X4"] if cur else "."
            out.append(f"  {'>>' if j == i else '  '} k{gk:>3} gold={lab} X4={meth:<28} type={s['line_type']:<10} x={s['x']:>5} w={s['w']:>5} | {s['text'][:90]}")
        out.append("")
    (A / "step-19g/error_reads.txt").write_text("\n".join(out))
    print(len(fp), len(fn), len(sample))


if __name__ == "__main__":
    main()
