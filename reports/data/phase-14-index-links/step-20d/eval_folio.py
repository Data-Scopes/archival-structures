"""Step 20d: build the 41 folio maps, front-index run check, LOO, bar F.
Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/step-20d/eval_folio.py
Reads data/text_layer_v2_dev/19e-a (v5 layer), planning/front_index_volumes.tsv, step-20c/entry_gold.tsv."""
import csv, math, sys
from collections import defaultdict
from pathlib import Path
import numpy as np
sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import build_folio_map
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs
from archival_structures.analysis.page_numbering import interpolate_folio_to_scan

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
P = Path("reports/data/phase-14-index-links")
OUT = P / "step-20d"
plan = list(csv.DictReader(open(P / "planning/front_index_volumes.tsv"), delimiter="\t"))

maps, vols, loo, run_ok = {}, [], [], 0
for r in plan:
    inv = r["inv"]
    v = read_volume_inputs(L, inv)
    run = front_index_run(v.flags)
    n_after = len(v.flags) - run[2] if run else None
    ok = run is not None and run[0] == int(r["front_first"]) and run[1] == int(r["front_last"]) and n_after == int(r["n_after"])
    run_ok += ok
    if not ok:
        print("RUN MISMATCH", inv, run, r)
    last = run[1]
    excl = {s for i, s in enumerate(v.sides) if i <= last or v.flags[i]}
    m = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl)
    maps[inv] = m
    ch = m.chain
    vols.append([inv, m.mode, m.n_anchors, f"{m.recto_share:.4f}", len(ch),
                 int(ch[0, 1]) if len(ch) else "", int(ch[-1, 1]) if len(ch) else "", run[0], run[1], run[2]])
    for i in range(1, len(ch) - 1):
        red = np.delete(ch, i, axis=0)
        s, _, _ = interpolate_folio_to_scan(ch[i, 1], red)
        loo.append([inv, int(ch[i, 1]), int(ch[i, 0]), math.floor(s + 0.5)])
print("front_index_run reproduces", run_ok, "of", len(plan))

def w(name, header, rows):
    with open(OUT / name, "w") as f:
        f.write("\t".join(header) + "\n")
        for x in rows:
            f.write("\t".join(map(str, x)) + "\n")
w("volumes.tsv", "inv mode n_anchors recto_share chain_len chain_first chain_last front_first front_last end".split(), vols)
w("loo.tsv", "inv number position predicted".split(), loo)
tot = sum(1 for x in loo); ok = sum(1 for x in loo if x[2] == x[3])
print(f"LOO pooled {ok}/{tot} = {ok/tot:.4f}")
per = defaultdict(lambda: [0, 0])
for x in loo:
    per[x[0]][0] += x[2] == x[3]; per[x[0]][1] += 1
lows = sorted((a / b, k, a, b) for k, (a, b) in per.items())[:6]
print("lowest per volume", [(k, f"{s:.4f}", a, b) for s, k, a, b in lows])

rows = []
for g in csv.DictReader(open(P / "step-20c/entry_gold.tsv"), delimiter="\t"):
    if g["target_scan_id"] in ("", "none") or g["folio_evidence"] not in ("printed", "neighbours"):
        continue
    m = maps.get(g["inv"])
    t = m.locate(int(g["target_locator"])) if m else None
    pred = t.front[0] if t else ""
    rows.append([g["read_id"], g["split"], g["inv"], g["target_locator"], g["target_scan_id"], pred,
                 t.method if t else "none", t.confidence if t else "", int(pred == g["target_scan_id"]), g["used_for"]])
w("gold_eval.tsv", "read_id split inv target_locator target_scan_id predicted_scan method confidence hit used_for".split(), rows)
def share(sel, label):
    n = len(sel); h = sum(x[8] for x in sel)
    print(f"{label}: {h}/{n}" + (f" = {h/n:.4f}" if n else ""))
for sp in ("test", "dev"):
    share([x for x in rows if x[1] == sp], sp)
    if sp == "test":
        share([x for x in rows if x[1] == sp and x[9] != "extra"], "test read-only")
    for me in ("printed", "interpolated", "extrapolated", "none"):
        share([x for x in rows if x[1] == sp and x[6] == me], f"  {sp} {me}")
for x in rows:
    if not x[8]:
        print("MISS", x)

# coverage: chain anchors per recto side after the front index (foliation volumes)
cov_n = cov_d = 0
for r in plan:
    m = maps[r["inv"]]
    if m.mode != "foliation":
        continue
    v = read_volume_inputs(L, r["inv"]); end = int(vols[[x[0] for x in vols].index(r["inv"])][9])
    cov_d += sum(1 for s in v.sides[end:] if s[1] == "recto"); cov_n += len(m.chain)
print(f"coverage (foliation volumes): chain anchors {cov_n} / rectos after front index {cov_d} = {cov_n/cov_d:.4f}")
print("modes", {k: sum(1 for x in vols if x[1] == k) for k in ("foliation", "pagination", "none")})
