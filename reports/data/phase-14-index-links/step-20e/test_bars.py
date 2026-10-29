"""Step 20e: bar L and bar E on the test gold rows (run ONCE, with the variant chosen by select.py).
Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/step-20e/test_bars.py
Writes test_rows.tsv (per entry), bar_e.tsv (per target row)."""
import sys
from collections import Counter
sys.path.insert(0, "."); sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *
from archival_structures.analysis.folio_map import build_folio_map
from archival_structures.analysis.index_links import (DEFAULT_LOCATOR_VARIANT, SG_LOCATOR_PARAMS, extract_locators,
                                                      front_index_run, read_volume_inputs)

V = DEFAULT_LOCATOR_VARIANT
prm = SG_LOCATOR_PARAMS[V]
print("variant", V)
rows = [g for g in gold() if g["split"] == "test"]
maps, texts = {}, {}
for inv in sorted({g["inv"] for g in rows}):
    v = read_volume_inputs(L, inv)
    run = front_index_run(v.flags)
    excl = {s for i, s in enumerate(v.sides) if i <= run[1] or v.flags[i]}
    maps[inv] = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl)
    texts.update({(inv,) + k: t for k, t in unit_texts(inv).items()})
tot = {"all": [0, 0, 0], "read": [0, 0, 0]}
out, e_rows = [], []
for g in rows:
    m = maps[g["inv"]]
    rng = (int(m.chain[0, 1]), int(m.chain[-1, 1])) if len(m.chain) else None
    t = texts[(g["inv"], g["scan_id"], g["unit_id"])]
    pred = [l.number for l in extract_locators(t, prm, folio_range=rng)]
    gn = gold_numbers(g)
    c = compare(pred, gn)
    for k in ("all",) + (("read",) if g["used_for"] != "extra" else ()):
        for i in range(3):
            tot[k][i] += c[i]
    out.append([g["read_id"], g["inv"], g["used_for"], g["entry_verdict"], gn, pred,
                sorted((Counter(pred) - Counter(gn)).elements()), sorted((Counter(gn) - Counter(pred)).elements()),
                t[-100:]])
    if g["target_scan_id"] not in ("", "none") and g["folio_evidence"] in ("printed", "neighbours"):
        tl = int(g["target_locator"])
        got = tl in pred
        tg = m.locate(tl)
        mp = tg is not None and tg.front[0] == g["target_scan_id"]
        e_rows.append([g["read_id"], g["inv"], g["used_for"], tl, int(got), int(mp), int(got and mp),
                       tg.front[0] if tg else "", g["target_scan_id"], tg.method if tg else "none", pred])
for k, (tp, n, ng) in tot.items():
    p, r, f = prf(tp, n, ng)
    print(f"bar L {k}: TP={tp} pred={n} gold={ng}  P={p:.4f} R={r:.4f} F1={f:.4f}")
for label, sel in (("read+extra", e_rows), ("read only", [x for x in e_rows if x[2] != "extra"])):
    h = sum(x[6] for x in sel)
    print(f"bar E {label}: {h}/{len(sel)} = {h/len(sel):.4f}; locator found {sum(x[4] for x in sel)}, map ok {sum(x[5] for x in sel)}")
with open(OUT / "test_rows.tsv", "w") as f:
    f.write("read_id\tinv\tused_for\tverdict\tgold\tpred\tfp\tfn\ttext_tail\n")
    for r in out:
        f.write("\t".join(map(str, r)).replace("\n", " ") + "\n")
with open(OUT / "bar_e.tsv", "w") as f:
    f.write("read_id\tinv\tused_for\ttarget_locator\tlocator_found\tmap_ok\thit\tpred_scan\tgold_scan\tmethod\tpred_numbers\n")
    for r in e_rows:
        f.write("\t".join(map(str, r)) + "\n")
