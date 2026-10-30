"""Step 21b error analysis without gold: (1) chain-end anchors of the 7 changed volumes (v1 vs v2, last 4 anchors);
(2) 10 seeded (random.Random(21)) G3 high -> low/medium links, with the bracketing anchors and the printed
numbers on the scans between them (first number-like text per recto, '.' if none).
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/step-21b/error_analysis.py
"""
import random
import sys

import numpy as np

sys.path.insert(0, ".")
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from eval_map import V1, V1DIR, all41, load  # noqa: E402
from archival_structures.analysis.folio_map import FOLIO_MAP_V2, NUMBER_PATTERN, build_folio_map  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
import re  # noqa: E402

LINKS = V1DIR.parent / "links"
CHANGED = ["3126", "3265", "3290", "3307", "3320", "3471", "3522"]


def chain_str(ch, n=4, tail=True):
    sel = ch[-n:] if tail else ch[:n]
    return " ".join(f"{int(n_)}@{int(p)}" for p, n_ in sel)


def scan_numbers(v, pos_from, pos_to, side_positions=False):
    """First number-like text per recto (per side when side_positions, i.e. pagination), '.' if none."""
    out = []
    for p in range(pos_from, pos_to + 1):
        side = tuple(v.sides[p - 1]) if side_positions else (v.scan_ids[p - 1], "recto")
        txt = "."
        for t in v.numbers.get(side, ()):
            m = re.match(NUMBER_PATTERN, t.strip())
            if m:
                txt = m.group(1)
                break
        out.append(f"{p}:{txt}")
    return " ".join(out)


print("== 1. chain ends of the 7 changed volumes (number@scan position)")
for inv in CHANGED:
    v, excl = load(inv)
    m1 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=V1)
    m2 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=FOLIO_MAP_V2)
    print(f"{inv} ({len(v.scan_ids)} scans, chain_cut={m2.chain_cut}, dup_dropped={m2.n_dup_dropped})")
    print("   v1 last 4:", chain_str(m1.chain))
    print("   v2 last 4:", chain_str(m2.chain))
    k = int(np.nonzero(m1.chain[:, 0] == m2.chain[-1, 0])[0][0])
    dropped = m1.chain[k + 1:]
    print("   dropped by v2 (in v1 chain):", " ".join(f"{int(n)}@{int(p)}" for p, n in dropped) or "(none; tail via ties)")
    print("   scan numbers after v2 end:", scan_numbers(v, int(m2.chain[-1, 0]), len(v.scan_ids)))

print("\n== 2. G3 high -> medium/low links, seed 21")
cands, ctx = [], {}
for inv in all41():
    v, excl = load(inv)
    m2 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl, params=FOLIO_MAP_V2)
    ctx[inv] = (v, m2)
    for r in read_tsv_gz(LINKS / f"{inv}.tsv.gz"):
        if r["confidence"] != "high":
            continue
        t = m2.locate(int(r["number"]))
        if t is not None and t.confidence in ("medium", "low") and t.method == "interpolated":
            cands.append((inv, r["scan_id"], int(r["number"]), t))
print(f"candidates: {len(cands)} links (v1 high -> v2 medium/low, interpolated)")
for inv, sid, n, t in random.Random(21).sample(cands, 10):
    v, m2 = ctx[inv]
    ch = m2.chain
    j = int(np.searchsorted(ch[:, 1], n))
    (p0, n0), (p1, n1) = ch[j - 1], ch[j]
    print(f"{inv} link on {sid[-4:]} number {n}: v2 {t.confidence} (dist {t.anchor_dist}, pos {t.position}); "
          f"bracket {int(n0)}@{int(p0)} .. {int(n1)}@{int(p1)} dn={int(n1-n0)} dpos={int(p1-p0)}")
    print("   printed numbers between:", scan_numbers(v, int(p0), int(p1), m2.mode == "pagination"))
