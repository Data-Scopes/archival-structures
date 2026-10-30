"""Phase-15 planning prototype of folio map v2 (rules G1-G3) against v1, on the 41 front-index volumes.

G1 jumps: after the v1 mode decision and the chain, split the chain where dn > JUMP_K * dpos + JUMP_C between
   consecutive anchors and keep the longest piece (ties: the earlier). A count bound fails on 3712, whose numbers
   continue from the previous volume (348..684 on 533 sides).
G2 ties: a number printed on two or more anchor positions within DUP_WINDOW positions is dropped (all copies).
G3 confidence: `high` needs anchor_dist <= 3 and a consistent bracket |dn - dpos| <= GAP_SLACK; `medium`
   needs anchor_dist <= 10 and |dn - dpos| <= 2 * GAP_SLACK... (see conf_v2); extrapolated stays `low`.
No gold is read except the spent 20c gold (dev-level information only; it is spent for map variants).
Run from the AS root: .venv/bin/python -I reports/data/phase-15-index-links-v2/planning/map_v2_proto.py
"""
import csv
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, ".")
from archival_structures.analysis.folio_map import NUMBER_PATTERN, build_folio_map  # noqa: E402
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.analysis.page_numbering import (  # noqa: E402
    interpolate_folio_to_scan, longest_increasing_chain)
import re  # noqa: E402

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
P14 = Path("reports/data/phase-14-index-links")
OUT = Path(__file__).parent
DUP_WINDOW = 2
GAP_SLACK = 2
JUMP_K = 1.0
JUMP_C = 10
ALL41 = [r["inv"] for r in csv.DictReader(open(P14 / "planning/front_index_volumes.tsv"), delimiter="\t")]


def anchors(v, excl):
    pos = {s: i + 1 for i, s in enumerate(v.scan_ids)}
    out = []
    for k, side in enumerate(v.sides):
        if side in excl or side[0] not in pos:
            continue
        for t in v.numbers.get(side, ()):
            m = re.match(NUMBER_PATTERN, t.strip())
            if m:
                out.append((k + 1, pos[side[0]], side[1], int(m.group(1))))
                break
    return out


def chain_v2(v, excl):
    a = anchors(v, excl)
    n_scans = len(v.scan_ids)
    # mode decision as v1 (all anchors): a bound before it turns 3712 (recto-only pagination) into foliation
    share = sum(1 for x in a if x[2] == "recto") / len(a) if a else float("nan")
    fol = bool(a) and share >= 0.9
    if fol:
        pts = sorted((x[1], x[3]) for x in a if x[2] == "recto")
        n_pos = n_scans
    else:
        pts = sorted((x[0], x[3]) for x in a)
        n_pos = len(v.sides)
    byn = defaultdict(list)
    for p, n in pts:
        byn[n].append(p)
    drop = {n for n, ps in byn.items() if len(ps) > 1 and min(b - a for a, b in zip(sorted(ps), sorted(ps)[1:])) <= DUP_WINDOW}
    pts = [(p, n) for p, n in pts if n not in drop]
    idx = longest_increasing_chain([n for _, n in pts])
    ch = np.array([pts[i] for i in idx], dtype=float).reshape(-1, 2)
    if len(ch) > 1:
        d = np.diff(ch, axis=0)
        cuts = np.nonzero(d[:, 1] > JUMP_K * d[:, 0] + JUMP_C)[0]
        bounds = [0] + [int(c) + 1 for c in cuts] + [len(ch)]
        pieces = [(b - a, -a, a, b) for a, b in zip(bounds, bounds[1:])]
        _, _, a0, b0 = max(pieces)
        ch = ch[a0:b0]
    mode = ("foliation" if fol else "pagination") if len(ch) >= 3 else "none"
    return mode, ch, n_pos, share, len(drop)


def locate(ch, n_pos, number):
    if len(ch) < 3 or number < 1 or number < ch[0, 1] - 6 or number > ch[-1, 1] + 6:
        return None
    hit = np.nonzero(ch[:, 1] == number)[0]
    if len(hit):
        return int(ch[hit[0], 0]), "printed", 0.0, "high"
    s, dist, inside = interpolate_folio_to_scan(number, ch, extrap=True, max_scan=n_pos, edge=6)
    pos = min(max(math.floor(s + 0.5), 1), n_pos)
    if not inside:
        return pos, "extrapolated", dist, "low"
    j = int(np.searchsorted(ch[:, 1], number))
    dn = ch[j, 1] - ch[j - 1, 1]
    dp = ch[j, 0] - ch[j - 1, 0]
    off = abs(dn - dp)
    conf = "high" if dist <= 3 and off <= GAP_SLACK else ("medium" if dist <= 10 and off <= 2 * GAP_SLACK else "low")
    return pos, "interpolated", dist, conf


def loo(ch):
    ok = tot = 0
    for i in range(1, len(ch) - 1):
        s, _, _ = interpolate_folio_to_scan(ch[i, 1], np.delete(ch, i, axis=0))
        tot += 1
        ok += math.floor(s + 0.5) == int(ch[i, 0])
    return ok, tot


rows = []
tot = Counter()
gold = [r for r in csv.DictReader(open(P14 / "step-20c/entry_gold.tsv"), delimiter="\t")
        if r["target_scan_id"] not in ("", "none") and r["folio_evidence"] != "unclear"]
links_v1 = Path("data/index_links_dev/20r-a/NL-HaNA_1.01.02_index_links_v1/links")
gold_hits = Counter()
for inv in ALL41:
    v = read_volume_inputs(L, inv)
    first, last, end = front_index_run(v.flags)
    excl = {s for i, s in enumerate(v.sides) if i <= last or v.flags[i]}
    m1 = build_folio_map(v.scan_ids, v.sides, v.numbers, exclude=excl)
    mode2, ch2, npos2, share2, ndrop = chain_v2(v, excl)
    o1, t1 = loo(m1.chain) if m1.mode != "none" else (0, 0)
    o2, t2 = loo(ch2) if mode2 != "none" else (0, 0)
    tot.update(o1=o1, t1=t1, o2=o2, t2=t2)
    # re-locate the v1 dev-build links' numbers
    from archival_structures.text_layer.io import read_tsv_gz
    ch_conf = Counter()
    for r in read_tsv_gz(links_v1 / f"{inv}.tsv.gz"):
        n = int(r["number"])
        t2_ = locate(ch2, npos2, n) if mode2 != "none" else None
        c1 = r["confidence"] or "none"
        c2 = t2_[3] if t2_ else "none"
        ch_conf[(c1, c2)] += 1
        if n > len(v.scan_ids):
            tot["over_" + c1 + "_v1"] += 1
            tot["over_" + c2 + "_v2"] += 1
    for k, c in ch_conf.items():
        tot["conf " + k[0] + "->" + k[1]] += c
    for g in (x for x in gold if x["inv"] == inv):
        n = int(g["target_locator"])
        gpos = v.scan_ids.index(g["target_scan_id"]) + 1
        t1_ = m1.locate(n)
        t2_ = locate(ch2, npos2, n) if mode2 != "none" else None
        gold_hits["v1"] += bool(t1_) and v.scan_ids.index(t1_.front[0]) + 1 == gpos
        gold_hits["v2"] += bool(t2_) and mode2 == "foliation" and t2_[0] == gpos
        gold_hits["n"] += 1
    rows.append([inv, m1.mode, mode2, len(m1.chain), len(ch2), int(m1.chain[-1, 1]) if len(m1.chain) else "",
                 int(ch2[-1, 1]) if len(ch2) else "", len(v.scan_ids), ndrop, f"{o1}/{t1}", f"{o2}/{t2}"])
with open(OUT / "map_v2_proto.tsv", "w", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow("inv mode_v1 mode_v2 chain_v1 chain_v2 last_v1 last_v2 n_scans dup_numbers_dropped loo_v1 loo_v2".split())
    w.writerows(rows)
for r in rows:
    if r[1] != r[2] or r[5] != r[6]:
        print("changed", r)
print(f"LOO (all modes) v1 {tot['o1']}/{tot['t1']} = {tot['o1']/tot['t1']:.4f}; v2 {tot['o2']}/{tot['t2']} = {tot['o2']/tot['t2']:.4f}")
print("dev-build links: confidence v1->v2", sorted((k, c) for k, c in tot.items() if k.startswith("conf")))
print("links with number > n_scans:", {k: c for k, c in tot.items() if k.startswith("over")})
print("dup numbers dropped, total", sum(r[8] for r in rows), "volumes with any", sum(1 for r in rows if r[8]))
print("spent 20c gold (dev-level info):", dict(gold_hits))
