"""Phase 17 planning prototype (Opus 5.5, 2026-11-01): folio-map options on the GM anchors; base reproduces 22b (647/929, 591/904) (F17-P.3).
Reads phase-16 data only (all dev in phase 17). Reference code for the step briefs, not a deliverable.
Run from the AS root: .venv/bin/python -I reports/data/phase-17-voc-rules-v1/planning/map_proto.py <scratch>/gm_inputs.pkl
"""
import sys, re, pickle, csv, dataclasses, itertools
sys.path.insert(0, ".")
import numpy as np
from archival_structures.analysis.folio_map import FolioMap, FolioMapParams, NUMBER_PATTERN
from archival_structures.analysis.page_numbering import longest_increasing_chain, interpolate_folio_to_scan
VOLS = pickle.load(open(sys.argv[1], "rb"))
rows = list(csv.DictReader(open("reports/data/phase-16-voc-generalisation/planning/gm_anchors.tsv"), delimiter="\t"))
PAT = re.compile(NUMBER_PATTERN)

def build(scan_ids, sides, numbers, excl, o):
    p = FolioMapParams(extrapolate=o.get("extrap", 6))
    scan_pos = {s: i + 1 for i, s in enumerate(scan_ids)}
    cands = []  # (sidepos, scanpos, page, [numbers])
    for i, side in enumerate(sides):
        if side in excl or side[0] not in scan_pos:
            continue
        ns = []
        for t in numbers.get(side, ()):
            m = PAT.match(t.strip())
            if m:
                ns.append(int(m.group(1)))
        if ns:
            cands.append((i + 1, scan_pos[side[0]], side[1], ns))
    share = sum(c[2] == "recto" for c in cands) / len(cands) if cands else float("nan")
    fol = bool(cands) and share >= 0.9
    def pts_of(choice):
        return sorted(((c[1] if fol else c[0]), n) for c, n in zip(cands, choice) if (not fol or c[2] == "recto"))
    choice = [c[3][0] for c in cands]
    def chain_of(pts):
        if o.get("ties"):
            out = []
            for k, (pos, n) in enumerate(pts):
                same = [q for q in pts if q[1] == n and abs(q[0] - pos) <= 3]
                keep = min(same) if o["ties"] == "first" else max(same)
                if (pos, n) == keep:
                    out.append((pos, n))
            pts = out
        idx = longest_increasing_chain([n for _, n in pts])
        ch = np.array([pts[i] for i in idx], float).reshape(-1, 2)
        if o.get("clean") and len(ch) >= 5:
            for _ in range(3):
                keep = np.ones(len(ch), bool)
                for i in range(1, len(ch) - 1):
                    a, b, c = ch[i - 1], ch[i], ch[i + 1]
                    exp = a[0] + (b[1] - a[1]) * (c[0] - a[0]) / (c[1] - a[1])
                    step = (c[0] - a[0]) / (c[1] - a[1])
                    if abs(b[0] - exp) > max(o["clean"], step * 1.0):
                        keep[i] = False
                if keep.all():
                    break
                ch = ch[keep]
        return ch, pts
    ch, pts = chain_of(pts_of(choice))
    if o.get("pick") and len(ch) >= 3:
        new = []
        for c, n0 in zip(cands, choice):
            if len(c[3]) == 1:
                new.append(n0); continue
            pos = c[1] if fol else c[0]
            est = np.interp(pos, ch[:, 0], ch[:, 1])
            new.append(min(c[3], key=lambda n: abs(n - est)))
        ch, pts = chain_of(pts_of(new))
    mode = ("foliation" if fol else "pagination") if len(ch) >= 3 else "none"
    if mode == "none":
        ch = np.empty((0, 2))
    fm = FolioMap(mode, ch, len(cands), share, tuple(scan_ids), tuple(tuple(s) for s in sides), p)
    pieces = []
    if o.get("pieces") and len(ch):
        used = {tuple(x) for x in ch}
        left = [q for q in pts if tuple(map(float, q)) not in used and not (ch[0, 0] <= q[0] <= ch[-1, 0])]
        for _ in range(3):
            if len(left) < 10:
                break
            idx = longest_increasing_chain([n for _, n in left])
            if len(idx) < 10:
                break
            sub = np.array([left[i] for i in idx], float)
            slope = np.median(np.diff(sub[:, 0]) / np.diff(sub[:, 1]))
            if 0.5 <= slope <= 3:
                pieces.append(FolioMap(mode, sub, len(sub), share, tuple(scan_ids), tuple(tuple(s) for s in sides), p))
            left = [q for k, q in enumerate(left) if k not in set(idx)]
    return fm, pieces

def locate(fm, pieces, n):
    t = fm.locate(n)
    if t is None or t.method == "extrapolated":
        for pc in pieces:
            u = pc.locate(n)
            if u is not None and u.method != "extrapolated":
                return u
    return t

def evaluate(o):
    res = {"dev": [0, 0], "test": [0, 0]}
    for inv in sorted({r["inv"] for r in rows}):
        if inv not in VOLS:
            continue
        scan_ids, sides, numbers, excl = VOLS[inv]
        fm, pcs = build(scan_ids, sides, numbers, excl, o)
        pos = {s: i for i, s in enumerate(scan_ids)}
        for r in rows:
            if r["inv"] != inv:
                continue
            gold = f"NL-HaNA_1.04.02_{inv}_{int(r['scan']):04d}"
            if gold not in pos:
                continue
            t = locate(fm, pcs, int(r["folio"]))
            res[r["split"]][1] += 1
            if t is not None:
                p = pos[t.front[0]]
                res[r["split"]][0] += gold in scan_ids[p:p + 2]
    return {k: (a, b, round(a / b, 4)) for k, (a, b) in res.items()}

OPTS = {"base": {}, "pick": {"pick": 1}, "ties_first": {"ties": "first"}, "ties_last": {"ties": "last"},
        "clean3": {"clean": 3}, "extrap30": {"extrap": 30}, "pieces": {"pieces": 1},
        "all_first": {"pick": 1, "ties": "first", "clean": 3, "extrap": 30, "pieces": 1},
        "all_last": {"pick": 1, "ties": "last", "clean": 3, "extrap": 30, "pieces": 1}}
for k, o in OPTS.items():
    print(k, evaluate(o), flush=True)
