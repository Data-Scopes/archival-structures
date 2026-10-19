"""Step 15d error analysis: containment, empty structure, page side.

Usage: .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15d/ea.py <run dir> [section]
section: containment | empty | side | all (default). Read only; seed 15.
"""
import csv
import random
import sys
from math import atan2, degrees
from pathlib import Path

import pandas as pd

RUNS = [("hand_tar", "NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
        ("hand_local", "NL-HaNA_1.01.02_text_layer_v2",
         ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
        ("print", "NL-HaNA_1.10.94_text_layer_v2", ["455", "456"])]
TOL = 5
KW = dict(sep="\t", quoting=csv.QUOTE_NONE, dtype=str, keep_default_na=False, na_values=[])


def load(run_dir, sub, layer, inv):
    root = Path(run_dir) / sub / layer
    reg = pd.read_csv(root / "regions_meta" / f"{inv}.tsv.gz", **KW)
    lin = pd.read_csv(root / "lines" / f"{inv}.lines.tsv.gz", **KW)
    sca = pd.read_csv(root / "scans" / f"{inv}.scans.tsv.gz", **KW)
    for c in ("x", "y", "w", "h"):
        reg[c] = pd.to_numeric(reg[c])
        lin[c] = pd.to_numeric(lin[c])
    sca["page_w"] = pd.to_numeric(sca["page_w"], errors="coerce")
    return reg, lin, sca


def bl_points(s):
    return [tuple(int(v) for v in p.split(",")) for p in s.split()] if s else []


def classify_line(r):
    """Heuristic class of a line exceeding its region box: overhang size and baseline angle."""
    exc = max(r.x_r - r.x, r.y_r - r.y, (r.x + r.w) - (r.x_r + r.w_r), (r.y + r.h) - (r.y_r + r.h_r))
    pts = bl_points(r.baseline)
    ang = degrees(atan2(pts[-1][1] - pts[0][1], pts[-1][0] - pts[0][0])) if len(pts) >= 2 else 0.0
    if abs(ang) > 2:
        return "rotated_or_sloped_baseline"
    if exc <= 30:
        return "small_overhang_le30px (box padding)"
    if exc <= 100:
        return "overhang_30_100px"
    return "overhang_gt100px (region narrower than line)"


def containment(run_dir):
    rng = random.Random(15)
    print("== containment (tolerance 5 px)")
    print("inv\tlines\tline_outside_region\tpct\tbaseline_outside_line\tlines_with_baseline\tpct")
    examples_l, examples_b = [], []
    for sub, layer, invs in RUNS:
        for inv in invs:
            reg, lin, _ = load(run_dir, sub, layer, inv)
            m = lin.merge(reg[["region_id", "x", "y", "w", "h"]], on="region_id", suffixes=("", "_r"))
            assert len(m) == len(lin)
            out = ((m.x < m.x_r - TOL) | (m.y < m.y_r - TOL) | (m.x + m.w > m.x_r + m.w_r + TOL)
                   | (m.y + m.h > m.y_r + m.h_r + TOL))
            hasb = m.baseline != ""
            bad_b = []
            for i in m.index[hasb]:
                r = m.loc[i]
                pts = bl_points(r.baseline)
                if any(px < r.x - TOL or px > r.x + r.w + TOL or py < r.y - TOL or py > r.y + r.h + TOL
                       for px, py in pts):
                    bad_b.append(i)
            print(f"{inv}\t{len(m)}\t{int(out.sum())}\t{100 * out.mean():.2f}\t{len(bad_b)}\t{int(hasb.sum())}"
                  f"\t{100 * len(bad_b) / max(1, hasb.sum()):.2f}")
            for i in m.index[out]:
                examples_l.append((inv, m.loc[i]))
            for i in bad_b:
                examples_b.append((inv, m.loc[i]))
    from collections import Counter
    print("\nclass counts, lines exceeding region box:", dict(Counter(classify_line(r) for _, r in examples_l)))
    print("class counts per inventory:", dict(Counter((inv, classify_line(r)) for inv, r in examples_l)))
    for title, ex in (("lines exceeding region box", examples_l), ("baselines outside line box", examples_b)):
        print(f"\n-- 10 random of {len(ex)}: {title}")
        for inv, r in rng.sample(ex, min(10, len(ex))):
            pts = bl_points(r.baseline)
            ang = ""
            if len(pts) >= 2:
                ang = f" angle={degrees(atan2(pts[-1][1] - pts[0][1], pts[-1][0] - pts[0][0])):.1f}"
            exc = (r.x_r - r.x, r.y_r - r.y, (r.x + r.w) - (r.x_r + r.w_r), (r.y + r.h) - (r.y_r + r.h_r))
            cls = classify_line(r) if "region" in title else ""
            print(f"{inv} {cls} {r.scan_id} {r.line_id} line=({r.x},{r.y},{r.w},{r.h}) region=({r.x_r},{r.y_r},{r.w_r},{r.h_r}) "
                  f"overhang L/T/R/B={tuple(int(v) for v in exc)}{ang} bl={r.baseline[:60]} text={r.text[:40]!r}")


def empty(run_dir):
    rng = random.Random(15)
    print("== empty structure")
    for sub, layer, invs in RUNS:
        for inv in invs:
            reg, lin, sca = load(run_dir, sub, layer, inv)
            z = reg[reg.n_lines == "0"]
            print(f"{inv}: scans={len(sca)} status={dict(sca.status.value_counts())} regions={len(reg)} "
                  f"zero_line_regions={len(z)} ({100 * len(z) / max(1, len(reg)):.1f}%) "
                  f"tags={dict(z.region_tag.value_counts())} median_wxh={z.w.median() if len(z) else ''}x{z.h.median() if len(z) else ''} "
                  f"regions_with_lines_but_no_text={int(((reg.n_lines != '0') & (reg.n_lines_text == '0')).sum())}")
            if sub == "print":
                print(f"-- 10 random zero-line regions of {inv} (seed 15)")
                pw = sca.set_index("scan_id").page_w
                for _, r in z.sample(min(10, len(z)), random_state=15).iterrows():
                    same = reg[reg.scan_id == r.scan_id]
                    print(f"  {r.scan_id} {r.region_xml_id} tag={r.region_tag} box=({r.x},{r.y},{r.w},{r.h}) "
                          f"page_w={pw[r.scan_id]} ro={r.region_ro_index!r} regions_on_scan={len(same)} "
                          f"zero_on_scan={int((same.n_lines == '0').sum())} side={r.page_side}")


def side(run_dir):
    rng = random.Random(15)
    print("== page side")
    print("inv\tlines\tleft%\tright%\tscans_with_lines\tone_sided_scans\tone_sided_left\tone_sided_right")
    pool = []
    for sub, layer, invs in RUNS:
        for inv in invs:
            reg, lin, sca = load(run_dir, sub, layer, inv)
            sides = lin.groupby("scan_id").page_side.agg(lambda s: set(s))
            one = sides[sides.map(len) == 1]
            l, rr = (one.map(lambda s: "left" in s).sum(), one.map(lambda s: "right" in s).sum())
            print(f"{inv}\t{len(lin)}\t{100 * (lin.page_side == 'left').mean():.1f}\t{100 * (lin.page_side == 'right').mean():.1f}"
                  f"\t{len(sides)}\t{len(one)}\t{l}\t{rr}")
            for sid in one.index:
                pool.append((inv, sid, one[sid], lin[(lin.scan_id == sid) & (lin.text != "")]))
    print(f"\n-- 10 random one-sided scans of {len(pool)}")
    for inv, sid, s, ll in rng.sample(pool, min(10, len(pool))):
        first = ll.head(3).text.str[:50].tolist()
        n = len(ll)
        print(f"{inv} {sid} side={sorted(s)} lines_with_text={n} first={first}")


if __name__ == "__main__":
    run_dir = sys.argv[1]
    which = sys.argv[2] if len(sys.argv) > 2 else "all"
    for name, fn in (("containment", containment), ("empty", empty), ("side", side)):
        if which in (name, "all"):
            fn(run_dir)
            print()
