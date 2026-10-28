"""Final phase-13 review (Opus, 2026-10-28): error analysis of step 19e's v5 dev build. Read-only on every layer.

    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/review_19e.py OUT damage
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/review_19e.py OUT gapfill [N]
    .venv/bin/python -I reports/data/phase-13-register-index-volumes/review/review_19e.py OUT side SCAN_ID PAGE

damage   units on detector-flagged sides that are known not to be index pages, v4 against v5. Evidence of "not index":
         (gt) the inventory GT labels the side resolution_page (42 GT volumes, planning/side_features.tsv, P10 mapping);
         (silver) step-19e/silver_sides.tsv (>= 5 republic_segmentation_v1 resolution lines); (read) a `not index` /
         `unreadable` verdict of the 19a reads (step-19c/d2_sides.tsv) or of 19g's fresh reads (108, 109); (review) every
         flagged side of the volumes in REVIEW_FP, read by this review (first lines of each side; none is an index page;
         11182_0529 recto, a table of years and folios, is left out). Per side:
         v4 main_text units, v5 main_text units, lines in them, v4 main units that v5 splits. -> OUT/damage.tsv, damage.json
gapfill  N (default 20) gap-filled sides (`rule` 0, `index_side` 1) in hand volumes outside the 42 GT volumes and the
         volumes of both entry gold sets (19a, 19g), never a side 19a read; random.Random(20261130) over sorted
         (inv, scan_id, page); prints each side's first 14 lines in layout order -> OUT/gapfill.txt
side     print one side's lines in layout order with v4/v5 unit boundaries -> stdout
"""
import csv
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

A = Path("reports/data/phase-13-register-index-volumes")
DEV = Path("data/text_layer_v2_dev")
V5 = DEV / "19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5"
SUB4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"
HAND_Z = set(Path(A / "planning/hand_15z-b.txt").read_text().split())
HAND = sorted(set(Path(A / "planning/hand_15y-b.txt").read_text().split()) | HAND_Z, key=int)


def v4_root(inv):
    return DEV / ("15z-b" if inv in HAND_Z else "15y-b") / SUB4


def tsv(path):
    return list(csv.DictReader(open(path), delimiter="\t"))


def pages(inv):
    return {(r["scan_id"], r["page"]): r for r in read_tsv_gz(V5 / "layout/pages" / f"{inv}.tsv.gz")}


def unit_lines(root, inv, keep_sides):
    """{(scan, page): [(unit_id, unit_type, unit_method, [line_ids])]} for the sides in keep_sides, in unit order."""
    units = [u for u in read_tsv_gz(root / "units/units" / f"{inv}.tsv.gz") if (u["scan_id"], u["page"]) in keep_sides]
    want = {(u["scan_id"], u["unit_id"]) for u in units}
    segs = defaultdict(list)
    for s in read_tsv_gz(root / "units/segments" / f"{inv}.tsv.gz"):
        if (s["scan_id"], s["unit_id"]) in want:
            segs[(s["scan_id"], s["unit_id"])].append(s)
    out = defaultdict(list)
    for u in units:
        ss = sorted(segs[(u["scan_id"], u["unit_id"])], key=lambda r: int(r["seg_order"]))
        out[(u["scan_id"], u["page"])].append((u["unit_id"], u["unit_type"], u["unit_method"], [s["line_id"] for s in ss]))
    return out


# Volumes outside the index volumes whose flagged sides this review read (`side`): letters, resolutions, accounts, rosters.
REVIEW_FP = ("359", "695", "882", "4170", "8287", "8706", "11182", "11220", "11281", "11930", "3221")
REVIEW_KEEP = {("NL-HaNA_1.01.02_11182_0529", "recto")}


def negatives():
    """{(inv, scan, page): set of evidence kinds} for sides known not to be index pages."""
    neg = defaultdict(set)
    gt_invs = {r["inv"] for r in tsv(A / "planning/gt_split.tsv")}
    for r in tsv(A / "planning/side_features.tsv"):
        if r["inv"] in gt_invs and r["gt_label"] == "resolution_page":
            neg[(r["inv"], r["scan_id"], r["page"])].add("gt")
    for r in tsv(A / "step-19e/silver_sides.tsv"):
        neg[(r["inv"], r["scan_id"], r["page"])].add("silver")
    for r in tsv(A / "step-19c/d2_sides.tsv"):
        if r["verdict"] in ("not index", "unreadable input"):
            neg[(r["inv"], r["scan_id"], r["page"])].add("read19a")
    for r in tsv(A / "review/fresh_entry_sides.tsv"):
        if r["read_id"] in ("108", "109"):
            neg[(r["inv"], r["scan_id"], r["page"])].add("read19g")
    for inv in REVIEW_FP:
        for k, r in pages(inv).items():
            if r["index_side"] == "1" and k not in REVIEW_KEEP:
                neg[(inv, *k)].add("review")
    return neg


def cmd_damage(out):
    neg = negatives()
    by_inv = defaultdict(set)
    for inv, scan, page in neg:
        by_inv[inv].add((scan, page))
    rows = []
    for inv in HAND:
        if inv not in by_inv:
            continue
        pg = pages(inv)
        flagged = {k for k in by_inv[inv] if pg.get(k, {}).get("index_side") == "1"}
        if not flagged:
            continue
        u4 = unit_lines(v4_root(inv), inv, flagged)
        u5 = unit_lines(V5, inv, flagged)
        for side in sorted(flagged):
            m4 = [u for u in u4[side] if u[1] == "main_text"]
            m5 = [u for u in u5[side] if u[1] == "main_text"]
            line_unit5 = {lid: u[0] for u in m5 for lid in u[3]}
            split = sum(1 for u in m4 if len({line_unit5.get(l) for l in u[3]}) > 1)
            n_lines5 = sum(len(u[3]) for u in m5)
            rows.append({"inv": inv, "scan_id": side[0], "page": side[1], "evidence": "+".join(sorted(neg[(inv, *side)])),
                         "rule": pg[side]["rule"], "n_lines_side": pg[side]["n_lines"], "v4_main": len(m4),
                         "v5_main": len(m5), "v4_main_split": split, "v5_main_lines": n_lines5,
                         "v4_index_units": sum(1 for u in u4[side] if u[1] == "index")})
    with open(out / "damage.tsv", "w") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    tot = Counter()
    by_ev = defaultdict(Counter)
    for r in rows:
        for k in ("v4_main", "v5_main", "v4_main_split", "v5_main_lines", "v4_index_units"):
            tot[k] += r[k]
            by_ev[r["evidence"]][k] += r[k]
        tot["sides"] += 1
        tot["by_rule" if r["rule"] == "1" else "by_gapfill"] += 1
        by_ev[r["evidence"]]["sides"] += 1
    per_inv = Counter(r["inv"] for r in rows)
    n_neg = Counter("+".join(sorted(v)) for v in neg.values())
    res = {"negatives_known": dict(n_neg), "flagged_negatives": dict(tot), "by_evidence": {k: dict(v) for k, v in by_ev.items()},
           "per_inv_sides": dict(per_inv)}
    (out / "damage.json").write_text(json.dumps(res, indent=1))
    print(json.dumps(res, indent=1))


def side_text(inv, keep):
    txt = {}
    for r in read_tsv_gz(V5 / "lines" / f"{inv}.lines.tsv.gz"):
        if r["scan_id"] in {k[0] for k in keep}:
            txt[(r["scan_id"], r["line_id"])] = r["text"]
    order = defaultdict(list)
    for r in read_tsv_gz(V5 / "layout/lines" / f"{inv}.tsv.gz"):
        if (r["scan_id"], r["page"]) in keep:
            order[(r["scan_id"], r["page"])].append(r)
    return txt, order


def show_side(f, inv, side, max_lines=18):
    pg = pages(inv)[side]
    txt, order = side_text(inv, {side})
    u5 = unit_lines(V5, inv, {side})[side]
    start5 = {u[3][0]: u[1] for u in u5 if u[3]}
    f.write(f"--- {inv} {side[0]} {side[1]} rule={pg['rule']} col_share={pg['col_share']} end_num={pg['end_num']} "
            f"date_start={pg['date_start']} n_lines={pg['n_lines']}\n")
    for i, r in enumerate(order[side]):
        if i >= max_lines:
            f.write(f"    ... {len(order[side]) - max_lines} more lines\n")
            break
        mark = ">" if r["line_id"] in start5 else " "
        f.write(f"  {mark}{r['layout_column']}{r['layout_role'][0]} {txt.get((side[0], r['line_id']), '')[:95]}\n")


def cmd_gapfill(out, n=20):
    excl = {r["inv"] for r in tsv(A / "planning/gt_split.tsv")}
    for name in ("planning/reads_entries.tsv", "review/fresh_entry_sides.tsv"):
        excl |= {r["inv"] for r in tsv(A / name)}
    read = {(r["scan_id"], r["page"]) for r in tsv(A / "planning/reads_sides.tsv")}
    pool = []
    for inv in HAND:
        if inv in excl:
            continue
        for k, r in pages(inv).items():
            if r["index_side"] == "1" and r["rule"] == "0" and k not in read:
                pool.append((inv, *k))
    pool.sort(key=lambda t: (int(t[0]), t[1], t[2]))
    pick = sorted(random.Random(20261130).sample(pool, n), key=lambda t: (int(t[0]), t[1], t[2]))
    with open(out / "gapfill.txt", "w") as f:
        f.write(f"pool {len(pool)} gap-filled sides in {len({p[0] for p in pool})} volumes outside GT and gold; sample {n}\n")
        for i, (inv, scan, page) in enumerate(pick, 1):
            f.write(f"\n##### {i}\n")
            show_side(f, inv, (scan, page), max_lines=14)
    print((out / "gapfill.txt").read_text())


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    cmd = sys.argv[2]
    if cmd == "damage":
        cmd_damage(out)
    elif cmd == "gapfill":
        cmd_gapfill(out, int(sys.argv[3]) if len(sys.argv) > 3 else 20)
    elif cmd == "side":
        scan, page = sys.argv[3], sys.argv[4]
        inv = scan.split("_")[-2]
        show_side(sys.stdout, inv, (scan, page), max_lines=200)


if __name__ == "__main__":
    main()
