"""Step 15j error analysis: old (15ir-a) vs new (15j-a) line order of print regions.

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/step-15j/order_diff.py \
        --old data/text_layer_v2_dev/15ir-a --new data/text_layer_v2_dev/15j-a --out OUTDIR
        [--region 455:SCAN:REGION_ID]

Writes into OUTDIR: `counts.json` (regions whose order changed per inventory and class, plus
`join/units` text differences), `sample_sorted.txt` (20 changed regions of the sorted classes,
Random(71)), `sample_unchanged_455.txt` (20, Random(72)), `unchanged_456.txt` (all),
`remaining_swaps.txt` (every remaining same-row swap pair in the sorted classes; swap = adjacent
non-blank lines, |delta baseline y| <= 15 px, box x going left). With --region prints one region.
"""
import argparse
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "planning"))
from common import PRINT_INV, baseline_y, layer_root, read  # noqa: E402

SORTED = ("merged", "merged+split_at_column", "split_at_column")


def load(base, inv):
    root = layer_root(base, "print")
    lines = read(root / "lines" / f"{inv}.lines.tsv.gz",
                 usecols=["scan_id", "line_id", "x", "y", "h", "baseline", "text"])
    lines["by"] = [baseline_y(b, y, h) for b, y, h in zip(lines.baseline, lines.y, lines.h)]
    info = {(r.scan_id, r.line_id): (int(r.x), r.by, r.text) for r in lines.itertuples()}
    lay = read(root / "layout" / "lines" / f"{inv}.tsv.gz",
               usecols=["scan_id", "line_id", "layout_region_id", "layout_line_idx"])
    lay["i"] = lay.layout_line_idx.astype(int)
    regions = defaultdict(list)
    for r in lay.sort_values("i", kind="stable").itertuples():
        regions[(r.scan_id, r.layout_region_id)].append(r.line_id)
    cls = {(r.scan_id, r.layout_region_id): r.change
           for r in read(root / "layout" / "regions" / f"{inv}.tsv.gz",
                         usecols=["scan_id", "layout_region_id", "change"]).itertuples()}
    return regions, cls, info


def fmt(scan, ids, info):
    out = []
    for lid in ids:
        x, by, t = info[(scan, lid)]
        out.append(f"    {by:7.1f} x={x:5d}  {lid[-14:]:>14}  {t[:40]}")
    return "\n".join(out)


def swaps(scan, ids, info):
    nb = [l for l in ids if info[(scan, l)][2].strip()]
    return [(a, b) for a, b in zip(nb, nb[1:])
            if abs(info[(scan, a)][1] - info[(scan, b)][1]) <= 15
            and info[(scan, b)][0] < info[(scan, a)][0]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--region", default="")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    counts, changed_sorted, changed_unch = {}, [], {"455": [], "456": []}
    swap_lines = []
    for inv in PRINT_INV:
        old, cls, oinfo = load(a.old, inv)
        new, ncls, ninfo = load(a.new, inv)
        assert cls == ncls
        c = Counter()
        for key, ids in new.items():
            if old[key] != ids:
                c[cls[key]] += 1
                if cls[key] in SORTED:
                    changed_sorted.append((inv, key))
                elif cls[key] == "unchanged":
                    changed_unch[inv].append(key)
            if cls[key] in SORTED:
                for p, q in swaps(key[0], ids, ninfo):
                    swap_lines.append((inv, key, p, q, ninfo))
        counts[inv] = {"regions_order_changed": dict(c)}
        # join/units text differences (multiset of (scan, text))
        def texts(base):
            u = read(layer_root(base, "print") / "join" / "units" / f"{inv}.tsv.gz",
                     usecols=["scan_id", "text"])
            return Counter(zip(u.scan_id, u.text))
        to, tn = texts(a.old), texts(a.new)
        counts[inv]["join_units_old"] = sum(to.values())
        counts[inv]["join_units_new"] = sum(tn.values())
        counts[inv]["join_units_texts_only_in_old"] = sum((to - tn).values())
        counts[inv]["join_units_texts_only_in_new"] = sum((tn - to).values())
        counts[inv]["_cache"] = None
        counts[inv].pop("_cache")
        globals().setdefault("_L", {})[inv] = (old, new, oinfo, ninfo)
    (out / "counts.json").write_text(json.dumps(counts, indent=1))

    def dump(path, items):
        with open(path, "w") as f:
            for inv, key in items:
                old, new, oinfo, ninfo = _L[inv]
                o, n = old[key], new[key]
                d = [i for i in range(len(n)) if o[i] != n[i]]
                lo, hi = max(0, d[0] - 1), min(len(n), d[-1] + 2)   # window around the changes
                if hi - lo > 14:
                    hi = lo + 14
                f.write(f"=== {inv} {key[0]} {key[1]} n_lines={len(n)} n_moved={len(d)} "
                        f"window={lo}:{hi}\n  OLD\n{fmt(key[0], o[lo:hi], oinfo)}\n"
                        f"  NEW\n{fmt(key[0], n[lo:hi], ninfo)}\n")

    dump(out / "sample_sorted.txt", random.Random(71).sample(
        sorted(changed_sorted), min(20, len(changed_sorted))))
    dump(out / "sample_unchanged_455.txt", [("455", k) for k in random.Random(72).sample(
        sorted(changed_unch["455"]), min(20, len(changed_unch["455"])))])
    dump(out / "unchanged_456.txt", [("456", k) for k in sorted(changed_unch["456"])])
    with open(out / "remaining_swaps.txt", "w") as f:
        for inv, key, p, q, info in swap_lines:
            new = _L[inv][1][key]
            i = new.index(p)
            f.write(f"=== {inv} {key[0]} {key[1]}  ({p[-10:]} -> {q[-10:]})\n"
                    f"{fmt(key[0], new[max(0, i - 1):i + 3], info)}\n")
    if a.region:
        inv, scan, rid = a.region.split(":", 2)
        dump(out / "region.txt", [(inv, (scan, rid))])
    print(json.dumps(counts, indent=1))
    print("remaining swap pairs:", len(swap_lines))


if __name__ == "__main__":
    main()
