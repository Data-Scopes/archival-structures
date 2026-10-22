"""Line order in layout regions, per `change` class (phase 09 planning; reused by step 15j).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/order.py \
        --layer data/text_layer_v2_dev/15ir-a [--sub print] [--inv 455,456] [--simulate] [--out F.json]

Per region, over its non-blank lines in `layout_line_idx` order:
- `jump`: adjacent pair whose baseline mean y goes back by more than 50 px;
- `swap`: adjacent pair with |delta baseline y| <= 15 px and box x going left (a same-row
  fragment read right to left).
Counts per class: regions, regions with a jump, swap pairs, and `lines_resorted` (if the column
exists). With `--simulate`, also what the row-tolerant sort (sort by baseline y; a line joins the
current row when its baseline y minus the row's first baseline y is <= 0.2 x the median box height
of the region's lines; rows by x) would give: regions whose order changes, jumps, swaps.
"""
import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import baseline_y, inventories, layer_root, read  # noqa: E402

ROW_TOL_FRAC = 0.2
JUMP_PX = 50
SWAP_PX = 15


def row_sort(ls, frac=ROW_TOL_FRAC):
    if not ls:
        return ls
    tol = frac * statistics.median(l["h"] for l in ls)
    rows, cur = [], []
    for l in sorted(ls, key=lambda l: (l["by"], l["x"], l["y"], l["line_id"])):
        if cur and l["by"] - cur[0]["by"] > tol:
            rows.append(cur)
            cur = []
        cur.append(l)
    rows.append(cur)
    return [l for r in rows for l in sorted(r, key=lambda l: (l["x"], l["by"], l["y"], l["line_id"]))]


def metrics(ls):
    jump = swap = 0
    for a, b in zip(ls, ls[1:]):
        jump += b["by"] < a["by"] - JUMP_PX
        swap += abs(b["by"] - a["by"]) <= SWAP_PX and b["x"] < a["x"]
    return jump, swap


def run(base, sub, invs, simulate):
    root = layer_root(base, sub)
    out = {}
    for inv in invs:
        L = read(root / "lines" / f"{inv}.lines.tsv.gz",
                 ["scan_id", "line_id", "x", "y", "h", "baseline", "text"])
        L = L[L.text.str.strip() != ""]
        info = {(s, i): {"line_id": i, "x": int(x), "y": int(y), "h": int(h), "by": baseline_y(b, y, h)}
                for s, i, x, y, h, b in zip(L.scan_id, L.line_id, L.x, L.y, L.h, L.baseline)}
        LL = read(root / "layout" / "lines" / f"{inv}.tsv.gz",
                  ["scan_id", "line_id", "layout_region_id", "layout_line_idx"])
        R = read(root / "layout" / "regions" / f"{inv}.tsv.gz")
        chg = dict(zip(zip(R.scan_id, R.layout_region_id), R.change))
        res = dict(zip(zip(R.scan_id, R.layout_region_id), R["lines_resorted"])) \
            if "lines_resorted" in R.columns else {}
        by = defaultdict(list)
        for s, i, rid, k in zip(LL.scan_id, LL.line_id, LL.layout_region_id, LL.layout_line_idx):
            if (s, i) in info:
                by[(s, rid)].append((int(k), info[(s, i)]))
        stats = defaultdict(Counter)
        for key, items in by.items():
            ls = [l for _, l in sorted(items, key=lambda t: t[0])]
            st = stats[chg.get(key, "?")]
            j, s = metrics(ls)
            st["regions"] += 1
            st["jump_regions"] += j > 0
            st["swap_pairs"] += s
            st["lines_resorted"] += res.get(key, "0") == "1"
            if simulate:
                rs = row_sort(ls)
                j2, s2 = metrics(rs)
                st["sim_order_changes"] += [l["line_id"] for l in rs] != [l["line_id"] for l in ls]
                st["sim_jump_regions"] += j2 > 0
                st["sim_swap_pairs"] += s2
        out[inv] = {c: dict(v) for c, v in sorted(stats.items())}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--sub", default="print", choices=["hand", "print"])
    ap.add_argument("--inv", default="")
    ap.add_argument("--simulate", action="store_true")
    ap.add_argument("--out", default="")
    a = ap.parse_args(argv)
    invs = a.inv.split(",") if a.inv else inventories(a.sub)
    res = run(a.layer, a.sub, invs, a.simulate)
    for inv, per in res.items():
        for c, st in per.items():
            print(inv, c, st)
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
