"""Print any page side for hand reading (listing logic of step-19a/show_side.py).

    .venv/bin/python -I show_any.py INV SCAN PAGE [LAYER]      SCAN = int or full scan id; PAGE verso|recto|single

Reads the v4 layers 15y-b / 15z-b (page assignment is the same as in the 19c clones).
Shows no prediction. Prints the side's header and lines; column 2 if x >= lo + 0.4*(hi-lo).
"""
import sys
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

PLAN = Path("reports/data/phase-13-register-index-volumes/planning")


def layer_of(inv):
    for layer in ("15y-b", "15z-b"):
        if inv in (PLAN / f"hand_{layer}.txt").read_text().split():
            return layer
    raise SystemExit(f"{inv} is not a hand inventory")


def side_lines(inv, scan_id, page, layer):
    base = Path("data/text_layer_v2_dev") / layer / "1.01.02" / "NL-HaNA_1.01.02_text_layer_v4"
    lay = {r["line_id"]: r["page"] for r in read_tsv_gz(base / "layout" / "lines" / f"{inv}.tsv.gz")
           if r["scan_id"] == scan_id}
    out = []
    for r in read_tsv_gz(base / "lines" / f"{inv}.lines.tsv.gz"):
        if r["scan_id"] != scan_id or lay.get(r["line_id"]) != page or not r["text"].strip():
            continue
        out.append(dict(line_id=r["line_id"], x=float(r["x"]), y=float(r["y"]), w=float(r["w"]),
                        text=r["text"].strip()))
    return out


def listing(lines):
    pool = [l for l in lines if len(l["text"]) >= 20]
    if len(pool) < 3:
        pool = lines
    if not pool:
        return lines
    xs = sorted(l["x"] for l in pool)
    lo = xs[len(xs) // 20]
    hi = max(l["x"] + l["w"] for l in pool)
    for l in lines:
        l["col"] = 2 if l["x"] >= lo + 0.4 * (hi - lo) else 1
    lines.sort(key=lambda l: (l["col"], l["y"], l["x"]))
    return lines


def show(inv, scan, page, layer=None, maxlines=None):
    scan_id = scan if "_" in str(scan) else f"NL-HaNA_1.01.02_{inv}_{int(scan):04d}"
    lines = listing(side_lines(inv, scan_id, page, layer or layer_of(inv)))
    print(f"inv={inv} scan_id={scan_id} page={page} n_lines={len(lines)}")
    for k, l in enumerate(lines[:maxlines], 1):
        print(f"{k}  {l['col']}  {l['x']:.0f}  {l['y']:.0f}  {l['w']:.0f} | {l['text']}")
    if maxlines and len(lines) > maxlines:
        print(f"... ({len(lines) - maxlines} more lines)")


if __name__ == "__main__":
    a = sys.argv[1:]
    show(a[0], a[1], a[2], a[3] if len(a) > 3 else None)
