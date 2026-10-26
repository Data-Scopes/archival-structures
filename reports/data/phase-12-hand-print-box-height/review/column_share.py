"""Phase-12 review: is a print page side one column or two? (design measurement for step 15wr)

For every `print`-mode volume of a v4 layer, rebuild the pre-split lines (a split pair becomes
its original box again: x = min x, right = max right; text = halves joined by a space), then per
page side (`page_side` column, as `derive_split_params` uses it) over lines with >= 20 characters:

* left, right = 5th percentile of x, 95th percentile of x + w (as `derive_split_params`);
* `mid_start` = share of lines starting at x >= left + 0.4 * (right - left): a two-column side
  has its right column's lines there, a single-column side almost none;
* `col_width` = share of lines with width in 0.35-0.6 of the span (the lines
  `derive_split_params` takes as column lines);
* `n_split` = split pairs in the layer (from `split_from`).

Usage: .venv/bin/python -I review/column_share.py LAYER_1_01_02 LAYER_1_10_94 MODES_TSV OUT_TSV
Reads only; writes OUT_TSV.
"""
import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np


def side_stats(rows):
    rows = [r for r in rows if len(r["text"]) >= 20]
    if len(rows) < 50:
        return None
    xs = np.array([r["x"] for r in rows], dtype=float)
    rights = np.array([r["x"] + r["w"] for r in rows], dtype=float)
    left, right = np.percentile(xs, 5), np.percentile(rights, 95)
    span = right - left
    ws = rights - xs
    return {"n": len(rows), "span": int(span),
            "mid_start": float(np.mean(xs >= left + 0.4 * span)),
            "col_width": float(np.mean((ws >= 0.35 * span) & (ws <= 0.6 * span)))}


def volume(path):
    plain, halves = [], defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE):
            if not r["text"].strip() or r["x"] == "":
                continue
            row = {"side": r["page_side"], "x": int(r["x"]), "w": int(r["w"]), "text": r["text"]}
            if r["split_from"]:
                halves[(r["scan_id"], r["split_from"])].append(row)
            else:
                plain.append(row)
    for parts in halves.values():
        x0 = min(p["x"] for p in parts)
        x1 = max(p["x"] + p["w"] for p in parts)
        plain.append({"side": parts[0]["side"], "x": x0, "w": x1 - x0,
                      "text": " ".join(p["text"] for p in parts)})
    by_side = defaultdict(list)
    for r in plain:
        by_side[r["side"]].append(r)
    return {s: side_stats(rr) for s, rr in by_side.items()}, len(halves)


def main():
    l1, l2, modes_tsv, out = sys.argv[1:5]
    with open(modes_tsv, encoding="utf-8") as fh:
        modes = [r for r in csv.DictReader(fh, delimiter="\t") if r["mode"] == "print"]
    rows = []
    for m in modes:
        layer = l1 if m["archive"] == "1.01.02" else l2
        st, n_split = volume(Path(layer) / "lines" / f"{m['inv']}.lines.tsv.gz")
        for side in ("left", "right", "single", ""):
            s = st.get(side)
            if s:
                rows.append([m["inv"], (m["series"] or "")[:30], side, s["n"], s["span"],
                             round(s["mid_start"], 3), round(s["col_width"], 3), n_split])
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("inv\tseries\tside\tn_lines\tspan\tmid_start\tcol_width\tn_split\n")
        for r in rows:
            fh.write("\t".join(str(v) for v in r) + "\n")


if __name__ == "__main__":
    main()
