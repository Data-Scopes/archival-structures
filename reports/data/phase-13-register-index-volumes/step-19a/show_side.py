"""Print one page side for hand reading. Usage: show_side.py {sides|entries} READ_ID.

Shows no stratum, split, group or prediction. Column 2 if x >= lo + 0.4*(hi-lo).
"""
import sys
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

PLAN = Path("reports/data/phase-13-register-index-volumes/planning")
FILES = {"sides": "reads_sides.tsv", "entries": "reads_entries.tsv"}


def load_row(which, read_id):
    with open(PLAN / FILES[which]) as f:
        head = f.readline().rstrip("\n").split("\t")
        for line in f:
            row = dict(zip(head, line.rstrip("\n").split("\t")))
            if row["read_id"] == str(read_id):
                return row
    raise SystemExit(f"read_id {read_id} not in {which}")


def side_lines(row):
    base = Path("data/text_layer_v2_dev") / row["layer"] / "1.01.02" / "NL-HaNA_1.01.02_text_layer_v4"
    lay = {}
    for r in read_tsv_gz(base / "layout" / "lines" / f"{row['inv']}.tsv.gz"):
        if r["scan_id"] == row["scan_id"]:
            lay[r["line_id"]] = r["page"]
    out = []
    for r in read_tsv_gz(base / "lines" / f"{row['inv']}.lines.tsv.gz"):
        if r["scan_id"] != row["scan_id"]:
            continue
        if lay.get(r["line_id"]) != row["page"] or not r["text"].strip():
            continue
        out.append(dict(line_id=r["line_id"], x=float(r["x"]), y=float(r["y"]),
                        w=float(r["w"]), text=r["text"].strip()))
    return out


def listing(lines):
    pool = [l for l in lines if len(l["text"]) >= 20]
    if len(pool) < 3:
        pool = lines
    xs = sorted(l["x"] for l in pool)
    lo = xs[len(xs) // 20]
    hi = max(l["x"] + l["w"] for l in pool)
    for l in lines:
        l["col"] = 2 if l["x"] >= lo + 0.4 * (hi - lo) else 1
    lines.sort(key=lambda l: (l["col"], l["y"], l["x"]))
    return lines


def main():
    which, rid = sys.argv[1], sys.argv[2]
    row = load_row(which, rid)
    lines = listing(side_lines(row))
    print(f"read_id={rid} inv={row['inv']} scan_id={row['scan_id']} page={row['page']} n_lines={len(lines)}")
    for k, l in enumerate(lines, 1):
        print(f"{k}  {l['col']}  {l['x']:.0f}  {l['y']:.0f}  {l['w']:.0f} | {l['text']}")


if __name__ == "__main__":
    main()
