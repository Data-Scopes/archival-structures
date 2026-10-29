"""Phase-14 planning: front-index volumes of the v5 dev build and the entry read list for step 20c.

Run from the AS root: .venv/bin/python -I reports/data/phase-14-index-links/planning/draw_reads.py

A volume has a *front index* if, in `layout/pages` order (= scans-table order), the flagged sides
(`index_side == "1"`) before the first run of >= 20 consecutive unflagged sides number >= 10, the first
of them is within the first 10 sides, and >= 300 sides follow the run; volumes with > 60% flagged
sides are index volumes and are left out. Index units = `units/units` rows with `unit_type` `main_text` on the
front-index sides (any `unit_method`: in 3274/3290 most entries are one-region `hand:region` units); text from
`join/units`.

Writes, next to this file:
- `front_index_volumes.tsv`: inv, n_sides, n_front_flagged, front_first, front_last, n_after, n_index_units
- `reads_entries.tsv`: read_id, split, role, inv, scan_id, page, unit_id (no text: the reader sees it
  through the step's tool)
Draw: seed 20261129; 12 volumes (4 dev, 8 test) from the front-index volumes sorted by inv; per volume 5
`read` entries + 1 `reserve`, uniformly among its index units with >= 3 stripped chars of text.
"""

import random
import sys
from pathlib import Path

sys.path.insert(0, ".")
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

LAYER = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
OUT = Path(__file__).parent
SEED = 20261129


def front_index(pages):
    flags = [p["index_side"] == "1" for p in pages]
    n = len(flags)
    if n == 0 or sum(flags) / n > 0.6:
        return None
    run, end = 0, None
    for i, f in enumerate(flags):
        run = 0 if f else run + 1
        if run >= 20:
            end = i - 19
            break
    if end is None:
        return None
    idx = [i for i in range(end) if flags[i]]
    if len(idx) < 10 or idx[0] >= 10 or n - end < 300:
        return None
    return idx[0], idx[-1], end


def main():
    vols = []
    for f in sorted((LAYER / "layout" / "pages").glob("*.tsv.gz")):
        inv = f.name.split(".")[0]
        pages = list(read_tsv_gz(f))
        fi = front_index(pages)
        if fi is None:
            continue
        first, last, end = fi
        front = {(p["scan_id"], p["page"]) for p in pages[: last + 1] if p["index_side"] == "1"}
        text = {(r["scan_id"], r["unit_id"]): r["text"] for r in read_tsv_gz(LAYER / "join" / "units" / f"{inv}.tsv.gz")}
        units = [u for u in read_tsv_gz(LAYER / "units" / "units" / f"{inv}.tsv.gz")
                 if u["unit_type"] == "main_text" and (u["scan_id"], u["page"]) in front
                 and len(text.get((u["scan_id"], u["unit_id"]), "").strip()) >= 3]
        vols.append(dict(inv=inv, n_sides=len(pages), n_front_flagged=len(front), front_first=first,
                         front_last=last, n_after=len(pages) - end, units=units))
    with open(OUT / "front_index_volumes.tsv", "w") as fh:
        fh.write("inv\tn_sides\tn_front_flagged\tfront_first\tfront_last\tn_after\tn_index_units\n")
        for v in sorted(vols, key=lambda v: int(v["inv"])):
            fh.write(f"{v['inv']}\t{v['n_sides']}\t{v['n_front_flagged']}\t{v['front_first']}\t{v['front_last']}"
                     f"\t{v['n_after']}\t{len(v['units'])}\n")
    rng = random.Random(SEED)
    eligible = sorted((v for v in vols if len(v["units"]) >= 6), key=lambda v: int(v["inv"]))
    chosen = rng.sample(eligible, 12)
    rows, rid = [], 0
    for k, v in enumerate(chosen):
        split = "dev" if k < 4 else "test"
        picks = rng.sample(v["units"], 6)
        for j, u in enumerate(picks):
            rid += 1
            rows.append((rid, split, "read" if j < 5 else "reserve", v["inv"], u["scan_id"], u["page"], u["unit_id"]))
    with open(OUT / "reads_entries.tsv", "w") as fh:
        fh.write("read_id\tsplit\trole\tinv\tscan_id\tpage\tunit_id\n")
        for r in rows:
            fh.write("\t".join(map(str, r)) + "\n")
    print(f"front-index volumes: {len(vols)}; eligible: {len(eligible)}; chosen: "
          f"dev {[v['inv'] for v in chosen[:4]]} test {[v['inv'] for v in chosen[4:]]}; rows {len(rows)}")


if __name__ == "__main__":
    main()
