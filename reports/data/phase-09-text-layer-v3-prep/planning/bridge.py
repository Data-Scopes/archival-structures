"""Merged print layout regions whose source pieces hold a guard-blocked pair (phase 09 planning).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/bridge.py \
        --layer data/text_layer_v2_dev/15ir-a [--out bridge.tsv]

The interleave guard (`_guard_blocks`, width_frac 0.5) is tested per pair, so a third piece can
bridge a blocked pair into one merge set. Pieces are rebuilt as (legacy region_id, line `column`)
groups of each merged region's lines. Writes every case with the smaller piece's text."""
import argparse
import itertools
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import PRINT_INV, layer_root, read  # noqa: E402


def blocks(s, l, wf=0.5):
    med = statistics.median(x["w"] for x in l)
    if not all(x["w"] < wf * med for x in s):
        return False
    st, sb = min(x["y"] for x in s), max(x["y"] + x["h"] for x in s)
    lt, lb = min(x["y"] for x in l), max(x["y"] + x["h"] for x in l)
    return st > lt and sb < lb


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--out", default="")
    a = ap.parse_args(argv)
    root = layer_root(a.layer, "print")
    rows = []
    for inv in PRINT_INV:
        L = read(root / "lines" / f"{inv}.lines.tsv.gz",
                 ["scan_id", "line_id", "region_id", "column", "x", "y", "w", "h", "text"])
        for c in "xywh":
            L[c] = L[c].astype(int)
        LL = read(root / "layout" / "lines" / f"{inv}.tsv.gz", ["scan_id", "line_id", "layout_region_id"])
        R = read(root / "layout" / "regions" / f"{inv}.tsv.gz", ["scan_id", "layout_region_id", "change"])
        merged = set(zip(R.scan_id[R.change.str.contains("merged")],
                         R.layout_region_id[R.change.str.contains("merged")]))
        n = nb = 0
        for key, g in LL.merge(L, on=["scan_id", "line_id"]).groupby(["scan_id", "layout_region_id"]):
            if key not in merged:
                continue
            n += 1
            pieces = {}
            for pk, pg in g.groupby(["region_id", "column"]):
                recs = pg.to_dict("records")
                pieces[pk] = [x for x in recs if x["text"].strip()] or recs
            bad = [(p, q) for p, q in itertools.combinations(pieces, 2)
                   if blocks(pieces[p], pieces[q]) or blocks(pieces[q], pieces[p])]
            if bad:
                nb += 1
                p, q = bad[0]
                small = p if len(pieces[p]) <= len(pieces[q]) else q
                rows.append((inv, key[0], key[1], len(pieces), len(bad),
                             " | ".join(x["text"][:30] for x in pieces[small][:3])))
        print(inv, "merged regions", n, "with a guard-blocked pair inside", nb)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write("inv\tscan_id\tlayout_region_id\tn_pieces\tn_blocked_pairs\tsmall_piece_text\n")
            for r in rows:
                fh.write("\t".join(map(str, r)) + "\n")


if __name__ == "__main__":
    main()
