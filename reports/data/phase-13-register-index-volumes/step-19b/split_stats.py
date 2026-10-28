"""Per-volume split statistics of the rebuilt hand clones (step 19b).
usage: split_stats.py OUT.tsv [INTERLEAVE_BASE.tsv INTERLEAVE_NEW.tsv]
Span piece = a piece whose hull is wider than 0.65 x the hull of the whole split region (approximation,
the layout tables do not record the side of a piece). Also checks duplicate layout_region_id per scan."""
import csv, sys
from collections import Counter
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *

def main(out, base_tsv=None, new_tsv=None):
    il = index_like_sides()
    tot = Counter()
    dup = 0
    with open(out, "w") as f:
        f.write("inv\tclone\tregions_split\tpieces\tspan_pieces\tlines_in_split\tsplit_index_like\tsplit_other\n")
        for inv, clone, root in volumes():
            rows = list(read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"))
            seen = Counter((r["scan_id"], r["layout_region_id"]) for r in rows)
            dup += sum(v > 1 for v in seen.values())
            c = Counter()
            for (scan, src), pcs in split_groups(inv, root).items():
                c["regions_split"] += 1
                c["pieces"] += len(pcs)
                x0 = min(int(p["x"]) for p in pcs); x1 = max(int(p["x"]) + int(p["w"]) for p in pcs)
                c["span_pieces"] += sum(int(p["w"]) > 0.65 * (x1 - x0) for p in pcs)
                c["lines"] += sum(int(p["n_lines"]) for p in pcs)
                c["il" if (inv, scan, pcs[0]["page"]) in il else "other"] += 1
            f.write(f"{inv}\t{clone}\t{c['regions_split']}\t{c['pieces']}\t{c['span_pieces']}\t{c['lines']}\t{c['il']}\t{c['other']}\n")
            tot.update(c)
    print(dict(tot), "duplicate (scan, layout_region_id):", dup)
    if base_tsv:
        import pandas as pd
        for nm, p in (("before", base_tsv), ("after", new_tsv)):
            d = pd.read_csv(p, sep="\t")
            print(nm, "units", d.units.sum(), "interleaved", d.interleaved.sum(), f"{d.interleaved.sum()/d.units.sum():.4%}")

if __name__ == "__main__":
    main(*sys.argv[1:])
