"""Step 19f statistics of the reordered regions: per volume reordered regions (flagged / other sides), wide lines,
right-starts-above-left share, duplicate (scan, layout_region_id). usage: stats.py OUT.tsv"""
import sys
from collections import Counter, defaultdict
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from common import *

def main(out):
    tot = Counter(); dup = 0; wide_hist = Counter()
    with open(out, "w") as f:
        f.write("inv\tclone\tregions\treordered\treordered_flagged\treordered_other\tlines_in_reordered\twide_lines\tright_above_left\n")
        for inv, clone, root in volumes():
            rows = list(read_tsv_gz(root / "layout" / "regions" / f"{inv}.tsv.gz"))
            dup += sum(v > 1 for v in Counter((r["scan_id"], r["layout_region_id"]) for r in rows).values())
            fl = flagged_sides(inv, root)
            re = [r for r in rows if "column_reordered" in r["change"]]
            keep = {(r["scan_id"], r["layout_region_id"]): r for r in re}
            lay = defaultdict(list)
            L = {r["line_id"]: r for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
            for r in read_tsv_gz(root / "layout" / "lines" / f"{inv}.tsv.gz"):
                if (r["scan_id"], r["layout_region_id"]) in keep:
                    lay[(r["scan_id"], r["layout_region_id"])].append(r)
            c = Counter()
            for k, r in keep.items():
                ls = sorted(lay[k], key=lambda x: int(x["layout_line_idx"]))
                rows_ = [L[x["line_id"]] for x in ls]
                tg = tag_lines(rows_)
                nw = tg.count("W"); wide_hist[min(nw, 3)] += 1
                c["wide"] += nw; c["lines"] += len(ls)
                ly = [int(l["y"]) for l, t in zip(rows_, tg) if t == "L" and l["text"].strip()]
                ry = [int(l["y"]) for l, t in zip(rows_, tg) if t == "R" and l["text"].strip()]
                c["right_above"] += min(ry) < min(ly)
                c["flagged" if (r["scan_id"], r["page"]) in fl else "other"] += 1
            f.write(f"{inv}\t{clone}\t{len(rows)}\t{len(re)}\t{c['flagged']}\t{c['other']}\t{c['lines']}\t{c['wide']}\t{c['right_above']}\n")
            tot.update(c); tot["reordered"] += len(re)
    print(dict(tot), "wide-lines-per-region hist (3=3+):", dict(sorted(wide_hist.items())), "duplicate (scan, layout_region_id):", dup)

if __name__ == "__main__":
    main(sys.argv[1])
