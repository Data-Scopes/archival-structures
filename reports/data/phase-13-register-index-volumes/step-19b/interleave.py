"""Phase-12 interleave metric over hand volumes (step 19b).
usage: interleave.py OUT.tsv BASE:LISTFILE [...]   (BASE = data/text_layer_v2_dev/<tag>; LISTFILE = hand inventories)
A main_text unit is interleaved if, over its segments in seg_order keeping lines with w >= 500,
at least 3 consecutive pairs differ in x by more than 800 px. Hand volumes = the inventories in LISTFILE (an inventory can be hand in both bases).
Also writes OUT.ids.txt with the interleaved unit ids (inv<TAB>unit_id)."""
import json, sys
from collections import defaultdict
from pathlib import Path
from archival_structures.text_layer.io import read_tsv_gz

L4 = "1.01.02/NL-HaNA_1.01.02_text_layer_v4"

def interleaved(xs):
    return sum(abs(a - b) > 800 for a, b in zip(xs, xs[1:])) >= 3

def run(base, listfile):
    root = Path(base) / L4
    for inv in sorted(Path(listfile).read_text().split(), key=int):
        lines = {r["line_id"]: (int(r["x"]), int(r["w"]))
                 for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz")}
        main = {r["unit_id"] for r in read_tsv_gz(root / "units" / "units" / f"{inv}.tsv.gz")
                if r["unit_type"] == "main_text"}
        segs = defaultdict(list)
        for r in read_tsv_gz(root / "units" / "segments" / f"{inv}.tsv.gz"):
            if r["unit_id"] in main:
                segs[r["unit_id"]].append((int(r["seg_order"]), r["line_id"]))
        bad = []
        for uid, s in segs.items():
            xs = [lines[l][0] for _, l in sorted(s) if lines[l][1] >= 500]
            if interleaved(xs):
                bad.append(uid)
        yield inv, Path(base).name, len(main), bad

if __name__ == "__main__":
    out, bases = sys.argv[1], sys.argv[2:]
    tu = ti = 0
    ids = open(out.replace(".tsv", ".ids.txt"), "w")
    with open(out, "w") as f:
        f.write("inv\tlayer\tunits\tinterleaved\n")
        for b in bases:
            base, listfile = b.split(":")
            for inv, layer, n, bad in run(base, listfile):
                f.write(f"{inv}\t{layer}\t{n}\t{len(bad)}\n"); tu += n; ti += len(bad)
                for u in bad: ids.write(f"{inv}\t{u}\n")
    print(f"units {tu} interleaved {ti} share {ti / tu:.4%}")
