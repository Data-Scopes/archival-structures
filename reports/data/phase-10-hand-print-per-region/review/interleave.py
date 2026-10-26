"""Review check: do 1.10.94 / 1.01.02 regions hold lines from two columns (interleaved baselines)?"""
import sys, statistics, random
from collections import defaultdict
from pagexml.parser import parse_pagexml_file
from archival_structures.text_layer.extract_v2 import extract_scan_v2
from archival_structures.text_layer.io import iter_pagexml
from archival_structures.text_layer.presets import PRESETS
src, inv, pname, nscan = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
preset = PRESETS[pname]
members = list(iter_pagexml(src, skip_resource_forks=preset.skip_resource_forks))
random.Random(16).shuffle(members)
def pts(b): return [tuple(map(float, p.split(","))) for p in str(b or "").split() if "," in p]
out = defaultdict(list)
for member, data in members[:nscan]:
    scan = parse_pagexml_file(member, pagexml_data=data)
    _, _, lrows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
    reg = defaultdict(list)
    for l in lrows:
        if l["text_raw"] and len(pts(l["baseline"])) >= 2: reg[l["region_id"]].append(l)
    for rid, ls in reg.items():
        if len(ls) < 3: continue
        P = [pts(l["baseline"]) for l in ls]
        ys = sorted(sum(y for _, y in p)/len(p) for p in P)
        d = [b-a for a, b in zip(ys, ys[1:]) if b-a > 0]
        if len(d) < 2: continue
        h = statistics.median(float(l["h"]) for l in ls)
        # column-wise: split lines by x-centre at the region's widest gap
        xc = sorted((sum(x for x, _ in p)/len(p), sum(y for _, y in p)/len(p)) for p in P)
        gaps = [(xc[i+1][0]-xc[i][0], i) for i in range(len(xc)-1)]
        g, i = max(gaps)
        widths = [max(x for x, _ in p)-min(x for x, _ in p) for p in P]
        cols = [xc[:i+1], xc[i+1:]] if g > 0.5*statistics.median(widths) else [xc]
        cd = []
        for c in cols:
            cy = sorted(y for _, y in c); cd += [b-a for a, b in zip(cy, cy[1:]) if b-a > 0]
        out["ncols"].append(len(cols)); out["hrel"].append(h/statistics.median(d))
        if len(cd) >= 2: out["hrel_col"].append(h/statistics.median(cd))
med = lambda v: round(statistics.median(v), 3) if v else None
print(inv, "regions", len(out["hrel"]), "2-col regions", sum(c == 2 for c in out["ncols"]),
      "hrel med", med(out["hrel"]), "column-wise", med(out["hrel_col"]))
