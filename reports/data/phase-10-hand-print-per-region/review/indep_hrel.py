"""Independent re-implementation of the P3 rule (review of 15q). Not using analysis/hand_print.py."""
import sys, statistics, json
from collections import defaultdict
from pagexml.parser import parse_pagexml_file
from archival_structures.text_layer.extract_v2 import extract_scan_v2
from archival_structures.text_layer.io import iter_pagexml
from archival_structures.text_layer.presets import PRESETS
T_LO, T_HI = 1.529, 1.534
src, inv = sys.argv[1], sys.argv[2]
preset = PRESETS["sg_1.01.02_v3"]
scans = {}
for member, data in iter_pagexml(src, skip_resource_forks=preset.skip_resource_forks):
    try: scan = parse_pagexml_file(member, pagexml_data=data)
    except Exception: continue
    _, _, lrows = extract_scan_v2(scan, inv=inv, inv_dir=inv, member=member, preset=preset)
    reg = defaultdict(list)
    for l in lrows:
        if l["text_raw"]: reg[l["region_id"]].append(l)
    feats = []
    for rid, ls in reg.items():
        if len(ls) < 3: continue
        hs = [float(l["h"]) for l in ls if l["h"] not in ("", None)]
        ys = []
        for l in ls:
            pts = [p for p in str(l["baseline"] or "").split() if "," in p]
            if len(pts) >= 2: ys.append(sum(float(p.split(",")[1]) for p in pts) / len(pts))
        ys.sort(); d = [b - a for a, b in zip(ys, ys[1:]) if b - a > 0]
        if not hs or len(d) < 2: continue
        feats.append((statistics.median(hs) / statistics.median(d), sum(len(l["text_raw"]) for l in ls)))
    if not feats: scans[member] = "unlabelled"; continue
    feats.sort(); tot = sum(c for _, c in feats)
    if tot < 200: scans[member] = "unlabelled"; continue
    acc = 0
    for v, c in feats:
        acc += c
        if acc >= tot / 2: break
    scans[member] = "print" if v <= T_LO else "hand" if v >= T_HI else "uncertain"
from collections import Counter
c = Counter(scans.values()); p, h = c["print"], c["hand"]
print(json.dumps({"inv": inv, **c, "share": round(p / (p + h), 4) if p + h else None}))
