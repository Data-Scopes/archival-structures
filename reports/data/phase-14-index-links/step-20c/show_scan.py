"""Print one scan: per side first 4 / last 3 lines and short numeric lines; --full SIDE prints all lines."""
import sys, re
sys.path.insert(0, "reports/data/phase-14-index-links/step-20c")
from common import *

args = sys.argv[1:]
full = None
if "--full" in args:
    i = args.index("--full"); full = args[i+1]; del args[i:i+2]
inv, ref = args
sc = scans(inv)
sid = sc[int(ref)-1] if ref.isdigit() else ref
pos = sc.index(sid) + 1
print(f"scan {sid} pos={pos}/{len(sc)}")
ls = [l for l in lines(inv).get(sid, []) if l["text"]]
for side in ("verso", "recto", "single"):
    s = [l for l in ls if l["page"] == side]
    if not s:
        continue
    if full:
        if side != full:
            continue
        s.sort(key=lambda l: (l["col"], l["idx"]))
        print(f"-- {side} ({len(s)} lines) FULL")
        for l in s:
            print(f"  y={l['y']:5d} {l['text']}")
        continue
    ys = [l["y"] for l in s]; lo, hi = min(ys), max(l["y"] + l["h"] for l in s)
    s.sort(key=lambda l: (l["y"], l["x"]))
    print(f"-- {side} ({len(s)} lines)")
    for l in s[:4]: print(f"  first y={l['y']:5d} {l['text']}")
    for l in s[-3:]: print(f"  last  y={l['y']:5d} {l['text']}")
    for l in s:
        if len(l["text"]) <= 8 and re.search(r"\d{1,4}", l["text"]):
            rel = (l["y"] - lo) / max(1, hi - lo)
            pos_ = "TOP" if rel < .2 else "BOTTOM" if rel > .8 else "mid"
            print(f"  num   y={l['y']:5d} {pos_} {l['text']}")
