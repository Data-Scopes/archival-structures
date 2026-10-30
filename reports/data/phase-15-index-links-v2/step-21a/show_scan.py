"""Print one scan. Usage: show_scan.py INV (POS | SCAN_ID) [--full verso|recto|single]"""
import re
import sys
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from _common import tab

args = sys.argv[1:]
full = None
if "--full" in args:
    i = args.index("--full")
    full = args[i + 1]
    del args[i:i + 2]
inv, ref = args[0], args[1]
scans = tab("scans", inv)
if ref.isdigit() and len(ref) < 6:
    pos = int(ref)
    sid = scans[pos - 1]["scan_id"]
else:
    sid = ref
    pos = [i for i, s in enumerate(scans, 1) if s["scan_id"] == sid][0]
print(f"inv={inv} scan_id={sid} pos={pos}/{len(scans)}")
lay = {r["line_id"]: r for r in tab("layout", inv) if r["scan_id"] == sid}
lines = {r["line_id"]: r for r in tab("lines", inv) if r["scan_id"] == sid}
sides = {}
for lid, lr in lay.items():
    ln = lines.get(lid)
    if ln and ln["text"].strip():
        sides.setdefault(lr["page"], []).append(ln)
NUM = re.compile(r"\d{1,4}")
ENDNUM = re.compile(r"(?<!\d)(\d{1,4})\.?\s*$")
for side in ("verso", "recto", "single"):
    ls = sorted(sides.get(side, []), key=lambda l: (float(l["y"]), float(l["x"])))
    if not ls or (full and side != full):
        continue
    ys = [float(l["y"]) for l in ls]
    lo, hi = min(ys), max(ys)
    print(f"-- {side}: {len(ls)} lines")
    def show(l):
        print(f"   x={int(float(l['x'])):5d} y={int(float(l['y'])):5d} {l['text'].strip()}")
    if full:
        for l in ls:
            show(l)
        continue
    for l in ls[:4]:
        show(l)
    print("   ...")
    for l in ls[-3:]:
        show(l)
    for l in ls:
        t = l["text"].strip()
        if len(t) <= 8 and NUM.search(t):
            f = (float(l["y"]) - lo) / ((hi - lo) or 1)
            zone = "top" if f <= 0.2 else "bottom" if f >= 0.8 else "mid"
            print(f"   [num] y={int(float(l['y']))} {zone}: {t}")
    # a 1-4 digit number at the END of the first or last line (long lines; added in session 7)
    for tag, l in (("first", ls[0]), ("last", ls[-1])):
        t = l["text"].strip()
        m = ENDNUM.search(t)
        if m and len(t) > 8:
            print(f"   [end-num] {tag} line y={int(float(l['y']))}: {m.group(1)}  <- ...{t[-30:]}")
