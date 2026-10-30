"""Print one read side for reading. Usage: show_side.py READ_ID   (no prediction, no labels)."""
import sys
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from _common import read_row, side_lines, tab

rid = sys.argv[1]
r = read_row(rid)
scans = tab("scans", r["inv"])
pos = [i for i, s in enumerate(scans, 1) if s["scan_id"] == r["scan_id"]][0]
print(f"read_id={rid} inv={r['inv']} scan_id={r['scan_id']} page={r['page']} scan_pos={pos}/{len(scans)}")
for k, (key, lid, ln) in enumerate(side_lines(r["inv"], r["scan_id"], r["page"]), 1):
    ut = (key[2] if key else "----")[:4]
    print(f"{k:3d} x={int(float(ln['x'])):5d} y={int(float(ln['y'])):5d} w={int(float(ln['w'])):5d} {ut:4s} {ln['text'].strip()}")
