"""Print one read entry: header, entry text, the side's lines with '>' on this unit's lines."""
import sys
sys.path.insert(0, "reports/data/phase-14-index-links/step-20c")
from common import *

rid = sys.argv[1]
row = next(r for r in reads() if r["read_id"] == rid)
inv, sid, page, uid = row["inv"], row["scan_id"], row["page"], row["unit_id"]
sc = scans(inv)
print(f"read_id={rid} inv={inv} scan_id={sid} page={page} pos={sc.index(sid)+1}/{len(sc)}")
text = next(r["text"] for r in read_tsv_gz(f"{L}/join/units/{inv}.tsv.gz") if r["scan_id"] == sid and r["unit_id"] == uid)
print("ENTRY:", text)
mine = {r["line_id"] for r in read_tsv_gz(f"{L}/join/segments/{inv}.tsv.gz") if r["scan_id"] == sid and r["unit_id"] == uid}
side = [l for l in lines(inv).get(sid, []) if l["page"] == page and l["text"]]
side.sort(key=lambda l: (l["col"], l["idx"]))
for k, l in enumerate(side, 1):
    print(f"{k:3d} {'>' if l['line_id'] in mine else ' '} {l['text']}")
