"""Phase-15 planning prototype of line-level entry assembly on front-index sides (spent 20c volumes only).

Side lines = the lines of every unit on the side except `page_number`-only furniture, in v5 unit order.
A2 order: page_structure._reorder_lines on a duck-typed region of the side's lines (SG_HAND_COLUMN_ORDER_V5).
Entry rule: an entry closes after a line whose text ends in a number run (`\\d{1,4}` then [.,:;]* at the end);
a line of only numbers and punctuation joins the previous entry.
Usage: assembly_proto.py INV [K]  (K: print the K-th front side). Without K: counts over the volume.
"""
import re
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, ".")
from archival_structures.analysis import page_structure as ps  # noqa: E402
from archival_structures.analysis.index_links import front_index_run, read_volume_inputs  # noqa: E402
from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402
from archival_structures.text_layer.presets import SG_HAND_COLUMN_ORDER_V5  # noqa: E402

L = Path("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5")
SPENT = {"3254", "3308", "3394", "3471", "3256", "3320", "3352", "3401", "3456", "3590", "3611", "3712"}
END_NUM = re.compile(r"(?<![\w])\d{1,4}[\s.,:;]*$")
ONLY_NUM = re.compile(r"[\d\s.,:;]+")

inv = sys.argv[1]
assert inv in SPENT, "planner prototypes read spent volumes only"
vi = read_volume_inputs(L, inv)
first, last, end = front_index_run(vi.flags)
front = [vi.sides[i] for i in range(last + 1) if vi.flags[i]]
lines = {(r["scan_id"], r["line_id"]): r for r in read_tsv_gz(L / "lines" / f"{inv}.lines.tsv.gz")}
units = {(u["scan_id"], u["unit_id"]): u for u in read_tsv_gz(L / "units/units" / f"{inv}.tsv.gz")}
segs = list(read_tsv_gz(L / "units/segments" / f"{inv}.tsv.gz"))
types = Counter(u["unit_type"] for u in units.values() if (u["scan_id"], u["page"]) in set(front))
by_side = {}
for s in segs:
    u = units[(s["scan_id"], s["unit_id"])]
    side = (u["scan_id"], u["page"])
    if side not in front or u["unit_type"] in ("page_number",):
        continue
    by_side.setdefault(side, []).append((int(u["unit_order"]), int(s["seg_order"]), s["line_id"], u["unit_type"]))


def side_lines(side):
    seen, out = set(), []
    for _, _, lid, ut in sorted(by_side.get(side, [])):
        if lid in seen:
            continue
        seen.add(lid)
        r = lines[(side[0], lid)]
        out.append(SimpleNamespace(id=lid, text=r["text"], metadata={}, utype=ut, baseline=None,
                                   coords=SimpleNamespace(x=int(r["x"]), y=int(r["y"]), w=int(r["w"]), h=int(r["h"]))))
    return out


def entries(ls):
    out, cur = [], []
    for ln in ls:
        t = (ln.text or "").strip()
        if not t:
            continue
        if ONLY_NUM.fullmatch(t) and out and not cur:
            out[-1].append(ln)
            continue
        cur.append(ln)
        if END_NUM.search(t):
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return out


stats = Counter()
for k, side in enumerate(front):
    ls = side_lines(side)
    re_ord = ps._reorder_lines(SimpleNamespace(lines=ls, id="side", metadata={}), SG_HAND_COLUMN_ORDER_V5, None)
    stats["sides"] += 1
    stats["reordered"] += re_ord is not None
    ls2 = re_ord or ls
    for e in entries(ls2):
        stats["entries"] += 1
        stats["with_num"] += bool(END_NUM.search(e[-1].text.strip()))
    if len(sys.argv) > 2 and k == int(sys.argv[2]):
        print(side, "reordered" if re_ord else "unchanged")
        for e in entries(ls2):
            print(" | ".join(f"{l.text.strip()}" for l in e)[:160], f"[{e[0].utype[:4]}]")
print(inv, dict(stats), "unit types on front sides:", dict(types))
