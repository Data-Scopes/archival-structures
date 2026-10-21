"""Step 15g error-analysis samples (random.Random(17), a fresh instance per sample).

Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g/ea.py KIND OUT.txt [LAYER_DIR]
KIND:
  type:<TYPE>      20 segments of that type over all 16 inventories (page_number, date_header,
                   running_header, catch_word, signature_mark, marginalia, index, other)
  split:<RULE>     10 split lines of rule:date_prefix | rule:margin_prefix | rule:pn_suffix
  recall:hand      20 main_text lines in a top/bottom band, hand layers (recall:print: 20 print)
  indexpages       20 pages typed index + 10 register pages (>=30% of lines end in a number, short
                   lines >=40%) of 3821-3823 that the rule missed
Each sample prints the previous and next line on the page (same page, y order) as context and
the 'x/y' position relative to the page (page_w, page_h)."""

import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import ALL_INVS, PARENT, PRINT, layer_root  # noqa: E402

from archival_structures.text_layer.io import read_tsv_gz  # noqa: E402

KIND, OUT = sys.argv[1], Path(sys.argv[2])
LAYER = Path(sys.argv[3]) if len(sys.argv) > 3 else PARENT / "15g-a"


class Inv:
    def __init__(self, inv):
        root = layer_root(LAYER, inv)
        self.inv = inv
        self.dims = {r["scan_id"]: (int(r["page_w"]), int(r["page_h"]))
                     for r in read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz")}
        self.lines = {(r["scan_id"], r["line_id"]): r
                      for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz") if r["text"].strip()}
        self.segs = list(read_tsv_gz(root / "types" / "segments" / f"{inv}.tsv.gz"))
        self.by_line = defaultdict(list)
        self.pages = defaultdict(dict)       # (scan, page) -> {line key: (y, x)}
        for i, s in enumerate(self.segs):
            key = (s["scan_id"], s["line_id"])
            self.by_line[key].append(i)
            cur = self.pages[(s["scan_id"], s["page"])]
            pos = (int(s["y"]), int(s["x"]))
            cur[key] = min(cur.get(key, pos), pos)
        self.order = {k: sorted(v, key=v.get) for k, v in self.pages.items()}

    def render_line(self, key):
        text = self.lines[key]["text"]
        marks = []
        for i in self.by_line[key]:
            s = self.segs[i]
            marks.append((int(s["char_start"]), int(s["char_end"]), s["line_type"]))
        if len(marks) == 1:
            return f"{text}   <{marks[0][2]}>"
        out = []
        for a, b, t in marks:
            out.append(f"[{t}: {text[a:b]}]")
        return " ".join(out)

    def show(self, i, n_ctx=1):
        s = self.segs[i]
        key = (s["scan_id"], s["line_id"])
        w, h = self.dims[s["scan_id"]]
        order = self.order[(s["scan_id"], s["page"])]
        pos = order.index(key)
        lines = [f"## {self.inv} {s['scan_id'].split('_', 2)[-1]} {s['page']} seg{s['seg_idx']} "
                 f"{s['line_type']} {s['type_method']} x={int(s['x'])/w:.2f} y={int(s['y'])/h:.2f} "
                 f"pos={pos}/{len(order)}"]
        for j in range(max(0, pos - n_ctx), min(len(order), pos + n_ctx + 1)):
            k = order[j]
            tag = ">>>" if j == pos else "   "
            lines.append(f"  {tag} {self.render_line(k)}")
        return "\n".join(lines)


def collect(pred, invs=ALL_INVS):
    """All (Inv, segment index) matching pred, with Inv objects cached lazily per inventory."""
    found = []
    for inv in invs:
        obj = Inv(inv)
        found += [(obj, i) for i, s in enumerate(obj.segs) if pred(obj, i, s)]
    return found


def band(obj, s):
    order = obj.order[(s["scan_id"], s["page"])]
    key = (s["scan_id"], s["line_id"])
    pos = order.index(key)
    return pos < 3 or pos >= len(order) - 2


def main():
    rng = random.Random(17)
    kind, _, arg = KIND.partition(":")
    out = []
    if kind == "type":
        cands = collect(lambda o, i, s: s["line_type"] == arg)
        pick = rng.sample(cands, min(20, len(cands)))
        out += [f"# {len(cands)} segments of type {arg}"] + [o.show(i) for o, i in pick]
    elif kind == "split":
        cands = collect(lambda o, i, s: s["seg_idx"] == "0" and any(
            o.segs[j]["type_method"] == arg for j in o.by_line[(s["scan_id"], s["line_id"])]))
        pick = rng.sample(cands, min(10, len(cands)))
        out += [f"# {len(cands)} lines split by {arg}"] + [o.show(i) for o, i in pick]
    elif kind == "recall":
        invs = [i for i in ALL_INVS if (i in PRINT) == (arg == "print")]
        cands = collect(lambda o, i, s: s["line_type"] == "main_text" and band(o, s), invs)
        pick = rng.sample(cands, min(20, len(cands)))
        out += [f"# {len(cands)} main_text segments in a band ({arg})"] + [o.show(i, 2) for o, i in pick]
    elif kind == "indexpages":
        pages = []
        for inv in ALL_INVS:
            obj = Inv(inv)
            by_page = defaultdict(list)
            for i, s in enumerate(obj.segs):
                by_page[(s["scan_id"], s["page"])].append(i)
            for key, idxs in by_page.items():
                types = {obj.segs[i]["line_type"] for i in idxs}
                n_index = sum(obj.segs[i]["line_type"] == "index" for i in idxs)
                if n_index and n_index >= 0.5 * len(idxs):
                    pages.append(("index", obj, key, idxs))
                elif inv in ("3821", "3822", "3823"):
                    ends = sum(obj.lines[(obj.segs[i]["scan_id"], obj.segs[i]["line_id"])]["text"].rstrip(". ")[-1:].isdigit()
                               for i in idxs)
                    if ends >= 0.3 * len(idxs) and "index" not in types:
                        pages.append(("missed", obj, key, idxs))
        for label in ("index", "missed"):
            group = [p for p in pages if p[0] == label]
            pick = rng.sample(group, min(20 if label == "index" else 10, len(group)))
            out.append(f"# {len(group)} pages ({label})")
            for _, obj, key, idxs in pick:
                out.append(f"## {label} {obj.inv} {key[0].split('_', 2)[-1]} {key[1]} lines={len(idxs)}")
                order = obj.order[key]
                for k in order[:12]:
                    out.append(f"    {obj.render_line(k)[:110]}")
    OUT.write_text("\n".join(out) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
