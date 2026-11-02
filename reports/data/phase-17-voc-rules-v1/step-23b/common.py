"""Step 23b helpers shared by compare.py and sample.py (read-only)."""
import csv
import gzip
from pathlib import Path

csv.field_size_limit(10**9)
OLD = Path("data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5")
NEW = Path("data/text_layer_voc_23b/1.04.02/NL-HaNA_1.04.02_text_layer_v6")
NOT = "¬"
UP_DIRS = ["scans", "lines", "mode", "layout/lines", "layout/pages", "types/segments", "units/units", "units/segments"]
CLASSES = ["guarded", "unit_end", "not_line_final", "other"]


def rd(p):
    with gzip.open(p, "rt", encoding="utf8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE))


def inventories(root):
    return sorted(p.name.split(".")[0] for p in (root / "join" / "units").glob("*.tsv.gz"))


def classify_nots(text, segs):
    """Yield (index, class, seg) for every ¬ in a unit text. `segs` = the unit's join segments, by seg_order."""
    bounds = [(int(s["char_start"]), int(s["char_end"]), s) for s in segs]
    for i, ch in enumerate(text):
        if ch != NOT:
            continue
        seg = next(((a, b, s) for a, b, s in bounds if a <= i < b), None)
        if seg is None:
            yield i, "other", None
            continue
        a, b, s = seg
        tail = text[i + 1:b]
        if tail.strip() != "":
            yield i, "not_line_final", s
        elif s is bounds[-1][2]:
            yield i, "unit_end", s
        elif s["boundary_kind"] not in ("merge", "plain", ""):
            yield i, "guarded", s
        else:
            yield i, "other", s
