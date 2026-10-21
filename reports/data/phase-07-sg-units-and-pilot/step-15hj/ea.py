"""Step 15hj error-analysis sample (Random(19)). Run from the AS root:
    .venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15hj/ea.py
Writes ea_sample.txt (what the verdicts in ea_verdicts.tsv were read from) and ea_counts.json."""
import json
import random
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common15hj import ALL_INVS, HERE, PRINT, root, wbc_set
from archival_structures.text_layer.join_v2 import join_unit, read_unit_texts
from archival_structures.text_layer.word_breaks import LEADING_LETTERS, TRAILING_LETTERS


def word(left, right):
    l = TRAILING_LETTERS.search(left[:-1]) if left else None
    rr = LEADING_LETTERS.match(right.lstrip("-„=.:¬ "))
    return (l.group(1) if l else "") + "|" + (rr.group(1) if rr else "")


def main():
    rnd = random.Random(19)
    events = []   # (group, inv, unit_id, kind, left, right, extra)
    last_dash = []
    counts = Counter()
    for inv in ALL_INVS:
        wbcs = wbc_set(inv)
        hand = inv not in PRINT
        for unit, segs in read_unit_texts(root(inv), inv):
            texts = [s["text"] for s in segs if s["text"]]
            if not texts:
                continue
            text, spans, bounds = join_unit(texts, wbcs)
            for i, (k, ch) in enumerate(bounds[:-1]):
                nxt = texts[i + 1]
                lead = spans[i + 1][2]
                rf = nxt[lead:].lstrip("".join(wbcs))[:1]
                ev = (("hand" if hand else "print"), inv, unit["unit_id"][-40:], k, texts[i][-30:],
                      nxt[:30], ch)
                if k == "merge":
                    events.append(("merge", *ev))
                    if rf.isupper() or rf.isdigit():
                        events.append(("merge_upper_digit", *ev))
                elif k != "plain":
                    events.append(("guarded", *ev))
            if (not hand) and unit["unit_type"] == "main_text" and texts[-1].endswith("-"):
                last_dash.append((inv, unit["unit_id"][-40:], texts[-1][-40:], len(texts)))
    by = {g: [e for e in events if e[0] == g] for g in ("merge", "merge_upper_digit", "guarded")}
    counts = {g: len(v) for g, v in by.items()}
    counts["print_main_units_ending_dash"] = len(last_dash)
    out = []
    for side in ("hand", "print"):
        pool = [e for e in by["merge"] if e[1] == side]
        for e in rnd.sample(pool, 15):
            out.append(("merge_" + side, e))
    for e in rnd.sample(by["guarded"], 20):
        out.append(("guarded", e))
    pool = by["merge_upper_digit"]
    for e in (rnd.sample(pool, 20) if len(pool) > 20 else pool):
        out.append(("merge_upper_digit", e))
    for tag, inv_, ch_, n_ in (("char_.455", "455", ".", 20), ("char_.3822", "3822", ".", 10),
                               ("char_:11281", "11281", ":", 10), ("char_=", None, "=", 10)):
        pool = [e for e in by["merge"] if e[7] == ch_ and (inv_ is None or e[2] == inv_)]
        counts[tag] = len(pool)
        for e in rnd.sample(pool, min(n_, len(pool))):
            out.append((tag, e))
    lines = []
    for g, e in out:
        lines.append(f"{g}\t{e[2]}\t{e[3]}\t{e[4]}\t{e[5]!r}\t{e[6]!r}\t{e[7]}\t=> {word(e[6], e[7])}")
    lines.append("")
    for x in rnd.sample(last_dash, min(10, len(last_dash))):
        lines.append("print_unit_ends_dash\t" + "\t".join(map(str, x)))
    (HERE / "ea_sample.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (HERE / "ea_counts.json").write_text(json.dumps(counts, indent=1))
    print(counts)


main()
