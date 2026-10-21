"""Step 15hj2 error-analysis sample (Random(23)). Run from the AS root:
    .venv/bin/python -I reports/data/phase-07-sg-units-and-pilot/step-15hj2/ea.py
Writes ea_sample.txt (what the verdicts were read from) and ea_counts.json."""
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from common15hj2 import HERE, LAYERS, layer_root
from archival_structures.text_layer.join_v2 import join_unit, read_unit_texts
from archival_structures.text_layer.word_breaks import read_word_break_sets

A = Path("data/text_layer_v2_dev/15hj-a"); B = Path("data/text_layer_v2_dev/15hj-b")

def events(layer, inv, chars=None, wbcs=None):
    r = layer_root(layer, inv)
    if wbcs is None:
        wbcs = read_word_break_sets(r)[0][inv]
    out = []
    for unit, segs in read_unit_texts(r, inv):
        texts = [s["text"] for s in segs if s["text"]]
        if not texts:
            continue
        text, spans, bounds = join_unit(texts, wbcs)
        for i, (k, ch) in enumerate(bounds[:-1]):
            if k == "merge" and (chars is None or ch in chars):
                out.append((inv, unit["unit_id"][-30:], texts[i][-32:], texts[i + 1][:32], ch))
    return out

def main():
    rnd = random.Random(23)
    counts, lines = {}, []
    new = []
    for inv in LAYERS["hand_tar"][1]:
        old = set(read_word_break_sets(layer_root(A, inv))[0][inv])
        added = set("-=„") - old
        ev = events(B, inv, chars=added) if added else []
        counts["new_" + inv] = len(ev)
        new += ev
    sel = rnd.sample(new, min(20, len(new)))
    lines += [f"new_hand_tar\t" + "\t".join(map(repr, e)) for e in sel]
    counts["new_total"] = len(new)
    for inv in ("455", "3822"):
        ev = events(A, inv, chars={"."})
        counts["dot_" + inv] = len(ev)
        lines += [f"dot_{inv}\t" + "\t".join(map(repr, e)) for e in rnd.sample(ev, min(10, len(ev)))]
    ev = events(B, "11281", chars={":"})
    counts["colon_11281"] = len(ev)
    lines += ["colon_11281\t" + "\t".join(map(repr, e)) for e in rnd.sample(ev, min(5, len(ev)))]
    (HERE / "ea_sample.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (HERE / "ea_counts.json").write_text(json.dumps(counts, indent=1))
    print(counts)
main()
