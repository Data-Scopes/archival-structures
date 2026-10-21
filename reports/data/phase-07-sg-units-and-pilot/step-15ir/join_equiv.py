"""Step 15ir criterion 7: with lead_marks="" join_unit == join_region on every unit of 15ir-a; and the offset violations
(full-span check) per inventory from join/stats; plus join_unit(lead_marks=preset) texts == join/units text."""
import json
from pathlib import Path
from archival_structures.text_layer.join import join_region
from archival_structures.text_layer.join_v2 import join_unit, read_unit_texts
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.word_breaks import read_word_break_sets
HAND = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
res = {}
for kind, invs, marks in (("hand", HAND, ","), ("print", ["455", "456"], "")):
    root = next(Path(f"data/text_layer_v2_dev/15ir-a/{kind}").glob("*_text_layer_v2"))
    sets, _ = read_word_break_sets(root)
    for inv in invs:
        wbcs = sets[inv]
        stats = json.load(open(root / "join" / "stats" / f"{inv}.json"))
        jt = {(u["scan_id"], u["unit_id"]): u["text"] for u in read_tsv_gz(root / "join" / "units" / f"{inv}.tsv.gz")}
        n = bad = bad_marks = 0
        for unit, segs in read_unit_texts(root, inv):
            texts = [s["text"] for s in segs if s["text"]]
            if not texts:
                continue
            n += 1
            if join_unit(texts, wbcs, lead_marks="")[0] != join_region(texts, wbcs):
                bad += 1
            if join_unit(texts, wbcs, lead_marks=marks)[0] != jt[(unit["scan_id"], unit["unit_id"])]:
                bad_marks += 1
        res[inv] = {"units_checked": n, "join_unit_ne_join_region": bad, "stored_text_ne_recomputed": bad_marks,
                    "n_offset_violations": stats["n_offset_violations"], "lead_marks": stats["lead_marks"]}
        print(inv, res[inv], flush=True)
json.dump(res, open(Path(__file__).parent / "join_equiv.json", "w"), indent=1)
