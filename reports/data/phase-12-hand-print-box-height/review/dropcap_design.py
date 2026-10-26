"""Phase-12 review: where do the print unit cuts fall? (design measurement for step 15x; reads only)

Recomputes the print paragraph cuts of `units.split_region` (library `paragraph_features`,
`cues_fired`, the 1.01.02 print `UnitParams` of the v4 preset) on an existing layer and counts each
cut by its cue set and by its distance d (in main lines) from the previous cut or the first main
line of the layout region. It never applies a new rule: it only describes where today's cuts are.

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/dropcap_design.py LAYER INV [INV ...]
"""
import sys
from collections import Counter
from pathlib import Path

from archival_structures.text_layer.presets import PRESETS, inventory_preset
from archival_structures.text_layer.units import (
    HEADER_TYPES, UnitParams, cues_fired, load_segments, paragraph_features)


def cuts_of_region(segs, params):
    segs = [dict(s) for s in segs]
    for pos, s in enumerate(segs):
        s["_pos"] = pos
    mains = [s for s in segs if s["line_type"] == "main_text"]
    if len(mains) < 2:
        return []
    hb = [False] * len(mains)
    for i in range(1, len(mains)):
        hb[i] = any(segs[p]["line_type"] in HEADER_TYPES
                    for p in range(mains[i - 1]["_pos"] + 1, mains[i]["_pos"]))
    out, last = [], 0
    for i, feat in enumerate(paragraph_features(mains, hb), start=1):
        cues = cues_fired(feat, params)
        if cues:
            out.append(("+".join(cues), i - last))
            last = i
    return out


def main():
    layer = Path(sys.argv[1])
    params = UnitParams.from_dict(dict(inventory_preset(PRESETS["sg_1.01.02_v4"], "print").unit_params))
    total = Counter()
    for inv in sys.argv[2:]:
        c = Counter()
        n_regions = 0
        for segs in load_segments(layer, inv).values():
            n_regions += 1
            for cue, d in cuts_of_region(segs, params):
                c[(cue, min(d, 4))] += 1
        total.update(c)
        ind = {d: c[("indent", d)] for d in (1, 2, 3, 4)}
        n = sum(c.values())
        print(f"{inv}: regions {n_regions}, cuts {n}; indent-only by d (1, 2, 3, >=4): {ind}; "
              f"other cue sets {n - sum(ind.values())}")
    n = sum(total.values())
    print("ALL cuts", n)
    for (cue, d), k in sorted(total.items(), key=lambda kv: -kv[1])[:14]:
        print(f"   {cue:24} d={'>=4' if d == 4 else d}: {k} ({k / n:.3f})")


if __name__ == "__main__":
    main()
