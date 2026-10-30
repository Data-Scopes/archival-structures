"""Step 21c: 10 seeded predicted entries (random.Random(2103)) of the spent phase-14 volumes 3590 and 3611, under the
selection in select.json, printed for reading (no gold). Run from the AS root: .venv/bin/python <this file>."""
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval_links as E
from archival_structures.analysis.index_links import entry_locators, front_index_run, read_volume_inputs

sel = json.loads((E.HERE / "select.json").read_text())
m, v = sel["method"], sel["variant"]
pool = []
for inv in ("3590", "3611"):
    vol = E.Volume(inv)
    vi = read_volume_inputs(E.LAYER, inv)
    run = front_index_run(vi.flags)
    front = [s for i, s in enumerate(vi.sides) if i <= run[1] and vi.flags[i]]
    for side in front:
        if m == "A0":
            for text, segs, first in vol.v1_entries(side):
                locs = [l.number for l in E.extract_locators(text, E.SG_LOCATOR_PARAMS[v], folio_range=vol.rng)]
                pool.append((inv, side, type("U", (), dict(text=text, line_ids=[g[2] for g in segs]))(), locs))
            continue
        for e in vol.assembled(side, m):
            if e.kind == "entry":
                pool.append((inv, side, e, [l.number for l, _ in entry_locators(e, E.SG_LOCATOR_PARAMS[v], folio_range=vol.rng)]))
print(f"selection {m} {v}; {len(pool)} predicted entries on front-index sides")
for i, (inv, side, e, locs) in enumerate(random.Random(2103).sample(pool, 10), 1):
    print(f"[{i}] {inv} {side[0][-4:]} {side[1]} lines={len(e.line_ids)} locators={locs}\n    {e.text[:300]}")
