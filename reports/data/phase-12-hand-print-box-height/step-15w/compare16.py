"""Step 15w, acceptance 2: 15w-a against 15v-a on the 16 shared inventories (wraps step-15v/compare.py).

    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15w/compare16.py
Files of inventories not in 15v-a are dropped from the 'missing' list (counted); everything else is as compare.py.
"""
import json
import sys
from pathlib import Path

import importlib.util  # noqa: E402

sys.path.insert(0, "reports/data/phase-09-text-layer-v3-prep/planning")   # compare.py imports common from here
_spec = importlib.util.spec_from_file_location("compare15v", "reports/data/phase-12-hand-print-box-height/step-15v/compare.py")
C = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(C)

SHARED = set("3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281 455 456".split())
out = {}
for sub in ("1.01.02", "1.10.94"):
    r = C.compare("data/text_layer_v2_dev/15v-a", "data/text_layer_v2_dev/15w-a", sub, sub)
    other = [m for m in r["missing"] if C.inv_of(Path(m[2:])) not in SHARED or C.inv_of(Path(m[2:])) is None and False]
    keep = [m for m in r["missing"] if m not in other]
    out[sub] = {"equal": r["equal"], "differ": r["differ"], "missing_shared_or_corpus": keep,
                "n_missing_other_invs": len(other)}
    print(f"== {sub}: {r['equal']} equal, {len(r['differ'])} differ, {len(keep)} missing (shared/corpus), "
          f"{len(other)} files of other inventories dropped")
    for f, d in r["differ"].items():
        print("  differ", f, json.dumps(d)[:300])
    for m in keep:
        print("  missing", m)
Path("reports/data/phase-12-hand-print-box-height/step-15w/compare16.json").write_text(json.dumps(out, indent=1))
