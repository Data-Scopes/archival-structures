"""Step 15fq2: compare 15g-b with 15g-a (types/{segments,regions}); read-only; seed 21 sample."""
import json, random
from collections import Counter
from pathlib import Path
import pandas as pd

P = Path("data/text_layer_v2_dev")
LAYERS = {"hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
          "hand_local": ("NL-HaNA_1.01.02_text_layer_v2", ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
          "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"])}


def rd(layer, sub, name, kind, inv):
    return pd.read_csv(P / layer / sub / name / "types" / kind / f"{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)


out = {}
for sub, (name, invs) in LAYERS.items():
    for inv in invs:
        r = {}
        for kind in ("segments", "regions"):
            a, b = rd("15g-a", sub, name, kind, inv), rd("15g-b", sub, name, kind, inv)
            r[kind + "_rows_a"], r[kind + "_rows_b"] = len(a), len(b)
            r[kind + "_equal"] = bool(a.equals(b))
            if not a.equals(b):
                key = [c for c in ("scan_id", "line_id", "seg_idx") if c in a.columns] or list(a.columns[:2])
                ka = set(map(tuple, a.astype(str).values)); kb = set(map(tuple, b.astype(str).values))
                r[kind + "_rows_only_a"], r[kind + "_rows_only_b"] = len(ka - kb), len(kb - ka)
            if kind == "segments":
                tc = [c for c in a.columns if "type" in c][0]
                r["type_col"] = tc
                r["types_a"], r["types_b"] = dict(Counter(a[tc])), dict(Counter(b[tc]))
        out[inv] = r
Path("reports/data/phase-07-sg-units-and-pilot/step-15fq2/compare_types.json").write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
for inv, r in out.items():
    print(inv, {k: v for k, v in r.items() if not k.startswith("types_")})
print(inv, r["types_a"], r["types_b"])
