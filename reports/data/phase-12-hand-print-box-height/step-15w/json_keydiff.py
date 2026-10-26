"""Step 15w: top-level (and one level down) keys that differ between 15v-a and 15w-a JSON files that compare16.py lists."""
import json
import sys
from pathlib import Path

R = "data/text_layer_v2_dev/%s/%s/NL-HaNA_%s_text_layer_v4/%s"


def load(tag, sub, rel):
    return json.loads(Path(R % (tag, sub, sub, rel)).read_text())


def diffkeys(a, b, prefix=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append(prefix + k + " (only " + ("A" if k in a else "B") + ")")
            elif a[k] != b[k]:
                sub = diffkeys(a[k], b[k], prefix + k + ".") if isinstance(a[k], dict) else [prefix + k]
                out += sub or [prefix + k]
    elif a != b:
        out.append(prefix.rstrip("."))
    return out


for sub, rels in (("1.01.02", ["extract_stats/3349.json", "extract_stats/3771.json", "extract_stats/11182.json",
                              "layout/stats.json", "types/stats.json", "units/stats.json", "units/params.json",
                              "wbc/corpus_default.json", "wbc/corpus_default_default.json", "wbc/corpus_default_robust.json"]),
                  ("1.10.94", ["extract_stats/455.json"])):
    for rel in rels:
        d = diffkeys(load("15v-a", sub, rel), load("15w-a", sub, rel))
        print(sub, rel, "->", d if len(d) < 14 else d[:14] + [f"... {len(d)} keys"])

print("--- per-inventory entries of the stats files, shared inventories, `seconds` ignored")
SHARED = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()


def strip(o):
    return {k: strip(v) for k, v in o.items() if k != "seconds"} if isinstance(o, dict) else o


for rel in ("layout/stats.json", "types/stats.json", "units/stats.json"):
    a, b = load("15v-a", "1.01.02", rel), load("15w-a", "1.01.02", rel)
    print(rel, "top-level keys A-only:", [k for k in a if k not in b][:5], "| shared diffs:",
          {i: diffkeys(strip(a[i]), strip(b[i])) for i in SHARED if i in a and strip(a[i]) != strip(b[i])})
a, b = load("15v-a", "1.01.02", "wbc/corpus_default.json"), load("15w-a", "1.01.02", "wbc/corpus_default.json")
for k in a:
    if not isinstance(a[k], (dict, list)) or k in ("default_set", "pinned", "derived_default_set", "pooled_rule"):
        print("wbc", k, a[k], "->", b[k])
