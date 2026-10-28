"""Step 15y acceptance 2: which keys of the corpus-level JSON files differ between 15x-a and 15y-b.
    .venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15y/corpus_check.py"""
import json
A = "data/text_layer_v2_dev/15x-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4/"
B = "data/text_layer_v2_dev/15y-b/1.01.02/NL-HaNA_1.01.02_text_layer_v4/"


def load(root, f):
    return json.load(open(root + f))


for f in ("layout", "types", "units"):
    a, b = load(A, f + "/stats.json"), load(B, f + "/stats.json")
    print(f"{f}/stats.json: entries that differ:", sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k)))
for f in ("corpus_default", "corpus_default_default", "corpus_default_robust"):
    a, b = load(A, f"wbc/{f}.json"), load(B, f"wbc/{f}.json")
    print(f"wbc/{f}.json: keys that differ:", sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k)),
          "| default_set equal:", a.get("default_set") == b.get("default_set"),
          "| pinned equal:", a.get("pinned") == b.get("pinned"))
