"""Criteria 1-3 and the manifest/log error analysis for step 15c.

    check.py OUT_DIR           (OUT_DIR holds r22/ built by as-build-text-layer; see the report)
Prints: compare counts vs REF (110 TSV byte-equal, 44 JSON equal, 3 run files), the manifest diff
against REF, and the progress.log lines that are not per-inventory progress lines.
"""
import json, re, sys
from pathlib import Path
from archival_structures.text_layer import compare

NLP = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection")
REF = NLP / "output/voc_text_layer/voc_text_layer_v1"
out = Path(sys.argv[1]) / "r22" / "voc_text_layer_v1"
invs = (Path(sys.argv[1]) / "inv22.txt").read_text().strip().split(",")

fr = compare.compare_layers(out, REF, invs, kinds=tuple(compare.TSV_KINDS))
tsv = fr[fr.kind.isin(compare.TSV_KINDS)]
js = fr[fr.kind.isin(compare.JSON_KINDS)]
n_tsv = int((tsv.byte_equal == True).sum()); n_js = int((js.json_equal == True).sum())  # noqa: E712
print(f"TSV byte-equal {n_tsv}/{len(tsv)}  (per kind: {tsv.groupby('kind').byte_equal.apply(lambda s: int((s==True).sum())).to_dict()})")
print(f"JSON equal {n_js}/{len(js)}; JSON byte-equal {int((js.byte_equal == True).sum())}/{len(js)}")
rf = compare.compare_run_files(out, REF)
print("run files byte-equal", int((rf.byte_equal == True).sum()), "/", len(rf))
print("TOTAL", n_tsv + n_js + int((rf.byte_equal == True).sum()), "/", len(fr) + len(rf))
bad = fr[(fr.byte_equal != True) & (fr.json_equal != True)]
print(bad.to_string() if len(bad) else "no differing rows")

new = json.loads((out / "manifest.json").read_text()); ref = json.loads((REF / "manifest.json").read_text())
print("\nlayer-level keys equal:", {k: new[k] == ref[k] for k in ("layer", "row_order", "columns", "format", "untagged_region_tag")})
print("layer-level key sets equal:", set(new) == set(ref), set(new) ^ set(ref))
for st in ref["stages"]:
    n, r = new["stages"][st], ref["stages"][st]
    print(f"\n[{st}] ref⊆new: {set(r) <= set(n)}  new−ref: {sorted(set(n) - set(r))}")
    for k in sorted(r):
        if n.get(k) != r[k]:
            a, b = json.dumps(r[k])[:110], json.dumps(n.get(k))[:110]
            print(f"   differs {k}:\n      ref {a}\n      new {b}")

print("\nprogress.log lines that are not per-inventory progress lines:")
pat = re.compile(r"^\S+ \[(extract|wbc|join)\] \d+/\d+ \S+ rows=")
for line in (out / "progress.log").read_text().splitlines():
    if not pat.match(line):
        print("  ", line[:200])
