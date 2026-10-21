"""Step 15fq2: per-segment print type changes 15g-a -> 15g-b (key scan_id, line_id, seg_idx); seed 21 sample."""
import json, random
from collections import Counter
import pandas as pd

P = "data/text_layer_v2_dev/%s/print/NL-HaNA_1.10.94_text_layer_v2/"
res = {}
rows = []
for inv in ("455", "456"):
    a = pd.read_csv(P % "15g-a" + f"types/segments/{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    b = pd.read_csv(P % "15g-b" + f"types/segments/{inv}.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    src = pd.read_csv(P % "15g-b" + f"lines/{inv}.lines.tsv.gz", sep="\t", dtype=str, keep_default_na=False,
                      usecols=["scan_id", "line_id", "text", "column"])
    k = ["scan_id", "line_id", "seg_idx"]
    m = a.merge(b, on=k, how="outer", suffixes=("_a", "_b"), indicator=True)
    res[inv] = {"key_only_a": int((m._merge == "left_only").sum()), "key_only_b": int((m._merge == "right_only").sum()),
                "type_changed": int(((m._merge == "both") & (m.line_type_a != m.line_type_b)).sum()),
                "method_changed": int(((m._merge == "both") & (m.type_method_a != m.type_method_b)).sum()),
                "region_changed": int(((m._merge == "both") & (m.layout_region_id_a != m.layout_region_id_b)).sum()),
                "types_a": dict(Counter(a.line_type)), "types_b": dict(Counter(b.line_type)),
                "change_pairs": {f"{x}->{y}": int(n) for (x, y), n in
                                 m[(m._merge == "both") & (m.line_type_a != m.line_type_b)]
                                 .groupby(["line_type_a", "line_type_b"]).size().items()}}
    ch = m[(m._merge == "both") & (m.line_type_a != m.line_type_b)]
    ch = ch.merge(src, on=["scan_id", "line_id"], how="left")
    ch["inv"] = inv
    rows.append(ch)
ch = pd.concat(rows)
print(json.dumps(res, indent=1))
rng = random.Random(21)
idx = sorted(rng.sample(range(len(ch)), min(10, len(ch))))
for i in idx:
    r = ch.iloc[i]
    print(r.inv, r.scan_id[-4:], f"col={r['column']}", f"{r.line_type_a}[{r.type_method_a}] -> {r.line_type_b}[{r.type_method_b}]",
          "|", r.text[r.char_start_b and int(r.char_start_b):int(r.char_end_b)][:60] if False else r.text[:60])
json.dump(res, open("reports/data/phase-07-sg-units-and-pilot/step-15fq2/compare_print.json", "w"), indent=1, sort_keys=True)
