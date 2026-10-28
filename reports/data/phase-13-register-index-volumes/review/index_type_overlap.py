import json, sys, pandas as pd
from collections import Counter
base = "data/text_layer_v2_dev/{}/1.01.02/NL-HaNA_1.01.02_text_layer_v4"
P = "reports/data/phase-13-register-index-volumes/planning/"
tot = Counter(); per = []
for layer, lst in (("19c-a", "hand_15y-b.txt"), ("19c-z", "hand_15z-b.txt")):
    root = base.format(layer)
    for inv in open(P + lst).read().split():
        seg = pd.read_csv(f"{root}/types/segments/{inv}.tsv.gz", sep="\t", usecols=["scan_id", "page", "line_type"], dtype=str)
        pg = pd.read_csv(f"{root}/layout/pages/{inv}.tsv.gz", sep="\t", usecols=["scan_id", "page", "index_side"], dtype=str)
        idx_sides = set(map(tuple, seg[seg.line_type == "index"][["scan_id", "page"]].drop_duplicates().values))
        flag = set(map(tuple, pg[pg.index_side == "1"][["scan_id", "page"]].values))
        main_on_flag = seg[(seg.line_type == "main_text") & seg.set_index(["scan_id","page"]).index.isin(list(flag))].shape[0]
        idx_on_flag = seg[(seg.line_type == "index") & seg.set_index(["scan_id","page"]).index.isin(list(flag))].shape[0]
        c = Counter(index_typed_sides=len(idx_sides), flagged=len(flag), both=len(idx_sides & flag),
                    index_typed_not_flagged=len(idx_sides - flag), index_segs=int((seg.line_type == "index").sum()),
                    main_segs_on_flagged=main_on_flag, index_segs_on_flagged=idx_on_flag)
        tot.update(c)
        if c["index_typed_sides"]:
            per.append((inv, dict(c)))
print(dict(tot))
print("volumes with index-typed sides:", len(per))
for inv, c in sorted(per, key=lambda x: -x[1]["index_typed_sides"])[:25]:
    print(inv, c)
