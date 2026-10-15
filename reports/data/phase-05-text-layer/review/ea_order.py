"""Review EA: 4 fresh regions (seed 505) whose line order differs, rebuilt (2023 zips) vs v1."""
import random, sys
from archival_structures.text_layer.io import read_region_lines
V1 = "/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1"
REB = sys.argv[1]
INV = "1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834".split()
diff = []
n_reg = 0
for inv in INV:
    a = {(m["scan_id"], m["region_id"]): t for m, t in read_region_lines(V1, inv)}
    b = {(m["scan_id"], m["region_id"]): t for m, t in read_region_lines(REB, inv)}
    assert a.keys() == b.keys(), inv
    n_reg += len(a)
    diff += [(inv, k, a[k], b[k]) for k in a if a[k] != b[k] and len(a[k]) <= 14]
    assert all(sorted(a[k]) == sorted(b[k]) for k in a)
print("regions", n_reg, "line order differs (<=14 lines)", len(diff))
for inv, k, va, vb in random.Random(505).sample(diff, 4):
    print("==", inv, k[0][-8:], k[1][-12:])
    print("  v1 :", " | ".join(t[:28] for t in va))
    print("  reb:", " | ".join(t[:28] for t in vb))
