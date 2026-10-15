"""Order-insensitive comparison of a local-input rebuild with v1 (planning finding)."""
import gzip, sys
from collections import defaultdict
from pathlib import Path
V1 = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1")
def rows(p): return [l.split("\t") for l in gzip.decompress(p.read_bytes()).decode().split("\n")[1:] if l]
tot = defaultdict(int)
seen = set()
for out in sys.argv[1:]:
    for p in sorted(Path(out, "lines").glob("*.lines.tsv.gz")):
        inv = p.name.split(".")[0]
        if inv in seen:
            continue  # an inventory rebuilt in two runs counts once
        seen.add(inv)
        a, b = rows(p), rows(V1 / "lines" / p.name)
        la, lb = defaultdict(list), defaultdict(list)
        for r in a: la[(r[0], r[1])].append((r[3], r[4]))
        for r in b: lb[(r[0], r[1])].append((r[3], r[4]))
        ra = {(r[0], r[1]): r[4] for r in rows(Path(out, "regions", f"{inv}.tsv.gz"))}
        rb = {(r[0], r[1]): r[4] for r in rows(V1 / "regions" / f"{inv}.tsv.gz")}
        ma = {(r[0], r[1]): r[2:] for r in rows(Path(out, "regions_meta", f"{inv}.tsv.gz"))}
        mb = {(r[0], r[1]): r[2:] for r in rows(V1 / "regions_meta" / f"{inv}.tsv.gz")}
        tot["inventories"] += 1; tot["regions"] += len(la) if la else len(ma)
        tot["line_multiset_equal_inv"] += all(sorted(la[k]) == sorted(lb.get(k, [])) for k in set(la) | set(lb))
        tot["region_keys_equal_inv"] += set(ma) == set(mb)
        tot["meta_values_equal_inv"] += ma == mb
        tot["regions_line_order_differs"] += sum(la[k] != lb.get(k) for k in la)
        tot["region_text_differs"] += sum(ra[k] != rb.get(k) for k in ra)
        tot["region_order_differs_inv"] += list(ma) != list(mb)
        # does region text differ only where line order differs?
        tot["text_diff_without_order_diff"] += sum(ra[k] != rb.get(k) and la.get(k) == lb.get(k) for k in ra)
print(dict(tot))
