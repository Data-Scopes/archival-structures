"""Step 15 error analysis: rebuilt layer (local 2023 zips) vs v1 (v2.1), order ignored.
Usage: .venv/bin/python -I ea.py <out_dir>   (reads <out_dir>/ref and <out_dir>/ab_as, built by check.py)
Writes <out_dir>/ea_summary.json and <out_dir>/ea_samples.txt.
"""
import json, random, sys
from collections import Counter, defaultdict
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

V1 = Path("/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/data/voc_text_layer_v1")
OUT = Path(sys.argv[1])
ROOTS = {}
for sub in ("ref", "ab_as"):
    for p in (OUT / sub / "lines").glob("*.lines.tsv.gz"):
        ROOTS[p.name[:-len(".lines.tsv.gz")]] = OUT / sub


def load(root, inv):
    lines = defaultdict(list)
    for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        lines[(r["scan_id"], r["region_id"])].append((r["line_id"], r["line_text"]))
    meta = {}
    order = []
    for r in read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz"):
        k = (r["scan_id"], r["region_id"])
        meta[k] = (r["region_box"], r["region_tag"], r["n_lines"])
        order.append(k)
    text = {(r["scan_id"], r["region_id"]): r["region_text"]
            for r in read_tsv_gz(root / "regions" / f"{inv}.tsv.gz")}
    return lines, meta, order, text


def one_moved(a, b):
    if len(a) != len(b) or a == b:
        return False
    for i in range(len(a)):
        a2 = a[:i] + a[i + 1:]
        for j in range(len(b)):
            if a2 == b[:j] + b[j + 1:]:
                return True
    return False


tot = Counter()
per_inv = {}
diff_regions = []   # (inv, key, v1 lines, ours lines)
region_order_diff_scans = []
for inv, root in sorted(ROOTS.items()):
    l1, m1, o1, t1 = load(V1, inv)
    l2, m2, o2, t2 = load(root, inv)
    d = {}
    d["line_multiset_equal"] = all(Counter(l1.get(k, [])) == Counter(l2.get(k, [])) for k in set(l1) | set(l2))
    d["region_keys_equal"] = set(m1) == set(m2)
    d["meta_equal"] = m1 == m2
    d["region_order_differs"] = o1 != o2
    d["scan_order_differs"] = list(dict.fromkeys(k[0] for k in o1)) != list(dict.fromkeys(k[0] for k in o2))
    n_order = n_text = n_text_in_order = 0
    for k in m1:
        order_diff = l1.get(k, []) != l2.get(k, [])
        text_diff = t1.get(k) != t2.get(k)
        n_order += order_diff
        n_text += text_diff
        n_text_in_order += text_diff and order_diff
        if order_diff:
            diff_regions.append((inv, k, l1.get(k, []), l2.get(k, [])))
    d.update(n_regions=len(m1), regions_line_order_differs=n_order, texts_differ=n_text,
             texts_differ_in_order_diff_regions=n_text_in_order)
    # scans whose region order differs
    by1, by2 = defaultdict(list), defaultdict(list)
    for k in o1: by1[k[0]].append(k[1])
    for k in o2: by2[k[0]].append(k[1])
    for s in by1:
        if by1[s] != by2.get(s) and sorted(by1[s]) == sorted(by2.get(s, [])):
            region_order_diff_scans.append((inv, s, by1[s], by2[s], m1))
    d["scans_region_order_differs"] = sum(1 for x in region_order_diff_scans if x[0] == inv)
    per_inv[inv] = d
    for k in ("n_regions", "regions_line_order_differs", "texts_differ", "texts_differ_in_order_diff_regions"):
        tot[k] += d[k]

summ = {"n_inventories": len(per_inv),
        "line_multiset_equal": sum(d["line_multiset_equal"] for d in per_inv.values()),
        "region_keys_equal": sum(d["region_keys_equal"] for d in per_inv.values()),
        "meta_equal": sum(d["meta_equal"] for d in per_inv.values()),
        "region_order_differs": sum(d["region_order_differs"] for d in per_inv.values()),
        "scan_order_differs": sum(d["scan_order_differs"] for d in per_inv.values()),
        "totals": dict(tot), "n_scans_region_order_differs": len(region_order_diff_scans),
        "per_inventory": per_inv}
cat = Counter()
rng = random.Random(15)
sample = rng.sample(diff_regions, min(10, len(diff_regions)))
lines_out = []
for inv, k, a, b in diff_regions:
    cat["one_moved" if one_moved([x[0] for x in a], [x[0] for x in b]) else "permuted_or_other"] += 1
summ["line_order_diff_auto_classes_all"] = dict(cat)
for inv, k, a, b in sample:
    lines_out.append(f"=== {inv} {k} n_lines={len(a)} one_moved={one_moved([x[0] for x in a],[x[0] for x in b])}")
    idx = [i for i in range(len(a)) if a[i] != b[i]]
    lo, hi = max(0, idx[0] - 1), min(len(a), idx[-1] + 2, max(0, idx[0] - 1) + 14)
    lines_out.append(f"  differing positions {idx[0]}..{idx[-1]} ({len(idx)} of {len(a)}); window {lo}..{hi}")
    lines_out.append("  v1 order (line_id: text):")
    for x in a[lo:hi]: lines_out.append(f"    {x[0][-6:]}: {x[1][:90]}")
    lines_out.append("  rebuilt order:")
    for x in b[lo:hi]: lines_out.append(f"    {x[0][-6:]}: {x[1][:90]}")
rng2 = random.Random(15)
for inv, s, a, b, m1 in rng2.sample(region_order_diff_scans, min(5, len(region_order_diff_scans))):
    lines_out.append(f"=== SCAN {s}  regions v1: {len(a)}")
    lines_out.append("  v1     : " + " ".join(f"{r}[{m1[(s, r)][1][:4]}]" for r in a[:14]))
    lines_out.append("  rebuilt: " + " ".join(f"{r}[{m1[(s, r)][1][:4]}]" for r in b[:14]))
(OUT / "ea_summary.json").write_text(json.dumps(summ, indent=1))
(OUT / "ea_samples.txt").write_text("\n".join(lines_out))
print({k: v for k, v in summ.items() if k != "per_inventory"})

# top-to-bottom monotonicity of paragraph-region top-y within a scan, v1 vs rebuilt
mono = Counter()
for inv, root in sorted(ROOTS.items()):
    for name, r in (("v1", V1), ("rebuilt", root)):
        ys = defaultdict(list)
        for x in read_tsv_gz(r / "regions_meta" / f"{inv}.tsv.gz"):
            if x["region_tag"] == "paragraph" and x["region_box"]:
                ys[x["scan_id"]].append(int(x["region_box"].split("-")[1]))
        for s, v in ys.items():
            if len(v) >= 2:
                mono[f"{name}_scans"] += 1
                mono[f"{name}_monotone"] += v == sorted(v)
print("paragraph top-y monotone within scan:", dict(mono))
