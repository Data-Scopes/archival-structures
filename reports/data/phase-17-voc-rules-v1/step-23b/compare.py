"""Step 23b: compare the phase-16 layer (22a, v5) with the v6 layer. Single process; writes compare.json.

    .venv/bin/python -I reports/data/phase-17-voc-rules-v1/step-23b/compare.py [--old DIR] [--new DIR] [--out FILE]
"""
import argparse
import collections
import gzip
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import CLASSES, NEW, NOT, OLD, UP_DIRS, classify_nots, inventories, rd  # noqa: E402


def sha(p):
    if not p.exists():
        return None
    data = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
    return hashlib.sha256(data).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", type=Path, default=OLD)
    ap.add_argument("--new", type=Path, default=NEW)
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "compare.json")
    a = ap.parse_args()
    invs_old, invs_new = inventories(a.old), inventories(a.new)
    res = {"inventories_old": len(invs_old), "inventories_new": len(invs_new),
           "inventory_sets_equal": invs_old == invs_new}
    # (a) upstream identical
    diffs = collections.Counter()
    diff_examples = []
    nfiles = 0
    for d in UP_DIRS:
        names = sorted({str(p.relative_to(a.old / d)) for p in (a.old / d).rglob("*") if p.is_file()}
                       | {str(p.relative_to(a.new / d)) for p in (a.new / d).rglob("*") if p.is_file()})
        for n in names:
            nfiles += 1
            if sha(a.old / d / n) != sha(a.new / d / n):
                diffs[d] += 1
                diff_examples.append(f"{d}/{n}")
    res["a_upstream"] = {"files_compared": nfiles, "differences": sum(diffs.values()),
                         "by_dir": dict(diffs), "examples": diff_examples[:10]}
    # (b) join changes
    n_units = n_changed = n_changed_no_not = n_unit_set_diff = 0
    old_not = new_not = 0
    changed_no_not_ex = []
    kinds = collections.Counter()
    cls = collections.Counter()
    other_ex = []
    for inv in invs_new:
        ou = {r["unit_id"]: r for r in rd(a.old / "join" / "units" / f"{inv}.tsv.gz")}
        nu = {r["unit_id"]: r for r in rd(a.new / "join" / "units" / f"{inv}.tsv.gz")}
        if ou.keys() != nu.keys():
            n_unit_set_diff += 1
        segs = collections.defaultdict(list)
        for s in rd(a.new / "join" / "segments" / f"{inv}.tsv.gz"):
            segs[s["unit_id"]].append(s)
        for uid, r in nu.items():
            n_units += 1
            t = r["text"]
            new_not += t.count(NOT)
            if t.count(NOT):
                for i, c, s in classify_nots(t, sorted(segs[uid], key=lambda s: int(s["seg_order"]))):
                    cls[c] += 1
                    if s is not None:
                        kinds[(c, s["boundary_kind"])] += 1
                    if c == "other" and len(other_ex) < 10:
                        other_ex.append([inv, uid, i, t[max(0, i - 30):i + 30]])
            o = ou.get(uid)
            if o is None:
                continue
            old_not += o["text"].count(NOT)
            if o["text"] != t:
                n_changed += 1
                if NOT not in o["text"]:
                    n_changed_no_not += 1
                    if len(changed_no_not_ex) < 10:
                        changed_no_not_ex.append([inv, uid])
    res["b_join"] = {"units": n_units, "units_with_set_difference_inventories": n_unit_set_diff,
                     "changed_units": n_changed, "changed_units_without_not_in_old_text": n_changed_no_not,
                     "examples": changed_no_not_ex}
    res["c_remaining_not"] = {"v5_22a_not_count": old_not, "v6_not_count": new_not,
                              "classes": {c: cls[c] for c in CLASSES},
                              "by_class_and_boundary_kind": {f"{c}/{k}": v for (c, k), v in sorted(kinds.items())},
                              "other_examples": other_ex}
    args = a.out
    args.write_text(json.dumps(res, indent=1, ensure_ascii=False))
    print(json.dumps(res, indent=1, ensure_ascii=False)[:3000])


if __name__ == "__main__":
    main()
