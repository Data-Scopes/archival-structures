"""Compare two layer bases table by table (phase 09; copy for phase 12 step 15v, with --sub-map,
--ignore-paths and --explained-invs added).

    .venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/compare.py \
        data/text_layer_v2_dev/15ir-a data/text_layer_v2_dev/15j-a [--sub hand,print] [--out F.json]

For each sub-layer, every file under the two roots (matched by relative path; the root names may
differ, e.g. `_v2` and `_v3`) except `manifest.json`, `progress.log` and `status.tsv` is compared on
decompressed bytes. For a differing `.tsv(.gz)` table it also reports the row counts, added and
dropped columns, and per common column the number of rows whose value differs (rows aligned by
position when the counts match, else only the column lists are reported). JSON files are compared
with the keys `seconds` and `elapsed*` removed.
"""
import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, "reports/data/phase-09-text-layer-v3-prep/planning")
from common import layer_root, raw_bytes, read  # noqa: E402

SKIP = {"manifest.json", "progress.log", "status.tsv"}


def _json_norm(b):
    def strip(o):
        if isinstance(o, dict):
            return {k: strip(v) for k, v in o.items() if k != "seconds" and not k.startswith("elapsed")}
        if isinstance(o, list):
            return [strip(v) for v in o]
        return o
    return strip(json.loads(b))


def table_diff(pa, pb):
    A, B = read(pa), read(pb)
    out = {"rows": [len(A), len(B)], "added_cols": [c for c in B.columns if c not in A.columns],
           "dropped_cols": [c for c in A.columns if c not in B.columns]}
    if len(A) == len(B):
        out["cols_differing_rows"] = {c: int((A[c].values != B[c].values).sum())
                                      for c in A.columns if c in B.columns
                                      if (A[c].values != B[c].values).any()}
    return out


def inv_of(f):
    """Inventory number of a per-inventory file (first digit run in the file name), else None."""
    m = re.match(r"(\d+)(?:[.\-_]|$)", f.name)
    return m.group(1) if m else None


def compare(base_a, base_b, sub_a, sub_b, ignore=(), explained=()):
    ra, rb = layer_root(base_a, sub_a), layer_root(base_b, sub_b)
    files = sorted({p.relative_to(ra) for p in ra.rglob("*") if p.is_file()}
                   | {p.relative_to(rb) for p in rb.rglob("*") if p.is_file()})
    res = {"equal": 0, "differ": {}, "missing": [], "ignored": 0, "explained": {}}
    for f in files:
        if f.name in SKIP:
            continue
        if any(str(f).startswith(p) for p in ignore):
            res["ignored"] += 1
            continue
        pa, pb = ra / f, rb / f
        if not pa.exists() or not pb.exists():
            res["missing"].append(f"{'A' if not pa.exists() else 'B'}:{f}")
            continue
        a, b = raw_bytes(pa), raw_bytes(pb)
        if f.suffix == ".json":
            same = _json_norm(a) == _json_norm(b)
        else:
            same = a == b
        if same:
            res["equal"] += 1
        elif inv_of(f) in explained:
            res["explained"].setdefault(inv_of(f), []).append(str(f))
        elif ".tsv" in f.name:
            res["differ"][str(f)] = table_diff(pa, pb)
        else:
            res["differ"][str(f)] = "bytes"
    return res


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("base_a")
    ap.add_argument("base_b")
    ap.add_argument("--sub-map", default="hand:hand,print:print", help="A:B,... sub-layer A of base_a vs B of base_b")
    ap.add_argument("--ignore-paths", default="", help="path prefixes (counted, not compared)")
    ap.add_argument("--explained-invs", default="", help="inventories whose differing files are counted, not listed")
    ap.add_argument("--out", default="")
    a = ap.parse_args(argv)
    ign = tuple(p for p in a.ignore_paths.split(",") if p)
    exp = set(i for i in a.explained_invs.split(",") if i)
    res = {}
    for pair in a.sub_map.split(","):
        sa, sb = pair.split(":")
        res[pair] = compare(a.base_a, a.base_b, sa, sb, ign, exp)
    for sub, r in res.items():
        print(f"== {sub}: {r['equal']} equal, {len(r['differ'])} differ, {len(r['missing'])} missing, "
              f"{r['ignored']} ignored, explained {{inv: n files}} = "
              f"{ {k: len(v) for k, v in sorted(r['explained'].items())} }")
        for f, d in r["differ"].items():
            print("  ", f, d)
        for f in r["missing"]:
            print("   missing", f)
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
