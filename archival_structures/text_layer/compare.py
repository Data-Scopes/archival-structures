"""Compare two text layers file by file (step 15c).

``left`` and ``right`` are layer roots (the directories holding ``lines/``). Every comparison
is by decompressed text as well as bytes, so a gzip-header difference is told apart from a
content difference, and a row-order difference from a content difference. No pagexml import; pandas is imported lazily.

    python -m archival_structures.text_layer.compare LEFT RIGHT [--inventories a,b] [--out TSV]
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

import numpy as np

from archival_structures.text_layer.io import inventory_sort_key

TSV_KINDS = {"lines": "lines/{inv}.lines.tsv.gz", "regions_meta": "regions_meta/{inv}.tsv.gz",
             "regions": "regions/{inv}.tsv.gz", "join_events": "join_events/{inv}.tsv.gz",
             "wbc_parts": "wbc/parts/{inv}.tsv"}
JSON_KINDS = {"extract_stats": "extract_stats/{inv}.json", "join_stats": "join_stats/{inv}.json"}
RUN_FILES = ("wbc/wbc_stats.tsv", "wbc/wbc_per_inventory.tsv", "wbc/corpus_default.json")
COLUMNS = ["inventory_number", "kind", "exists_left", "exists_right", "byte_equal", "rows_left",
           "rows_right", "content_equal", "multiset_equal", "order_only", "json_equal"]
BOOLEANS = ["exists_left", "exists_right", "byte_equal", "content_equal", "multiset_equal",
            "order_only", "json_equal"]


def _read_text(path):
    raw = Path(path).read_bytes()
    if str(path).endswith(".gz"):
        raw = gzip.decompress(raw)
    return raw.decode("utf-8")


def _rows(text):
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _compare_tsv(left, right):
    import pandas as pd
    row = {"exists_left": left.exists(), "exists_right": right.exists(), "byte_equal": pd.NA,
           "rows_left": pd.NA, "rows_right": pd.NA, "content_equal": pd.NA,
           "multiset_equal": pd.NA, "order_only": pd.NA, "json_equal": pd.NA}
    if left.exists():
        row["rows_left"] = max(0, len(_rows(_read_text(left))) - 1)
    if right.exists():
        row["rows_right"] = max(0, len(_rows(_read_text(right))) - 1)
    if left.exists() and right.exists():
        text_l, text_r = _read_text(left), _read_text(right)
        row["byte_equal"] = left.read_bytes() == right.read_bytes()
        row["content_equal"] = text_l == text_r
        rows_l, rows_r = _rows(text_l), _rows(text_r)
        row["multiset_equal"] = (rows_l[:1] == rows_r[:1]
                                 and sorted(rows_l[1:]) == sorted(rows_r[1:]))
        row["order_only"] = bool(row["multiset_equal"] and not row["content_equal"])
    return row


def _load_json(path, ignore):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = {k: v for k, v in data.items() if k not in ignore}
    return data


def _compare_json(left, right, ignore):
    import pandas as pd
    row = {"exists_left": left.exists(), "exists_right": right.exists(), "byte_equal": pd.NA,
           "rows_left": pd.NA, "rows_right": pd.NA, "content_equal": pd.NA,
           "multiset_equal": pd.NA, "order_only": pd.NA, "json_equal": pd.NA}
    if left.exists() and right.exists():
        row["byte_equal"] = left.read_bytes() == right.read_bytes()
        row["json_equal"] = _load_json(left, ignore) == _load_json(right, ignore)
    return row


def _stems(root, suffix):
    return {p.name[:-len(suffix)] for p in (Path(root) / "lines").glob(f"*{suffix}")}


def compare_layers(left, right, inventories=None, *, kinds=("lines", "regions_meta", "regions"),
                   json_kinds=("extract_stats", "join_stats"), ignore_json_keys=("source",)):
    """One row per inventory and kind. Defaults to inventories present in ``lines/`` on both sides."""
    import pandas as pd
    left, right = Path(left), Path(right)
    if inventories is None:
        suffix = ".lines.tsv.gz"
        inventories = sorted(_stems(left, suffix) & _stems(right, suffix), key=inventory_sort_key)
    rows = []
    for inv in inventories:
        for kind in kinds:
            rel = TSV_KINDS[kind].format(inv=inv)
            rows.append({"inventory_number": inv, "kind": kind,
                         **_compare_tsv(left / rel, right / rel)})
        for kind in json_kinds:
            rel = JSON_KINDS[kind].format(inv=inv)
            rows.append({"inventory_number": inv, "kind": kind,
                         **_compare_json(left / rel, right / rel, ignore_json_keys)})
    return pd.DataFrame(rows, columns=COLUMNS)


def compare_run_files(left, right, files=RUN_FILES):
    """The run-level files (wbc/...). One row per file; ``inventory_number`` holds the path."""
    import pandas as pd
    rows = []
    for rel in files:
        l, r = Path(left) / rel, Path(right) / rel
        if rel.endswith(".json"):
            row = _compare_json(l, r, ())
        else:
            row = _compare_tsv(l, r)
        rows.append({"inventory_number": rel, "kind": "run_file", **row})
    return pd.DataFrame(rows, columns=COLUMNS)


def summarise(frame):
    """{kind: {column: count of True, 'n': rows}} for the boolean columns."""
    out = {}
    for kind, sub in frame.groupby("kind", sort=False):
        entry = {"n": int(len(sub))}
        for col in BOOLEANS:
            entry[col] = int((sub[col] == True).sum())  # noqa: E712 -- NA-safe
        out[kind] = entry
    return out


def _true(value):
    """True only for a real True (pd.NA and False are both not)."""
    return isinstance(value, (bool, np.bool_)) and bool(value)


def _all_ok(frame):
    """Every TSV byte-equal, every JSON equal."""
    for _, r in frame.iterrows():
        is_json = r["kind"] in JSON_KINDS or str(r["inventory_number"]).endswith(".json")
        if not _true(r["json_equal"] if is_json else r["byte_equal"]):
            return False
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compare two text layers file by file.")
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--inventories", help="comma-separated inventory numbers")
    group.add_argument("--inventories-file", help="file with one inventory number per line")
    parser.add_argument("--kinds", default="lines,regions_meta,regions",
                        help="comma-separated TSV kinds (%s)" % ",".join(TSV_KINDS))
    parser.add_argument("--out", type=Path, help="write the comparison frame as TSV")
    args = parser.parse_args(argv)
    inventories = None
    if args.inventories:
        inventories = [s.strip() for s in args.inventories.split(",") if s.strip()]
    elif args.inventories_file:
        inventories = Path(args.inventories_file).read_text().split()
    kinds = tuple(k for k in args.kinds.split(",") if k)
    frame = compare_layers(args.left, args.right, inventories, kinds=kinds)
    summary = summarise(frame)
    for kind, entry in summary.items():
        print(kind, entry)
    ok = bool(len(frame)) and _all_ok(frame)
    print("IDENTICAL" if ok else "DIFFERENT", f"({len(frame)} comparisons)")
    if args.out:
        frame.to_csv(args.out, sep="\t", index=False)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
