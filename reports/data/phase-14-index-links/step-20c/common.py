"""Shared loaders for the step-20c print tools (print only, no decisions)."""
import csv, re
from functools import lru_cache
from archival_structures.text_layer.io import read_tsv_gz

L = "data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5"
READS = "reports/data/phase-14-index-links/planning/reads_entries.tsv"


@lru_cache(None)
def scans(inv):
    return [r["scan_id"] for r in read_tsv_gz(f"{L}/scans/{inv}.scans.tsv.gz")]


@lru_cache(None)
def lines(inv):
    """scan_id -> list of dict(line_id, x, y, text, page, col, idx) from lines + layout/lines."""
    lay = {}
    for r in read_tsv_gz(f"{L}/layout/lines/{inv}.tsv.gz"):
        lay[(r["scan_id"], r["line_id"])] = r
    out = {}
    for r in read_tsv_gz(f"{L}/lines/{inv}.lines.tsv.gz"):
        k = (r["scan_id"], r["line_id"])
        a = lay.get(k, {})
        def i(v):
            try:
                return int(v)
            except Exception:
                return -1
        out.setdefault(r["scan_id"], []).append(dict(
            line_id=r["line_id"], x=i(r["x"]), y=i(r["y"]), h=i(r["h"]), text=r["text"].strip(),
            page=a.get("page", "?"), col=i(a.get("layout_column", "")), idx=i(a.get("layout_line_idx", ""))))
    return out


def reads():
    return list(csv.DictReader(open(READS), delimiter="\t"))
