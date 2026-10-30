"""Shared loaders for the step-21a tools (print only; no labels are decided here)."""
import re
from archival_structures.text_layer.io import read_tsv_gz

L = ("data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/")
READS = "reports/data/phase-15-index-links-v2/planning/reads_sides.tsv"


def tab(kind, inv):
    p = {"lines": f"lines/{inv}.lines.tsv.gz", "layout": f"layout/lines/{inv}.tsv.gz",
         "units": f"units/units/{inv}.tsv.gz", "segs": f"units/segments/{inv}.tsv.gz",
         "scans": f"scans/{inv}.scans.tsv.gz"}[kind]
    return list(read_tsv_gz(L + p))


def read_row(read_id):
    with open(READS) as f:
        hdr = f.readline().rstrip("\n").split("\t")
        for ln in f:
            r = dict(zip(hdr, ln.rstrip("\n").split("\t")))
            if r["read_id"] == read_id:
                return r
    raise SystemExit(f"unknown read_id {read_id}")


def side_lines(inv, scan_id, page):
    """Lines of one side with non-empty text, in unit/segment order (unitless last by y)."""
    lay = {r["line_id"]: r for r in tab("layout", inv) if r["scan_id"] == scan_id}
    lines = {r["line_id"]: r for r in tab("lines", inv) if r["scan_id"] == scan_id}
    units = {r["unit_id"]: r for r in tab("units", inv) if r["scan_id"] == scan_id}
    segpos = {}
    for s in tab("segs", inv):
        if s["scan_id"] == scan_id:
            u = units.get(s["unit_id"])
            key = (int(u["unit_order"]), int(s["seg_order"]), u["unit_type"]) if u else None
            if s["line_id"] not in segpos or (key and key < segpos[s["line_id"]]):
                segpos[s["line_id"]] = key
    out = []
    for lid, lr in lay.items():
        if lr["page"] != page or lid not in lines:
            continue
        ln = lines[lid]
        if not ln["text"].strip():
            continue
        out.append((segpos.get(lid), lid, ln))
    inu = sorted([o for o in out if o[0]], key=lambda o: o[0][:2])
    nou = sorted([o for o in out if not o[0]], key=lambda o: float(o[2]["y"]))
    return inu + nou
