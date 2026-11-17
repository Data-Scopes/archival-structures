"""Page element types: one record per text-layer line with the layer's own calls, mapped to the vocabulary.

Phase 18 step 24a. Reads a text-layer inventory (tables `lines/`, `layout/lines/`, `types/segments/`,
`regions_meta/`; schema in `docs/text_layer.md`) and gives, per non-empty line, the line type the layer's type stage
called, the PageXML region tag, and the layout role. `layer_call` maps these to vocabulary tags
(`archival_structures.datasets.vocabulary`); `listlike_records` builds the input of
`archival_structures.analysis.listlike`. Lines are keyed on (scan_id, line_id): line ids repeat across scans in AsdSAA.
"""
from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

LINE_TYPE_TO_GENERIC = {"main_text": "generic:running_text", "page_number": "generic:numbering:page",
    "running_header": "generic:running_title", "date_header": "generic:running_title:date",
    "catch_word": "generic:quire_marks:catchwords", "signature_mark": "generic:quire_marks:signature",
    "marginalia": None, "index": "generic:list", "other": "generic:custom"}
# PageXML region tags (Laypa, VOC) -> generic type, for a reported baseline only
REGION_TAG_TO_GENERIC = {"paragraph": "generic:running_text", "header": "generic:heading",
    "catch-word": "generic:quire_marks:catchwords", "page-number": "generic:numbering:page",
    "signature-mark": "generic:quire_marks:signature", "marginalia": None, "untagged": None}
REGION_TAG_PLACEMENT = {"marginalia": "margin"}
DATE_COMPATIBLE = frozenset({"heading", "running_title"})  # a date_header call is right for either family when the gold subtype is date


def _table(layer_root, kind, inv):
    root = Path(layer_root)
    return {"lines": root / "lines" / f"{inv}.lines.tsv.gz",
            "layout": root / "layout" / "lines" / f"{inv}.tsv.gz",
            "segments": root / "types" / "segments" / f"{inv}.tsv.gz",
            "regions": root / "regions_meta" / f"{inv}.tsv.gz"}[kind]


def read_layer_lines(layer_root, inv, scan_ids=None) -> list:
    """One dict per non-empty line of `lines/<inv>.lines.tsv.gz`, in that file's order, optionally only for `scan_ids`
    (a set). Keys: scan_id, line_id, region_id, page_side, x, y, w, h (ints), text, layout_region_id, layout_column,
    layout_role, page, line_type, type_method, n_segments, region_tag. line_type/type_method come from the segment with
    the most characters (char_end - char_start; tie: lowest seg_idx). Joins on (scan_id, line_id) and, for region_tag,
    (scan_id, region_id). Raises ValueError on a duplicate (scan_id, line_id), on a line without a layout row, or on a
    non-empty line without a types row."""
    wanted = None if scan_ids is None else set(scan_ids)
    layout = {}
    for r in read_tsv_gz(_table(layer_root, "layout", inv)):
        if wanted is None or r["scan_id"] in wanted:
            layout[(r["scan_id"], r["line_id"])] = r
    best = {}
    count = {}
    for r in read_tsv_gz(_table(layer_root, "segments", inv)):
        if wanted is not None and r["scan_id"] not in wanted:
            continue
        key = (r["scan_id"], r["line_id"])
        count[key] = count.get(key, 0) + 1
        rank = (-(int(r["char_end"]) - int(r["char_start"])), int(r["seg_idx"]))
        if key not in best or rank < best[key][0]:
            best[key] = (rank, r)
    tags = {}
    for r in read_tsv_gz(_table(layer_root, "regions", inv)):
        if wanted is None or r["scan_id"] in wanted:
            tags[(r["scan_id"], r["region_id"])] = r["region_tag"]
    out, seen = [], set()
    for r in read_tsv_gz(_table(layer_root, "lines", inv)):
        if wanted is not None and r["scan_id"] not in wanted:
            continue
        key = (r["scan_id"], r["line_id"])
        if key in seen:
            raise ValueError(f"duplicate (scan_id, line_id) {key} in {inv}")
        seen.add(key)
        if key not in layout:
            raise ValueError(f"line {key} of {inv} has no layout row")
        if not r["text"].strip():
            continue
        if key not in best:
            raise ValueError(f"non-empty line {key} of {inv} has no types row")
        lay, seg = layout[key], best[key][1]
        out.append({"scan_id": r["scan_id"], "line_id": r["line_id"], "region_id": r["region_id"],
                    "page_side": r["page_side"], "x": int(r["x"]), "y": int(r["y"]), "w": int(r["w"]),
                    "h": int(r["h"]), "text": r["text"], "layout_region_id": lay["layout_region_id"],
                    "layout_column": lay["layout_column"], "layout_role": lay["layout_role"],
                    "page": lay["page"], "line_type": seg["line_type"], "type_method": seg["type_method"],
                    "n_segments": count[key], "region_tag": tags.get((r["scan_id"], r["region_id"]))})
    return out


def layer_call(line) -> dict:
    """The layer's call for a line as vocabulary tags (see the module docstring)."""
    lt, rt = line["line_type"], line.get("region_tag")
    return {"type": LINE_TYPE_TO_GENERIC[lt],
            "placement": "margin" if lt == "marginalia" else "main",
            "region_type": REGION_TAG_TO_GENERIC.get(rt),
            "region_placement": REGION_TAG_PLACEMENT.get(rt, "main" if rt not in (None, "", "untagged") else None)}


def listlike_records(lines) -> list:
    """listlike input: {"text", "x0": x, "x1": x + w, "group": (scan_id, layout_region_id)} per line, same order."""
    return [{"text": l["text"], "x0": l["x"], "x1": l["x"] + l["w"], "group": (l["scan_id"], l["layout_region_id"])}
            for l in lines]


def type_family(tag):
    """'generic:heading:date' -> 'heading'; None -> None."""
    if not tag:
        return None
    parts = tag.split(":")
    return parts[1] if len(parts) > 1 else parts[0]
