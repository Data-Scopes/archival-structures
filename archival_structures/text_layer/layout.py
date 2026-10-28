"""Layout stage of the v2 text layer (steps 15fp, 15fq): rows ↔ pdm adapter and the ``layout/`` tables.

Reads ``scans/``, ``regions_meta/`` and ``lines/`` of one inventory (never rewritten), runs
``archival_structures.analysis.page_structure`` per scan, and writes

* ``layout/scans/<inv>.tsv.gz``    one row per scan: page mode, separation x, counts;
* ``layout/regions/<inv>.tsv.gz``  one row per layout region (and per empty source region);
* ``layout/lines/<inv>.tsv.gz``    one row per input line: its page, layout region and role.

Region coords are the hull of the line boxes, not the ``regions_meta`` box (F06-15d.3); the
``regions_meta`` box only gives the id of an unchanged region. Schemas: ``docs/text_layer.md``,
*Layout tables (steps 15fp, 15fq)*.
"""

import time
from collections import Counter, defaultdict
from pathlib import Path

import pagexml.model.physical_document_model as pdm
from pagexml.model.coords import Baseline

from archival_structures.analysis.page_structure import (
    PAGE_SINGLE, LayoutParams, assign_column_roles, choose_gutter_rule, find_separation_x,
    fit_gutter, furniture_region_ids, group_regions_into_columns, hull_coords,
    inventory_line_width_rel, is_opening, merge_overlapping_text_regions, split_regions_on_line_field, split_regions_into_columns, sort_lines,
    split_scan_into_pages)
from archival_structures.analysis.index_pages import fill_runs, side_features, side_rule
from archival_structures.text_layer.extract_v2 import legacy_doc_id
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz

LAYOUT_SCAN_COLUMNS = [
    "scan_id", "page_mode", "separation_x", "split_method", "n_lines_crossing", "n_regions_in",
    "n_regions_out", "n_merges", "n_gutter_splits", "gutter_rule", "n_column_splits",
    "n_furniture", "n_guard_blocked", "n_column_blocked", "n_regions_resorted"]
LAYOUT_REGION_COLUMNS = [
    "scan_id", "page", "layout_region_id", "x", "y", "w", "h", "layout_column", "layout_role",
    "n_lines", "source_region_ids", "change", "furniture", "column_value", "lines_resorted"]
LAYOUT_LINE_COLUMNS = [
    "scan_id", "line_id", "region_id", "page", "layout_region_id", "layout_line_idx",
    "layout_column", "layout_role"]
STAT_KEYS = ("n_scans", "n_openings", "n_single", "n_lines", "n_lines_crossing",
             "n_scans_crossing", "n_regions_in", "n_regions_out", "n_regions_empty", "n_merges",
             "n_gutter_splits", "max_merge_passes", "n_column_splits", "n_furniture",
             "n_guard_blocked", "n_column_blocked", "n_regions_resorted")
UNION_KEYS = ("source_region_ids",)


def _region_id(scan_id, coords):
    return legacy_doc_id(scan_id, coords.box)
PAGE_COLUMNS = ["scan_id", "page", "n_lines", "n_text8", "n_text20", "col_share", "end_num", "date_start",
                "rule", "index_side"]   # step 19c: layout/pages/<inv>.tsv.gz, only with params.index_sides
PAGE_ORDER = {"verso": 0, "recto": 1, PAGE_SINGLE: 0}


def _parse_points(text):
    return [tuple(int(float(v)) for v in p.split(",")) for p in text.split()] if text else []


def _line(row):
    x, y, w, h = (int(row[k]) for k in ("x", "y", "w", "h"))
    points = _parse_points(row["baseline"])
    return pdm.PageXMLTextLine(
        doc_id=row["line_id"], metadata={"scan_id": row["scan_id"], "line_id": row["line_id"],
                  "column": row.get("column") or ""},
        coords=pdm.Coords.coords_from_box_params(x, y, w, h),
        baseline=Baseline(points) if points else None, text=row["text"])


def _box_centre_x(row):
    return int(row["x"]) + int(row["w"]) / 2


def _page_coords(page_name, separation_x, page_w, page_h):
    if not page_w or not page_h:
        return None
    sep = int(round(separation_x)) if separation_x is not None else page_w
    x, w = {"verso": (0, sep), "recto": (sep, page_w - sep)}.get(page_name, (0, page_w))
    return pdm.Coords.coords_from_box_params(x, 0, w, page_h)


def _change(region):
    """``unchanged`` or the ``+``-join of ``merged``, ``split_at_gutter``, ``split_at_column``, ``column_reordered``."""
    parts = [name for key, name in (("merged_from", "merged"), ("split_from", "split_at_gutter"),
                                    ("column_split_from", "split_at_column"),
                                    # step 19f (phase 13 review, F13-R.7): B1 failed on extent-based span
                                    # lines and piece order; reorder keeps one region and one unit
                                    ("column_reorder_from", "column_reordered"))
             if region.metadata.get(key)]
    return "+".join(parts) or "unchanged"


def _column_value(region):
    values = {(line.metadata or {}).get("column", "") for line in region.lines}
    return values.pop() if len(values) == 1 else ""


def layout_scan(scan_row, region_rows, lines_by_region, gutter_rel, params, *,
                gutter_rule="gap", med_w_rel=None, blocked_out=None):
    """One scan → (scan row, region rows, {line_id: line row}, n merge passes). ``blocked_out``,
    if a list, receives (page, id_a, id_b) for every guard-blocked pair."""
    scan_id = scan_row["scan_id"]
    page_w = int(scan_row["page_w"] or 0)
    page_h = int(scan_row["page_h"] or 0)
    opening = is_opening(page_w, page_h, params)
    order = {r["region_id"]: i for i, r in enumerate(region_rows)}
    regions, empty = [], []
    for r in region_rows:
        lines = lines_by_region.get(r["region_id"], [])
        if not any(l["text"].strip() for l in lines):
            empty.append(r)
            continue
        pdm_lines = [_line(l) for l in sorted(lines, key=lambda l: int(l["line_idx"]))]
        region = pdm.PageXMLTextRegion(
            doc_id=r["region_id"], coords=hull_coords(pdm_lines), lines=pdm_lines,
            metadata={"scan_id": scan_id, "region_tag": r["region_tag"],
                      "source_region_ids": [r["region_id"]]})
        region.set_as_parent(pdm_lines)
        regions.append(region)

    all_lines = [l for region in regions for l in region.lines]
    if not opening:
        separation_x, method = None, "none"
    elif not regions:
        separation_x, method = gutter_rel * page_w, "none"
    else:
        boxes = [(l.coords.x, l.coords.w) for l in all_lines]
        separation_x, method = find_separation_x(boxes, page_w, gutter_rel, params,
                                                 rule=gutter_rule, med_w_rel=med_w_rel)
    pages, n_splits = split_scan_into_pages(regions, separation_x if opening else None,
                                             id_fn=_region_id)
    crossing = 0 if not opening else sum(
        l.coords.x <= separation_x <= l.coords.x + l.coords.w for l in all_lines)

    out_regions, line_out, n_merges, passes = [], {}, 0, 0
    n_out = n_column_splits = n_furniture = n_blocked = n_col_blocked = 0
    row_tol = params.row_tol_frac if params.line_sort == "row" else None
    n_resorted = 0
    for page_name, page_regions in pages.items():
        if params.split_on_line_field:
            page_regions, n_cs = split_regions_on_line_field(
                page_regions, params.split_on_line_field, id_fn=_region_id,
                sort_lines=params.sort_split_lines, row_tol_frac=row_tol)
            n_column_splits += n_cs
        if params.hand_column_split is not None:   # step 19b
            page_regions, n_hs = split_regions_into_columns(
                page_regions, params.hand_column_split, id_fn=_region_id, row_tol_frac=row_tol)
            n_column_splits += n_hs
        bottom = max((l.coords.y + l.coords.h for r in page_regions for l in r.lines), default=0)
        furniture = furniture_region_ids(page_regions, bottom, page_h, params)
        n_furniture += len(furniture)
        if params.merge:
            blocked, col_blocked = {}, {}
            merged, n_passes = merge_overlapping_text_regions(
                page_regions, params, union_keys=UNION_KEYS, id_fn=_region_id,
                exclude=furniture, blocked=blocked, column_blocked=col_blocked)
            n_blocked += len(blocked)
            n_col_blocked += len(col_blocked)
            if blocked_out is not None:
                blocked_out += [(page_name, a, b) for a, b in blocked]
        else:
            merged, n_passes = list(page_regions), 0
        passes = max(passes, n_passes)
        columns = group_regions_into_columns(merged, params, column_id_prefix=f"{scan_id}-{page_name}")
        assign_column_roles(columns, params, page_id=f"{scan_id}-{page_name}",
                            page_coords=_page_coords(page_name, separation_x, page_w, page_h),
                            metadata={"scan_id": scan_id, "page": page_name})
        ordered = [r for c in columns for r in c.text_regions]
        for region in ordered:
            change = _change(region)
            sources = sorted(region.metadata["source_region_ids"], key=order.__getitem__)
            n_merges += "merged" in change.split("+")
            n_out += 1
            box = region.coords
            if change == "unchanged":
                assert region.id == sources[0]
            lines = region.lines
            if change == "unchanged" and params.sort_unchanged_lines:
                lines = sort_lines(region.lines, params)   # step 15j
            resorted = int([l.id for l in lines] != [l.id for l in region.lines])
            n_resorted += resorted
            out_regions.append({
                "scan_id": scan_id, "page": page_name, "layout_region_id": region.id,
                "x": box.x, "y": box.y, "w": box.w, "h": box.h,
                "layout_column": region.metadata["layout_column"],
                "layout_role": region.metadata["layout_role"], "n_lines": len(region.lines),
                "source_region_ids": " ".join(sources), "change": change,
                "furniture": int(region.id in furniture), "column_value": _column_value(region),
                "lines_resorted": resorted})
            for i, line in enumerate(lines):
                line_out[line.id] = {
                    "page": page_name, "layout_region_id": region.id, "layout_line_idx": i,
                    "layout_column": region.metadata["layout_column"],
                    "layout_role": region.metadata["layout_role"]}
    for r in empty:
        if not opening:
            page_name = PAGE_SINGLE
        else:
            page_name = "verso" if _box_centre_x(r) < separation_x else "recto"
        out_regions.append({
            "scan_id": scan_id, "page": page_name, "layout_region_id": r["region_id"],
            "x": r["x"], "y": r["y"], "w": r["w"], "h": r["h"], "layout_column": "",
            "layout_role": "empty", "n_lines": len(lines_by_region.get(r["region_id"], [])),
            "source_region_ids": r["region_id"], "change": "unchanged", "furniture": 0,
            "column_value": "", "lines_resorted": 0})
        for i, l in enumerate(sorted(lines_by_region.get(r["region_id"], []),
                                     key=lambda l: int(l["line_idx"]))):
            line_out[l["line_id"]] = {
                "page": page_name, "layout_region_id": r["region_id"], "layout_line_idx": i,
                "layout_column": "", "layout_role": "empty"}
    scan_out = {
        "scan_id": scan_id, "page_mode": "opening" if opening else "single",
        "separation_x": "" if separation_x is None else f"{separation_x:g}",
        "split_method": method, "n_lines_crossing": crossing, "n_regions_in": len(regions),
        "n_regions_out": n_out, "n_merges": n_merges, "n_gutter_splits": n_splits,
        "gutter_rule": gutter_rule, "n_column_splits": n_column_splits,
        "n_furniture": n_furniture, "n_guard_blocked": n_blocked,
        "n_column_blocked": n_col_blocked, "n_regions_resorted": n_resorted}
    return scan_out, out_regions, line_out, passes


def _write_pages_table(path, line_rows, line_map, cfg) -> int:
    """Per page side features, rule and filled index flag (step 19c); returns the number of index sides."""
    sides = defaultdict(list)
    for l in line_rows:
        sides[(l["scan_id"], line_map[(l["scan_id"], l["line_id"])]["page"])].append(l)
    keys = sorted(sides, key=lambda k: (int(k[0].rsplit("_", 1)[1]), PAGE_ORDER[k[1]], k[1]))
    feats = [side_features(sides[k], cfg) for k in keys]
    rules = [side_rule(f, cfg) for f in feats]
    flags = fill_runs(rules, cfg.get("gap", 0), cfg.get("min_run", 1))

    def num(v):
        return "" if v is None else str(round(v, 4))
    rows = [[k[0], k[1], f["n_lines"], f["n_text8"], f["n_text20"], num(f["col_share"]), num(f["end_num"]),
             num(f["date_start"]), int(r), int(x)] for k, f, r, x in zip(keys, feats, rules, flags)]
    write_tsv_gz(path, PAGE_COLUMNS, rows)
    return sum(flags)


def layout_inventory(inv, layer_root, params: LayoutParams = LayoutParams()) -> dict:
    """Write the ``layout/`` tables of one inventory; return its stats."""
    start = time.monotonic()
    root = Path(layer_root)
    scans = list(read_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz"))
    regions_by_scan = defaultdict(list)
    for r in read_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz"):
        regions_by_scan[r["scan_id"]].append(r)
    line_rows = list(read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"))
    lines_by_region = defaultdict(list)
    for l in line_rows:
        lines_by_region[l["region_id"]].append(l)

    nonempty = {rid for rid, lines in lines_by_region.items() if any(l["text"].strip() for l in lines)}
    landscape = []
    for s in scans:
        if is_opening(int(s["page_w"] or 0), int(s["page_h"] or 0), params):
            boxes = [(int(l["x"]), int(l["w"])) for r in regions_by_scan[s["scan_id"]]
                     if r["region_id"] in nonempty for l in lines_by_region[r["region_id"]]]
            landscape.append((int(s["page_w"]), boxes))
    gutter_rel, n_with_gap = fit_gutter(landscape, params)
    n_with_lines = sum(bool(boxes) for _, boxes in landscape)
    gutter_rule = choose_gutter_rule(n_with_gap, n_with_lines, params)
    med_w_rel = inventory_line_width_rel(landscape)

    scan_out, region_out, line_map = [], [], {}
    passes = 0
    for s in scans:
        s_row, r_rows, l_map, n_passes = layout_scan(
            s, regions_by_scan[s["scan_id"]], lines_by_region, gutter_rel, params,
            gutter_rule=gutter_rule, med_w_rel=med_w_rel)
        scan_out.append(s_row)
        region_out += r_rows
        for line_id, v in l_map.items():
            line_map[(s["scan_id"], line_id)] = v
        passes = max(passes, n_passes)
    out_lines = []
    for l in line_rows:
        v = line_map[(l["scan_id"], l["line_id"])]
        out_lines.append({"scan_id": l["scan_id"], "line_id": l["line_id"],
                          "region_id": l["region_id"], **v})

    out = root / "layout"
    write_tsv_gz(out / "scans" / f"{inv}.tsv.gz", LAYOUT_SCAN_COLUMNS,
                 ([r[c] for c in LAYOUT_SCAN_COLUMNS] for r in scan_out))
    write_tsv_gz(out / "regions" / f"{inv}.tsv.gz", LAYOUT_REGION_COLUMNS,
                 ([r[c] for c in LAYOUT_REGION_COLUMNS] for r in region_out))
    write_tsv_gz(out / "lines" / f"{inv}.tsv.gz", LAYOUT_LINE_COLUMNS,
                 ([r[c] for c in LAYOUT_LINE_COLUMNS] for r in out_lines))

    n_index_sides = None
    if params.index_sides is not None:   # step 19c
        n_index_sides = _write_pages_table(out / "pages" / f"{inv}.tsv.gz", line_rows, line_map,
                                           params.index_sides)

    sums = Counter()
    for r in scan_out:
        for k in ("n_lines_crossing", "n_regions_in", "n_regions_out", "n_merges", "n_gutter_splits",
                  "n_column_splits", "n_furniture", "n_guard_blocked", "n_column_blocked",
                  "n_regions_resorted"):
            sums[k] += r[k]
    roles = Counter(r["layout_role"] for r in region_out)
    stats = {
        "n_scans": len(scan_out),
        "n_openings": sum(r["page_mode"] == "opening" for r in scan_out),
        "n_single": sum(r["page_mode"] == "single" for r in scan_out),
        "n_lines": len(out_lines),
        "n_scans_crossing": sum(r["n_lines_crossing"] > 0 for r in scan_out),
        "n_regions_empty": roles["empty"],
        "max_merge_passes": passes,
        **{k: sums[k] for k in ("n_lines_crossing", "n_regions_in", "n_regions_out", "n_merges",
                                "n_gutter_splits", "n_column_splits", "n_furniture",
                                "n_guard_blocked", "n_column_blocked", "n_regions_resorted")},
        "gutter_rule": gutter_rule,
        "med_w_rel": round(med_w_rel, 5),
        "n_openings_with_lines": n_with_lines,
        "split_method": dict(sorted(Counter(r["split_method"] for r in scan_out).items())),
        "layout_role": dict(sorted(roles.items())),
        "gutter_rel": round(gutter_rel, 5),
        "n_scans_with_gap": n_with_gap,
        "n_landscape_scans": len(landscape),
    }
    if n_index_sides is not None:
        stats["n_index_sides"] = n_index_sides
    stats["seconds"] = round(time.monotonic() - start, 3)
    return stats
