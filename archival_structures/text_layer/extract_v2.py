"""Stage 1 of the text layer, v2 (phase 06): scans, regions and lines with geometry.

Three tables per inventory (all in document order: scans by file name, regions depth-first,
lines in XML order; reading-order indices are columns, not the row order):

* ``scans/<inv>.scans.tsv.gz``          one row per scan, including scans without text
* ``regions_meta/<inv>.tsv.gz``         one row per region, ``region_id`` = legacy ``doc_id``
* ``lines/<inv>.lines.tsv.gz``          one row per text line: box, baseline, confidence

plus ``extract_stats/<inv>.json``. pagexml is imported inside functions only.
"""

import hashlib
import re
import time
from collections import Counter
from pathlib import Path

from archival_structures.model.scan_id import ScanId
from archival_structures.text_layer.extract import iter_text_regions, region_tag_of
from archival_structures.text_layer.io import (
    input_bytes, iter_pagexml, sanitise, write_json_atomic, write_tsv_gz)

SCAN_COLUMNS_V2 = [
    "scan_id", "archive_id", "inv_num", "inv_dir", "scan_num", "scan_variant", "scan_id_ok",
    "member", "status", "page_w", "page_h", "n_regions", "n_regions_text", "n_lines",
    "n_lines_text", "n_words"]
REGION_COLUMNS_V2 = [
    "scan_id", "page_side", "region_id", "region_xml_id", "parent_region_xml_id", "region_tag",
    "region_ro_index", "x", "y", "w", "h", "n_lines", "n_lines_text"]
LINE_COLUMNS_V2 = [
    "scan_id", "page_side", "column", "region_id", "line_idx", "line_id", "x", "y", "w", "h",
    "baseline", "conf", "text_raw", "text", "edit_flags", "split_from"]

STATUSES = ("ok", "no_words", "no_regions", "partial_shot_dropped", "parse_error")
_DEELOPNAME = re.compile(r"_deelopname(\d+)$")


def legacy_doc_id(scan_id, box):
    """The doc_id of the legacy region TSVs (pagexml-tools region doc id)."""
    return f"{scan_id}-text_region-{box['x']}-{box['y']}-{box['w']}-{box['h']}"


def legacy_region_text(line_texts):
    """Region text of the legacy TSVs: non-empty lines joined with WBCs {-, „}."""
    from archival_structures.text_layer.join import library_join
    return library_join([t for t in line_texts if t], ["-", "„"])


def _side(box, page_w):
    if not box or not page_w:
        return ""
    return "left" if box["x"] + box["w"] / 2 < page_w / 2 else "right"


def _meta(obj, name):
    return (getattr(obj, "metadata", None) or {}).get(name, "")


def _fmt_conf(conf):
    return "" if conf is None else format(conf, ".6g")


def extract_scan_v2(scan, *, inv, inv_dir, member, preset, sanitised=None, counts=None,
                    parse_error=False):
    """Rows for one parsed scan: (scan_row, region_rows, line_rows), all dicts.

    `scan=None` (or `parse_error=True`) gives a ``parse_error`` scan row with no other rows.
    `sanitised` / `counts` are optional Counters updated in place."""
    sanitised = Counter() if sanitised is None else sanitised
    counts = Counter() if counts is None else counts
    base = member.rsplit("/", 1)[-1]
    if scan is None or parse_error:
        scan_id = base[:-4] if base.endswith(".xml") else base
        parsed = ScanId.try_parse(scan_id, digits=None, prefix=preset.scan_id_prefix)
        return _scan_row(scan_id, parsed, preset, inv_dir, member, "parse_error",
                         "", "", 0, 0, 0, 0, 0), [], []
    scan_id = sanitise(scan.id or "", sanitised)
    if scan_id.endswith(".jpg"):
        scan_id = scan_id[:-4]
    parsed = ScanId.try_parse(scan_id, digits=None, prefix=preset.scan_id_prefix)
    page_w = scan.coords.width if scan.coords is not None else ""
    page_h = scan.coords.height if scan.coords is not None else ""
    pw = page_w if isinstance(page_w, (int, float)) else 0

    region_rows, line_rows = [], []
    n_regions = n_regions_text = n_lines = n_lines_text = n_words = 0
    for region, _depth in iter_text_regions(scan.text_regions or []):
        n_regions += 1
        xml_id = sanitise(region.id or "", sanitised)
        box = region.coords.box if region.coords is not None else None
        if box is None:
            counts["regions_without_coords"] += 1
            region_id = f"{scan_id}-text_region-{xml_id}"
            rbox = {"x": "", "y": "", "w": "", "h": ""}
        else:
            region_id = legacy_doc_id(scan_id, box)
            rbox = box
        ro = (region.metadata or {}).get("reading_order", {}) or {}
        ro_index = ro.get("index", "")
        parent = ""
        if (region.metadata or {}).get("parent_type") == "text_region":
            parent = sanitise(region.metadata.get("parent_id") or "", sanitised)
        tag = sanitise(region_tag_of(region, Counter(), preset.untagged_tag), sanitised)
        lines = region.lines or []
        r_text = 0
        for idx, line in enumerate(lines):
            lbox = line.coords.box if line.coords is not None else None
            raw = sanitise(line.text, sanitised)
            if raw:
                r_text += 1
            n_words += len(line.words or [])
            baseline = ""
            if line.baseline is not None and line.baseline.points:
                baseline = " ".join(f"{x},{y}" for x, y in line.baseline.points)
            line_rows.append({
                "scan_id": scan_id,
                "page_side": _side(lbox, pw),
                "column": sanitise(str(_meta(line, "column")), sanitised),
                "region_id": region_id,
                "line_idx": idx,
                "line_id": sanitise(line.id or "", sanitised),
                "x": lbox["x"] if lbox else "", "y": lbox["y"] if lbox else "",
                "w": lbox["w"] if lbox else "", "h": lbox["h"] if lbox else "",
                "baseline": baseline,
                "conf": _fmt_conf(line.conf),
                "text_raw": raw,
                "text": raw,
                "edit_flags": sanitise(str(_meta(line, "edit_flags")), sanitised),
                "split_from": sanitise(str(_meta(line, "split_from")), sanitised),
            })
        n_lines += len(lines)
        n_lines_text += r_text
        n_regions_text += 1 if r_text else 0
        region_rows.append({
            "scan_id": scan_id, "page_side": _side(box, pw), "region_id": region_id,
            "region_xml_id": xml_id, "parent_region_xml_id": parent, "region_tag": tag,
            "region_ro_index": ro_index, "x": rbox["x"], "y": rbox["y"], "w": rbox["w"],
            "h": rbox["h"], "n_lines": len(lines), "n_lines_text": r_text})

    if _DEELOPNAME.search(scan_id):
        status = "partial_shot_dropped"
        region_rows, line_rows = [], []
    elif n_regions == 0:
        status = "no_regions"
    elif n_lines_text == 0:
        status = "no_words"
    else:
        status = "ok"
    scan_row = _scan_row(scan_id, parsed, preset, inv_dir, member, status, page_w, page_h,
                         n_regions, n_regions_text, n_lines, n_lines_text, n_words)
    return scan_row, region_rows, line_rows


def _scan_row(scan_id, parsed, preset, inv_dir, member, status, page_w, page_h, n_regions,
              n_regions_text, n_lines, n_lines_text, n_words):
    variant = _DEELOPNAME.search(scan_id)
    return {
        "scan_id": scan_id,
        "archive_id": preset.archive_id or "",
        "inv_num": str(parsed.inventory) if parsed else inv_dir,
        "inv_dir": inv_dir,
        "scan_num": parsed.number if parsed else "",
        "scan_variant": f"deelopname{variant.group(1)}" if variant else "",
        "scan_id_ok": 1 if parsed else 0,
        "member": member if "/" in member and not Path(member).is_absolute() else Path(member).name,
        "status": status, "page_w": page_w, "page_h": page_h, "n_regions": n_regions,
        "n_regions_text": n_regions_text, "n_lines": n_lines, "n_lines_text": n_lines_text,
        "n_words": n_words}


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None):
    """Stage 1 (v2) for one inventory. Returns a status dict (picklable)."""
    from pagexml.parser import parse_pagexml_file

    start = time.monotonic()
    source = Path(source)
    stored_bytes, xml_bytes = input_bytes(source)
    counts, sanitised, status_counts = Counter(), Counter(), Counter()
    parse_errors = []
    scan_rows, region_rows, line_rows = [], [], []
    for member, data in iter_pagexml(source, skip_resource_forks=preset.skip_resource_forks):
        kwargs = dict(inv=inv, inv_dir=inv, member=member, preset=preset,
                      sanitised=sanitised, counts=counts)
        try:
            scan = parse_pagexml_file(member, pagexml_data=data)
        except Exception as exc:  # a corrupt file must not kill the run
            parse_errors.append(f"{member}: {type(exc).__name__}: {str(exc)[:200]}")
            scan = None
        if scan is not None and scan_hook is not None and not _is_dropped(scan):
            scan = scan_hook(scan)
        s_row, r_rows, l_rows = extract_scan_v2(scan, **kwargs)
        status_counts[s_row["status"]] += 1
        scan_rows.append(s_row)
        region_rows += r_rows
        line_rows += l_rows
    seen_regions, seen_lines = set(), set()
    dup_regions, dup_lines = [], []
    for r in region_rows:
        if r["region_id"] in seen_regions:
            dup_regions.append(r["region_id"])
        seen_regions.add(r["region_id"])
    for r in line_rows:
        key = (r["scan_id"], r["line_id"])
        if key in seen_lines:
            dup_lines.append(key)
        seen_lines.add(key)

    root = Path(out_root)
    n_scans = write_tsv_gz(root / "scans" / f"{inv}.scans.tsv.gz", SCAN_COLUMNS_V2,
                           [[r[c] for c in SCAN_COLUMNS_V2] for r in scan_rows])
    n_regions = write_tsv_gz(root / "regions_meta" / f"{inv}.tsv.gz", REGION_COLUMNS_V2,
                             [[r[c] for c in REGION_COLUMNS_V2] for r in region_rows])
    n_lines = write_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz", LINE_COLUMNS_V2,
                           [[r[c] for c in LINE_COLUMNS_V2] for r in line_rows])
    stats = {
        "inventory_number": inv,
        "format_version": 2,
        "source": str(source),
        "source_sha256": _sha256(source) if source.is_file() else None,
        "n_scans": n_scans, "n_regions": n_regions, "n_lines": n_lines,
        "status_counts": {s: status_counts.get(s, 0) for s in STATUSES},
        "counts": dict(sorted(counts.items())),
        "sanitised": dict(sorted(sanitised.items())),
        "parse_errors": parse_errors,
        "n_duplicate_region_ids": len(dup_regions),
        "n_duplicate_line_ids": len(dup_lines),
        "duplicate_region_ids": dup_regions[:20],
        "duplicate_line_ids": [list(k) for k in dup_lines[:20]],
    }
    write_json_atomic(root / "extract_stats" / f"{inv}.json", stats)
    return {
        "inventory_number": inv, "stage": "extract", "status": "done", "n_rows": n_regions,
        "n_lines": n_lines, "n_scans": n_scans, "n_errors": len(parse_errors),
        "seconds": round(time.monotonic() - start, 3),
        "input_bytes": stored_bytes, "input_xml_bytes": xml_bytes,
    }


def _is_dropped(scan):
    return bool(_DEELOPNAME.search((scan.id or "").removesuffix(".jpg")))
