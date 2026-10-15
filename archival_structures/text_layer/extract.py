"""Stage 1 of the text layer: PageXML to ``lines/``, ``regions_meta/``, ``extract_stats/``.

Ported from NLP's ``startpage/voc_text_layer.py`` (step 15). Row order in every output is
PageXML document order (scans sorted by file name, regions depth-first, lines in XML order).
That order is chosen for determinism and is NOT a reading order.

pagexml is imported inside functions only.
"""

import time
from collections import Counter
from pathlib import Path

from archival_structures.model.scan_id import ScanId
from archival_structures.text_layer.io import (
    format_box, input_bytes, iter_pagexml, sanitise, write_json_atomic, write_tsv_gz)

LINES_COLUMNS = ["scan_id", "region_id", "line_idx", "line_id", "line_text"]
META_COLUMNS = ["scan_id", "region_id", "region_box", "region_tag", "n_lines"]

# Tag for regions without a Laypa `structure {type:...}`.
DEFAULT_UNTAGGED_TAG = "unclassified"

ROW_ORDER_STATEMENT = (
    "Rows are in PageXML document order (scans sorted by file name, regions depth-first "
    "as they appear in the XML, lines in XML order within a region). This order is chosen "
    "for determinism only and is NOT a reading order; PageXML reading order is ignored."
)


def iter_text_regions(regions, depth=0):
    """Depth-first, document order; yields (region, depth)."""
    for region in regions:
        yield region, depth
        if region.text_regions:
            yield from iter_text_regions(region.text_regions, depth + 1)


def region_tag_of(region, counts, untagged_tag=DEFAULT_UNTAGGED_TAG):
    attrs = region.attrs or {}
    metadata = region.metadata or {}
    if "type" in metadata and metadata.get("structure"):
        return metadata["type"]
    if "custom" not in attrs:
        counts["untagged_no_custom"] += 1
    else:
        counts["untagged_custom_without_structure_type"] += 1
    return untagged_tag


def scan_id_matches(scan_id, inv, prefix=None):
    """True if scan_id names inventory `inv` (and has `prefix`, when given).

    Parsed with ScanId, accepting any scan-number width. Ids whose scan number isn't plain
    digits (v1: 1186 '_0571A', 9817 '_P01') don't parse; for those, fall back to NLP's
    original test, '_{inv}_' in scan_id."""
    parsed = ScanId.try_parse(scan_id, digits=None)
    if parsed is None:
        return f"_{inv}_" in scan_id
    return str(parsed.inventory) == inv and (prefix is None or parsed.prefix == prefix)


def extract_inventory(inv, source, out_root, *, untagged_tag=DEFAULT_UNTAGGED_TAG,
                      scan_id_prefix=None):
    """Stage 1 for one inventory. Returns a status dict (picklable)."""
    from pagexml.parser import parse_pagexml_file

    start = time.monotonic()
    stored_bytes, xml_bytes = input_bytes(source)
    counts = Counter()
    sanitised = Counter()
    parse_errors = []
    scan_id_mismatch = []
    scans_without_regions = []
    meta_rows, line_rows = [], []
    seen = set()
    for member, data in iter_pagexml(source):
        try:
            scan = parse_pagexml_file(member, pagexml_data=data)
        except Exception as exc:  # a corrupt file must not kill a 6,914-inventory run
            parse_errors.append(f"{member}: {type(exc).__name__}: {str(exc)[:200]}")
            continue
        counts["n_scans"] += 1
        scan_id = sanitise(scan.id, sanitised)
        if not scan_id_matches(scan_id, inv, scan_id_prefix):
            scan_id_mismatch.append(f"{member} -> {scan_id}")
        counts["table_regions"] += len(scan.table_regions or [])
        if not scan.text_regions:
            scans_without_regions.append(scan_id)
        for region, depth in iter_text_regions(scan.text_regions or []):
            region_id = sanitise(region.id, sanitised)
            key = (scan_id, region_id)
            assert key not in seen, f"duplicate (scan_id, region_id) in {inv}: {key}"
            seen.add(key)
            if depth > 0:
                counts["nested_text_regions"] += 1
            if region.text_regions:
                counts["text_regions_with_children"] += 1
            tag = sanitise(region_tag_of(region, counts, untagged_tag), sanitised)
            box = format_box(region.coords)
            if not box:
                counts["regions_without_coords"] += 1
            lines = region.lines or []
            if not lines:
                counts["zero_line_regions"] += 1
            meta_rows.append((scan_id, region_id, box, tag, len(lines)))
            for idx, line in enumerate(lines):
                if line.text is None:
                    counts["lines_text_none"] += 1
                text = sanitise(line.text, sanitised)
                line_rows.append((scan_id, region_id, idx, sanitise(line.id or "", sanitised), text))
    n_lines = write_tsv_gz(Path(out_root) / "lines" / f"{inv}.lines.tsv.gz", LINES_COLUMNS, line_rows)
    n_regions = write_tsv_gz(Path(out_root) / "regions_meta" / f"{inv}.tsv.gz", META_COLUMNS, meta_rows)
    stats = {
        "inventory_number": inv,
        "source": str(source),
        "n_regions": n_regions,
        "n_lines": n_lines,
        "counts": dict(sorted(counts.items())),
        "sanitised": dict(sorted(sanitised.items())),
        "parse_errors": parse_errors,
        "scan_id_mismatch": scan_id_mismatch[:50],
        "n_scan_id_mismatch": len(scan_id_mismatch),
        # scans parsed fine but holding no TextRegion: they have no rows in
        # regions/ or regions_meta/, which is not the same as a missing scan
        "scans_without_text_regions": scans_without_regions,
    }
    write_json_atomic(Path(out_root) / "extract_stats" / f"{inv}.json", stats)
    return {
        "inventory_number": inv, "stage": "extract", "status": "done",
        "n_rows": n_regions, "n_lines": n_lines, "n_scans": counts["n_scans"],
        "n_errors": len(parse_errors), "seconds": round(time.monotonic() - start, 3),
        "input_bytes": stored_bytes, "input_xml_bytes": xml_bytes,
    }
