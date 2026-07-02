"""Convert resolution_inventory_metadata.json to per-inventory ground truth files.

The source JSON captures book sections and title pages as page ranges.  Page
numbers follow the convention: scan *s* contains verso page ``2s - 2`` and
recto page ``2s - 1``, so page 64 and 65 are both on scan 33.

Each ground truth file is written to::

    data/ground_truth/<series_name>/<inventory_num>.yaml

Only the ``section`` and ``title_page`` layers are populated from this source;
session-, resolution-, and attendance-list layers require separate annotation.

Usage::

    python scripts/convert_resolution_metadata.py

"""

from __future__ import annotations

import json
from pathlib import Path

from archival_structures.annotation.ground_truth import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    write_ground_truth,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILE = REPO_ROOT / 'data' / 'ground_truth' / 'resolution_inventory_metadata.json'
OUTPUT_DIR = REPO_ROOT / 'data' / 'ground_truth'

DOCUMENT_TYPE = 'resolutions_book'


# ---------------------------------------------------------------------------
# Page / scan helpers
# ---------------------------------------------------------------------------

def page_num_to_scan_num(page_num: int) -> int:
    """Return the scan number that contains *page_num*.

    Scan *s* holds verso page ``2s - 2`` and recto page ``2s - 1``.
    The inverse is ``s = (page + 2) // 2``, which works for both parities.
    """
    return (page_num + 2) // 2


def page_num_to_side(page_num: int) -> str:
    """Return ``"verso"`` for even page numbers, ``"recto"`` for odd."""
    return 'verso' if page_num % 2 == 0 else 'recto'


def page_range_to_spans(inventory_id: str, start_page: int, end_page: int) -> list[Span]:
    """Build an explicit span list for a contiguous page range.

    Each scan that overlaps the range is included; the ``page_side`` is set to
    ``"full"`` when both pages on the scan fall within the range, or to
    ``"verso"`` / ``"recto"`` when only one side does.
    """
    start_scan = page_num_to_scan_num(start_page)
    end_scan = page_num_to_scan_num(end_page)
    spans: list[Span] = []
    for scan_num in range(start_scan, end_scan + 1):
        scan_id = f'{inventory_id}_{scan_num:04d}.jpg'
        verso_page = 2 * scan_num - 2
        recto_page = 2 * scan_num - 1
        verso_in = start_page <= verso_page <= end_page
        recto_in = start_page <= recto_page <= end_page
        if verso_in and recto_in:
            spans.append(Span(scan_id=scan_id, page_side='full'))
        elif verso_in:
            spans.append(Span(scan_id=scan_id, page_side='verso'))
        elif recto_in:
            spans.append(Span(scan_id=scan_id, page_side='recto'))
    return spans


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------

def convert_inventory(record: dict) -> InventoryGroundTruth:
    inventory_id: str = record['inventory_id']
    ead_ref = {
        'unitid': record['series_name'],
        'file_unitid': str(record['inventory_num']),
    }

    layers: dict[str, list[AnnotationElement]] = {}

    # -- section layer -------------------------------------------------------
    sections = record.get('sections', [])
    section_elements: list[AnnotationElement] = []
    for idx, sec in enumerate(sections, start=1):
        sec_id = f'sec_{idx:03d}'
        spans = page_range_to_spans(inventory_id, sec['start'], sec['end'])
        metadata: dict = {'start_page': sec['start'], 'end_page': sec['end']}
        section_elements.append(AnnotationElement(
            id=sec_id,
            type=sec['page_type'],
            spans=spans,
            metadata=metadata,
        ))
    if section_elements:
        layers['section'] = section_elements

    # -- title_page layer ----------------------------------------------------
    title_page_nums: list[int] = record.get('title_page_nums', [])
    title_page_elements: list[AnnotationElement] = []
    for idx, page_num in enumerate(title_page_nums, start=1):
        tp_id = f'tp_{idx:03d}'
        scan_num = page_num_to_scan_num(page_num)
        scan_id = f'{inventory_id}_{scan_num:04d}.jpg'
        side = page_num_to_side(page_num)
        title_page_elements.append(AnnotationElement(
            id=tp_id,
            type='title_page',
            spans=[Span(scan_id=scan_id, page_side=side)],
            metadata={'page_num': page_num},
        ))
    if title_page_elements:
        layers['title_page'] = title_page_elements

    return InventoryGroundTruth(
        inventory_id=inventory_id,
        ead_ref=ead_ref,
        document_type=DOCUMENT_TYPE,
        layers=layers,
    )


def main() -> None:
    with open(SOURCE_FILE, encoding='utf-8') as fh:
        records: list[dict] = json.load(fh)

    written = 0
    for record in records:
        gt = convert_inventory(record)
        series = record['series_name']
        inv_num = record['inventory_num']
        out_path = OUTPUT_DIR / series / f'{inv_num}.yaml'
        write_ground_truth(gt, out_path)
        written += 1

    print(f'Wrote {written} ground truth files under {OUTPUT_DIR}/')


if __name__ == '__main__':
    main()
