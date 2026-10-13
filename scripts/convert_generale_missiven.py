"""Convert the Overview of Generale Missiven (doi:10.34894/SRDMFU) to per-inventory ground truth.

The overview lists 950 *generale missiven* in the VOC archive (NL-HaNA 1.04.02), each with the
first and last scan and folio it occupies. Each missive becomes one element of a ``document``
layer, typed ``generale_missive``, with one span per scan from ``Beginscan`` to ``Eindscan``.

Things to know about the labels:

- **Coverage is partial.** Only the missives in a volume are labelled, not every document, so
  these files support recall measurement of document starts and ends, not precision.
- **Scan-level only.** The overview does not say where on a scan a missive starts or ends, so
  every span is ``page_side: full``. A missive that shares its first scan with the end of the
  previous one (``shares_scan_with_previous``) probably starts mid-page.
- **Overlaps are flagged, not dropped** (see :func:`classify_overlap`). The overview registers
  some missives twice (``likely_duplicate_of``), which would inflate recall if both were
  counted, and lists some missives inside others (``nested_in``).
- **The numeric ``Eindscan`` column is used, not the last-scan filename.** The filename column
  disagrees with ``Eindscan`` in 75 rows, 68 of which just repeat the first-scan filename;
  ``Eindscan`` agrees with the folio counts (about 2 scans per folio on single-page scans, 1 on
  double-page scans).
- **Inventory ids come from the first-scan filename**, which keeps suffixes such as ``1179B``
  that the numeric inventory column drops.
- Rows that the dataset's own manual check flags as not digitised, removed as a duplicate, not
  consultable or missing from the archive are skipped and listed in ``skipped.tsv``.

Output::

    ground_truth/NL-HaNA_1.04.02/generale_missiven_overview/converted/<inventory_id>.yaml
    ground_truth/NL-HaNA_1.04.02/generale_missiven_overview/converted/skipped.tsv

Usage::

    python scripts/convert_generale_missiven.py [--source CSV] [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import csv
import datetime
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from archival_structures.annotation.ground_truth import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    write_ground_truth,
)

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = REPO_ROOT / 'ground_truth' / 'NL-HaNA_1.04.02' / 'generale_missiven_overview'
SOURCE_FILE = DATASET_DIR / 'source' / 'Overzicht van Generale Missiven in 1_04_02.csv'
OUTPUT_DIR = DATASET_DIR / 'converted'

ARCHIVE_ID = 'NL-HaNA_1.04.02'
DOCUMENT_TYPE = 'unspecified'
LAYER = 'document'
ELEMENT_TYPE = 'generale_missive'

PROVENANCE = {
    'source': 'doi:10.34894/SRDMFU (K. Pepping, Overzicht van Generale Missiven in 1.04.02)',
    'method': 'compiled_overview',
    'coverage': 'partial',
    'parameters': {
        'end_scan_field': 'Eindscan',
        'start_end_precision': 'scan',
    },
}

# manual-check notes that mean the row has no usable location in the digitised archive
EXCLUDE_NOTE = re.compile(
    r'Niet gedigitaliseerd|verwijderd|niet raadpleegbaar|Ontbreekt uit het archief',
    re.IGNORECASE,
)
FIRST_SCAN_NAME = re.compile(r'NL-HaNA_1\.04\.02_([^_]+)_\d+$')
SCAN_LAYOUT = {'Enkel': 'single', 'Dubbel': 'double'}

COLUMNS = {
    'ID': 'gm_id',
    'Problemen gevonden tijdens handmatige check:': 'manual_check_note',
    'ID in TANAP database': 'tanap_id',
    'Inv.nr. Nationaal Archief (1.04.02)': 'inv_nr',
    'Deel v. inventarisnummer': 'inventory_part',
    'Kamer': 'kamer',
    'Beginfolio': 'begin_folio',
    'Eindfolio': 'end_folio',
    'Beginscan': 'begin_scan',
    'Eindscan': 'end_scan',
    'Enkele of dubbele pagina op scan': 'scan_layout',
    'RGP Deel waarin de missive is opgenomen': 'rgp_volume',
    'RGP pagina waarop de missive begint': 'rgp_page',
    'Beschrijving in TANAP': 'description',
    'Datum': 'date_text',
    'Datum (numeriek)': 'date',
    'Bestandsnaam van eerste scan': 'first_scan_name',
}


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------

def scan_id(inventory: str, scan_num: int) -> str:
    return f'{ARCHIVE_ID}_{inventory}_{scan_num:04d}.jpg'


def inventory_of(row: pd.Series) -> str | None:
    """Inventory id from the first-scan filename (keeps suffixes), else the numeric column."""
    match = FIRST_SCAN_NAME.match(str(row['first_scan_name']))
    if match:
        return match.group(1)
    if pd.notna(row['inv_nr']):
        return str(int(row['inv_nr']))
    return None


def skip_reason(row: pd.Series, inventory: str | None) -> str | None:
    note = row['manual_check_note']
    if isinstance(note, str) and EXCLUDE_NOTE.search(note):
        return f'manual check: {note}'
    if inventory is None:
        return 'no inventory number'
    if pd.isna(row['begin_scan']) or pd.isna(row['end_scan']):
        return 'no begin or end scan'
    if row['end_scan'] < row['begin_scan']:
        return 'end scan before begin scan'
    return None


def element_metadata(row: pd.Series) -> dict:
    """Optional per-missive fields, leaving out empty values."""
    metadata = {}
    for key in ('gm_id', 'tanap_id', 'rgp_volume', 'begin_folio', 'end_folio'):
        if pd.notna(row[key]):
            metadata[key] = int(row[key])
    for key in ('date', 'date_text', 'description', 'kamer', 'inventory_part', 'rgp_page',
                'manual_check_note'):
        if pd.notna(row[key]) and str(row[key]).strip():
            metadata[key] = str(row[key]).strip()
    if row['scan_layout'] in SCAN_LAYOUT:
        metadata['scan_layout'] = SCAN_LAYOUT[row['scan_layout']]
    return metadata


def element_id(row: pd.Series) -> str:
    return f"gm_{int(row['gm_id']):04d}"


def classify_overlap(row: pd.Series, other: pd.Series) -> dict:
    """Flag how *row* overlaps *other*, an earlier-starting missive in the same inventory.

    The folio range decides, because it is what the compilers recorded per missive:

    - the same folio range means the missive is registered twice (``likely_duplicate_of``);
      count only one of the pair when measuring recall;
    - a folio range inside the other's means a missive nested in another (``nested_in``);
    - otherwise a single shared scan is an ordinary boundary that probably falls mid-page
      (``shares_scan_with_previous``), and a larger overlap is unexplained
      (``overlap_scans_with_previous``) and worth checking by hand.
    """
    folios = (row['begin_folio'], row['end_folio'])
    other_folios = (other['begin_folio'], other['end_folio'])
    if not any(pd.isna(f) for f in folios + other_folios):
        if folios == other_folios:
            return {'likely_duplicate_of': element_id(other)}
        if other_folios[0] <= folios[0] and folios[1] <= other_folios[1]:
            return {'nested_in': element_id(other)}
    overlap = int(other['end_scan']) - int(row['begin_scan']) + 1
    if overlap == 1:
        return {'shares_scan_with_previous': True}
    return {'overlap_scans_with_previous': overlap}


def convert(source: Path) -> tuple[list[InventoryGroundTruth], list[dict]]:
    df = pd.read_csv(source, usecols=list(COLUMNS)).rename(columns=COLUMNS)
    by_inventory: dict[str, list[pd.Series]] = defaultdict(list)
    skipped = []
    for _, row in df.iterrows():
        inventory = inventory_of(row)
        reason = skip_reason(row, inventory)
        if reason:
            skipped.append({'gm_id': row['gm_id'], 'inventory': inventory or '', 'reason': reason})
            continue
        by_inventory[inventory].append(row)

    converted = []
    for inventory, rows in sorted(by_inventory.items()):
        rows.sort(key=lambda r: (r['begin_scan'], -r['end_scan'], r['gm_id']))
        elements = []
        reach = None  # the row reaching furthest so far, which a new row may overlap
        for row in rows:
            begin, end = int(row['begin_scan']), int(row['end_scan'])
            metadata = element_metadata(row)
            if reach is not None and begin <= reach['end_scan']:
                metadata.update(classify_overlap(row, reach))
            if reach is None or end > reach['end_scan']:
                reach = row
            elements.append(AnnotationElement(
                id=element_id(row),
                type=ELEMENT_TYPE,
                spans=[Span(scan_id(inventory, s), page_side='full') for s in range(begin, end + 1)],
                metadata=metadata,
            ))
        converted.append(InventoryGroundTruth(
            inventory_id=f'{ARCHIVE_ID}_{inventory}',
            ead_ref={'unitid': ARCHIVE_ID, 'file_unitid': inventory},
            document_type=DOCUMENT_TYPE,
            layers={LAYER: elements},
            provenance={LAYER: PROVENANCE},
            extra={'converted_by': 'scripts/convert_generale_missiven.py',
                   'converted_on': datetime.date.today().isoformat()},
        ))
    return converted, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--source', type=Path, default=SOURCE_FILE)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()

    converted, skipped = convert(args.source)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for gt in converted:
        write_ground_truth(gt, args.output_dir / f'{gt.inventory_id}.yaml')
    with open(args.output_dir / 'skipped.tsv', 'w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=['gm_id', 'inventory', 'reason'], delimiter='\t')
        writer.writeheader()
        writer.writerows(skipped)

    elements = [e for gt in converted for e in gt.layers[LAYER]]
    print(f'{len(converted)} inventories, {len(elements)} missives written to {args.output_dir}')
    print(f'{len(skipped)} rows skipped (see skipped.tsv)')
    for flag in ('likely_duplicate_of', 'nested_in', 'shares_scan_with_previous',
                 'overlap_scans_with_previous'):
        flagged = [e.id for e in elements if flag in e.metadata]
        print(f'{flag}: {len(flagged)} {" ".join(flagged) if len(flagged) <= 12 else ""}')


if __name__ == '__main__':
    main()
