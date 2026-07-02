# Ground truth format

The ground truth format records human-validated annotations about the structure
of archival documents.  It is designed to be:

- **scan-anchored** — every annotation references a scan ID and a bounding box
  (or page side) rather than a PageXML region ID, so the file remains useful
  even without PageXML access and across PageXML re-processing runs;
- **layered** — different annotation types (sections, sessions, resolutions, …)
  occupy separate named layers, so a partially-annotated inventory is still a
  valid file;
- **per-inventory** — one YAML file per inventory number, stored under
  `data/ground_truth/<archive_id>/<inventory_num>.yaml`.

---

## File structure

```yaml
inventory_id: NL-HaNA_1.01.02_3771        # required
ead_ref:                                   # required
  unitid: NL-HaNA_1.01.02                 # archive ID (EAD unitid)
  file_unitid: "3771"                      # inventory number within archive
document_type: resolutions_book            # required

scans:                                     # optional — omitted when empty
  NL-HaNA_1.01.02_3771_0023.jpg: {width: 5234, height: 6812}

layers:                                    # required — present layers only
  section: [...]
  title_page: [...]
  session: [...]
  attendance_list: [...]
  resolution: [...]
```

`scans` records the pixel dimensions of scans that appear in at least one span.
It is populated from PageXML (when available) and needed to map bounding boxes
onto thumbnails, which are typically at a different resolution.

---

## Spans

A **span** is the smallest addressable unit: a physical region on a single scan.
It appears in the `spans` list of every annotation element.

```yaml
# Preferred form — bounding box in original scan pixel coordinates
- {scan_id: NL-HaNA_1.01.02_3771_0023.jpg, bbox: [120, 340, 4900, 1500]}

# Fallback form — page side when pixel coordinates are not yet available
- {scan_id: NL-HaNA_1.01.02_3771_0023.jpg, page_side: verso}
# page_side values: "verso", "recto", "full" (both sides of a two-page scan)
```

Bounding boxes use the `[x, y, w, h]` convention in the coordinate space of the
**original scan image** (not the thumbnail).  The `scans` section supplies the
dimensions needed to re-normalise to any other resolution.

### Why bounding boxes rather than PageXML region IDs?

- PageXML regions are layout-artefacts (arbitrary bounding boxes from the ATR
  pipeline), not meaningful document units.
- There can be many (partially) overlapping layers, which are often determined
  after the PageXML has been generated and for which new regions have been
  created that are not written back into the PageXML file, so there is no place
  to look up their coordinates.
- The ground truth file is useful with only the scan images, without access to
  PageXML.
- Multiple transcription versions of the same scan can have different region
  counts and IDs; a bbox still unambiguously selects overlapping text lines
  regardless of version.

### Derived span identifiers

The `pagexml-tools` package generates stable span identifiers from
`(scan_id, bbox)` in the form
`NL-HaNA_1.01.02_3771_0023-region-120-340-4900-1500`.
These are derivable from the YAML fields and useful as index keys or
cross-references, but are not stored in the file.

---

## Annotation elements

Each layer contains a list of **elements**.  Every element has:

| field | description |
|---|---|
| `id` | stable identifier unique within the inventory (e.g. `sec_001`) |
| `type` | layer-specific label (e.g. `index_page`, `session`) |
| `spans` | ordered list of all physical locations that make up the element |
| *(others)* | layer-specific metadata fields (e.g. `date`, `page_num`) |

```yaml
layers:
  session:
    - id: ses_001
      type: session
      date: 1626-01-02
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0063.jpg, bbox: [120, 340, 4900, 1200]}
        - {scan_id: NL-HaNA_1.01.02_3771_0064.jpg, bbox: [120, 100, 4900, 3600]}
```

All spans are listed explicitly — not just the first and last.  The order of
`spans` is the annotator-assigned reading order.  For elements with complex
layout (e.g. multi-column text where column A and column B each continue
independently on subsequent pages) the two reading streams are modelled as
two separate elements, each with their own span list.

---

## Layer reference (resolution books)

| layer | `type` values | key metadata fields |
|---|---|---|
| `section` | `index_page`, `resolution_page`, `respect_page`, `letter_page`, `non_sg_resolution_page` | `start_page`, `end_page` |
| `title_page` | `title_page` | `page_num` |
| `session` | `session` | `date` |
| `attendance_list` | `attendance_list` | `session_id` |
| `resolution` | `resolution` | `session_id` |

Only the `scan` level (implicit in every `scan_id` reference) is universal
across all inventory types.  Layers are omitted when they have no annotations.

---

## Page numbers and scan numbers

For resolution books scanned as two-page openings, the page-to-scan mapping is:

```
scan s  →  verso page: 2s − 2,  recto page: 2s − 1
```

Inverse: `scan = (page_num + 2) // 2`  (works for both verso and recto).

Example: pages 64 and 65 are both on scan 33.

---

## Annotated example — `NL-HaNA_1.01.02_3771`

```yaml
inventory_id: NL-HaNA_1.01.02_3771
ead_ref:
  unitid: NL-HaNA_1.01.02
  file_unitid: '3771'
document_type: resolutions_book
layers:
  section:
    - id: sec_001
      type: resolution_page
      start_page: 11
      end_page: 794
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0007.jpg, page_side: recto}
        - {scan_id: NL-HaNA_1.01.02_3771_0008.jpg, page_side: full}
        # … (all intermediate scans listed explicitly) …
        - {scan_id: NL-HaNA_1.01.02_3771_0398.jpg, page_side: full}
    - id: sec_002
      type: resolution_page
      start_page: 799
      end_page: 1500
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0401.jpg, page_side: recto}
        - {scan_id: NL-HaNA_1.01.02_3771_0402.jpg, page_side: full}
        # …
    - id: sec_003
      type: index_page
      start_page: 1501
      end_page: 1540
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0752.jpg, page_side: recto}
        - {scan_id: NL-HaNA_1.01.02_3771_0753.jpg, page_side: full}
        # …
  title_page:
    - id: tp_001
      type: title_page
      page_num: 7
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0005.jpg, page_side: recto}
    - id: tp_002
      type: title_page
      page_num: 11
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0007.jpg, page_side: recto}
    - id: tp_003
      type: title_page
      page_num: 795
      spans:
        - {scan_id: NL-HaNA_1.01.02_3771_0399.jpg, page_side: recto}
```

---

## Python API

```python
from pathlib import Path
from archival_structures.annotation import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    read_ground_truth,
    write_ground_truth,
)

# Read
gt = read_ground_truth(Path('data/ground_truth/NL-HaNA_1.01.02/3771.yaml'))
print(gt.inventory_id)            # 'NL-HaNA_1.01.02_3771'
sections = gt.layers['section']   # list[AnnotationElement]
first_span = sections[0].spans[0]
print(first_span.scan_id, first_span.page_side)

# Build from scratch
gt = InventoryGroundTruth(
    inventory_id='NL-HaNA_1.01.02_3771',
    ead_ref={'unitid': 'NL-HaNA_1.01.02', 'file_unitid': '3771'},
    document_type='resolutions_book',
    scans={
        'NL-HaNA_1.01.02_3771_0023.jpg': {'width': 5234, 'height': 6812},
    },
    layers={
        'session': [
            AnnotationElement(
                id='ses_001',
                type='session',
                spans=[
                    Span('NL-HaNA_1.01.02_3771_0063.jpg', bbox=[120, 340, 4900, 1200]),
                    Span('NL-HaNA_1.01.02_3771_0064.jpg', bbox=[120, 100, 4900, 3600]),
                ],
                metadata={'date': '1626-01-02'},
            ),
        ],
    },
)

# Write
write_ground_truth(gt, Path('data/ground_truth/NL-HaNA_1.01.02/3771.yaml'))
```

---

## Connection to EAD and Records in Contexts (RiC)

The `ead_ref` block points into the EAD finding aid at inventory level
(`unitid` = fonds/series, `file_unitid` = inventory number).  EAD covers the
archival hierarchy down to the inventory; the intra-volume structure captured
here (sections, sessions, resolutions) lies below what EAD models.

The ground truth maps loosely onto RiC-O concepts:

| Ground truth concept | RiC-O class |
|---|---|
| Inventory | `rico:RecordSet` |
| Section | `rico:RecordSet` (sub-group) |
| Session / resolution | `rico:Record` |
| Scan | `rico:Instantiation` |
| Span `(scan_id, bbox)` | `rico:Instantiation` + `oa:FragmentSelector` |

RiC types can be stored as optional `ric_type` keys in element `metadata`.
Export to RiC-O / W3C Web Annotation is possible by mapping each span to an
`oa:SpecificResource` with an `oa:FragmentSelector` using XYWH media fragment
syntax (e.g. `"xywh=120,340,4900,1200"`).

---

## API reference

```{automodule} archival_structures.annotation.ground_truth
:members:
```
