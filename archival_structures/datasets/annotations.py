"""Read/write ground-truth labels for the text-analysis tasks in `archival_structures.analysis`.

Labels are stored as plain JSON, one file per scan, under
`data/annotations/<institute_id>/<archive_id>/<inventory_num_id>/annotations-<scan_id>.json`
(an extension of the directory convention already used by `image_drawing.ThumbnailSelectionTagger`,
and matching the `<institute_id>/<archive_id>/<inventory_num_id>/<scan>` structure the image and
PageXML directories use). Crucially, labels reference PageXML ids (`scan_id`, line `id`) rather
than re-encoding coordinates, so they stay valid however the underlying scan/thumbnail files are
organised on disk.

Cross-page document elements (task 4) don't belong to a single scan, so they get one file per
inventory number instead:
`data/annotations/<institute_id>/<archive_id>/<inventory_num_id>/elements-<inventory_num_id>.json`.
"""
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Dict, FrozenSet, List, Optional, Tuple

import pagexml.model.physical_document_model as pdm

from archival_structures.datasets.images_transcriptions import DATA_DIR

ANNOTATIONS_DIR = DATA_DIR / 'annotations'


@dataclass
class OpeningLabel:
    """Ground truth for task 1: is this scan a two-page opening, and if so, where's the split?"""

    is_opening: bool
    separation_x: Optional[float] = None


@dataclass
class ScanAnnotation:
    """All ground-truth labels for a single scan."""

    scan_id: str
    opening: Optional[OpeningLabel] = None
    page_layout: Optional[str] = None
    # line id -> line-type label (task 3), e.g. {"r1-line1": "body", "r2-line1": "closing"}
    lines: Dict[str, Optional[str]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """This annotation as a plain (JSON-serialisable) dict."""
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "ScanAnnotation":
        """Rebuild a `ScanAnnotation` from a dict produced by `to_dict`."""
        opening = OpeningLabel(**data['opening']) if data.get('opening') else None
        return ScanAnnotation(scan_id=data['scan_id'], opening=opening,
                              page_layout=data.get('page_layout'), lines=data.get('lines', {}))


@dataclass
class ElementSpan:
    """The part of a document element that falls on one particular scan."""

    scan_id: str
    line_ids: List[str]


@dataclass
class Element:
    """Ground truth for task 4: a (possibly cross-page) document/page element, e.g. a 'closing'
    that starts at the bottom of one scan and continues onto the next."""

    element_type: str
    spans: List[ElementSpan]

    def to_dict(self) -> dict:
        """This element as a plain (JSON-serialisable) dict."""
        return {'element_type': self.element_type, 'spans': [asdict(s) for s in self.spans]}

    @staticmethod
    def from_dict(data: dict) -> "Element":
        """Rebuild an `Element` from a dict produced by `to_dict`."""
        return Element(element_type=data['element_type'],
                       spans=[ElementSpan(**s) for s in data['spans']])


def scan_annotation_path(institute_id: str, archive_id: str, inventory_num_id: str, scan_id: str,
                         annotations_dir: Path = ANNOTATIONS_DIR) -> Path:
    """Path a `ScanAnnotation` for `scan_id` should be saved to/loaded from (see the module
    docstring for the directory convention)."""
    return annotations_dir / institute_id / archive_id / inventory_num_id / f"annotations-{scan_id}.json"


def elements_path(institute_id: str, archive_id: str, inventory_num_id: str,
                  annotations_dir: Path = ANNOTATIONS_DIR) -> Path:
    """Path the `Element` list for inventory number `inventory_num_id` should be saved
    to/loaded from (see the module docstring for the directory convention)."""
    return annotations_dir / institute_id / archive_id / inventory_num_id / f"elements-{inventory_num_id}.json"


def new_scan_annotation(scan: pdm.PageXMLScan) -> ScanAnnotation:
    """Build an empty `ScanAnnotation` skeleton for `scan`, with one (unlabelled) entry per text
    line, so an annotator can see exactly which line ids need a label."""
    lines = {line.id: None for line in scan.get_lines()}
    return ScanAnnotation(scan_id=scan.id, lines=lines)


def save_scan_annotation(annotation: ScanAnnotation, path: Path) -> None:
    """Write `annotation` to `path` as JSON, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as fh:
        json.dump(annotation.to_dict(), fh, indent=2, sort_keys=True)


def load_scan_annotation(path: Path) -> ScanAnnotation:
    """Load a `ScanAnnotation` previously written with `save_scan_annotation`."""
    with open(path) as fh:
        return ScanAnnotation.from_dict(json.load(fh))


def save_elements(elements: List[Element], path: Path) -> None:
    """Write `elements` to `path` as JSON, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as fh:
        json.dump([element.to_dict() for element in elements], fh, indent=2, sort_keys=True)


def load_elements(path: Path) -> List[Element]:
    """Load an `Element` list previously written with `save_elements`."""
    with open(path) as fh:
        return [Element.from_dict(item) for item in json.load(fh)]


def load_opening_labels(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all `opening` labels under `annotations_dir` (searched recursively) into a
    DataFrame with columns scan_id, is_opening, separation_x."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        if annotation.opening is not None:
            rows.append({'scan_id': annotation.scan_id, 'is_opening': annotation.opening.is_opening,
                        'separation_x': annotation.opening.separation_x})
    return pd.DataFrame(rows, columns=['scan_id', 'is_opening', 'separation_x'])


def load_page_layout_labels(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all `page_layout` labels under `annotations_dir` into a DataFrame with columns
    scan_id, page_layout."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        if annotation.page_layout is not None:
            rows.append({'scan_id': annotation.scan_id, 'page_layout': annotation.page_layout})
    return pd.DataFrame(rows, columns=['scan_id', 'page_layout'])


def load_line_labels(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all per-line labels under `annotations_dir` into a DataFrame with columns
    scan_id, line_id, label."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        for line_id, label in annotation.lines.items():
            if label is not None:
                rows.append({'scan_id': annotation.scan_id, 'line_id': line_id, 'label': label})
    return pd.DataFrame(rows, columns=['scan_id', 'line_id', 'label'])


OPENING_TAG = 'book_opening'
PAGE_LAYOUT_TAGS: FrozenSet[str] = frozenset({'table', 'title_page', 'book_cover'})


def parse_thumb_path(image_path: str) -> Tuple[str, str, str, str]:
    """Resolve a `<institute_id>/<archive_id>/<inventory_num_id-or-bare-number>/<scan_id>`
    image path (as used by `archival_structures.stream_analysis.groundtruth
    .interactive_annotation`'s `{image_path: [label, ...]}` label files) into
    `(institute_id, archive_id, inventory_num_id, scan_id)`.

    The third path component is accepted either as a bare inventory number (e.g. `'148'`,
    the convention `data/thumbs/...` directories actually use) or as the full
    `<archive_id>_<number>` id (the convention `data/PageXML/...` directories use) --
    `inventory_num_id` is always returned in the latter, full form.

    Only correct for archives where the thumbnail filename is an exact match to the PageXML
    `scan.id` (true for `NL-HaNA`, per `docs/findings.md`'s thumbnail-filename-convention
    finding -- not necessarily true for archives that use a glob pattern or positional
    pairing instead)."""
    parts = PurePosixPath(image_path).parts
    scan_id, raw_inv, archive_id, institute_id = parts[-1], parts[-2], parts[-3], parts[-4]
    inventory_num_id = raw_inv if raw_inv.startswith(f"{archive_id}_") else f"{archive_id}_{raw_inv}"
    return institute_id, archive_id, inventory_num_id, scan_id


def import_bulk_image_labels(labels_path: Path, annotations_dir: Path = ANNOTATIONS_DIR,
                             opening_tag: str = OPENING_TAG,
                             page_layout_tags: FrozenSet[str] = PAGE_LAYOUT_TAGS) -> Dict[str, int]:
    """Import labels from a bulk image tagger's `{image_path: [label, ...]}` JSON (as written
    by `archival_structures.stream_analysis.groundtruth.interactive_annotation.save_labels`)
    into the per-scan `ScanAnnotation` ground truth used by the text-analysis tasks.

    `opening_tag`'s presence/absence on an image sets `opening.is_opening` (`separation_x`
    stays unset -- the bulk tagger has no spatial annotation, only whole-image tags). If
    exactly one of `page_layout_tags` is present on an image, it sets `page_layout` to that
    tag; if more than one is present (not expected -- `page_layout` is a single string field,
    and in practice these tags don't co-occur), that image is skipped and counted in
    `'skipped_ambiguous'` instead of guessing.

    Never overwrites a field already set on an existing saved `ScanAnnotation` -- only fills
    in `opening`/`page_layout` where currently `None`. There's no way to tell an unreviewed
    auto-suggestion from a human-confirmed value once saved, so the safe default is to treat
    anything already on disk as authoritative. Lines and any other existing fields are left
    untouched.

    Returns counts: `{'scans_seen', 'opening_set', 'page_layout_set', 'skipped_ambiguous'}`."""
    with open(labels_path) as fh:
        raw_labels: Dict[str, List[str]] = json.load(fh)

    counts = {'scans_seen': 0, 'opening_set': 0, 'page_layout_set': 0, 'skipped_ambiguous': 0}

    for image_path, labels in raw_labels.items():
        institute_id, archive_id, inventory_num_id, scan_id = parse_thumb_path(image_path)
        counts['scans_seen'] += 1
        path = scan_annotation_path(institute_id, archive_id, inventory_num_id, scan_id,
                                    annotations_dir=annotations_dir)
        annotation = load_scan_annotation(path) if path.exists() else ScanAnnotation(scan_id=scan_id)

        changed = False
        if annotation.opening is None:
            annotation.opening = OpeningLabel(is_opening=opening_tag in labels)
            counts['opening_set'] += 1
            changed = True

        if annotation.page_layout is None:
            present = sorted(page_layout_tags.intersection(labels))
            if len(present) > 1:
                counts['skipped_ambiguous'] += 1
            elif len(present) == 1:
                annotation.page_layout = present[0]
                counts['page_layout_set'] += 1
                changed = True

        if changed:
            save_scan_annotation(annotation, path)

    return counts


def _box_overlap_fraction(line: pdm.PageXMLTextLine, box: Dict[str, float]) -> float:
    """Fraction of `line`'s own box area covered by `box` (a dict with `x`/`y`/`w`/`h`)."""
    lx0, ly0 = line.coords.left, line.coords.top
    lx1, ly1 = lx0 + line.coords.width, ly0 + line.coords.height
    bx0, by0 = box['x'], box['y']
    bx1, by1 = bx0 + box['w'], by0 + box['h']
    ix0, iy0 = max(lx0, bx0), max(ly0, by0)
    ix1, iy1 = min(lx1, bx1), min(ly1, by1)
    iw, ih = max(0.0, ix1 - ix0), max(0.0, iy1 - iy0)
    line_area = line.coords.width * line.coords.height
    return (iw * ih) / line_area if line_area else 0.0


def migrate_legacy_region_annotations(annotations_dir: Path = ANNOTATIONS_DIR,
                                      pagexml_dir: Optional[Path] = None,
                                      min_overlap: float = 0.5) -> Dict[str, int]:
    """One-off migration: convert `annotations-<scan>.json` files written by the older
    `image_drawing.ThumbnailSelectionTagger` region-drawing tool (a JSON list of
    `{thumb_box, orig_box}` region dicts, predating `ScanAnnotation` but sharing its
    directory/filename convention) into proper `ScanAnnotation.lines` entries -- so they stop
    crashing `load_scan_annotation`/the `load_*` aggregate functions (which expect every
    `annotations-*.json` file to be `ScanAnnotation`-shaped) and become usable ground truth
    for task 3 (line clustering).

    Each region marks a whole zone (e.g. a marginal-note column, a multi-line closing block)
    rather than a single line, so every PageXML line whose own box overlaps a region by at
    least `min_overlap` (fraction of the *line's* area covered) gets that region's label
    written to `ScanAnnotation.lines[line_id]`. Only fills in lines not already labelled.

    The original file's content is preserved alongside it as `legacy-<original filename>` (a
    name that doesn't match the `annotations-*.json` glob) before being overwritten with the
    migrated `ScanAnnotation`, so nothing is lost if a region happens to be mismatched.

    Requires the matching PageXML to be available locally (under `pagexml_dir`, defaulting to
    `archival_structures.datasets.images_transcriptions.PAGEXML_DIR`, in the
    `<institute_id>/<archive_id>/<inventory_num_id>/<scan-stem>.xml` directory convention) to
    look up line boxes -- scans without it are skipped and counted in `'skipped_no_pagexml'`.

    Returns counts: `{'files_seen', 'files_migrated', 'lines_labelled',
    'skipped_no_pagexml'}`."""
    import pagexml.parser as pagexml_parser

    if pagexml_dir is None:
        from archival_structures.datasets.images_transcriptions import PAGEXML_DIR
        pagexml_dir = PAGEXML_DIR

    counts = {'files_seen': 0, 'files_migrated': 0, 'lines_labelled': 0, 'skipped_no_pagexml': 0}

    for path in Path(annotations_dir).rglob('annotations-*.json'):
        with open(path) as fh:
            raw = json.load(fh)
        if not isinstance(raw, list):
            continue  # already a proper ScanAnnotation file, nothing to migrate

        counts['files_seen'] += 1
        institute_id, archive_id, inventory_num_id = path.parts[-4], path.parts[-3], path.parts[-2]
        scan_stem = path.name[len('annotations-'):-len('.json')].rsplit('.', 1)[0]
        xml_matches = list((Path(pagexml_dir) / institute_id / archive_id / inventory_num_id)
                           .glob(f"{scan_stem}.xml"))
        if not xml_matches:
            counts['skipped_no_pagexml'] += 1
            continue

        scan = pagexml_parser.parse_pagexml_file(str(xml_matches[0]))
        annotation = new_scan_annotation(scan)
        for region in raw:
            box = region['orig_box']
            label = box['label']
            for line in scan.get_lines():
                if annotation.lines.get(line.id) is None and _box_overlap_fraction(line, box) >= min_overlap:
                    annotation.lines[line.id] = label
                    counts['lines_labelled'] += 1

        backup_path = path.with_name(f"legacy-{path.name}")
        if not backup_path.exists():
            with open(backup_path, 'w') as fh:
                json.dump(raw, fh, indent=2)
        save_scan_annotation(annotation, path)
        counts['files_migrated'] += 1

    return counts
