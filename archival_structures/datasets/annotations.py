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
from pathlib import Path
from typing import Dict, List, Optional

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
