"""Read/write ground-truth labels for the text-analysis tasks in `archival_structures.analysis`.

Labels are stored as plain JSON, one file per scan, under
`data/annotations/<institute_id>/<archive_id>/<inventory_num_id>/annotations-<scan_id>.json`
(an extension of the directory convention already used by `image_drawing.ThumbnailSelectionTagger`,
and matching the `<institute_id>/<archive_id>/<inventory_num_id>/<scan>` structure the image and
PageXML directories use). Crucially, labels reference PageXML ids (`scan_id`, line `id`) rather
than re-encoding coordinates, so they stay valid however the underlying scan/thumbnail files are
organised on disk.

`ScanAnnotation` carries tags (see `archival_structures.datasets.vocabulary` for the
`namespace:type(:subtype)?(#N)?` grammar) at three levels of granularity -- the whole scan, one
page (verso/recto) of a two-page opening, and individual lines/regions within a page. The same
tag strings are valid at every level; only what they're attached to changes.

Cross-page document elements (task 4) don't belong to a single scan, so they get one file per
inventory number instead:
`data/annotations/<institute_id>/<archive_id>/<inventory_num_id>/elements-<inventory_num_id>.json`.
"""
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Tuple

import pagexml.model.physical_document_model as pdm

from archival_structures.datasets.images_transcriptions import DATA_DIR
from archival_structures.datasets.vocabulary import DOCTYPE, GENERIC, make_tag

ANNOTATIONS_DIR = DATA_DIR / 'annotations'


@dataclass
class OpeningLabel:
    """Ground truth for task 1: is this scan a two-page opening, and if so, where's the split?"""

    is_opening: bool
    separation_x: Optional[float] = None


@dataclass
class RegionTag:
    """Tags for one region within a scan that doesn't correspond to a single PageXML line --
    e.g. a multi-line zone like a marginal-note column or a closing block. Identified either
    by an existing PageXML element id (`element_id`, preferred when the zone matches one --
    stays valid however the scan/thumbnail files are organised) or by a raw pixel box (`box`,
    the fallback the older `image_drawing.ThumbnailSelectionTagger` region-drawing tool used,
    for hand-drawn zones with no single matching PageXML element). A region's side (verso/
    recto, for a two-page opening) isn't stored separately -- derive it from `box`/the
    underlying PageXML element's coordinates against `ScanAnnotation.opening.separation_x`."""

    tags: List[str] = field(default_factory=list)
    element_id: Optional[str] = None
    box: Optional[Dict[str, float]] = None

    @staticmethod
    def from_dict(data: dict) -> "RegionTag":
        """Rebuild a `RegionTag` from a dict produced by `asdict`."""
        return RegionTag(tags=list(data.get('tags', [])), element_id=data.get('element_id'),
                         box=data.get('box'))


@dataclass
class ScanAnnotation:
    """All ground-truth labels for a single scan, at three levels of granularity:

    - `tags`: scan-level tags (e.g. `carrier:opening`, `generic:table`) -- apply to the whole
      image. For a single-page scan, this is also where content-type tags for that one page
      belong, since `pages` only applies to two-page openings.
    - `pages`: page-level tags, keyed by `'verso'`/`'recto'`, for scans that are two-page
      openings (`opening.is_opening`); empty for single-page scans.
    - `lines`: per-line tags (task 3), keyed by PageXML line id.
    - `regions`: tags for zones spanning more than one line, or with no PageXML id at all (see
      `RegionTag`).

    See `archival_structures.datasets.vocabulary` for the tag grammar and its four namespaces."""

    scan_id: str
    opening: Optional[OpeningLabel] = None
    tags: List[str] = field(default_factory=list)
    pages: Dict[str, List[str]] = field(default_factory=dict)
    lines: Dict[str, List[str]] = field(default_factory=dict)
    regions: List[RegionTag] = field(default_factory=list)

    def to_dict(self) -> dict:
        """This annotation as a plain (JSON-serialisable) dict."""
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "ScanAnnotation":
        """Rebuild a `ScanAnnotation` from a dict produced by `to_dict`."""
        opening = OpeningLabel(**data['opening']) if data.get('opening') else None
        return ScanAnnotation(
            scan_id=data['scan_id'], opening=opening,
            tags=list(data.get('tags', [])),
            pages={side: list(tags) for side, tags in data.get('pages', {}).items()},
            lines={line_id: list(tags) for line_id, tags in data.get('lines', {}).items()},
            regions=[RegionTag.from_dict(r) for r in data.get('regions', [])],
        )


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
    """Build an empty `ScanAnnotation` skeleton for `scan`, with one (untagged) entry per text
    line, so an annotator can see exactly which line ids need a label."""
    lines = {line.id: [] for line in scan.get_lines()}
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


def load_scan_tags(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all scan-level tags under `annotations_dir` into a long-format DataFrame with
    columns scan_id, tag (one row per scan per tag)."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        for tag in annotation.tags:
            rows.append({'scan_id': annotation.scan_id, 'tag': tag})
    return pd.DataFrame(rows, columns=['scan_id', 'tag'])


def load_page_tags(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all page-level tags (verso/recto, for two-page openings) under
    `annotations_dir` into a long-format DataFrame with columns scan_id, side, tag."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        for side, tags in annotation.pages.items():
            for tag in tags:
                rows.append({'scan_id': annotation.scan_id, 'side': side, 'tag': tag})
    return pd.DataFrame(rows, columns=['scan_id', 'side', 'tag'])


def load_line_tags(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all per-line tags under `annotations_dir` into a long-format DataFrame with
    columns scan_id, line_id, tag (one row per line per tag)."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        for line_id, tags in annotation.lines.items():
            for tag in tags:
                rows.append({'scan_id': annotation.scan_id, 'line_id': line_id, 'tag': tag})
    return pd.DataFrame(rows, columns=['scan_id', 'line_id', 'tag'])


def load_region_tags(annotations_dir: Path) -> "pd.DataFrame":
    """Collect all zone-level region tags (`ScanAnnotation.regions`) under `annotations_dir`
    into a long-format DataFrame with columns scan_id, element_id, box, tag (one row per
    region per tag)."""
    import pandas as pd
    rows = []
    for path in Path(annotations_dir).rglob('annotations-*.json'):
        annotation = load_scan_annotation(path)
        for region in annotation.regions:
            for tag in region.tags:
                rows.append({'scan_id': annotation.scan_id, 'element_id': region.element_id,
                            'box': region.box, 'tag': tag})
    return pd.DataFrame(rows, columns=['scan_id', 'element_id', 'box', 'tag'])


OPENING_TAG = 'book_opening'
BULK_LABEL_TAG_MAP: Dict[str, str] = {
    'table': make_tag(GENERIC, 'table'),
    'title_page': make_tag(GENERIC, 'title_page'),
    'book_cover': make_tag(GENERIC, 'cover'),
}


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
                             label_tag_map: Dict[str, str] = BULK_LABEL_TAG_MAP) -> Dict[str, int]:
    """Import labels from a bulk image tagger's `{image_path: [label, ...]}` JSON (as written
    by `archival_structures.stream_analysis.groundtruth.interactive_annotation.save_labels`)
    into the per-scan `ScanAnnotation` ground truth used by the text-analysis tasks.

    `opening_tag`'s presence/absence on an image sets `opening.is_opening` (`separation_x`
    stays unset -- the bulk tagger has no spatial annotation, only whole-image tags). Every
    other label present that has an entry in `label_tag_map` gets its mapped `generic:` tag
    added to `ScanAnnotation.tags` (skipped if already present) -- since tags are multi-valued
    there's no ambiguity to resolve the way a single `page_layout` string once had.

    Never overwrites `opening` if already set on an existing saved `ScanAnnotation` -- only
    fills it in when currently `None`. There's no way to tell an unreviewed auto-suggestion
    from a human-confirmed value once saved, so the safe default is to treat anything already
    on disk as authoritative. Tags are additive (a tag is only ever added, never removed), so
    they don't need the same guard. Lines/regions/pages are left untouched.

    Returns counts: `{'scans_seen', 'opening_set', 'tags_added'}`."""
    with open(labels_path) as fh:
        raw_labels: Dict[str, List[str]] = json.load(fh)

    counts = {'scans_seen': 0, 'opening_set': 0, 'tags_added': 0}

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

        for label in labels:
            tag = label_tag_map.get(label)
            if tag and tag not in annotation.tags:
                annotation.tags.append(tag)
                counts['tags_added'] += 1
                changed = True

        if changed:
            save_scan_annotation(annotation, path)

    return counts


LEGACY_REGION_LABEL_TAG_MAP: Dict[str, str] = {
    'table': make_tag(GENERIC, 'table'),
    'closing': make_tag(GENERIC, 'closing'),
    'marginalium': make_tag(GENERIC, 'marginalia'),
    'body': make_tag(GENERIC, 'running_text'),
    'paragraph': make_tag(GENERIC, 'running_text'),
    'seal': make_tag(GENERIC, 'seal'),
}


def _legacy_label_to_tag(label: str, label_tag_map: Dict[str, str]) -> str:
    """Map a free-text label from the old `ThumbnailSelectionTagger` format to a `generic:`
    tag via `label_tag_map`, or, for a label with no mapped entry, preserve it verbatim as
    `doctype:legacy:<label>` rather than silently dropping or guessing at it."""
    tag = label_tag_map.get(label)
    return tag if tag else make_tag(DOCTYPE, 'legacy', label)


def migrate_legacy_region_annotations(annotations_dir: Path = ANNOTATIONS_DIR,
                                      pagexml_dir: Optional[Path] = None,
                                      label_tag_map: Dict[str, str] = LEGACY_REGION_LABEL_TAG_MAP
                                      ) -> Dict[str, int]:
    """One-off migration: convert `annotations-<scan>.json` files written by the older
    `image_drawing.ThumbnailSelectionTagger` region-drawing tool (a JSON list of
    `{thumb_box, orig_box}` region dicts, predating `ScanAnnotation` but sharing its
    directory/filename convention) into proper `ScanAnnotation.regions` entries -- so they
    stop crashing `load_scan_annotation`/the `load_*` aggregate functions (which expect every
    `annotations-*.json` file to be `ScanAnnotation`-shaped).

    Each region keeps its own box (the legacy format was box-anchored, not id-anchored, so no
    attempt is made to resolve it down to one matching PageXML element) and gets one tag, via
    `label_tag_map` (`_legacy_label_to_tag` -- unmapped labels are preserved as
    `doctype:legacy:<label>` rather than dropped).

    The original file's content is preserved alongside it as `legacy-<original filename>` (a
    name that doesn't match the `annotations-*.json` glob) before being overwritten with the
    migrated `ScanAnnotation`.

    Requires the matching PageXML to be available locally (under `pagexml_dir`, defaulting to
    `archival_structures.datasets.images_transcriptions.PAGEXML_DIR`, in the
    `<institute_id>/<archive_id>/<inventory_num_id>/<scan-stem>.xml` directory convention) --
    only to recover `scan.id` and build the per-line skeleton (`new_scan_annotation`); scans
    without it are skipped and counted in `'skipped_no_pagexml'`.

    Returns counts: `{'files_seen', 'files_migrated', 'regions_migrated',
    'skipped_no_pagexml'}`."""
    import pagexml.parser as pagexml_parser

    if pagexml_dir is None:
        from archival_structures.datasets.images_transcriptions import PAGEXML_DIR
        pagexml_dir = PAGEXML_DIR

    counts = {'files_seen': 0, 'files_migrated': 0, 'regions_migrated': 0, 'skipped_no_pagexml': 0}

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
            tag = _legacy_label_to_tag(box['label'], label_tag_map)
            annotation.regions.append(RegionTag(
                tags=[tag], box={'x': box['x'], 'y': box['y'], 'w': box['w'], 'h': box['h']}))
            counts['regions_migrated'] += 1

        backup_path = path.with_name(f"legacy-{path.name}")
        if not backup_path.exists():
            with open(backup_path, 'w') as fh:
                json.dump(raw, fh, indent=2)
        save_scan_annotation(annotation, path)
        counts['files_migrated'] += 1

    return counts
