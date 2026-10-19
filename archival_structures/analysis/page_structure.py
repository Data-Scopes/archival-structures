"""Page layout on pagexml-tools objects: page split, overlapping-region merge, columns, roles.

Step 15fp. Per scan, in this order:

1. ``split_scan_into_pages``: a portrait scan is one page (``single``); an opening is cut at a
   separation x (``find_separation_x``, around the per-inventory ``fit_gutter``) into ``verso``
   and ``recto``. Lines go by their box centre; a region with lines on both sides is split in
   two. Coordinates stay in scan space.
2. ``merge_overlapping_text_regions``, per page: regions that overlap
   (``pagexml.helper.pagexml_helper.regions_overlap``) are merged, repeated to a fixed point.
3. ``group_regions_into_columns`` and ``assign_column_roles``: columns of horizontally
   overlapping regions, each ``main`` or ``extra`` by its share of the page's characters.

Ported from the republic-project (https://github.com/HuygensING/republic-project, MIT licence,
same author): ``merge_overlapping_sets``, ``get_overlapping_text_regions`` and
``merge_overlapping_text_regions`` in ``republic/helper/pagexml_helper.py``, and the
column-assignment criterion of ``split_column_regions`` in
``republic/parser/pagexml/republic_page_parser.py``. Changes from the original:

* the transitive closure is deterministic (sets ordered by their first member in input order,
  members in input order); the original iterates Python sets, so its line order is arbitrary;
* a merged region's lines are sorted by (baseline mean y, box left), its metadata is a
  first-member-wins union plus ``merged_from``, and the merge repeats until no pair overlaps;
* republic's ``merge_extra_regions_with_columns`` (``page_date_parser.py``) is not ported: the
  column criterion already joins a margin region to the column it overlaps;
* republic's date machinery (``DateRegionClassifier`` etc.) is not ported: it needs neural line
  classes and REPUBLIC region types that SG does not have.

pagexml-tools 0.9.0 traps worked around here (UA-005): every region goes in ``page.columns`` or
``page.extra``, never ``page.text_regions``; ``set_parentage`` skips ``extra``, so the extra
regions get their parent set explicitly.

No text-layer imports: ``archival_structures.text_layer.layout`` adapts table rows to this.
"""

import copy
import dataclasses
import statistics
from itertools import combinations
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import regions_overlap
from pagexml.model.coords import parse_derived_coords

PAGES_OPENING = ("verso", "recto")
PAGE_SINGLE = "single"


@dataclasses.dataclass(frozen=True)
class LayoutParams:
    overlap_threshold: float = 0.5
    col_min_ratio: float = 0.5
    col_max_ratio: float = 0.75
    main_min_char_frac: float = 0.25
    min_gap_px: int = 20
    gutter_window_frac: float = 0.15
    portrait_ratio: float = 1.0   # a scan is an opening iff page_w > portrait_ratio * page_h

    def to_dict(self):
        return dataclasses.asdict(self)


def region_box_id(scan_id: str, coords: pdm.Coords) -> str:
    """The pagexml-tools derived region id (``set_derived_id``), as used for legacy ``doc_id``s."""
    return f"{scan_id}-text_region-{coords.x}-{coords.y}-{coords.w}-{coords.h}"


def hull_coords(docs: Sequence[pdm.PageXMLDoc]) -> pdm.Coords:
    """``parse_derived_coords`` (convex hull), or the bounding box if the hull is degenerate."""
    try:
        return parse_derived_coords(list(docs))
    except Exception:  # noqa: BLE001 -- Qhull on collinear points; fall back to the box
        points = [p for d in docs for p in d.coords.points]
        xs, ys = [p[0] for p in points], [p[1] for p in points]
        return pdm.Coords.coords_from_box_params(min(xs), min(ys), max(xs) - min(xs),
                                                 max(ys) - min(ys))


def _scan_id(region) -> str:
    return (region.metadata or {}).get("scan_id", "")


def _line_sort_key(line: pdm.PageXMLTextLine):
    if line.baseline is not None and line.baseline.points:
        y = sum(p[1] for p in line.baseline.points) / len(line.baseline.points)
    else:
        y = line.coords.y + line.coords.h / 2
    return (y, line.coords.x, line.coords.y, line.id or "")  # ties: box top, then id


def _new_region(lines, metadata, types=(), id_fn=region_box_id) -> pdm.PageXMLTextRegion:
    coords = hull_coords(lines)
    region = pdm.PageXMLTextRegion(doc_id="temp_id", metadata=metadata, coords=coords, lines=lines)
    for t in types:
        if t not in region.types:
            region.add_type(t)
    region.id = id_fn(metadata.get("scan_id", ""), coords)
    region.set_as_parent(lines)
    return region


# --- merge ----------------------------------------------------------------------------------

def get_overlapping_region_sets(regions: Sequence[pdm.PageXMLTextRegion],
                                overlap_threshold: float = 0.5) -> List[List[pdm.PageXMLTextRegion]]:
    """Connected components of the ``regions_overlap`` graph (port of republic's
    ``get_overlapping_text_regions`` + ``merge_overlapping_sets``), made deterministic: sets
    are ordered by their first member in input order, members keep input order. Every region
    is in exactly one set; singletons included."""
    n = len(regions)
    neighbours = [[] for _ in range(n)]
    for i, j in combinations(range(n), 2):
        if regions_overlap(regions[i], regions[j], threshold=overlap_threshold):
            neighbours[i].append(j)
            neighbours[j].append(i)
    seen = [False] * n
    sets = []
    for start in range(n):
        if seen[start]:
            continue
        seen[start] = True
        component, stack = [start], [start]
        while stack:
            for j in neighbours[stack.pop()]:
                if not seen[j]:
                    seen[j] = True
                    component.append(j)
                    stack.append(j)
        sets.append([regions[i] for i in sorted(component)])
    assert sum(len(s) for s in sets) == n
    return sets


def merge_regions(regions: Sequence[pdm.PageXMLTextRegion], *, union_keys: Tuple[str, ...] = (),
                  id_fn: Callable = region_box_id) -> pdm.PageXMLTextRegion:
    """Merge regions into one (port of the merge in republic's ``merge_overlapping_text_regions``).

    Lines are sorted by (baseline mean y, box left; ties by box top, then line id); coords are the hull of the lines; metadata
    is a union where the first member wins, except for ``union_keys``, whose list values are
    concatenated over the members (order kept, duplicates dropped). ``metadata["merged_from"]``
    lists the member ids, flattened through members that were themselves merged. Types are the
    union. The id is ``id_fn(scan_id, coords)``."""
    lines = sorted((line for r in regions for line in r.lines), key=_line_sort_key)
    metadata = copy.deepcopy(regions[0].metadata or {})
    for r in regions[1:]:
        for key, value in (r.metadata or {}).items():
            if key not in metadata:
                metadata[key] = copy.deepcopy(value)
    for key in union_keys + ("merged_from",):
        values = []
        for r in regions:
            member = (r.metadata or {}).get(key)
            if key == "merged_from" and not member:
                member = [r.id]
            for v in member or []:
                if v not in values:
                    values.append(v)
        metadata[key] = values
    types = []
    for r in regions:
        for t in sorted(r.types):
            if t not in types:
                types.append(t)
    return _new_region(lines, metadata, types, id_fn)


def merge_overlapping_text_regions(regions: Sequence[pdm.PageXMLTextRegion],
                                   params: LayoutParams = LayoutParams(), *,
                                   union_keys: Tuple[str, ...] = (),
                                   id_fn: Callable = region_box_id) -> Tuple[List, int]:
    """Merge overlapping regions until no pair overlaps (a merged hull can newly overlap a third
    region). A merged region takes the place of its first member. Regions not in any merge are
    returned as the same objects, ids unchanged. Returns (regions, number of passes that merged)."""
    current = list(regions)
    passes = 0
    while True:
        sets = get_overlapping_region_sets(current, params.overlap_threshold)
        if all(len(s) == 1 for s in sets):
            return current, passes
        passes += 1
        current = [s[0] if len(s) == 1 else merge_regions(s, union_keys=union_keys, id_fn=id_fn)
                   for s in sets]


# --- page split -----------------------------------------------------------------------------

def _coverage(boxes: Sequence[Tuple[int, int]], page_w: int) -> np.ndarray:
    """Number of line x-extents [x, x+w) covering each pixel column of the scan."""
    diff = np.zeros(page_w + 1, dtype=np.int64)
    for x, w in boxes:
        lo, hi = max(0, int(x)), min(page_w, int(x) + max(int(w), 1))
        if lo < hi:
            diff[lo] += 1
            diff[hi] -= 1
    return np.cumsum(diff[:-1])


def _zero_gaps(coverage: np.ndarray, lo: int, hi: int, min_gap_px: int) -> List[Tuple[int, int]]:
    """Zero-coverage runs [start, end) inside [lo, hi), clipped to it, at least min_gap_px wide."""
    lo, hi = max(0, lo), min(len(coverage), hi)
    if lo >= hi:
        return []
    zero = np.concatenate(([False], coverage[lo:hi] == 0, [False]))
    edges = np.flatnonzero(np.diff(zero.astype(np.int8)))
    runs = [(lo + int(a), lo + int(b)) for a, b in zip(edges[::2], edges[1::2])]
    return [(a, b) for a, b in runs if b - a >= min_gap_px]


def fit_gutter(scans: Sequence[Tuple[int, Sequence[Tuple[int, int]]]],
               params: LayoutParams = LayoutParams()) -> Tuple[float, int]:
    """Per-inventory gutter position. ``scans`` is [(page_w, [(line x, line w), ...])] for the
    landscape scans. Per scan, the centre of the widest zero-coverage gap (≥ min_gap_px) inside
    [0.35, 0.65]·page_w. Returns (median centre/page_w, number of scans with a gap); 0.5 if no
    scan has one."""
    rel = []
    for page_w, boxes in scans:
        if not page_w or not boxes:
            continue
        gaps = _zero_gaps(_coverage(boxes, page_w), int(0.35 * page_w), int(0.65 * page_w) + 1,
                          params.min_gap_px)
        if gaps:
            a, b = max(gaps, key=lambda g: (g[1] - g[0], -g[0]))
            rel.append((a + b) / 2 / page_w)
    return (statistics.median(rel) if rel else 0.5), len(rel)


def find_separation_x(boxes: Sequence[Tuple[int, int]], page_w: int, gutter_rel: float,
                      params: LayoutParams = LayoutParams()) -> Tuple[float, str]:
    """The x that separates verso from recto, and how it was found. ``gap``: the centre of the
    zero-coverage gap (≥ min_gap_px, clipped to the window) nearest ``gutter_rel·page_w``,
    within ±gutter_window_frac·page_w. ``min_coverage``: else the x of minimum coverage in that
    window, the one nearest the gutter on ties."""
    centre = gutter_rel * page_w
    lo = int(round(centre - params.gutter_window_frac * page_w))
    hi = int(round(centre + params.gutter_window_frac * page_w)) + 1
    lo, hi = max(0, lo), min(page_w, hi)
    coverage = _coverage(boxes, page_w)
    gaps = _zero_gaps(coverage, lo, hi, params.min_gap_px)
    if gaps:
        a, b = min(gaps, key=lambda g: (abs((g[0] + g[1]) / 2 - centre), g[0]))
        return (a + b) / 2, "gap"
    window = coverage[lo:hi]
    candidates = np.flatnonzero(window == window.min()) + lo
    x = int(min(candidates, key=lambda c: (abs(c - centre), c)))
    return float(x), "min_coverage"


def is_opening(page_w, page_h, params: LayoutParams = LayoutParams()) -> bool:
    return bool(page_w) and bool(page_h) and page_w > params.portrait_ratio * page_h


def _line_centre_x(line) -> float:
    return line.coords.x + line.coords.w / 2


def split_scan_into_pages(regions: Sequence[pdm.PageXMLTextRegion], separation_x: Optional[float],
                          *, id_fn: Callable = region_box_id
                          ) -> Tuple[Dict[str, List[pdm.PageXMLTextRegion]], int]:
    """Assign regions to pages. With ``separation_x=None`` (portrait) all go to ``single``.
    Otherwise lines go to verso (box centre < separation_x) or recto; a region with lines on both
    sides becomes two regions, lines in their order, ``metadata["split_from"] = [region id]``.
    Returns ({page: regions in input order}, number of regions split)."""
    if separation_x is None:
        return {PAGE_SINGLE: list(regions)}, 0
    pages = {"verso": [], "recto": []}
    n_split = 0
    for region in regions:
        sides = {"verso": [], "recto": []}
        for line in region.lines:
            sides["verso" if _line_centre_x(line) < separation_x else "recto"].append(line)
        if not sides["verso"] or not sides["recto"]:
            pages["verso" if sides["verso"] else "recto"].append(region)
            continue
        n_split += 1
        for side, lines in sides.items():
            metadata = copy.deepcopy(region.metadata or {})
            metadata["split_from"] = [region.id]
            pages[side].append(_new_region(lines, metadata, sorted(region.types), id_fn))
    return pages, n_split


# --- columns and roles ----------------------------------------------------------------------

def _joins_column(region, column_coords, params: LayoutParams) -> bool:
    """republic's ``split_column_regions`` criterion on horizontal pixel overlap."""
    probe = pdm.PageXMLColumn(coords=column_coords)
    overlap = pdm.get_horizontal_overlap(probe, region)
    tr_frac = overlap / (region.coords.width or 1)
    col_frac = overlap / column_coords.width if column_coords.width > 0 else 0
    return (min(tr_frac, col_frac) > params.col_min_ratio
            and max(tr_frac, col_frac) > params.col_max_ratio)


def group_regions_into_columns(regions: Sequence[pdm.PageXMLTextRegion],
                               params: LayoutParams = LayoutParams(), *, column_id_prefix: str = ""
                               ) -> List[pdm.PageXMLColumn]:
    """Regions sorted by left each join the first column (as it is so far) that meets the
    republic criterion, else start a new one. Columns are numbered by left edge (``metadata
    ["layout_column"]``, also set on their regions); regions in a column are ordered by top."""
    groups: List[List] = []
    hulls: List[pdm.Coords] = []
    for region in sorted(regions, key=lambda r: (r.coords.left, r.coords.top)):
        for i, coords in enumerate(hulls):
            if _joins_column(region, coords, params):
                groups[i].append(region)
                hulls[i] = hull_coords(groups[i])
                break
        else:
            groups.append([region])
            hulls.append(hull_coords([region]))
    order = sorted(range(len(groups)), key=lambda i: (hulls[i].left, hulls[i].top))
    columns = []
    for number, i in enumerate(order):
        members = sorted(groups[i], key=lambda r: (r.coords.top, r.coords.left))
        column = pdm.PageXMLColumn(doc_id=f"{column_id_prefix}-column-{hulls[i].box_string}",
                                   metadata={"layout_column": number}, coords=hulls[i],
                                   text_regions=members)
        column.set_as_parent(members)
        for r in members:
            r.metadata["layout_column"] = number
        columns.append(column)
    return columns


def region_chars(region) -> int:
    return sum(len(line.text or "") for line in region.lines)


def assign_column_roles(columns: Sequence[pdm.PageXMLColumn], params: LayoutParams = LayoutParams(),
                        *, page_id: str = "", page_coords: Optional[pdm.Coords] = None,
                        metadata: Optional[dict] = None) -> pdm.PageXMLPage:
    """A column is ``main`` if its characters are ≥ main_min_char_frac × those of the page's
    largest column, else ``extra``. Returns a page with ``columns`` = the main columns and
    ``extra`` = the regions of the extra columns; ``metadata["layout_role"]`` is set on columns
    and regions, and the parentage is set, including ``extra``."""
    chars = [sum(region_chars(r) for r in c.text_regions) for c in columns]
    top = max(chars, default=0)
    main, extra = [], []
    for column, n in zip(columns, chars):
        role = "main" if n >= params.main_min_char_frac * top else "extra"
        column.metadata["layout_role"] = role
        for r in column.text_regions:
            r.metadata["layout_role"] = role
        if role == "main":
            main.append(column)
        else:
            extra += column.text_regions
    if page_coords is None:  # PageXMLPage derives coords from its children, and raises on none
        page_coords = hull_coords(columns) if columns else pdm.Coords([(0, 0)])
    page = pdm.PageXMLPage(doc_id=page_id, metadata=dict(metadata or {}), coords=page_coords,
                           columns=main, extra=extra)
    pdm.set_parentage(page)
    page.set_as_parent(extra)  # set_parentage skips `extra` (pagexml-tools 0.9.0)
    for region in extra:
        region.set_as_parent(region.lines)
    return page
