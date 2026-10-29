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

pagexml-tools 0.9.1 (UA-005): every region goes in ``page.columns`` or ``page.extra``, never
``page.text_regions``. ``set_parentage`` reaches ``extra`` and ``PageXMLPage`` derives its own
coords when none are given, so neither is worked around any more.

No text-layer imports: ``archival_structures.text_layer.layout`` adapts table rows to this.
"""

import copy
import dataclasses
import re
import statistics
from itertools import combinations
from typing import Callable, Collection, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pagexml.model.physical_document_model as pdm
from pagexml.helper.pagexml_helper import regions_overlap
from pagexml.model.coords import parse_derived_coords

PAGES_OPENING = ("verso", "recto")
PAGE_SINGLE = "single"


# step 19b (phase 13, P2/P6): defaults of the geometric column split of hand regions
HAND_COLUMN_SPLIT_DEFAULTS = {"min_lines": 3, "min_gap_frac": 0.25, "max_col_frac": 0.65,
                              "min_col_w_frac": 0.3, "tol_h": 0.5, "min_overlap_h": 2.0,
                              "max_span_frac": 0.34,
                              # step 19f: "pieces" = 19b (split into pieces); "reorder" = keep the region
                              # whole and re-order its lines column by column (F13-R.7: B1 failed on
                              # extent-based span lines and piece order). span_reach_frac: reorder only
                              "mode": "pieces", "span_reach_frac": 0.5}


@dataclasses.dataclass(frozen=True)
class LayoutParams:
    overlap_threshold: float = 0.5
    col_min_ratio: float = 0.5
    col_max_ratio: float = 0.75
    main_min_char_frac: float = 0.25
    min_gap_px: int = 20
    gutter_window_frac: float = 0.15
    portrait_ratio: float = 1.0   # a scan is an opening iff page_w > portrait_ratio * page_h
    # step 15fq
    gutter_rule: str = "auto"          # "gap" (15fp) | "main_edge" | "auto"
    gap_share_min: float = 0.95        # auto: "gap" if n_scans_with_gap / n_openings_with_lines >= this
    long_line_frac: float = 0.6        # a line is long if w/page_w >= long_line_frac * med_w_rel
    min_long_lines: int = 5            # per side, else fall back to the gap rule
    edge_quantile: float = 0.95
    split_on_line_field: str = "column"   # "" = off
    furniture_patterns: tuple = ()     # regexes (re.IGNORECASE, fullmatch on the stripped joined text)
    furniture_max_lines: int = 2
    furniture_max_chars: int = 30
    catchword_band: float = 0.04       # 0 = off
    interleave_guard: bool = True
    interleave_width_frac: float = 0.5
    merge: bool = True                 # False = no merging at all
    # step 15fq2
    column_merge_guard: bool = True    # never merge regions whose lines carry different non-empty
                                       # values of ``split_on_line_field``
    # step 15ir
    sort_split_lines: bool = True      # sort the lines of each piece of a column-split region
                                       # (``_line_sort_key``); False = source order (15fp)
    # step 15j, P4: rows within row_tol_frac x median box h; planning measured baseline gaps
    # < 15 px within a row, about 48 px between rows, h about 75 px
    line_sort: str = "row"             # "baseline" (``_line_sort_key``) | "row"
    row_tol_frac: float = 0.2
    sort_unchanged_lines: bool = False  # layout: also sort the lines of ``unchanged`` regions
    # step 19b (phase 13, P2/P6): geometric split of hand regions that hold side-by-side columns;
    # None = off
    hand_column_split: Optional[dict] = dataclasses.field(default=None, hash=False)
    # step 19c (phase 13, P8): index-side detector (``analysis.index_pages``); None = off
    index_sides: Optional[dict] = dataclasses.field(default=None, hash=False)

    def __post_init__(self):
        if self.gutter_rule not in ("gap", "main_edge", "auto"):
            raise ValueError(f"gutter_rule must be 'gap', 'main_edge' or 'auto', "
                             f"got {self.gutter_rule!r}")
        if self.line_sort not in ("baseline", "row"):
            raise ValueError(f"line_sort must be 'baseline' or 'row', got {self.line_sort!r}")
        if not 0 < self.row_tol_frac < 1:
            raise ValueError(f"row_tol_frac must be in (0, 1), got {self.row_tol_frac!r}")
        if self.hand_column_split is not None:
            bad = set(self.hand_column_split) - set(HAND_COLUMN_SPLIT_DEFAULTS)
            if bad:
                raise ValueError(f"unknown hand_column_split keys: {sorted(bad)}")
            mode = self.hand_column_split.get("mode", "pieces")
            if mode not in ("pieces", "reorder"):
                raise ValueError(f"hand_column_split mode must be 'pieces' or 'reorder', got {mode!r}")
        if self.index_sides is not None:
            from .index_pages import INDEX_SIDES_DEFAULTS
            bad = set(self.index_sides) - set(INDEX_SIDES_DEFAULTS)
            if bad:
                raise ValueError(f"unknown index_sides keys: {sorted(bad)}")

    def to_dict(self):
        d = dataclasses.asdict(self)
        if self.hand_column_split is None:
            del d["hand_column_split"]   # keeps every v1-v4 params.json byte-identical
        else:
            d["hand_column_split"] = dict(self.hand_column_split)
        if self.index_sides is None:
            del d["index_sides"]   # keeps every v1-v4 params.json byte-identical
        else:
            d["index_sides"] = dict(self.index_sides)
        d["furniture_patterns"] = list(self.furniture_patterns)
        return d

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        if "furniture_patterns" in d:
            d["furniture_patterns"] = tuple(d["furniture_patterns"])
        return cls(**d)


LEGACY_15FP = LayoutParams(gutter_rule="gap", split_on_line_field="", furniture_patterns=(),
                           catchword_band=0.0, interleave_guard=False,
                           column_merge_guard=False, sort_split_lines=False,
                           line_sort="baseline")


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


def sort_lines_by_row(lines, row_tol_frac: float) -> list:
    """Sort lines into printed rows (step 15j, P4: rows within ``row_tol_frac`` x median box h;
    planning measured baseline gaps < 15 px within a row, about 48 px between rows, h about
    75 px). Lines are walked in ``_line_sort_key`` order; a line joins the current row when its
    baseline y minus the row's first line's baseline y is <= tol. Rows keep their order, each
    row is sorted by (box x, baseline y, box top, id)."""
    lines = list(lines)
    if not lines:
        return []
    tol = row_tol_frac * statistics.median(l.coords.h for l in lines)
    rows, row_y = [], None
    for line in sorted(lines, key=_line_sort_key):
        y = _line_sort_key(line)[0]
        if rows and y - row_y <= tol:
            rows[-1].append(line)
        else:
            rows.append([line])
            row_y = y
    return [l for row in rows
            for l in sorted(row, key=lambda l: (l.coords.x, _line_sort_key(l)[0], l.coords.y,
                                                l.id or ""))]


def sort_lines(lines, params: "LayoutParams") -> list:
    """Row sort or baseline sort, by ``params.line_sort``."""
    if params.line_sort == "row":
        return sort_lines_by_row(lines, params.row_tol_frac)
    return sorted(lines, key=_line_sort_key)


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

def _guard_blocks(a, b, width_frac: float) -> bool:
    """Interleave guard: True if one region (S) has only lines narrower than ``width_frac`` times
    the median line width of the other (L), and S's line y-range lies strictly inside L's."""
    for s_reg, l_reg in ((a, b), (b, a)):
        if not s_reg.lines or not l_reg.lines:
            continue
        med = statistics.median(line.coords.w for line in l_reg.lines)
        if not all(line.coords.w < width_frac * med for line in s_reg.lines):
            continue
        s_top = min(line.coords.y for line in s_reg.lines)
        s_bottom = max(line.coords.y + line.coords.h for line in s_reg.lines)
        l_top = min(line.coords.y for line in l_reg.lines)
        l_bottom = max(line.coords.y + line.coords.h for line in l_reg.lines)
        if s_top > l_top and s_bottom < l_bottom:
            return True
    return False


def get_overlapping_region_sets(regions: Sequence[pdm.PageXMLTextRegion],
                                overlap_threshold: float = 0.5, *,
                                exclude: Collection[str] = (),
                                guard_width_frac: Optional[float] = None,
                                blocked: Optional[Dict[Tuple[str, str], None]] = None,
                                column_field: str = "",
                                column_blocked: Optional[Dict[Tuple[str, str], None]] = None
                                ) -> List[List[pdm.PageXMLTextRegion]]:
    """Connected components of the ``regions_overlap`` graph (port of republic's
    ``get_overlapping_text_regions`` + ``merge_overlapping_sets``), made deterministic: sets
    are ordered by their first member in input order, members keep input order. Every region
    is in exactly one set; singletons included.

    Regions whose id is in ``exclude`` take no part (always singletons). With
    ``guard_width_frac`` set, an overlapping pair is not linked when ``_guard_blocks`` says so;
    such pairs are added to ``blocked`` (a dict used as an ordered set, keyed by id pair).

    With ``column_field`` set, two components whose lines carry, together, more than one distinct
    non-empty value of that line metadata field are never joined (union-find over the edges in
    ``combinations`` order; a ``''`` value conflicts with nothing). Skipped edges are added to
    ``column_blocked``."""
    n = len(regions)
    excluded = set(exclude)
    neighbours = [[] for _ in range(n)]
    edges = []
    for i, j in combinations(range(n), 2):
        if regions[i].id in excluded or regions[j].id in excluded:
            continue
        if regions_overlap(regions[i], regions[j], threshold=overlap_threshold):
            if guard_width_frac is not None and _guard_blocks(regions[i], regions[j],
                                                              guard_width_frac):
                if blocked is not None:
                    blocked[(regions[i].id, regions[j].id)] = None
                continue
            neighbours[i].append(j)
            neighbours[j].append(i)
            edges.append((i, j))
    if column_field:
        parent = list(range(n))
        vals = [{v for v in ((l.metadata or {}).get(column_field, "") for l in r.lines) if v}
                for r in regions]

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for i, j in edges:
            ri, rj = find(i), find(j)
            if ri == rj:
                continue
            if len(vals[ri] | vals[rj]) > 1:
                if column_blocked is not None:
                    column_blocked[(regions[i].id, regions[j].id)] = None
                continue
            parent[rj] = ri
            vals[ri] = vals[ri] | vals[rj]
        groups = {}
        for i in range(n):
            groups.setdefault(find(i), []).append(i)
        sets = [[regions[i] for i in g] for g in sorted(groups.values(), key=lambda g: g[0])]
        assert sum(len(g) for g in sets) == n
        return sets
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
                  id_fn: Callable = region_box_id, row_tol_frac: Optional[float] = None
                  ) -> pdm.PageXMLTextRegion:
    """Merge regions into one (port of the merge in republic's ``merge_overlapping_text_regions``).

    Lines are sorted by (baseline mean y, box left; ties by box top, then line id); coords are the hull of the lines; metadata
    is a union where the first member wins, except for ``union_keys``, whose list values are
    concatenated over the members (order kept, duplicates dropped). ``metadata["merged_from"]``
    lists the member ids, flattened through members that were themselves merged. Types are the
    union. The id is ``id_fn(scan_id, coords)``. With ``row_tol_frac`` (step 15j) the lines are
    sorted by ``sort_lines_by_row`` instead."""
    all_lines = [line for r in regions for line in r.lines]
    if row_tol_frac is None:
        lines = sorted(all_lines, key=_line_sort_key)
    else:
        lines = sort_lines_by_row(all_lines, row_tol_frac)
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
                                   id_fn: Callable = region_box_id,
                                   exclude: Collection[str] = (),
                                   blocked: Optional[Dict[Tuple[str, str], None]] = None,
                                   column_blocked: Optional[Dict[Tuple[str, str], None]] = None
                                   ) -> Tuple[List, int]:
    """Merge overlapping regions until no pair overlaps (a merged hull can newly overlap a third
    region). A merged region takes the place of its first member. Regions not in any merge are
    returned as the same objects, ids unchanged. Returns (regions, number of passes that merged).
    ``exclude`` (region ids) and the interleave guard (``params.interleave_guard``) keep regions
    out of merges; ``blocked`` collects the guard-blocked id pairs of the **final** regions (the
    last pass only; step 15ir, earlier passes counted a relation again under each merged id). The column guard
    (``params.column_merge_guard``, on ``params.split_on_line_field``) keeps regions with different
    non-empty column values apart; ``column_blocked`` collects those id pairs."""
    current = list(regions)
    passes = 0
    guard = params.interleave_width_frac if params.interleave_guard else None
    column_field = params.split_on_line_field if params.column_merge_guard else ""
    row_tol = params.row_tol_frac if params.line_sort == "row" else None
    while True:
        # fresh dicts per pass: only the last pass (pairs of final regions) goes to the caller
        pass_blocked, pass_col_blocked = {}, {}
        sets = get_overlapping_region_sets(current, params.overlap_threshold, exclude=exclude,
                                           guard_width_frac=guard, blocked=pass_blocked,
                                           column_field=column_field,
                                           column_blocked=pass_col_blocked)
        if all(len(s) == 1 for s in sets):
            if blocked is not None:
                blocked.update(pass_blocked)
            if column_blocked is not None:
                column_blocked.update(pass_col_blocked)
            return current, passes
        passes += 1
        current = [s[0] if len(s) == 1 else merge_regions(s, union_keys=union_keys, id_fn=id_fn,
                                                          row_tol_frac=row_tol)
                   for s in sets]


# --- column split on a line field, furniture -----------------------------------------------

def split_regions_on_line_field(regions: Sequence[pdm.PageXMLTextRegion], field: str, *,
                                id_fn: Callable = region_box_id, sort_lines: bool = False,
                                row_tol_frac: Optional[float] = None
                                ) -> Tuple[List[pdm.PageXMLTextRegion], int]:
    """Split each region into one region per value of ``line.metadata[field]`` (missing = '').
    A region with one value is returned as the same object. Otherwise the groups are ordered by
    first occurrence, lines keep their order (or, with ``sort_lines``, are sorted by
    ``_line_sort_key`` within each piece; or by ``sort_lines_by_row`` when ``row_tol_frac`` is given; never for an unsplit region), coords are the hull, and
    ``metadata["column_split_from"] = [original id]``. Returns (regions, number of regions split)."""
    out, n_splits = [], 0
    for region in regions:
        groups: Dict[str, list] = {}
        for line in region.lines:
            groups.setdefault((line.metadata or {}).get(field, ""), []).append(line)
        if len(groups) <= 1:
            out.append(region)
            continue
        n_splits += 1
        for lines in groups.values():
            if sort_lines:
                lines = (sorted(lines, key=_line_sort_key) if row_tol_frac is None
                         else sort_lines_by_row(lines, row_tol_frac))
            metadata = copy.deepcopy(region.metadata or {})
            metadata["column_split_from"] = [region.id]
            out.append(_new_region(lines, metadata, sorted(region.types), id_fn))
    return out, n_splits


def split_regions_into_columns(regions: Sequence[pdm.PageXMLTextRegion], cfg: dict, *,
                               id_fn: Callable = region_box_id,
                               row_tol_frac: Optional[float] = None
                               ) -> Tuple[List[pdm.PageXMLTextRegion], int]:
    """Geometric column split of regions that hold two side-by-side text columns.

    Rationale (phase 13 P2/P6, F12-R.22): a hand PageXML region with two columns lists its lines
    alternating between the columns, so the hand unit interleaves them (2,612 of 399,714
    main_text units in v4). Lines carry no ``column`` value in hand mode, so the field split
    never fires. Prototype on raw regions: 3471 had 165 of 5,857 regions split; the span guard
    removed the two fragmented-line splits of 3222. ``cfg`` overrides
    ``HAND_COLUMN_SPLIT_DEFAULTS``. A region is split at the largest x-gap between narrow lines
    when both sides hold >= ``min_lines`` lines of sufficient width that overlap vertically and
    few lines span the gap. Lines are walked in reading order; a span line (heading) closes the
    current left/right block. Unchanged regions are returned as the same object. Returns
    (regions, number of regions split)."""
    c = {**HAND_COLUMN_SPLIT_DEFAULTS, **cfg}
    if c["mode"] == "reorder":
        return _reorder_regions_by_column(regions, c, id_fn, row_tol_frac)
    out, n_splits = [], 0
    for region in regions:
        pieces = _column_pieces(region, c, row_tol_frac)
        if pieces is None:
            out.append(region)
            continue
        n_splits += 1
        for lines in pieces:
            metadata = copy.deepcopy(region.metadata or {})
            metadata["column_split_from"] = [region.id]
            out.append(_new_region(lines, metadata, sorted(region.types), id_fn))
    return out, n_splits


def _column_geometry(region, c: dict):
    """Steps 1-3 shared by both modes: (T, W, mh, sx, tol) or None when no gutter is found."""
    T = [l for l in region.lines if (l.text or "").strip()]
    k = c["min_lines"]
    if len(T) < 2 * k:
        return None
    x0 = min(l.coords.x for l in T)
    W = max(l.coords.x + l.coords.w for l in T) - x0
    mh = statistics.median(l.coords.h for l in T)
    N = sorted((l for l in T if l.coords.w <= c["max_col_frac"] * W),
               key=lambda l: (l.coords.x, l.coords.y, l.id or ""))
    if len(N) < 2 * k:
        return None
    gap, gi = max(((N[i + 1].coords.x - N[i].coords.x, -i) for i in range(len(N) - 1)))
    gi = -gi
    if gap < c["min_gap_frac"] * W:
        return None
    sx = N[gi + 1].coords.x
    tol = c["tol_h"] * mh
    return T, W, mh, sx, tol


def _reorder_lines(region, c: dict, row_tol_frac: Optional[float]):
    """Step 19f, mode "reorder": the region's lines re-ordered left column then right column
    (each line classed by where it starts), or None when a guard keeps the region."""
    geo = _column_geometry(region, c)
    if geo is None:
        return None
    T, W, mh, sx, tol = geo
    k = c["min_lines"]

    def order(lines):
        return (sort_lines_by_row(lines, row_tol_frac) if row_tol_frac is not None
                else sorted(lines, key=_line_sort_key))

    is_right = {id(l): l.coords.x >= sx - tol for l in region.lines}
    R = [l for l in T if is_right[id(l)]]
    if len(R) < k:
        return None
    wR = statistics.median(l.coords.w for l in R)
    left_T = [l for l in T if not is_right[id(l)]]
    wide = [l for l in left_T if l.coords.x + l.coords.w > sx + c["span_reach_frac"] * wR]
    wide_ids = {id(l) for l in wide}
    Ln = [l for l in left_T if id(l) not in wide_ids]
    if len(Ln) < k or len(wide) > c["max_span_frac"] * len(T):
        return None
    for part in (Ln, R):
        if statistics.median(l.coords.w for l in part) < c["min_col_w_frac"] * W:
            return None
    overlap = (min(max(l.coords.y + l.coords.h for l in Ln), max(l.coords.y + l.coords.h for l in R))
               - max(min(l.coords.y for l in Ln), min(l.coords.y for l in R)))
    if overlap < c["min_overlap_h"] * mh:
        return None
    new_order = (order([l for l in region.lines if not is_right[id(l)]])
                 + order([l for l in region.lines if is_right[id(l)]]))
    if [id(l) for l in new_order] == [id(l) for l in region.lines]:
        return None
    return new_order


def _reorder_regions_by_column(regions, c: dict, id_fn, row_tol_frac):
    out, n = [], 0
    for region in regions:
        new_order = _reorder_lines(region, c, row_tol_frac)
        if new_order is None:
            out.append(region)
            continue
        n += 1
        metadata = copy.deepcopy(region.metadata or {})
        metadata["column_reorder_from"] = [region.id]
        out.append(_new_region(new_order, metadata, sorted(region.types), id_fn))
    return out, n


def _column_pieces(region, c: dict, row_tol_frac: Optional[float]):
    geo = _column_geometry(region, c)
    if geo is None:
        return None
    T, W, mh, sx, tol = geo
    k = c["min_lines"]

    def side(l):
        if l.coords.x + l.coords.w <= sx + tol:
            return "L"
        return "R" if l.coords.x >= sx - tol else "S"

    sides = {id(l): side(l) for l in region.lines}
    left = [l for l in T if sides[id(l)] == "L"]
    right = [l for l in T if sides[id(l)] == "R"]
    span = [l for l in T if sides[id(l)] == "S"]
    if len(left) < k or len(right) < k or len(span) > c["max_span_frac"] * len(T):
        return None
    for part in (left, right):
        if statistics.median(l.coords.w for l in part) < c["min_col_w_frac"] * W:
            return None
    overlap = (min(max(l.coords.y + l.coords.h for l in left),
                   max(l.coords.y + l.coords.h for l in right))
               - max(min(l.coords.y for l in left), min(l.coords.y for l in right)))
    if overlap < c["min_overlap_h"] * mh:
        return None

    def order(lines):
        return (sort_lines_by_row(lines, row_tol_frac) if row_tol_frac is not None
                else sorted(lines, key=_line_sort_key))

    pieces, cur_l, cur_r, cur_s = [], [], [], []
    for l in sorted(region.lines, key=_line_sort_key):
        sd = sides[id(l)]
        if sd == "S":
            if cur_l or cur_r:
                pieces += [order(p) for p in (cur_l, cur_r) if p]
                cur_l, cur_r = [], []
            cur_s.append(l)
            continue
        if cur_s:
            pieces.append(order(cur_s))
            cur_s = []
        (cur_l if sd == "L" else cur_r).append(l)
    pieces += [order(p) for p in (cur_l, cur_r, cur_s) if p]
    return pieces


def furniture_region_ids(regions: Sequence[pdm.PageXMLTextRegion], page_text_bottom: float,
                         page_h: float, params: LayoutParams = LayoutParams()) -> Set[str]:
    """Ids of page furniture: regions with at most ``furniture_max_lines`` lines and
    ``furniture_max_chars`` characters (joined with spaces, stripped) whose text fullmatches one
    of ``furniture_patterns`` (IGNORECASE), or catch words: one line, one token of at most 15
    characters, box top at or below ``page_text_bottom - catchword_band * page_h``."""
    patterns = [re.compile(p, re.IGNORECASE) for p in params.furniture_patterns]
    found = set()
    for region in regions:
        if not region.lines or len(region.lines) > params.furniture_max_lines:
            continue
        text = " ".join((line.text or "") for line in region.lines).strip()
        if len(text) > params.furniture_max_chars:
            continue
        if any(p.fullmatch(text) for p in patterns):
            found.add(region.id)
        elif (params.catchword_band > 0 and len(region.lines) == 1 and len(text.split()) == 1
              and len(text) <= 15
              and region.lines[0].coords.y >= page_text_bottom - params.catchword_band * page_h):
            found.add(region.id)
    return found


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


def inventory_line_width_rel(scans: Sequence[Tuple[int, Sequence[Tuple[int, int]]]]) -> float:
    """Median of line width / page_w over all lines of the landscape scans (same input as
    ``fit_gutter``); 0.0 if there are none."""
    rel = [w / page_w for page_w, boxes in scans if page_w for _, w in boxes]
    return float(statistics.median(rel)) if rel else 0.0


def choose_gutter_rule(n_scans_with_gap: int, n_openings_with_lines: int,
                       params: LayoutParams = LayoutParams()) -> str:
    """``gap`` or ``main_edge``. Under ``auto``: ``gap`` if the share of openings with a gap in
    the gutter window is at least ``gap_share_min`` (or there are no openings), else ``main_edge``."""
    if params.gutter_rule != "auto":
        return params.gutter_rule
    if n_openings_with_lines <= 0:
        return "gap"
    share = n_scans_with_gap / n_openings_with_lines
    return "gap" if share >= params.gap_share_min else "main_edge"


def find_main_edge_x(lines_xw: Sequence[Tuple[int, int]], page_w: int, gutter_rel: float,
                     med_w_rel: float, params: LayoutParams = LayoutParams()) -> Optional[float]:
    """Separation from the main text of both pages: the smaller of the verso long lines' right
    edge (``edge_quantile``) and the recto long lines' left edge (1 - ``edge_quantile``).
    Long = ``w/page_w >= long_line_frac·med_w_rel``; verso/recto by box centre against
    ``gutter_rel·page_w``. None if either side has fewer than ``min_long_lines`` long lines or
    the result is further than ``gutter_window_frac·page_w`` from the gutter."""
    c0 = gutter_rel * page_w
    min_w = params.long_line_frac * med_w_rel * page_w
    verso_right, recto_left = [], []
    for x, w in lines_xw:
        if w < min_w:
            continue
        if x + w / 2 < c0:
            verso_right.append(x + w)
        else:
            recto_left.append(x)
    if len(verso_right) < params.min_long_lines or len(recto_left) < params.min_long_lines:
        return None
    se = min(float(np.quantile(verso_right, params.edge_quantile)),
             float(np.quantile(recto_left, 1 - params.edge_quantile)))
    if abs(se - c0) > params.gutter_window_frac * page_w:
        return None
    return se


def find_separation_x(boxes: Sequence[Tuple[int, int]], page_w: int, gutter_rel: float,
                      params: LayoutParams = LayoutParams(), *, rule: str = "gap",
                      lines_xw: Optional[Sequence[Tuple[int, int]]] = None,
                      med_w_rel: Optional[float] = None) -> Tuple[float, str]:
    """The x that separates verso from recto, and how it was found. ``main_edge`` (only with
    ``rule="main_edge"`` and a result from ``find_main_edge_x``): the edge of the two pages' main
    text. ``gap``: the centre of the
    zero-coverage gap (≥ min_gap_px, clipped to the window) nearest ``gutter_rel·page_w``,
    within ±gutter_window_frac·page_w. ``min_coverage``: else the x of minimum coverage in that
    window, the one nearest the gutter on ties."""
    if rule == "main_edge" and med_w_rel:
        se = find_main_edge_x(boxes if lines_xw is None else lines_xw, page_w, gutter_rel,
                              med_w_rel, params)
        if se is not None:
            return se, "main_edge"
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
    page = pdm.PageXMLPage(doc_id=page_id, metadata=dict(metadata or {}), coords=page_coords,
                           columns=main, extra=extra)
    pdm.set_parentage(page)
    return page
