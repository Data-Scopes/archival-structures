"""Detection and characterisation of significant whitespace regions within pages.

PageXML transcriptions record text but not the *absence* of text. Significant whitespace areas
-- a large gap between two text blocks on a notary deed, or an empty half of a two-page table
opening -- carry structural information: they often mark document or section boundaries.

Empty regions are computed geometrically from the page's text content via
`pagexml.helper.spatial_helper.make_empty_regions`, which iteratively carves the full page
bounding box by all text regions to find residual whitespace rectangles. Because that procedure
produces many tiny artefact gaps for densely-typeset pages (inter-cell spacing in a table
register, rounding in ATR coordinates), a minimum-size filter is essential:
`compute_page_empty_regions` keeps only regions large enough relative to the full page.

`find_boundary_adjacent_symbols` connects empty regions to relational line-neighbourhood
analysis (`relational_patterns`): for each significant empty region it finds the immediately
adjacent lines (above and below) and computes the full set of RCC-8 relational symbols for
those lines, enabling `boundary_symbol_scores` to flag which symbols are systematically
over-represented at structural whitespace boundaries.

Performance note
----------------
`make_empty_regions` uses a BFS/DFS carving algorithm whose runtime is quadratic in the
number of text regions per page.  Pages where the ATR pipeline creates one
``PageXMLTextRegion`` per table cell (70-200+ regions/page) can therefore take minutes per
page.  `compute_page_empty_regions` automatically pre-groups regions into vertical blocks
with `group_regions_into_blocks` before calling `make_empty_regions`, reducing the effective
region count from hundreds to single digits without losing any whitespace gap that is large
enough to survive the ``min_rel_height`` filter.
"""

from collections import Counter
from typing import Dict, List, Optional, Sequence, Tuple, Union

import hdbscan
import numpy as np
import pandas as pd
import pagexml.model.physical_document_model as pdm
from pagexml.helper.spatial_helper import make_empty_regions
from sklearn.preprocessing import normalize

from archival_structures.analysis import region_calculus
from archival_structures.analysis.neighbourhood_analysis import LineNeighbourHood

VERTICAL_DIRECTIONS = {'above', 'below'}
HORIZONTAL_DIRECTIONS = {'left', 'right'}

_GROUP_REGION_THRESHOLD = 20


def group_regions_into_blocks(
    regions: List[pdm.PageXMLRegion],
    max_vertical_gap: int,
) -> List[pdm.PageXMLRegion]:
    """Merge text regions into vertical blocks by proximity.

    Regions are sorted by their top coordinate; a new block starts whenever the
    gap between the *next* region's top and the *current block's maximum bottom*
    exceeds ``max_vertical_gap``.  Returns one merged ``PageXMLRegion`` per block
    whose bounding box is the union of all constituent regions' bounding boxes.

    Setting ``max_vertical_gap`` to roughly 70 % of the minimum-significant-region
    height (e.g. ``int(0.7 * min_rel_height * page_height)``) ensures that every
    vertical gap large enough to survive the size filter is *not* collapsed, so the
    set of significant empty regions found after grouping is the same as without it.

    The main use case is pages where the ATR pipeline creates one text region per
    table cell (70-200+ regions): grouping reduces them to 2-5 column-band blocks,
    cutting ``make_empty_regions`` runtime from minutes to milliseconds.
    """
    if not regions:
        return []
    sorted_r = sorted(regions, key=lambda r: r.coords.top)
    groups: List[List[pdm.PageXMLRegion]] = [[sorted_r[0]]]
    group_max_bottom = [sorted_r[0].coords.bottom]
    for r in sorted_r[1:]:
        if r.coords.top - group_max_bottom[-1] <= max_vertical_gap:
            groups[-1].append(r)
            group_max_bottom[-1] = max(group_max_bottom[-1], r.coords.bottom)
        else:
            groups.append([r])
            group_max_bottom.append(r.coords.bottom)
    merged = []
    for i, g in enumerate(groups):
        left   = min(r.coords.left   for r in g)
        top    = min(r.coords.top    for r in g)
        right  = max(r.coords.right  for r in g)
        bottom = max(r.coords.bottom for r in g)
        coords = pdm.Coords(
            [(left, top), (right, top), (right, bottom), (left, bottom)]
        )
        merged.append(
            pdm.PageXMLRegion(doc_id=f'block_{i}', doc_type='text_region', coords=coords)
        )
    return merged


class _PageProxy:
    """Duck-typed page wrapper used to pass pre-grouped regions to make_empty_regions."""

    def __init__(self, page: pdm.PageXMLScan,
                 grouped_regions: List[pdm.PageXMLRegion]) -> None:
        self.id = page.id
        self.coords = page.coords
        self._grouped = grouped_regions

    def get_textual_regions(self) -> List[pdm.PageXMLRegion]:
        return self._grouped

    def get_lines(self) -> list:
        return []


def compute_page_empty_regions(
    page: pdm.PageXMLScan,
    min_rel_height: float = 0.03,
    min_rel_width: float = 0.05,
) -> List[pdm.PageXMLRegion]:
    """Significant whitespace regions within `page`, filtered by minimum relative size.

    Calls `pagexml.helper.spatial_helper.make_empty_regions` to compute all whitespace
    rectangles geometrically, then retains only those whose height is at least
    `min_rel_height` of the page height and whose width is at least `min_rel_width` of
    the page width. The defaults eliminate inter-line spacing and table-cell gaps while
    keeping structurally meaningful blank areas.

    When the page has more than 20 text regions, regions are first grouped into vertical
    blocks with `group_regions_into_blocks` (gap threshold = 70 % of
    ``min_rel_height × page_height``) before calling ``make_empty_regions``.  This
    reduces runtime from quadratic to near-constant in the number of input regions
    without affecting which whitespace gaps survive the size filter.
    """
    if page.coords is None:
        return []
    page_h = page.coords.height
    page_w = page.coords.width
    if page_h == 0 or page_w == 0:
        return []
    regions = page.get_textual_regions()
    if len(regions) > _GROUP_REGION_THRESHOLD:
        max_gap = int(0.7 * min_rel_height * page_h)
        grouped = group_regions_into_blocks(regions, max_gap)
        doc = _PageProxy(page, grouped)
    else:
        doc = page
    empty = make_empty_regions(doc)
    return [
        er for er in empty
        if er.coords.height / page_h >= min_rel_height
        and er.coords.width / page_w >= min_rel_width
    ]


def _classify_location(rel_top: float, rel_left: float,
                        rel_height: float, rel_width: float) -> str:
    """Label the spatial position of an empty region on the page."""
    if rel_top < 0.1:
        return 'top'
    if rel_top + rel_height > 0.9:
        return 'bottom'
    if rel_left < 0.1:
        return 'left'
    if rel_left + rel_width > 0.9:
        return 'right'
    return 'interior'


def extract_empty_region_features(
    page: pdm.PageXMLScan,
    empty_region: pdm.PageXMLRegion,
) -> dict:
    """Feature dict for one empty region on `page`.

    Keys: ``page_id``, ``region_id``, ``rel_height``, ``rel_width``,
    ``aspect_ratio`` (height/width; >1 = tall, <1 = wide), ``rel_top``,
    ``rel_left``, ``location`` ('top'/'bottom'/'left'/'right'/'interior').
    """
    page_h = page.coords.height
    page_w = page.coords.width
    rel_height = empty_region.coords.height / page_h
    rel_width = empty_region.coords.width / page_w
    rel_top = empty_region.coords.top / page_h
    rel_left = empty_region.coords.left / page_w
    aspect_ratio = (empty_region.coords.height / empty_region.coords.width
                    if empty_region.coords.width > 0 else float('inf'))
    location = _classify_location(rel_top, rel_left, rel_height, rel_width)
    return {
        'page_id': page.id,
        'region_id': empty_region.id,
        'rel_height': rel_height,
        'rel_width': rel_width,
        'aspect_ratio': aspect_ratio,
        'rel_top': rel_top,
        'rel_left': rel_left,
        'location': location,
    }


def extract_corpus_empty_region_features(
    pages: List[pdm.PageXMLScan],
    min_rel_height: float = 0.03,
    min_rel_width: float = 0.05,
) -> pd.DataFrame:
    """One row per (page, empty region) across `pages`, with all feature columns.

    Pages with no significant empty regions (after filtering) contribute no rows.
    """
    rows = []
    for page in pages:
        for er in compute_page_empty_regions(page, min_rel_height=min_rel_height,
                                             min_rel_width=min_rel_width):
            rows.append(extract_empty_region_features(page, er))
    return pd.DataFrame(rows)


def cluster_empty_regions(
    df: pd.DataFrame,
    features: Sequence[str] = ('aspect_ratio', 'rel_height', 'rel_width', 'rel_top', 'rel_left'),
    min_cluster_size: int = 5,
) -> pd.Series:
    """Cluster the empty regions in `df` by their geometric features.

    L2-normalises rows before HDBSCAN (matching `cluster_page_layouts` idiom) so that
    overall region size doesn't dominate over shape and position. Returns cluster labels
    (-1 = noise) as a `pd.Series` indexed by ``(page_id, region_id)`` MultiIndex.
    """
    if df.empty:
        idx = pd.MultiIndex.from_tuples([], names=['page_id', 'region_id'])
        return pd.Series([], index=idx, dtype=int, name='cluster')
    X = normalize(df[list(features)].to_numpy(dtype=float))
    labels = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size,
                              metric='euclidean',
                              cluster_selection_method='eom').fit_predict(X)
    index = pd.MultiIndex.from_arrays([df['page_id'], df['region_id']],
                                      names=['page_id', 'region_id'])
    return pd.Series(labels, index=index, name='cluster')


def _line_above(lines: List[pdm.PageXMLTextLine],
                er_top: int) -> Optional[pdm.PageXMLTextLine]:
    """Text line whose bottom is closest to `er_top` from above (bottom <= er_top)."""
    candidates = [l for l in lines if l.coords.bottom <= er_top]
    if not candidates:
        return None
    return max(candidates, key=lambda l: l.coords.bottom)


def _line_below(lines: List[pdm.PageXMLTextLine],
                er_bottom: int) -> Optional[pdm.PageXMLTextLine]:
    """Text line whose top is closest to `er_bottom` from below (top >= er_bottom)."""
    candidates = [l for l in lines if l.coords.top >= er_bottom]
    if not candidates:
        return None
    return min(candidates, key=lambda l: l.coords.top)


def _get_relation(line: pdm.PageXMLTextLine, neighbour: pdm.PageXMLTextLine,
                  direction: str, max_diff: int) -> str:
    if direction in VERTICAL_DIRECTIONS:
        return region_calculus.get_vertical_region_relation(line, neighbour, max_diff=max_diff)
    return region_calculus.get_horizontal_region_relation(line, neighbour, max_diff=max_diff)


def find_boundary_adjacent_symbols(
    pages: List[pdm.PageXMLScan],
    line_clusters: pd.Series,
    page_empty_regions: Dict[str, List[pdm.PageXMLRegion]],
    max_vertical_dist: Optional[int] = None,
    max_diff: int = 20,
    directions: Sequence[str] = ('below', 'right'),
) -> pd.DataFrame:
    """Relational line-neighbourhood symbols adjacent to significant empty regions.

    For each empty region in `page_empty_regions[page.id]`, finds:
    - ``above_line``: the text line whose bottom is closest above the empty region's top
    - ``below_line``: the text line whose top is closest below the empty region's bottom

    For each adjacent line, builds a `LineNeighbourHood` and computes the RCC-8 relational
    symbols ``(own_cluster, direction, relation, neighbour_cluster)`` in `directions`,
    mirroring `RelationalPattern._compute_pattern_freq`.

    :param page_empty_regions: maps page id → list of empty regions to check (caller controls
        the size threshold -- use a larger `min_rel_height` than Feature 1 to focus on
        structural gaps rather than table-cell spacing).
    :return: DataFrame with columns ``page_id``, ``empty_region_id``, ``side``
        ('above'/'below'), ``line_id``, ``own_cluster``, ``direction``, ``relation``,
        ``neighbour_cluster``.
    """
    rows = []
    for page in pages:
        ers = page_empty_regions.get(page.id, [])
        if not ers:
            continue
        lines = [l for l in page.get_lines() if l.text]
        if not lines:
            continue
        neighbourhood = LineNeighbourHood(lines, max_vertical_dist=max_vertical_dist)
        for er in ers:
            adjacent: List[Tuple[str, pdm.PageXMLTextLine]] = []
            above = _line_above(lines, er.coords.top)
            if above is not None:
                adjacent.append(('above', above))
            below = _line_below(lines, er.coords.bottom)
            if below is not None:
                adjacent.append(('below', below))
            for side, line in adjacent:
                own_cluster = line_clusters.get((page.id, line.id), -1)
                for direction in directions:
                    neighbours = neighbourhood.get_rel_neighbour(line, direction)
                    if not neighbours:
                        continue
                    neighbour = neighbours[0]
                    relation = _get_relation(line, neighbour, direction, max_diff)
                    neighbour_cluster = line_clusters.get((page.id, neighbour.id), -1)
                    rows.append({
                        'page_id': page.id,
                        'empty_region_id': er.id,
                        'side': side,
                        'line_id': line.id,
                        'own_cluster': own_cluster,
                        'direction': direction,
                        'relation': relation,
                        'neighbour_cluster': neighbour_cluster,
                    })
    return pd.DataFrame(rows)


def boundary_symbol_scores(
    adjacent_df: pd.DataFrame,
    corpus_symbol_counts: Counter,
) -> pd.DataFrame:
    """For each relational symbol appearing adjacent to empty regions, compute its
    boundary affinity: ``boundary_count / corpus_count``.

    High affinity means the symbol is over-represented near whitespace boundaries
    relative to its overall corpus frequency.

    :param adjacent_df: output of `find_boundary_adjacent_symbols`.
    :param corpus_symbol_counts: a `Counter` of ``(own_cluster, direction, relation,
        neighbour_cluster)`` tuples across the whole corpus (e.g. from
        `RelationalPattern.coll_pattern_freq`).
    :return: DataFrame with columns ``own_cluster``, ``direction``, ``relation``,
        ``neighbour_cluster``, ``boundary_count``, ``corpus_count``, ``affinity``,
        sorted by ``affinity`` descending.
    """
    if adjacent_df.empty:
        return pd.DataFrame(columns=['own_cluster', 'direction', 'relation',
                                     'neighbour_cluster', 'boundary_count',
                                     'corpus_count', 'affinity'])
    symbol_cols = ['own_cluster', 'direction', 'relation', 'neighbour_cluster']
    boundary_counts = (
        adjacent_df[symbol_cols]
        .value_counts()
        .reset_index()
        .rename(columns={0: 'boundary_count', 'count': 'boundary_count'})
    )
    boundary_counts['corpus_count'] = boundary_counts.apply(
        lambda row: corpus_symbol_counts.get(
            (row['own_cluster'], row['direction'], row['relation'], row['neighbour_cluster']), 0
        ),
        axis=1,
    )
    boundary_counts['affinity'] = boundary_counts.apply(
        lambda row: row['boundary_count'] / row['corpus_count']
        if row['corpus_count'] > 0 else float('inf'),
        axis=1,
    )
    return boundary_counts.sort_values('affinity', ascending=False).reset_index(drop=True)
