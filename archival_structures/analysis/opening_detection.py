"""Detect and split two-page "book opening" scans.

A scan represents a book opening when it shows two facing pages side by side. The original
version of this module decided that from a height-profile gap between the two pages (see
`archival_structures.analysis.page`). Checked against a full real inventory number
(`NL-AsnDA_0114.11_1`, 630 scans, where in fact only the first and last scans are single pages),
that gap heuristic produced 18 false negatives: dense handwriting frequently bridges the gutter
with no cleanly detectable empty gap, even on genuine two-page spreads.

What does separate single pages from openings cleanly in that inventory is scan width relative
to the rest of the inventory: single pages are digitised at roughly half the width of an opening
(in this case, a ratio of ~0.52), since a flatbed/camera setup captures a consistent area
regardless of how many physical pages are on it. `is_opening`/`get_opening_features` therefore
use `corpus_median_width` (computed once per inventory number, see `compute_corpus_median_width`)
as the primary signal. The height-profile gap is kept as a secondary feature, mainly useful for
*locating* the split point via `separation_point` once a scan is already known to be an opening --
and as a fallback heuristic on a single scan with no corpus context, which is inherently less
reliable.

This needs no training data and no extra dependencies. If it turns out to be too unreliable on a
collection where pages don't have a roughly-fixed digitisation width (e.g. mixed-size inventories),
the natural extension point is `archival_structures.stream_analysis.overview.embeddings.extract_embeddings`
(DINOv2/CLIP): extract one embedding per scan, concatenate it with the features computed here
(`get_opening_features`), and train a small classifier (e.g. logistic regression) on labelled
examples such as `archival_structures.datasets.images_transcriptions.test_pairs`. That is future
work, not implemented here, since there are currently only a handful of labelled examples to learn from.
"""
import statistics
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy.typing as npt
import pagexml.model.physical_document_model as pdm
import pandas as pd
from pagexml.helper.pagexml_helper import transform_doc_coords
from pagexml.model.coords import Coords

from archival_structures.analysis.page import get_height_profile, get_separation_point


@dataclass
class OpeningFeatures:
    """Features describing a scan's width relative to its inventory, and the gap (if any) at
    its centre, used to decide whether the scan is a two-page opening."""

    separation_point: pdm.Point
    gap_width: int
    width_ratio: Optional[float]
    is_opening: bool


def compute_corpus_median_width(scans: Iterable[pdm.PageXMLScan]) -> float:
    """The median scan width across `scans`, for use as `corpus_median_width`. Compute this once
    per inventory number and reuse it for every scan in that inventory."""
    return statistics.median(scan.coords.width for scan in scans)


def get_opening_features(scan: pdm.PageXMLScan, corpus_median_width: float = None,
                         height_profile: Dict[str, Dict[int, float]] = None,
                         interval_width: int = 5, window_size: int = 200,
                         max_gap_height: float = 0, min_gap_width: int = 20,
                         min_width_ratio: float = 0.75) -> OpeningFeatures:
    """Compute the features used to decide whether `scan` is a two-page opening.

    :param scan: a PageXMLScan
    :param corpus_median_width: the median scan width across the scan's inventory number (see
                                `compute_corpus_median_width`). When given, `width_ratio` and the
                                width-based decision are used. When omitted, there's no corpus
                                context to compare against, so the (less reliable) height-profile
                                gap heuristic is used instead.
    :param height_profile: a pre-computed height profile (see `page.get_height_profile`); if not
                           given, it is computed for just this scan
    :param interval_width: see `page.get_height_profile`
    :param window_size: the size of the central window to search for a gap, see `page.get_separation_point`
    :param max_gap_height: the maximum text height at a point for it to still count as part of
                           the gap (0 means: only truly empty points count)
    :param min_gap_width: the minimum width (in pixels) the contiguous run of low-text-height
                          points around the separation point must have to count as a real column
                          gap. This guards against a single isolated low point (e.g. between two
                          letters) being mistaken for one.
    :param min_width_ratio: the minimum ratio of `scan`'s width to `corpus_median_width` for the
                            scan to count as an opening.
    """
    if height_profile is None:
        height_profile = get_height_profile([scan], interval_width=interval_width)
    separation_point = get_separation_point(scan, height_profile, interval_width=interval_width,
                                            window_size=window_size)
    sep_x = separation_point.x
    gap_width = 0
    if separation_point.y <= max_gap_height:
        # measure how far the low-text-height run extends to either side of the separation point
        left = sep_x
        while height_profile[scan.id].get(left - interval_width, max_gap_height + 1) <= max_gap_height:
            left -= interval_width
        right = sep_x
        while height_profile[scan.id].get(right + interval_width, max_gap_height + 1) <= max_gap_height:
            right += interval_width
        gap_width = right - left

    width_ratio = scan.coords.width / corpus_median_width if corpus_median_width else None
    if width_ratio is not None:
        is_opening = width_ratio >= min_width_ratio
    else:
        is_opening = separation_point.y <= max_gap_height and gap_width >= min_gap_width
    return OpeningFeatures(separation_point=separation_point, gap_width=gap_width,
                           width_ratio=width_ratio, is_opening=is_opening)


def is_opening(scan: pdm.PageXMLScan, corpus_median_width: float = None,
              height_profile: Dict[str, Dict[int, float]] = None,
              interval_width: int = 5, window_size: int = 200,
              max_gap_height: float = 0, min_gap_width: int = 20,
              min_width_ratio: float = 0.75) -> bool:
    """Determine whether `scan` shows a two-page opening, rather than a single page.
    See `get_opening_features` for the parameters."""
    return get_opening_features(scan, corpus_median_width=corpus_median_width,
                                height_profile=height_profile, interval_width=interval_width,
                                window_size=window_size, max_gap_height=max_gap_height,
                                min_gap_width=min_gap_width, min_width_ratio=min_width_ratio).is_opening


def split_scan(scan: pdm.PageXMLScan, separation_point: pdm.Point) -> Tuple[pdm.PageXMLScan, pdm.PageXMLScan]:
    """Split `scan` into two scans at `separation_point.x`: a verso (left) scan with the original
    coordinate space, and a recto (right) scan whose coordinates are translated so it starts at
    x=0. Text regions are assigned to a side by their horizontal centre relative to the
    separation point; a region should not itself straddle the gap, but if one does it is
    assigned to whichever side contains its centre."""
    sep_x = round(separation_point.x)
    left_regions: List[pdm.PageXMLTextRegion] = []
    right_regions: List[pdm.PageXMLTextRegion] = []
    for region in scan.text_regions:
        centre_x = (region.coords.left + region.coords.right) / 2
        if centre_x < sep_x:
            left_regions.append(transform_doc_coords(region, translate_by=(0, 0)))
        else:
            right_regions.append(transform_doc_coords(region, translate_by=(-sep_x, 0)))

    verso = pdm.PageXMLScan(
        doc_id=f"{scan.id}-verso", metadata=dict(scan.metadata),
        coords=Coords.coords_from_box_params(0, 0, sep_x, scan.coords.h),
        text_regions=left_regions,
    )
    recto_width = scan.coords.w - sep_x
    recto = pdm.PageXMLScan(
        doc_id=f"{scan.id}-recto", metadata=dict(scan.metadata),
        coords=Coords.coords_from_box_params(0, 0, recto_width, scan.coords.h),
        text_regions=right_regions,
    )
    return verso, recto


def split_image(image: npt.NDArray, separation_point: pdm.Point) -> Tuple[npt.NDArray, npt.NDArray]:
    """Split a pixel array into a left (verso) and right (recto) half at `separation_point.x`.

    `image` must already be in the same coordinate space as `separation_point` (i.e. the full
    scan, not a thumbnail) -- to split a thumbnail, first convert the separation point with
    `ImageSelection.scan_to_thumb.apply(Box(separation_point.x, 0, 0, 0)).x` (see
    `archival_structures.model.image.Transform`)."""
    sep_x = round(separation_point.x)
    return image[:, :sep_x], image[:, sep_x:]


# ---------------------------------------------------------------------------
# Inventory structure: book of openings, vs. a mixed folder/booklet
# ---------------------------------------------------------------------------
#
# `is_opening` above answers a per-scan, binary question, given that the inventory it belongs to
# is *already known* to be a consistent book of openings (it needs `corpus_median_width` from
# exactly such an inventory to work reliably). The functions below answer the broader question:
# does a given inventory number *represent* a book of openings in the first place, or is it a
# folder/booklet with a mixture of single-page sheets, two-page openings, and odd-shaped sheets
# (oversized fold-outs, undersized inserts, ...)?
#
# The approach generalises the width-ratio idea: `find_width_groups` groups a set of scans by
# width (sequential gap-merging, robust to a few extreme outliers -- a histogram with fixed bin
# edges turned out to fragment real clusters depending on arbitrary bin-edge placement, and
# `archival_structures.clustering.peaks`' binning only counts exact value matches rather than
# aggregating nearby values, which works for densely-sampled pixel coordinates but not for a
# handful of distinct scan widths). `identify_page_unit_widths` then looks for a pair of groups
# consistent with a real single-page/opening relationship: the wider group's width is roughly
# double the narrower one's, openings (wider) are at least as numerous as single pages (narrower)
# -- a real book has more interior openings than covers -- and together the two groups account
# for nearly all scans. This was validated against six real, independently-labelled inventory
# numbers (four books, two mixed folders) and matched the expected verdict on all six; see
# `tests/test_analysis_opening_detection.py` and `notebooks/demo/inventory-structure-demo.ipynb`.
#
# One real-world wrinkle this is robust to by construction: some book scans show a sheet glued
# onto a page, masking the text behind it, with the *next* scan showing the same opening with the
# sheet folded back to reveal the text. Both states are still photographed at the full opening
# width, so they don't disturb the width-based grouping -- they just mean a "book of openings"
# inventory can have more scans than physical openings, which is expected, not an anomaly.

PAGE_TYPE_SINGLE = 'single_page'
PAGE_TYPE_OPENING = 'opening'
PAGE_TYPE_ODD = 'odd_shaped'

STRUCTURE_BOOK_OF_OPENINGS = 'book_of_openings'
STRUCTURE_MIXED = 'mixed'


@dataclass
class WidthGroup:
    """A group of scans with closely similar widths (see `find_width_groups`)."""

    center: float
    scans: List[pdm.PageXMLScan]

    @property
    def count(self) -> int:
        """Number of scans in this group."""
        return len(self.scans)

    @property
    def height_center(self) -> float:
        """Median scan height within this group."""
        return statistics.median(scan.coords.height for scan in self.scans)


def find_width_groups(scans: List[pdm.PageXMLScan], merge_frac: float = 0.08) -> List[WidthGroup]:
    """Group `scans` by width: sort by width, and start a new group whenever the gap to the
    previous (sorted) scan's width exceeds `merge_frac` of that previous width.

    This sequential gap-merge is robust to a handful of extreme outliers, unlike picking the
    single largest gap in the sorted widths (one oversized fold-out can create a bigger gap than
    the real single-page/opening boundary), and unlike fixed-width histogram binning (which can
    fragment a real cluster depending on where the bin edges happen to fall, especially for small
    inventories with only a handful of distinct widths).
    """
    ordered = sorted(scans, key=lambda scan: scan.coords.width)
    groups: List[List[pdm.PageXMLScan]] = [[ordered[0]]]
    for scan in ordered[1:]:
        prev_width = groups[-1][-1].coords.width
        if (scan.coords.width - prev_width) <= merge_frac * prev_width:
            groups[-1].append(scan)
        else:
            groups.append([scan])
    return [WidthGroup(center=statistics.median(s.coords.width for s in group), scans=group)
            for group in groups]


def identify_page_unit_widths(width_groups: List[WidthGroup], total: int,
                              min_ratio: float = 1.3, max_ratio: float = 2.6,
                              min_share: float = 0.15,
                              min_combined_share: float = 0.85) -> Optional[Tuple[WidthGroup, WidthGroup]]:
    """Find the `(single_page, opening)` pair among `width_groups` consistent with a book of
    openings: the wider group's width is roughly double the narrower group's (within
    `[min_ratio, max_ratio]`), the wider (opening) group is at least as large as the narrower
    (single-page) one -- a real book has more openings than covers -- and together the two
    groups make up nearly all the scans (`min_combined_share`), with neither being a negligible
    minority (`min_share`, applied to each group individually). Among all pairs satisfying this,
    returns the one with the most combined scans.

    Returns `None` if no such pair exists -- i.e. there's no consistent single-page/opening
    structure across `width_groups`, meaning `total` is unlikely to be a single book of openings.
    """
    best = None
    for g1 in width_groups:
        for g2 in width_groups:
            if g1.center >= g2.center:
                continue
            ratio = g2.center / g1.center
            if not (min_ratio <= ratio <= max_ratio):
                continue
            if g2.count < g1.count:
                continue
            if min(g1.count, g2.count) / total < min_share:
                continue
            if (g1.count + g2.count) / total < min_combined_share:
                continue
            combined = g1.count + g2.count
            if best is None or combined > best[0]:
                best = (combined, g1, g2)
    return (best[1], best[2]) if best else None


def _classify_page_types(scans: List[pdm.PageXMLScan],
                         pair: Optional[Tuple[WidthGroup, WidthGroup]],
                         height_tolerance: float) -> pd.Series:
    if pair is None:
        return pd.Series({scan.id: PAGE_TYPE_ODD for scan in scans}, name='page_type')
    single_group, opening_group = pair
    single_ids = {scan.id for scan in single_group.scans}
    opening_ids = {scan.id for scan in opening_group.scans}
    page_types = {}
    for scan in scans:
        if scan.id in single_ids:
            ref_height, label = single_group.height_center, PAGE_TYPE_SINGLE
        elif scan.id in opening_ids:
            ref_height, label = opening_group.height_center, PAGE_TYPE_OPENING
        else:
            page_types[scan.id] = PAGE_TYPE_ODD
            continue
        height_diff = abs(scan.coords.height - ref_height)
        page_types[scan.id] = label if height_diff <= height_tolerance * ref_height else PAGE_TYPE_ODD
    return pd.Series(page_types, name='page_type')


def classify_page_types(scans: List[pdm.PageXMLScan], merge_frac: float = 0.08,
                        min_ratio: float = 1.3, max_ratio: float = 2.6,
                        min_share: float = 0.15, min_combined_share: float = 0.85,
                        height_tolerance: float = 0.2) -> pd.Series:
    """Classify each scan in `scans` as `'single_page'`, `'opening'`, or `'odd_shaped'`.

    A scan is `'single_page'`/`'opening'` if its width places it in the corresponding group from
    `identify_page_unit_widths`, *and* its height is within `height_tolerance` of that group's
    typical height (catching e.g. an oversized fold-out that happens to be opening-width but is
    much taller). Everything else -- including every scan, if no consistent single/opening width
    pair is found at all -- is `'odd_shaped'`.

    Returns a `pd.Series` of page-type strings indexed by scan id.
    """
    width_groups = find_width_groups(scans, merge_frac=merge_frac)
    pair = identify_page_unit_widths(width_groups, len(scans), min_ratio=min_ratio, max_ratio=max_ratio,
                                     min_share=min_share, min_combined_share=min_combined_share)
    return _classify_page_types(scans, pair, height_tolerance)


@dataclass
class InventoryStructure:
    """The result of `classify_inventory_structure`."""

    structure_type: str  # STRUCTURE_BOOK_OF_OPENINGS or STRUCTURE_MIXED
    single_page_width: Optional[float]
    opening_width: Optional[float]
    page_types: pd.Series  # scan id -> page type, see `classify_page_types`
    type_counts: Dict[str, int]


def classify_inventory_structure(scans: List[pdm.PageXMLScan], merge_frac: float = 0.08,
                                 min_ratio: float = 1.3, max_ratio: float = 2.6,
                                 min_share: float = 0.15, min_combined_share: float = 0.85,
                                 height_tolerance: float = 0.2,
                                 max_odd_share: float = 0.15) -> InventoryStructure:
    """Classify a whole inventory number as `STRUCTURE_BOOK_OF_OPENINGS` (predominantly two-page
    openings, usually with single-page covers at the start/end and possibly elsewhere -- see the
    glued-flap note above) or `STRUCTURE_MIXED` (a folder/booklet without one consistent page
    size: a mixture of single sheets, openings, and odd-shaped sheets).

    Classified as `STRUCTURE_BOOK_OF_OPENINGS` only if both: a valid single-page/opening width
    pair is found (`identify_page_unit_widths`), and odd-shaped scans are no more than
    `max_odd_share` of the total once the height check is applied too -- so a collection that
    happens to contain *some* page-doubling pair but is otherwise too heterogeneous still comes
    out `STRUCTURE_MIXED`.
    """
    width_groups = find_width_groups(scans, merge_frac=merge_frac)
    pair = identify_page_unit_widths(width_groups, len(scans), min_ratio=min_ratio, max_ratio=max_ratio,
                                     min_share=min_share, min_combined_share=min_combined_share)
    page_types = _classify_page_types(scans, pair, height_tolerance)
    type_counts = page_types.value_counts().to_dict()
    odd_share = type_counts.get(PAGE_TYPE_ODD, 0) / len(scans)

    is_book = pair is not None and odd_share <= max_odd_share
    single_group, opening_group = pair if pair is not None else (None, None)
    return InventoryStructure(
        structure_type=STRUCTURE_BOOK_OF_OPENINGS if is_book else STRUCTURE_MIXED,
        single_page_width=single_group.center if single_group else None,
        opening_width=opening_group.center if opening_group else None,
        page_types=page_types,
        type_counts=type_counts,
    )


def split_inventory_into_pages(scans: List[pdm.PageXMLScan], structure: InventoryStructure = None,
                               include_odd_shaped: bool = True) -> List[pdm.PageXMLScan]:
    """Turn `scans` into single-*page*-level scans: every `'opening'`-typed scan (per
    `structure.page_types`) is split in two with `split_scan`/`get_opening_features`;
    `'single_page'`-typed scans (e.g. covers) are kept as-is.

    This is the preprocessing step that makes per-line/per-page analyses (line indentation
    clustering, page-layout clustering, cross-page sequence patterns) meaningful on a book of
    openings: clustering directly on un-split two-page scans conflates the left and right page's
    columns into one absolute coordinate frame, where they don't actually share a coordinate
    system (each page's own left margin is what indentation should be measured relative to).

    :param structure: the result of `classify_inventory_structure(scans)`; computed for you if omitted.
    :param include_odd_shaped: if True (default), `'odd_shaped'` scans are kept unchanged in the
                               output (excluding them silently could lose real content); set to
                               False to drop them instead.
    :return: a list of single-page scans, in no particular order relative to `scans` (each
            opening contributes a verso then a recto immediately after it).
    """
    if structure is None:
        structure = classify_inventory_structure(scans)
    pages: List[pdm.PageXMLScan] = []
    for scan in scans:
        page_type = structure.page_types.get(scan.id)
        if page_type == PAGE_TYPE_OPENING:
            features = get_opening_features(scan)
            verso, recto = split_scan(scan, features.separation_point)
            pages.append(verso)
            pages.append(recto)
        elif page_type == PAGE_TYPE_SINGLE:
            pages.append(scan)
        elif include_odd_shaped:
            pages.append(scan)
    return pages
