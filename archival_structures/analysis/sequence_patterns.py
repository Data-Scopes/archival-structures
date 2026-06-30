"""Find recurring sequences and neighbourhoods of text-line types across a whole inventory
number, building on the line-cluster labels from `archival_structures.analysis.line_clustering`.

An inventory number is one continuous archival file, represented as an ordered sequence of scans
(images). `build_line_sequence` puts every line of every scan into that single document order
(trusting the PageXML's own region/line reading order, see its docstring). Once lines are in one
global sequence, `find_ngram_patterns` counts recurring cluster-label n-grams (e.g. "a run of
body lines followed by a short indented line"), and `segment_into_elements` groups runs of the
same label into page/document elements -- which can span a page break, since the sequence
doesn't stop at scan boundaries. `detect_cross_page_continuation` is what decides whether a label
run that happens to straddle a scan boundary is *really* one continuing element (e.g. a closing
that starts at the bottom of one page and finishes at the top of the next) rather than two
unrelated runs that coincidentally share a label, by checking the boundary lines' spatial
relation with `archival_structures.analysis.region_calculus`'s region-connection-calculus.
"""
import re
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

import pagexml.model.physical_document_model as pdm
import pandas as pd
from pagexml.helper.pagexml_helper import transform_doc_coords

from archival_structures.analysis import region_calculus
from archival_structures.datasets.annotations import Element, ElementSpan


@dataclass
class LineEvent:
    """One text line's place in the global, cross-scan document sequence."""

    scan_id: str
    line_id: str
    cluster: Optional[Union[int, str]]
    position: int  # 0-based position of this line within its scan
    n_lines_in_scan: int

    @property
    def is_first_in_scan(self) -> bool:
        """True if this is the first line (in reading order) of its scan."""
        return self.position == 0

    @property
    def is_last_in_scan(self) -> bool:
        """True if this is the last line (in reading order) of its scan."""
        return self.position == self.n_lines_in_scan - 1


def _scan_sequence_number(scan: pdm.PageXMLScan) -> int:
    """The page/scan sequence number encoded in `scan.id`'s filename, e.g. 2 for
    'NL-AsnDA_0114.11_1_0002.jpg'. Falls back to the last run of digits found anywhere in the id
    if the filename doesn't follow that `..._NNNN` convention (see `make_iventory_paragraphs.py`,
    which uses the same convention)."""
    stem = re.sub(r'\.[A-Za-z0-9]+$', '', scan.id)
    try:
        return int(stem.split('_')[-1])
    except ValueError:
        match = re.search(r'(\d+)(?!.*\d)', stem)
        if match:
            return int(match.group(1))
        raise ValueError(f"could not determine a sequence number from scan id: {scan.id}")


def _scan_sort_key(scan: pdm.PageXMLScan) -> Tuple[int, int]:
    """Sort key for ordering scans: by sequence number, then verso before recto for scans
    produced by `opening_detection.split_scan` (which share one sequence number -- the original
    two-page scan's -- between its two halves, since they're literally two halves of the same
    numbered scan)."""
    side = 1 if scan.id.endswith('-recto') else 0
    return _scan_sequence_number(scan), side


def build_line_sequence(scans: List[pdm.PageXMLScan],
                        line_clusters: pd.Series = None) -> List[LineEvent]:
    """Put every line of every scan in `scans` into one global document-order sequence.

    Scans are ordered by their filename sequence number (see `_scan_sequence_number`). Within a
    scan, text regions are taken in `scan.get_textual_regions()`'s order -- which already
    reflects the PageXML's own `ReadingOrder` when the source file has one -- and lines within a
    region are taken in their stored order, which is the original transcription order. This is
    *not* a re-derived geometric reading order (contrast with e.g. the simple top/left sort used
    for browsing in the annotation notebook): it trusts the source PageXML's own ordering.

    :param scans: the scans of one inventory number (in any order -- they get sorted here)
    :param line_clusters: a `pd.Series` mapping (scan id, line id) -> cluster label (e.g.
                          `line_df.set_index(['scan_id', 'line_id'])['cluster']`, built from
                          `line_clustering.cluster_lines`/`cluster_lines_by_peaks`). Keyed by the
                          *pair*, not line id alone -- PageXML line ids (e.g. `r1l1`) are only
                          unique within one scan, not across a whole inventory number. If
                          `line_clusters` is omitted, every `LineEvent.cluster` is `None`.
    """
    sequence: List[LineEvent] = []
    for scan in sorted(scans, key=_scan_sort_key):
        lines = [line for region in scan.get_textual_regions() for line in region.get_lines()]
        n_lines = len(lines)
        for position, line in enumerate(lines):
            cluster = line_clusters.get((scan.id, line.id)) if line_clusters is not None else None
            sequence.append(LineEvent(scan_id=scan.id, line_id=line.id, cluster=cluster,
                                      position=position, n_lines_in_scan=n_lines))
    return sequence


def find_ngram_patterns(line_sequence: List[LineEvent],
                        n: Union[int, Iterable[int]] = (2, 3)) -> Counter:
    """Count frequencies of consecutive cluster-label n-grams in `line_sequence` (which should
    already be in document order, e.g. from `build_line_sequence`). N-grams are *not* broken at
    scan boundaries, since a recurring pattern (or a continuing element) may straddle a page
    break. Windows containing an unlabelled line (cluster `None` or HDBSCAN noise `-1`) are
    skipped, since neither represents a real line type.

    :param n: a window size, or an iterable of window sizes (e.g. `(2, 3)` for bigrams and trigrams).
    """
    window_sizes = [n] if isinstance(n, int) else list(n)
    labels = [event.cluster for event in line_sequence]
    counts = Counter()
    for window in window_sizes:
        for i in range(len(labels) - window + 1):
            ngram = tuple(labels[i:i + window])
            if any(label is None or label == -1 for label in ngram):
                continue
            counts[ngram] += 1
    return counts


def detect_cross_page_continuation(scan_a: pdm.PageXMLScan, line_a: pdm.PageXMLTextLine,
                                   line_b: pdm.PageXMLTextLine,
                                   max_vertical_gap: int = 150,
                                   max_horizontal_diff: int = 100) -> bool:
    """Check whether `line_a` (the last line of a label-run on `scan_a`) and `line_b` (the first
    line of the same-labelled run on the next scan in sequence) are spatially consistent with
    being one element continuing across the page break, rather than two unrelated runs that
    happen to share a label.

    `line_b` is translated down by `scan_a`'s height, putting both lines into one synthetic
    combined coordinate space (as if `scan_a` and the next scan were stacked vertically), then
    `region_calculus.get_vertical_region_relation`/`get_horizontal_region_relation` (region
    connection calculus) check that the two lines are close enough vertically (within
    `max_vertical_gap`, allowing for bottom/top page margins) and similarly indented (within
    `max_horizontal_diff`) to plausibly be the same element. Either check returning `'DC'`
    (disconnected) means they're not."""
    translated_b = transform_doc_coords(line_b, translate_by=(0, scan_a.coords.height))
    vertical_relation = region_calculus.get_vertical_region_relation(line_a, translated_b,
                                                                      max_diff=max_vertical_gap)
    horizontal_relation = region_calculus.get_horizontal_region_relation(line_a, translated_b,
                                                                         max_diff=max_horizontal_diff)
    return vertical_relation != 'DC' and horizontal_relation != 'DC'


def segment_into_elements(line_sequence: List[LineEvent], scans: List[pdm.PageXMLScan],
                          max_vertical_gap: int = 150, max_horizontal_diff: int = 100) -> List[Element]:
    """Run-length-segment `line_sequence` into `Element`s: contiguous runs of lines sharing the
    same cluster label. Lines with no label (`None` or HDBSCAN noise `-1`) end the current run
    and are not part of any element.

    A run that spans a scan boundary is only kept as a single `Element` (with one `ElementSpan`
    per scan it touches) if `detect_cross_page_continuation` confirms the boundary lines are
    spatially consistent with being one real continuing element; otherwise it is split into two
    separate `Element`s at that boundary. This is how document elements spanning multiple images
    get detected: a run of e.g. `closing`-labelled lines ending near the bottom of one scan and
    resuming near the top of the next becomes one `Element` with two spans.

    :param scans: the same scans `line_sequence` was built from (used to look up scan heights
                  and line coordinates for the cross-page check).
    """
    scans_by_id = {scan.id: scan for scan in scans}
    # keyed by (scan_id, line_id), not line_id alone -- PageXML line ids (e.g. 'r1l1') are only
    # unique within one scan, not across a whole inventory number
    lines_by_id = {(scan.id, line.id): line for scan in scans for line in scan.get_lines()}

    elements: List[Element] = []
    current_label = None
    current_run: List[LineEvent] = []

    def flush_run():
        """Close out `current_run` (the in-progress run of same-label lines): split it into
        cross-page-aware spans and append the resulting `Element`(s) to `elements`."""
        if not current_run:
            return
        spans = _split_run_into_spans(current_run, scans_by_id, lines_by_id,
                                      max_vertical_gap, max_horizontal_diff)
        for span_group in spans:
            elements.append(Element(element_type=current_label, spans=span_group))

    for event in line_sequence:
        label = event.cluster
        if label is None or label == -1:
            flush_run()
            current_run = []
            current_label = None
            continue
        if label != current_label:
            flush_run()
            current_run = []
            current_label = label
        current_run.append(event)
    flush_run()
    return elements


def _split_run_into_spans(run: List[LineEvent], scans_by_id: Dict[str, pdm.PageXMLScan],
                          lines_by_id: Dict[tuple, pdm.PageXMLTextLine],
                          max_vertical_gap: int, max_horizontal_diff: int) -> List[List[ElementSpan]]:
    """Group a same-label run of LineEvents into one or more lists of ElementSpans: one list per
    Element, where a run is only kept together across a scan boundary if
    `detect_cross_page_continuation` confirms it, otherwise it's split into separate Elements
    there."""
    # first, group consecutive events by scan id into per-scan line-id lists, in order
    scan_groups: List[tuple] = []
    for event in run:
        if scan_groups and scan_groups[-1][0] == event.scan_id:
            scan_groups[-1][1].append(event.line_id)
        else:
            scan_groups.append((event.scan_id, [event.line_id]))

    element_groups: List[List[ElementSpan]] = [[ElementSpan(scan_id=scan_groups[0][0],
                                                             line_ids=scan_groups[0][1])]]
    for (prev_scan_id, prev_line_ids), (scan_id, line_ids) in zip(scan_groups, scan_groups[1:]):
        scan_a = scans_by_id[prev_scan_id]
        line_a = lines_by_id[(prev_scan_id, prev_line_ids[-1])]
        line_b = lines_by_id[(scan_id, line_ids[0])]
        continues = detect_cross_page_continuation(scan_a, line_a, line_b,
                                                   max_vertical_gap=max_vertical_gap,
                                                   max_horizontal_diff=max_horizontal_diff)
        new_span = ElementSpan(scan_id=scan_id, line_ids=line_ids)
        if continues:
            element_groups[-1].append(new_span)
        else:
            element_groups.append([new_span])
    return element_groups
