"""Grid-based page-layout fingerprinting.

Each page in a `doc_set` is rasterised onto a shared `unit_size`-pixel grid (one cell is "on"
wherever a text line falls), then `GridPattern` slides a small `pattern_size` x `pattern_size`
window across that grid and counts how often each local on/off pattern occurs, both
per-document and corpus-wide. The resulting per-document TF-IDF vector (`GridPattern.tfidf`)
is a layout fingerprint that `archival_structures.analysis.page_layout_clustering` clusters
with HDBSCAN -- two pages with a similar arrangement of text lines get similar fingerprints,
regardless of the actual text content.
"""

import pickle
import time
from collections import Counter, defaultdict
from typing import List, Set, Union

import numpy as np
import pagexml.model.physical_document_model as pdm
import pagexml.analysis.layout_stats as layout
from numpy.lib.stride_tricks import sliding_window_view


def get_doc_set_grid_points(doc_set: Union[List[pdm.PageXMLRegion], Set[pdm.PageXMLRegion]],
                            unit_size: int) -> dict[tuple[int, int], int]:
    """All `(x, y)` grid coordinates (spaced `unit_size` pixels apart) covering the largest
    page in `doc_set`, mapped to a dense integer index -- the shared coordinate system every
    page in the set is rasterised onto."""
    max_x, max_y = get_interpolated_max_x_y(doc_set, unit_size=unit_size)
    grid_points = [(x, y) for x in range(0, max_x, unit_size) for y in range(0, max_y, unit_size)]
    print("done setting base grid points")

    return {point: pi for pi, point in enumerate(grid_points)}


def get_interpolated_max_x_y(doc_set: Union[List[pdm.PageXMLRegion], Set[pdm.PageXMLRegion]],
                             unit_size: int = 50) -> tuple[int, int]:
    """The largest page width/height in `doc_set`, rounded to the nearest `unit_size`."""
    max_x = max(page.coords.right for page in doc_set)
    max_y = max(page.coords.bottom for page in doc_set)

    max_x = int(np.round(max_x / unit_size, 0) * unit_size)
    max_y = int(np.round(max_y / unit_size, 0) * unit_size)
    return max_x, max_y


def doc_grid_points_to_vectors(doc_grid_points: np.array) -> np.array:
    """Flatten a `(num_docs, range_x, range_y)` grid-point array into `(num_docs, range_x *
    range_y)`, one row per document."""
    dims = doc_grid_points.shape
    if len(dims) != 3:
        raise IndexError(f"invalid shape for doc_grid_pionts: {dims}, must be a 3D array.")
    return np.resize(doc_grid_points, (dims[0], dims[1] * dims[2]))


def time_step(start, prev_step):
    """Print elapsed time since `prev_step` and since `start`, returning the current time
    (for chaining: `step = time_step(start, step)`)."""
    curr_step = time.time()
    took = curr_step - prev_step
    total = curr_step - start
    print(f"took {took: >.1f} seconds (total: {total: >.1f})")
    return curr_step


def get_line_grid_left_right(line: pdm.PageXMLTextLine, unit_size: int = 50, indent: int = 0):
    """`line`'s left/right extent (from its baseline if available, else its coordinates),
    snapped to the nearest `unit_size` grid line. `indent` shifts the snapped boundary inward
    on both sides before rounding."""
    if line.baseline is not None and line.baseline.points is not None:
        left = line.baseline.left
        right = line.baseline.right
    elif line.coords is not None and line.coords.points is not None:
        left = line.coords.left
        right = line.coords.right
    else:
        return None, None
    # print(f"original left: {left}\toriginal right: {right}")
    left += indent
    dist_from_prev = left % unit_size
    dist_from_next = unit_size - dist_from_prev
    # print(f"left dist_from_next: {dist_from_next}")
    left = left - dist_from_prev if dist_from_next > unit_size / 4 else left + dist_from_next
    right -= indent
    dist_from_prev = right % unit_size
    dist_from_next = unit_size - dist_from_prev
    # print(f"right dist_from_next: {dist_from_next}")
    right = right - dist_from_prev if dist_from_prev < unit_size / 4 else right + dist_from_next
    return left, right


def get_line_grid_top_bottom(line: pdm.PageXMLTextLine, unit_size: int = 50):
    """`line`'s top/bottom extent (from its baseline and x-height if available, else a
    margin-trimmed fraction of its bounding box), snapped to the nearest `unit_size` grid
    line."""
    if line.baseline is not None and line.baseline.points is not None:
        if line.xheight is not None:
            top = line.baseline.top - line.xheight
        else:
            top = line.coords.top + int(line.coords.height * 0.25)
        bottom = line.baseline.bottom
    elif line.coords is not None and line.coords.points is not None:
        top = line.coords.top + int(line.coords.height * 0.25)
        bottom = line.coords.bottom - int(line.coords.height * 0.25)
    else:
        return None
    # print(f"original bottom: {bottom}\toriginal top: {top}")
    dist_from_prev = top % unit_size
    dist_from_next = unit_size - dist_from_prev
    top = top - dist_from_prev if dist_from_next > unit_size / 4 else top + dist_from_next
    dist_from_prev = bottom % unit_size
    dist_from_next = unit_size - dist_from_prev
    bottom = bottom - dist_from_prev if dist_from_prev < unit_size / 4 else bottom + dist_from_next
    return top, bottom


def get_line_grid_points(doc_set: Union[List[pdm.PageXMLRegion], Set[pdm.PageXMLRegion]],
                         unit_size: int = 50, indent: int = 0) -> np.array:
    """Rasterise every document in `doc_set` onto a shared `unit_size`-pixel grid: a
    `(num_docs, range_x, range_y)` array where a cell is 1.0 wherever any text line's
    (snapped) bounding box falls, 0.0 elsewhere."""
    max_x, max_y = get_interpolated_max_x_y(doc_set, unit_size=unit_size)
    range_x, range_y = int(max_x / unit_size), int(max_y / unit_size)

    doc_grid_points = np.zeros((len(doc_set), range_x, range_y))

    for di, doc in enumerate(doc_set):
        for line in doc.get_lines():
            left, right = get_line_grid_left_right(line, unit_size=unit_size, indent=indent)
            top, bottom = get_line_grid_top_bottom(line, unit_size=unit_size)
            x_start, x_end = int(left / unit_size), int(right / unit_size)
            y_start, y_end = int(top / unit_size), int(bottom / unit_size)
            doc_grid_points[di][x_start:x_end, y_start:y_end] = 1.0
    print(f"done setting line grid_points for {len(doc_set)} docs, "
          f"with {range_x} x and {range_y} y points")
    return doc_grid_points


def get_region_grid_points(doc_set: Union[List[pdm.PageXMLRegion], Set[pdm.PageXMLRegion]],
                           unit_size: int = 50) -> np.array:
    """Like `get_line_grid_points`, but rasterised from text *region* bounding boxes rather
    than individual lines -- coarser, since region detection tends to merge visually distinct
    elements (prefer `get_line_grid_points` for layout fingerprinting)."""
    positions = ['left', 'top', 'right', 'bottom']
    max_x, max_y = get_interpolated_max_x_y(doc_set, unit_size=unit_size)
    range_x, range_y = int(max_x / unit_size), int(max_y / unit_size)

    doc_grid_points = np.zeros((len(doc_set), range_x, range_y))

    for di, doc in enumerate(doc_set):
        for tr in doc.get_textual_regions():
            tr_pos = {}
            for pos in positions:
                tr_pos[pos] = int(np.round(tr.coords.__getattribute__(pos) / unit_size, 0))
            x_start, x_end = tr_pos['left'], tr_pos['right']
            y_start, y_end = tr_pos['top'], tr_pos['bottom']
            doc_grid_points[di][x_start:x_end, y_start:y_end] = 1.0
    print(f"done setting region grid_points for {len(doc_set)} docs, "
          f"with {range_x} x and {range_y} y points")
    return doc_grid_points


def compute_pattern_tfidf(doc_pattern_freq: defaultdict, pattern_doc_freq: Counter, vocab: dict,
                          num_docs: int, vocab_size: int) -> tuple:
    """TF-IDF over per-document pattern-frequency counters, shared between `GridPattern` and
    `archival_structures.analysis.relational_patterns.RelationalPattern` -- they differ only in
    what a "pattern" is (a 3x3 pixel micro-pattern vs. a relational `(cluster, relation,
    cluster)` symbol); the TF-IDF math over per-document frequency counters is identical.

    :param doc_pattern_freq: per-document pattern frequency, `{doc_index: Counter}`
    :param pattern_doc_freq: corpus-wide document frequency per pattern, `{pattern: num_docs}`
    :param vocab: `{pattern: vocab_index}`
    :param num_docs: total number of documents
    :param vocab_size: total number of distinct patterns (`len(vocab)`)
    :return: `(tf, tfidf)`, each shape `(num_docs, vocab_size)`
    """
    tf = np.zeros((num_docs, vocab_size))
    tfidf = np.zeros((num_docs, vocab_size))
    for di in doc_pattern_freq:
        doc_length = sum(doc_pattern_freq[di].values())
        for pattern, count in doc_pattern_freq[di].most_common():
            prob_tf = count / doc_length
            log_tf = 1 + np.log(count)
            df = pattern_doc_freq[pattern]
            idf = np.log(num_docs / df)
            pattern_idx = vocab[pattern]
            tf[di][pattern_idx] = prob_tf
            tfidf[di][pattern_idx] = log_tf * idf
    return tf, tfidf


class GridPattern:
    """Per-document layout fingerprints, built by rasterising each document's text lines onto
    a shared grid (`get_line_grid_points`) and counting local `pattern_size` x `pattern_size`
    on/off patterns across that grid, both per-document and corpus-wide. `self.tfidf` (shape
    `(num_docs, vocab_size)`) is the resulting fingerprint matrix, suitable for clustering or
    nearest-neighbour search; `self.doc_pfidf(doc_id)` looks up one document's row by id."""

    def __init__(self, doc_set: Union[List[pdm.PageXMLRegion], Set[pdm.PageXMLRegion]],
                 unit_size: int = 50, pattern_size: int = 3):
        # self.doc_set = doc_set
        self.doc_ids = [doc.id for doc in doc_set]
        self.id2idx = {doc.id: di for di, doc in enumerate(doc_set)}
        self.idx2id = {di: doc.id for di, doc in enumerate(doc_set)}
        self.num_docs = len(doc_set)
        self.unit_size = unit_size
        self.pattern_size = pattern_size
        start = time.time()
        self.grid_points = get_doc_set_grid_points(doc_set, unit_size=unit_size)
        step = time_step(start, start)
        self.doc_grid_points = get_line_grid_points(doc_set, unit_size)
        step = time_step(start, step)
        self.coll_pattern_freq = Counter()
        self.doc_pattern_freq = defaultdict(Counter)
        self.pattern_doc_freq = Counter()
        self._compute_pattern_freq()
        step = time_step(start, step)
        self.vocab = {pattern: pi for pi, pattern in enumerate(self.pattern_doc_freq.keys())}
        self.vocab_inv = {v: k for k, v in self.vocab.items()}
        self.vocab_size = len(self.vocab)
        self.tf = None
        self.tfidf = self._compute_pattern_tfidf()
        time_step(start, step)

    def doc_pf(self, doc_id):
        """Raw local-pattern frequency counter (`Counter`) for the document with id `doc_id`."""
        if doc_id not in self.id2idx:
            raise KeyError(f"unknown doc_id {doc_id}")
        return self.doc_pattern_freq[self.id2idx[doc_id]]

    def doc_pfidf(self, doc_id):
        """TF-IDF layout-fingerprint vector (shape `(vocab_size,)`) for the document with id
        `doc_id` -- a row of `self.tfidf`."""
        if doc_id not in self.id2idx:
            raise KeyError(f"unknown doc_id {doc_id}")
        return self.tfidf[self.id2idx[doc_id]]

    def _compute_pattern_freq(self):
        window_shape = (self.pattern_size, self.pattern_size)
        for di in range(0, self.num_docs):
            windows = sliding_window_view(self.doc_grid_points[di], window_shape)
            patterns = np.reshape(windows, (windows.shape[0] * windows.shape[1], self.pattern_size**2))
            patterns = [tuple(pattern) for pattern in patterns]
            # patterns = np.array([(2 ** (np.where(pattern)[0]+1)).sum() for pattern in patterns])
            self.doc_pattern_freq[di].update(patterns)
            self.coll_pattern_freq.update(patterns)

        for doc_id in self.doc_pattern_freq:
            self.pattern_doc_freq.update([pattern for pattern in self.doc_pattern_freq[doc_id]])
        print(f"done computing patterns, {len(self.coll_pattern_freq)} distinct patterns")

    def _compute_pattern_tfidf(self):
        self.tf, tf_idf = compute_pattern_tfidf(self.doc_pattern_freq, self.pattern_doc_freq,
                                                self.vocab, self.num_docs, self.vocab_size)
        print("done computing tf-idf of patterns")
        return tf_idf

    def save(self, filename: str):
        """Pickle this `GridPattern` to `filename`."""
        with open(filename, 'wb') as fh:
            pickle.dump(self, fh)

    @staticmethod
    def load(filename: str) -> 'GridPattern':
        """Load a `GridPattern` previously written with `save`."""
        with open(filename, 'rb') as fh:
            grid_pattern = pickle.load(fh)
        return grid_pattern
