"""Relational line-neighbourhood fingerprinting for page-layout clustering.

`archival_structures.analysis.grid_analysis.GridPattern` fingerprints a page by the spatial
arrangement of raw line pixels, with no notion of line *type* or of the *relationship* between
neighbouring lines. `RelationalPattern` is a complementary fingerprint built from, for each
line, its own cluster label (from `archival_structures.analysis.line_clustering`) and the RCC-8
spatial relation (`archival_structures.analysis.region_calculus`) to its immediate
below/right neighbour's cluster label -- e.g. `(body_text, 'below', 'EC', body_text)` vs.
`(body_text, 'below', 'EC', closing)`. Corpus-wide frequency of these relational symbols is
TF-IDF'd the same way `GridPattern` does it (`grid_analysis.compute_pattern_tfidf`), giving a
second per-page feature vector that captures line-*neighbourhood* patterns rather than raw
geometry -- e.g. a recurring "body line with a shorter line directly below it" symbol, which a
pixel-pattern fingerprint can't represent directly.

The neighbourhood itself is deliberately small: by default just the immediate `below` and
`right` neighbour of each line (`archival_structures.analysis.neighbourhood_analysis.
LineNeighbourHood`), not multi-hop chains -- start small, since the symbol vocabulary already
grows with `(num_line_clusters)^2 * num_relations * num_directions`.

This module takes no dependency on `line_clustering` or any specific clustering strategy --
`RelationalPattern` takes a caller-supplied `line_clusters` series, the same decoupling already
used between `archival_structures.datasets.bulk_tagging` and `archival_structures.stream_analysis`.
"""

import pickle
from collections import Counter, defaultdict
from typing import List, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import pagexml.model.physical_document_model as pdm

from archival_structures.analysis import region_calculus
from archival_structures.analysis.grid_analysis import compute_pattern_tfidf
from archival_structures.analysis.neighbourhood_analysis import (
    LineNeighbourHood, get_neighbouring_line_pairs, line_vertical_gap,
)

NOISE_CLUSTER_LABEL = -1
VERTICAL_DIRECTIONS = {'above', 'below'}
HORIZONTAL_DIRECTIONS = {'left', 'right'}


def corpus_adjacent_line_vertical_gaps(scans: List[pdm.PageXMLScan]) -> List[int]:
    """The vertical gap (`line_vertical_gap`) between every pair of reading-order-adjacent
    lines on each page, pooled across `scans`.

    Originally scoped to lines within the same `PageXMLTextRegion` (a region's own lines are
    known, by PageXML's own structure, to belong together) -- but region granularity turns out
    to vary by archive: `NL-HaNA_2.10.50_1`'s ATR output puts exactly one line per
    `TextRegion` (a region per table cell), so no within-region adjacent pair ever exists there.
    Pooling over each whole page's lines instead still only counts genuine, unambiguous
    vertical-adjacency pairs (`get_neighbouring_line_pairs`' `'below'` relation requires direct
    horizontal overlap, not just page proximity), so it remains a meaningful "real local line
    gap" distribution without depending on a region granularity that isn't guaranteed."""
    gaps = []
    for scan in scans:
        lines = scan.get_lines()
        if len(lines) < 2:
            continue
        for line, neighbour, rel in get_neighbouring_line_pairs(lines):
            if rel == 'below' and neighbour is not None:
                gaps.append(line_vertical_gap(line, neighbour))
    return gaps


def derive_max_vertical_dist(scans: List[pdm.PageXMLScan], percentile: float = 95.0) -> int:
    """The `percentile`-th percentile of `corpus_adjacent_line_vertical_gaps(scans)` -- lines
    closer together than what real adjacent-line gaps look like, this often, count as vertical
    neighbours. Replaces a hand-tuned pixel constant with a per-corpus, per-resolution empirical
    threshold (the lesson from `sequence_patterns.py`'s `max_vertical_gap`/`max_horizontal_diff`,
    which needed manual recalibration per archive -- see `docs/findings.md`)."""
    gaps = corpus_adjacent_line_vertical_gaps(scans)
    if not gaps:
        raise ValueError("no within-region line pairs found to derive a threshold from")
    return int(np.percentile(gaps, percentile))


class RelationalPattern:
    """Per-document relational-neighbourhood fingerprints. For each line in each document, and
    each direction in `directions`, looks up that line's nearest neighbour
    (`LineNeighbourHood`) and emits a symbol `(own_cluster, direction, relation,
    neighbour_cluster)`, where `relation` is the RCC-8 relation between the two lines along the
    relevant axis (`region_calculus`). `self.tfidf` (shape `(num_docs, vocab_size)`) is the
    resulting fingerprint matrix, mirroring `GridPattern`'s API so it drops into the same
    downstream idioms (clustering, `colour_clustering.project_umap`, ...)."""

    def __init__(self, doc_set: List[pdm.PageXMLScan], line_clusters: pd.Series,
                 max_vertical_dist: int = None, directions: Sequence[str] = ('below', 'right'),
                 max_diff: int = 20):
        self.doc_ids = [doc.id for doc in doc_set]
        self.id2idx = {doc.id: di for di, doc in enumerate(doc_set)}
        self.idx2id = {di: doc.id for di, doc in enumerate(doc_set)}
        self.num_docs = len(doc_set)
        self.max_vertical_dist = max_vertical_dist
        self.directions = tuple(directions)
        self.max_diff = max_diff
        self.line_clusters = line_clusters
        self.coll_pattern_freq = Counter()
        self.doc_pattern_freq = defaultdict(Counter)
        self.pattern_doc_freq = Counter()
        self._compute_pattern_freq(doc_set)
        self.vocab = {pattern: pi for pi, pattern in enumerate(self.pattern_doc_freq.keys())}
        self.vocab_inv = {v: k for k, v in self.vocab.items()}
        self.vocab_size = len(self.vocab)
        self.tf = None
        self.tfidf = self._compute_tfidf()

    def _cluster_label(self, scan_id: str, line_id: str) -> Union[int, str]:
        key = (scan_id, line_id)
        if key not in self.line_clusters.index:
            return NOISE_CLUSTER_LABEL
        return self.line_clusters.loc[key]

    def _relation(self, line: pdm.PageXMLTextLine, neighbour: pdm.PageXMLTextLine,
                 direction: str) -> str:
        if direction in VERTICAL_DIRECTIONS:
            return region_calculus.get_vertical_region_relation(line, neighbour, max_diff=self.max_diff)
        return region_calculus.get_horizontal_region_relation(line, neighbour, max_diff=self.max_diff)

    def _compute_pattern_freq(self, doc_set: List[pdm.PageXMLScan]):
        for di, doc in enumerate(doc_set):
            lines = doc.get_lines()
            neighbourhood = LineNeighbourHood(lines, max_vertical_dist=self.max_vertical_dist)
            symbols = []
            for line in lines:
                own_cluster = self._cluster_label(doc.id, line.id)
                for direction in self.directions:
                    neighbours = neighbourhood.get_rel_neighbour(line, direction)
                    if not neighbours:
                        continue
                    neighbour = neighbours[0]
                    relation = self._relation(line, neighbour, direction)
                    neighbour_cluster = self._cluster_label(doc.id, neighbour.id)
                    symbols.append((own_cluster, direction, relation, neighbour_cluster))
            self.doc_pattern_freq[di].update(symbols)
            self.coll_pattern_freq.update(symbols)

        for doc_idx in self.doc_pattern_freq:
            self.pattern_doc_freq.update([pattern for pattern in self.doc_pattern_freq[doc_idx]])
        print(f"done computing relational patterns, {len(self.coll_pattern_freq)} distinct patterns")

    def _compute_tfidf(self) -> np.ndarray:
        self.tf, tfidf = compute_pattern_tfidf(self.doc_pattern_freq, self.pattern_doc_freq,
                                               self.vocab, self.num_docs, self.vocab_size)
        print("done computing tf-idf of relational patterns")
        return tfidf

    def doc_pf(self, doc_id: str) -> Counter:
        """Raw local relational-pattern frequency counter (`Counter`) for the document with id
        `doc_id`."""
        if doc_id not in self.id2idx:
            raise KeyError(f"unknown doc_id {doc_id}")
        return self.doc_pattern_freq[self.id2idx[doc_id]]

    def doc_pfidf(self, doc_id: str) -> np.ndarray:
        """TF-IDF relational-fingerprint vector (shape `(vocab_size,)`) for the document with
        id `doc_id` -- a row of `self.tfidf`."""
        if doc_id not in self.id2idx:
            raise KeyError(f"unknown doc_id {doc_id}")
        return self.tfidf[self.id2idx[doc_id]]

    def save(self, filename: str):
        """Pickle this `RelationalPattern` to `filename`."""
        with open(filename, 'wb') as fh:
            pickle.dump(self, fh)

    @staticmethod
    def load(filename: str) -> 'RelationalPattern':
        """Load a `RelationalPattern` previously written with `save`."""
        with open(filename, 'rb') as fh:
            relational_pattern = pickle.load(fh)
        return relational_pattern
