"""Cluster pages by the spatial layout of their text lines.

`archival_structures.analysis.grid_analysis.GridPattern` already grids a set of PageXML
documents by their *line* geometry (not region geometry -- it calls `get_line_grid_points`, not
`get_region_grid_points`), extracts recurring small grid patterns, and computes a TF-IDF vector
per document over those patterns. That TF-IDF vector is exactly the per-page layout feature
vector this task needs; this module just clusters those vectors with HDBSCAN, following the same
idiom as `archival_structures.clustering.colour_clustering.cluster_features`.
"""
from typing import List, Tuple

import hdbscan
import pagexml.model.physical_document_model as pdm
import pandas as pd
from sklearn.preprocessing import normalize

from archival_structures.analysis.grid_analysis import GridPattern

HDBSCAN_MIN_CLUSTER_SIZE = 5


def build_grid_pattern(scans: List[pdm.PageXMLScan], unit_size: int = 50,
                       pattern_size: int = 3) -> GridPattern:
    """Build a `GridPattern` over `scans`. Exposed separately from `cluster_page_layouts` so the
    (expensive, grid-construction) `GridPattern` can be reused -- e.g. saved/loaded via its own
    `save`/`load`, or clustered again with different HDBSCAN parameters without rebuilding it."""
    return GridPattern(scans, unit_size=unit_size, pattern_size=pattern_size)


def cluster_page_layouts(scans: List[pdm.PageXMLScan], unit_size: int = 50, pattern_size: int = 3,
                         min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
                         min_samples: int = None) -> Tuple[pd.Series, GridPattern]:
    """Cluster `scans` by the layout of their text lines.

    :return: a `(clusters, grid_pattern)` pair, where `clusters` is a `pd.Series` of integer
            cluster labels (-1 = noise) indexed by scan id, and `grid_pattern` is the
            `GridPattern` built along the way (its `.tfidf` matrix and `.vocab` may be useful for
            further inspection, e.g. with `archival_structures.clustering.colour_clustering.project_umap`).

    TF-IDF row norms vary a lot between pages (a page with more text has a "longer" vector even
    if its *pattern composition* is identical to a shorter page), which under plain Euclidean
    distance dominates clustering by overall text density rather than layout shape. Rows are
    L2-normalised before clustering to correct for that -- equivalent to clustering by cosine
    similarity, which HDBSCAN's backend does not support directly.
    """
    grid_pattern = build_grid_pattern(scans, unit_size=unit_size, pattern_size=pattern_size)
    normalised_tfidf = normalize(grid_pattern.tfidf)
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples,
                                metric='euclidean', cluster_selection_method='eom')
    labels = clusterer.fit_predict(normalised_tfidf)
    scan_ids = [grid_pattern.idx2id[i] for i in range(grid_pattern.num_docs)]
    return pd.Series(labels, index=scan_ids, name='cluster'), grid_pattern
