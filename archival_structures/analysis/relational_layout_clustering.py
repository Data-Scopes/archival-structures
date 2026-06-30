"""Cluster pages by their line-neighbourhood relational fingerprint.

`archival_structures.analysis.relational_patterns.RelationalPattern` builds a TF-IDF vector per
page over `(own_cluster, direction, relation, neighbour_cluster)` symbols -- exactly the kind of
per-page feature vector `archival_structures.analysis.page_layout_clustering.cluster_page_layouts`
clusters for the pixel-pattern fingerprint. This module clusters `RelationalPattern.tfidf` the
same way, with HDBSCAN, as a complementary view: `page_layout_clustering` groups pages by *what
the layout looks like*, this module groups them by *how line types relate to their neighbours*.
"""
from typing import List, Sequence, Tuple

import hdbscan
import pagexml.model.physical_document_model as pdm
import pandas as pd
from sklearn.preprocessing import normalize

from archival_structures.analysis.relational_patterns import RelationalPattern

HDBSCAN_MIN_CLUSTER_SIZE = 5


def build_relational_pattern(scans: List[pdm.PageXMLScan], line_clusters: pd.Series,
                             max_vertical_dist: int = None,
                             directions: Sequence[str] = ('below', 'right'),
                             max_diff: int = 20) -> RelationalPattern:
    """Build a `RelationalPattern` over `scans`. Exposed separately from
    `cluster_relational_layouts` so the (expensive) `RelationalPattern` can be reused -- e.g.
    saved/loaded via its own `save`/`load`, or clustered again with different HDBSCAN
    parameters without rebuilding it."""
    return RelationalPattern(scans, line_clusters, max_vertical_dist=max_vertical_dist,
                             directions=directions, max_diff=max_diff)


def cluster_relational_layouts(scans: List[pdm.PageXMLScan], line_clusters: pd.Series,
                               max_vertical_dist: int = None,
                               directions: Sequence[str] = ('below', 'right'),
                               max_diff: int = 20,
                               min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
                               min_samples: int = None) -> Tuple[pd.Series, RelationalPattern]:
    """Cluster `scans` by their line-neighbourhood relational fingerprint.

    :param line_clusters: per-line cluster labels, as a `pd.Series` indexed by `(scan_id,
            line_id)` -- e.g. from `archival_structures.analysis.line_clustering.cluster_lines`
            with that index set explicitly. This module takes no dependency on `line_clustering`
            or any specific clustering strategy.
    :return: a `(clusters, relational_pattern)` pair, where `clusters` is a `pd.Series` of
            integer cluster labels (-1 = noise) indexed by scan id, and `relational_pattern` is
            the `RelationalPattern` built along the way.

    As with `cluster_page_layouts`, TF-IDF row norms vary with how many relational symbols a
    page has -- rows are L2-normalised before clustering, equivalent to clustering by cosine
    similarity.
    """
    relational_pattern = build_relational_pattern(scans, line_clusters, max_vertical_dist=max_vertical_dist,
                                                   directions=directions, max_diff=max_diff)
    normalised_tfidf = normalize(relational_pattern.tfidf)
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples,
                                metric='euclidean', cluster_selection_method='eom')
    labels = clusterer.fit_predict(normalised_tfidf)
    scan_ids = [relational_pattern.idx2id[i] for i in range(relational_pattern.num_docs)]
    return pd.Series(labels, index=scan_ids, name='cluster'), relational_pattern
