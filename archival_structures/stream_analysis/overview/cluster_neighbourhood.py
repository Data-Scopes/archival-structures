"""
Explore the HDBSCAN cluster hierarchy around a specific cluster.

Bulk-tagging a cluster of visually similar pages (e.g. DINOv2 embeddings + UMAP/
HDBSCAN) rarely captures every instance of a document type: HDBSCAN often splits
a single real-world page type across several clusters, especially when it sits
at varying density in the embedding space. This module answers "what else is
nearby?" using the *cluster hierarchy itself* — the condensed tree HDBSCAN
builds while deciding which sub-clusters to keep — rather than an ad hoc
distance in the original feature space.

Concretely: :func:`nearest_cluster_branches` walks up the tree from a query
cluster's node, and at each ancestor step reports the sibling branch that
merges with it -- together with the distance at which that merge happens (the
inverse of the split's ``lambda_val``) and how many of the currently reported
final clusters fall inside that branch. A branch is often a single cluster,
but can also be a whole subtree containing several clusters that split apart
again *after* separating from the query's branch — those clusters merge with
the query at the same tree distance and can't be told apart by the hierarchy
alone.

:func:`rank_by_centroid_distance` breaks that tie using an auxiliary distance
(Euclidean distance between cluster centroids in embedding or UMAP space),
letting you order same-branch clusters by which one is actually closest to
the query cluster.

Typical usage, after fitting HDBSCAN and picking a query cluster to
bulk-annotate::

    from archival_structures.stream_analysis.overview.cluster_neighbourhood import (
        describe_cluster_neighbourhood, format_cluster_neighbourhood,
    )

    branches = describe_cluster_neighbourhood(
        query_label, clusterer.condensed_tree_, embeddings, labels, top_n=5,
    )
    format_cluster_neighbourhood(branches)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from archival_structures.stream_analysis.overview.clustering import cluster_label_map


def get_cluster_tree_maps(
    condensed_tree,
) -> tuple[dict[int, tuple[int, float]], dict[int, list[tuple[int, float, int]]]]:
    """
    Build parent/child lookup tables over the *cluster* structure of a
    condensed tree, ignoring rows that just record an individual point
    falling out of a cluster (``child_size == 1``) -- those aren't cluster
    merges, just noise shedding, and would otherwise clutter the walk.

    Args:
        condensed_tree: a ``hdbscan.plots.CondensedTree`` -- either
            ``clusterer.condensed_tree_`` or the object returned by
            ``load_cluster_hierarchy(...)["condensed_tree"]``.

    Returns:
        A ``(parent_of, children_of)`` pair:

        - ``parent_of``: ``{child_node_id: (parent_node_id, lambda_val)}``
        - ``children_of``: ``{parent_node_id: [(child_node_id, lambda_val,
          child_size), ...]}``

        Every node id here is a condensed-tree node id, not a final cluster
        label -- use :func:`~archival_structures.stream_analysis.overview.clustering.cluster_label_map`
        to translate between the two.
    """
    df = condensed_tree.to_pandas()
    cluster_df = df[df["child_size"] > 1]

    parent_of: dict[int, tuple[int, float]] = {}
    children_of: dict[int, list[tuple[int, float, int]]] = {}
    for row in cluster_df.itertuples(index=False):
        parent_of[int(row.child)] = (int(row.parent), float(row.lambda_val))
        children_of.setdefault(int(row.parent), []).append(
            (int(row.child), float(row.lambda_val), int(row.child_size))
        )
    return parent_of, children_of


def _collect_descendant_clusters(
    node_id: int,
    children_of: dict[int, list[tuple[int, float, int]]],
    node_to_label: dict[int, int],
) -> list[int]:
    """Final cluster labels found by descending from *node_id*.

    Stops as soon as it reaches a node that is itself a selected cluster --
    HDBSCAN's excess-of-mass selection can prefer a coarser node over its own
    (more fragmented) descendants, so a selected node's internal structure
    should not be unpacked further.
    """
    if node_id in node_to_label:
        return [node_to_label[node_id]]
    leaves: list[int] = []
    for child_id, _lambda_val, _child_size in children_of.get(node_id, []):
        leaves.extend(_collect_descendant_clusters(child_id, children_of, node_to_label))
    return leaves


def nearest_cluster_branches(
    query_label: int,
    condensed_tree,
    top_n: int = 5,
) -> list[dict]:
    """
    Walk up the condensed tree from *query_label* and return the nearest
    sibling branches, ordered by increasing merge distance.

    At each step, the branch that merges with the query's growing cluster is
    either a single final cluster or a whole subtree that contains several
    final clusters (because it split further *after* separating from the
    query) -- both cases are reported the same way, with ``n_clusters``
    telling you which happened.

    A branch that dissolved entirely into noise before reaching
    ``min_cluster_size`` (no final cluster inside it) is skipped -- there is
    nothing to annotate there -- but the walk still climbs past it, so it
    never counts against *top_n*.

    Args:
        query_label: the cluster label to search around (as in
            ``clusterer.labels_`` / ``cluster_label_map``). Must be a
            selected cluster (not the noise label ``-1``).
        condensed_tree: a ``hdbscan.plots.CondensedTree``.
        top_n: maximum number of branches to return.

    Returns:
        A list of up to *top_n* dicts, ordered by ascending ``distance``,
        each with keys:

        - ``distance``: ``1 / lambda_val`` of the merge split -- the same
          units as the space HDBSCAN was fit on (e.g. UMAP coordinates).
        - ``lambda_val``: the raw condensed-tree split value.
        - ``node_id``: condensed-tree node id of the branch's root.
        - ``n_points``: number of points in the branch (all its final
          clusters together, plus any sub-min_cluster_size noise inside it).
        - ``cluster_labels``: final cluster labels contained in the branch,
          ascending.
        - ``n_clusters``: ``len(cluster_labels)`` -- how many of the
          currently reported clusters would be merged with the query at
          this distance.

    Raises:
        ValueError: if *query_label* is not a selected cluster in this tree.
    """
    label_to_node = cluster_label_map(condensed_tree)
    if query_label not in label_to_node:
        raise ValueError(
            f"Cluster {query_label} is not a selected cluster in this condensed tree "
            f"(selected labels: {sorted(label_to_node)})"
        )

    node_to_label = {node_id: label for label, node_id in label_to_node.items()}
    parent_of, children_of = get_cluster_tree_maps(condensed_tree)

    branches: list[dict] = []
    current = label_to_node[query_label]

    while len(branches) < top_n and current in parent_of:
        parent, _lambda_val = parent_of[current]
        siblings = [
            (child_id, child_lambda, child_size)
            for child_id, child_lambda, child_size in children_of.get(parent, [])
            if child_id != current
        ]
        for sibling_id, sibling_lambda, sibling_size in siblings:
            cluster_labels = _collect_descendant_clusters(sibling_id, children_of, node_to_label)
            if not cluster_labels:
                continue  # branch is pure noise -- nothing to annotate
            branches.append({
                "distance": 1.0 / sibling_lambda,
                "lambda_val": sibling_lambda,
                "node_id": sibling_id,
                "n_points": sibling_size,
                "cluster_labels": sorted(cluster_labels),
                "n_clusters": len(cluster_labels),
            })
        current = parent

    return branches[:top_n]


def cluster_centroids(embeddings: np.ndarray, labels: np.ndarray) -> dict[int, np.ndarray]:
    """
    Mean embedding vector per cluster label.

    Args:
        embeddings: *(N, D)* embedding array (raw embeddings or a UMAP
            projection -- whichever space you want centroid distances in).
        labels: *(N,)* cluster labels aligned with *embeddings*.

    Returns:
        ``{label: centroid}`` for every non-noise label (``-1`` excluded).
    """
    labels = np.asarray(labels)
    return {
        int(label): embeddings[labels == label].mean(axis=0)
        for label in np.unique(labels)
        if label != -1
    }


def rank_by_centroid_distance(
    query_label: int,
    candidate_labels: list[int],
    centroids: dict[int, np.ndarray],
) -> list[tuple[int, float]]:
    """
    Order *candidate_labels* by Euclidean centroid distance to *query_label*.

    The condensed tree alone cannot distinguish between clusters inside the
    same branch -- they merge with the query at the same tree distance by
    construction. This ranks them by an auxiliary distance instead, so that
    when a branch contains several clusters you know which one to check
    first.

    Args:
        query_label: the cluster label centroid distances are measured from.
        candidate_labels: cluster labels to rank (e.g. one branch's
            ``cluster_labels`` from :func:`nearest_cluster_branches`).
        centroids: output of :func:`cluster_centroids`.

    Returns:
        ``[(label, distance), ...]`` sorted by ascending distance.
    """
    query_centroid = centroids[query_label]
    return sorted(
        (
            (label, float(np.linalg.norm(centroids[label] - query_centroid)))
            for label in candidate_labels
        ),
        key=lambda pair: pair[1],
    )


def describe_cluster_neighbourhood(
    query_label: int,
    condensed_tree,
    embeddings: np.ndarray,
    labels: np.ndarray,
    top_n: int = 5,
) -> list[dict]:
    """
    Convenience wrapper: :func:`nearest_cluster_branches` plus a within-branch
    centroid-distance ranking for every branch found.

    Args:
        query_label, condensed_tree, top_n: see :func:`nearest_cluster_branches`.
        embeddings, labels: see :func:`cluster_centroids` -- pass the same
            embedding space the ranking should happen in (raw embeddings or
            the UMAP projection HDBSCAN was actually fit on).

    Returns:
        The same list :func:`nearest_cluster_branches` returns, with an
        added ``ranked_clusters`` key per branch: ``[(label, distance), ...]``
        sorted by ascending centroid distance to *query_label*.
    """
    centroids = cluster_centroids(embeddings, labels)
    branches = nearest_cluster_branches(query_label, condensed_tree, top_n=top_n)
    for branch in branches:
        branch["ranked_clusters"] = rank_by_centroid_distance(
            query_label, branch["cluster_labels"], centroids
        )
    return branches


def format_cluster_neighbourhood(branches: list[dict]) -> pd.DataFrame:
    """
    Render :func:`nearest_cluster_branches` / :func:`describe_cluster_neighbourhood`
    output as a notebook-friendly table.

    Args:
        branches: output of either function above.

    Returns:
        A `pandas.DataFrame` with one row per branch: ``rank``, ``distance``,
        ``n_clusters``, ``n_points``, and a ``clusters`` column listing the
        cluster labels (annotated with centroid distance when
        ``ranked_clusters`` is present).
    """
    rows = []
    for i, branch in enumerate(branches, start=1):
        ranked = branch.get("ranked_clusters")
        if ranked is not None:
            clusters_str = ", ".join(f"{label} (Δ={dist:.2f})" for label, dist in ranked)
        else:
            clusters_str = ", ".join(str(label) for label in branch["cluster_labels"])
        rows.append({
            "rank": i,
            "distance": round(branch["distance"], 3),
            "n_clusters": branch["n_clusters"],
            "n_points": branch["n_points"],
            "clusters": clusters_str,
        })
    return pd.DataFrame(rows)
