import numpy as np
import pytest

hdbscan = pytest.importorskip("hdbscan")

from archival_structures.stream_analysis.overview.cluster_neighbourhood import (
    cluster_centroids,
    describe_cluster_neighbourhood,
    format_cluster_neighbourhood,
    get_cluster_tree_maps,
    nearest_cluster_branches,
    rank_by_centroid_distance,
)
from archival_structures.stream_analysis.overview.clustering import cluster_label_map


def _fit_five_blob_hierarchy():
    """Five well-separated Gaussian blobs with a known nesting structure.

    Blobs are placed so the resulting HDBSCAN condensed tree has a clear,
    reproducible hierarchy: blob 0 is the most isolated (merges with
    everything else only at the root), blobs 3/4 are the closest pair, and
    blobs 1/2/3/4 merge together before merging with blob 0.
    """
    rng = np.random.default_rng(0)
    centers = [(0, 0), (0, 5), (5, 0), (5, 5), (2.5, 10)]
    points = [rng.normal(loc=c, scale=0.3, size=(30, 2)) for c in centers]
    X = np.vstack(points)
    true_labels = np.repeat(np.arange(5), 30)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=10, min_samples=3)
    labels = clusterer.fit_predict(X)
    assert labels.max() + 1 == 5, "test assumes exactly 5 clusters recovered"
    assert (labels == -1).sum() == 0, "test assumes no noise points"
    return X, labels, true_labels, clusterer


class TestGetClusterTreeMaps:
    def test_filters_out_single_point_dropouts(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        parent_of, children_of = get_cluster_tree_maps(clusterer.condensed_tree_)

        df = clusterer.condensed_tree_.to_pandas()
        n_cluster_rows = int((df["child_size"] > 1).sum())
        assert len(parent_of) == n_cluster_rows

        # every child appears as a key exactly once (each node created by one split)
        all_children = [child for children in children_of.values() for child, _, _ in children]
        assert len(all_children) == len(set(all_children))


class TestNearestClusterBranches:
    def test_raises_for_unselected_label(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        with pytest.raises(ValueError):
            nearest_cluster_branches(-1, clusterer.condensed_tree_)
        with pytest.raises(ValueError):
            nearest_cluster_branches(999, clusterer.condensed_tree_)

    def test_distances_strictly_increase(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        for label in label_to_node:
            branches = nearest_cluster_branches(label, clusterer.condensed_tree_, top_n=10)
            distances = [b["distance"] for b in branches]
            assert distances == sorted(distances)
            assert len(distances) == len(set(distances))

    def test_all_clusters_eventually_reachable(self):
        """Climbing to the root from any cluster must reach every other cluster."""
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        all_labels = set(label_to_node)

        for label in all_labels:
            branches = nearest_cluster_branches(label, clusterer.condensed_tree_, top_n=10)
            seen = {label}
            for b in branches:
                seen.update(b["cluster_labels"])
            assert seen == all_labels

    def test_top_n_limits_number_of_branches(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        query_label = next(iter(label_to_node))
        branches = nearest_cluster_branches(query_label, clusterer.condensed_tree_, top_n=1)
        assert len(branches) <= 1

    def test_branch_fields_are_consistent(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        query_label = next(iter(label_to_node))
        branches = nearest_cluster_branches(query_label, clusterer.condensed_tree_, top_n=10)
        for b in branches:
            assert b["distance"] == pytest.approx(1.0 / b["lambda_val"])
            assert b["n_clusters"] == len(b["cluster_labels"])
            assert b["cluster_labels"] == sorted(b["cluster_labels"])
            assert query_label not in b["cluster_labels"]


class TestClusterCentroids:
    def test_excludes_noise_and_matches_manual_mean(self):
        X, labels, _, _ = _fit_five_blob_hierarchy()
        centroids = cluster_centroids(X, labels)
        assert -1 not in centroids
        for label in np.unique(labels):
            if label == -1:
                continue
            np.testing.assert_allclose(centroids[int(label)], X[labels == label].mean(axis=0))


class TestRankByCentroidDistance:
    def test_orders_by_ascending_distance(self):
        X, labels, _, _ = _fit_five_blob_hierarchy()
        centroids = cluster_centroids(X, labels)
        all_labels = sorted(centroids)
        query_label = all_labels[0]
        candidates = all_labels[1:]

        ranked = rank_by_centroid_distance(query_label, candidates, centroids)
        distances = [d for _, d in ranked]
        assert distances == sorted(distances)
        assert {label for label, _ in ranked} == set(candidates)

    def test_matches_known_blob_geometry(self):
        """Blobs 3 and 4 (centres (5,5) and (2.5,10)) are closer to each other
        than blob 3 is to blob 1 (centre (0,5)) -- sanity-check against the
        known synthetic geometry rather than just internal consistency."""
        X, labels, _, _ = _fit_five_blob_hierarchy()
        centroids = cluster_centroids(X, labels)
        ranked = rank_by_centroid_distance(3, [1, 4], centroids)
        assert ranked[0][0] == 4


class TestDescribeAndFormatClusterNeighbourhood:
    def test_adds_ranked_clusters_per_branch(self):
        X, labels, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        query_label = next(iter(label_to_node))

        branches = describe_cluster_neighbourhood(
            query_label, clusterer.condensed_tree_, X, labels, top_n=10
        )
        for b in branches:
            assert "ranked_clusters" in b
            assert {label for label, _ in b["ranked_clusters"]} == set(b["cluster_labels"])

    def test_format_produces_one_row_per_branch(self):
        X, labels, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        query_label = next(iter(label_to_node))

        branches = describe_cluster_neighbourhood(
            query_label, clusterer.condensed_tree_, X, labels, top_n=10
        )
        df = format_cluster_neighbourhood(branches)
        assert len(df) == len(branches)
        assert list(df["rank"]) == list(range(1, len(branches) + 1))
        assert set(df.columns) == {"rank", "distance", "n_clusters", "n_points", "clusters"}

    def test_format_without_ranking(self):
        _, _, _, clusterer = _fit_five_blob_hierarchy()
        label_to_node = cluster_label_map(clusterer.condensed_tree_)
        query_label = next(iter(label_to_node))
        branches = nearest_cluster_branches(query_label, clusterer.condensed_tree_, top_n=10)
        df = format_cluster_neighbourhood(branches)
        assert len(df) == len(branches)
