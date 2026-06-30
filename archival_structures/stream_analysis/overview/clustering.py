"""
Cluster document embeddings with UMAP + HDBSCAN.

The pipeline is split into two independently cacheable steps:

  1. `reduce_dimensions`: embeddings (+ optional layout features) -> UMAP 2D
  2. `cluster_embeddings`: UMAP 2D -> HDBSCAN labels

UMAP is normally the expensive step (especially the nearest-neighbour graph
built during `fit`). HDBSCAN is cheap to re-run with different parameters.
Caching them separately means you can sweep `min_cluster_size`, `min_samples`,
or `metric` for HDBSCAN without recomputing UMAP each time.

`run_clustering` is a convenience wrapper that runs both steps and preserves
the combined-cache behaviour from earlier versions of this module.

Optionally blends normalised layout features into the embedding space before
clustering (controlled by config: combine_layout_features). HDBSCAN assigns
label -1 to images that don't fit any cluster (outliers).

Exploring the cluster hierarchy (e.g. to find near-merge candidates) doesn't
require keeping the live HDBSCAN clusterer object around — `cluster_embeddings`
can cache the condensed tree and cluster persistence scores via
`save_cluster_hierarchy`, and `load_cluster_hierarchy` reconstructs them later
(including the rich `CondensedTree`/`SingleLinkageTree` objects with `.plot()`
and `.to_pandas()`) from that lightweight cache file.
"""

import json
import logging
from pathlib import Path

import hdbscan
import numpy as np
import umap
from hdbscan.plots import CondensedTree, SingleLinkageTree

from archival_structures.stream_analysis.overview.layout_analysis import layout_features_to_matrix

logger = logging.getLogger(__name__)


def reduce_dimensions(
    embeddings: np.ndarray,
    image_ids: list[str],
    config: dict,
    layout_features: dict | None = None,
    cache_file: str | None = None,
    umap_fit_sample_size: int | None = None,
    umap_transform_batch_size: int | None = None,
) -> dict:
    """
    Run UMAP dimensionality reduction and cache the 2D projection.

    Args:
        embeddings:       (N, D) float32 embedding matrix (L2-normalised)
        image_ids:        list of N image path strings
        config:           pipeline config dict (from AnalysisConfig.to_dict())
        layout_features:  optional dict from extract_all_layout_features()
        cache_file:       path to cache the UMAP result JSON. Overrides
                          config["clustering"]["umap_cache_file"]. Pass None to
                          skip caching.
        umap_fit_sample_size: if set and smaller than N, fit UMAP on a random
                          sample of this many images instead of the full set, then
                          project every image (including the sample) with
                          `.transform()`. This bounds the memory/time of the
                          expensive nearest-neighbour graph construction during
                          `fit`, which is what scales worst with N. Results will
                          differ slightly from a full `fit_transform`, since
                          out-of-sample points are projected onto a graph they
                          didn't help build.
        umap_transform_batch_size: chunk size used when projecting points with
                          `.transform()` (only relevant when umap_fit_sample_size
                          is set). Bounds peak memory during the transform step.
                          Defaults to transforming everything in one batch.

    Returns a dict with keys:
        umap_2d:   (N, 2) UMAP coordinates
        image_ids: list of N IDs (same order as input)
    """
    _cache_path = cache_file if cache_file is not None else config["clustering"].get("umap_cache_file")
    cache_enabled = _cache_path is not None

    if cache_enabled and Path(_cache_path).exists():
        logger.info(f"Loading cached UMAP projection from {_cache_path}")
        with open(_cache_path) as f:
            data = json.load(f)
        if len(data["image_ids"]) != len(image_ids):
            logger.warning(
                f"Cached UMAP result has {len(data['image_ids'])} images, "
                f"but {len(image_ids)} were requested — cache may be stale "
                "for this set of inventories."
            )
        data["umap_2d"] = np.array(data["umap_2d"])
        return data

    # --- Optionally augment embeddings with layout features ------------------
    feature_matrix = embeddings.copy()

    if config["clustering"].get("combine_layout_features") and layout_features:
        logger.info("Combining embeddings with layout features")
        layout_matrix = layout_features_to_matrix(image_ids, layout_features)
        weight = config["clustering"].get("layout_weight", 0.2)
        feature_matrix = np.hstack([
            feature_matrix,
            layout_matrix * weight,
        ])

    # --- UMAP -----------------------------------------------------------------
    umap_cfg = config["clustering"]["umap"]
    n_total = feature_matrix.shape[0]
    random_state = umap_cfg.get("random_state", 42)

    reducer = umap.UMAP(
        n_neighbors=umap_cfg["n_neighbors"],
        min_dist=umap_cfg["min_dist"],
        n_components=umap_cfg["n_components"],
        metric=umap_cfg["metric"],
        random_state=random_state,
    )

    if umap_fit_sample_size and umap_fit_sample_size < n_total:
        rng = np.random.default_rng(random_state)
        sample_idx = rng.choice(n_total, size=umap_fit_sample_size, replace=False)
        logger.info(
            f"Fitting UMAP on a sample of {umap_fit_sample_size}/{n_total} images "
            f"(n_neighbors={umap_cfg['n_neighbors']}, metric={umap_cfg['metric']}), "
            "then projecting the full set"
        )
        reducer.fit(feature_matrix[sample_idx])

        batch_size = umap_transform_batch_size or n_total
        umap_2d = np.empty((n_total, umap_cfg["n_components"]), dtype=np.float32)
        for start in range(0, n_total, batch_size):
            end = min(start + batch_size, n_total)
            umap_2d[start:end] = reducer.transform(feature_matrix[start:end])
            logger.info(f"  UMAP transform: {end}/{n_total} images projected")
    else:
        logger.info(
            f"Running UMAP (n_neighbors={umap_cfg['n_neighbors']}, "
            f"metric={umap_cfg['metric']}) on {n_total} images …"
        )
        umap_2d = reducer.fit_transform(feature_matrix)

    result = {"image_ids": image_ids, "umap_2d": umap_2d}

    if cache_enabled:
        Path(_cache_path).parent.mkdir(parents=True, exist_ok=True)
        with open(_cache_path, "w") as f:
            json.dump({"image_ids": image_ids, "umap_2d": umap_2d.tolist()}, f)
        logger.info(f"UMAP projection cached → {_cache_path}")

    return result


def save_cluster_hierarchy(
    clusterer,
    path: str,
    save_single_linkage_tree: bool = False,
) -> None:
    """
    Cache the HDBSCAN cluster hierarchy needed to explore merge candidates,
    without keeping the full clusterer object (which holds the raw data,
    KD-trees, etc., and isn't a lightweight thing to pickle/reload).

    Saves the condensed tree (the structure behind `clusterer.labels_` — each
    row records a parent/child split and the `lambda_val`, roughly an inverse
    distance, at which it happened) plus `cluster_persistence_` (a stability
    score per cluster; low values mean the split was weak — a candidate for
    merging with its sibling/parent). The single linkage tree (the full
    dendrogram before HDBSCAN's stability-based pruning) is much larger
    (~O(N) rows) and is only saved if requested.

    Args:
        clusterer: a fitted hdbscan.HDBSCAN object
        path:      output path (".npz" extension is added if missing)
        save_single_linkage_tree: also cache the full single-linkage dendrogram

    Reload with `load_cluster_hierarchy(path)`.
    """
    if clusterer.condensed_tree_ is None:
        raise ValueError(
            "clusterer.condensed_tree_ is None — this shouldn't happen for a "
            "fitted HDBSCAN clusterer."
        )

    # Normalise the extension up front so save/load always agree on the
    # actual filename — np.savez silently appends ".npz" if missing.
    path = str(path) if str(path).endswith(".npz") else f"{path}.npz"

    arrays = {
        "condensed_tree": clusterer.condensed_tree_.to_numpy(),
        # CondensedTree needs the final point labels to know which tree nodes
        # are the "selected" clusters (used by select_clusters=True in .plot()).
        "labels": clusterer.labels_,
        "cluster_persistence": clusterer.cluster_persistence_,
    }
    if save_single_linkage_tree:
        if clusterer.single_linkage_tree_ is None:
            logger.warning("clusterer.single_linkage_tree_ is None — skipping")
        else:
            arrays["single_linkage_tree"] = clusterer.single_linkage_tree_.to_numpy()

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, **arrays)
    logger.info(f"Cluster hierarchy cached → {path}")


def load_cluster_hierarchy(path: str) -> dict:
    """
    Reload a hierarchy cached by `save_cluster_hierarchy` and reconstruct the
    rich `CondensedTree` / `SingleLinkageTree` objects (same `.plot()`,
    `.to_pandas()`, `.to_networkx()` methods as `clusterer.condensed_tree_`)
    from the raw arrays — no live HDBSCAN clusterer required.

    Returns a dict with keys:
        condensed_tree:      hdbscan.plots.CondensedTree
        cluster_persistence: (n_clusters,) array of stability scores
        single_linkage_tree: hdbscan.plots.SingleLinkageTree, if it was saved
    """
    # Mirror the extension normalisation done in save_cluster_hierarchy.
    path = str(path) if str(path).endswith(".npz") else f"{path}.npz"
    data = np.load(path, allow_pickle=True)

    condensed_tree = CondensedTree(data["condensed_tree"], data["labels"])

    result = {
        "condensed_tree": condensed_tree,
        "cluster_persistence": data["cluster_persistence"],
    }
    if "single_linkage_tree" in data:
        result["single_linkage_tree"] = SingleLinkageTree(data["single_linkage_tree"])

    return result


def cluster_label_map(condensed_tree) -> dict[int, int]:
    """
    Map final cluster labels (0, 1, 2, ... as used in `labels_` /
    `get_cluster_members`) to their node id in the condensed tree's
    `parent`/`child` columns.

    The condensed tree's node ids are an internal numbering — point ids
    0..N-1 for individual images, then arbitrary higher ids for every
    candidate sub-cluster the algorithm considered while building the tree.
    Only some of those candidate nodes are actually "selected" as the final
    clusters reported in `labels_`; this resolves which node id each final
    label corresponds to, using the same selection logic HDBSCAN's own
    `.plot(select_clusters=True)` uses internally.

    Use the result to label the `parent`/`child` columns of
    `condensed_tree.to_pandas()` with actual cluster numbers — most rows
    won't correspond to a final cluster (they're intermediate tree
    structure), only the selected ones will.

    Args:
        condensed_tree: a hdbscan.plots.CondensedTree — either
                        `clusterer.condensed_tree_` or the object returned by
                        `load_cluster_hierarchy(...)["condensed_tree"]`.

    Returns a dict mapping label -> node id. Label -1 (outliers) is never
    included, since it has no corresponding tree node.
    """
    selected_node_ids = condensed_tree._select_clusters()
    return {label: int(node_id) for label, node_id in enumerate(selected_node_ids)}


def cluster_embeddings(
    umap_result: dict,
    config: dict,
    results_file: str | None = None,
    return_clusterer: bool = False,
    gen_min_span_tree: bool = False,
    prediction_data: bool = True,
    max_cluster_log_lines: int = 20,
    hierarchy_file: str | None = None,
    save_single_linkage_tree: bool = False,
) -> dict:
    """
    Run HDBSCAN on a (cached or freshly computed) UMAP 2D projection.

    Args:
        umap_result:      output of `reduce_dimensions` — dict with
                          "umap_2d" and "image_ids".
        config:           pipeline config dict (from AnalysisConfig.to_dict()).
                          Only config["clustering"]["hdbscan"] is used here.
        results_file:     path to cache the clustering result JSON. Overrides
                          config["clustering"]["results_file"]. Pass None to
                          skip caching. Use a different path per parameter
                          combination when sweeping HDBSCAN settings.
        return_clusterer: whether to also return the fitted HDBSCAN object.
                          Prefer `hierarchy_file` if you only need it to explore
                          the cluster hierarchy — that avoids keeping the much
                          larger clusterer object (raw data, KD-trees, ...) alive.
        gen_min_span_tree: whether HDBSCAN should retain the minimum spanning
                          tree (only needed to later call
                          `clusterer.minimum_spanning_tree_.plot()`). Leave
                          False unless you need it — for large N, plotting the
                          full tree inline can itself stall/crash a Jupyter
                          browser tab; downsample before plotting if you do.
        prediction_data:  whether HDBSCAN should retain the data structures
                          needed for `approximate_predict()` on new points.
        max_cluster_log_lines: log one line per cluster only while there are
                          at most this many; above that, log a size summary
                          plus the largest few clusters instead. Logging
                          thousands of lines per run can flood Jupyter's iopub
                          channel and cause a "Server Connection Error" /
                          unresponsive kernel.
        hierarchy_file:   if set, cache the condensed tree + cluster persistence
                          scores via `save_cluster_hierarchy` — see that function
                          for what this captures and how to use it to find
                          merge candidates. Overrides
                          config["clustering"].get("hierarchy_file"). Pass None
                          (the default) to skip.
        save_single_linkage_tree: forwarded to `save_cluster_hierarchy`.

    Returns a dict with keys:
        umap_2d:        (N, 2) UMAP coordinates (passed through from umap_result)
        labels:          (N,)   cluster labels (-1 = outlier)
        image_ids:       list of N IDs
        n_clusters:      number of clusters found (excluding outliers)
        n_outliers:      number of outlier images
        probabilities:   (N,) strength of each point's membership in its cluster
        outlier_scores:  (N,) GLOSH outlier score for each point
    """
    _cache_path = results_file if results_file is not None else config["clustering"].get("results_file")
    cache_enabled = _cache_path is not None

    if cache_enabled and Path(_cache_path).exists():
        logger.info(f"Loading cached clustering results from {_cache_path}")
        with open(_cache_path) as f:
            data = json.load(f)
        data["umap_2d"] = np.array(data["umap_2d"])
        data["labels"] = np.array(data["labels"])
        # Older cache files predate these fields — keep loading them gracefully.
        if "probabilities" in data:
            data["probabilities"] = np.array(data["probabilities"])
        if "outlier_scores" in data:
            data["outlier_scores"] = np.array(data["outlier_scores"])
        return data

    image_ids = umap_result["image_ids"]
    umap_2d = np.asarray(umap_result["umap_2d"])

    # --- HDBSCAN --------------------------------------------------------------
    hdb_cfg = config["clustering"]["hdbscan"]
    logger.info(
        f"Running HDBSCAN (min_cluster_size={hdb_cfg['min_cluster_size']}) …"
    )
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=hdb_cfg["min_cluster_size"],
        min_samples=hdb_cfg.get("min_samples", 5),
        metric=hdb_cfg.get("metric", "euclidean"),
        prediction_data=prediction_data,
        gen_min_span_tree=gen_min_span_tree,
    )
    labels = clusterer.fit_predict(umap_2d)

    n_clusters = int(labels.max()) + 1
    n_outliers = int((labels == -1).sum())
    logger.info(
        f"Found {n_clusters} clusters, {n_outliers} outliers "
        f"({100 * n_outliers / len(labels):.1f} %)"
    )

    # Cluster size summary. Avoid one log line per cluster when there are
    # many — each call is a separate message sent to the notebook front end,
    # and thousands of them in a tight loop can overwhelm Jupyter's iopub
    # channel (manifests as "Server Connection Error" / a hung kernel).
    if n_clusters:
        sizes = np.bincount(labels[labels >= 0], minlength=n_clusters)
        if n_clusters <= max_cluster_log_lines:
            for c, size in enumerate(sizes):
                logger.info(f"  Cluster {c}: {int(size)} images")
        else:
            top = np.argsort(sizes)[::-1][:5]
            logger.info(
                f"  Cluster sizes: min={int(sizes.min())}, "
                f"median={int(np.median(sizes))}, max={int(sizes.max())} "
                f"(showing top 5 of {n_clusters} clusters)"
            )
            for c in top:
                logger.info(f"    Cluster {c}: {int(sizes[c])} images")

    result = {
        "image_ids": image_ids,
        "umap_2d": umap_2d,
        "labels": labels,
        "n_clusters": n_clusters,
        "n_outliers": n_outliers,
        "probabilities": clusterer.probabilities_,
        "outlier_scores": clusterer.outlier_scores_,
    }

    if cache_enabled:
        Path(_cache_path).parent.mkdir(parents=True, exist_ok=True)
        with open(_cache_path, "w") as f:
            json.dump({
                "image_ids": image_ids,
                "umap_2d": umap_2d.tolist(),
                "labels": labels.tolist(),
                "n_clusters": n_clusters,
                "n_outliers": n_outliers,
                "probabilities": clusterer.probabilities_.tolist(),
                "outlier_scores": clusterer.outlier_scores_.tolist(),
            }, f)
        logger.info(f"Clustering results cached → {_cache_path}")

    _hierarchy_path = hierarchy_file if hierarchy_file is not None else config["clustering"].get("hierarchy_file")
    if _hierarchy_path is not None:
        save_cluster_hierarchy(clusterer, _hierarchy_path, save_single_linkage_tree=save_single_linkage_tree)
        result["hierarchy_file"] = _hierarchy_path

    if return_clusterer:
        return result, clusterer
    return result


def run_clustering(
    embeddings: np.ndarray,
    image_ids: list[str],
    config: dict,
    layout_features: dict | None = None,
    results_file: str | None = None,
    return_clusterer: bool = False,
    umap_cache_file: str | None = None,
    umap_fit_sample_size: int | None = None,
    umap_transform_batch_size: int | None = None,
    gen_min_span_tree: bool = False,
    prediction_data: bool = True,
    hierarchy_file: str | None = None,
    save_single_linkage_tree: bool = False,
) -> dict:
    """
    Convenience wrapper: run `reduce_dimensions` then `cluster_embeddings`.

    For exploring multiple HDBSCAN parameter settings on the same embeddings,
    prefer calling `reduce_dimensions` once (with a `cache_file`) and then
    `cluster_embeddings` repeatedly with different `config["clustering"]["hdbscan"]`
    values and different `results_file` paths — that way UMAP only runs once.

    Args:
        embeddings, image_ids, config, layout_features: see `reduce_dimensions`.
        results_file:     forwarded to `cluster_embeddings`.
        return_clusterer: forwarded to `cluster_embeddings`.
        umap_cache_file:  forwarded to `reduce_dimensions` as `cache_file`.
        umap_fit_sample_size, umap_transform_batch_size: forwarded to
                          `reduce_dimensions`.
        gen_min_span_tree, prediction_data: forwarded to `cluster_embeddings`.
        hierarchy_file, save_single_linkage_tree: forwarded to
                          `cluster_embeddings` — see `save_cluster_hierarchy`.

    Returns the same dict as `cluster_embeddings`.
    """
    umap_result = reduce_dimensions(
        embeddings,
        image_ids,
        config,
        layout_features=layout_features,
        cache_file=umap_cache_file,
        umap_fit_sample_size=umap_fit_sample_size,
        umap_transform_batch_size=umap_transform_batch_size,
    )
    return cluster_embeddings(
        umap_result,
        config,
        results_file=results_file,
        return_clusterer=return_clusterer,
        gen_min_span_tree=gen_min_span_tree,
        prediction_data=prediction_data,
        hierarchy_file=hierarchy_file,
        save_single_linkage_tree=save_single_linkage_tree,
    )


def get_cluster_members(clustering: dict) -> dict[int, list[str]]:
    """Return {cluster_label: [image_id, …]} mapping (outliers at key -1)."""
    members: dict[int, list[str]] = {}
    for img_id, label in zip(clustering["image_ids"], clustering["labels"].tolist()):
        members.setdefault(int(label), []).append(img_id)
    return members
