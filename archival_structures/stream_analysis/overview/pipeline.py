"""
Overview pipeline (Part 1).

Steps:
  1. Extract visual embeddings (CLIP or DINOv2)
  2. Extract layout features (thumbnail-safe, no OCR)
  3. Cluster with UMAP + HDBSCAN
  4. VLM-tag a random sample (requires ANTHROPIC_API_KEY)
  5. Generate visualisations
"""

import json
import logging
from pathlib import Path

import numpy as np

from archival_structures.stream_analysis.config import AnalysisConfig
from archival_structures.stream_analysis.overview.embeddings import extract_embeddings
from archival_structures.stream_analysis.overview.layout_analysis import extract_all_layout_features
from archival_structures.stream_analysis.overview.clustering import (
    run_clustering,
    reduce_dimensions,
    cluster_embeddings,
    get_cluster_members,
)
from archival_structures.stream_analysis.overview.visualization import (
    plot_umap,
    plot_cluster_grids,
    plot_layout_distributions,
    plot_vlm_summary,
)

logger = logging.getLogger(__name__)


def run_overview(cfg: AnalysisConfig, run_vlm: bool = True) -> dict:
    """
    Run the full overview pipeline.

    Args:
        cfg:     AnalysisConfig instance
        run_vlm: whether to run VLM tagging (requires ANTHROPIC_API_KEY)

    Returns the clustering result dict.
    """
    config = cfg.to_dict()
    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1: Embeddings
    # ------------------------------------------------------------------
    logger.info("=== Step 1: Extracting visual embeddings ===")
    embeddings, image_ids = extract_embeddings(config)
    logger.info(f"  {len(image_ids)} images, embedding dim = {embeddings.shape[1]}")

    # ------------------------------------------------------------------
    # Step 2: Layout features
    # ------------------------------------------------------------------
    logger.info("=== Step 2: Extracting layout features ===")
    layout_features = extract_all_layout_features(config)

    if layout_features:
        sample = next(iter(layout_features.values()))
        logger.info(f"  Example layout features: {json.dumps(sample, indent=None)}")

    # ------------------------------------------------------------------
    # Step 3: Clustering
    # ------------------------------------------------------------------
    logger.info("=== Step 3: Clustering ===")
    clustering = run_clustering(embeddings, image_ids, config, layout_features)
    cluster_members = get_cluster_members(clustering)

    logger.info(f"  Clusters found: {clustering['n_clusters']}")
    logger.info(f"  Outliers:       {clustering['n_outliers']}")
    for c_id, members in sorted(cluster_members.items()):
        name = "outliers" if c_id == -1 else f"Cluster {c_id}"
        logger.info(f"    {name}: {len(members)} images")

    # ------------------------------------------------------------------
    # Step 4: VLM tagging (optional)
    # ------------------------------------------------------------------
    vlm_tags = {}
    if run_vlm:
        logger.info("=== Step 4: VLM tagging ===")
        try:
            from archival_structures.stream_analysis.overview.vlm_tagging import (
                run_vlm_tagging,
                summarise_vlm_tags,
            )
            vlm_tags = run_vlm_tagging(config, image_ids)
            summary = summarise_vlm_tags(vlm_tags)
            logger.info("  VLM tag summary:")
            for field, counts in summary.items():
                logger.info(f"    {field}: {counts}")
        except ImportError:
            logger.warning("  anthropic package not installed — skipping VLM tagging")
        except Exception as e:
            logger.warning(f"  VLM tagging failed: {e}")
    else:
        logger.info("=== Step 4: VLM tagging skipped ===")
        vlm_tags_file = config["vlm_tagging"]["results_file"]
        if Path(vlm_tags_file).exists():
            with open(vlm_tags_file) as f:
                vlm_tags = json.load(f)
            logger.info(f"  Loaded {len(vlm_tags)} cached VLM tags")

    # ------------------------------------------------------------------
    # Step 5: Visualisations
    # ------------------------------------------------------------------
    logger.info("=== Step 5: Generating visualisations ===")
    plot_umap(clustering, config)
    plot_cluster_grids(clustering, config)
    plot_layout_distributions(clustering, layout_features, config)
    if vlm_tags:
        plot_vlm_summary(vlm_tags, config)

    logger.info(f"All visualisations saved to {cfg.visualization_dir}/")
    logger.info("Overview pipeline complete. Review the cluster grids and UMAP plot.")
    return clustering


def load_combined_inventories(
    inventory_dirs: list[str | Path],
) -> tuple[np.ndarray, list[str], dict[str, dict]]:
    """
    Load pre-computed per-inventory embeddings and layout features and
    concatenate them, without copying files into a merged directory — the
    on-disk files produced by `extract_inventory_features.ipynb` are used
    directly.

    Memory note: embeddings are read via numpy's memory-mapped mode just to
    discover their shape, then copied directly into one preallocated array —
    there is never more than one full copy of the combined embeddings in
    memory (unlike building a Python list of per-inventory arrays and calling
    `np.vstack`, which briefly holds two full copies at once).

    Args:
        inventory_dirs: list of per-inventory output directories, each containing
                        ``embeddings.npy``, ``image_ids.json``, and
                        ``layout_features.json`` as written by the extraction
                        notebook.

    Returns:
        embeddings:      (N, D) float32 array
        image_ids:       list of N path strings
        layout_features: dict mapping image_id -> feature dict
    """
    inventory_dirs = [Path(d) for d in inventory_dirs]

    # --- First pass: validate files and read embedding shapes without loading
    #     the underlying data (mmap_mode="r" only reads the .npy header).
    memmaps: list[tuple[Path, np.memmap, Path, Path, int]] = []
    embedding_dim: int | None = None
    total_n = 0

    for inv_dir in inventory_dirs:
        emb_file    = inv_dir / "embeddings.npy"
        ids_file    = inv_dir / "image_ids.json"
        layout_file = inv_dir / "layout_features.json"

        missing = [f for f in (emb_file, ids_file, layout_file) if not f.exists()]
        if missing:
            raise FileNotFoundError(
                f"Missing output files in {inv_dir}: "
                + ", ".join(f.name for f in missing)
                + "\nRun the extract_inventory_features notebook first."
            )

        mm = np.load(str(emb_file), mmap_mode="r")
        n, dim = mm.shape
        if embedding_dim is None:
            embedding_dim = dim
        elif dim != embedding_dim:
            raise ValueError(
                f"Embedding dimension mismatch: {inv_dir} has dim {dim}, "
                f"expected {embedding_dim} (from an earlier inventory)"
            )

        memmaps.append((inv_dir, mm, ids_file, layout_file, n))
        total_n += n

    logger.info(
        f"Combining {total_n} images from {len(inventory_dirs)} inventories "
        f"(embedding dim={embedding_dim})"
    )

    # --- Second pass: copy each inventory's embeddings directly into its slice
    #     of one preallocated array — never holding more than one full copy.
    combined_embeddings = np.empty((total_n, embedding_dim), dtype=np.float32)
    all_image_ids: list[str] = []
    all_layout: dict[str, dict] = {}

    # Log progress periodically rather than once per inventory — with
    # thousands of inventories, one INFO line each can flood Jupyter's iopub
    # channel and cause a "Server Connection Error" / unresponsive kernel.
    log_every = max(1, len(memmaps) // 20)

    offset = 0
    for ii, (inv_dir, mm, ids_file, layout_file, n) in enumerate(memmaps):
        combined_embeddings[offset:offset + n] = mm
        all_image_ids.extend(json.loads(ids_file.read_text()))
        all_layout.update(json.loads(layout_file.read_text()))
        offset += n
        if (ii + 1) % log_every == 0 or (ii + 1) == len(memmaps):
            logger.info(f"  loaded {ii + 1}/{len(inventory_dirs)} inventories ({offset} images so far)")

    return combined_embeddings, all_image_ids, all_layout


def cluster_inventories(
    inventory_dirs: list[str | Path],
    cfg: AnalysisConfig,
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
    Load a list of inventories and run clustering on the combined set.

    This is a convenience wrapper around `load_combined_inventories` +
    `run_clustering`. If you plan to try several HDBSCAN parameter settings
    on the same set of inventories, prefer the lower-level calls so UMAP only
    runs once:

    ::

        embeddings, image_ids, layout_features = load_combined_inventories(inv_dirs)
        umap_result = reduce_dimensions(
            embeddings, image_ids, cfg.to_dict(), layout_features,
            cache_file=cfg.umap_cache_file,
        )
        for min_cluster_size in (5, 10, 20):
            cfg.hdbscan_min_cluster_size = min_cluster_size
            clustering = cluster_embeddings(
                umap_result, cfg.to_dict(),
                results_file=f"outputs/clustering_mcs{min_cluster_size}.json",
            )

    Args:
        inventory_dirs: see `load_combined_inventories`.
        cfg:            AnalysisConfig supplying UMAP/HDBSCAN hyperparameters.
        results_file:   where to cache the clustering result JSON.  Pass ``None``
                        to skip caching.
        return_clusterer: forwarded to `run_clustering` — also return the fitted
                        HDBSCAN clusterer object.
        umap_cache_file: forwarded to `run_clustering` — where to cache the UMAP
                        projection, independently of the HDBSCAN results. Reusing
                        this path across calls with different HDBSCAN settings
                        skips recomputing UMAP.
        umap_fit_sample_size: forwarded to `run_clustering` — fit UMAP on a random
                        sample of this many images, then transform the rest.
        umap_transform_batch_size: forwarded to `run_clustering` — chunk size for
                        the transform step when `umap_fit_sample_size` is set.
        gen_min_span_tree: forwarded to `run_clustering` — leave False unless
                        you need `clusterer.minimum_spanning_tree_.plot()`;
                        for 100k+ images that plot can itself hang/crash a
                        Jupyter browser tab. Downsample before plotting if used.
        prediction_data: forwarded to `run_clustering`.
        hierarchy_file: forwarded to `run_clustering` — cache the condensed
                        tree + cluster persistence scores for later exploration
                        of near-merge candidates via `load_cluster_hierarchy`.
        save_single_linkage_tree: forwarded to `run_clustering`.

    Returns the clustering result dict (same format as ``run_clustering``).
    """
    embeddings, image_ids, layout_features = load_combined_inventories(inventory_dirs)

    return run_clustering(
        embeddings,
        image_ids,
        cfg.to_dict(),
        layout_features=layout_features,
        results_file=results_file,
        return_clusterer=return_clusterer,
        umap_cache_file=umap_cache_file,
        umap_fit_sample_size=umap_fit_sample_size,
        umap_transform_batch_size=umap_transform_batch_size,
        gen_min_span_tree=gen_min_span_tree,
        prediction_data=prediction_data,
        hierarchy_file=hierarchy_file,
        save_single_linkage_tree=save_single_linkage_tree,
    )
