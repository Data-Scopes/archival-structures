"""
Visualisation utilities for the overview pipeline.

Produces:
  1. UMAP scatter plot coloured by cluster
  2. Per-cluster image grids (representative samples)
  3. Layout feature distributions per cluster
  4. VLM tag summary bar charts
"""

import json
import logging
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.gridspec import GridSpec
from PIL import Image

logger = logging.getLogger(__name__)


def _ensure_output_dir(config: dict) -> Path:
    out = Path(config["visualization"]["output_dir"])
    out.mkdir(parents=True, exist_ok=True)
    return out


# ---------------------------------------------------------------------------
# 0. Minimum spanning tree (safe for large N)
# ---------------------------------------------------------------------------

def plot_minimum_spanning_tree(clusterer, config: dict) -> Path:
    """
    Save HDBSCAN's minimum spanning tree plot to a file instead of rendering
    it inline.

    For 100k+ images the full MST has 100k+ edges. Calling
    `clusterer.minimum_spanning_tree_.plot()` directly in a notebook cell
    renders that whole figure inline — pushing a very large image over
    Jupyter's iopub channel, which can stall the kernel and crash the browser
    tab. This renders to a non-interactive Agg backend and writes a PNG
    instead, so nothing large is ever sent back to the front end.

    There's no supported way to downsample which edges get drawn (the
    library's `.plot()` always draws the full tree) — for very large
    datasets, consider whether you need this plot at all, or only enable
    `gen_min_span_tree=True` while developing on a smaller subsample.

    Args:
        clusterer: fitted HDBSCAN object created with gen_min_span_tree=True
        config:    pipeline config dict (used for the output directory)

    Returns the path to the saved PNG.
    """
    if clusterer.minimum_spanning_tree_ is None:
        raise ValueError(
            "clusterer.minimum_spanning_tree_ is None — re-run clustering with "
            "gen_min_span_tree=True to enable this plot."
        )

    import matplotlib
    out_dir = _ensure_output_dir(config)
    out_path = out_dir / "minimum_spanning_tree.png"

    with matplotlib.rc_context({"backend": "Agg"}):
        fig = plt.figure(figsize=(10, 10))
        clusterer.minimum_spanning_tree_.plot()
        fig.savefig(out_path, dpi=100, bbox_inches="tight")
        plt.close(fig)

    logger.info(f"Minimum spanning tree plot saved → {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# 1. UMAP scatter plot
# ---------------------------------------------------------------------------

def plot_umap(clustering: dict, config: dict) -> None:
    umap_2d = np.array(clustering["umap_2d"])
    labels = np.array(clustering["labels"])
    n_clusters = clustering["n_clusters"]
    out_dir = _ensure_output_dir(config)

    fig, ax = plt.subplots(figsize=(12, 9))
    palette = sns.color_palette("tab20", max(n_clusters, 1))

    # Plot outliers first in light grey
    outlier_mask = labels == -1
    if outlier_mask.any():
        ax.scatter(
            umap_2d[outlier_mask, 0], umap_2d[outlier_mask, 1],
            c="#cccccc", s=config["visualization"]["umap_point_size"],
            alpha=0.4, label="outliers", linewidths=0,
        )

    for cluster_id in range(n_clusters):
        mask = labels == cluster_id
        ax.scatter(
            umap_2d[mask, 0], umap_2d[mask, 1],
            c=[palette[cluster_id % 20]],
            s=config["visualization"]["umap_point_size"],
            alpha=config["visualization"]["umap_alpha"],
            label=f"C{cluster_id} (n={mask.sum()})",
            linewidths=0,
        )

    ax.legend(
        loc="upper right", fontsize=7,
        ncol=max(1, n_clusters // 15),
        markerscale=3,
    )
    ax.set_title(
        f"UMAP projection — {n_clusters} clusters, "
        f"{int(outlier_mask.sum())} outliers",
        fontsize=12,
    )
    ax.set_xlabel("UMAP-1")
    ax.set_ylabel("UMAP-2")
    ax.set_aspect("equal", "datalim")
    plt.tight_layout()

    out_path = out_dir / "umap_clusters.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"UMAP plot saved → {out_path}")


# ---------------------------------------------------------------------------
# 2. Per-cluster image grids
# ---------------------------------------------------------------------------

def _load_thumb(path: str, size: int = 120) -> Image.Image | None:
    try:
        img = Image.open(path).convert("RGB")
        img.thumbnail((size, size))
        # Pad to square
        sq = Image.new("RGB", (size, size), (240, 240, 240))
        x_off = (size - img.width) // 2
        y_off = (size - img.height) // 2
        sq.paste(img, (x_off, y_off))
        return sq
    except Exception:
        return None


def plot_cluster_grids(clustering: dict, config: dict) -> None:
    """Save one image grid per cluster showing representative samples."""
    labels = np.array(clustering["labels"])
    image_ids = clustering["image_ids"]
    n_clusters = clustering["n_clusters"]
    n_samples = config["visualization"]["cluster_grid_n"]
    out_dir = _ensure_output_dir(config)

    grid_cols = math.ceil(math.sqrt(n_samples))
    grid_rows = math.ceil(n_samples / grid_cols)
    thumb_px = 120

    for cluster_id in range(n_clusters):
        members = [img_id for img_id, lbl in zip(image_ids, labels) if lbl == cluster_id]
        sample = members[:n_samples] if len(members) >= n_samples else members

        fig, axes = plt.subplots(grid_rows, grid_cols, figsize=(grid_cols * 1.5, grid_rows * 1.5))
        axes = np.array(axes).flatten()

        for i, ax in enumerate(axes):
            ax.axis("off")
            if i < len(sample):
                thumb = _load_thumb(sample[i], thumb_px)
                if thumb:
                    ax.imshow(np.array(thumb))

        fig.suptitle(
            f"Cluster {cluster_id}  (n={len(members)})",
            fontsize=10, y=1.01,
        )
        plt.tight_layout()
        out_path = out_dir / f"cluster_{cluster_id:03d}_grid.png"
        fig.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close(fig)

    logger.info(f"Cluster grids saved to {out_dir}")


# ---------------------------------------------------------------------------
# 3. Layout feature distributions per cluster
# ---------------------------------------------------------------------------

def plot_layout_distributions(
    clustering: dict,
    layout_features: dict[str, dict],
    config: dict,
) -> None:
    """Box-plots of key layout metrics broken down by cluster."""
    labels = np.array(clustering["labels"])
    image_ids = clustering["image_ids"]
    out_dir = _ensure_output_dir(config)

    features_of_interest = [
        "ink_density", "text_density", "estimated_line_count",
        "line_coverage", "num_column_blocks", "local_variance_norm",
        "aspect_ratio", "mean_brightness",
    ]

    cluster_data: dict[str, dict[int, list]] = {f: {} for f in features_of_interest}

    for img_id, label in zip(image_ids, labels.tolist()):
        feats = layout_features.get(img_id, {})
        for f in features_of_interest:
            cluster_data[f].setdefault(label, []).append(feats.get(f, 0.0))

    n_features = len(features_of_interest)
    fig, axes = plt.subplots(2, math.ceil(n_features / 2), figsize=(16, 8))
    axes = axes.flatten()

    for ax, feature in zip(axes, features_of_interest):
        sorted_labels = sorted(cluster_data[feature].keys())
        data = [cluster_data[feature][lbl] for lbl in sorted_labels]
        tick_labels = [
            "outlier" if lbl == -1 else f"C{lbl}"
            for lbl in sorted_labels
        ]
        ax.boxplot(data, labels=tick_labels, showfliers=False)
        ax.set_title(feature, fontsize=9)
        ax.tick_params(axis="x", labelsize=7)

    for ax in axes[n_features:]:
        ax.axis("off")

    plt.suptitle("Layout feature distributions by cluster", fontsize=12)
    plt.tight_layout()
    out_path = out_dir / "layout_distributions.png"
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Layout distribution plot saved → {out_path}")


# ---------------------------------------------------------------------------
# 4. VLM tag summary
# ---------------------------------------------------------------------------

def plot_vlm_summary(vlm_tags: dict, config: dict) -> None:
    """Bar charts for each categorical VLM tag field."""
    from archival_structures.stream_analysis.overview.vlm_tagging import summarise_vlm_tags

    summary = summarise_vlm_tags(vlm_tags)
    out_dir = _ensure_output_dir(config)

    categorical = [
        "document_type", "writing_mode", "colour_profile",
        "condition", "approximate_text_density", "orientation",
    ]
    boolean_fields = [
        "has_stamp", "has_signature", "has_table", "has_ruled_lines",
        "has_logo_or_letterhead", "has_photograph_or_illustration",
    ]

    n_plots = len(categorical) + 1  # +1 for combined boolean
    fig, axes = plt.subplots(
        math.ceil(n_plots / 3), 3,
        figsize=(18, 4 * math.ceil(n_plots / 3)),
    )
    axes = axes.flatten()

    for ax, field in zip(axes, categorical):
        counts = summary.get(field, {})
        if not counts:
            ax.axis("off")
            continue
        labels, values = zip(*sorted(counts.items(), key=lambda x: -x[1]))
        ax.barh(labels, values, color="steelblue")
        ax.set_title(field, fontsize=10)
        ax.tick_params(labelsize=8)

    # Boolean summary as grouped bar
    bool_ax = axes[len(categorical)]
    bool_labels = [f.replace("has_", "") for f in boolean_fields]
    true_vals = [summary.get(f, {}).get("true", 0) for f in boolean_fields]
    x = np.arange(len(bool_labels))
    bool_ax.bar(x, true_vals, color="tomato")
    bool_ax.set_xticks(x)
    bool_ax.set_xticklabels(bool_labels, rotation=30, ha="right", fontsize=8)
    bool_ax.set_title("Feature presence (% of tagged images)", fontsize=10)
    bool_ax.set_ylabel("count")

    for ax in axes[n_plots:]:
        ax.axis("off")

    plt.suptitle(
        f"VLM tag summary  (n={len(vlm_tags)} images)",
        fontsize=13, y=1.01,
    )
    plt.tight_layout()
    out_path = out_dir / "vlm_tag_summary.png"
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"VLM summary plot saved → {out_path}")
