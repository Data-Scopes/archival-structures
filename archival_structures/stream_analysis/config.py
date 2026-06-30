"""
AnalysisConfig: single source of truth for all pipeline parameters.

Can be built from:
  - A YAML file (AnalysisConfig.from_yaml)
  - CLI argparse namespace (AnalysisConfig.from_args)
  - Direct construction in Python (e.g. in a notebook)

Call .to_dict() to get the legacy nested-dict format expected by the
individual pipeline modules.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class AnalysisConfig:
    # --- Input images ---
    image_dir: str = "data/images"
    image_extensions: list[str] = field(
        default_factory=lambda: ["jp2", "jpg", "jpeg", "png", "tif", "tiff"]
    )
    batch_size: int = 32

    # --- Output root ---
    output_dir: str = "outputs"

    # --- Embeddings ---
    embedding_model: str = "dinov2"          # "clip" or "dinov2"
    clip_model: str = "openai/clip-vit-base-patch32"
    dinov2_model: str = "facebook/dinov2-base"

    # --- Clustering ---
    combine_layout_features: bool = True
    layout_weight: float = 0.2
    umap_n_neighbors: int = 15
    umap_min_dist: float = 0.1
    umap_n_components: int = 2
    umap_metric: str = "cosine"
    umap_random_state: int = 42
    hdbscan_min_cluster_size: int = 10
    hdbscan_min_samples: int = 5
    hdbscan_metric: str = "euclidean"

    # --- VLM tagging ---
    vlm_provider: str = "anthropic"
    vlm_model: str = "claude-haiku-4-5"
    vlm_sample_size: int = 500
    vlm_batch_size: int = 1

    # --- Visualization ---
    cluster_grid_n: int = 9
    umap_point_size: int = 3
    umap_alpha: float = 0.6

    # --- Ground truth ---
    samples_per_cluster: int = 30

    # --- Active learning ---
    active_learning_batch_size: int = 20
    active_learning_model: str = "logistic_regression"
    uncertainty_strategy: str = "margin"     # "margin" or "entropy"

    # ------------------------------------------------------------------ #
    # Derived paths (computed from output_dir; override by setting them)  #
    # ------------------------------------------------------------------ #

    @property
    def embeddings_cache(self) -> str:
        return str(Path(self.output_dir) / "embeddings.npy")

    @property
    def image_ids_file(self) -> str:
        return str(Path(self.output_dir) / "image_ids.json")

    @property
    def layout_cache(self) -> str:
        return str(Path(self.output_dir) / "layout_features.json")

    @property
    def umap_cache_file(self) -> str:
        return str(Path(self.output_dir) / "umap_projection.json")

    @property
    def clustering_results_file(self) -> str:
        return str(Path(self.output_dir) / "clustering_results.json")

    @property
    def cluster_hierarchy_file(self) -> str:
        return str(Path(self.output_dir) / "cluster_hierarchy.npz")

    @property
    def vlm_results_file(self) -> str:
        return str(Path(self.output_dir) / "vlm_tags.json")

    @property
    def visualization_dir(self) -> str:
        return str(Path(self.output_dir) / "visualizations")

    @property
    def groundtruth_dir(self) -> str:
        return str(Path(self.output_dir) / "groundtruth")

    @property
    def label_file(self) -> str:
        return str(Path(self.output_dir) / "groundtruth" / "labels.json")

    @property
    def label_studio_export_file(self) -> str:
        return str(Path(self.output_dir) / "groundtruth" / "label_studio_import.json")

    @property
    def active_learning_suggestions_file(self) -> str:
        return str(Path(self.output_dir) / "groundtruth" / "active_learning_suggestions.json")

    # ------------------------------------------------------------------ #
    # Constructors                                                         #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_yaml(cls, path: str) -> "AnalysisConfig":
        """Load config from a YAML file (same schema as the original config.yaml)."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls._from_dict(data)

    @classmethod
    def from_args(cls, args) -> "AnalysisConfig":
        """Build from an argparse Namespace.  Missing attributes keep defaults."""
        cfg = cls()
        mapping = {
            "image_dir": "image_dir",
            "output_dir": "output_dir",
            "embedding_model": "embedding_model",
            "batch_size": "batch_size",
            "umap_n_neighbors": "umap_n_neighbors",
            "hdbscan_min_cluster_size": "hdbscan_min_cluster_size",
            "vlm_model": "vlm_model",
            "vlm_sample_size": "vlm_sample_size",
            "samples_per_cluster": "samples_per_cluster",
        }
        for arg_attr, cfg_attr in mapping.items():
            val = getattr(args, arg_attr, None)
            if val is not None:
                setattr(cfg, cfg_attr, val)
        return cfg

    @classmethod
    def _from_dict(cls, data: dict) -> "AnalysisConfig":
        """Convert the legacy nested YAML dict to an AnalysisConfig."""
        img = data.get("images", {})
        emb = data.get("embeddings", {})
        clu = data.get("clustering", {})
        umap = clu.get("umap", {})
        hdb = clu.get("hdbscan", {})
        vlm = data.get("vlm_tagging", {})
        viz = data.get("visualization", {})
        gt = data.get("groundtruth", {})
        al = data.get("active_learning", {})

        cfg = cls(
            image_dir=img.get("input_dir", cls.image_dir),
            image_extensions=img.get("extensions", cls.image_extensions),
            batch_size=img.get("batch_size", cls.batch_size),
            embedding_model=emb.get("model", cls.embedding_model),
            clip_model=emb.get("clip_model", cls.clip_model),
            dinov2_model=emb.get("dinov2_model", cls.dinov2_model),
            combine_layout_features=clu.get("combine_layout_features", cls.combine_layout_features),
            layout_weight=clu.get("layout_weight", cls.layout_weight),
            umap_n_neighbors=umap.get("n_neighbors", cls.umap_n_neighbors),
            umap_min_dist=umap.get("min_dist", cls.umap_min_dist),
            umap_n_components=umap.get("n_components", cls.umap_n_components),
            umap_metric=umap.get("metric", cls.umap_metric),
            umap_random_state=umap.get("random_state", cls.umap_random_state),
            hdbscan_min_cluster_size=hdb.get("min_cluster_size", cls.hdbscan_min_cluster_size),
            hdbscan_min_samples=hdb.get("min_samples", cls.hdbscan_min_samples),
            hdbscan_metric=hdb.get("metric", cls.hdbscan_metric),
            vlm_provider=vlm.get("provider", cls.vlm_provider),
            vlm_model=vlm.get("model", cls.vlm_model),
            vlm_sample_size=vlm.get("sample_size", cls.vlm_sample_size),
            vlm_batch_size=vlm.get("batch_size", cls.vlm_batch_size),
            cluster_grid_n=viz.get("cluster_grid_n", cls.cluster_grid_n),
            umap_point_size=viz.get("umap_point_size", cls.umap_point_size),
            umap_alpha=viz.get("umap_alpha", cls.umap_alpha),
            samples_per_cluster=gt.get("samples_per_cluster", cls.samples_per_cluster),
            active_learning_batch_size=al.get("batch_size", cls.active_learning_batch_size),
            active_learning_model=al.get("model", cls.active_learning_model),
            uncertainty_strategy=al.get("uncertainty_strategy", cls.uncertainty_strategy),
        )

        # If the YAML specifies an output location we can infer output_dir from it
        out_dir = viz.get("output_dir") or emb.get("cache_file")
        if out_dir:
            # Try to derive output_dir as the parent of any known output path
            candidate = Path(emb.get("cache_file", "outputs/embeddings.npy")).parent
            cfg.output_dir = str(candidate)

        return cfg

    # ------------------------------------------------------------------ #
    # Legacy dict interface (used by pipeline modules)                    #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        """Return the nested dict format consumed by the pipeline modules."""
        return {
            "images": {
                "input_dir": self.image_dir,
                "extensions": self.image_extensions,
                "batch_size": self.batch_size,
            },
            "embeddings": {
                "model": self.embedding_model,
                "clip_model": self.clip_model,
                "dinov2_model": self.dinov2_model,
                "cache_file": self.embeddings_cache,
                "image_ids_file": self.image_ids_file,
            },
            "layout": {
                "cache_file": self.layout_cache,
            },
            "clustering": {
                "combine_layout_features": self.combine_layout_features,
                "layout_weight": self.layout_weight,
                "umap": {
                    "n_neighbors": self.umap_n_neighbors,
                    "min_dist": self.umap_min_dist,
                    "n_components": self.umap_n_components,
                    "metric": self.umap_metric,
                    "random_state": self.umap_random_state,
                },
                "hdbscan": {
                    "min_cluster_size": self.hdbscan_min_cluster_size,
                    "min_samples": self.hdbscan_min_samples,
                    "metric": self.hdbscan_metric,
                },
                "umap_cache_file": self.umap_cache_file,
                "results_file": self.clustering_results_file,
                "hierarchy_file": self.cluster_hierarchy_file,
            },
            "vlm_tagging": {
                "provider": self.vlm_provider,
                "model": self.vlm_model,
                "sample_size": self.vlm_sample_size,
                "results_file": self.vlm_results_file,
                "batch_size": self.vlm_batch_size,
            },
            "visualization": {
                "output_dir": self.visualization_dir,
                "cluster_grid_n": self.cluster_grid_n,
                "umap_point_size": self.umap_point_size,
                "umap_alpha": self.umap_alpha,
            },
            "groundtruth": {
                "samples_per_cluster": self.samples_per_cluster,
                "output_dir": self.groundtruth_dir,
                "label_file": self.label_file,
                "label_studio_export": self.label_studio_export_file,
            },
            "active_learning": {
                "batch_size": self.active_learning_batch_size,
                "model": self.active_learning_model,
                "uncertainty_strategy": self.uncertainty_strategy,
                "suggestions_file": self.active_learning_suggestions_file,
            },
        }
