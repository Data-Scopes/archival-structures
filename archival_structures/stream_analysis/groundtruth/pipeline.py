"""
Ground truth creation pipeline (Part 2).

Assumes the overview pipeline has already been run (embeddings, layout
features, and clustering results are cached on disk).

Steps:
  1. Load overview pipeline outputs from cache
  2. Cluster-stratified sampling
  3. Export to Label Studio (with optional VLM pre-annotations)
  4. [After annotating] Active learning: suggest next batch to label
"""

import json
import logging
from pathlib import Path

from archival_structures.stream_analysis.config import AnalysisConfig
from archival_structures.stream_analysis.overview.embeddings import extract_embeddings
from archival_structures.stream_analysis.overview.layout_analysis import extract_all_layout_features
from archival_structures.stream_analysis.overview.clustering import run_clustering, get_cluster_members
from archival_structures.stream_analysis.groundtruth.stratified_sampling import (
    stratified_sample,
    save_sample,
)
from archival_structures.stream_analysis.groundtruth.label_studio_export import (
    export_label_studio,
    print_label_config,
)
from archival_structures.stream_analysis.groundtruth.active_learning import ActiveLearner

logger = logging.getLogger(__name__)


def _load_overview_outputs(config: dict):
    """Re-load (from cache) all overview pipeline artefacts needed for Part 2."""
    embeddings, image_ids = extract_embeddings(config)
    layout_features = extract_all_layout_features(config)
    clustering = run_clustering(embeddings, image_ids, config, layout_features)
    return embeddings, image_ids, layout_features, clustering


def _load_vlm_tags(config: dict) -> dict:
    vlm_file = config["vlm_tagging"]["results_file"]
    if Path(vlm_file).exists():
        with open(vlm_file) as f:
            tags = json.load(f)
        logger.info(f"Loaded {len(tags)} VLM tags from {vlm_file}")
        return tags
    logger.info("No VLM tags found — proceeding without pre-annotations")
    return {}


def run_export(cfg: AnalysisConfig) -> str:
    """
    Stratified sample + Label Studio export.

    Returns the path to the exported Label Studio JSON file.
    """
    config = cfg.to_dict()
    embeddings, image_ids, layout_features, clustering = _load_overview_outputs(config)
    vlm_tags = _load_vlm_tags(config)

    logger.info("=== Stratified sampling ===")
    sampled = stratified_sample(clustering, config, vlm_tags)
    save_sample(sampled, config["groundtruth"]["output_dir"])

    logger.info("=== Exporting to Label Studio ===")
    out_path = export_label_studio(sampled, config, vlm_tags)

    total = sum(len(v) for v in sampled.values())
    pre_tagged = sum(
        1 for ids in sampled.values() for img_id in ids if img_id in vlm_tags
    )
    print(f"\nDone. {total} images exported to {out_path}")
    print(f"  {pre_tagged} images have VLM pre-annotations.")
    print("\nNext steps:")
    print("  1. Run print_label_studio_config() to get the XML for Label Studio.")
    print("  2. Import the JSON file into Label Studio and start annotating.")
    print(f"  3. Export labels and save to: {config['groundtruth']['label_file']}")
    print("  4. Run run_active_learning() to get suggestions for the next batch.")
    return out_path


def run_active_learning(cfg: AnalysisConfig) -> list[dict]:
    """
    Train on current labels, suggest next batch, report accuracy.

    Returns the list of suggested image dicts.
    """
    config = cfg.to_dict()
    label_file = config["groundtruth"]["label_file"]
    if not Path(label_file).exists():
        raise FileNotFoundError(
            f"Label file not found: {label_file}\n"
            "Annotate some images first, then save their labels to that path."
        )

    embeddings, image_ids, _, _ = _load_overview_outputs(config)

    al = ActiveLearner(embeddings, image_ids, config)
    al.load_labels(label_file)

    if al.n_labeled < 10:
        raise ValueError(
            f"Only {al.n_labeled} labels found. "
            "Annotate at least 10 images (across 2+ classes) before running active learning."
        )

    acc = al.cross_val_score()
    logger.info(f"Cross-validated accuracy on {al.n_labeled} labels: {acc:.3f}")

    suggestions = al.suggest_next_batch()
    al.save_suggestions(suggestions)

    suggestions_file = config["active_learning"]["suggestions_file"]
    print(f"\nActive learning results:")
    print(f"  Labelled images:   {al.n_labeled}")
    print(f"  Classes:           {al.unique_classes}")
    print(f"  CV accuracy:       {acc:.1%}")
    print(f"  Suggestions saved: {suggestions_file}")
    print(f"\nTop 5 suggested images to annotate next:")
    for s in suggestions[:5]:
        print(
            f"  [{s['uncertainty']:.3f}] {Path(s['image_id']).name}"
            f"  (model says: {s['top_prediction']} p={s['top_prob']:.2f},"
            f" vs {s['second_prediction']} p={s['second_prob']:.2f})"
        )
    return suggestions
