"""
Cluster-stratified sampling for ground truth creation.

Samples a fixed number of images from each cluster so that rare document
types (small clusters) are represented alongside common ones.
Outliers (cluster -1) are also sampled separately — they often contain
the most unusual and informative documents.
"""

import json
import logging
import random
from pathlib import Path

import numpy as np

from archival_structures.stream_analysis.overview.clustering import get_cluster_members

logger = logging.getLogger(__name__)


def stratified_sample(
    clustering: dict,
    config: dict,
    vlm_tags: dict | None = None,
    random_seed: int = 42,
) -> dict[int, list[str]]:
    """
    Sample `samples_per_cluster` images from each cluster.

    If `vlm_tags` is provided and a cluster has fewer images than the
    requested sample size, all images are included.

    Args:
        clustering:    output of run_clustering()
        config:        pipeline config dict (from AnalysisConfig.to_dict())
        vlm_tags:      optional dict of VLM tags — already-tagged images
                       are prioritised in the sample (saves annotation effort)
        random_seed:   for reproducibility

    Returns:
        dict mapping cluster_id → list of sampled image_ids
    """
    random.seed(random_seed)
    n = config["groundtruth"]["samples_per_cluster"]
    cluster_members = get_cluster_members(clustering)
    sampled: dict[int, list[str]] = {}

    tagged_set = set(vlm_tags.keys()) if vlm_tags else set()

    for cluster_id, members in sorted(cluster_members.items()):
        if not members:
            continue

        if vlm_tags:
            # Prioritise already-tagged images to avoid redundant API calls
            tagged_in_cluster = [m for m in members if m in tagged_set]
            untagged_in_cluster = [m for m in members if m not in tagged_set]
            priority = tagged_in_cluster[:n]
            remaining_slots = n - len(priority)
            if remaining_slots > 0:
                priority += random.sample(
                    untagged_in_cluster,
                    min(remaining_slots, len(untagged_in_cluster)),
                )
            sampled[cluster_id] = priority
        else:
            sampled[cluster_id] = random.sample(members, min(n, len(members)))

        label = "outliers" if cluster_id == -1 else f"Cluster {cluster_id}"
        logger.info(f"  {label}: {len(sampled[cluster_id])} / {len(members)} sampled")

    total = sum(len(v) for v in sampled.values())
    logger.info(f"Total sampled: {total} images across {len(sampled)} clusters")
    return sampled


def save_sample(sampled: dict[int, list[str]], output_dir: str) -> str:
    """Save sampled image IDs to disk. Returns the output file path."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sampled_ids.json"

    # Convert int keys to strings for JSON serialisation
    serialisable = {str(k): v for k, v in sampled.items()}
    with open(out_path, "w") as f:
        json.dump(serialisable, f, indent=2)

    logger.info(f"Sample saved → {out_path}")
    return str(out_path)


def load_sample(output_dir: str) -> dict[int, list[str]]:
    """Load a previously saved sample from disk."""
    path = Path(output_dir) / "sampled_ids.json"
    with open(path) as f:
        data = json.load(f)
    return {int(k): v for k, v in data.items()}


def flat_sample(sampled: dict[int, list[str]]) -> list[str]:
    """Flatten the cluster→ids dict into a single list."""
    return [img_id for ids in sampled.values() for img_id in ids]
