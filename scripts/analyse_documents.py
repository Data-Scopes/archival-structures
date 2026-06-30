#!/usr/bin/env python
"""
CLI entry point for the document image analysis pipeline.

Overview pipeline (Part 1):
    python scripts/analyse_documents.py overview \\
        --image-dir /path/to/images \\
        --output-dir /path/to/outputs

    # Skip VLM tagging (no API key needed):
    python scripts/analyse_documents.py overview \\
        --image-dir /path/to/images --no-vlm

    # Load all settings from a YAML file:
    python scripts/analyse_documents.py overview \\
        --config archival_structures/stream_analysis/config.yaml

Ground truth pipeline (Part 2):
    # Export stratified sample for Label Studio:
    python scripts/analyse_documents.py groundtruth export \\
        --image-dir /path/to/images --output-dir /path/to/outputs

    # After annotating, run active learning:
    python scripts/analyse_documents.py groundtruth active-learning \\
        --image-dir /path/to/images --output-dir /path/to/outputs

    # Print Label Studio XML config:
    python scripts/analyse_documents.py groundtruth label-config
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure the project root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from archival_structures.stream_analysis import AnalysisConfig, run_overview, run_export, run_active_learning
from archival_structures.stream_analysis.groundtruth.label_studio_export import print_label_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add args shared by all subcommands."""
    parser.add_argument("--config", metavar="YAML",
                        help="Load settings from a YAML file (same schema as config.yaml). "
                             "CLI arguments override YAML values.")
    parser.add_argument("--image-dir", metavar="DIR",
                        help="Directory containing document images")
    parser.add_argument("--output-dir", metavar="DIR", default="outputs",
                        help="Root output directory (default: outputs)")
    parser.add_argument("--embedding-model", choices=["dinov2", "clip"],
                        help="Embedding model (default: dinov2)")
    parser.add_argument("--batch-size", type=int, metavar="N",
                        help="Images per batch during embedding extraction")
    parser.add_argument("--umap-n-neighbors", type=int, metavar="N",
                        help="UMAP n_neighbors parameter")
    parser.add_argument("--hdbscan-min-cluster-size", type=int, metavar="N",
                        help="HDBSCAN min_cluster_size parameter")
    parser.add_argument("--vlm-model", metavar="MODEL",
                        help="Anthropic model to use for VLM tagging")
    parser.add_argument("--vlm-sample-size", type=int, metavar="N",
                        help="Number of images to tag with the VLM")


def _build_config(args: argparse.Namespace) -> AnalysisConfig:
    """Merge YAML (if given) with CLI overrides."""
    if args.config:
        cfg = AnalysisConfig.from_yaml(args.config)
    else:
        cfg = AnalysisConfig()

    # Apply CLI overrides (only when explicitly provided)
    overrides = {
        "image_dir": getattr(args, "image_dir", None),
        "output_dir": getattr(args, "output_dir", None),
        "embedding_model": getattr(args, "embedding_model", None),
        "batch_size": getattr(args, "batch_size", None),
        "umap_n_neighbors": getattr(args, "umap_n_neighbors", None),
        "hdbscan_min_cluster_size": getattr(args, "hdbscan_min_cluster_size", None),
        "vlm_model": getattr(args, "vlm_model", None),
        "vlm_sample_size": getattr(args, "vlm_sample_size", None),
        "samples_per_cluster": getattr(args, "samples_per_cluster", None),
    }
    for attr, val in overrides.items():
        if val is not None:
            setattr(cfg, attr, val)

    return cfg


def cmd_overview(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    if not cfg.image_dir:
        print("Error: --image-dir is required (or set images.input_dir in your YAML).")
        sys.exit(1)
    run_overview(cfg, run_vlm=not args.no_vlm)


def cmd_groundtruth_export(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    if not cfg.image_dir:
        print("Error: --image-dir is required.")
        sys.exit(1)
    run_export(cfg)


def cmd_groundtruth_active_learning(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    if not cfg.image_dir:
        print("Error: --image-dir is required.")
        sys.exit(1)
    run_active_learning(cfg)


def cmd_groundtruth_label_config(args: argparse.Namespace) -> None:
    print_label_config()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archival document image analysis pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- overview subcommand -------------------------------------------------
    p_overview = subparsers.add_parser(
        "overview",
        help="Run the overview pipeline: embed, cluster, tag, visualise",
    )
    _add_common_args(p_overview)
    p_overview.add_argument(
        "--no-vlm", action="store_true",
        help="Skip VLM tagging (no ANTHROPIC_API_KEY needed)",
    )
    p_overview.set_defaults(func=cmd_overview)

    # --- groundtruth subcommand ----------------------------------------------
    p_gt = subparsers.add_parser(
        "groundtruth",
        help="Ground truth creation: sampling, Label Studio export, active learning",
    )
    gt_sub = p_gt.add_subparsers(dest="gt_command", required=True)

    p_export = gt_sub.add_parser("export", help="Stratified sample + Label Studio export")
    _add_common_args(p_export)
    p_export.add_argument("--samples-per-cluster", type=int, metavar="N",
                          help="Images sampled per cluster (default: 30)")
    p_export.set_defaults(func=cmd_groundtruth_export)

    p_al = gt_sub.add_parser("active-learning", help="Suggest next images to annotate")
    _add_common_args(p_al)
    p_al.set_defaults(func=cmd_groundtruth_active_learning)

    p_lc = gt_sub.add_parser("label-config", help="Print Label Studio XML labelling config")
    p_lc.set_defaults(func=cmd_groundtruth_label_config)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
