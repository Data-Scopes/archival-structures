#!/usr/bin/env python
"""
CLI entry point for the page-order-problem corruption simulation harness (see
docs/challenges/simulation_harness.md).

Sweeps the four corruption operators across rates and seeds against every ground-truth YAML file
in a directory, writing one CorruptedStream YAML per (inventory, case, rate, seed) combination,
plus a manifest.csv summarising the run.

    python scripts/simulate_corruption.py \\
        data/ground_truth/NL-HaNA_1.01.02 \\
        outputs/corrupted/NL-HaNA_1.01.02

    # Restrict to specific cases, rates, and seeds:
    python scripts/simulate_corruption.py \\
        data/ground_truth/NL-HaNA_1.01.02 outputs/corrupted/NL-HaNA_1.01.02 \\
        --cases foreign_document_insertion document_level_swap \\
        --rates 0.1 0.3 0.5 0.8 \\
        --seeds 0 1 2
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure the project root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from archival_structures.stream_analysis.corruption import (
    CASE_NAMES,
    simulate_collection,
    write_manifest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    parser = argparse.ArgumentParser(
        description="Sweep the page-order-problem corruption operators across a directory of "
                    "real InventoryGroundTruth YAML files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "ground_truth_dir", type=Path,
        help="Directory of InventoryGroundTruth YAML files "
             "(e.g. data/ground_truth/NL-HaNA_1.01.02)",
    )
    parser.add_argument(
        "output_dir", type=Path,
        help="Where to write CorruptedStream YAML output and manifest.csv",
    )
    parser.add_argument(
        "--layer", default="section",
        help="Ground-truth layer to treat as the canonical document stream (default: section)",
    )
    parser.add_argument(
        "--cases", nargs="+", choices=CASE_NAMES, default=list(CASE_NAMES),
        help="Which corruption operators to run (default: all four)",
    )
    parser.add_argument(
        "--rates", nargs="+", type=float, default=[0.1, 0.3, 0.5],
        help="Corruption rates to sweep (default: 0.1 0.3 0.5)",
    )
    parser.add_argument(
        "--seeds", nargs="+", type=int, default=[0],
        help="RNG seeds to sweep, for repeated draws at the same rate (default: 0)",
    )
    args = parser.parse_args()

    results = simulate_collection(
        args.ground_truth_dir,
        args.output_dir,
        layer=args.layer,
        cases=tuple(args.cases),
        rates=tuple(args.rates),
        seeds=tuple(args.seeds),
    )

    manifest_path = args.output_dir / "manifest.csv"
    write_manifest(results, manifest_path)
    print(f"wrote {len(results)} corrupted streams; manifest at {manifest_path}")


if __name__ == "__main__":
    main()
