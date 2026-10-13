"""Sweep the corruption operators across rates, seeds, and a directory of real ground-truth
inventories, writing one `CorruptedStream` YAML per (inventory, case, rate, seed) combination.

See `docs/challenges/simulation_harness.md` for the overall pipeline this is the last stage of.
`scripts/simulate_corruption.py` is the CLI wrapper around `simulate_collection` below.
"""

from __future__ import annotations

import csv
import logging
import random
from dataclasses import dataclass
from pathlib import Path

from archival_structures.annotation.ground_truth import read_ground_truth
from archival_structures.stream_analysis.corruption.canonical_stream import (
    StreamUnit,
    extract_canonical_stream,
)
from archival_structures.stream_analysis.corruption.operators import (
    ProvenanceEntry,
    document_level_swap,
    foreign_document_insertion,
    initial_provenance,
    intra_document_swap,
    swap_and_insertion,
)
from archival_structures.stream_analysis.corruption.schema import (
    CorruptedStream,
    write_corrupted_stream,
)

logger = logging.getLogger(__name__)

CASE_NAMES: tuple[str, ...] = (
    'intra_document_swap',
    'foreign_document_insertion',
    'swap_and_insertion',
    'document_level_swap',
)


def _apply_case(
    case: str,
    stream: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    rate: float,
    rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
    """Dispatch to the matching operator in `operators.py`. `swap_and_insertion` takes two rates
    (`swap_rate`, `insertion_rate`) rather than one -- for a rate *sweep*, `rate` is applied to
    both, which is a simplification worth revisiting if the two need to vary independently."""
    if case == 'intra_document_swap':
        return intra_document_swap(stream, provenance, rate, rng)
    if case == 'foreign_document_insertion':
        return foreign_document_insertion(stream, provenance, rate, rng)
    if case == 'swap_and_insertion':
        return swap_and_insertion(stream, provenance, swap_rate=rate, insertion_rate=rate, rng=rng)
    if case == 'document_level_swap':
        return document_level_swap(stream, provenance, rate, rng)
    raise ValueError(f'unknown case {case!r}, expected one of {CASE_NAMES}')


@dataclass(frozen=True)
class SimulationResult:
    """One (inventory, case, rate, seed) run's outcome -- one row of the sweep's manifest."""

    inventory_id: str
    case: str
    rate: float
    seed: int
    n_units: int
    n_documents: int
    n_touched: int
    output_path: Path


def simulate_inventory(
    gt_path: Path,
    output_dir: Path,
    *,
    layer: str = 'section',
    cases: tuple[str, ...] = CASE_NAMES,
    rates: tuple[float, ...] = (0.1, 0.3, 0.5),
    seeds: tuple[int, ...] = (0,),
) -> list[SimulationResult]:
    """Run every `(case, rate, seed)` combination against one ground-truth file, writing one
    `CorruptedStream` YAML per combination under `output_dir/<inventory_id>/`.

    Returns an empty list (with a log warning, not an error) for an inventory whose `layer` isn't
    populated -- expected for a real, partially-annotated corpus (5 of 633 `NL-HaNA_1.01.02`
    files have no layers at all yet, see `docs/challenges/simulation_harness.md`), not a failure
    condition a caller sweeping a whole collection should abort on.
    """
    gt = read_ground_truth(gt_path)
    if layer not in gt.layers or not gt.layers[layer]:
        logger.warning('%s: layer %r not populated, skipping', gt.inventory_id, layer)
        return []

    canonical = extract_canonical_stream(gt, layer=layer)
    n_documents = len({u.document_id for u in canonical})

    results: list[SimulationResult] = []
    for case in cases:
        for rate in rates:
            for seed in seeds:
                rng = random.Random(seed)
                provenance = initial_provenance(canonical)
                observed, out_provenance = _apply_case(case, canonical, provenance, rate, rng)
                n_touched = sum(1 for p in out_provenance if p.events)

                cs = CorruptedStream(
                    inventory_id=gt.inventory_id,
                    layer=layer,
                    case=case,
                    params={'rate': rate},
                    seed=seed,
                    observed=observed,
                    provenance=out_provenance,
                )
                out_path = (
                    Path(output_dir) / gt.inventory_id / f'{case}-rate{rate}-seed{seed}.yaml'
                )
                write_corrupted_stream(cs, out_path)

                results.append(SimulationResult(
                    inventory_id=gt.inventory_id,
                    case=case,
                    rate=rate,
                    seed=seed,
                    n_units=len(canonical),
                    n_documents=n_documents,
                    n_touched=n_touched,
                    output_path=out_path,
                ))
    return results


def simulate_collection(
    ground_truth_dir: Path,
    output_dir: Path,
    **kwargs,
) -> list[SimulationResult]:
    """`simulate_inventory` for every `*.yaml` file directly under `ground_truth_dir`. A single
    inventory's failure (e.g. a malformed ground-truth file) is logged and skipped rather than
    aborting the whole collection sweep."""
    results: list[SimulationResult] = []
    for gt_path in sorted(Path(ground_truth_dir).glob('*.yaml')):
        try:
            results.extend(simulate_inventory(gt_path, output_dir, **kwargs))
        except Exception:
            logger.exception('failed to simulate %s', gt_path)
    return results


def write_manifest(results: list[SimulationResult], path: Path) -> None:
    """Write `results` as a CSV manifest -- one row per `(inventory, case, rate, seed)` run,
    pointing at the `CorruptedStream` YAML it produced."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow([
            'inventory_id', 'case', 'rate', 'seed',
            'n_units', 'n_documents', 'n_touched', 'output_path',
        ])
        for r in results:
            writer.writerow([
                r.inventory_id, r.case, r.rate, r.seed,
                r.n_units, r.n_documents, r.n_touched, str(r.output_path),
            ])
