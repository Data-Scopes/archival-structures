"""`CorruptedStream`: a corrupted observed page stream plus its ground-truth alignment back to
the canonical stream it was derived from.

Kept separate from `archival_structures.annotation.ground_truth.InventoryGroundTruth` rather than
extending it in place -- see "Output format" in `docs/challenges/simulation_harness.md` -- since
one real ground-truth inventory is expected to produce many corrupted variants (one per
corruption-case/rate/seed combination), not a single corrupted copy per inventory.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from archival_structures.stream_analysis.corruption.canonical_stream import StreamUnit
from archival_structures.stream_analysis.corruption.operators import ProvenanceEntry


@dataclass
class CorruptedStream:
    """One corrupted observed stream derived from one inventory's canonical stream.

    Parameters
    ----------
    inventory_id:
        The `InventoryGroundTruth.inventory_id` this was derived from.
    layer:
        Which ground-truth layer the canonical stream was extracted from, e.g. `"section"`.
    case:
        Which corruption operator was applied, e.g. `"intra_document_swap"`,
        `"foreign_document_insertion"`, `"swap_and_insertion"`, `"document_level_swap"`.
    params:
        The rate(s) (and any other operator arguments) used, e.g. `{"rate": 0.1}`.
    seed:
        The RNG seed used to produce `observed`, for exact reproducibility.
    observed:
        The corrupted stream itself, in observed order.
    provenance:
        Parallel list to `observed`: each entry's `canonical_index` is that unit's position in
        the original canonical stream, and `events` names which operator(s) touched it.
    """

    inventory_id: str
    layer: str
    case: str
    params: dict[str, Any]
    seed: int
    observed: list[StreamUnit]
    provenance: list[ProvenanceEntry]

    def as_dict(self) -> dict[str, Any]:
        return {
            'inventory_id': self.inventory_id,
            'layer': self.layer,
            'case': self.case,
            'params': self.params,
            'seed': self.seed,
            'observed': [_unit_as_dict(u) for u in self.observed],
            'provenance': [
                {'canonical_index': p.canonical_index, 'events': list(p.events)}
                for p in self.provenance
            ],
        }


def _unit_as_dict(unit: StreamUnit) -> dict[str, Any]:
    d: dict[str, Any] = {'scan_id': unit.scan_id, 'document_id': unit.document_id}
    if unit.page_side is not None:
        d['page_side'] = unit.page_side
    if unit.bbox is not None:
        d['bbox'] = unit.bbox
    return d


def write_corrupted_stream(cs: CorruptedStream, path: Path) -> None:
    """Serialise *cs* to a YAML file at *path*, creating parent dirs as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        yaml.dump(cs.as_dict(), fh, allow_unicode=True, sort_keys=False, default_flow_style=False)


def read_corrupted_stream(path: Path) -> CorruptedStream:
    """Deserialise a corrupted-stream YAML file written by `write_corrupted_stream`."""
    path = Path(path)
    with open(path, 'r', encoding='utf-8') as fh:
        data = yaml.safe_load(fh)

    observed = [
        StreamUnit(
            scan_id=u['scan_id'],
            document_id=u['document_id'],
            page_side=u.get('page_side'),
            bbox=u.get('bbox'),
        )
        for u in data['observed']
    ]
    provenance = [
        ProvenanceEntry(canonical_index=p['canonical_index'], events=tuple(p.get('events', [])))
        for p in data['provenance']
    ]
    return CorruptedStream(
        inventory_id=data['inventory_id'],
        layer=data['layer'],
        case=data['case'],
        params=data['params'],
        seed=data['seed'],
        observed=observed,
        provenance=provenance,
    )
