"""Corruption operators implementing the four page-order-problem cases.

Each operator matches one case in `docs/challenges/page_order_problems.md` 1:1: intra-document
page swap (Case 1), foreign-document insertion (Case 2), the two combined (Case 3), and
document-level swap (Case 4). All four share the signature
`(stream, provenance, rate, rng) -> (observed_stream, provenance)`, so they compose --
`swap_and_insertion` is literally `intra_document_swap` followed by
`foreign_document_insertion`, each threading the other's provenance through.

Every operator is a pure function: it takes a `random.Random` explicitly rather than touching
global random state, so a run is exactly reproducible from its seed alone.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from archival_structures.stream_analysis.corruption.canonical_stream import (
    StreamUnit,
    document_blocks,
)


@dataclass(frozen=True)
class ProvenanceEntry:
    """Where one position in an observed stream came from.

    `canonical_index` is the unit's position in the *original* canonical stream passed into the
    first operator of a (possibly composed) corruption chain -- not the previous operator's
    output. `events` names every operator that touched this position, in application order. A
    unit's true `document_id` doesn't need to be tracked here: it lives on the `StreamUnit`
    itself and never changes as operators reorder the stream.
    """

    canonical_index: int
    events: tuple[str, ...] = ()


def initial_provenance(stream: list[StreamUnit]) -> list[ProvenanceEntry]:
    """The identity provenance for `stream` -- pass this as `provenance` for the first operator
    applied to a freshly-extracted canonical stream."""
    return [ProvenanceEntry(canonical_index=i) for i in range(len(stream))]


def _tag(entry: ProvenanceEntry, event: str) -> ProvenanceEntry:
    return ProvenanceEntry(canonical_index=entry.canonical_index, events=entry.events + (event,))


def intra_document_swap(
    stream: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    rate: float,
    rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
    """Case 1: for each document with >= 2 pages, with probability `rate`, transpose two of its
    pages -- a permutation restricted to that document's own positions, identity elsewhere (see
    `docs/challenges/page_order_problems.md#case-1-intra-document-page-swap`). Documents with
    fewer than 2 pages are left untouched (no position to swap).
    """
    stream = list(stream)
    provenance = list(provenance)
    for _doc_id, start, end in document_blocks(stream):
        if end - start < 2 or rng.random() >= rate:
            continue
        i, j = rng.sample(range(start, end), 2)
        stream[i], stream[j] = stream[j], stream[i]
        # provenance travels with its unit, so position k keeps describing stream[k]
        provenance[i], provenance[j] = (
            _tag(provenance[j], 'intra_document_swap'),
            _tag(provenance[i], 'intra_document_swap'),
        )
    return stream, provenance


def foreign_document_insertion(
    stream: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    rate: float,
    rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
    """Case 2: with probability `rate`, each document with >= 2 pages ("target") has one other
    whole document's pages ("donor") spliced into one of its own interior positions (see
    `docs/challenges/page_order_problems.md#case-2-foreign-document-insertion-interleaving`).

    This implements one practically-motivated member of the shuffle-product family the docs
    describe -- a single contiguous foreign block inserted at one point, matching a physically
    misfiled clump of pages -- rather than sampling the full combinatorial interleaving space
    (arbitrary per-page shuffling of two documents is a far less physically plausible corruption
    than one contiguous foreign insertion). No-op if fewer than two documents are present.

    A document can never be both a target *and* a donor in the same call: donating its pages
    away removes its own block from the stream entirely, which would orphan any insertion meant
    to land inside it. Roles are assigned by processing committed targets in random order and
    locking in both sides of each pairing (`target -> donor`) as soon as it's made, so a document
    already spoken for as a target can't later be picked as someone else's donor, and vice versa.
    This still allows most committed targets to pair up even when *every* document in the stream
    independently qualifies as a target -- confirmed against real `NL-HaNA_1.01.02` ground truth,
    where the common case is 2-4 sections that are all comfortably long enough to qualify (see
    `docs/challenges/simulation_harness.md`); an earlier version that excluded every committed
    target from the donor pool up front left this common case with zero eligible donors at all.
    """
    stream = list(stream)
    provenance = list(provenance)
    blocks = document_blocks(stream)
    if len(blocks) < 2:
        return stream, provenance

    doc_ids = [doc_id for doc_id, _, _ in blocks]
    targets = [doc_id for doc_id, start, end in blocks if end - start >= 2]
    committed_targets = [t for t in targets if rng.random() < rate]
    rng.shuffle(committed_targets)

    donors_used: set[str] = set()
    assigned_targets: set[str] = set()
    target_donor: dict[str, str] = {}
    for target_id in committed_targets:
        if target_id in donors_used:
            continue  # already spent as someone else's donor this call; can't also be a target
        candidates = [
            d for d in doc_ids
            if d != target_id and d not in donors_used and d not in assigned_targets
        ]
        if not candidates:
            continue
        donor_id = rng.choice(candidates)
        donors_used.add(donor_id)
        assigned_targets.add(target_id)
        target_donor[target_id] = donor_id

    if not target_donor:
        return stream, provenance

    donor_ids = set(target_donor.values())
    removed: dict[str, tuple[list[StreamUnit], list[ProvenanceEntry]]] = {}
    kept_stream: list[StreamUnit] = []
    kept_prov: list[ProvenanceEntry] = []
    for unit, prov in zip(stream, provenance):
        if unit.document_id in donor_ids:
            units, provs = removed.setdefault(unit.document_id, ([], []))
            units.append(unit)
            provs.append(prov)
        else:
            kept_stream.append(unit)
            kept_prov.append(prov)

    new_stream: list[StreamUnit] = []
    new_prov: list[ProvenanceEntry] = []
    i = 0
    while i < len(kept_stream):
        doc_id = kept_stream[i].document_id
        j = i
        while j < len(kept_stream) and kept_stream[j].document_id == doc_id:
            j += 1
        block_units, block_prov = kept_stream[i:j], kept_prov[i:j]
        if doc_id in target_donor:
            donor_units, donor_prov = removed[target_donor[doc_id]]
            donor_prov = [_tag(p, 'foreign_document_insertion') for p in donor_prov]
            split = rng.randint(1, len(block_units) - 1)
            new_stream.extend(block_units[:split])
            new_prov.extend(block_prov[:split])
            new_stream.extend(donor_units)
            new_prov.extend(donor_prov)
            new_stream.extend(block_units[split:])
            new_prov.extend(block_prov[split:])
        else:
            new_stream.extend(block_units)
            new_prov.extend(block_prov)
        i = j

    return new_stream, new_prov


def swap_and_insertion(
    stream: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    swap_rate: float,
    insertion_rate: float,
    rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
    """Case 3: `intra_document_swap` followed by `foreign_document_insertion`, provenance
    threaded through both (see
    `docs/challenges/page_order_problems.md#case-3-swap-and-insertion-combined`)."""
    stream, provenance = intra_document_swap(stream, provenance, swap_rate, rng)
    stream, provenance = foreign_document_insertion(stream, provenance, insertion_rate, rng)
    return stream, provenance


def document_level_swap(
    stream: list[StreamUnit],
    provenance: list[ProvenanceEntry],
    rate: float,
    rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
    """Case 4: independently, with probability `rate`, each *adjacent* pair of documents in the
    block order swaps places (see
    `docs/challenges/page_order_problems.md#case-4-document-level-swap`). Each document's own
    internal page order is untouched -- only which position its whole block occupies changes.

    This is a bounded, local perturbation model rather than an arbitrary global permutation over
    all documents: one document ending up next to where it should be (misfiled relative to a
    neighbour) is a more physically plausible corruption for numbered deeds or dated sessions
    than a fully arbitrary reshuffle of the whole document order. No-op if fewer than two
    documents are present.
    """
    stream = list(stream)
    provenance = list(provenance)
    blocks = document_blocks(stream)
    if len(blocks) < 2:
        return stream, provenance

    order = list(range(len(blocks)))
    moved: set[int] = set()
    for k in range(len(order) - 1):
        if rng.random() < rate:
            order[k], order[k + 1] = order[k + 1], order[k]
            moved.add(order[k])
            moved.add(order[k + 1])

    new_stream: list[StreamUnit] = []
    new_prov: list[ProvenanceEntry] = []
    for block_idx in order:
        _doc_id, start, end = blocks[block_idx]
        new_stream.extend(stream[start:end])
        if block_idx in moved:
            new_prov.extend(_tag(p, 'document_level_swap') for p in provenance[start:end])
        else:
            new_prov.extend(provenance[start:end])
    return new_stream, new_prov
