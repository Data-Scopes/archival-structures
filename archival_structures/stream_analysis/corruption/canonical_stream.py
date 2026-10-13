"""Extract a canonical page stream from an `InventoryGroundTruth` annotation layer.

See `docs/challenges/page_order_problems.md` for the formal `C = (c_1, ..., c_n)` notation this
implements, and `docs/challenges/simulation_harness.md` for how this stage fits into the wider
corruption-simulation pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass

from archival_structures.annotation.ground_truth import InventoryGroundTruth


@dataclass(frozen=True)
class StreamUnit:
    """One position in a canonical or observed page stream.

    `document_id` is the id of the `AnnotationElement` (e.g. `"sec_002"`) this unit was drawn
    from -- the ground-truth label the corruption operators and any downstream evaluation both
    key on. It travels with the unit regardless of where a corruption operator moves it, so it
    always reflects true document membership, not the unit's current stream position.
    """

    scan_id: str
    document_id: str
    page_side: str | None = None
    bbox: list[int] | None = None


def extract_canonical_stream(gt: InventoryGroundTruth, layer: str = 'section') -> list[StreamUnit]:
    """The canonical page stream implied by `gt.layers[layer]`, flattened in element order.

    `layer` defaults to `"section"` -- the only layer populated in this repo's real ground-truth
    files today (see `docs/challenges/simulation_harness.md`) -- but any layer name present in
    `gt.layers` works, so this keeps working once finer-grained layers such as `"session"` or
    `"resolution"` are populated.

    Repeated spans referencing the exact same `(scan_id, page_side)` within one element's own
    span list collapse to a single unit, as a defensive guard against duplicate annotation
    entries. Two different elements referencing different sides of the same scan (e.g. one
    element's `page_side: verso` ending where the next element's `page_side: recto` begins) are
    *not* a collision -- they produce two distinct units, which is the correct representation of
    a document boundary that falls inside a two-page scan.

    Raises `KeyError` if `layer` isn't present in `gt.layers` -- check `layer in gt.layers` first
    if the layer might legitimately be absent (most inventories don't have every layer
    populated).
    """
    stream: list[StreamUnit] = []
    seen: set[tuple[str, str | None]] = set()
    for element in gt.layers[layer]:
        for span in element.spans:
            key = (span.scan_id, span.page_side)
            if span.bbox is None and key in seen:
                continue
            seen.add(key)
            stream.append(StreamUnit(
                scan_id=span.scan_id,
                document_id=element.id,
                page_side=span.page_side,
                bbox=span.bbox,
            ))
    return stream


def document_blocks(stream: list[StreamUnit]) -> list[tuple[str, int, int]]:
    """`(document_id, start, end)` for each maximal contiguous run of equal `document_id` in
    `stream`, `end` exclusive.

    Safe to call on a stream already corrupted by `intra_document_swap` (reorders only *within*
    a document's own positions) or `document_level_swap` (moves whole documents as units) --
    every `document_id` still appears as exactly one block afterwards. **Not** safe to call on a
    stream already corrupted by `foreign_document_insertion`: the target document's own units are
    deliberately split into two separated runs by the spliced-in donor, so its `document_id`
    appears as two separate block tuples, not one -- accurate as far as this function's own
    contract goes (it reports contiguous runs, and those runs genuinely aren't contiguous
    anymore), but it breaks the "one block per document_id" assumption
    `archival_structures.stream_analysis.corruption.operators` makes of its own input for
    `foreign_document_insertion` and `document_level_swap`. None of the composed operators
    re-invoke this after a prior `foreign_document_insertion` (`swap_and_insertion` only ever
    runs insertion last) -- but any future operator or metric built on top of an already-inserted
    stream needs to account for this rather than assume it away.
    """
    blocks: list[tuple[str, int, int]] = []
    if not stream:
        return blocks
    start = 0
    for i in range(1, len(stream) + 1):
        if i == len(stream) or stream[i].document_id != stream[start].document_id:
            blocks.append((stream[start].document_id, start, i))
            start = i
    return blocks
