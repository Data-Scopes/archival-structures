import random
from pathlib import Path

import pytest

from archival_structures.annotation.ground_truth import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    read_ground_truth,
)
from archival_structures.stream_analysis.corruption.canonical_stream import (
    StreamUnit,
    extract_canonical_stream,
)
from archival_structures.stream_analysis.corruption.metrics import observed_start_labels
from archival_structures.stream_analysis.corruption.operators import (
    document_level_swap,
    foreign_document_insertion,
    initial_provenance,
    intra_document_swap,
    swap_and_insertion,
)
from archival_structures.stream_analysis.corruption.schema import (
    CorruptedStream,
    read_corrupted_stream,
    write_corrupted_stream,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _make_stream(*lengths: int) -> list[StreamUnit]:
    """A synthetic canonical stream with one document per length in `lengths`, each document's
    pages numbered sequentially within it (`d{k}_p{i}`)."""
    stream = []
    for doc_idx, length in enumerate(lengths):
        doc_id = f'd{doc_idx}'
        for page_idx in range(length):
            stream.append(StreamUnit(scan_id=f'{doc_id}_p{page_idx}.jpg', document_id=doc_id))
    return stream


class TestExtractCanonicalStream:
    def test_flattens_elements_in_order(self):
        gt = InventoryGroundTruth(
            inventory_id='TEST_1',
            ead_ref={'unitid': 'TEST', 'file_unitid': '1'},
            document_type='resolutions_book',
            layers={
                'section': [
                    AnnotationElement(id='sec_001', type='index_page', spans=[
                        Span(scan_id='TEST_1_0001.jpg', page_side='recto'),
                        Span(scan_id='TEST_1_0002.jpg', page_side='full'),
                    ]),
                    AnnotationElement(id='sec_002', type='resolution_page', spans=[
                        Span(scan_id='TEST_1_0002.jpg', page_side='recto'),
                        Span(scan_id='TEST_1_0003.jpg', page_side='full'),
                    ]),
                ],
            },
        )
        stream = extract_canonical_stream(gt, layer='section')
        assert [u.document_id for u in stream] == ['sec_001', 'sec_001', 'sec_002', 'sec_002']
        # scan 0002 appears twice -- once per side -- not deduplicated, since the two spans
        # carry different page_side values and belong to different elements
        assert [u.scan_id for u in stream] == [
            'TEST_1_0001.jpg', 'TEST_1_0002.jpg', 'TEST_1_0002.jpg', 'TEST_1_0003.jpg',
        ]
        assert [u.page_side for u in stream] == ['recto', 'full', 'recto', 'full']

    def test_deduplicates_repeated_span_within_one_element(self):
        gt = InventoryGroundTruth(
            inventory_id='TEST_1',
            ead_ref={'unitid': 'TEST', 'file_unitid': '1'},
            document_type='resolutions_book',
            layers={
                'section': [
                    AnnotationElement(id='sec_001', type='index_page', spans=[
                        Span(scan_id='TEST_1_0001.jpg', page_side='recto'),
                        Span(scan_id='TEST_1_0001.jpg', page_side='recto'),
                    ]),
                ],
            },
        )
        stream = extract_canonical_stream(gt, layer='section')
        assert len(stream) == 1

    def test_against_real_ground_truth_file(self):
        path = REPO_ROOT / 'data' / 'ground_truth' / 'NL-HaNA_1.01.02' / '3771.yaml'
        gt = read_ground_truth(path)
        stream = extract_canonical_stream(gt, layer='section')

        assert len(stream) > 0
        # sections cover the volume end to end without gaps, so every unit belongs to some
        # section, and section ids appear in file order (sec_001, then sec_002, ...)
        doc_ids_in_order = []
        for unit in stream:
            if not doc_ids_in_order or doc_ids_in_order[-1] != unit.document_id:
                doc_ids_in_order.append(unit.document_id)
        assert doc_ids_in_order == sorted(doc_ids_in_order)

        # the real sub-page boundary: scan 0024 is split between sec_001 (verso) and
        # sec_002 (recto) -- see docs/challenges/simulation_harness.md
        boundary_units = [u for u in stream if u.scan_id == 'NL-HaNA_1.01.02_3771_0024.jpg']
        assert len(boundary_units) == 2
        sides = {u.page_side: u.document_id for u in boundary_units}
        assert sides == {'verso': 'sec_001', 'recto': 'sec_002'}


def _document_membership(stream: list[StreamUnit]) -> dict[str, set[str]]:
    membership: dict[str, set[str]] = {}
    for unit in stream:
        membership.setdefault(unit.document_id, set()).add(unit.scan_id)
    return membership


class TestIntraDocumentSwap:
    def test_rate_zero_is_a_no_op(self):
        stream = _make_stream(4, 2, 3)
        provenance = initial_provenance(stream)
        observed, out_provenance = intra_document_swap(
            stream, provenance, rate=0.0, rng=random.Random(0),
        )
        assert observed == stream
        assert out_provenance == provenance

    def test_preserves_document_membership_and_boundaries(self):
        stream = _make_stream(4, 2, 3)
        provenance = initial_provenance(stream)
        observed, out_provenance = intra_document_swap(
            stream, provenance, rate=1.0, rng=random.Random(1),
        )
        assert _document_membership(observed) == _document_membership(stream)
        assert [u.document_id for u in observed] == [u.document_id for u in stream]
        # canonical_index is a permutation of range(len(stream)), nothing lost or duplicated
        assert sorted(p.canonical_index for p in out_provenance) == list(range(len(stream)))

    def test_single_page_document_is_untouched(self):
        stream = _make_stream(1, 3)
        provenance = initial_provenance(stream)
        observed, _ = intra_document_swap(stream, provenance, rate=1.0, rng=random.Random(2))
        assert observed[0] == stream[0]

    def test_actually_reorders_at_rate_one(self):
        stream = _make_stream(6)
        provenance = initial_provenance(stream)
        observed, out_provenance = intra_document_swap(
            stream, provenance, rate=1.0, rng=random.Random(3),
        )
        assert observed != stream
        assert any(p.events for p in out_provenance)

    def test_start_label_follows_a_moved_start_page(self):
        # one 6-page document; at rate 1 two of its pages swap. Framing-A ground truth must
        # follow the canonical start page (d0_p0) to wherever it now sits.
        stream = _make_stream(6)
        provenance = initial_provenance(stream)
        for seed in range(20):
            observed, out_provenance = intra_document_swap(
                stream, provenance, rate=1.0, rng=random.Random(seed),
            )
            if observed[0] != stream[0]:
                break
        else:
            raise AssertionError('no seed in 0..19 moved the start page')
        labels = observed_start_labels(observed, out_provenance, stream)
        assert labels == [u.scan_id == 'd0_p0.jpg' for u in observed]
        assert labels[0] is False


def _apply(operator: str, stream, provenance, rng):
    if operator == 'swap_and_insertion':
        return swap_and_insertion(stream, provenance, 1.0, 1.0, rng)
    fn = {
        'intra_document_swap': intra_document_swap,
        'foreign_document_insertion': foreign_document_insertion,
        'document_level_swap': document_level_swap,
    }[operator]
    return fn(stream, provenance, 1.0, rng)


@pytest.mark.parametrize('operator', [
    'intra_document_swap', 'foreign_document_insertion', 'swap_and_insertion',
    'document_level_swap',
])
@pytest.mark.parametrize('seed', range(5))
def test_provenance_points_at_the_unit_in_its_position(operator, seed):
    """Every operator: provenance[k] describes observed[k], i.e.
    canonical[provenance[k].canonical_index] is the unit now at position k."""
    canonical = _make_stream(4, 3, 5, 2, 6)
    observed, provenance = _apply(
        operator, canonical, initial_provenance(canonical), random.Random(seed),
    )
    assert len(provenance) == len(observed) == len(canonical)
    assert [canonical[p.canonical_index] for p in provenance] == observed


class TestForeignDocumentInsertion:
    def test_rate_zero_is_a_no_op(self):
        stream = _make_stream(3, 2, 4)
        provenance = initial_provenance(stream)
        observed, out_provenance = foreign_document_insertion(
            stream, provenance, rate=0.0, rng=random.Random(0),
        )
        assert observed == stream
        assert out_provenance == provenance

    def test_single_document_is_a_no_op(self):
        stream = _make_stream(5)
        provenance = initial_provenance(stream)
        observed, _ = foreign_document_insertion(
            stream, provenance, rate=1.0, rng=random.Random(0),
        )
        assert observed == stream

    def test_pairs_up_targets_even_when_every_document_is_eligible(self):
        # regression test: an earlier version excluded *every* committed target from the donor
        # pool up front, so when every document in the stream independently qualified as a
        # target (the common real-data case -- see docs/challenges/simulation_harness.md), no
        # donor was ever available and the operator silently no-opped. Two mutually-eligible
        # documents at rate=1.0 should still pair up (one becomes the other's donor).
        stream = _make_stream(4, 4)
        provenance = initial_provenance(stream)
        observed, out_provenance = foreign_document_insertion(
            stream, provenance, rate=1.0, rng=random.Random(9),
        )
        assert any('foreign_document_insertion' in p.events for p in out_provenance)
        assert sorted(p.canonical_index for p in out_provenance) == list(range(len(stream)))
        assert _document_membership(observed) == _document_membership(stream)

    def test_conserves_all_units_and_splits_target_contiguity(self):
        # only d0 has >= 2 pages, so it's the only insertion target -- and d1/d2 (1 page each)
        # are guaranteed available as donors.
        stream = _make_stream(4, 1, 1)
        provenance = initial_provenance(stream)
        observed, out_provenance = foreign_document_insertion(
            stream, provenance, rate=1.0, rng=random.Random(4),
        )
        # every canonical unit still appears exactly once
        assert sorted(p.canonical_index for p in out_provenance) == list(range(len(stream)))
        assert _document_membership(observed) == _document_membership(stream)

        # at least one document's own units are no longer one contiguous run -- that's the
        # entire point of Case 2 (see docs/challenges/page_order_problems.md)
        doc_id_runs: list[str] = []
        for unit in observed:
            if not doc_id_runs or doc_id_runs[-1] != unit.document_id:
                doc_id_runs.append(unit.document_id)
        assert len(doc_id_runs) > len(set(u.document_id for u in observed))

    def test_marks_donor_pages_with_event(self):
        stream = _make_stream(4, 1, 1)
        provenance = initial_provenance(stream)
        _observed, out_provenance = foreign_document_insertion(
            stream, provenance, rate=1.0, rng=random.Random(5),
        )
        assert any('foreign_document_insertion' in p.events for p in out_provenance)


class TestSwapAndInsertion:
    def test_composes_both_operators(self):
        # only d0 has >= 2 pages: enough for a swap on its own positions, and small enough (only
        # one target-eligible document) to guarantee a donor is available for insertion too --
        # see the comment in TestForeignDocumentInsertion.test_conserves_all_units_and_splits_target_contiguity
        stream = _make_stream(4, 1, 1)
        provenance = initial_provenance(stream)
        observed, out_provenance = swap_and_insertion(
            stream, provenance, swap_rate=1.0, insertion_rate=1.0, rng=random.Random(6),
        )
        assert sorted(p.canonical_index for p in out_provenance) == list(range(len(stream)))
        assert _document_membership(observed) == _document_membership(stream)
        events = {e for p in out_provenance for e in p.events}
        assert 'intra_document_swap' in events
        assert 'foreign_document_insertion' in events


class TestDocumentLevelSwap:
    def test_rate_zero_is_a_no_op(self):
        stream = _make_stream(3, 2, 4)
        provenance = initial_provenance(stream)
        observed, out_provenance = document_level_swap(
            stream, provenance, rate=0.0, rng=random.Random(0),
        )
        assert observed == stream
        assert out_provenance == provenance

    def test_single_document_is_a_no_op(self):
        stream = _make_stream(5)
        provenance = initial_provenance(stream)
        observed, _ = document_level_swap(stream, provenance, rate=1.0, rng=random.Random(0))
        assert observed == stream

    def test_preserves_each_documents_internal_order(self):
        stream = _make_stream(3, 2, 4)
        provenance = initial_provenance(stream)
        observed, out_provenance = document_level_swap(
            stream, provenance, rate=1.0, rng=random.Random(7),
        )
        assert _document_membership(observed) == _document_membership(stream)
        for doc_id in {u.document_id for u in stream}:
            canonical_order = [u.scan_id for u in stream if u.document_id == doc_id]
            observed_order = [u.scan_id for u in observed if u.document_id == doc_id]
            assert observed_order == canonical_order
        assert sorted(p.canonical_index for p in out_provenance) == list(range(len(stream)))


class TestCorruptedStreamIO:
    def test_round_trips_through_yaml(self, tmp_path):
        stream = _make_stream(3, 2)
        provenance = initial_provenance(stream)
        observed, out_provenance = intra_document_swap(
            stream, provenance, rate=1.0, rng=random.Random(8),
        )
        cs = CorruptedStream(
            inventory_id='TEST_1',
            layer='section',
            case='intra_document_swap',
            params={'rate': 1.0},
            seed=8,
            observed=observed,
            provenance=out_provenance,
        )
        path = tmp_path / 'TEST_1-intra_document_swap-rate1.0-seed8.yaml'
        write_corrupted_stream(cs, path)
        loaded = read_corrupted_stream(path)

        assert loaded.inventory_id == cs.inventory_id
        assert loaded.observed == cs.observed
        assert loaded.provenance == cs.provenance
