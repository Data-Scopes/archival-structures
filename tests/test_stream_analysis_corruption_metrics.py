import pytest

from archival_structures.stream_analysis.corruption.canonical_stream import StreamUnit
from archival_structures.stream_analysis.corruption.operators import ProvenanceEntry
from archival_structures.stream_analysis.corruption.metrics import (
    grouping_ari,
    observed_start_labels,
    pairs_to_groups,
    score_same_document_pairs,
    score_start_classification,
    true_same_document_pairs,
    true_start_indices,
    window_diff,
)


class TestScoreStartClassification:
    def test_perfect_prediction(self):
        true_labels = [True, False, False, True, False]
        result = score_start_classification(true_labels, true_labels)
        assert result.precision == 1.0
        assert result.recall == 1.0
        assert result.f1 == 1.0

    def test_one_false_positive_and_one_false_negative(self):
        true_labels = [True, False, False, True, False]
        # predicted flags position 2 instead of position 3
        predicted = [True, False, True, False, False]
        result = score_start_classification(true_labels, predicted)
        # tp=1 (position 0), fp=1 (position 2), fn=1 (position 3)
        assert result.precision == pytest.approx(0.5)
        assert result.recall == pytest.approx(0.5)
        assert result.f1 == pytest.approx(0.5)

    def test_raises_on_length_mismatch(self):
        with pytest.raises(ValueError):
            score_start_classification([True, False], [True])


class TestTrueStartIndicesAndObservedStartLabels(object):
    def _case_1_worked_example(self):
        """The pi_b permutation from docs/challenges/page_order_problems.md's Case 1 worked
        example: canonical D1=(c1,c2,c3,c4), D2=(c5,c6); observed = (c3,c2,c1,c4,c5,c6)."""
        canonical = [
            StreamUnit(scan_id='c1', document_id='d1'),
            StreamUnit(scan_id='c2', document_id='d1'),
            StreamUnit(scan_id='c3', document_id='d1'),
            StreamUnit(scan_id='c4', document_id='d1'),
            StreamUnit(scan_id='c5', document_id='d2'),
            StreamUnit(scan_id='c6', document_id='d2'),
        ]
        observed = [
            canonical[2], canonical[1], canonical[0], canonical[3], canonical[4], canonical[5],
        ]
        provenance = [
            ProvenanceEntry(canonical_index=2),
            ProvenanceEntry(canonical_index=1),
            ProvenanceEntry(canonical_index=0),
            ProvenanceEntry(canonical_index=3),
            ProvenanceEntry(canonical_index=4),
            ProvenanceEntry(canonical_index=5),
        ]
        return canonical, observed, provenance

    def test_true_start_indices_on_canonical_stream(self):
        canonical, _observed, _provenance = self._case_1_worked_example()
        assert true_start_indices(canonical) == {0, 4}

    def test_observed_start_labels_follows_the_true_start_page_not_stream_position(self):
        # this is the exact point of docs/challenges/page_order_problems.md's Case 1: under
        # pi_b, c1 (the true start of d1) sits at *position 2* in the observed stream, not
        # position 0 -- an oracle classifier using canonical ground truth must say "no" for c3
        # (observed position 0) and "yes" for c1 (observed position 2).
        canonical, observed, provenance = self._case_1_worked_example()
        labels = observed_start_labels(observed, provenance, canonical)
        assert labels == [False, False, True, False, True, False]

    def test_observed_start_labels_duplicate_start_is_continuation(self):
        canonical = [
            StreamUnit(scan_id='c1', document_id='d1'),
            StreamUnit(scan_id='c2', document_id='d1'),
            StreamUnit(scan_id='c3', document_id='d2'),
        ]
        observed = [canonical[0], canonical[0], canonical[1], canonical[2]]
        provenance = [ProvenanceEntry(canonical_index=i) for i in (0, 0, 1, 2)]
        assert observed_start_labels(observed, provenance, canonical) == [True, False, False, True]

    def test_observed_start_labels_unchanged_without_repeats(self):
        canonical, observed, provenance = self._case_1_worked_example()
        assert observed_start_labels(observed, provenance, canonical) == [
            False, False, True, False, True, False,
        ]


class TestWindowDiff:
    def test_zero_for_identical_labels(self):
        labels = [True, False, True, False]
        assert window_diff(labels, labels) == 0.0

    def test_known_value_for_one_off_boundary(self):
        # 2 true segments (boundary between position 1 and 2); predicted boundary shifted one
        # position earlier (between position 0 and 1). Hand-computed with the default k=1:
        # true gaps  = [False, True, False]
        # pred gaps  = [True, False, False]
        # window 0: true={False}, pred={True}  -> disagree
        # window 1: true={True},  pred={False} -> disagree
        # window 2: true={False}, pred={False} -> agree
        # window_diff = 2/3
        true_labels = [True, False, True, False]
        predicted_labels = [True, True, False, False]
        assert window_diff(true_labels, predicted_labels) == pytest.approx(2 / 3)

    def test_raises_on_length_mismatch(self):
        with pytest.raises(ValueError):
            window_diff([True, False], [True])

    def test_raises_with_fewer_than_two_positions(self):
        with pytest.raises(ValueError):
            window_diff([True], [True])

    def test_raises_when_k_exceeds_available_gaps(self):
        with pytest.raises(ValueError):
            window_diff([True, False, True], [True, False, True], k=5)


class TestSameDocumentPairs:
    def test_true_same_document_pairs(self):
        observed = [
            StreamUnit(scan_id='a', document_id='d1'),
            StreamUnit(scan_id='b', document_id='d1'),
            StreamUnit(scan_id='c', document_id='d2'),
        ]
        assert true_same_document_pairs(observed) == [True, False]

    def test_score_same_document_pairs_perfect(self):
        pairs = [True, False, True]
        result = score_same_document_pairs(pairs, pairs)
        assert result.precision == 1.0
        assert result.recall == 1.0
        assert result.f1 == 1.0

    def test_raises_on_length_mismatch(self):
        with pytest.raises(ValueError):
            score_same_document_pairs([True, False], [True])


class TestPairsToGroupsAndGroupingARI:
    def test_pairwise_perfect_prediction_can_still_have_imperfect_grouping(self):
        # the Case 2 interleaving example from
        # docs/challenges/page_order_problems.md#case-2-foreign-document-insertion-interleaving
        observed = [
            StreamUnit(scan_id='d1_1', document_id='d1'),
            StreamUnit(scan_id='d2_1', document_id='d2'),
            StreamUnit(scan_id='d2_2', document_id='d2'),
            StreamUnit(scan_id='d1_2', document_id='d1'),
        ]
        true_pairs = true_same_document_pairs(observed)
        assert true_pairs == [False, True, False]

        # a Framing-B classifier that gets every single pairwise label exactly right...
        predicted_groups = pairs_to_groups(true_pairs)
        assert predicted_groups == [0, 1, 1, 2]

        # ...still doesn't recover the true grouping: d1's two pages land in separate groups
        assert grouping_ari(observed, predicted_groups) < 1.0

        # a genuinely correct grouping (matching document_id exactly, whatever its ids are
        # called) does score a perfect 1.0
        perfect_groups = [0, 1, 1, 0]
        assert grouping_ari(observed, perfect_groups) == pytest.approx(1.0)

    def test_pairs_to_groups_all_same_document(self):
        assert pairs_to_groups([True, True, True]) == [0, 0, 0, 0]

    def test_pairs_to_groups_all_different_documents(self):
        assert pairs_to_groups([False, False]) == [0, 1, 2]

    def test_grouping_ari_raises_on_length_mismatch(self):
        observed = [StreamUnit(scan_id='a', document_id='d1')]
        with pytest.raises(ValueError):
            grouping_ari(observed, [0, 1])
