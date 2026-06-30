from unittest import TestCase

import pagexml.model.physical_document_model as pdm
import pandas as pd

from archival_structures.analysis.sequence_patterns import (
    LineEvent, build_line_sequence, detect_cross_page_continuation,
    find_ngram_patterns, segment_into_elements,
)


def make_line(doc_id: str, x: int, y: int, w: int = 300, h: int = 30):
    return pdm.PageXMLTextLine(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(x, y, w, h))


def make_scan(doc_id: str, lines, width: int = 1000, height: int = 1000):
    region = pdm.PageXMLTextRegion(doc_id='r1', coords=pdm.Coords.coords_from_box_params(0, 0, width, height),
                                   lines=lines)
    return pdm.PageXMLScan(doc_id=doc_id, coords=pdm.Coords.coords_from_box_params(0, 0, width, height),
                           text_regions=[region])


class TestBuildLineSequence(TestCase):

    def test_orders_scans_by_sequence_number_regardless_of_input_order(self):
        scan1 = make_scan('inv_0001', [make_line('l1', 100, 50)])
        scan2 = make_scan('inv_0002', [make_line('l2', 100, 50)])
        sequence = build_line_sequence([scan2, scan1])
        self.assertEqual(['inv_0001', 'inv_0002'], [event.scan_id for event in sequence])

    def test_strips_extension_for_sequence_number(self):
        scan1 = make_scan('inv_0001.jpg', [make_line('l1', 100, 50)])
        scan2 = make_scan('inv_0002.jpg', [make_line('l2', 100, 50)])
        sequence = build_line_sequence([scan2, scan1])
        self.assertEqual(['inv_0001.jpg', 'inv_0002.jpg'], [event.scan_id for event in sequence])

    def test_attaches_cluster_labels_and_position_info(self):
        lines = [make_line(f'l{i}', 100, 50 + i * 40) for i in range(3)]
        scan = make_scan('inv_0001', lines)
        clusters = pd.Series({('inv_0001', 'l0'): 'body', ('inv_0001', 'l1'): 'body',
                              ('inv_0001', 'l2'): 'closing'})
        sequence = build_line_sequence([scan], clusters)
        self.assertEqual(['body', 'body', 'closing'], [event.cluster for event in sequence])
        self.assertTrue(sequence[0].is_first_in_scan)
        self.assertFalse(sequence[0].is_last_in_scan)
        self.assertTrue(sequence[-1].is_last_in_scan)
        self.assertEqual(3, sequence[0].n_lines_in_scan)

    def test_cluster_is_none_when_no_line_clusters_given(self):
        scan = make_scan('inv_0001', [make_line('l1', 100, 50)])
        sequence = build_line_sequence([scan])
        self.assertIsNone(sequence[0].cluster)

    def test_disambiguates_same_line_id_reused_across_scans(self):
        # PageXML line ids (e.g. 'l0') are only unique within one scan, not across an inventory
        # number -- the cluster lookup must be keyed by (scan_id, line_id), not line_id alone
        scan1 = make_scan('inv_0001', [make_line('l0', 100, 50)])
        scan2 = make_scan('inv_0002', [make_line('l0', 100, 50)])
        clusters = pd.Series({('inv_0001', 'l0'): 'body', ('inv_0002', 'l0'): 'closing'})
        sequence = build_line_sequence([scan1, scan2], clusters)
        self.assertEqual('body', sequence[0].cluster)
        self.assertEqual('closing', sequence[1].cluster)


class TestFindNgramPatterns(TestCase):

    def test_counts_consecutive_label_ngrams_across_scan_boundaries(self):
        sequence = [
            LineEvent('s1', 'l1', 'body', 0, 2), LineEvent('s1', 'l2', 'closing', 1, 2),
            LineEvent('s2', 'l3', 'closing', 0, 1),
        ]
        counts = find_ngram_patterns(sequence, n=2)
        self.assertEqual(1, counts[('body', 'closing')])
        self.assertEqual(1, counts[('closing', 'closing')])

    def test_skips_windows_with_unlabelled_or_noise_lines(self):
        sequence = [
            LineEvent('s1', 'l1', 'body', 0, 3), LineEvent('s1', 'l2', None, 1, 3),
            LineEvent('s1', 'l3', -1, 2, 3),
        ]
        counts = find_ngram_patterns(sequence, n=2)
        self.assertEqual(0, sum(counts.values()))


class TestDetectCrossPageContinuation(TestCase):

    def test_true_when_lines_meet_at_page_break_with_matching_indentation(self):
        scan_a = make_scan('s1', [], height=1000)
        line_a = make_line('la', x=110, y=950, w=200)
        line_b = make_line('lb', x=115, y=20, w=200)
        self.assertTrue(detect_cross_page_continuation(scan_a, line_a, line_b))

    def test_false_when_lines_are_far_apart_vertically(self):
        scan_a = make_scan('s1', [], height=1000)
        line_a = make_line('la', x=110, y=500, w=200)  # nowhere near the bottom edge
        line_b = make_line('lb', x=115, y=20, w=200)
        self.assertFalse(detect_cross_page_continuation(scan_a, line_a, line_b))

    def test_false_when_indentation_differs_a_lot(self):
        scan_a = make_scan('s1', [], height=1000)
        line_a = make_line('la', x=50, y=950, w=100)
        line_b = make_line('lb', x=800, y=20, w=100)  # far off to the right
        self.assertFalse(detect_cross_page_continuation(scan_a, line_a, line_b))


class TestSegmentIntoElements(TestCase):

    def test_single_scan_runs_become_separate_elements(self):
        lines = [make_line(f'l{i}', 100, 50 + i * 40) for i in range(4)]
        scan = make_scan('inv_0001', lines)
        clusters = pd.Series({('inv_0001', 'l0'): 'body', ('inv_0001', 'l1'): 'body',
                              ('inv_0001', 'l2'): 'closing', ('inv_0001', 'l3'): 'closing'})
        sequence = build_line_sequence([scan], clusters)
        elements = segment_into_elements(sequence, [scan])
        self.assertEqual(['body', 'closing'], [el.element_type for el in elements])
        self.assertEqual(['l0', 'l1'], elements[0].spans[0].line_ids)
        self.assertEqual(['l2', 'l3'], elements[1].spans[0].line_ids)

    def test_unlabelled_lines_break_a_run(self):
        lines = [make_line(f'l{i}', 100, 50 + i * 40) for i in range(3)]
        scan = make_scan('inv_0001', lines)
        clusters = pd.Series({('inv_0001', 'l0'): 'body', ('inv_0001', 'l1'): None,
                              ('inv_0001', 'l2'): 'body'})
        sequence = build_line_sequence([scan], clusters)
        elements = segment_into_elements(sequence, [scan])
        self.assertEqual(2, len(elements))
        self.assertEqual(['l0'], elements[0].spans[0].line_ids)
        self.assertEqual(['l2'], elements[1].spans[0].line_ids)

    def test_merges_genuine_cross_page_continuation_into_one_element(self):
        body_lines_1 = [make_line(f's1-l{i}', 100, 50 + i * 100) for i in range(3)]
        closing_1 = make_line('s1-closing', 110, 950, w=200)
        scan1 = make_scan('scan_0001', body_lines_1 + [closing_1])

        closing_2 = make_line('s2-closing', 115, 20, w=200)
        body_lines_2 = [make_line(f's2-l{i}', 100, 100 + i * 100) for i in range(2)]
        scan2 = make_scan('scan_0002', [closing_2] + body_lines_2)

        clusters = {(scan1.id, line.id): 'body' for line in body_lines_1}
        clusters.update({(scan2.id, line.id): 'body' for line in body_lines_2})
        clusters[(scan1.id, closing_1.id)] = 'closing'
        clusters[(scan2.id, closing_2.id)] = 'closing'

        sequence = build_line_sequence([scan1, scan2], pd.Series(clusters))
        elements = segment_into_elements(sequence, [scan1, scan2])

        closing_elements = [el for el in elements if el.element_type == 'closing']
        self.assertEqual(1, len(closing_elements))
        self.assertEqual(2, len(closing_elements[0].spans))
        self.assertEqual('scan_0001', closing_elements[0].spans[0].scan_id)
        self.assertEqual('scan_0002', closing_elements[0].spans[1].scan_id)

    def test_does_not_merge_coincidental_same_label_run_across_pages(self):
        mid_heading = make_line('s1-heading', 100, 500, w=200)
        scan1 = make_scan('scan_0001', [mid_heading])
        top_heading = make_line('s2-heading', 110, 20, w=200)
        scan2 = make_scan('scan_0002', [top_heading])

        clusters = pd.Series({(scan1.id, mid_heading.id): 'heading',
                              (scan2.id, top_heading.id): 'heading'})
        sequence = build_line_sequence([scan1, scan2], clusters)
        elements = segment_into_elements(sequence, [scan1, scan2])

        self.assertEqual(2, len(elements))
        self.assertEqual(1, len(elements[0].spans))
        self.assertEqual(1, len(elements[1].spans))
