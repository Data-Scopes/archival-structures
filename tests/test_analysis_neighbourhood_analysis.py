import unittest

import pagexml.model.physical_document_model as pdm

import archival_structures.analysis.neighbourhood_analysis as na


class TestNeighbourhoodLines(unittest.TestCase):

    def setUp(self) -> None:
        coords1 = pdm.Coords([(100, 100), (500, 100), (500, 200), (100, 200)])
        coords2 = pdm.Coords([(100, 200), (200, 200), (200, 300), (100, 300)])
        coords3 = pdm.Coords([(200, 200), (400, 200), (400, 300), (200, 300)])
        coords4 = pdm.Coords([(400, 200), (500, 200), (500, 300), (400, 300)])
        coords5 = pdm.Coords([(150, 400), (450, 400), (450, 500), (150, 500)])
        self.line1 = pdm.PageXMLTextLine(doc_id='l1', coords=coords1, text='do')
        self.line2 = pdm.PageXMLTextLine(doc_id='l2', coords=coords2, text='not')
        self.line3 = pdm.PageXMLTextLine(doc_id='l3', coords=coords3, text='read')
        self.line4 = pdm.PageXMLTextLine(doc_id='l4', coords=coords4, text='this')
        self.line5 = pdm.PageXMLTextLine(doc_id='l5', coords=coords5, text='please')
        self.lines = [self.line1, self.line2, self.line3, self.line4, self.line5]
        self.region1 = pdm.PageXMLTextRegion(doc_id='r1', lines=[self.line1])
        self.region2 = pdm.PageXMLTextRegion(doc_id='r2', lines=self.lines)

    def test_single_line_has_none_neighbours(self):
        line_pairs = na.get_neighbouring_line_pairs(self.region1.lines)
        for lp in line_pairs:
            l1, l2, rel = lp
            print(f"{'None' if l1 is None else l1.id: >6} - {'None' if l2 is None else l2.id: >6} - {rel}")
        print(len(line_pairs))
        self.assertEqual(True, all(l2 is None for l1, l2, rel in line_pairs))

    def test_top_line_has_six_pairs(self):
        line_pairs = na.get_neighbouring_line_pairs(self.region2.lines)
        l1_pairs = [lp for lp in line_pairs if lp[0].id == self.line1.id]
        self.assertEqual(6, len(l1_pairs))

    def test_left_line_has_none_left(self):
        line_pairs = na.get_neighbouring_line_pairs(self.region2.lines)
        l2_pairs = [lp for lp in line_pairs if lp[0] == self.line2.id]
        l2_left_pair = [lp for lp in l2_pairs if lp[2] == 'left']
        self.assertEqual(True, all(rel is None for l1, l2, rel in l2_left_pair))

    def test_max_vertical_dist_sets_neighbour_to_none(self):
        nh = na.LineNeighbourHood(self.region2.lines, max_vertical_dist=50)
        for l1, rel in [(self.line2, 'below'), (self.line5, 'above')]:
            with self.subTest(l1.id):
                self.assertEqual(True, all(l2 is None for l2 in nh.has_rel_neighbour[l1][rel]))

    def test_line_neighbourhood_can_set_lines_separately(self):
        nh = na.LineNeighbourHood()
        nh.add_lines(self.region2.lines)
        self.assertEqual('right', nh.have_rel[(self.line2, self.line3)])

    def test_line_neighbourhood_has_all_directions_for_all_lines(self):
        nh = na.LineNeighbourHood(self.region2.lines)
        for li, l1 in enumerate(nh.has_rel_neighbour):
            for ri, rel in enumerate(['above', 'below', 'left', 'right']):
                with self.subTest(f"{li}-{ri}",):
                    self.assertEqual(True, len(nh.has_rel_neighbour[l1][rel]) > 0)

    def test_right_returns_actual_neighbour(self):
        nh = na.LineNeighbourHood(self.region2.lines)
        self.assertEqual([self.line3], nh.right(self.line2))

    def test_left_returns_none_at_row_edge(self):
        nh = na.LineNeighbourHood(self.region2.lines)
        self.assertIsNone(nh.left(self.line2))

    def test_top_returns_above_neighbour(self):
        nh = na.LineNeighbourHood(self.region2.lines)
        self.assertEqual([self.line1], nh.top(self.line2))

    def test_bottom_returns_below_neighbour(self):
        nh = na.LineNeighbourHood(self.region2.lines)
        self.assertEqual([self.line5], nh.bottom(self.line2))


class TestLineVerticalGap(unittest.TestCase):
    """Regression tests for the `pdm.vertical_distance` bug: for baseline-bearing lines it
    returns `abs(bottom1 - bottom2)` unconditionally, without checking whether the lines'
    vertical extents actually overlap. With slanted (non-flat) baselines whose y-ranges overlap,
    this reports a nonzero "gap" for lines that don't actually have one.  `line_vertical_gap`
    must compute the real (overlap-aware) gap instead."""

    def test_gap_is_zero_for_overlapping_slanted_baselines(self):
        # baseline1 spans y in [100, 140], baseline2 spans y in [120, 160] -- these ranges
        # overlap, so the real vertical gap is 0.
        baseline1 = pdm.Baseline([(100, 100), (500, 140)])
        baseline2 = pdm.Baseline([(100, 160), (500, 120)])
        line1 = pdm.PageXMLTextLine(doc_id='l1', coords=pdm.Coords.coords_from_box_params(100, 90, 400, 60),
                                    baseline=baseline1)
        line2 = pdm.PageXMLTextLine(doc_id='l2', coords=pdm.Coords.coords_from_box_params(100, 110, 400, 60),
                                    baseline=baseline2)

        self.assertEqual(0, na.line_vertical_gap(line1, line2))
        # confirm this is a genuine regression test: the upstream library function disagrees,
        # reporting the buggy baseline-bottom-to-baseline-bottom offset (20) instead of 0.
        self.assertEqual(20, pdm.vertical_distance(line1, line2))

    def test_max_vertical_dist_filters_baseline_bearing_lines_correctly(self):
        # two rows, ~200px apart (top-to-bottom gap), each line has a flat baseline
        baseline1 = pdm.Baseline([(100, 115), (500, 115)])
        baseline2 = pdm.Baseline([(100, 335), (500, 335)])
        line1 = pdm.PageXMLTextLine(doc_id='l1', coords=pdm.Coords.coords_from_box_params(100, 100, 400, 20),
                                    baseline=baseline1)
        line2 = pdm.PageXMLTextLine(doc_id='l2', coords=pdm.Coords.coords_from_box_params(100, 320, 400, 20),
                                    baseline=baseline2)

        # the real gap (220px) exceeds this threshold, so line1/line2 must NOT be neighbours
        nh = na.LineNeighbourHood([line1, line2], max_vertical_dist=50)
        self.assertIsNone(nh.bottom(line1))
        self.assertIsNone(nh.top(line2))
