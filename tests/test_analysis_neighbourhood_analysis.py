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
