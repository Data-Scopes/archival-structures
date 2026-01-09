from unittest import TestCase

import pagexml.model.physical_document_model as pdm

import archival_structures.analysis.allen_interval as ali


def make_line(x, y, w, h):
    coords = pdm.Coords.coords_from_box_params(x, y, w, h)
    return pdm.PageXMLTextLine(coords=coords)


class TestAllenInterval(TestCase):

    def test_before(self):
        line1 = make_line(30, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('before', ali.get_allen_interval(line1, line2, debug=1))

    def test_met_by(self):
        line1 = make_line(20, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('met by', ali.get_allen_interval(line1, line2, debug=1))

    def test_overlapped_by(self):
        line1 = make_line(18, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('oi', ali.get_allen_interval(line1, line2, debug=1))

    def test_finishes(self):
        line1 = make_line(14, 10, 6, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('f', ali.get_allen_interval(line1, line2, debug=1))

    def test_contains(self):
        line1 = make_line(12, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('d', ali.get_allen_interval(line1, line2, debug=1))

    def test_started_by(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(10, 10, 6, 10)
        self.assertEqual('si', ali.get_allen_interval(line1, line2, debug=1))

    def test_equal(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('equal', ali.get_allen_interval(line1, line2, debug=1))

    def test_starts(self):
        line1 = make_line(10, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('s', ali.get_allen_interval(line1, line2, debug=1))

    def test_contained_by(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 4, 10)
        self.assertEqual('di', ali.get_allen_interval(line1, line2, debug=1))

    def test_finished_by(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(16, 10, 4, 10)
        self.assertEqual('fi', ali.get_allen_interval(line1, line2, debug=1))

    def test_overlaps(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 14, 10)
        self.assertEqual('o', ali.get_allen_interval(line1, line2, debug=1))

    def test_meets(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(20, 10, 10, 10)
        self.assertEqual('meets', ali.get_allen_interval(line1, line2, debug=1))

    def test_after(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(30, 10, 10, 10)
        self.assertEqual('after', ali.get_allen_interval(line1, line2, debug=1))


class TestExtendedAllenInterval(TestCase):

    def test_before(self):
        line1 = make_line(30, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('before', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_met_by(self):
        line1 = make_line(20, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('met by', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_least_least(self):
        line1 = make_line(18, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('loli', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_most_least(self):
        line1 = make_line(16, 10, 6, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('moli', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_most_most(self):
        line1 = make_line(14, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('momi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_least_most(self):
        line1 = make_line(14, 10, 14, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('lomi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_least_finishes(self):
        line1 = make_line(16, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('lf', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_most_finishes(self):
        line1 = make_line(14, 10, 6, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('mf', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contains_left(self):
        line1 = make_line(12, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('ld', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contains_center(self):
        line1 = make_line(13, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('cd', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contains_right(self):
        line1 = make_line(14, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('rd', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_starts_least(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(10, 10, 4, 10)
        self.assertEqual('lsi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_starts_most(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(10, 10, 6, 10)
        self.assertEqual('msi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_equal(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('equal', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_started_by_least(self):
        line1 = make_line(10, 10, 4, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('ls', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_started_by_most(self):
        line1 = make_line(10, 10, 6, 10)
        line2 = make_line(10, 10, 10, 10)
        self.assertEqual('ms', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contained_by_left(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(12, 10, 4, 10)
        self.assertEqual('ldi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contained_by_center(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(13, 10, 4, 10)
        self.assertEqual('cdi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_contained_by_right(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 4, 10)
        self.assertEqual('rdi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_least_finished_by(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(16, 10, 4, 10)
        self.assertEqual('lfi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_left_most_finished_by(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 6, 10)
        self.assertEqual('mfi', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_right_least_least(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(18, 10, 10, 10)
        self.assertEqual('lol', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_right_least_most(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(16, 10, 6, 10)
        self.assertEqual('lom', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_right_most_most(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 10, 10)
        self.assertEqual('mom', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_right_most_least(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(14, 10, 14, 10)
        self.assertEqual('mol', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_meets(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(20, 10, 10, 10)
        self.assertEqual('meets', ali.get_extended_allen_interval(line1, line2, debug=1))

    def test_after(self):
        line1 = make_line(10, 10, 10, 10)
        line2 = make_line(30, 10, 10, 10)
        self.assertEqual('after', ali.get_extended_allen_interval(line1, line2, debug=1))
