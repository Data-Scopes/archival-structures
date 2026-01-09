from unittest import TestCase

import pagexml.model.physical_document_model as pdm

import archival_structures.analysis.region_calculus as rcc


def generate_regions():
    coords1 = pdm.Coords.coords_from_box_params(x=100, y=100, w=100, h=100)
    coords2 = pdm.Coords.coords_from_box_params(x=200, y=100, w=100, h=100)
    coords3 = pdm.Coords.coords_from_box_params(x=300, y=100, w=100, h=100)
    coords4 = pdm.Coords.coords_from_box_params(x=150, y=150, w=100, h=100)
    coords5 = pdm.Coords.coords_from_box_params(x=150, y=100, w=50, h=100)
    coords6 = pdm.Coords.coords_from_box_params(x=125, y=125, w=50, h=50)
    coords7 = pdm.Coords.coords_from_box_params(x=101, y=101, w=100, h=100)
    region1 = pdm.PageXMLRegion(coords=coords1)
    region2 = pdm.PageXMLRegion(coords=coords2)
    region3 = pdm.PageXMLRegion(coords=coords3)
    region4 = pdm.PageXMLRegion(coords=coords4)
    region5 = pdm.PageXMLRegion(coords=coords5)
    region6 = pdm.PageXMLRegion(coords=coords6)
    region7 = pdm.PageXMLRegion(coords=coords7)
    return region1, region2, region3, region4, region5, region6, region7


class TestRegionCalculus(TestCase):

    def setUp(self) -> None:
        regions = generate_regions()
        self.region1 = regions[0]
        self.region2 = regions[1]
        self.region3 = regions[2]
        self.region4 = regions[3]
        self.region5 = regions[4]
        self.region6 = regions[5]


    def test_connected(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region2, debug=1)
        self.assertEqual('EC', relation)

    def test_disconnected(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region3, debug=1)
        self.assertEqual('DC', relation)

    def test_partial_overlap(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region4, debug=1)
        self.assertEqual('PO', relation)

    def test_tangential_proper_part(self):
        relation = rcc.get_horizontal_region_relation(self.region5, self.region1, debug=1)
        self.assertEqual('TPP', relation)

    def test_tangential_proper_part_inverse(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region5, debug=1)
        self.assertEqual('TPPi', relation)

    def test_non_tangential_proper_part(self):
        relation = rcc.get_horizontal_region_relation(self.region6, self.region1, debug=1)
        self.assertEqual('NTPP', relation)

    def test_non_tangential_proper_part_inverse(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region6, debug=1)
        self.assertEqual('NTPPi', relation)

    def test_equal(self):
        relation = rcc.get_vertical_region_relation(self.region1, self.region5, debug=1)
        self.assertEqual('EQ', relation)


class TestThickRegionCalculus(TestCase):

    def setUp(self) -> None:
        regions = generate_regions()
        self.region1 = regions[6]
        self.region2 = regions[1]
        self.region3 = regions[2]
        self.region4 = regions[3]
        self.region5 = regions[4]
        self.region6 = regions[5]

    def test_thick_connected(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region2, max_diff=2, debug=1)
        self.assertEqual('EC', relation)

    def test_thick_disconnected(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region3, max_diff=2, debug=1)
        self.assertEqual('DC', relation)

    def test_thick_partial_overlap(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region4, max_diff=2, debug=1)
        self.assertEqual('PO', relation)

    def test_thick_tangential_proper_part(self):
        relation = rcc.get_horizontal_region_relation(self.region5, self.region1, max_diff=2, debug=1)
        self.assertEqual('TPP', relation)

    def test_thick_tangential_proper_part_inverse(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region5, max_diff=2, debug=1)
        self.assertEqual('TPPi', relation)

    def test_thick_non_tangential_proper_part(self):
        relation = rcc.get_horizontal_region_relation(self.region6, self.region1, max_diff=2, debug=1)
        self.assertEqual('NTPP', relation)

    def test_thick_non_tangential_proper_part_inverse(self):
        relation = rcc.get_horizontal_region_relation(self.region1, self.region6, max_diff=2, debug=1)
        self.assertEqual('NTPPi', relation)

    def test_thick_equal(self):
        relation = rcc.get_vertical_region_relation(self.region1, self.region5, max_diff=2, debug=1)
        self.assertEqual('EQ', relation)
