import unittest

import pagexml.model.physical_document_model as pdm
import archival_structures.analysis.grid_analysis as grid_analysis


class TestGetLineGridPoints(unittest.TestCase):

    def test_get_line_grid_left_right_round_coords(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(100, 100), (400, 100), (400, 200), (100, 200)]))
        left, right = grid_analysis.get_line_grid_left_right(line, indent=0)
        self.assertEqual((100, 400), (left, right))

    def test_get_line_grid_left_right_round_coords_indent(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(100, 100), (400, 100), (400, 200), (100, 200)]))
        left, right = grid_analysis.get_line_grid_left_right(line, indent=25)
        self.assertEqual((100, 400), (left, right))

    def test_get_line_grid_left_right_specific_coords_1(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(137, 100), (423, 100), (423, 200), (137, 200)]))
        left, right = grid_analysis.get_line_grid_left_right(line, indent=0)
        self.assertEqual((100, 450), (left, right))

    def test_get_line_grid_left_right_specific_coords_2(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(137, 100), (412, 100), (412, 200), (137, 200)]))
        left, right = grid_analysis.get_line_grid_left_right(line, indent=0)
        self.assertEqual((100, 400), (left, right))

    def test_get_line_grid_top_bottom_round_coords(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(100, 100), (400, 100), (400, 200), (100, 200)]))
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((100, 200), (top, bottom))

    def test_get_line_grid_top_bottom_specific_coords_1(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(100, 130), (400, 130), (400, 190), (100, 190)]))
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((150, 200), (top, bottom))

    def test_get_line_grid_top_bottom_specific_coords_2(self) -> None:
        line = pdm.PageXMLTextLine(coords=pdm.Coords([(100, 115), (400, 115), (400, 175), (100, 175)]))
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((100, 150), (top, bottom))

    def test_get_line_grid_top_bottom_round_baseline(self) -> None:
        line = pdm.PageXMLTextLine(baseline=pdm.Baseline([(100, 100), (400, 100)]), xheight=30)
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((50, 100), (top, bottom))

    def test_get_line_grid_top_bottom_specific_baseline_1(self) -> None:
        line = pdm.PageXMLTextLine(baseline=pdm.Baseline([(100, 130), (400, 130)]), xheight=30)
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((100, 150), (top, bottom))

    def test_get_line_grid_top_bottom_specific_baseline_2(self) -> None:
        line = pdm.PageXMLTextLine(baseline=pdm.Baseline([(100, 120), (400, 120)]), xheight=30)
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((100, 150), (top, bottom))

    def test_get_line_grid_top_bottom_specific_baseline_3(self) -> None:
        line = pdm.PageXMLTextLine(baseline=pdm.Baseline([(100, 110), (400, 110)]), xheight=30)
        top, bottom = grid_analysis.get_line_grid_top_bottom(line)
        self.assertEqual((50, 100), (top, bottom))
