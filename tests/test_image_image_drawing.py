import unittest

from PIL import Image

import archival_structures.image.image_base as im_base


class TestSelection(unittest.TestCase):

    def setUp(self) -> None:
        self.image_file = 'data/thumbs/NL-AsnDA/NL-AsnDA_0114.11/NL-AsnDA_0114.11_1/thumb-width_300-scan-NL-AsnDA_0114.11_1_0002.jp2.png'
        self.im = Image.open(self.image_file)
        self.im_w, self.im_h = self.im.size
        self.scan_width = 4904
        self.scan_height = 4009
        self.row = {
            'scan_width': self.scan_width,
            'scan_height': self.scan_height,
            'filepath': self.image_file
        }
        self.page_verso = {
            'x': 0,
            'y': 0,
            'width': 2450,
            'height': 4009
        }
        self.page_recto = {
            'x': 2450,
            'y': 0,
            'width': 2454,
            'height': 4009
        }

    def test_make_selection_sets_default_x_y(self):
        selection = im_base.make_selection(self.im_w, self.im_h, self.image_file)
        self.assertEqual(0, selection.selection_box.x)

    def test_make_selection_from_row_sets_default_x_y(self):
        selection = im_base.make_selection_from_row(self.row)
        self.assertEqual(0, selection.selection_box.x)

    def test_make_selection_from_row_uses_scan_width(self):
        selection = im_base.make_selection_from_row(self.row)
        self.assertEqual(self.scan_width, selection.selection_box.w)

    def test_selection_has_correct_width_scale(self):
        selection = im_base.make_selection_from_row(self.row)
        self.assertEqual(self.scan_width / self.im_w, selection.thumb_width_scale)

    def test_selection_has_page_box(self):
        self.row['x'], self.row['y'] = self.page_recto['x'], self.page_recto['y']
        self.row['width'], self.row['height'] = self.page_recto['width'], self.page_recto['height']
        selection = im_base.make_selection_from_row(self.row)
        # thumb_selection_box rounds to the nearest whole thumbnail pixel, so compare against
        # the same rounding rather than the unrounded scan-space division.
        expected_x = round(self.page_recto['x'] / selection.thumb_width_scale)
        self.assertEqual(expected_x, selection.thumb_selection_box.x)

    def test_scan_to_thumb_to_scan_box(self):
        self.row['x'], self.row['y'] = self.page_recto['x'], self.page_recto['y']
        self.row['width'], self.row['height'] = self.page_recto['width'], self.page_recto['height']
        selection = im_base.make_selection_from_row(self.row)
        scan_box = {'label': 'marginalium', 'x': 2489.3546875, 'y': 1626.7513790765656, 'w': 444.425,
                    'h': 475.1177294402923}
        scan_box = im_base.Box(scan_box['x'], scan_box['y'], scan_box['w'], scan_box['h'], label=scan_box['label'])
        thumb_box = im_base.scan_box_to_thumb_box(selection, scan_box)
        new_scan_box = im_base.thumb_box_to_scan_box(selection, thumb_box)
        for attr in ['label', 'x', 'y', 'w', 'h']:
            with self.subTest(attr=attr):
                self.assertEqual(scan_box.__getattribute__(attr), new_scan_box.__getattribute__(attr))
        self.assertEqual(scan_box, new_scan_box)

