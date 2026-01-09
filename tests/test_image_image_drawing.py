import unittest

import skimage.feature
from PIL import Image

import archival_structures.image.image_drawing as im_draw


class TestSelection(unittest.TestCase):

    def setUp(self) -> None:
        self.image_file = 'data/thumbs/NL-AsnDA_0114.11/1/NL-AsnDA_0114.11_1_0002.png'
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
        selection = im_draw.make_selection(self.im_w, self.im_h, self.image_file)
        self.assertEqual(0, selection.sel_x)

    def test_make_selection_from_row_sets_default_x_y(self):
        selection = im_draw.make_selection_from_row(self.row)
        self.assertEqual(0, selection.sel_x)

    def test_make_selection_from_row_uses_scan_width(self):
        selection = im_draw.make_selection_from_row(self.row)
        self.assertEqual(self.scan_width, selection.sel_width)

    def test_selection_has_correct_width_scale(self):
        selection = im_draw.make_selection_from_row(self.row)
        self.assertEqual(self.scan_width / self.im_w, selection.thumb_width_scale)

    def test_selection_has_page_box(self):
        self.row['x'], self.row['y'] = self.page_recto['x'], self.page_recto['y']
        self.row['width'], self.row['height'] = self.page_recto['width'], self.page_recto['height']
        selection = im_draw.make_selection_from_row(self.row)
        self.assertEqual(self.page_recto['x'] / selection.thumb_width_scale, selection.selection_box[0])
