from unittest import TestCase

import numpy as np

import archival_structures.image.image_base as im_base


TEST_IMAGE = 'data/thumbs/NL-AsnDA/NL-AsnDA_0114.11/NL-AsnDA_0114.11_1/thumb-width_300-scan-NL-AsnDA_0114.11_1_0002.jp2.png'

class TestImageBase(TestCase):

    def test_compute_resize_width_height(self):
        max_pixels = 10000
        width, height = 600, 800
        resized_width, resized_height = im_base.compute_resize_width_height(width, height, max_pixels)
        resize_ratio = np.sqrt((width * height) / max_pixels )
        # print(f"resize_ratio: {resize_ratio}\twidth: {width}\theight: {height}\tmax_pixels: {max_pixels}\tresized_width: {resized_width}\tresized_height: {resized_height}")
        self.assertEqual((int(width / resize_ratio), int(height / resize_ratio)), (resized_width, resized_height))

    def test_load_thumbnail_with_max_pixels(self):
        thumb = im_base.load_thumbnail(TEST_IMAGE, max_pixels=10000)
        self.assertEqual((110, 90), thumb.image.size)

    def test_load_thumbnail_as_array_with_max_pixels(self):
        thumb = im_base.load_thumbnail(TEST_IMAGE, as_array=True, max_pixels=10000)
        # cv2 arrays are (height, width, channels), unlike PIL's (width, height) .size --
        # the reverse order of the PIL-based test above is correct, not a typo.
        self.assertEqual((90, 110), thumb.image.shape[:2])