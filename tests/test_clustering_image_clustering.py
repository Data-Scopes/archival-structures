from unittest import TestCase

import numpy as np

from archival_structures.clustering.image_clustering import get_luminosity_peaks


class FakeScan:
    id = 's1'


class TestGetLuminosityPeaks(TestCase):

    def test_region_type_is_passed_through_not_hardcoded(self):
        # two well-separated luminosity clusters so compute_peaks finds two distinct peaks
        dark = np.tile(np.array([30, 30, 30], dtype=np.uint8), (200, 1))
        light = np.tile(np.array([220, 220, 220], dtype=np.uint8), (200, 1))
        rgb_pixels = np.concatenate([dark, light])

        text_rows = get_luminosity_peaks(rgb_pixels, FakeScan(), region_type='text')
        empty_rows = get_luminosity_peaks(rgb_pixels, FakeScan(), region_type='empty')

        self.assertGreater(len(text_rows), 0)
        self.assertTrue(all(row['region_type'] == 'text' for row in text_rows))
        self.assertTrue(all(row['region_type'] == 'empty' for row in empty_rows))

    def test_region_type_defaults_to_empty(self):
        dark = np.tile(np.array([30, 30, 30], dtype=np.uint8), (200, 1))
        light = np.tile(np.array([220, 220, 220], dtype=np.uint8), (200, 1))
        rgb_pixels = np.concatenate([dark, light])

        rows = get_luminosity_peaks(rgb_pixels, FakeScan())
        self.assertTrue(all(row['region_type'] == 'empty' for row in rows))
