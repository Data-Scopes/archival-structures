import unittest

import archival_structures.clustering.peaks as peak_clustering


class TestPeakSelection(unittest.TestCase):

    def setUp(self) -> None:
        self.lw_vals = [
            0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0,
            550.0, 600.0, 650.0, 700.0, 750.0, 800.0, 850.0, 900.0, 950.0, 1000.0,
            1050.0,  1100.0, 1150.0, 1200.0, 1250.0, 1300.0, 1350.0, 1400.0, 1450.0, 1500.0,
            1550.0, 1600.0, 1650.0, 1700.0, 1750.0, 1800.0, 1850.0, 1900.0, 1950.0, 2000.0,
            2050.0, 2100.0, 2150.0, 2200.0, 2250.0
        ]
        self.lw_dist = [
            1968, 4678, 1981, 3656, 2427, 1464, 1252, 1266, 1336, 1307,
            1186, 890, 978, 923, 808, 710, 666, 543, 516, 417,
            483, 502, 476, 459, 402, 358, 317, 368, 526, 961,
            985, 369, 210, 365, 1683, 8064, 7344, 3163, 660, 17,
            2, 8, 1, 0, 1, 2
        ]
        values_lists = [[x] * y for x, y in zip(self.lw_vals, self.lw_dist)]
        self.values = [val for val_list in values_lists for val in val_list]
        self.cluster_peak = peak_clustering

    def test_compute_peaks(self):
        peaks = self.cluster_peak.compute_peaks(self.values, width_bin_size=50)
        self.assertEqual(9, len(peaks))

    def test_select_peaks_with_min_prominence(self):
        peaks = peak_clustering.compute_peaks(self.values, width_bin_size=50,
                                              min_prominence=100)
        self.assertEqual(4, len(peaks))

    def test_select_peaks_with_min_prominence_frac(self):
        peaks = peak_clustering.compute_peaks(self.values, width_bin_size=50,
                                              min_prominence_frac=0.25)
        self.assertEqual(6, len(peaks))

    def test_adjust_peaks(self):
        peaks = peak_clustering.compute_peaks(self.values, width_bin_size=50,
                                              min_prominence_frac=0.25)
        peaks = peak_clustering.adjust_peak_widths(peaks, width_bin_size=50, decimals=0)
        self.assertEqual((100, 150), (peaks[0].base_right, peaks[1].base_left))
