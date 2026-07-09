from unittest import TestCase

import numpy as np

from archival_structures.clustering.ink_luminosity import find_ink_like_classes, ink_like_mask


def make_luminosity_grid(height: int, width: int, paper_l: float = 80.0) -> np.ndarray:
    return np.full((height, width), paper_l)


def scatter_small_dark_pixels(l_channel: np.ndarray, y_range, x_range, step: int, value: float = 20.0):
    """Place isolated single-pixel "ink" marks on `l_channel`, spaced apart so each one is its
    own connected component."""
    for y in range(*y_range, step):
        for x in range(*x_range, step):
            l_channel[y, x] = value


class TestInkLikeMask(TestCase):

    def test_matches_scattered_dark_pixels_not_uniform_blob(self):
        h, w = 100, 100
        l_channel = make_luminosity_grid(h, w, paper_l=80.0)
        l_channel[10:30, 10:30] = 95.0  # uniform bright artefact -- one big blob, not ink
        scatter_small_dark_pixels(l_channel, (40, 95), (5, 95), step=4)

        mask = np.ones((h, w), dtype=bool)
        ink_mask = ink_like_mask(l_channel, mask, n_classes=3)

        self.assertTrue(np.all(l_channel[ink_mask] == 20.0))
        self.assertGreater(ink_mask.sum(), 0)
        self.assertFalse(ink_mask[15:25, 15:25].any())

    def test_returns_all_false_when_fit_fails(self):
        l_channel = make_luminosity_grid(10, 10)
        mask = np.zeros((10, 10), dtype=bool)
        mask[0, 0] = True

        ink_mask = ink_like_mask(l_channel, mask, n_classes=3)

        self.assertEqual((10, 10), ink_mask.shape)
        self.assertFalse(ink_mask.any())

    def test_returns_all_false_when_no_class_is_ink_like(self):
        h, w = 50, 50
        l_channel = make_luminosity_grid(h, w, paper_l=80.0)
        mask = np.ones((h, w), dtype=bool)

        # a uniform image has no ink-like (many-small-components) class at all
        ink_mask = ink_like_mask(l_channel, mask, n_classes=3)

        self.assertFalse(ink_mask.any())

    def test_mask_is_union_of_every_ink_like_class(self):
        h, w = 100, 100
        l_channel = make_luminosity_grid(h, w, paper_l=90.0)
        scatter_small_dark_pixels(l_channel, (2, 48), (2, 98), step=3, value=15.0)
        scatter_small_dark_pixels(l_channel, (52, 98), (2, 98), step=3, value=50.0)
        mask = np.ones((h, w), dtype=bool)

        fit = find_ink_like_classes(l_channel, mask, n_classes=3)
        self.assertIsNotNone(fit)
        _, ink_like_scores = fit
        self.assertGreaterEqual(len(ink_like_scores), 2)

        combined = ink_like_mask(l_channel, mask, n_classes=3)
        self.assertTrue(np.all(np.isin(l_channel[combined], [15.0, 50.0])))
        self.assertTrue(combined[2:48, 2:98:3].any())
        self.assertTrue(combined[52:98, 2:98:3].any())
