from unittest import TestCase

import numpy as np
import pagexml.model.physical_document_model as pdm

from archival_structures.clustering.colour_clustering import (
    extract_ink_pixels, find_ink_like_classes, find_ink_luminosity_class,
    find_missing_text_candidates, score_multi_colour_text,
)


def make_luminosity_grid(height: int, width: int, paper_l: float = 80.0) -> np.ndarray:
    return np.full((height, width), paper_l)


def scatter_small_dark_pixels(l_channel: np.ndarray, y_range, x_range, step: int, value: float = 20.0):
    """Place isolated single-pixel "ink" marks on `l_channel`, spaced apart so each one is its
    own connected component."""
    for y in range(*y_range, step):
        for x in range(*x_range, step):
            l_channel[y, x] = value


class TestFindInkLuminosityClass(TestCase):

    def test_picks_scattered_dark_pixels_over_one_big_bright_blob(self):
        # mimics the real failure case: a brownish/dark page with a small light sticker --
        # the sticker is brighter than both ink and paper but forms a single large blob, while
        # genuine ink fragments into many small per-glyph components regardless of polarity
        l_channel = make_luminosity_grid(100, 100, paper_l=80.0)
        l_channel[10:30, 10:30] = 95.0  # one big light sticker blob (not ink)
        scatter_small_dark_pixels(l_channel, (40, 95), (5, 95), step=4)  # scattered dark ink

        mask = np.ones((100, 100), dtype=bool)
        fit = find_ink_luminosity_class(l_channel, mask, n_classes=3)

        self.assertIsNotNone(fit)
        thresholds, ink_class_id = fit
        class_ids = np.digitize(l_channel[mask], bins=thresholds)
        ink_pixel_values = l_channel[mask][class_ids == ink_class_id]
        # the chosen class should be the dark scattered pixels, not the bright sticker
        self.assertTrue(np.all(ink_pixel_values == 20.0))

    def test_returns_none_for_too_few_pixels(self):
        l_channel = make_luminosity_grid(10, 10)
        mask = np.zeros((10, 10), dtype=bool)
        mask[0, 0] = True
        self.assertIsNone(find_ink_luminosity_class(l_channel, mask, n_classes=3))


class TestFindInkLikeClasses(TestCase):

    def test_excludes_paper_class_with_many_pixels_but_low_component_density(self):
        # a paper class can rack up some small components from holes punched by scattered ink,
        # but nowhere near the density of genuine ink -- density, not raw count, must decide
        h, w = 100, 100
        l_channel = make_luminosity_grid(h, w, paper_l=80.0)
        scatter_small_dark_pixels(l_channel, (2, 98), (2, 98), step=3)  # dense scattered ink
        mask = np.ones((h, w), dtype=bool)

        fit = find_ink_like_classes(l_channel, mask, n_classes=2)
        self.assertIsNotNone(fit)
        _, ink_like = fit
        ink_like_means = {round(s["mean_l"]) for s in ink_like}
        self.assertEqual({20}, ink_like_means)


class TestScoreMultiColourText(TestCase):

    def test_higher_chroma_spread_for_two_ink_colours_than_one(self):
        h, w = 100, 100

        def scatter_coloured(l_channel, a_channel, b_channel, y_range, l_val, a_val, b_val):
            for y in range(*y_range, 3):
                for x in range(5, 95, 3):
                    l_channel[y, x] = l_val
                    a_channel[y, x] = a_val
                    b_channel[y, x] = b_val

        l1 = make_luminosity_grid(h, w, paper_l=80.0)
        a1, b1 = np.zeros((h, w)), np.zeros((h, w))
        scatter_coloured(l1, a1, b1, (5, 95), 20.0, 0.0, 0.0)  # one neutral-coloured ink
        one_colour_lab = np.stack([l1, a1, b1], axis=-1)

        l2 = make_luminosity_grid(h, w, paper_l=80.0)
        a2, b2 = np.zeros((h, w)), np.zeros((h, w))
        scatter_coloured(l2, a2, b2, (5, 50), 20.0, 20.0, 20.0)    # brownish ink
        scatter_coloured(l2, a2, b2, (50, 95), 20.0, -20.0, -10.0)  # bluish-black ink
        two_colour_lab = np.stack([l2, a2, b2], axis=-1)

        mask = np.ones((h, w), dtype=bool)
        one_colour_score = score_multi_colour_text(one_colour_lab, mask)
        two_colour_score = score_multi_colour_text(two_colour_lab, mask)

        self.assertIsNotNone(one_colour_score)
        self.assertIsNotNone(two_colour_score)
        self.assertGreater(two_colour_score["ink_chroma_spread"], one_colour_score["ink_chroma_spread"])
        self.assertAlmostEqual(0.0, one_colour_score["ink_chroma_spread"])

    def test_returns_none_for_too_few_pixels(self):
        l_channel = make_luminosity_grid(10, 10)
        mask = np.zeros((10, 10), dtype=bool)
        mask[0, 0] = True
        image_lab = np.stack([l_channel, np.zeros((10, 10)), np.zeros((10, 10))], axis=-1)
        self.assertIsNone(score_multi_colour_text(image_lab, mask))


class TestExtractInkPixels(TestCase):

    def test_ink_mask_matches_scattered_dark_pixels_not_sticker(self):
        h, w = 100, 100
        l_channel = make_luminosity_grid(h, w, paper_l=80.0)
        l_channel[10:30, 10:30] = 95.0
        scatter_small_dark_pixels(l_channel, (40, 95), (5, 95), step=4)

        image_lab = np.zeros((h, w, 3))
        image_lab[:, :, 0] = l_channel
        textline_mask = np.ones((h, w), dtype=bool)

        ink_mask = extract_ink_pixels(image_lab, textline_mask, n_classes=3)

        # all flagged ink pixels should be the scattered dark marks
        self.assertTrue(np.all(l_channel[ink_mask] == 20.0))
        self.assertGreater(ink_mask.sum(), 0)
        # the sticker block must not be classified as ink
        self.assertFalse(ink_mask[15:25, 15:25].any())


class TestFindMissingTextCandidates(TestCase):

    def setUp(self) -> None:
        self.h, self.w = 100, 100
        self.l_channel = make_luminosity_grid(self.h, self.w, paper_l=80.0)

        # confirmed text-line area (rows 0-39): scattered ink, used to fit thresholds
        self.textline_mask = np.zeros((self.h, self.w), dtype=bool)
        self.textline_mask[0:40, :] = True
        scatter_small_dark_pixels(self.l_channel, (2, 38), (2, 98), step=3)

        # empty region 1 (rows 40-69): same ink-like scattered pattern -- should be flagged
        scatter_small_dark_pixels(self.l_channel, (42, 68), (2, 98), step=3)
        # empty region 2 (rows 70-99): left blank -- should not be flagged

        self.image_lab = np.zeros((self.h, self.w, 3))
        self.image_lab[:, :, 0] = self.l_channel

        self.scan = pdm.PageXMLScan(doc_id='s1',
                                    coords=pdm.Coords.coords_from_box_params(0, 0, self.w, self.h))
        self.er_with_ink = pdm.PageXMLRegion(
            doc_id='er_with_ink', doc_type='empty_region',
            coords=pdm.Coords.coords_from_box_params(0, 40, self.w, 30))
        self.er_blank = pdm.PageXMLRegion(
            doc_id='er_blank', doc_type='empty_region',
            coords=pdm.Coords.coords_from_box_params(0, 70, self.w, 30))
        self.scan.empty_regions = [self.er_with_ink, self.er_blank]
        # background = everything outside the confirmed text-line area, i.e. both empty
        # regions -- a real `build_masks` would give the same, since neither is a confirmed
        # TextRegion; whether either one secretly contains ink is exactly what's being tested
        self.bg_mask = np.zeros((self.h, self.w), dtype=bool)
        self.bg_mask[40:100, :] = True

    def test_flags_region_with_ink_like_pixels_not_blank_region(self):
        candidates = find_missing_text_candidates(
            self.image_lab, self.scan, textline_mask=self.textline_mask, bg_mask=self.bg_mask,
            n_classes=2)

        flagged_ids = {c["region_id"] for c in candidates}
        self.assertIn("er_with_ink", flagged_ids)
        self.assertNotIn("er_blank", flagged_ids)

    def test_returns_empty_list_when_no_empty_regions(self):
        self.scan.empty_regions = []
        candidates = find_missing_text_candidates(
            self.image_lab, self.scan, textline_mask=self.textline_mask, bg_mask=self.bg_mask,
            n_classes=2)
        self.assertEqual([], candidates)
