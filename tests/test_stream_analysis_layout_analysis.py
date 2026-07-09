from unittest import TestCase

import numpy as np
from scipy.ndimage import gaussian_filter

from archival_structures.stream_analysis.overview.layout_analysis import extract_layout_features


def make_paper(h: int, w: int, colour=(230, 220, 200)) -> np.ndarray:
    """A uniform paper-coloured RGB image."""
    img = np.zeros((h, w, 3), dtype=np.uint8)
    img[:, :] = colour
    return img


def scatter_marks(img: np.ndarray, y_range, x_range, step: int, colour):
    """Place isolated single-pixel marks on `img`, spaced apart so each one is its own
    connected component -- mirrors the ink scatter fixtures used for colour_clustering."""
    for y in range(*y_range, step):
        for x in range(*x_range, step):
            img[y, x] = colour


def add_noise(img: np.ndarray, sigma: float = 6.0, blur: float = 2.0, seed: int = 0) -> np.ndarray:
    """Small, spatially-smoothed per-pixel variation, matching the paper-grain/compression
    noise real scans always have -- gives multiotsu enough distinct luminosity values to split
    into several classes, the same as any real thumbnail would. The blur matters: independent
    per-pixel noise, split by a threshold running through the middle of a large flat region,
    produces a dense checkerboard of alternating single-pixel components that reads as
    "many small components" and gets mistaken for ink -- real paper grain has some spatial
    correlation, which a low-pass-filtered noise field reproduces."""
    rng = np.random.default_rng(seed)
    noise = gaussian_filter(rng.normal(0, sigma, img.shape[:2]), sigma=blur)
    noisy = img.astype(np.float32) + noise[:, :, None]
    return np.clip(noisy, 0, 255).astype(np.uint8)


class TestExtractLayoutFeatures(TestCase):

    def test_blank_page_has_zero_ink_density_and_no_ink_classes(self):
        img = make_paper(150, 150)
        feats = extract_layout_features(img)

        self.assertEqual(0.0, feats["ink_density"])
        self.assertEqual(0, feats["n_ink_luminosity_classes"])
        self.assertEqual(0.0, feats["ink_chroma_spread"])

    def test_uniform_artefact_is_not_counted_as_ink(self):
        """A large uniform dark block (sticker, stain, scan border) should not inflate ink
        density just because it is dark -- it forms one big connected component, not the
        many-small-components signature of genuine ink."""
        img = make_paper(150, 150)
        img[20:60, 20:60] = (90, 90, 90)  # uniform mid-grey block, not scattered marks
        scatter_marks(img, (80, 140), (10, 140), step=4, colour=(15, 15, 15))
        img = add_noise(img)

        feats = extract_layout_features(img)

        self.assertGreater(feats["ink_density"], 0.0)
        # ink density should be far below the block's own area fraction (40*40 / 150*150 ~ 0.071)
        # if the block itself were being counted as ink
        self.assertLess(feats["ink_density"], 0.05)

    def test_two_ink_colours_both_captured_as_separate_luminosity_classes(self):
        img = make_paper(150, 150, colour=(235, 230, 220))
        scatter_marks(img, (5, 70), (5, 145), step=3, colour=(20, 20, 20))
        scatter_marks(img, (75, 145), (5, 145), step=3, colour=(60, 60, 130))
        img = add_noise(img)

        feats = extract_layout_features(img)

        self.assertGreaterEqual(feats["n_ink_luminosity_classes"], 2)
        self.assertGreater(feats["ink_density"], 0.0)

    def test_returns_expected_keys(self):
        img = make_paper(120, 120)
        feats = extract_layout_features(img)

        expected_keys = {
            "width", "height", "aspect_ratio", "is_grayscale", "mean_brightness",
            "colourfulness", "ink_density", "text_density", "text_component_count",
            "large_component_count", "n_ink_luminosity_classes", "ink_chroma_spread",
            "estimated_line_count", "line_coverage", "num_column_blocks",
            "has_ruled_lines", "has_vertical_lines", "has_table_structure",
            "local_variance_norm",
        }
        self.assertEqual(expected_keys, set(feats.keys()))

    def test_grayscale_input_does_not_raise(self):
        img = np.full((100, 100), 200, dtype=np.uint8)
        scatter_marks_2d = img  # 2D grayscale path
        for y in range(10, 90, 4):
            for x in range(10, 90, 4):
                scatter_marks_2d[y, x] = 30
        feats = extract_layout_features(img)
        self.assertTrue(feats["is_grayscale"])
