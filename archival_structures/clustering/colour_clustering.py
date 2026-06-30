"""
colour_clustering.py
====================
Colour-based clustering of archival scan images using PageXML transcriptions.

For each image/PageXML pair this module extracts two colour signatures:
  - background colour: pixels outside all TextRegion polygons
  - text (ink) colour:  foreground pixels inside TextLine polygons, separated from
                        paper by multiotsu thresholding plus a connected-component
                        check (see `find_ink_luminosity_class`)

The same ink-luminosity thresholds can also be tested against a page's untranscribed
`empty_regions` to flag candidate missing-transcription areas -- see
`find_missing_text_candidates`.

Each signature is a compact feature vector encoding:
  - the LAB values of the two most dominant colours (by pixel count)
  - a uniformity score (proportion of pixels in the single dominant colour)
  - a two-page flag (background colours are perceptually far apart)
  - an inverted flag (background is darker than text)

The module then clusters all images along both dimensions independently
using HDBSCAN, and provides UMAP projections for visualisation.

Dependencies
------------
    pip install pagexml-tools scikit-learn scikit-image hdbscan umap-learn opencv-python

Usage
-----
    pairs = [
        {"image_path": "path/to/image.jpg", "pagexml_path": "path/to/page.xml"},
        ...
    ]
    results = run_colour_clustering(pairs)
    # results is a list of dicts, one per image, with feature vectors and cluster labels
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import cv2
import hdbscan
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pagexml.model.physical_document_model as pdm
import skimage
import umap.umap_ as umap
from pagexml.helper.spatial_helper import make_empty_regions
from pagexml.parser import parse_pagexml_file
from scipy.ndimage import binary_fill_holes, label as connected_components
from scipy.spatial.distance import cdist, pdist, squareform
from skimage.color import rgb2lab
from skimage.filters import threshold_multiotsu
from sklearn.cluster import KMeans

from archival_structures.clustering.colour_quantisation import cluster_main_image_colours
from archival_structures.utils.image_utils import rescale_pagexml_scan_to_image
from archival_structures.utils.image_utils import load_image  # project import


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of pixels randomly sampled per mask for k-means (balances speed vs fidelity)
N_PIXEL_SAMPLES = 8_000

# Number of dominant colours extracted per mask
N_DOMINANT_COLOURS = 2

# Perceptual distance threshold (ΔE in LAB) above which two background
# colour centroids are considered distinct enough to flag a two-page opening
TWO_PAGE_DELTA_E_THRESHOLD = 20.0

# Uniformity score below which an image is considered to have a split background
TWO_PAGE_UNIFORMITY_THRESHOLD = 0.70

# HDBSCAN minimum cluster size (tune to your dataset size)
HDBSCAN_MIN_CLUSTER_SIZE = 5


# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------


def show_cluster_pixels(clusterer: KMeans, image: np.ndarray, label: int):
    """Plot `image` (LAB) with pixels belonging to k-means cluster `label` shown in that
    cluster's actual colour, and every other pixel in black or red (whichever contrasts more
    with the cluster colour)."""
    h, w, _ = image.shape
    image_array = image.reshape(h*w, -1)
    labels = clusterer.predict(image_array)
    label_colour = clusterer.cluster_centers_[label]
    print(f"label_colour: {label_colour}")
    if label_colour[0] > 50:
        rest_colour = [0.0, 0.0, 0.0]
    else:
        rest_colour = [100.0, 0.0, 0.0]
    focus = np.where(labels.reshape(h, w, -1) == label, label_colour, rest_colour)
    focus = skimage.color.lab2rgb(focus)
    plt.imshow(focus)



# ---------------------------------------------------------------------------
# Mask construction
# ---------------------------------------------------------------------------


def _polygon_to_mask(points: list[tuple[int, int]], shape: tuple[int, int]) -> np.ndarray:
    """
    Rasterise a polygon defined by (x, y) tuples into a boolean mask.

    Parameters
    ----------
    points:
        List of (x, y) pixel coordinates from .coords.points.
    shape:
        (height, width) of the target image.

    Returns
    -------
    Boolean mask of shape `shape`, True inside the polygon.
    """
    h, w = shape
    mask = np.zeros((h, w), dtype=np.uint8)
    pts = np.array(points, dtype=np.int32).reshape((-1, 1, 2))
    cv2.fillPoly(mask, [pts], color=1)
    # Fill any holes that cv2.fillPoly may leave for complex polygons
    mask = binary_fill_holes(mask).astype(bool)
    return mask


def build_masks(
    image: np.ndarray,
    scan: pdm.PageXMLScan,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Build a background mask and a raw text-region mask from a PageXMLScan.

    Background mask  = pixels that lie outside every TextRegion polygon.
    Text-region mask   = pixels that lie inside any TextRegion polygon
                         but outside any TextLine polygon.
    Text-line mask   = pixels that lie inside any TextLine polygon.
                       Ink vs paper separation is done downstream (see
                       `extract_ink_pixels`) so that the inverted-page case
                       can be handled correctly.

    Parameters
    ----------
    image:
        RGB image array as returned by `load_image`.
    scan:
        PageXMLScan object returned by `parse_pagexml_file`.

    Returns
    -------
    background_mask : boolean array, True where background pixels are
    textregion_mask   : boolean array, True where text-region pixels are
    textline_mask   : boolean array, True where text-line pixels are
    """
    h, w = image.shape[:2]
    region_union = np.zeros((h, w), dtype=bool)
    textline_union = np.zeros((h, w), dtype=bool)

    for region in scan.get_textual_regions():
        if region.coords and region.coords.points:
            region_union |= _polygon_to_mask(region.coords.points, (h, w))

        for line in region.get_lines():
            if line.coords and line.coords.points:
                textline_union |= _polygon_to_mask(line.coords.points, (h, w))

    line_mask = textline_union
    region_mask = np.logical_xor(region_union, textline_union)
    background_mask = ~region_union
    return background_mask, region_mask, line_mask


# ---------------------------------------------------------------------------
# Ink / paper separation inside text lines
# ---------------------------------------------------------------------------
#
# Earlier versions of this module decided ink-vs-paper polarity ("is this page
# inverted, i.e. light text on a dark background?") from a single global comparison
# of mean luminosity between background and text-line pixels. That comparison is
# fragile: a small light sticker or stain on an otherwise brownish/dark page can
# pull the text-line mean luminosity up enough to flip the polarity decision for
# the entire page.
#
# `find_ink_luminosity_class` below avoids that by not deciding polarity at all.
# It fits multiotsu thresholds on the luminosity of confirmed text-line pixels,
# splitting them into a handful of luminosity classes, then picks whichever class
# breaks into many small connected components -- the signature of genuine ink,
# which fragments into one blob per glyph/stroke -- rather than whichever class is
# darkest or lightest. A sticker or stain instead shows up as one or a few large
# components, regardless of how bright or dark it is, so it is not mistaken for ink.


def _component_sizes(mask: np.ndarray) -> np.ndarray:
    """Sizes of all connected components in a boolean mask (4-connectivity)."""
    labelled, n_components = connected_components(mask)
    if n_components == 0:
        return np.array([], dtype=int)
    return np.bincount(labelled.ravel())[1:]  # drop background label 0


def _fit_multiotsu_thresholds(l_channel: np.ndarray, pixel_mask: np.ndarray,
                              n_classes: int) -> np.ndarray | None:
    masked_l = l_channel[pixel_mask]
    if masked_l.size < 100:
        return None
    try:
        return threshold_multiotsu(masked_l, classes=n_classes)
    except ValueError:
        # Not enough distinct luminosity values to split into n_classes
        return None


def score_luminosity_classes(
    l_channel: np.ndarray,
    pixel_mask: np.ndarray,
    thresholds: np.ndarray,
    small_component_frac: float = 0.0005,
) -> list[dict[str, Any]]:
    """
    Per multiotsu luminosity class (as defined by `thresholds`): pixel count, mean
    luminosity, connected-component count, and how many of those components are "small"
    (the many-small-components signature of genuine ink -- see `find_ink_luminosity_class`).

    One dict per class, in class-id order, with keys ``class_id``, ``n_pixels``, ``mean_l``,
    ``n_components``, ``n_small_components``.
    """
    n_classes = len(thresholds) + 1
    total_pixels = int(pixel_mask.sum())
    small_cutoff = max(4, int(total_pixels * small_component_frac))

    class_ids = np.digitize(l_channel[pixel_mask], bins=thresholds)
    scores = []
    for class_id in range(n_classes):
        class_mask = np.zeros_like(pixel_mask)
        class_mask[pixel_mask] = class_ids == class_id
        n_pixels = int(class_mask.sum())
        sizes = _component_sizes(class_mask)
        scores.append({
            "class_id": class_id,
            "n_pixels": n_pixels,
            "mean_l": float(l_channel[class_mask].mean()) if n_pixels else float("nan"),
            "n_components": int(len(sizes)),
            "n_small_components": int((sizes < small_cutoff).sum()),
        })
    return scores


def find_ink_luminosity_class(
    l_channel: np.ndarray,
    pixel_mask: np.ndarray,
    n_classes: int = 3,
    small_component_frac: float = 0.0005,
) -> tuple[np.ndarray, int] | None:
    """
    Fit multiotsu thresholds on `l_channel` restricted to `pixel_mask` (typically a
    page's text-line mask), then identify which resulting luminosity class is ink by
    counting small connected components per class -- ink fragments into many small
    per-glyph blobs, unlike paper or a uniform artefact (sticker, glare, stain), which
    form one or a few large ones regardless of how bright or dark they are.

    Parameters
    ----------
    l_channel:
        LAB L channel of the full image, shape (H, W).
    pixel_mask:
        Boolean mask of pixels to fit thresholds on, shape (H, W).
    n_classes:
        Number of multiotsu luminosity classes to split into.
    small_component_frac:
        Components smaller than this fraction of `pixel_mask`'s pixel count count
        as "small" for the purposes of picking the ink class.

    Returns
    -------
    (thresholds, ink_class_id), or None if there are too few pixels to threshold
    reliably. `thresholds` can be reused on other masks via `classify_by_luminosity`
    to test whether they contain ink-like pixels, without refitting multiotsu.
    """
    thresholds = _fit_multiotsu_thresholds(l_channel, pixel_mask, n_classes)
    if thresholds is None:
        return None

    scores = score_luminosity_classes(l_channel, pixel_mask, thresholds, small_component_frac)
    best = max(scores, key=lambda s: s["n_small_components"])
    return thresholds, best["class_id"]


def find_ink_like_classes(
    l_channel: np.ndarray,
    pixel_mask: np.ndarray,
    n_classes: int = 4,
    small_component_frac: float = 0.0005,
    min_small_component_density: float = 0.15,
) -> tuple[np.ndarray, list[dict[str, Any]]] | None:
    """
    Like `find_ink_luminosity_class`, but returns *every* multiotsu class that looks ink-like,
    not just the single best one.

    A class counts as ink-like if at least `min_small_component_density` of its own pixels sit
    in small components -- this has to be a *density* (fraction of the class's own pixels), not
    an absolute count: on real handwritten text at typical thumbnail resolution, even the
    dominant paper-coloured band accumulates hundreds of small components from paper grain and
    anti-aliasing once it has enough total pixels, so an absolute count doesn't separate ink
    from paper. The density does -- empirically, paper-like bands sit around 1-2%, ink-like
    bands around 25-45%, validated against real scans from `NL-AsdSAA_89_3.1`.

    Note that a single ink colour typically still produces 2-3 ink-like classes here (the
    fade from dark ink core to paper through anti-aliasing naturally splits into several
    multiotsu bands) -- so the *count* of ink-like classes on its own is a weak signal for "this
    page has multiple ink colours". See `score_multi_colour_text`, which combines this with a
    chroma-spread check that discriminates much better in practice.

    Returns
    -------
    (thresholds, ink_like_scores), where `ink_like_scores` is the subset of
    `score_luminosity_classes`'s output meeting `min_small_component_density`, sorted by
    `n_small_components` descending. None if there are too few pixels to threshold reliably.
    """
    thresholds = _fit_multiotsu_thresholds(l_channel, pixel_mask, n_classes)
    if thresholds is None:
        return None

    scores = score_luminosity_classes(l_channel, pixel_mask, thresholds, small_component_frac)
    ink_like = sorted(
        (s for s in scores
         if s["n_pixels"] and s["n_small_components"] / s["n_pixels"] >= min_small_component_density),
        key=lambda s: -s["n_small_components"])
    return thresholds, ink_like


def classify_by_luminosity(l_channel: np.ndarray, pixel_mask: np.ndarray,
                           thresholds: np.ndarray) -> np.ndarray:
    """Digitize `l_channel` pixels within `pixel_mask` using pre-fit `thresholds`
    (from `find_ink_luminosity_class`), returning the class id per masked pixel in
    the same flattened order as `l_channel[pixel_mask]`."""
    return np.digitize(l_channel[pixel_mask], bins=thresholds)


def extract_ink_pixels(image_lab: np.ndarray, textline_mask: np.ndarray,
                       n_classes: int = 3) -> np.ndarray:
    """
    Boolean mask of ink pixels within `textline_mask`, found via
    `find_ink_luminosity_class` (multiotsu + connected-component shape, robust to
    page polarity and to small bright/dark artefacts). Returns `textline_mask`
    unchanged if there are too few pixels to threshold reliably.
    """
    l_channel = image_lab[:, :, 0]  # L in [0, 100]
    fit = find_ink_luminosity_class(l_channel, textline_mask, n_classes=n_classes)
    if fit is None:
        return textline_mask.copy()
    thresholds, ink_class_id = fit
    class_ids = classify_by_luminosity(l_channel, textline_mask, thresholds)
    ink_mask = np.zeros_like(textline_mask)
    ink_mask[textline_mask] = class_ids == ink_class_id
    return ink_mask


def score_multi_colour_text(
    image_lab: np.ndarray,
    textline_mask: np.ndarray,
    n_luminosity_classes: int = 4,
    small_component_frac: float = 0.0005,
    min_small_component_density: float = 0.15,
) -> dict[str, Any] | None:
    """
    Screening signal for whether a page's text-line pixels look like they contain more than one
    ink colour -- meant for ranking/shortlisting pages for a human look, not a final verdict.

    Two complementary measurements:
      - ``n_ink_like_luminosity_classes`` (`find_ink_like_classes`): how many multiotsu
        luminosity bands show the many-small-connected-components ink signature. Kept for
        transparency, but on its own this rarely discriminates -- a single ink colour's fade
        from dark core to paper through anti-aliasing typically produces 2-3 such bands already.
      - ``ink_chroma_spread``: standard deviation of the LAB a/b chroma channels among pixels
        classified as ink by a clean binary ink/paper split (`find_ink_luminosity_class` with
        2 classes). Different ink colours (not just antialiasing shades of one ink) separate in
        the a/b plane in a way lightness shading alone does not, so this discriminates better in
        practice -- validated against real scans from `NL-AsdSAA_89_3.1`.

    Returns None if there are too few text-line pixels to fit thresholds reliably.
    """
    l_channel = image_lab[:, :, 0]

    multi_fit = find_ink_like_classes(l_channel, textline_mask, n_classes=n_luminosity_classes,
                                      small_component_frac=small_component_frac,
                                      min_small_component_density=min_small_component_density)
    n_ink_like = len(multi_fit[1]) if multi_fit is not None else 0

    binary_fit = find_ink_luminosity_class(l_channel, textline_mask, n_classes=2,
                                           small_component_frac=small_component_frac)
    if binary_fit is None:
        return None
    thresholds, ink_class_id = binary_fit
    class_ids = classify_by_luminosity(l_channel, textline_mask, thresholds)
    ink_mask = np.zeros_like(textline_mask)
    ink_mask[textline_mask] = class_ids == ink_class_id
    n_ink_pixels = int(ink_mask.sum())
    if n_ink_pixels == 0:
        return None

    a_std = float(image_lab[:, :, 1][ink_mask].std())
    b_std = float(image_lab[:, :, 2][ink_mask].std())

    return {
        "n_ink_pixels": n_ink_pixels,
        "n_ink_like_luminosity_classes": n_ink_like,
        "ink_chroma_a_std": a_std,
        "ink_chroma_b_std": b_std,
        "ink_chroma_spread": float(np.hypot(a_std, b_std)),
    }


# ---------------------------------------------------------------------------
# Missing-transcription detection
# ---------------------------------------------------------------------------
#
# `scan.empty_regions` (from `pagexml.helper.spatial_helper.make_empty_regions`) marks the
# space on a page not covered by any transcribed TextRegion -- usually genuinely blank, but
# sometimes containing text that the ATR pipeline missed entirely (e.g. text in the margin,
# at an unusual orientation, or in a colour the recognizer wasn't trained on). The functions
# below test each empty region against the page's own confirmed ink-luminosity thresholds
# (from `find_ink_luminosity_class`, fit on real transcribed text lines) to flag regions worth
# a human look.


def find_missing_text_candidates(
    image_lab: np.ndarray,
    scan: pdm.PageXMLScan,
    textline_mask: np.ndarray | None = None,
    bg_mask: np.ndarray | None = None,
    n_classes: int = 3,
    small_component_frac: float = 0.0005,
    min_region_pixels: int = 200,
    ink_fraction_ratio: float = 1.5,
    min_ink_fraction: float = 0.05,
) -> list[dict[str, Any]]:
    """
    Flag `scan.empty_regions` whose pixels look like genuine ink rather than blank paper.

    Ink-luminosity thresholds are fit once on the page's confirmed text-line pixels (not on
    each empty region individually -- an empty region may have too few or no ink pixels of its
    own to fit thresholds on reliably), then reused to classify each empty region's pixels.

    On low-resolution images (e.g. 300px-wide thumbnails, where individual ink strokes alias
    into noise-sized fragments), compression/paper-texture noise pushes a non-trivial, fairly
    uniform fraction of pixels in *every* background pixel into the ink class -- a fixed
    absolute cutoff then flags almost everything. To stay robust at low resolution, a region is
    only flagged if its ink_fraction clearly exceeds the page's own overall background
    ink_fraction (computed once across all non-text pixels, not per empty-region fragment --
    `pagexml.helper.spatial_helper.make_empty_regions` often chops one real area into many small
    fragments, and comparing a fragment against other nearby fragments of the *same* area
    contaminates that comparison). This also means a two-page opening scan should be split into
    its verso/recto pages first (see `archival_structures.analysis.opening_detection`) before
    calling this, if the two sides have differently-toned backgrounds -- otherwise the
    background baseline mixes two different papers and dilutes the contrast on each side.

    Parameters
    ----------
    image_lab:
        Image converted to LAB colour space, shape (H, W, 3).
    scan:
        PageXMLScan with `empty_regions` populated (see
        `pagexml.helper.spatial_helper.make_empty_regions`).
    textline_mask, bg_mask:
        Boolean masks of confirmed TextLine pixels and of background (non-TextRegion) pixels,
        shape (H, W). Computed via `build_masks` if not given.
    n_classes, small_component_frac:
        Passed through to `find_ink_luminosity_class`.
    min_region_pixels:
        Empty regions smaller than this are skipped -- their ink_fraction estimate is too
        noisy at low pixel counts, especially on low-resolution thumbnails.
    ink_fraction_ratio:
        A region is flagged if its ink_fraction is at least this many times the page's overall
        background ink_fraction.
    min_ink_fraction:
        Absolute floor a region's ink_fraction must also clear, so a page where the background
        is already near-zero noise doesn't get a region flagged for being marginally noisier
        than that (still essentially blank) background.

    Returns
    -------
    List of dicts, one per flagged region, with keys ``region_id``, ``box`` (left, top,
    right, bottom), ``ink_fraction``, ``n_pixels``, and the page's ``background_ink_fraction``
    and ``required_ink_fraction`` used to flag it. Empty if no region is flagged, if there
    aren't enough text-line pixels to fit ink thresholds, or if no region clears
    `min_region_pixels`.
    """
    if not scan.empty_regions:
        return []

    if textline_mask is None or bg_mask is None:
        built_bg_mask, _, built_textline_mask = build_masks(image_lab, scan)
        textline_mask = built_textline_mask if textline_mask is None else textline_mask
        bg_mask = built_bg_mask if bg_mask is None else bg_mask

    l_channel = image_lab[:, :, 0]
    fit = find_ink_luminosity_class(l_channel, textline_mask, n_classes=n_classes,
                                    small_component_frac=small_component_frac)
    if fit is None:
        return []
    thresholds, ink_class_id = fit

    if bg_mask.any():
        bg_class_ids = classify_by_luminosity(l_channel, bg_mask, thresholds)
        background_ink_fraction = float((bg_class_ids == ink_class_id).mean())
    else:
        background_ink_fraction = 0.0
    required = max(background_ink_fraction * ink_fraction_ratio, min_ink_fraction)

    h, w = image_lab.shape[:2]
    candidates = []
    for region in scan.empty_regions:
        if not region.coords or not region.coords.points:
            continue
        region_mask = _polygon_to_mask(region.coords.points, (h, w))
        n_pixels = int(region_mask.sum())
        if n_pixels < min_region_pixels:
            continue
        class_ids = classify_by_luminosity(l_channel, region_mask, thresholds)
        ink_fraction = float((class_ids == ink_class_id).mean())
        if ink_fraction >= required:
            candidates.append({
                "region_id": region.id,
                "box": (region.coords.left, region.coords.top,
                       region.coords.right, region.coords.bottom),
                "ink_fraction": ink_fraction,
                "n_pixels": n_pixels,
                "background_ink_fraction": background_ink_fraction,
                "required_ink_fraction": required,
            })
    return candidates


# ---------------------------------------------------------------------------
# Dominant-colour extraction
# ---------------------------------------------------------------------------


def _sample_pixels(
    image_lab: np.ndarray,
    mask: np.ndarray | None = None,
    n_samples: int = N_PIXEL_SAMPLES,
    rng: np.random.Generator | None = None,
) -> np.ndarray | None:
    """
    Randomly sample up to `n_samples` LAB pixels from `image_lab` where
    `mask` is True.

    Returns
    -------
    Array of shape (n, 3) or None if the mask contains no pixels.
    """
    if rng is None:
        rng = np.random.default_rng()

    if mask is None:
        h, w, _ = image_lab.shape
        pixels = image_lab.reshape((h*w,3))
    else:
        pixels = image_lab[mask]  # shape: (N, 3)
    if pixels.shape[0] == 0:
        return None

    if pixels.shape[0] > n_samples:
        idx = rng.choice(pixels.shape[0], size=n_samples, replace=False)
        pixels = pixels[idx]

    return pixels


def determine_number_of_colours(image_lab: np.ndarray, rng: np.random.Generator | None = None,
                                min_colours: int = 0, max_colours: int = 256) -> int:
    """Estimate how many distinct colours `image_lab` has by HDBSCAN-clustering a random
    pixel sample and counting non-noise clusters, clamped to `[min_colours, max_colours]`. For
    use as the `n_colours` argument to `classify_dominant_colours`/`cluster_main_image_colours`
    when that count isn't known in advance."""
    if rng is None:
        rng = np.random.default_rng()

    clusterer = hdbscan.HDBSCAN(min_cluster_size=100, min_samples=100,
                               metric="euclidean", cluster_selection_method="eom")

    img_pixels = _sample_pixels(image_lab, rng=rng, n_samples=N_PIXEL_SAMPLES)
    clusterer.fit(img_pixels)
    n_colours = len([label for label in np.unique(clusterer.labels_) if label != -1])
    if n_colours < min_colours:
        n_colours = min_colours
    if n_colours > max_colours:
        n_colours = max_colours
    return n_colours



def map_bg_ratio(ratio: float, min_ratio: float = 3.0) -> str:
    """Classify a k-means colour cluster from its background-fraction / text-line-fraction
    ratio (`bg_frac / tl_frac`, as computed in `classify_dominant_colours`): `'bg'` if `ratio`
    exceeds `min_ratio` (much more common in the background), `'tl'` if it's below
    `1/min_ratio` (much more common in text lines), else `'unk'` (roughly equally common in
    both, so not clearly either)."""
    if ratio > min_ratio:
        return 'bg'
    elif ratio < (1/min_ratio):
        return 'tl'
    else:
        return 'unk'


def classify_dominant_colours(image_lab: np.ndarray, scan: pdm.PageXMLScan, n_colours: int) -> pd.DataFrame:
    """K-means cluster `image_lab` into `n_colours` colours, merge clusters whose centroids are
    perceptually close (ΔE < 10), then for each surviving cluster report its LAB centroid and
    how its pixels distribute across `scan`'s background/text-region/text-line masks (see
    `build_masks`) -- one row per colour, with `bg_frac`/`tr_frac`/`tl_frac` columns and a
    `colour_focus` label from `map_bg_ratio`. Predates and is superseded for ink/paper
    separation by `find_ink_luminosity_class` (multiotsu + connected-component shape, more
    robust to page polarity and small artefacts) -- kept for whole-image multi-colour
    classification rather than per-line ink extraction."""
    h, w, _ = image_lab.shape
    kmeans, labels = cluster_main_image_colours(image_lab, n_colours)
    cluster_lums = kmeans.cluster_centers_[:, 0]
    cluster_as = kmeans.cluster_centers_[:, 1]
    cluster_bs = kmeans.cluster_centers_[:, 2]
    kmeans.cluster_centers_
    
    image_labels = labels.reshape(h, w, -1)

    bg_mask, tr_mask, tl_mask = build_masks(image_lab, scan)

    distvec = pdist(kmeans.cluster_centers_, metric='euclidean')
    pd.DataFrame(squareform(distvec)).style.format(precision=0).background_gradient(axis=None)
    sq_vec = squareform(distvec)
    map_cluster = {}
    for c1 in np.arange(len(sq_vec)):
        for c2 in np.arange(c1+1, len(sq_vec)):
            if c1 == c2:
                continue
            if sq_vec[c1, c2] >= 10:
                continue
            if c1 in map_cluster:
                map_cluster[c2] = map_cluster[c1]
                image_labels[image_labels == c2] = map_cluster[c1]
            else:
                map_cluster[c2] = c1
                image_labels[image_labels == c2] = c1

    bg_freq = {lid: freq for lid, freq in zip(*np.unique(image_labels[bg_mask], return_counts=True))}
    tl_freq = {lid: freq for lid, freq in zip(*np.unique(image_labels[tl_mask], return_counts=True))}
    tr_freq = {lid: freq for lid, freq in zip(*np.unique(image_labels[tr_mask], return_counts=True))}
    im_freq = {lid: freq for lid, freq in zip(*np.unique(labels, return_counts=True))}

    tl_total = sum(tl_freq.values())
    tr_total = sum(tl_freq.values())
    im_total = sum(im_freq.values())
    bg_total = sum(bg_freq.values())

    rows = []
    for lid in im_freq:
        im_frac = im_freq[lid] / im_total
        if lid not in tl_freq:
            tl_freq[lid] = 0
        if lid not in tr_freq:
            tr_freq[lid] = 0
        if lid not in bg_freq:
            bg_freq[lid] = 0
        tl_frac = tl_freq[lid] / tl_total
        tr_frac = tr_freq[lid] / tr_total
        bg_frac = bg_freq[lid] / bg_total
        row = [
            lid, cluster_lums[lid], cluster_as[lid], cluster_bs[lid], 
            im_freq[lid], bg_freq[lid], tr_freq[lid], tl_freq[lid], im_frac, bg_frac, tr_frac, tl_frac
        ]
        rows.append(row)


    columns = [
        'cluster_id', 'lum', 'a', 'b', 
        'im_freq', 'bg_freq', 'tr_freq', 'tl_freq', 
        'im_frac', 'bg_frac', 'tr_frac', 'tl_frac'
        ]
    df = pd.DataFrame(rows, columns=columns)
    df.apply(lambda row: row['tl_frac'] / max(row['bg_frac'], 1e-5), axis=1)
    df['bg_ratio'] = df.apply(lambda row: max(row['bg_frac'], 1e-5) / max(row['tl_frac'], 1e-5), axis=1)
    df['tr_ratio'] = df.apply(lambda row: max(row['tr_frac'], 1e-5) / max(row['tl_frac'], 1e-5), axis=1)
    df['colour_focus'] = df.bg_ratio.apply(map_bg_ratio)
    return df


def extract_dominant_colours(
    pixels: np.ndarray,
    n_colours: int = N_DOMINANT_COLOURS,
    random_state: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Run k-means on a pixel sample and return the dominant colour centroids
    sorted by decreasing cluster size.

    Parameters
    ----------
    pixels:
        Array of shape (N, 3) in LAB colour space.
    n_colours:
        Number of dominant colours to extract (k in k-means).
    random_state:
        Seed for reproducibility.

    Returns
    -------
    centroids   : array of shape (n_colours, 3), sorted by prevalence
    proportions : array of shape (n_colours,), fractional pixel counts
    """
    if len(pixels) < n_colours:
        # Pad with repetitions if there are too few pixels
        pixels = np.tile(pixels, (n_colours, 1))[:n_colours]

    km = KMeans(n_clusters=n_colours, random_state=random_state, n_init="auto")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        km.fit(pixels)

    labels = km.labels_
    counts = np.bincount(labels, minlength=n_colours).astype(float)
    proportions = counts / counts.sum()

    # Sort by descending cluster size
    order = np.argsort(-proportions)
    centroids = km.cluster_centers_[order]
    proportions = proportions[order]

    return centroids, proportions


# ---------------------------------------------------------------------------
# Feature-vector assembly
# ---------------------------------------------------------------------------


def _delta_e(lab1: np.ndarray, lab2: np.ndarray) -> float:
    """Euclidean distance in LAB space (approximation of ΔE CIE76)."""
    return float(np.linalg.norm(lab1 - lab2))


def build_feature_vector(
    bg_centroids: np.ndarray,
    bg_proportions: np.ndarray,
    ink_centroids: np.ndarray,
    ink_proportions: np.ndarray,
) -> dict[str, Any]:
    """
    Assemble the full feature dictionary for one image, including derived flags.

    The feature vector contains:
      - bg_colour_1, bg_colour_2   : LAB triplets of the two dominant background colours
      - bg_uniformity              : proportion of pixels in the dominant background colour
      - ink_colour_1, ink_colour_2 : LAB triplets of the two dominant ink colours
      - ink_uniformity             : proportion of pixels in the dominant ink colour
      - two_page_flag              : 1 if background colours are perceptually distinct
      - inverted_flag              : 1 if background is darker than ink
      - mixed_ink_flag             : 1 if ink colours are perceptually distinct

    Parameters
    ----------
    bg_centroids   : (2, 3) array of background colour centroids in LAB
    bg_proportions : (2,) array of background colour proportions
    ink_centroids  : (2, 3) array of ink colour centroids in LAB
    ink_proportions: (2,) array of ink colour proportions

    Returns
    -------
    Dictionary with scalar and array values as described above.
    """
    bg_uniformity = float(bg_proportions[0])
    ink_uniformity = float(ink_proportions[0])

    # Two-page flag: background has two perceptually distant colours
    bg_delta_e = _delta_e(bg_centroids[0], bg_centroids[1])
    two_page_flag = int(
        bg_uniformity < TWO_PAGE_UNIFORMITY_THRESHOLD
        or bg_delta_e > TWO_PAGE_DELTA_E_THRESHOLD
    )

    # Mixed-ink flag: text appears in two perceptually distant colours
    ink_delta_e = _delta_e(ink_centroids[0], ink_centroids[1])
    mixed_ink_flag = int(
        ink_uniformity < TWO_PAGE_UNIFORMITY_THRESHOLD
        or ink_delta_e > TWO_PAGE_DELTA_E_THRESHOLD
    )

    # Inverted flag: dominant background colour is darker than dominant ink colour
    # L channel is index 0 in LAB
    inverted_flag = int(bg_centroids[0][0] < ink_centroids[0][0])

    return {
        # Background
        "bg_colour_1": bg_centroids[0].tolist(),
        "bg_colour_2": bg_centroids[1].tolist(),
        "bg_uniformity": bg_uniformity,
        "bg_delta_e": float(bg_delta_e),
        # Ink
        "ink_colour_1": ink_centroids[0].tolist(),
        "ink_colour_2": ink_centroids[1].tolist(),
        "ink_uniformity": ink_uniformity,
        "ink_delta_e": float(ink_delta_e),
        # Flags
        "two_page_flag": two_page_flag,
        "inverted_flag": inverted_flag,
        "mixed_ink_flag": mixed_ink_flag,
    }


# ---------------------------------------------------------------------------
# Per-image processing
# ---------------------------------------------------------------------------


def process_image_pair(
    pair: dict[str, str],
    rng: np.random.Generator | None = None,
) -> dict[str, Any] | None:
    """
    Process a single image / PageXML pair and return its feature dictionary.

    Parameters
    ----------
    pair:
        Dict with keys ``image_path`` and ``pagexml_path``.
    rng:
        Optional random generator for reproducible pixel sampling.

    Returns
    -------
    Feature dictionary (see `build_feature_vector`) augmented with
    ``image_path``, or None if processing failed.
    """

    image_path = pair["image_path"]
    pagexml_path = pair["pagexml_path"]

    try:
        # Load image (load_image converts BGR -> RGB internally, despite the name of cv2.imread)
        image_rgb = load_image(image_path)
        if image_rgb is None:
            raise ValueError(f"load_image returned None for {image_path}")

        # Convert RGB → LAB
        # rgb2lab expects float in [0, 1]
        image_lab = rgb2lab(image_rgb.astype(np.float32) / 255.0)

        # Parse PageXML
        scan = parse_pagexml_file(pagexml_path)

        # rescale scan to image size
        scan = rescale_pagexml_scan_to_image(scan, image_rgb)

        # Build spatial masks
        bg_mask, textregion_mask, textline_mask = build_masks(image_rgb, scan)

        # --- Background colour signature ---
        bg_pixels = _sample_pixels(image_lab, bg_mask, rng=rng)
        if bg_pixels is None or len(bg_pixels) < N_DOMINANT_COLOURS:
            raise ValueError(f"Too few background pixels in {image_path}")
        bg_centroids, bg_proportions = extract_dominant_colours(bg_pixels)

        # --- Ink colour signature (after multiotsu ink/paper separation) ---
        ink_mask = extract_ink_pixels(image_lab, textline_mask)
        ink_pixels = _sample_pixels(image_lab, ink_mask, rng=rng)

        if ink_pixels is None or len(ink_pixels) < N_DOMINANT_COLOURS:
            # Fall back to raw textline pixels if Otsu separation yields too few
            ink_pixels = _sample_pixels(image_lab, textline_mask, rng=rng)

        if ink_pixels is None or len(ink_pixels) < N_DOMINANT_COLOURS:
            raise ValueError(f"Too few ink pixels in {image_path}")

        ink_centroids, ink_proportions = extract_dominant_colours(ink_pixels)

        # Assemble feature vector (inverted_flag is recomputed from centroids here)
        features = build_feature_vector(bg_centroids, bg_proportions, ink_centroids, ink_proportions)
        features["image_path"] = image_path

        return features

    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Skipping {image_path}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def extract_features(
    pairs: list[dict[str, str]],
    seed: int = 42,
) -> list[dict[str, Any]]:
    """
    Extract colour features for a list of image/PageXML pairs.

    Parameters
    ----------
    pairs:
        List of dicts, each with ``image_path`` and ``pagexml_path`` keys.
    seed:
        Random seed for reproducibility.

    Returns
    -------
    List of feature dicts (failed images are silently skipped).
    """
    rng = np.random.default_rng(seed)
    results = []
    n = len(pairs)
    for i, pair in enumerate(pairs):
        print(f"  [{i + 1}/{n}] {Path(pair['image_path']).name}")
        feat = process_image_pair(pair, rng=rng)
        if feat is not None:
            results.append(feat)
    return results


# ---------------------------------------------------------------------------
# Feature matrix helpers
# ---------------------------------------------------------------------------


def features_to_matrix(
    features: list[dict[str, Any]],
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    Flatten the feature dicts into two numeric matrices, one per clustering
    dimension (background colour, ink colour).

    Each row encodes one image as:
        [L1, a1, b1, L2, a2, b2, uniformity, two_page_or_mixed_ink, delta_e]

    The flag columns are scaled so they have a similar magnitude to the LAB
    and uniformity columns and thus influence the clustering proportionally.

    Parameters
    ----------
    features:
        List of feature dicts from `extract_features`.

    Returns
    -------
    bg_matrix  : float array of shape (N, 9) for background clustering
    ink_matrix : float array of shape (N, 9) for ink clustering
    image_paths: list of image paths in the same order as the matrix rows
    """
    image_paths = [f["image_path"] for f in features]

    bg_rows = []
    ink_rows = []

    for f in features:
        # Background row
        bg_rows.append([
            *f["bg_colour_1"],         # L, a, b of dominant bg colour
            *f["bg_colour_2"],         # L, a, b of secondary bg colour
            f["bg_uniformity"],        # [0, 1]
            f["two_page_flag"] * 50,   # scaled binary flag → ~[0, 50]
            f["bg_delta_e"],           # perceptual distance between bg colours
        ])

        # Ink row
        ink_rows.append([
            *f["ink_colour_1"],
            *f["ink_colour_2"],
            f["ink_uniformity"],
            f["mixed_ink_flag"] * 50,
            f["ink_delta_e"],
        ])

    return np.array(bg_rows, dtype=float), np.array(ink_rows, dtype=float), image_paths


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------


def cluster_features(
    matrix: np.ndarray,
    min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
) -> np.ndarray:
    """
    Cluster a feature matrix using HDBSCAN.

    Noise points (label == -1) are images that do not fit any cluster.

    Parameters
    ----------
    matrix:
        Float array of shape (N, D).
    min_cluster_size:
        Minimum number of images to form a cluster.

    Returns
    -------
    Integer array of cluster labels, shape (N,).
    """
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    clusterer.fit(matrix)
    return clusterer.labels_


# ---------------------------------------------------------------------------
# UMAP projection for visualisation
# ---------------------------------------------------------------------------


def project_umap(
    matrix: np.ndarray,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    random_state: int = 42,
) -> np.ndarray:
    """
    Reduce a feature matrix to 2D using UMAP for visualisation.

    Parameters
    ----------
    matrix:
        Float array of shape (N, D).
    n_neighbors:
        UMAP neighbourhood size (higher = more global structure).
    min_dist:
        UMAP minimum distance between embedded points.
    random_state:
        Seed for reproducibility.

    Returns
    -------
    Float array of shape (N, 2).
    """
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        random_state=random_state,
    )
    return reducer.fit_transform(matrix)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_colour_clustering(
    pairs: list[dict[str, str]],
    min_cluster_size: int = HDBSCAN_MIN_CLUSTER_SIZE,
    seed: int = 42,
) -> list[dict[str, Any]]:
    """
    Full pipeline: feature extraction → clustering → UMAP projection.

    Parameters
    ----------
    pairs:
        List of dicts with ``image_path`` and ``pagexml_path`` keys.
    min_cluster_size:
        Passed to HDBSCAN (tune to dataset size).
    seed:
        Random seed for k-means, UMAP, and pixel sampling.

    Returns
    -------
    List of result dicts, one per successfully processed image.  Each dict
    contains all feature values plus:
      - ``bg_cluster``    : background colour cluster label (int; -1 = noise)
      - ``ink_cluster``   : ink colour cluster label
      - ``bg_umap_x/y``  : 2-D UMAP coordinates for the background space
      - ``ink_umap_x/y`` : 2-D UMAP coordinates for the ink space
    """
    print("=== Step 1: Extracting colour features ===")
    features = extract_features(pairs, seed=seed)

    if not features:
        raise RuntimeError("No features could be extracted. Check your input paths.")

    print(f"\n=== Step 2: Clustering {len(features)} images ===")
    bg_matrix, ink_matrix, image_paths = features_to_matrix(features)

    bg_labels = cluster_features(bg_matrix, min_cluster_size=min_cluster_size)
    ink_labels = cluster_features(ink_matrix, min_cluster_size=min_cluster_size)

    print(f"Background clusters: {len(set(bg_labels)) - (1 if -1 in bg_labels else 0)}"
          f" ({(bg_labels == -1).sum()} noise points)")
    print(f"Ink clusters:        {len(set(ink_labels)) - (1 if -1 in ink_labels else 0)}"
          f" ({(ink_labels == -1).sum()} noise points)")

    print("\n=== Step 3: UMAP projections ===")
    bg_umap = project_umap(bg_matrix, random_state=seed)
    ink_umap = project_umap(ink_matrix, random_state=seed)

    # Attach clustering results to feature dicts
    results = []
    for i, feat in enumerate(features):
        feat["bg_cluster"] = int(bg_labels[i])
        feat["ink_cluster"] = int(ink_labels[i])
        feat["bg_umap_x"] = float(bg_umap[i, 0])
        feat["bg_umap_y"] = float(bg_umap[i, 1])
        feat["ink_umap_x"] = float(ink_umap[i, 0])
        feat["ink_umap_y"] = float(ink_umap[i, 1])
        results.append(feat)

    return results


def cluster_spatial_stats(labels: np.array, h: int, w: int, cluster_id):
    """
    labels: 1D array of length h*w (from cv2.kmeans), or 2D (h, w).
    Returns centroid, spread, and ratios that indicate edge-concentration.

    How to read the output:

    spread_ratio_vs_uniform > ~1.1 together with a small centroid_offset_from_image_center → classic "ring around the border" pattern. Pixels are pushed outward, so they spread more than uniform while still averaging to the middle.

    spread_ratio_vs_uniform < ~0.9 and small centroid_offset → compact blob in the middle.
    
    Small spread + large centroid_offset → blob sitting in/near one corner or edge (also "edge" in a looser sense).
    
    mean_edge_distance_norm near 0 → pixels really do sit near image borders; near 0.5–0.7 → they sit toward the middle.
    """
    label_img = labels.reshape(h, w)
    ys, xs = np.where(label_img == cluster_id)
    n = len(ys)
    if n == 0:
        return None

    # Centroid (pixel coords) and normalized to [0, 1]
    cy, cx = ys.mean(), xs.mean()
    cy_n, cx_n = cy / h, cx / w

    # How far the centroid sits from the image center (0 = center, ~0.71 = corner)
    centroid_offset = np.hypot(cy_n - 0.5, cx_n - 0.5)

    # Spread: std dev along each axis, normalized by image dim
    sy_n, sx_n = ys.std() / h, xs.std() / w

    # Radial spread: mean distance of cluster pixels from the cluster centroid,
    # normalized by the image diagonal
    diag = np.hypot(h, w)
    radial = np.hypot(ys - cy, xs - cx).mean() / diag

    # For a uniform distribution over the full image, std along each axis
    # is 1/sqrt(12) ≈ 0.2887 of the image dim. Ratio > 1 => more spread than
    # uniform, which is what you get when mass sits near the borders.
    uniform_std = 1 / np.sqrt(12)
    spread_ratio = 0.5 * (sy_n + sx_n) / uniform_std

    # Mean distance of cluster pixels to the nearest image edge,
    # normalized by min(h, w)/2 (so 0 = on the border, 1 = at the center line)
    d_edge = np.minimum.reduce([ys, h - 1 - ys, xs, w - 1 - xs])
    edge_distance = d_edge.mean() / (min(h, w) / 2)

    return {
        "n_pixels": n,
        "centroid_xy_norm": (float(cx_n), float(cy_n)),
        "centroid_offset_from_image_center": float(centroid_offset),
        "std_norm": (float(sx_n), float(sy_n)),
        "radial_spread_norm": float(radial),
        "spread_ratio_vs_uniform": float(spread_ratio),
        "mean_edge_distance_norm": float(edge_distance),
        "is_edge": float(spread_ratio) > 1.1 or float(edge_distance) < 0.25
    }


if __name__ == "__main__":
    img = cv2.imread("your_image.png")            # (h, w, 3)
    h, w = img.shape[:2]
    Z = img.reshape(-1, 3).astype(np.float32)

    K = 8
    criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 20, 1.0)
    _, labels, centers = cv2.kmeans(Z, K, None, criteria, 5, cv2.KMEANS_PP_CENTERS)
    labels = labels.flatten()                      # shape (h*w,)

    for k in range(K):
        print(k, cluster_spatial_stats(labels, h, w, k))
