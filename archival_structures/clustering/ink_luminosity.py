"""
ink_luminosity.py
==================
Mask-agnostic multiotsu ink/paper separation.

Earlier versions of `colour_clustering.py` decided ink-vs-paper polarity ("is this page
inverted, i.e. light text on a dark background?") from a single global comparison of mean
luminosity between background and text pixels. That comparison is fragile: a small light
sticker or stain on an otherwise brownish/dark page can pull the mean luminosity up enough to
flip the polarity decision for the entire page.

The functions here avoid that by not deciding polarity at all. They fit multiotsu thresholds on
the luminosity of a given `pixel_mask`, splitting it into a handful of luminosity classes, then
identify which class(es) look like ink by counting small connected components -- ink fragments
into many small per-glyph blobs, unlike paper or a uniform artefact (sticker, glare, stain),
which form one or a few large ones regardless of how bright or dark they are.

Nothing here is specific to PageXML. `pixel_mask` is just a boolean array over the image -- it
can come from PageXML TextLine polygons (as `colour_clustering.py` does, restricting the fit to
confirmed text pixels), or from the whole image when no transcription is available (as
`archival_structures.stream_analysis.overview.layout_analysis` does). The same thresholds and
the same ink-likeness test apply either way.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy.ndimage import label as connected_components
from skimage.filters import threshold_multiotsu


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
    Fit multiotsu thresholds on `l_channel` restricted to `pixel_mask`, then identify which
    resulting luminosity class is ink by counting small connected components per class -- ink
    fragments into many small per-glyph blobs, unlike paper or a uniform artefact (sticker,
    glare, stain), which form one or a few large ones regardless of how bright or dark they are.

    Returns a single "best" class. Use `find_ink_like_classes` when a page may have ink in more
    than one luminosity band (e.g. genuinely multi-coloured text) and all of them should count.

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


def ink_like_mask(
    l_channel: np.ndarray,
    pixel_mask: np.ndarray,
    n_classes: int = 4,
    small_component_frac: float = 0.0005,
    min_small_component_density: float = 0.15,
) -> np.ndarray:
    """
    Boolean mask (same shape as `pixel_mask`) of pixels within `pixel_mask` that fall in *any*
    ink-like multiotsu luminosity class, per `find_ink_like_classes`.

    This is the multi-class counterpart of fitting `find_ink_luminosity_class` and keeping only
    its single best class -- use this whenever a page's ink may legitimately span more than one
    luminosity band (e.g. genuinely multi-coloured text; or a single ink colour whose
    anti-aliased fade is being under-counted by a single-class mask).

    Returns an all-False mask of `pixel_mask`'s shape if there are too few pixels to threshold
    reliably (mirroring `find_ink_like_classes` returning None in that case).
    """
    fit = find_ink_like_classes(l_channel, pixel_mask, n_classes=n_classes,
                                small_component_frac=small_component_frac,
                                min_small_component_density=min_small_component_density)
    mask = np.zeros_like(pixel_mask)
    if fit is None:
        return mask
    thresholds, ink_like_scores = fit
    if not ink_like_scores:
        return mask
    ink_class_ids = {s["class_id"] for s in ink_like_scores}
    class_ids = classify_by_luminosity(l_channel, pixel_mask, thresholds)
    mask[pixel_mask] = np.isin(class_ids, list(ink_class_ids))
    return mask


def score_multi_colour_text(
    image_lab: np.ndarray,
    pixel_mask: np.ndarray,
    n_luminosity_classes: int = 4,
    small_component_frac: float = 0.0005,
    min_small_component_density: float = 0.15,
) -> dict[str, Any] | None:
    """
    Screening signal for whether `pixel_mask`'s pixels look like they contain more than one ink
    colour -- meant for ranking/shortlisting pages for a human look, not a final verdict.

    `pixel_mask` is typically a page's confirmed text-line mask (from PageXML), but nothing here
    requires that -- passing a whole-image mask screens for multi-coloured text directly from an
    image with no transcription available, at the cost of the background/paper pixels diluting
    the fit somewhat compared to a mask restricted to known text pixels.

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

    Returns None if there are too few pixels to fit thresholds reliably.
    """
    l_channel = image_lab[:, :, 0]

    multi_fit = find_ink_like_classes(l_channel, pixel_mask, n_classes=n_luminosity_classes,
                                      small_component_frac=small_component_frac,
                                      min_small_component_density=min_small_component_density)
    n_ink_like = len(multi_fit[1]) if multi_fit is not None else 0

    binary_fit = find_ink_luminosity_class(l_channel, pixel_mask, n_classes=2,
                                           small_component_frac=small_component_frac)
    if binary_fit is None:
        return None
    thresholds, ink_class_id = binary_fit
    class_ids = classify_by_luminosity(l_channel, pixel_mask, thresholds)
    ink_mask = np.zeros_like(pixel_mask)
    ink_mask[pixel_mask] = class_ids == ink_class_id
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
