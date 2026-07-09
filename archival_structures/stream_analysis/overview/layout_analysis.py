"""
Layout feature extraction that works at thumbnail resolution.

Because OCR/HTR is unreliable at low resolution, we extract structural
and statistical features instead:

  - Ink density       : fraction of ink-like pixels (overall content density)
  - Text density      : count of small connected components per area
                        (text = many small blobs; photos = few large ones)
  - Estimated lines   : peaks in the horizontal projection profile
  - Line coverage     : fraction of rows that contain content
  - Column blocks     : number of vertical content columns (layout structure)
  - Ruled lines       : long horizontal/vertical strokes (forms, tables)
  - Local variance    : Laplacian variance → sharpness / texture richness
  - Colour profile    : grayscale vs. colour, mean brightness
  - Aspect ratio      : portrait / landscape / square
  - Image size        : raw pixel dimensions

All features are scale-invariant or explicitly normalised, so they are
comparable across images of different thumbnail sizes.

Ink pixels are found with the same multiotsu + connected-component-shape technique as
`archival_structures.clustering.ink_luminosity` (originally built for PageXML text-line
masks): fit multiotsu luminosity thresholds and keep whichever class(es) fragment into many
small components -- the signature of genuine ink, as opposed to a large uniform region of
paper, a glued-on sheet, or a scan border. Nothing in that technique is PageXML-specific --
`pixel_mask` is just a boolean array -- so here it is applied to the *whole image*, since no
transcription is available to restrict the fit to. Every ink-like class is unioned into the
ink mask (not just the single best one), so pages with genuinely multi-coloured text are not
under-counted -- `n_ink_luminosity_classes` and `ink_chroma_spread` surface that directly as
features. This replaces an earlier single-threshold (adaptive Gaussian) binarisation, which
had no way to tell ink apart from a uniform bright/dark artefact by shape.

`_INK_N_CLASSES`/`_INK_MIN_DENSITY` were validated against `NL-AsdSAA_89_3.1` (heterogeneous
document types, layouts, and -- often within a single scan -- multiple ink colours) and
`NL-HaNA_2.10.50_1` (stable ink colour but variable paper tone, including scans with a
lighter sheet glued on top): the density threshold used for PageXML text-line masks (0.15)
turned out too strict for a whole-image mask at typical thumbnail resolution, where ink
components alias into fewer, larger blobs than on a mask already restricted to text pixels;
0.02 cleanly separated ink-like classes (which sat at 0.03-0.28 across both corpora) from
paper/border classes (which sat at 0.000-0.005) without ever falling back to an empty mask.
"""

import json
import logging
from pathlib import Path

import cv2
import numpy as np
from skimage.color import rgb2lab
from tqdm import tqdm

from archival_structures.clustering.ink_luminosity import classify_by_luminosity, find_ink_like_classes
from archival_structures.stream_analysis.utils.image_loader import collect_image_paths

logger = logging.getLogger(__name__)

# See the module docstring for how these were chosen.
_INK_N_CLASSES = 4
_INK_MIN_DENSITY = 0.02


# ---------------------------------------------------------------------------
# Feature extraction for a single image
# ---------------------------------------------------------------------------

def extract_layout_features(img_array: np.ndarray) -> dict:
    """
    Extract layout features from an RGB numpy array.

    Works reliably at 100–300 px resolution; most metrics degrade
    gracefully even below that.
    """
    h, w = img_array.shape[:2]
    is_color_input = img_array.ndim == 3 and img_array.shape[2] == 3

    # --- Grayscale conversion -------------------------------------------------
    if is_color_input:
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    else:
        gray = img_array.copy()

    # --- Colour profile -------------------------------------------------------
    if is_color_input:
        channel_stds = img_array.std(axis=(0, 1))           # std per R/G/B channel
        is_grayscale = bool(channel_stds.max() < 8)         # channels nearly identical
        mean_brightness = float(img_array.mean() / 255.0)
        # Colourfulness: mean inter-channel deviation
        colourfulness = float(channel_stds.mean() / 255.0)
    else:
        is_grayscale = True
        mean_brightness = float(gray.mean() / 255.0)
        colourfulness = 0.0

    # --- Aspect ratio & size --------------------------------------------------
    aspect_ratio = round(w / h, 4)

    # --- Ink detection (multiotsu + connected-component shape) ---------------
    # See the module docstring for why this replaced single-threshold binarisation.
    if is_color_input:
        image_lab = rgb2lab(img_array.astype(np.float32) / 255.0)
    else:
        image_lab = rgb2lab(np.stack([img_array] * 3, axis=-1).astype(np.float32) / 255.0)
    l_channel = image_lab[:, :, 0]
    whole_image_mask = np.ones((h, w), dtype=bool)

    fit = find_ink_like_classes(l_channel, whole_image_mask, n_classes=_INK_N_CLASSES,
                                min_small_component_density=_INK_MIN_DENSITY)
    ink_mask = np.zeros((h, w), dtype=bool)
    n_ink_luminosity_classes = 0
    ink_chroma_spread = 0.0
    if fit is not None:
        thresholds, ink_like_scores = fit
        n_ink_luminosity_classes = len(ink_like_scores)
        if ink_like_scores:
            ink_class_ids = {s["class_id"] for s in ink_like_scores}
            class_ids = classify_by_luminosity(l_channel, whole_image_mask, thresholds)
            ink_mask[whole_image_mask] = np.isin(class_ids, list(ink_class_ids))
            # Chroma spread among ink pixels: different ink colours (not just anti-aliasing
            # shades of one ink) separate in the LAB a/b plane -- see
            # archival_structures.clustering.ink_luminosity.score_multi_colour_text.
            a_std = float(image_lab[:, :, 1][ink_mask].std())
            b_std = float(image_lab[:, :, 2][ink_mask].std())
            ink_chroma_spread = float(np.hypot(a_std, b_std))

    binary = (ink_mask.astype(np.uint8)) * 255

    # --- Ink density ----------------------------------------------------------
    ink_density = float(binary.sum() / (h * w * 255))

    # --- Connected component analysis ----------------------------------------
    # Small components = characters / strokes; large = illustrations / stamps
    num_labels, _, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)

    areas = stats[1:, cv2.CC_STAT_AREA]          # skip background label 0
    img_area = h * w

    if len(areas) > 0:
        # "Text-like" = components smaller than 1 % of image area, larger than noise
        min_area = max(2, img_area * 0.00005)
        max_area = img_area * 0.01
        text_mask = (areas >= min_area) & (areas <= max_area)
        text_component_count = int(text_mask.sum())
        # Normalise to components per thousand pixels so different image sizes compare
        text_density = round(text_component_count / (img_area / 1000), 4)

        # Large components (stamps, logos, illustrations)
        large_mask = areas > img_area * 0.02
        large_component_count = int(large_mask.sum())
    else:
        text_density = 0.0
        text_component_count = 0
        large_component_count = 0

    # --- Horizontal projection profile → line estimation ---------------------
    # Dilate horizontally to merge characters on the same text line
    kernel_width = max(3, w // 15)
    kernel_h = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_width, 1))
    dilated_h = cv2.dilate(binary, kernel_h)
    row_sums = dilated_h.sum(axis=1).astype(float)

    if row_sums.max() > 0:
        threshold = row_sums.max() * 0.25
        in_line = row_sums > threshold
        transitions = np.diff(in_line.astype(np.int8))
        estimated_line_count = int((transitions == 1).sum())
        line_coverage = round(float(in_line.mean()), 4)
    else:
        estimated_line_count = 0
        line_coverage = 0.0

    # --- Vertical column structure -------------------------------------------
    kernel_height = max(3, h // 15)
    kernel_v = cv2.getStructuringElement(cv2.MORPH_RECT, (1, kernel_height))
    dilated_v = cv2.dilate(binary, kernel_v)
    col_sums = dilated_v.sum(axis=0).astype(float)

    if col_sums.max() > 0:
        threshold_v = col_sums.max() * 0.25
        in_col = col_sums > threshold_v
        col_transitions = np.diff(in_col.astype(np.int8))
        num_column_blocks = int((col_transitions == 1).sum())
    else:
        num_column_blocks = 0

    # --- Ruled lines (forms, tables) -----------------------------------------
    # Long unbroken horizontal strokes
    min_line_len = max(w // 4, 10)
    kernel_long_h = cv2.getStructuringElement(cv2.MORPH_RECT, (min_line_len, 1))
    horizontal_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_long_h)
    has_ruled_lines = bool(horizontal_lines.sum() > 0)

    # Long unbroken vertical strokes
    min_vline_len = max(h // 4, 10)
    kernel_long_v = cv2.getStructuringElement(cv2.MORPH_RECT, (1, min_vline_len))
    vertical_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_long_v)
    has_vertical_lines = bool(vertical_lines.sum() > 0)

    # Combine: likely a form/table if both directions are present
    has_table_structure = bool(has_ruled_lines and has_vertical_lines)

    # --- Local variance (sharpness / texture richness) -----------------------
    # High variance → lots of edges → text-heavy or detailed image
    # Low variance → blank areas or photographic gradients
    laplacian_var = float(cv2.Laplacian(gray, cv2.CV_64F).var())
    # Normalise by image size so comparison across resolutions is meaningful
    local_variance_norm = round(laplacian_var / max(h * w, 1) * 1e6, 4)

    return {
        # Physical attributes
        "width": w,
        "height": h,
        "aspect_ratio": aspect_ratio,
        "is_grayscale": is_grayscale,
        "mean_brightness": round(mean_brightness, 4),
        "colourfulness": round(colourfulness, 4),
        # Content density
        "ink_density": round(ink_density, 4),
        "text_density": text_density,          # text-like components per 1k px
        "text_component_count": text_component_count,
        "large_component_count": large_component_count,
        "n_ink_luminosity_classes": n_ink_luminosity_classes,
        "ink_chroma_spread": round(ink_chroma_spread, 4),
        # Text layout
        "estimated_line_count": estimated_line_count,
        "line_coverage": line_coverage,        # 0–1: fraction of rows with content
        "num_column_blocks": num_column_blocks,
        # Structural elements
        "has_ruled_lines": has_ruled_lines,
        "has_vertical_lines": has_vertical_lines,
        "has_table_structure": has_table_structure,
        # Texture
        "local_variance_norm": local_variance_norm,
    }


# ---------------------------------------------------------------------------
# Batch extraction over all images
# ---------------------------------------------------------------------------

def extract_all_layout_features(config: dict) -> dict[str, dict]:
    """
    Extract layout features for every image and cache to disk.

    Returns a dict mapping image_id → feature dict.
    """
    cache_file = config["layout"]["cache_file"]

    if Path(cache_file).exists():
        logger.info("Loading cached layout features from disk")
        with open(cache_file) as f:
            return json.load(f)

    paths = collect_image_paths(
        config["images"]["input_dir"],
        config["images"]["extensions"],
    )

    results = {}
    for path in tqdm(paths, desc="Extracting layout features"):
        img = cv2.imread(str(path))
        if img is None:
            logger.warning(f"Could not read {path}")
            continue
        # OpenCV loads BGR; convert to RGB for consistency
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        features = extract_layout_features(img_rgb)
        results[str(path)] = features

    Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Extracted layout features for {len(results)} images → {cache_file}")
    return results


def layout_features_to_matrix(
    image_ids: list[str],
    layout_features: dict[str, dict],
) -> np.ndarray:
    """
    Convert the layout feature dicts to a float matrix aligned with image_ids.
    Boolean features are cast to 0/1.

    Returns an (N, F) float32 array.
    """
    numeric_keys = [
        "aspect_ratio", "is_grayscale", "mean_brightness", "colourfulness",
        "ink_density", "text_density", "text_component_count", "large_component_count",
        "n_ink_luminosity_classes", "ink_chroma_spread",
        "estimated_line_count", "line_coverage", "num_column_blocks",
        "has_ruled_lines", "has_vertical_lines", "has_table_structure",
        "local_variance_norm",
    ]

    rows = []
    for img_id in image_ids:
        feats = layout_features.get(img_id, {})
        row = [float(feats.get(k, 0.0)) for k in numeric_keys]
        rows.append(row)

    matrix = np.array(rows, dtype=np.float32)

    # Min-max normalise each feature column so no single feature dominates
    col_min = matrix.min(axis=0)
    col_max = matrix.max(axis=0)
    denom = np.where(col_max - col_min > 0, col_max - col_min, 1.0)
    matrix = (matrix - col_min) / denom

    return matrix
