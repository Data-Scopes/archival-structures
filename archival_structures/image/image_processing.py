# !/usr/local/bin/python3
# Source - https://stackoverflow.com/a/51822265
# Posted by Mark Setchell, modified by community. See post 'Timeline' for change history
# Retrieved 2026-01-07, License - CC BY-SA 4.0


"""Thumbnail generation/layout, and region-extraction/clamping for PageXML scans.

`get_region_scan_size`/`select_scan_region_from_thumbnail`/`select_scan_regions_from_thumbnail`
are the actively-used building blocks for cropping a PageXML region out of its scan's
thumbnail (via `archival_structures.image.image_base.ImageSelection`). `make_thumbnail_filename`/
`resize_image`/`make_image_thumbnail`/`make_thumbnail_matrix`/`save_thumbnail_matrix` are
standalone thumbnail-file utilities.

The peak-location analysis from `make_peak_row` through `get_lab_peaks_locations` (whole-image
LAB k-means quantisation + per-dimension luminosity/chroma peak detection) predates and is
considered a dead end by the package author for ink/text-colour classification specifically --
see `archival_structures.clustering.colour_clustering` (multiotsu + connected-component shape,
and chroma spread for multi-colour-text screening) for the current approach.
"""

from __future__ import annotations

import os
from collections import defaultdict, Counter
from pathlib import Path
from typing import List, Tuple
from typing import Dict, Literal, Sequence, Union

import cv2
import numpy as np
import numpy.typing as npt
import pandas as pd
import skimage
import pagexml.model.physical_document_model as pdm
from PIL import Image
from scipy.stats import entropy

import archival_structures.utils.image_utils as im_utils
from archival_structures.clustering.colour_clustering import cluster_main_image_colours
from archival_structures.clustering.colour_quantisation import quantise_image_colours
from archival_structures.clustering.peaks import compute_peaks, Peak
from archival_structures.image.image_base import Scan, Box, ImageSelection, ThumbnailArray
from archival_structures.image.image_base import load_thumbnail, clamp_box
from archival_structures.image.pagexml_bridge import region_to_box


def make_thumbnail_filename(image_filepath: Path, thumb_dir: Path,
                            thumb_width: int = 500, image_format: str = 'png'):
    """Thumbnail path for `image_filepath` in `thumb_dir`, following this codebase's
    `thumb-width_<thumb_width>-scan-<name>.<image_format>` naming convention."""
    scan_name, image_ext = os.path.splitext(image_filepath.name)
    file_name = f"{scan_name}.{image_format}"
    # print((image_filepath.name, scan_name, image_ext, file_name))
    return thumb_dir.joinpath(f"thumb-width_{thumb_width}-scan-{file_name}")


def resize_image(image_fp: Path, thumb_width: int = None, thumb_height: int = None):
    """Load the image at `image_fp` and resize it to `thumb_width` or `thumb_height` (give
    exactly one), preserving aspect ratio."""
    if thumb_width is None and thumb_height is None:
        raise ValueError("thumb_width or thumb_height must be given")
    im = cv2.imread(str(image_fp))
    height, width, _ = im.shape
    if thumb_width is not None:
        reduction_factor = width / thumb_width
        thumb_height = int(height / reduction_factor)
    elif thumb_height is not None:
        reduction_factor = height / thumb_height
        thumb_width = int(width / reduction_factor)
    thumb = cv2.resize(im, (thumb_width, thumb_height))
    return thumb


def select_image_region(im: npt.NDArray, x: int, y: int, w: int = None, h: int = None):
    """Select a region from an image.

    If no width is given, the region is from x to the right-most side.
    Similar for y when no height is given.
    """
    # im.shape order is height, width, color-dimensions
    if w is None:
        w = im.shape[1] - x
    if h is None:
        h = im.shape[0] - y
    return im[y:y+h, x:x+w]


def make_image_thumbnail(image_fp: Path, thumb_fp: Path, thumb_width: int = None, thumb_height: int = None):
    """Resize the image at `image_fp` (`resize_image`) and write it to `thumb_fp`."""
    if thumb_width is None and thumb_height is None:
        raise ValueError("thumb_width or thumb_height must be given")
    thumb = resize_image(image_fp, thumb_width=thumb_width, thumb_height=thumb_height)
    cv2.imwrite(str(thumb_fp), thumb)


def _read_bgr(fp: Path) -> npt.NDArray:
    im = cv2.imread(str(fp), cv2.IMREAD_COLOR)
    if im is None:
        raise FileNotFoundError(f"Could not read image: {fp}")
    return im


def _resize_keep_aspect(im: npt.NDArray, *, width: int | None = None, height: int | None = None) -> npt.NDArray:
    if (width is None) == (height is None):
        raise ValueError("Pass exactly one of width or height.")

    h, w = im.shape[:2]
    if width is not None:
        scale = width / w
        new_w = width
        new_h = max(1, int(round(h * scale)))
    else:
        scale = height / h
        new_h = height
        new_w = max(1, int(round(w * scale)))

    interp = cv2.INTER_AREA if scale < 1 else cv2.INTER_CUBIC
    return cv2.resize(im, (new_w, new_h), interpolation=interp)


def make_thumbnail_matrix(
        image_files: Sequence[str | Path],
        *,
        ncols: int,
        unify: Literal["height", "width"] = "height",
        cell_size: int = 160,                  # common height (if unify="height") or common width (if unify="width")
        pad: int = 8,                          # padding inside each cell (around the thumbnail)
        gap: int = 10,                         # gap between cells
        bg_color_bgr: tuple[int, int, int] = (245, 245, 245),
        align: Literal["center", "topleft"] = "center",
) -> np.ndarray:
    """
    Returns a single BGR image (OpenCV format) with thumbnails laid out in a grid.

    - unify="height": every thumb is resized to `cell_size` pixels high, variable width.
    - unify="width": every thumb is resized to `cell_size` pixels wide, variable height.
    """
    if ncols <= 0:
        raise ValueError("ncols must be > 0")
    if cell_size <= 0:
        raise ValueError("cell_size must be > 0")
    if pad < 0 or gap < 0:
        raise ValueError("pad and gap must be >= 0")

    # 1) Load + resize thumbnails
    thumbs: list[np.ndarray] = []
    for fp in image_files:
        im = _read_bgr(Path(fp))
        if unify == "height":
            thumbs.append(_resize_keep_aspect(im, height=cell_size))
        elif unify == "width":
            thumbs.append(_resize_keep_aspect(im, width=cell_size))
        else:
            raise ValueError("unify must be 'height' or 'width'")

    if not thumbs:
        raise ValueError("No images provided")

    # 2) Compute per-cell dimensions (we use a fixed cell size so the grid is clean)
    cell_w = max(t.shape[1] for t in thumbs) + 2 * pad
    cell_h = max(t.shape[0] for t in thumbs) + 2 * pad

    n = len(thumbs)
    nrows = int(np.ceil(n / ncols))

    out_w = ncols * cell_w + (ncols - 1) * gap
    out_h = nrows * cell_h + (nrows - 1) * gap

    canvas = np.full((out_h, out_w, 3), bg_color_bgr, dtype=np.uint8)

    # 3) Paste each thumb into its cell
    for idx, thumb in enumerate(thumbs):
        r, c = divmod(idx, ncols)

        x0 = c * (cell_w + gap)
        y0 = r * (cell_h + gap)

        if align == "center":
            dx = (cell_w - thumb.shape[1]) // 2
            dy = (cell_h - thumb.shape[0]) // 2
        elif align == "topleft":
            dx = pad
            dy = pad
        else:
            raise ValueError("align must be 'center' or 'topleft'")

        x = x0 + dx
        y = y0 + dy

        canvas[y : y + thumb.shape[0], x : x + thumb.shape[1]] = thumb

    return canvas


def save_thumbnail_matrix(
        image_files: Sequence[str | Path],
        out_file: str | Path,
        *,
        ncols: int,
        unify: Literal["height", "width"] = "height",
        cell_size: int = 160,
) -> None:
    """Build a thumbnail grid (`make_thumbnail_matrix`) from `image_files` and write it to
    `out_file`."""
    grid = make_thumbnail_matrix(
        image_files,
        ncols=ncols,
        unify=unify,
        cell_size=cell_size,
    )
    ok = cv2.imwrite(str(out_file), grid)
    if not ok:
        raise OSError(f"Could not write output image: {out_file}")


def get_region_scan_size(scan: pdm.PageXMLScan, region: pdm.PageXMLRegion) -> Scan:
    """Determine the dimensions of the scan a `region` belongs to.

    Falls back to `region.metadata['scan_width']`/`['scan_height']` when `scan` is
    not given, which callers may have set when they extracted `region` from a scan
    that is no longer at hand (e.g. it was filtered into a separate list)."""
    if scan is not None:
        return Scan(scan.coords.width, scan.coords.height)
    if 'scan_width' in region.metadata and 'scan_height' in region.metadata:
        return Scan(region.metadata['scan_width'], region.metadata['scan_height'])
    raise ValueError("scan must be given if region does not have 'scan_width' and 'scan_height' in metadata")


def select_scan_region_from_thumbnail(scan: pdm.PageXMLScan, region: pdm.PageXMLRegion,
                                      thumbnail: ThumbnailArray) -> ImageSelection:
    """For a given PageXML scan, a region in that scan and a thumbnail of the scan, return the thumbnail selection
    corresponding to the region."""
    scan_size = get_region_scan_size(scan, region)
    region_box = clamp_box(region_to_box(region), scan_size.width, scan_size.height)
    return ImageSelection(scan_size, region_box, thumbnail)


def select_scan_regions_from_thumbnail(scan: pdm.PageXMLScan, regions: List[pdm.PageXMLRegion],
                                       thumb: ThumbnailArray,
                                       resize: Tuple[int, int] = None, as_array: bool = True):
    """Each scan has one thumbnail and multiple regions. This function selects the regions from the scan in
    the corresponding thumbnail. """
    image_selections = []
    for region in regions:
        selection = select_scan_region_from_thumbnail(scan, region, thumb)
        image_selections.append(selection)
    return image_selections


def merge_region_images_to_1d_pixels(region_images: List[npt.NDArray]):
    """Merge a list of region images into a single 1D pixel array. Image regions have different shapes, thus
    are merged into a single list of pixels."""
    # step 1: reshape each image to one dimension
    images_1d = [im.reshape((im.shape[0] * im.shape[1], im.shape[2])) for im in region_images]
    return np.concatenate(images_1d)


def make_peak_row(peak, region_type: str, pixels: List[int], scan_id: int):
    """`peak`'s own fields plus `scan_id`, `region_type`, and how many of `pixels` fall within
    the peak's base range -- one row for a luminosity/chroma-peak summary table."""
    row = peak.__dict__
    row['scan_id'] = scan_id
    row['pixels_in_selection'] = len(pixels)
    row['pixels_in_peak'] = len([p for p in pixels if peak.base_left <= p <= peak.base_right])
    row['pixels_in_peak_frac'] = row['pixels_in_peak'] / len(pixels)
    row['region_type'] = region_type
    return row


def compute_pixel_peaks(pixels: npt.NDArray, min_prominence: int = None, rel_height: float = 0.70,
                             width_bin_size: int = 1, debug: int = 0):
    """Compute the luminosity peaks of the image."""
    if min_prominence is None:
        min_prominence = len(pixels) / 40
    peaks = compute_peaks(pixels, width_bin_size, min_prominence=min_prominence, rel_height=rel_height, debug=debug)
    return peaks


def analyse_image_colour_peaks(thumb: ThumbnailArray, scan: pdm.PageXMLScan, debug: int = 0):
    """Analyse the colour peaks of the image."""
    thumb = im_utils.make_lab_thumb(thumb)
    text_regions = [tr for tr in scan.get_textual_regions() if tr.area > 0]
    empty_selections = select_scan_regions_from_thumbnail(scan, scan.empty_regions, thumb)
    text_selections = select_scan_regions_from_thumbnail(scan, text_regions, thumb)
    pixels_empty = merge_region_images_to_1d_pixels([sel.cropped for sel in empty_selections])
    pixels_text = merge_region_images_to_1d_pixels([sel.cropped for sel in text_selections])
    pixels_all = merge_region_images_to_1d_pixels([thumb.image])
    lum_pixels_empty = pixels_empty[:, 0]
    lum_pixels_text = pixels_text[:, 0]
    peaks_empty = compute_pixel_peaks(lum_pixels_empty, rel_height=0.70, debug=debug)
    peaks_text = compute_pixel_peaks(lum_pixels_text, rel_height=0.30, debug=debug)
    return peaks_empty, peaks_text


def make_peak_value_map(pixel_array: npt.NDArray, peaks: List[Peak]) -> Dict[int, int]:
    """Create a mapping from pixel values within the range of a peak to the peak value.

    A peak has a highest point (x) and a base (left and right or low and high points)
    and all pixels with a value within that range will be mapped to the peak x value.

    This allows you to reduce the variation in an image by mapping all pixels within
    the range of a peak to a single peak value."""
    min_val = pixel_array.min()
    max_val = pixel_array.max()
    val_map = {val: -1 for val in np.arange(min_val, max_val + 1)}
    for pi, peak in enumerate(peaks):
        for val in np.arange(peak.base_left, peak.base_right + 1):
            val_map[val] = peak.peak_x
    return val_map


def cluster_pixels_by_peaks(selections: List[ImageSelection],
                            thumb_mapped: Dict[str, npt.NDArray],):
    """For each LAB dimension and each x/y coordinate covered by `selections`' boxes, count how
    often each peak-cluster id (`thumb_mapped`, from `make_peak_value_map`) occurs at that
    coordinate. Returns `(cluster_dist_x, cluster_dist_y)`: nested `dim -> cluster_id ->
    Counter[coordinate]` distributions, used by `compute_cluster_entropy`/
    `compute_cluster_stats` to check whether a colour cluster is spatially concentrated (e.g.
    near page edges) or spread evenly."""
    cluster_dist_x = defaultdict(lambda: defaultdict(Counter))
    cluster_dist_y = defaultdict(lambda: defaultdict(Counter))

    for selection in selections:
        box = selection.thumb_selection_box

        for dim in 'lab':
            # cluster_ids = set(val_map['all'][dim].values())
            for x in range(box.x, box.x + box.w):
                freqs = Counter(thumb_mapped[dim][:, x])
                for cluster_id in freqs:
                    cluster_dist_x[dim][cluster_id][x] += freqs[cluster_id]

            for y in range(box.y, box.y + box.h):
                freqs = Counter(thumb_mapped[dim][y, :])
                for cluster_id in freqs:
                    cluster_dist_y[dim][cluster_id][y] += freqs[cluster_id]
    return cluster_dist_x, cluster_dist_y


def compute_cluster_entropy(cluster_dist: Counter):
    """KL-divergence (`scipy.stats.entropy`) of `cluster_dist`'s coordinate distribution from
    uniform -- near 0 if the cluster's pixels are spread evenly across the coordinate,
    higher if concentrated."""
    total = sum(cluster_dist.values())
    probs = [x / total for x in cluster_dist.values()]
    uniform = [1 / len(probs) for _ in range(len(probs))]
    return entropy(probs, uniform)


def get_regions(thumb_scan: pdm.PageXMLScan):
    """`thumb_scan`'s regions grouped by type: `'all'` (the whole scan), `'empty'`
    (`thumb_scan.empty_regions`), `'text'` (confirmed text regions)."""
    return {
        'all': [thumb_scan],
        'empty': [er for er in thumb_scan.empty_regions],
        'text': [tr for tr in thumb_scan.get_textual_regions()]
    }


def make_selections(thumb_scan: pdm.PageXMLScan, regions: Dict[str, List[pdm.PageXMLRegion]],
                           thumb_lab: ThumbnailArray):
    """`select_scan_regions_from_thumbnail` applied per region-type group from `get_regions`,
    returning `region_type -> list[ImageSelection]`."""
    selections = {}
    for region_type in regions:
        selections[region_type] = select_scan_regions_from_thumbnail(thumb_scan,
                                                                     regions[region_type],
                                                                     thumb_lab)
    return selections


def make_pixels(selections: Dict[str, List[ImageSelection]]):
    """`merge_region_images_to_1d_pixels` applied per region-type group from `make_selections`,
    returning `region_type -> flat pixel array`."""
    pixels = {}
    for region_type in selections:
        pixels[region_type] = merge_region_images_to_1d_pixels([sel.cropped for sel in selections[region_type]])
    return pixels


def compute_cluster_stats(cluster_dist: Counter):
    """Summary statistics of `cluster_dist`'s coordinate distribution (expanded from its
    counts): `(total, mean, median, min, max, std, n_distinct)`."""
    values = np.array([v for x in cluster_dist for v in [x] * cluster_dist[x]])
    total = len(values)
    mean = np.mean(values)
    std = np.std(values)
    median = np.median(values)
    min = np.min(values)
    max = np.max(values)
    n = len(cluster_dist)
    return total, mean, median, min, max, std, n


def get_lab_peaks_locations(thumb_scan: pdm.PageXMLScan, thumb_rgb: ThumbnailArray,
                            n_colours: int = 32,
                            width_bin_size: int = 1, rel_height: float = 0.5,
                            min_prominence: Union[int, float] = None):
    """Quantise `thumb_rgb` to `n_colours` LAB colours, find luminosity/chroma peaks per LAB
    dimension and region type (`'all'`/`'empty'`/`'text'`), then map every pixel to its peak's
    colour-cluster id and analyse each cluster's spatial distribution (`cluster_pixels_by_peaks`,
    `compute_cluster_entropy`/`compute_cluster_stats`).

    Returns `(peak_df, location_df)`: one row per peak (luminosity/chroma value + region/
    dimension), and one row per colour cluster (spatial spread/entropy stats). Predates and is
    superseded by `archival_structures.clustering.colour_clustering` for ink/text-colour
    classification (see this module's docstring)."""
    region_types = ['all', 'empty', 'text']
    dimensions = ['l', 'a', 'b']
    thumb_lab = im_utils.make_lab_thumb(thumb_rgb)
    thumb_lab.image = thumb_lab.image.astype(int)

    kmeans, labels = cluster_main_image_colours(thumb_lab.image, n_colours)
    h, w, _ = thumb_lab.image.shape
    labels_2d = labels.reshape(h * w, -1)
    thumb_lab.image = quantise_image_colours(thumb_lab.image, n_colours=32)

    if min_prominence is None:
        h, w, _ = thumb_lab.image.shape
        min_prominence = w * h / 1000

    regions = get_regions(thumb_scan)
    selections = make_selections(thumb_scan, regions, thumb_lab)
    pixels = make_pixels(selections)

    peaks = defaultdict(dict)
    val_map = defaultdict(dict)
    for region_type in region_types:
        for dim_idx, dim in enumerate(dimensions):
            peaks[region_type][dim] = compute_peaks(pixels[region_type][:, dim_idx],
                                                    width_bin_size=width_bin_size, rel_height=rel_height,
                                                    min_prominence=min_prominence)
            val_map[region_type][dim] = make_peak_value_map(pixels[region_type][:, dim_idx], peaks[region_type][dim])

    dim = 'l'
    region_type = 'all'
    rows = []
    for region_type in region_types:
        for dim_idx, dim in enumerate(dimensions):
            rd_rows = [peak.__dict__ for peak in peaks[region_type][dim]]
            for row in rd_rows:
                row['region'] = region_type
                row['dim'] = dim
            rows.extend(rd_rows)
    peak_df = pd.DataFrame(rows)

    thumb_mapped = {}
    cluster_freq = {}

    for dim_idx, dim in enumerate(dimensions):
        thumb_dim = thumb_lab.image[:, :, dim_idx]
        thumb_mapped[dim] = np.array([[val_map['all'][dim][col] for col in row] for row in thumb_dim])
        cluster_freq[dim] = Counter(thumb_mapped[dim].flatten())

    cluster_dist_x, cluster_dist_y = cluster_pixels_by_peaks(selections['all'], thumb_mapped)
    rows = []
    for dim in cluster_freq:
        for cid in sorted(cluster_freq[dim]):
            ent_x = compute_cluster_entropy(cluster_dist_x[dim][cid])
            ent_y = compute_cluster_entropy(cluster_dist_y[dim][cid])
            total_x, mean_x, median_x, min_x, max_x, std_x, n_x = compute_cluster_stats(cluster_dist_x[dim][cid])
            total_y, mean_y, median_y, min_y, may_y, std_y, n_y = compute_cluster_stats(cluster_dist_y[dim][cid])
            row = {
                'lab_dim': dim, 'cluster_id': cid, 'freq': cluster_freq[dim][cid],
                'entropy_x': ent_x, 'entropy_y': ent_y,
                'total_x': total_x, 'mean_x': mean_x, 'median_x': median_x, 'min_x': min_x,
                'max_x': max_x, 'std_x': std_x, 'n_x': n_x,
                'total_y': total_y, 'mean_y': mean_y, 'median_y': median_y, 'min_y': min_y,
                'may_y': may_y, 'std_y': std_y, 'n_y': n_y,
            }
            rows.append(row)

    location_df = pd.DataFrame(rows)
    return peak_df, location_df