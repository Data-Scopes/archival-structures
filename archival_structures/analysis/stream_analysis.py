"""Whole-image luminosity categorisation for a stream of scans.

`categorise_luminosities` splits an image's luminosity into `num_classes` multiotsu bands;
`make_luminosity_ranges` then labels each band's size relative to the whole image and flags
two whole-page QC signals: ``inverted`` (the dominant/background band isn't the lightest one,
suggesting light-on-dark text) and ``quarantined`` (no single band dominates enough to
confidently call it the background -- worth a human look). `colour_categorise_image_stream`
runs this over many images, optionally in parallel.

This operates at whole-page granularity (one verdict per image); for line/region-level ink
classification see `archival_structures.clustering.colour_clustering`.
"""

import multiprocessing
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import skimage

from skimage.filters import threshold_multiotsu
from tqdm import tqdm

from archival_structures.datasets import test_pairs
from archival_structures.utils.image_utils import load_image


def make_luminosity_ranges(lums: np.ndarray, regions, thresholds: np.ndarray
                           ) -> Tuple[List[Dict[str, Any]], bool, bool]:
    """For each multiotsu luminosity band in `regions` (as produced by
    `categorise_luminosities`): its luminosity range, pixel count/fraction, and whether it's
    the dominant `'background'` band (>70% of pixels) or `'other'`.

    Returns `(luminosity_ranges, quarantined, inverted)`: `inverted` is True if the dominant
    band isn't the lightest one (light text on a dark background); `quarantined` is True if no
    band reaches the 70% dominance threshold at all (no confident background call -- worth a
    human look)."""
    region_levels, region_sizes = np.unique(regions, return_counts=True)
    lums_ranges = [0] + list(thresholds) + [100]
    inverted = False
    quarantined = False
    max_frac = 0.0
    luminosity_ranges: List[Dict[str, Any]] = []
    for rl, rs in zip(region_levels, region_sizes):
        rf = rs/len(lums)
        if rf > max_frac:
            max_frac = rf
        if rf > 0.7:
            rt = 'background'
            if rl != region_levels[-1]:
                inverted = True
                quarantined = True
        else:
            rt = 'other'
        luminosity_range: Dict[str, Any] = {
            'level': rl, 'num_pixels': rs, 'frac_pixels': rf, 'element_type': rt,
            'range_low': lums_ranges[rl], 'range_high': lums_ranges[rl+1]
        }
        luminosity_ranges.append(luminosity_range)
        #print(row)
    if max_frac < 0.7:
        quarantined = True
    return luminosity_ranges, quarantined, inverted

    
def categorise_luminosities(image: np.ndarray, num_classes: int = 4) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Split `image`'s L channel into `num_classes` multiotsu luminosity bands.

    Returns `(lums, regions, thresholds)`: `lums` is the flattened L channel, `regions` is the
    per-pixel band id (same shape as `image`'s first two dims), `thresholds` are the multiotsu
    cut points."""
    # make 1D array of all luminosities
    h, w, _ = image.shape
    lums = image.reshape(h * w, -1)[:, 0]
    
    # create numpy array of multiotsu thresholds
    thresholds: np.ndarray = threshold_multiotsu(lums, classes=num_classes)
    if not isinstance(thresholds, np.ndarray):
        thresholds = np.array([thresholds])
    
    # generate regions based on the threshold values
    regions = np.digitize(image[:, :, 0], bins=thresholds)
    
    return lums, regions, thresholds


def colour_categorise_image(image_path: Union[str, Path]):
    """Load and luminosity-categorise the image at `image_path` (see `categorise_luminosities`
    and `make_luminosity_ranges`). Returns `(image_labels, image_luminosities)`: a dict of
    whole-image metadata (`width`, `height`, `inverted`, `quarantined`) and a list of
    per-luminosity-band dicts, both tagged with `image_path`."""
    image: np.ndarray = load_image(image_path)
    h, w, _ = image.shape
    image_lab: np.ndarray = skimage.color.rgb2lab(image)
    lums, regions, thresholds = categorise_luminosities(image_lab, num_classes=4)
    image_luminosities, quarantined, inverted = make_luminosity_ranges(lums, regions, thresholds)
    image_labels: Dict[str, Any] = {
        'image_path': image_path, 'width': w, 'height': h, 
        'inverted': inverted, 'quarantined': quarantined
    }
    for image_luminosity in image_luminosities:
        image_luminosity['image_path'] = image_path
    return image_labels, image_luminosities

    
def colour_categorise_image_stream(image_paths: List[Union[str, Path]], num_processes: int = 1):
    """Run `colour_categorise_image` over `image_paths`, optionally in parallel
    (`num_processes` > 1). Returns `(stream_labels, stream_luminosities)`: per-image metadata
    dicts, and a flat list of per-luminosity-band dicts across all images."""
    stream_labels: List[Dict[str, Any]] = []
    stream_luminosities = []
    if num_processes == 1:
        stream_labels, stream_luminosities = zip(*[colour_categorise_image(ip) for ip in image_paths])
    else:
        with multiprocessing.Pool(processes=num_processes) as pool:
            data = list(tqdm(pool.imap(colour_categorise_image, image_paths), total=len(image_paths), desc='colour categorising images in a stream'))
            stream_labels, stream_luminosities = zip(*data)
    stream_luminosities = [image_lum for image_lums in stream_luminosities for image_lum in image_lums]
    return stream_labels, stream_luminosities
        
        
if __name__ == "__main__":
    image_paths = [tp['image_path'] for tp in test_pairs]
    stream_labels, stream_luminosities = colour_categorise_image_stream(image_paths, num_processes=4)
    print(stream_labels)