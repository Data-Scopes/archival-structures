# !/usr/local/bin/python3
# Source - https://stackoverflow.com/a/51822265
# Posted by Mark Setchell, modified by community. See post 'Timeline' for change history
# Retrieved 2026-01-07, License - CC BY-SA 4.0


import os
from pathlib import Path
from typing import List, Tuple

import cv2
import numpy as np
import pagexml.model.physical_document_model as pdm
from PIL import Image

from archival_structures.image.image_base import Scan, Box, ImageSelection, Thumbnail, load_thumbnail, make_selection


def make_thumbnail_filename(image_filepath: Path, thumb_dir: Path,
                            thumb_width: int = 500, image_format: str = 'png'):
    scan_name, image_ext = os.path.splitext(image_filepath.name)
    file_name = f"{scan_name}.{image_format}"
    # print((image_filepath.name, scan_name, image_ext, file_name))
    return thumb_dir.joinpath(f"thumb-width_{thumb_width}-scan-{file_name}")


def resize_image(image_fp: Path, thumb_width: int = 300):
    im = cv2.imread(str(image_fp))
    height, width, _ = im.shape
    reduction_factor = width / thumb_width
    thumb_height = int(height / reduction_factor)
    thumb = cv2.resize(im, (thumb_width, thumb_height))
    return thumb


def select_image_region(im: np.array, x: int, y: int, w: int = None, h: int = None):
    """Select a region from an image.

    If no width is given, the region is from x to the right-most side.
    Similar for y when no height is given.
    """
    # im.shape order is height, width, color-dimensions
    if w is None:
        w = im.shape[1] - x
    if h is None:
        h = im.shape[0] - y
    return im[y:y+h, x:x+h]


def make_image_thumbnail(image_fp: Path, thumb_fp: Path, thumb_width: int = 300):
    thumb = resize_image(image_fp, thumb_width=thumb_width)
    cv2.imwrite(str(thumb_fp), thumb)


def select_scan_region_from_thumbnail(region: pdm.PageXMLRegion, thumbnail: Thumbnail,
                                      scan: pdm.PageXMLScan, resize: Tuple[int, int] = None):
    """For a given PageXML scan, a region in that scan and a thumbnail of the scan, return the thumbnail selection
    corresponding to the region."""
    if scan is not None:
        scan_size = Scan(scan.coords.width, scan.coords.height)
    elif 'scan_width' in region.metadata and 'scan_height' in region.metadata:
        scan_size = Scan(region.metadata['scan_width'], region.metadata['scan_height'])
    else:
        raise ValueError("scan must be given if region does not have 'scan_width' and 'scan_height' in metadata")

    region_box = Box.from_json(region.coords.box)
    if region_box.x < 0:
        region_box.x = 0
    if region_box.y < 0:
        region_box.y = 0
    selection = ImageSelection(scan_size, region_box, thumbnail)
    region_image = selection.cropped
    if resize is not None:
        try:
            region_image = cv2.resize(region_image, resize)
        except BaseException:
            print(f"region: {region.id}")
            print(f"region_image.shape: {region_image.shape}\tresize: {resize}")
            raise
    return region_image


def select_scan_regions_from_thumbnail(scan: pdm.PageXMLScan, thumb_path: Path,
                                       resize: Tuple[int, int] = None, as_array: bool = True):
    """Each scan has one thumbnail and multiple regions. This function selects the regions from the scan in
    the corresponding thumbnail. """
    thumbnail = load_thumbnail(thumb_path, as_array=as_array)

    region_images = []
    for region in scan.regions:
        region_image = select_scan_region_from_thumbnail(scan, region, thumbnail, resize=resize)
        region_images.append(region_image)
    return region_images


def merge_region_images_to_1d_pixels(region_images: List[np.array]):
    """Merge a list of region images into a single 1D pixel array. Image regions have different shapes, thus
    are merged into a single list of pixels."""
    # step 1: reshape each image to one dimension
    images_1d = [im.reshape((im.shape[0] * im.shape[1], im.shape[2])) for im in region_images]
    return np.concatenate(images_1d)
