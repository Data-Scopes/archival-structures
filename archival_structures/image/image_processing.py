# !/usr/local/bin/python3
# Source - https://stackoverflow.com/a/51822265
# Posted by Mark Setchell, modified by community. See post 'Timeline' for change history
# Retrieved 2026-01-07, License - CC BY-SA 4.0


import os
from pathlib import Path

import cv2
import numpy as np


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
