"""Building and converting `archival_structures.model.image` coordinate-space selections.

`make_selection`/`make_selection_from_row` are the entry points for constructing an
`ImageCanvasSelection` (scan + selection box + thumbnail), from which `Transform`-based
conversions between scan/thumbnail/canvas coordinate spaces follow (see `model.image`).
`thumb_box_to_scan_box`/`scan_box_to_thumb_box` are thin convenience wrappers around those
transforms; `load_thumbnail` loads the actual thumbnail image (PIL or numpy array).
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Union

import numpy as np
import cv2
from PIL import Image

import archival_structures.utils.image_utils as im_utils
from archival_structures.model.image import ImageCanvasSelection, Box, Scan, ImageSelection, Thumbnail, ThumbnailArray


def thumb_box_to_scan_box(image_sel: ImageCanvasSelection, thumb_box: Box) -> Box:
    """Convert a box drawn on `image_sel`'s canvas into scan-space coordinates."""
    return image_sel.canvas_to_scan.apply(thumb_box)


def scan_box_to_thumb_box(image_sel: ImageCanvasSelection, scan_box: Box) -> Box:
    """Convert a scan-space box into `image_sel`'s canvas coordinates."""
    return image_sel.scan_to_canvas.apply(scan_box)


def make_selection(scan_width: int, scan_height: int, thumb_path: str,
                   width: int = None, height: int = None,
                   x: int = 0, y: int = 0, canvas_width: Union[int, float] = 300, as_array: bool = False):
    """Build an `ImageCanvasSelection` for a scan of size `scan_width` x `scan_height`, whose
    thumbnail is loaded from `thumb_path`. `(x, y, width, height)` define the selection box in
    scan-space (the full scan if `width`/`height` aren't given); `canvas_width` is the display
    width the selection is fit to (see `ImageCanvasSelection.thumb_to_canvas`)."""
    if width is None:
        width = scan_width
    if height is None:
        height = scan_height
    scan = Scan(scan_width, scan_height)
    selection_box = Box(x, y, width, height)
    thumbnail = load_thumbnail(thumb_path, as_array=as_array)
    return ImageCanvasSelection(scan, selection_box, thumbnail, canvas_width)


def make_selection_from_row(row: Dict[str, any], canvas_width: Union[int, float] = 300,
                            as_array: bool = False) -> ImageSelection:
    """`make_selection`, reading its arguments from a dict-like `row` (e.g. a DataFrame row)
    with `scan_width`/`scan_height`/`filepath` keys and optional `x`/`y`/`width`/`height` keys
    (selecting the full scan if the latter are absent)."""
    x = row['x'] if 'x' in row else 0
    y = row['y'] if 'y' in row else 0
    width = row['width'] if 'width' in row else row['scan_width']
    height = row['height'] if 'height' in row else row['scan_height']
    return make_selection(row['scan_width'], row['scan_height'], row['filepath'],
                          width, height, x, y, canvas_width=canvas_width, as_array=as_array)


def get_image_size(img_path: str):
    """`(width, height)` of the image at `img_path`, via PIL."""
    im = Image.open(img_path)
    return im.size


def boxes_overlap(box1: Box, box2: Box) -> bool:
    """Check if two boxes overlap."""
    min_x = min(box1.x, box2.x)
    min_y = min(box1.y, box2.y)
    max_x = max(box1.x + box1.w, box2.x + box2.w)
    max_y = max(box1.y + box1.h, box2.y + box2.h)
    overlap_x = max_x - min_x
    overlap_y = max_y - min_y
    return overlap_x > 0 and overlap_y > 0


def clamp_box(box: Box, width: Union[int, float], height: Union[int, float]) -> Box:
    """Clamp `box` so it stays within a `width` x `height` rectangle anchored at the origin."""
    x = max(box.x, 0)
    y = max(box.y, 0)
    w = min(box.w, width - x)
    h = min(box.h, height - y)
    return Box(x, y, w, h, box.label)


def resize_thumbnail(image: Union[np.ndarray, Image.Image, cv2.UMat], width: int, height: int) -> Union[np.ndarray, Image.Image, cv2.UMat]:
    """Resize an image to the given width and height."""
    if isinstance(image, np.ndarray):
        return cv2.resize(image, (width, height))
    elif isinstance(image, Image.Image):
        return image.resize((width, height))
    elif isinstance(image, cv2.UMat):
        return cv2.resize(image, (width, height))
    else:
        raise TypeError(f"Unsupported image type: {type(image)}")


def compute_resize_width_height(image_width: int, image_height: int, max_pixels: int) -> Tuple[int, int]:
    """Compute the width and height of the resized image to fit the maximum number of pixels."""
    if image_width * image_height < max_pixels:
        return image_width, image_height
    resize_ratio = np.sqrt(max_pixels / (image_width * image_height))
    return int(image_width * resize_ratio), int(image_height * resize_ratio)


def load_thumbnail(thumb_path: Union[str, Path], as_array: bool = False, max_pixels: int = None) -> Union[Thumbnail, ThumbnailArray]:
    """
    Load a thumbnail from the given path and return a Thumbnail object.

    :param thumb_path: Path to the thumbnail
    :param as_array: If True, return a numpy array instead of a Thumbnail object.
    :param max_pixels: Resize the thumbnail to have at most this number of pixels
                       (width times height), while preserving the aspect ratio.
    """
    thumb_file = os.path.split(thumb_path)[-1]
    if as_array:
        image = im_utils.load_image(thumb_path)
        image_height, image_width = image.shape[:2]
        if max_pixels is not None:
            image_width, image_height = compute_resize_width_height(image_width, image_height, max_pixels)
            image = cv2.resize(image, (image_width, image_height), interpolation=cv2.INTER_AREA)
        return ThumbnailArray(thumb_file, thumb_path, image_width, image_height, image)
    else:
        image = Image.open(thumb_path)
        image_width, image_height = image.size
        if max_pixels is not None:
            image_width, image_height = compute_resize_width_height(image_width, image_height, max_pixels)
            image = image.resize((image_width, image_height))
        return Thumbnail(thumb_file, thumb_path, image_width, image_height, image)


