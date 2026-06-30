"""Low-level image loading/conversion and PageXML-coordinate pixel selection helpers, used
throughout `archival_structures.image`/`clustering`/`analysis`.

`load_image` is the canonical way this codebase loads an image as RGB (despite using
`cv2.imread`, which natively returns BGR -- it converts before returning).
`rescale_pagexml_scan_to_image` is the standard way to bring a PageXML scan's coordinates into
the pixel space of a specific image file (e.g. a thumbnail smaller than the scan's own declared
dimensions) before doing any pixel-level work with it.
"""

import copy
from pathlib import Path
from typing import Union

import cv2
import numpy as np
import numpy.typing as npt
import pagexml.model.physical_document_model as pdm
import skimage
from PIL import Image
from pagexml.helper.pagexml_helper import transform_doc_coords

from archival_structures.image.pagexml_bridge import (
    polygon_to_box_pixels, region_to_image_crop, region_polygon_mask,
)
from archival_structures.model.image import ThumbnailArray


def load_image(image_path: Union[str, Path]) -> Union[np.ndarray, Image.Image, cv2.UMat]:
    """Load an image from the given path and return it as a numpy array."""
    image = cv2.imread(image_path)
    return cv2.cvtColor(image, cv2.COLOR_BGR2RGB)


def convert_image(orig_image, converted_image):
    """Open `orig_image`, convert it to RGB, and save it as `converted_image` (PNG)."""
    im = Image.open(orig_image)
    im.convert("RGB").save(converted_image,
                           #'JPEG',
                           'PNG',
                           # quality_mode='dB',
                           # quality_layers=[41]
                           )


def make_lab_thumb(thumb: ThumbnailArray) -> ThumbnailArray:
    """A copy of `thumb` with its `image` converted to LAB colour space (rounded to whole
    numbers), leaving the original untouched."""
    thumb_lab = copy.deepcopy(thumb)
    thumb_lab.image = skimage.color.rgb2lab(thumb_lab.image)
    thumb_lab.image = np.round(thumb_lab.image, 0)
    return thumb_lab


def rescale_pagexml_scan_to_image(scan: pdm.PageXMLDoc, image: npt.NDArray):
    """Rescale the pagexml coordinates to the image size."""
    img_h, img_w, _ = image.shape
    scan_w, scan_h = scan.coords.width, scan.coords.height
    # the rescale factor is the average of the width and height ratios
    rescale_factor = (img_w / scan_w + img_h / scan_h ) / 2
    return transform_doc_coords(scan, rescale_by=rescale_factor)


def select_polygon_pixels(image: npt.NDArray, region: pdm.PageXMLDoc):
    """Select the pixels of `image` that fall inside the polygon of `region.coords`.

    :param image: the image pixel array to select from, shape (height, width, channels)
    :param region: the PageXML region whose polygon to select pixels for
    :return: an (N, channels) array of the pixels inside the polygon"""
    return polygon_to_box_pixels(image, region)


def select_pagexml_region_from_image(image: npt.NDArray, region: pdm.PageXMLDoc):
    """Select a region from an image based on the coordinates in a PageXML region.

    :param image: the image pixel array to select from, shape (height, width, channels)
    :param region: the PageXML region to select from
    :return: the selected region of the image as a numpy array"""
    return region_to_image_crop(image, region)


def create_region_polygon_mask(image: npt.NDArray, region: pdm.PageXMLDoc):
    """Create a mask for a polygon region."""
    return region_polygon_mask(image, region)


def main():
    """CLI/script entry point: batch-convert every `.jp2` thumbnail under a hardcoded
    directory to `.png` (`convert_image`)."""
    thumb_dir = Path("../../data/thumbs/NL-AsnDA_0114.11")
    jp2_files = list(thumb_dir.glob('*.jp2'))
    print(f"number of jp2 files: {len(jp2_files)}")
    for ti, tf_jp2 in enumerate(jp2_files):
        # tf_jpg = thumb_dir / tf_jp2.name.replace('.jp2', '.jpg')
        # print(f"{ti} {tf_jp2} {tf_jpg}")
        # convert_image(orig_image=tf_jp2, converted_image=tf_jpg)
        tf_png = thumb_dir / tf_jp2.name.replace('.jp2', '.png')
        print(f"{ti} {tf_jp2} {tf_png}")
        convert_image(orig_image=tf_jp2, converted_image=tf_png)


if __name__ == "__main__":
    main()
