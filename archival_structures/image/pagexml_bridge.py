"""The only module that converts between pagexml-tools coordinate objects (`Coords`)
and this package's own image-space coordinate objects (`Box`).

Keeping this conversion in one place means callers never need to know whether
they're looking at a PageXML `Coords` or a `Box` - they convert at the boundary
and work with `Box` everywhere else.
"""
from typing import List, Tuple

import numpy as np
import numpy.typing as npt
import pagexml.model.physical_document_model as pdm
from pagexml.model.coords import Coords

from archival_structures.model.image import Box


def coords_to_box(coords: Coords, label: str = None) -> Box:
    """Convert a PageXML `Coords` object to a `Box`."""
    return Box(coords.x, coords.y, coords.w, coords.h, label)


def box_to_coords(box: Box) -> Coords:
    """Convert a `Box` to a PageXML `Coords` object (as an axis-aligned rectangle)."""
    return Coords.coords_from_box_params(round(box.x), round(box.y), round(box.w), round(box.h))


def region_to_box(region: pdm.PageXMLDoc) -> Box:
    """Get the bounding `Box` of a PageXML region, text line or other coordinate-bearing
    PageXML element, in scan (i.e. original image) coordinate space."""
    return coords_to_box(region.coords, label=region.id)


def polygon_to_box_pixels(image: npt.NDArray, region: pdm.PageXMLDoc) -> npt.NDArray:
    """Select the pixels of `image` that fall inside the polygon described by
    `region.coords`. `region.coords.points` are (x, y) pairs; `image` is indexed
    (row, column) = (y, x), so the two axes are swapped here.

    :param image: the image pixel array to select from, shape (height, width, channels)
    :param region: the PageXML region whose polygon to select pixels for
    :return: an (N, channels) array of the pixels inside the polygon
    """
    import skimage.draw
    xs, ys = zip(*region.coords.points)
    rows, cols = skimage.draw.polygon(ys, xs)
    return image[rows, cols]


def region_polygon_mask(image: npt.NDArray, region: pdm.PageXMLDoc) -> npt.NDArray:
    """Create a boolean mask, the same height/width as `image`, that is True for pixels
    inside the polygon described by `region.coords`.

    :param image: the image pixel array the mask should match the shape of
    :param region: the PageXML region whose polygon to mask
    :return: a boolean mask of shape (height, width)
    """
    import skimage.draw
    xs, ys = zip(*region.coords.points)
    rows, cols = skimage.draw.polygon(ys, xs)
    mask = np.zeros(image.shape[:2], dtype=bool)
    mask[rows, cols] = True
    return mask


def crop_box_from_image(image: npt.NDArray, box: Box) -> npt.NDArray:
    """Crop `image` to the area described by `box`, in the same coordinate space as
    the image (i.e. `box` must already be in image pixel coordinates, not scan/PageXML
    coordinates - rescale it first if needed)."""
    x, y, w, h = round(box.x), round(box.y), round(box.w), round(box.h)
    return image[y:y + h, x:x + w]


def region_to_image_crop(image: npt.NDArray, region: pdm.PageXMLDoc) -> npt.NDArray:
    """Crop `image` to the bounding box of a PageXML region, assuming `image` is at
    the same scale as the PageXML document `region` belongs to."""
    return crop_box_from_image(image, region_to_box(region))


def box_to_text_region(box: Box, doc_id: str = None) -> pdm.PageXMLTextRegion:
    """Build a new `PageXMLTextRegion` whose coordinates are the bounding box of `box`,
    for adding a region annotated in image space back into a PageXML document
    (after rescaling/translating `box` into scan coordinate space)."""
    return pdm.PageXMLTextRegion(doc_id=doc_id, coords=box_to_coords(box))
