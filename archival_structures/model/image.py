"""The coordinate-space abstraction shared across `archival_structures.image` and
`archival_structures.analysis`: a scan has its own native pixel coordinates, a thumbnail of
that scan has its own (smaller) pixel coordinates, and a canvas rendering a cropped/scaled
selection of the thumbnail has yet another. `Transform` is the affine mapping between any two
of these; `ImageSelection`/`ImageCanvasSelection` compute the right `Transform` for a given
scan+thumbnail(+canvas) combination so call sites never re-derive the scaling math themselves.
`Box` is the plain rectangle type these transforms operate on.
"""

import numpy as np
from dataclasses import dataclass
from typing import Dict, Union

import cv2
from PIL import Image


@dataclass
class Box:
    """A rectangle (`x`, `y`, `w`, `h`) in some coordinate space, with an optional `label`."""

    x: Union[int, float]
    y: Union[int, float]
    w: Union[int, float]
    h: Union[int, float]
    label: str = None

    @staticmethod
    def from_json(json_data: Dict[str, int|float|str]):
        """Build a `Box` from a dict with `x`/`y`/`w`/`h` keys (e.g. deserialized JSON)."""
        return Box(json_data['x'], json_data['y'], json_data['w'], json_data['h'])


@dataclass(frozen=True)
class Transform:
    """An affine mapping between two coordinate spaces (e.g. scan pixels and
    thumbnail pixels), applied independently per axis: out = in * scale + offset.

    Used to convert `Box` coordinates between the different coordinate spaces
    a region can be expressed in (full scan, full thumbnail, a cropped
    sub-region of the thumbnail, a canvas rendering of that sub-region, ...)
    without re-deriving the conversion math at each call site.
    """

    scale_x: float
    scale_y: float
    offset_x: float = 0.0
    offset_y: float = 0.0

    def apply(self, box: Box) -> Box:
        """Map `box` from this transform's input space to its output space."""
        return Box(
            box.x * self.scale_x + self.offset_x,
            box.y * self.scale_y + self.offset_y,
            box.w * self.scale_x,
            box.h * self.scale_y,
            box.label,
        )

    def inverse(self) -> "Transform":
        """The transform that maps back from this transform's output space to its input space."""
        return Transform(
            1 / self.scale_x,
            1 / self.scale_y,
            -self.offset_x / self.scale_x,
            -self.offset_y / self.scale_y,
        )

    def then(self, other: "Transform") -> "Transform":
        """Compose this transform with `other`, applied afterwards: `self.then(other).apply(box)`
        is equivalent to `other.apply(self.apply(box))`."""
        return Transform(
            self.scale_x * other.scale_x,
            self.scale_y * other.scale_y,
            self.offset_x * other.scale_x + other.offset_x,
            self.offset_y * other.scale_y + other.offset_y,
        )

    @staticmethod
    def identity() -> "Transform":
        """A transform that leaves coordinates unchanged."""
        return Transform(1.0, 1.0)

    @staticmethod
    def between_dims(from_width: float, from_height: float, to_width: float, to_height: float) -> "Transform":
        """A transform that scales a `from_width` x `from_height` rectangle (anchored at
        the origin) onto a `to_width` x `to_height` rectangle."""
        return Transform(to_width / from_width, to_height / from_height)

    @staticmethod
    def fit_box_to_width(box: Box, target_width: float) -> "Transform":
        """A transform that crops to `box` and scales it, preserving aspect ratio,
        so that its width becomes `target_width`."""
        scale = target_width / box.w
        return Transform(scale, scale, -box.x * scale, -box.y * scale)


@dataclass
class Scan:
    """A scan's own pixel dimensions (from its PageXML `Coords`), independent of any
    thumbnail/canvas rendering of it."""

    width: int
    height: int

@dataclass
class ScanSelection:
    """A `selection_box` (in scan-space) within a given `scan`."""

    scan: Scan
    selection_box: Box


@dataclass
class Thumbnail:
    """A loaded thumbnail image (PIL), with its own pixel dimensions and source file info. See
    `archival_structures.image.image_base.load_thumbnail`."""

    filename: str
    filepath: str
    width: int
    height: int
    image: Image.Image


@dataclass
class ThumbnailArray:
    """Like `Thumbnail`, but `image` is a numpy/cv2 pixel array rather than a PIL `Image`
    (`load_thumbnail(..., as_array=True)`)."""

    filename: str
    filepath: str
    width: int
    height: int
    image: Union[np.ndarray, Image.Image, cv2.UMat]


@dataclass
class ThumbnailSelection:
    """A `selection_box` (in thumbnail-space) within a given `thumbnail`."""

    thumbnail: Thumbnail
    selection_box: Box


@dataclass
class ImageSelection:
    """A `selection_box` (in scan-space) within `scan`, whose `thumbnail` is also known --
    enough to derive the `Transform` between scan-space and thumbnail-space (`scan_to_thumb`/
    `thumb_to_scan`) and to crop the selection out of the thumbnail (`cropped`)."""

    scan: Scan
    selection_box: Box
    thumbnail: Union[Thumbnail, ThumbnailArray]

    @property
    def scan_to_thumb(self) -> Transform:
        """Maps scan-space coordinates onto the (full) thumbnail."""
        return Transform.between_dims(self.scan.width, self.scan.height,
                                      self.thumbnail.width, self.thumbnail.height)

    @property
    def thumb_to_scan(self) -> Transform:
        """Maps (full) thumbnail coordinates onto the scan."""
        return self.scan_to_thumb.inverse()

    @property
    def thumb_width_scale(self):
        """Legacy alias: scan pixels per thumbnail pixel, horizontally."""
        return 1 / self.scan_to_thumb.scale_x

    @property
    def thumb_height_scale(self):
        """Legacy alias: scan pixels per thumbnail pixel, vertically."""
        return 1 / self.scan_to_thumb.scale_y

    @property
    def thumb_selection_box(self) -> Box:
        """`selection_box`, converted from scan space into (full) thumbnail space."""
        thumb_box = self.scan_to_thumb.apply(self.selection_box)
        return Box(round(thumb_box.x), round(thumb_box.y), round(thumb_box.w), round(thumb_box.h),
                   thumb_box.label)

    @property
    def cropped(self):
        """`selection_box`'s pixels, cropped out of the (full) thumbnail."""
        thumb_box = self.thumb_selection_box
        left, right = thumb_box.x, thumb_box.x + thumb_box.w
        top, bottom = thumb_box.y, thumb_box.y + thumb_box.h
        if isinstance(self.thumbnail, ThumbnailArray):
            return self.thumbnail.image[top:bottom, left:right]
        else:
            return self.thumbnail.image.crop((left, top, right, bottom))


@dataclass
class ImageCanvasSelection(ImageSelection):
    """An `ImageSelection` additionally rendered onto a canvas of `canvas_width` pixels wide
    (aspect-ratio-preserved), adding the `Transform`s to/from that canvas's own coordinate
    space (`scan_to_canvas`/`canvas_to_scan`/`thumb_to_canvas`/`canvas_to_thumb`)."""

    canvas_width: Union[int, float]

    @property
    def thumb_to_canvas(self) -> Transform:
        """Maps (full) thumbnail coordinates onto the canvas, which renders
        `thumb_selection_box` (a crop of the thumbnail) at width `canvas_width`,
        preserving its aspect ratio."""
        return Transform.fit_box_to_width(self.thumb_selection_box, self.canvas_width)

    @property
    def canvas_to_thumb(self) -> Transform:
        """Maps canvas coordinates back onto the (full) thumbnail."""
        return self.thumb_to_canvas.inverse()

    @property
    def scan_to_canvas(self) -> Transform:
        """Maps scan-space coordinates onto the canvas."""
        return self.scan_to_thumb.then(self.thumb_to_canvas)

    @property
    def canvas_to_scan(self) -> Transform:
        """Maps canvas coordinates back onto the scan."""
        return self.scan_to_canvas.inverse()

    @property
    def canvas_height(self):
        """Canvas height implied by `canvas_width` and `thumb_selection_box`'s aspect ratio."""
        return self.canvas_width * (self.thumb_selection_box.h / self.thumb_selection_box.w)

