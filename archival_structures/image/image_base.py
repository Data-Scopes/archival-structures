import json
import os
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, Union

import numpy as np
import cv2
import ipywidgets as widgets
from PIL import Image


@dataclass
class Box:

    x: Union[int, float]
    y: Union[int, float]
    w: Union[int, float]
    h: Union[int, float]
    label: str = None

    @staticmethod
    def from_json(json_data: Dict):
        return Box(json_data['x'], json_data['y'], json_data['w'], json_data['h'])


@dataclass
class Scan:

    width: int
    height: int

@dataclass
class ScanSelection:

    scan: Scan
    selection_box: Box


@dataclass
class Thumbnail:

    filename: str
    filepath: str
    width: int
    height: int
    image: Image.Image


@dataclass
class ThumbnailArray:

    filename: str
    filepath: str
    width: int
    height: int
    image: Union[np.ndarray, Image.Image, cv2.UMat]


@dataclass
class ThumbnailSelection:

    thumbnail: Thumbnail
    selection_box: Box


@dataclass
class ImageSelection:

    scan: Scan
    selection_box: Box
    thumbnail: Thumbnail

    @property
    def sel_width_scale(self):
        return self.scan.width / self.selection_box.w

    @property
    def sel_height_scale(self):
        return self.scan.height / self.selection_box.h

    @property
    def thumb_width_scale(self):
        return self.scan.width / self.thumbnail.width

    @property
    def thumb_height_scale(self):
        return self.scan.height / self.thumbnail.height

    @property
    def sel_thumb_width_scale(self):
        return self.thumb_width_scale / self.sel_width_scale

    @property
    def sel_thumb_height_scale(self):
        return self.thumb_height_scale / self.sel_height_scale

    @property
    def thumb_sel_width(self):
        return self.selection_box.w * self.thumbnail.width / self.scan.width

    @property
    def image_sel_height(self):
        return self.selection_box.h * self.thumbnail.height / self.scan.height

    @property
    def thumb_selection_box(self):
        return Box(self.selection_box.x / self.thumb_width_scale, self.selection_box.y / self.thumb_height_scale,
                   self.selection_box.w / self.thumb_width_scale, self.selection_box.h / self.thumb_width_scale)

    @property
    def cropped(self):
        thumb_box = self.thumb_selection_box
        left, right = thumb_box.x, thumb_box.x + thumb_box.w
        top, bottom = thumb_box.y, thumb_box.y + thumb_box.h
        if isinstance(self.thumbnail, ThumbnailArray):
            return self.thumbnail.image[int(left):int(right), int(top):int(bottom)]
        else:
            return self.thumbnail.image.crop((left, top, right, bottom))


@dataclass
class ImageCanvasSelection(ImageSelection):

    canvas_width: Union[int, float]

    @property
    def canvas_height(self):
        # self.image_sel_width
        aspect_ratio = self.scan.width / self.scan.height
        return self.image_sel_height * (self.canvas_width / self.thumb_sel_width)

    @property
    def canvas_width_scale(self):
        return self.canvas_width / self.thumbnail.width

    @property
    def canvas_height_scale(self):
        return self.canvas_height / self.thumbnail.height


def thumb_box_to_scan_box(image_sel: ImageCanvasSelection, thumb_box: Box) -> Box:
    scan_sel_box = Box(
        thumb_box.x * (image_sel.sel_thumb_width_scale / image_sel.canvas_width_scale),
        thumb_box.y * (image_sel.sel_thumb_height_scale / image_sel.canvas_height_scale),
        thumb_box.w * (image_sel.sel_thumb_width_scale / image_sel.canvas_width_scale),
        thumb_box.h * (image_sel.sel_thumb_height_scale / image_sel.canvas_height_scale),
        )
    scan_box = Box(
        scan_sel_box.x + image_sel.selection_box.x,
        scan_sel_box.y + image_sel.selection_box.y,
        scan_sel_box.w,
        scan_sel_box.h,
        thumb_box.label,
        )
    return scan_box


def scan_box_to_thumb_box(image_sel: ImageCanvasSelection, scan_box: Box):
    scan_sel_box = Box(
        scan_box.x - image_sel.selection_box.x,
        scan_box.y - image_sel.selection_box.y,
        scan_box.w,
        scan_box.h,
        )
    thumb_box = Box(
        scan_sel_box.x / (image_sel.sel_thumb_width_scale / image_sel.canvas_width_scale),
        scan_sel_box.y / (image_sel.sel_thumb_height_scale / image_sel.canvas_height_scale),
        scan_sel_box.w / (image_sel.sel_thumb_width_scale / image_sel.canvas_width_scale),
        scan_sel_box.h / (image_sel.sel_thumb_height_scale / image_sel.canvas_height_scale),
        scan_box.label
    )
    return thumb_box


def make_selection(scan_width: int, scan_height: int, thumb_path: str,
                   width: int = None, height: int = None,
                   x: int = 0, y: int = 0, canvas_width: Union[int, float] = 300, as_array: bool = False):
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
    x = row['x'] if 'x' in row else 0
    y = row['y'] if 'y' in row else 0
    width = row['width'] if 'width' in row else row['scan_width']
    height = row['height'] if 'height' in row else row['scan_height']
    return make_selection(row['scan_width'], row['scan_height'], row['filepath'],
                          width, height, x, y, canvas_width=canvas_width, as_array=as_array)


def cropped_image_to_widgets_image(image_sel: ImageSelection):
    img_bytes = BytesIO()
    cropped_img = image_sel.cropped
    cropped_img.save(img_bytes, format='PNG')
    return widgets.Image(value=img_bytes.getvalue())


def get_image_size(img_path: str):
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


def load_thumbnail(thumb_path: Union[str, Path], as_array: bool = False) -> Union[Thumbnail, ThumbnailArray]:
    """
    Load a thumbnail from the given path and return a Thumbnail object.
    """
    thumb_file = os.path.split(thumb_path)[-1]
    if as_array:
        image = cv2.imread(thumb_path)
        image_width, image_height = image.shape[:2]
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return ThumbnailArray(thumb_file, thumb_path, image_width, image_height, image)
    else:
        image = Image.open(thumb_path)
        image_width, image_height = image.size
        return Thumbnail(thumb_file, thumb_path, image_width, image_height, image)


