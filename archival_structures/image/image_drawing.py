import os
import json
from dataclasses import dataclass
from io import BytesIO
from typing import Dict

import pandas as pd
import ipywidgets as widgets
from ipycanvas import Canvas
from IPython.display import display, clear_output
from PIL import Image


@dataclass
class ImageSelection:

    scan_width: int
    scan_height: int
    image_path: str
    image_file: str
    image: Image
    image_width: int
    image_height: int
    sel_width: int
    sel_height: int
    sel_x: int
    sel_y: int
    box_width: int

    @property
    def sel_width_scale(self):
        return self.scan_width / self.sel_width

    @property
    def sel_height_scale(self):
        return self.scan_height / self.sel_height

    @property
    def thumb_width_scale(self):
        return self.scan_width / self.image_width

    @property
    def thumb_height_scale(self):
        return self.scan_height / self.image_height

    @property
    def sel_thumb_width_scale(self):
        return self.thumb_width_scale / self.sel_width_scale

    @property
    def sel_thumb_height_scale(self):
        return self.thumb_height_scale / self.sel_height_scale

    @property
    def image_sel_x(self):
        return self.sel_x / self.image_width

    @property
    def image_sel_width(self):
        return self.sel_width * self.image_width / self.scan_width

    @property
    def image_sel_y(self):
        return self.sel_y / self.image_height

    @property
    def image_sel_height(self):
        return self.sel_height * self.image_height / self.scan_height

    @property
    def box_height(self):
        # self.image_sel_width
        aspect_ratio = self.scan_width / self.scan_height
        return self.image_sel_height * (self.box_width / self.image_sel_width)

    @property
    def box_width_scale(self):
        return self.box_width / self.image_width

    @property
    def box_height_scale(self):
        return self.box_height / self.image_height

    @property
    def selection_box(self):
        image_left = self.sel_x / self.thumb_width_scale
        image_right = (self.sel_x + self.sel_width) / self.thumb_width_scale
        image_top = self.sel_y / self.thumb_height_scale
        image_bottom = (self.sel_y + self.sel_height) / self.thumb_height_scale
        return image_left, image_top, image_right, image_bottom

    @property
    def cropped(self):
        return self.image.crop(self.selection_box)


def thumb_box_to_scan_box(image_sel: ImageSelection, thumb_box: Dict[str, float]):
    scan_sel_box = {
        'x': thumb_box['x'] * (image_sel.sel_thumb_width_scale / image_sel.box_width_scale),
        'y': thumb_box['y'] * (image_sel.sel_thumb_height_scale / image_sel.box_height_scale),
        'w': thumb_box['w'] * (image_sel.sel_thumb_width_scale / image_sel.box_width_scale),
        'h': thumb_box['h'] * (image_sel.sel_thumb_height_scale / image_sel.box_height_scale),
    }
    scan_box = {
        'label': thumb_box['label'],
        'x': scan_sel_box['x'] + image_sel.sel_x,
        'y': scan_sel_box['y'] + image_sel.sel_y,
        'w': scan_sel_box['w'],
        'h': scan_sel_box['h'],
    }
    print(f"image_width: {image_sel.image_width}\timage_height: {image_sel.image_height}")
    print(f"box_width: {image_sel.box_width}\tbox_height: {image_sel.box_height}")
    print(f"\nTHUMB_BOX: {thumb_box}\n")
    print(f"image_sel.image_sel_width: {image_sel.image_sel_width}")
    print(f"image_sel.image_sel_height: {image_sel.image_sel_height}")
    print(f"image_sel.thumb_width_scale: {image_sel.thumb_width_scale}")
    print(f"image_sel.box_width_scale: {image_sel.box_width_scale}")
    print(f"image_sel.thumb_height_scale: {image_sel.thumb_height_scale}")
    print(f"image_sel.box_height_scale: {image_sel.box_height_scale}")
    print(f"image_sel.sel_thumb_width_scale: {image_sel.sel_thumb_width_scale}")
    print(f"image_sel.box_width_scale: {image_sel.box_width_scale}")
    print(f"image_sel.sel_thumb_height_scale: {image_sel.sel_thumb_height_scale}")
    print(f"image_sel.box_height_scale: {image_sel.box_height_scale}")
    print(f"\nSCAN_SEL_BOX: {scan_sel_box}")
    print(f"\nsCAN_BOX: {scan_box}")
    """
    """
    return scan_box


def scan_box_to_thumb_box(image_sel: ImageSelection, scan_box: Dict[str, float]):
    scan_sel_box = {
        'x': scan_box['x'] - image_sel.sel_x,
        'y': scan_box['y'] - image_sel.sel_y,
        'w': scan_box['w'],
        'h': scan_box['h'],
    }
    thumb_box = {
        'label': scan_box['label'],
        'x': scan_sel_box['x'] / (image_sel.sel_thumb_width_scale / image_sel.box_width_scale),
        'y': scan_sel_box['y'] / (image_sel.sel_thumb_height_scale / image_sel.box_height_scale),
        'w': scan_sel_box['w'] / (image_sel.sel_thumb_width_scale / image_sel.box_width_scale),
        'h': scan_sel_box['h'] / (image_sel.sel_thumb_height_scale / image_sel.box_height_scale),
    }
    print(f"scan_width: {image_sel.scan_width}\tscan_height: {image_sel.scan_height}")
    print(f"image_width: {image_sel.image_width}\timage_height: {image_sel.image_height}")
    print(f"image_sel.image_sel_width: {image_sel.image_sel_width}")
    print(f"image_sel.image_sel_height: {image_sel.image_sel_height}")
    print(f"box_width: {image_sel.box_width}\tbox_height: {image_sel.box_height}")
    print(f"image_sel.thumb_width_scale: {image_sel.thumb_width_scale}")
    print(f"image_sel.box_width_scale: {image_sel.box_width_scale}")
    print(f"image_sel.thumb_height_scale: {image_sel.thumb_height_scale}")
    print(f"image_sel.box_height_scale: {image_sel.box_height_scale}")
    return thumb_box


def make_selection(scan_width: int, scan_height: int, image_path: str,
                   width: int = None, height: int = None,
                   x: int = 0, y: int = 0, box_width: int = 300):
    if width is None:
        width = scan_width
    if height is None:
        height = scan_height
    image = Image.open(image_path)
    image_file = os.path.split(image_path)[-1]
    image_width, image_height = image.size
    return ImageSelection(scan_width, scan_height,
                          image_path, image_file, image, image_width, image_height,
                          width, height, x, y, box_width)


def make_selection_from_row(row: Dict[str, any], box_width: int = 300) -> ImageSelection:
    x = row['x'] if 'x' in row else 0
    y = row['y'] if 'y' in row else 0
    width = row['width'] if 'width' in row else row['scan_width']
    height = row['height'] if 'height' in row else row['scan_height']
    return make_selection(row['scan_width'], row['scan_height'], row['filepath'],
                          width, height, x, y, box_width=box_width)


def cropped_image_to_widgets_image(image_sel: ImageSelection):
    img_bytes = BytesIO()
    cropped_img = image_sel.cropped
    cropped_img.save(img_bytes, format='PNG')
    return widgets.Image(value=img_bytes.getvalue())


def get_image_size(img_path: str):
    im = Image.open(img_path)
    return im.size


class ObjectDetectionTagger:

    def __init__(self, df: pd.DataFrame, output_dir: str, image_dir: str = None,
                 labels=None, rows=2, cols=2, image_info_col=None):
        """
        Usage:
        tagger = ObjectDetectionTagger(df, './thumbs/', labels=['Dog', 'Cat', 'Tree'])
        tagger.display()
        """
        self.df = df
        self.rows = [row for _, row in df.iterrows()]
        self.image_dir = image_dir
        self.output_dir = output_dir
        self.output_file_map = self._make_output_file_map()
        self.labels = labels or ['Object']
        self.image_info = {}
        # self.filepath_map = {}
        if image_info_col is not None:
            for row in self.rows:
                if image_dir is not None:
                    row['filepath'] = os.path.join(image_dir, row['filename'])
                else:
                    row['filepath'] = row['filename']
                    row['filename'] = os.path.split(row['filename'])
                self.image_info[row['filename']] = row[image_info_col]
                # self.filepath_map[row['filepath']] = row['filename']
        self.num_rows = rows
        self.num_cols = cols
        self.images_per_page = rows * cols
        self.box_width = 600 if cols == 1 else 300

        # State
        self.current_page = 0
        self.current_label = self.labels[0]
        self.annotations = self._load_annotations()

        # Drawing State
        self.is_drawing = False
        self.start_point = None

        # UI
        self.output_container = widgets.Output()
        self.page_label = widgets.Label()
        self.label_selector = widgets.Dropdown(options=self.labels, description='Active Label:')

    def _make_output_file_map(self):
        output_file_map = {}
        for row in self.rows:
            base_dir = self.output_dir
            if os.path.exists(base_dir) is False:
                os.mkdir(base_dir)
            if 'archive_id' in row:
                base_dir = os.path.join(base_dir, row['archive_id'])
            if os.path.exists(base_dir) is False:
                os.mkdir(base_dir)
            if 'inventory_id' in row:
                base_dir = os.path.join(base_dir, row['inventory_id'])
            if os.path.exists(base_dir) is False:
                os.mkdir(base_dir)
            output_file_map[row['filename']] = os.path.join(base_dir, f"annotations-{row['filename']}.json")
        return output_file_map

    def _load_annotations(self):
        annotations = {}
        for row in self.rows:
            # print(f"loading annotations for fname {row['filename']}")
            output_file = self.output_file_map[row['filename']]
            # print(f"\toutput_file: {output_file} exists: {os.path.exists(output_file)}")
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    annotations[row['filename']] = json.load(f)
            # print(f"\tannotations: {annotations[row['filename']]}")
        return annotations

    def _save_annotations(self, fname):
        output_file = self.output_file_map[fname]
        with open(output_file, 'w') as f:
            json.dump(self.annotations[fname], f, indent=4)

    def _render_page(self):
        start = self.current_page * self.images_per_page
        subset = self.df.iloc[start:start + self.images_per_page]

        cells = []
        for _, row in subset.iterrows():
            row['filepath'] = os.path.join(self.image_dir, row['filename'])
            image_sel = make_selection_from_row(row)
            cells.append(self._create_drawing_cell(image_sel=image_sel))

        grid = widgets.GridBox(cells, layout=widgets.Layout(
            grid_template_columns=f"repeat({self.num_cols}, {self.box_width+20}px)", grid_gap='20px'))

        with self.output_container:
            clear_output(wait=True)
            display(grid)

        total_p = (len(self.df) - 1) // self.images_per_page + 1
        self.page_label.value = f"Page {self.current_page + 1} of {total_p}"

    def _create_drawing_cell(self, image_sel: ImageSelection):
        # Create Canvas (width x height for the grid cell)
        canvas = Canvas(width=image_sel.box_width, height=image_sel.box_height)

        # Draw background image
        if os.path.exists(image_sel.image_file):
            # img = widgets.Image.from_file(image_sel.image_file)
            img = cropped_image_to_widgets_image(image_sel)
            canvas.draw_image(img, 0, 0, image_sel.box_width, image_sel.box_height)
            # canvas.draw_image(img, image_sel.image_sel_x, image_sel.image_sel_y,
            #                   image_sel.image_sel_width, image_sel.image_sel_height)

        # Draw existing boxes
        self._redraw_canvas(canvas, image_sel)

        # Mouse Events for Drawing
        def handle_mousedown(x, y):
            self.is_drawing = True
            self.start_point = (x, y)

        def handle_mouseup(x, y):
            if self.is_drawing:
                x0, y0 = self.start_point
                thumb_box = {
                    "label": self.label_selector.value,
                    "x": min(x0, x), "y": min(y0, y),
                    "w": abs(x - x0), "h": abs(y - y0)
                }
                scan_box = thumb_box_to_scan_box(image_sel, thumb_box)
                new_box = {
                    "type": "image_region_label",
                    "thumb_box": thumb_box,
                    "orig_box": scan_box
                }
                if image_sel.image_file not in self.annotations:
                    self.annotations[image_sel.image_file] = []
                self.annotations[image_sel.image_file].append(new_box)
                self._save_annotations(image_sel.image_file)
                self._redraw_canvas(canvas, image_sel)
                self.is_drawing = False

        canvas.on_mouse_down(handle_mousedown)
        canvas.on_mouse_up(handle_mouseup)

        # Clear button for this image
        clear_btn = widgets.Button(description="Clear Boxes", button_style='danger',
                                   layout={'width': f'{self.box_width}px'})

        fname = image_sel.image_file

        def clear_boxes(b):
            # print(f'loading annotations for fname {fname}')
            raise ValueError(f"Remove annotations that are in the selection. ")
            self.annotations[fname] = []
            self._save_annotations(fname)
            self._redraw_canvas(canvas, image_sel)

        clear_btn.on_click(clear_boxes)

        image_label = f"{fname} - {self.image_info[fname]}" if fname in self.image_info else fname
        return widgets.VBox([widgets.Label(image_label), canvas, clear_btn])

    def _redraw_canvas(self, canvas, image_sel: ImageSelection):
        # Reset canvas background
        # img = widgets.Image.from_file(image_sel.image_file)
        img = cropped_image_to_widgets_image(image_sel)

        canvas.clear()
        # Create Canvas (box_width x box_height for the grid cell)
        canvas.draw_image(img, 0, 0, image_sel.box_width, image_sel.box_height)
        # canvas.draw_image(img, image_sel.image_sel_x, image_sel.image_sel_y,
        #                   image_sel.image_sel_width, image_sel.image_sel_height)

        # Draw all saved boxes
        # print(f"\nDRAWING ANNOTATIONS for {image_sel.image_file}")
        if image_sel.image_file in self.annotations:
            canvas.line_width = 2
            for box in self.annotations[image_sel.image_file]:
                scan_box = box["orig_box"]
                thumb_box = scan_box_to_thumb_box(image_sel, scan_box)
                canvas.stroke_style = 'red'
                canvas.stroke_rect(thumb_box['x'], thumb_box['y'], thumb_box['w'], thumb_box['h'])
                canvas.fill_style = 'red'
                canvas.fill_text(thumb_box['label'], thumb_box['x'], thumb_box['y'] - 5)

    def display(self):
        prev_btn = widgets.Button(description="Previous", icon="arrow-left")
        next_btn = widgets.Button(description="Next", icon="arrow-right")

        prev_btn.on_click(lambda _: self._change_page(-1))
        next_btn.on_click(lambda _: self._change_page(1))

        controls = widgets.HBox([prev_btn, self.page_label, next_btn, self.label_selector],
                                layout={'justify_content': 'center', 'align_items': 'center'})

        display(controls, self.output_container)
        self._render_page()

    def _change_page(self, delta):
        self.current_page = max(0, self.current_page + delta)
        self._render_page()
