import datetime
import os
import json
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Union

import pandas as pd
import ipywidgets as widgets
from ipycanvas import Canvas
from IPython.display import display, clear_output

from archival_structures.image.image_base import Box, ImageSelection, boxes_overlap
from archival_structures.image.image_base import make_selection_from_row, cropped_image_to_widgets_image
from archival_structures.image.image_base import scan_box_to_thumb_box, thumb_box_to_scan_box

def anno_in_selection(anno: Dict[str, int], image_sel: ImageSelection) -> bool:
    """Check if the annotation box is within the selection box"""
    # check overlap of boxes
    anno_box = Box(anno['orig_box']['x'], anno['orig_box']['y'], anno['orig_box']['w'], anno['orig_box']['h'])
    return boxes_overlap(anno_box, image_sel.selection_box)


class ThumbnailSelectionTagger:

    def __init__(self, df: pd.DataFrame, output_dir: str, image_dir: str = None,
                 labels=None, rows=2, cols=2, image_info_col=None):
        """
        Usage:
        tagger = ObjectDetectionTagger(df, './thumbs/', labels=['Dog', 'Cat', 'Tree'])
        tagger.display()
        """
        self.df = df
        self.rows: List[dict] = [row for _, row in df.iterrows()]
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
        self.canvas_width = 600 / self.num_cols

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
        # subset = self.df.iloc[start:start + self.images_per_page]
        paging_rows = self.rows[start:start + self.images_per_page]

        cells = []
        for row in paging_rows:
            row['filepath'] = os.path.join(self.image_dir, row['filename'])
            image_sel = make_selection_from_row(row, canvas_width=self.canvas_width)
            cells.append(self._create_drawing_cell(image_sel=image_sel))

        grid = widgets.GridBox(cells, layout=widgets.Layout(
            grid_template_columns=f"repeat({self.num_cols}, {self.canvas_width+20}px)", grid_gap='20px'))

        with self.output_container:
            clear_output(wait=True)
            display(grid)

        total_p = (len(self.rows) - 1) // self.images_per_page + 1
        self.page_label.value = f"Page {self.current_page + 1} of {total_p}"

    def _create_drawing_cell(self, image_sel: ImageSelection):
        # Create Canvas (width x height for the grid cell)
        canvas = Canvas(width=image_sel.canvas_width, height=image_sel.canvas_height)

        # Draw background image
        if os.path.exists(image_sel.thumbnail.filepath):
            # img = widgets.Image.from_file(image_sel.image_file)
            img = cropped_image_to_widgets_image(image_sel)
            canvas.draw_image(img, 0, 0, image_sel.canvas_width, image_sel.canvas_height)

        # Draw existing boxes
        self._redraw_canvas(canvas, image_sel)

        # Mouse Events for Drawing
        def handle_mousedown(x, y):
            self.is_drawing = True
            self.start_point = (x, y)

        def handle_mouseup(x, y):
            if self.is_drawing:
                x0, y0 = self.start_point
                thumb_box = Box(
                    min(x0, x), min(y0, y),
                    abs(x - x0), abs(y - y0),
                    self.label_selector.value
                )
                scan_box = thumb_box_to_scan_box(image_sel, thumb_box)
                annotation = {
                    "type": "image_region_label",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "thumb_box": thumb_box.__dict__,
                    "orig_box": scan_box.__dict__,
                }
                if image_sel.thumbnail.filename not in self.annotations:
                    self.annotations[image_sel.thumbnail.filename] = []
                self.annotations[image_sel.thumbnail.filename].append(annotation)
                self._save_annotations(image_sel.thumbnail.filename)
                self._redraw_canvas(canvas, image_sel)
                self.is_drawing = False

        canvas.on_mouse_down(handle_mousedown)
        canvas.on_mouse_up(handle_mouseup)

        # Clear button for this image
        clear_all_btn = widgets.Button(description="Clear All Boxes", button_style='danger',
                                   layout={'width': f'{self.canvas_width}px'})
        clear_last_btn = widgets.Button(description="Clear Last Box", button_style='danger',
                                   layout={'width': f'{self.canvas_width}px'})

        fname = image_sel.thumbnail.filename

        def clear_last_box(b):
            print(f'clearing last annotation for fname {fname}')
            in_selection_annotations = [anno for anno in self.annotations[fname] if anno_in_selection(anno, image_sel)]
            print(f"in selection annotations: {len(in_selection_annotations)}")
            last_anno = in_selection_annotations[-1]
            print(f"last annotation: {last_anno}")
            self.annotations[fname].remove(last_anno)
            self._save_annotations(fname)
            self._redraw_canvas(canvas, image_sel)

        def clear_all_boxes(b):
            # print(f'loading annotations for fname {fname}')
            print(f'clearing all annotations for fname {fname}')
            in_selection_annotations = {anno for anno in self.annotations[fname] if anno_in_selection(anno, image_sel)}
            self.annotations[fname] = [anno for anno in self.annotations[fname] if anno not in in_selection_annotations]
            self._save_annotations(fname)
            self._redraw_canvas(canvas, image_sel)

        clear_all_btn.on_click(clear_all_boxes)
        clear_last_btn.on_click(clear_last_box)

        image_label = f"{fname} - {self.image_info[fname]}" if fname in self.image_info else fname
        return widgets.VBox([widgets.Label(image_label), canvas, clear_last_btn, clear_all_btn])

    def _redraw_canvas(self, canvas, image_sel: ImageSelection):
        # Reset canvas background
        # img = widgets.Image.from_file(image_sel.image_file)
        img = cropped_image_to_widgets_image(image_sel)

        canvas.clear()
        # Create Canvas (canvas_width x canvas_height for the grid cell)
        canvas.draw_image(img, 0, 0, image_sel.canvas_width, image_sel.canvas_height)

        # Draw all saved boxes
        # print(f"\nDRAWING ANNOTATIONS for {image_sel.image_file}")
        if image_sel.thumbnail.filename in self.annotations:
            canvas.line_width = 2
            for anno in self.annotations[image_sel.thumbnail.filename]:
                orig_box = anno["orig_box"]
                scan_selection_box = Box(orig_box['x'], orig_box['y'], orig_box['w'], orig_box['h'], orig_box['label'])
                thumb_box = scan_box_to_thumb_box(image_sel, scan_selection_box)
                canvas.stroke_style = 'red'
                canvas.stroke_rect(thumb_box.x, thumb_box.y, thumb_box.w, thumb_box.h)
                canvas.fill_style = 'red'
                canvas.fill_text(thumb_box.label, thumb_box.x, thumb_box.y - 5)

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
