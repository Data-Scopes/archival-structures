"""An ipywidgets-based paginated image-tagging tool for Jupyter notebooks: free-text and
predefined-dropdown tags per image, persisted to a TSV (`filename`, comma-separated `tags`
columns), with filtering by tagged/untagged status.
"""

import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
import os


class ImageTagger:
    """Paginated image-tagging grid: each cell shows an image, its current tags, a free-text
    + Enter input, a predefined-tag dropdown with an Add button, and Undo/Clear buttons. Tags
    are persisted to `output_file` (a TSV) after every change. Call `.display()` to render."""

    def __init__(self, df, image_dir, output_file='image_tags.tsv',
                 predefined_tags=None, rows=5, cols=3, show_warnings: bool = False):
        self.df = df
        self.image_dir = image_dir
        self.output_file = output_file
        self.predefined_tags = predefined_tags or []
        self.rows = rows
        self.cols = cols
        self.images_per_page = rows * cols
        self.show_warnings = show_warnings

        # State
        self.current_page = 0
        self.filter_mode = 'Show All'

        # UI Components
        self.output_container = widgets.Output()
        self.page_label = widgets.Label()
        self.filter_dropdown = widgets.Dropdown(
            options=['Show All', 'Untagged Only', 'Tagged Only'],
            value='Show All',
            description='Filter:',
            layout={'width': '250px'}
        )

        self.jump_input = widgets.BoundedIntText(
            value=1,
            min=1,
            max=1,  # Updated dynamically
            step=1,
            description='Go to:',
            layout={'width': '120px'}
        )

        # Initialize TSV
        if not os.path.exists(self.output_file):
            pd.DataFrame(columns=['filename', 'tags']).to_csv(self.output_file, sep='\t', index=False)

    def _get_tag_data(self):
        return pd.read_csv(self.output_file, sep='\t')

    def _get_current_tags(self, filename):
        tdf = self._get_tag_data()
        row = tdf[tdf['filename'] == filename]
        if not row.empty:
            val = str(row.iloc[0]['tags'])
            return "" if val == 'nan' else val
        return ""

    def _update_tsv(self, filename, tag, action='add'):
        current_data = self._get_tag_data()
        if action == 'add':
            if filename in current_data['filename'].values:
                idx = current_data.index[current_data['filename'] == filename][0]
                existing = str(current_data.at[idx, 'tags'])
                updated = f"{existing}, {tag}".strip("nan, ")
                current_data.at[idx, 'tags'] = updated
            else:
                new_row = pd.DataFrame({'filename': [filename], 'tags': [tag]})
                current_data = pd.concat([current_data, new_row], ignore_index=True)

        elif action == 'undo':
            if filename in current_data['filename'].values:
                idx = current_data.index[current_data['filename'] == filename][0]
                tags_list = str(current_data.at[idx, 'tags']).split(", ")
                if tags_list and tags_list != ['nan']:
                    tags_list.pop()  # Remove last
                    current_data.at[idx, 'tags'] = ", ".join(tags_list)

        elif action == 'clear':
            if filename in current_data['filename'].values:
                idx = current_data.index[current_data['filename'] == filename][0]
                current_data.at[idx, 'tags'] = ""

        current_data.to_csv(self.output_file, sep='\t', index=False)

    def _render_page(self):
        tdf = self._get_tag_data()
        tagged_files = tdf[tdf['tags'].notna() & (tdf['tags'].astype(str).str.strip() != "")]['filename'].tolist()
        status_dict = {f: True for f in tagged_files}

        if self.filter_mode == 'Untagged Only':
            filtered_df = self.df[~self.df['filename'].map(lambda x: status_dict.get(x, False))]
        elif self.filter_mode == 'Tagged Only':
            filtered_df = self.df[self.df['filename'].map(lambda x: status_dict.get(x, False))]
        else:
            filtered_df = self.df

        total_filtered = len(filtered_df)
        total_p = (total_filtered - 1) // self.images_per_page + 1

        # Update jump input limits
        self.jump_input.max = max(1, total_p)
        if self.current_page >= total_p: self.current_page = 0
        self.jump_input.value = self.current_page + 1

        start = self.current_page * self.images_per_page
        subset = filtered_df.iloc[start:start + self.images_per_page]

        items = [self._create_cell(row['filename']) for _, row in subset.iterrows()]

        grid = widgets.GridBox(items, layout=widgets.Layout(
            grid_template_columns=f"repeat({self.cols}, 220px)", grid_gap='20px'))

        with self.output_container:
            clear_output(wait=True)
            display(grid)

        self.page_label.value = f"Page {self.current_page + 1} of {max(1, total_p)} (Total: {total_filtered})"

    def _create_cell(self, fname):
        img_path = os.path.join(self.image_dir, fname)
        try:
            with open(img_path, "rb") as f:
                img_w = widgets.Image(value=f.read(), format='jpg', width=180, height=120)
        except BaseException as err:
            if self.show_warnings is True:
                print(err)
            img_w = widgets.Label(value=f"Missing: {fname}", layout={'height': '120px'})

        tags_display = widgets.HTML(value=self._get_html_tags(fname), layout={'width': '180px'})
        text_in = widgets.Text(placeholder='Tag + Enter', layout={'width': '180px'})
        drop = widgets.Dropdown(options=self.predefined_tags, layout={'width': '125px'})
        add_b = widgets.Button(description="Add", button_style='info', layout={'width': '55px'})

        undo_b = widgets.Button(description="Undo Last", layout={'width': '88px'})
        clear_b = widgets.Button(description="Clear All", button_style='danger', layout={'width': '88px'})

        # Handlers
        def on_sub(sender):
            """Add the free-text input's value as a tag (on Enter) and clear the input."""
            if sender.value.strip():
                self._update_tsv(fname, sender.value.strip())
                tags_display.value = self._get_html_tags(fname)
                sender.value = ""

        def on_add(b):
            """Add the dropdown's selected predefined tag."""
            self._update_tsv(fname, drop.value)
            tags_display.value = self._get_html_tags(fname)

        def on_undo(b):
            """Remove the most recently added tag."""
            self._update_tsv(fname, "", action='undo')
            tags_display.value = self._get_html_tags(fname)

        def on_clear(b):
            """Remove all tags for this image."""
            self._update_tsv(fname, "", action='clear')
            tags_display.value = self._get_html_tags(fname)

        text_in.on_submit(on_sub)
        add_b.on_click(on_add)
        undo_b.on_click(on_undo)
        clear_b.on_click(on_clear)

        return widgets.VBox([
            img_w, widgets.Label(value=fname), tags_display,
            text_in, widgets.HBox([drop, add_b]),
            widgets.HBox([undo_b, clear_b])
        ])

    def _get_html_tags(self, fname):
        tags = self._get_current_tags(fname)
        return (f"<div style='line-height:1.1; font-size:11px; color:#444; height:35px; overflow-y:auto; "
                f"border:1px solid #ddd; padding:2px;'><b>Tags:</b> {tags}</div>")

    def display(self):
        """Render the filter dropdown, navigation controls, and the first page of the
        tagging grid."""
        prev_btn = widgets.Button(description="Previous", icon="arrow-left")
        next_btn = widgets.Button(description="Next", icon="arrow-right")

        prev_btn.on_click(lambda _: self._change_page(-1))
        next_btn.on_click(lambda _: self._change_page(1))
        self.filter_dropdown.observe(self._on_filter_change, names='value')

        nav = widgets.HBox([prev_btn, self.page_label, next_btn], layout={'justify_content': 'center'})
        top = widgets.HBox([self.filter_dropdown], layout={'justify_content': 'center', 'padding': '10px'})

        display(top, nav, self.output_container)
        self._render_page()

    def _change_page(self, delta):
        self.current_page = max(0, self.current_page + delta)
        self._render_page()

    def _on_filter_change(self, change):
        self.filter_mode = change['new']
        self.current_page = 0
        self._render_page()
