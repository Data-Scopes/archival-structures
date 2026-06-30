"""ipywidgets-based bulk tagging: a grid of thumbnails with a structured tag builder
(namespace/type/subtype/number, following `archival_structures.datasets.vocabulary`'s
`namespace:type(:subtype)?(#N)?` grammar) instead of free text, writing directly into each
image's `ScanAnnotation.tags` -- no separate import step.

This module has no dependency on any particular clustering pipeline, only on a plain
`image_ids: list[str]` (e.g. one cluster's members, however you got there) -- deliberately, so
it works the same way whether you cluster scans by visual/layout similarity in-package (e.g.
`archival_structures.analysis.page_layout_clustering.cluster_page_layouts`, which returns a
`pandas.Series` of cluster labels indexed by scan id) or via
`archival_structures.stream_analysis` (see `docs/stream_analysis.md`), which has its own,
unrelated `get_cluster_members`:

::

    image_ids = get_cluster_members(clustering)[cluster_id]
    annotate_image_grid_with_tags(image_ids)

This pairs with (but is unrelated to and doesn't import from)
`archival_structures.stream_analysis.groundtruth.interactive_annotation.annotate_image_grid`,
which offers the same paginated-grid/persistent-selection workflow but a free-text label
Combobox writing to a separate `{image_path: [label, ...]}` JSON, not `ScanAnnotation`.

Limitation inherited from `parse_thumb_path`: `image_ids` only resolve to the right scan where
the thumbnail filename is an exact match to the PageXML `scan.id` (true for `NL-HaNA`, not
necessarily other archives -- see `docs/findings.md`'s thumbnail-filename-convention finding).
"""
import io
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import ipywidgets as widgets
from IPython.display import display
from PIL import Image

from archival_structures.datasets.annotations import (
    ANNOTATIONS_DIR, ScanAnnotation, load_scan_annotation, load_scan_tags, parse_thumb_path,
    save_scan_annotation, scan_annotation_path,
)
from archival_structures.datasets.vocabulary import (
    CARRIER, CARRIER_TYPES, DOCTYPE, GENERIC, GENERIC_TYPES, NAMESPACES, POSITION,
    POSITION_TYPES, Tag, is_known_tag, make_tag,
)

logger = logging.getLogger(__name__)

_NAMESPACE_TYPES = {CARRIER: CARRIER_TYPES, GENERIC: GENERIC_TYPES, POSITION: POSITION_TYPES}


def type_options_for_namespace(namespace: str, doctype_suggestions: Optional[List[str]] = None
                               ) -> List[str]:
    """Suggested `type` values for `namespace` -- the controlled vocabulary's own suggestions
    for `carrier:`/`generic:`/`position:`, or `doctype_suggestions` (types already used
    elsewhere on disk, see `used_doctype_types`) for the deliberately open `doctype:`."""
    if namespace == DOCTYPE:
        return sorted(doctype_suggestions or [])
    types = _NAMESPACE_TYPES.get(namespace)
    return sorted(types.keys()) if types else []


def subtype_options_for_type(namespace: str, type_value: str) -> List[str]:
    """Suggested `subtype` values for `type_value` within `namespace`, or `[]` if `namespace`
    is `doctype:` (open, no subtype suggestions) or `type_value` has none of its own."""
    if namespace == DOCTYPE or not type_value:
        return []
    types = _NAMESPACE_TYPES.get(namespace)
    if not types:
        return []
    subtypes = types.get(type_value)
    return sorted(subtypes) if subtypes else []


def build_tag_from_inputs(namespace: str, type_value: str, subtype: str = '',
                          number_str: str = '') -> Optional[str]:
    """Build a tag string from raw widget input, or `None` if the inputs don't form a valid
    tag (missing namespace/type, a non-digit number, or characters that fail `Tag.parse`'s
    round trip, e.g. uppercase or spaces)."""
    type_value = type_value.strip()
    if not namespace or not type_value:
        return None
    subtype = subtype.strip() or None
    number_str = number_str.strip()
    if number_str and not number_str.isdigit():
        return None
    number = int(number_str) if number_str else None
    try:
        tag = make_tag(namespace, type_value, subtype, number)
        Tag.parse(tag)
    except ValueError:
        return None
    return tag


def used_doctype_types(annotations_dir: Path) -> List[str]:
    """Distinct `doctype:` types already used somewhere under `annotations_dir`, for
    autocomplete -- `doctype:` has no built-in suggestion list since it's deliberately open
    (see `archival_structures.datasets.vocabulary`), so suggestions are sourced from prior
    usage instead."""
    types = set()
    for tag in load_scan_tags(annotations_dir)['tag']:
        try:
            parsed = Tag.parse(tag)
        except ValueError:
            continue
        if parsed.namespace == DOCTYPE:
            types.add(parsed.type)
    return sorted(types)


def _load_thumbnail_bytes(path: str, size: int = 150) -> Optional[bytes]:
    """Load an image, downscale to a size x size square (padded), return JPEG bytes."""
    try:
        img = Image.open(path).convert("RGB")
        img.thumbnail((size, size))
        square = Image.new("RGB", (size, size), (240, 240, 240))
        x_off = (size - img.width) // 2
        y_off = (size - img.height) // 2
        square.paste(img, (x_off, y_off))
        buf = io.BytesIO()
        square.save(buf, format="JPEG", quality=85)
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"Could not load thumbnail for {path}: {e}")
        return None


def _resolve_display_names(image_ids: List[str],
                           display_names: Union[List[str], Dict[str, str], None]) -> Dict[str, str]:
    """Build an {image_id: display_name} mapping, defaulting to the filename."""
    if display_names is None:
        return {img_id: Path(img_id).name for img_id in image_ids}
    if isinstance(display_names, dict):
        return {img_id: display_names.get(img_id, Path(img_id).name) for img_id in image_ids}
    if len(display_names) != len(image_ids):
        raise ValueError(
            f"display_names has {len(display_names)} entries but image_ids has "
            f"{len(image_ids)} -- they must be the same length and in the same order.")
    return dict(zip(image_ids, display_names))


def annotate_image_grid_with_tags(
    image_ids: List[str],
    rows: int = 3,
    cols: int = 4,
    thumbnail_size: int = 150,
    display_names: Union[List[str], Dict[str, str], None] = None,
    name_font_size: int = 10,
    annotations_dir: Path = ANNOTATIONS_DIR,
    default_selected: bool = True,
) -> widgets.VBox:
    """
    Paginated, checkbox-selectable grid of thumbnails for bulk-tagging by cluster, with a
    structured tag builder (namespace dropdown, type/subtype comboboxes with vocabulary-driven
    suggestions, optional instance number) instead of free text -- see the module docstring for
    the tag grammar and how to feed it a cluster's image ids.

    Selection persists across pages, so you can page through a large cluster, uncheck a few
    outliers wherever they appear, then build one tag and click "Add to selected" once to tag
    everything still checked -- across all pages, not just the one currently visible.

    Each selected image's `ScanAnnotation.tags` is saved immediately on every "Add"/"Remove"
    click (no separate import step) -- `image_ids` are resolved to scans via `parse_thumb_path`
    (see its docstring for which archives this resolves correctly for).

    Args:
        image_ids:        list of image paths to display/tag, e.g. one cluster's members.
        rows, cols:        grid size per page.
        thumbnail_size:    thumbnail edge length in pixels.
        display_names:     optional per-image name shown instead of the filename -- a dict
                          {image_id: name} or a list aligned with `image_ids`.
        name_font_size:    font size (px) for the name shown under each thumbnail.
        annotations_dir:   where to read/write each scan's `ScanAnnotation` (see
                          `archival_structures.datasets.annotations`).
        default_selected:  whether checkboxes start checked -- True suits "whole cluster is
                          X, uncheck the exceptions"; False suits picking a handful by hand.

    Returns an ipywidgets.VBox. Either display it explicitly or let it auto-display as the
    last expression in a notebook cell.
    """
    if not image_ids:
        raise ValueError("image_ids is empty -- nothing to tag.")

    name_map = _resolve_display_names(image_ids, display_names)
    doctype_suggestions = used_doctype_types(annotations_dir)

    def _annotation_path(img_id: str) -> Path:
        institute_id, archive_id, inventory_num_id, scan_id = parse_thumb_path(img_id)
        return scan_annotation_path(institute_id, archive_id, inventory_num_id, scan_id,
                                    annotations_dir=annotations_dir)

    def _load_or_new(img_id: str) -> ScanAnnotation:
        path = _annotation_path(img_id)
        if path.exists():
            return load_scan_annotation(path)
        _, _, _, scan_id = parse_thumb_path(img_id)
        return ScanAnnotation(scan_id=scan_id)

    current_tags: Dict[str, List[str]] = {img_id: _load_or_new(img_id).tags for img_id in image_ids}

    per_page = rows * cols
    n_pages = max(1, -(-len(image_ids) // per_page))  # ceil division
    state = {"page": 0}
    selected: Dict[str, bool] = {img_id: default_selected for img_id in image_ids}
    checkboxes: Dict[str, widgets.Checkbox] = {}

    out = widgets.Output()
    status = widgets.HTML()
    tag_preview = widgets.HTML()

    namespace_dropdown = widgets.Dropdown(options=list(NAMESPACES), description='Namespace:')
    type_combo = widgets.Combobox(placeholder='type…', options=[], description='Type:',
                                  ensure_option=False)
    subtype_combo = widgets.Combobox(placeholder='subtype (optional)…', options=[],
                                     description='Subtype:', ensure_option=False)
    number_text = widgets.Text(placeholder='# (optional)', description='Number:')
    add_btn = widgets.Button(description="Add to selected", button_style="primary")
    remove_btn = widgets.Button(description="Remove from selected", button_style="danger")
    select_all_btn = widgets.Button(description="Select all (page)")
    select_none_btn = widgets.Button(description="Select none (page)")

    page_label = widgets.Label()
    first_btn = widgets.Button(description="<< First")
    prev_btn = widgets.Button(description="< Prev")
    next_btn = widgets.Button(description="Next >")
    last_btn = widgets.Button(description="Last >>")

    def _current_tag() -> Optional[str]:
        return build_tag_from_inputs(namespace_dropdown.value, type_combo.value,
                                     subtype_combo.value, number_text.value)

    def _update_preview(_change=None):
        if not type_combo.value.strip():
            tag_preview.value = "<i style='color:#888;'>type a tag type to see a preview</i>"
            return
        tag = _current_tag()
        if tag is None:
            tag_preview.value = ("<i style='color:#b00;'>invalid tag -- namespace/type/subtype "
                                 "must be lowercase letters/digits/underscore, number must be a "
                                 "positive integer</i>")
            return
        marker = '' if is_known_tag(tag) else (" <span style='color:#b58900;'>(not in the "
                                               "suggested vocabulary -- still fine to use)</span>")
        tag_preview.value = f"Tag: <code>{tag}</code>{marker}"

    def _on_namespace_change(_change):
        type_combo.options = type_options_for_namespace(namespace_dropdown.value, doctype_suggestions)
        type_combo.value = ''
        subtype_combo.options = []
        subtype_combo.value = ''
        _update_preview()

    def _on_type_change(_change):
        subtype_combo.options = subtype_options_for_type(namespace_dropdown.value,
                                                          type_combo.value.strip())
        subtype_combo.value = ''
        _update_preview()

    namespace_dropdown.observe(_on_namespace_change, names='value')
    type_combo.observe(_on_type_change, names='value')
    subtype_combo.observe(_update_preview, names='value')
    number_text.observe(_update_preview, names='value')
    _on_namespace_change(None)  # initialise type options for the default namespace

    def _page_ids() -> List[str]:
        start = state["page"] * per_page
        return image_ids[start:start + per_page]

    def _render() -> None:
        with out:
            out.clear_output(wait=True)
            checkboxes.clear()
            grid_cells = []
            for img_id in _page_ids():
                thumb_bytes = _load_thumbnail_bytes(img_id, size=thumbnail_size)
                if thumb_bytes:
                    img_widget = widgets.Image(
                        value=thumb_bytes, format="jpeg",
                        width=f"{thumbnail_size}px", height=f"{thumbnail_size}px",
                    )
                else:
                    img_widget = widgets.HTML(
                        f"<div style='width:{thumbnail_size}px;height:{thumbnail_size}px;"
                        "background:#eee;text-align:center;line-height:"
                        f"{thumbnail_size}px;'>missing</div>"
                    )

                name_html = widgets.HTML(
                    f"<div title='{img_id}' style='font-size:{name_font_size}px;color:#333;"
                    f"text-align:center;white-space:nowrap;overflow:hidden;"
                    f"text-overflow:ellipsis;width:{thumbnail_size}px;'>{name_map[img_id]}</div>"
                )

                cb = widgets.Checkbox(
                    value=selected[img_id], description="select", indent=False,
                    layout=widgets.Layout(width=f"{thumbnail_size}px"),
                )

                def _on_change(change, img_id=img_id):
                    selected[img_id] = change["new"]

                cb.observe(_on_change, names="value")
                checkboxes[img_id] = cb

                current = ", ".join(current_tags.get(img_id, []))
                tags_html = widgets.HTML(
                    f"<div style='font-size:{name_font_size}px;color:#666;"
                    f"min-height:14px;text-align:center;'>{current}</div>"
                )
                grid_cells.append(widgets.VBox([name_html, img_widget, cb, tags_html]))

            grid = widgets.GridBox(
                grid_cells,
                layout=widgets.Layout(
                    grid_template_columns=f"repeat({cols}, {thumbnail_size + 20}px)"
                ),
            )
            display(grid)
        page_label.value = f"Page {state['page'] + 1} / {n_pages}  ({len(image_ids)} images total)"

    def _go_to(page: int) -> None:
        state["page"] = max(0, min(page, n_pages - 1))
        _render()

    first_btn.on_click(lambda _: _go_to(0))
    prev_btn.on_click(lambda _: _go_to(state["page"] - 1))
    next_btn.on_click(lambda _: _go_to(state["page"] + 1))
    last_btn.on_click(lambda _: _go_to(n_pages - 1))

    def _select_all_page(_btn) -> None:
        for img_id in _page_ids():
            selected[img_id] = True
            checkboxes[img_id].value = True

    def _select_none_page(_btn) -> None:
        for img_id in _page_ids():
            selected[img_id] = False
            checkboxes[img_id].value = False

    select_all_btn.on_click(_select_all_page)
    select_none_btn.on_click(_select_none_page)

    def _chosen_ids() -> List[str]:
        return [img_id for img_id, is_sel in selected.items() if is_sel]

    def _add(_btn) -> None:
        tag = _current_tag()
        if tag is None:
            status.value = "<span style='color:red;'>Build a valid namespace + type first.</span>"
            return
        chosen = _chosen_ids()
        if not chosen:
            status.value = "<span style='color:red;'>No images selected.</span>"
            return
        n_added = 0
        for img_id in chosen:
            annotation = _load_or_new(img_id)
            if tag not in annotation.tags:
                annotation.tags.append(tag)
                save_scan_annotation(annotation, _annotation_path(img_id))
                current_tags[img_id] = annotation.tags
                n_added += 1
        if namespace_dropdown.value == DOCTYPE:
            doctype_type = Tag.parse(tag).type
            if doctype_type not in doctype_suggestions:
                doctype_suggestions.append(doctype_type)
                doctype_suggestions.sort()
                type_combo.options = doctype_suggestions
        status.value = f"<span style='color:green;'>Added '{tag}' to {n_added} image(s).</span>"
        _render()

    def _remove(_btn) -> None:
        tag = _current_tag()
        if tag is None:
            status.value = "<span style='color:red;'>Build a valid namespace + type first.</span>"
            return
        chosen = _chosen_ids()
        if not chosen:
            status.value = "<span style='color:red;'>No images selected.</span>"
            return
        n_removed = 0
        for img_id in chosen:
            annotation = _load_or_new(img_id)
            if tag in annotation.tags:
                annotation.tags.remove(tag)
                save_scan_annotation(annotation, _annotation_path(img_id))
                current_tags[img_id] = annotation.tags
                n_removed += 1
        status.value = f"<span style='color:green;'>Removed '{tag}' from {n_removed} image(s).</span>"
        _render()

    add_btn.on_click(_add)
    remove_btn.on_click(_remove)

    controls = widgets.HBox([first_btn, prev_btn, page_label, next_btn, last_btn])
    tag_controls = widgets.HBox([namespace_dropdown, type_combo, subtype_combo, number_text])
    action_controls = widgets.HBox([add_btn, remove_btn])
    selection_controls = widgets.HBox([select_all_btn, select_none_btn])

    _update_preview()
    _render()

    return widgets.VBox([controls, tag_controls, tag_preview, action_controls,
                         selection_controls, status, out])
