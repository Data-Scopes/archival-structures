"""
Interactive Jupyter widget for bulk-labelling clusters of images.

Shows a paginated grid of thumbnails with a checkbox per image — checked by
default, since the common workflow is "this whole cluster is type X, uncheck
the few that don't belong" rather than picking individual images one by one.
Each image can carry multiple labels (e.g. "letter" and "handwritten" both
apply): typing a label and clicking "Add to selected" adds it to whatever
labels the selected images already have, without removing existing ones.
"Remove from selected" removes just that one label, leaving any others intact.

Labels are persisted as ``{image_path: [label, ...]}`` JSON. Older files
written as ``{image_path: label_string}`` (e.g. by hand, or by an earlier
version of this module) are read transparently — each string is wrapped into
a single-item list on load.

Note: `archival_structures.stream_analysis.groundtruth.active_learning.ActiveLearner.load_labels`
expects one string per image (single-label multiclass classification), not a
list — multi-label sets produced here aren't directly usable by that loop. If
you want to feed labels from here into `run_active_learning`, pick a single
label per image (e.g. by writing to a different file with `{k: v[0] ...}`).
"""

import io
import json
import logging
from pathlib import Path

import ipywidgets as widgets
from IPython.display import display
from PIL import Image

logger = logging.getLogger(__name__)


def _load_thumbnail_bytes(path: str, size: int = 150) -> bytes | None:
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


def _resolve_display_names(
    image_ids: list[str],
    display_names: list[str] | dict[str, str] | None,
) -> dict[str, str]:
    """
    Build an {image_id: display_name} mapping, defaulting to the filename.

    `display_names` may be:
      - None: every image falls back to `Path(image_id).name`.
      - a dict {image_id: name}: looked up per image, falling back to the
        filename for any image_id not present.
      - a list of names the same length as `image_ids`, in the same order.
    """
    if display_names is None:
        return {img_id: Path(img_id).name for img_id in image_ids}
    if isinstance(display_names, dict):
        return {img_id: display_names.get(img_id, Path(img_id).name) for img_id in image_ids}
    if len(display_names) != len(image_ids):
        raise ValueError(
            f"display_names has {len(display_names)} entries but image_ids has "
            f"{len(image_ids)} — they must be the same length and in the same order."
        )
    return dict(zip(image_ids, display_names))


def load_labels(label_file: str) -> dict[str, list[str]]:
    """
    Load a label JSON file as {image_path: [label, ...]}, or {} if it doesn't
    exist yet.

    Normalises older files saved as {image_path: label_string} (single string
    per image) by wrapping each string into a one-item list, so callers always
    see one consistent shape regardless of which version wrote the file.
    """
    path = Path(label_file)
    if not path.exists():
        return {}
    with open(path) as f:
        raw = json.load(f)
    return {
        img_id: ([value] if isinstance(value, str) else list(value))
        for img_id, value in raw.items()
    }


def save_labels(updates: dict[str, list[str]], label_file: str) -> None:
    """
    Merge `updates` into the on-disk label file and write it back.

    Reads-merges-writes (rather than overwriting) so re-opening this widget
    on a different cluster, or running it in two notebook cells, doesn't
    clobber labels assigned to *other* images. Each key in `updates` should
    already hold that image's complete, final label list — this function
    replaces the on-disk entry for that image wholesale, it does not union
    lists itself. Writes to a temp file and renames over the target, so a
    crash mid-write can't corrupt the existing file.
    """
    path = Path(label_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    on_disk = load_labels(label_file)
    on_disk.update(updates)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(on_disk, f, indent=2)
    tmp_path.replace(path)
    logger.info(f"Saved {len(updates)} label update(s) → {label_file} ({len(on_disk)} images total)")


def annotate_image_grid(
    image_ids: list[str],
    label_file: str,
    rows: int = 3,
    cols: int = 4,
    thumbnail_size: int = 150,
    label_choices: list[str] | None = None,
    default_selected: bool = True,
    display_names: list[str] | dict[str, str] | None = None,
    name_font_size: int = 10,
) -> widgets.VBox:
    """
    Display a paginated, checkbox-selectable grid of images for bulk labelling.

    Selection state persists across pages, so you can page through a large
    cluster, uncheck a few outliers wherever they appear, then click
    "Add to selected" once to label everything still checked — across all
    pages, not just the one currently visible.

    Each image can carry multiple labels. "Add to selected" adds the typed
    label to every selected image without disturbing labels they already
    have; "Remove from selected" removes just that one label.

    Args:
        image_ids:        list of image paths to display/annotate — e.g. one
                          cluster's members from
                          `get_cluster_members(clustering)[cluster_id]`, or use
                          `annotate_cluster()` below to skip that step.
        label_file:       JSON file to read existing labels from and write new
                          ones to. Format: {image_path: [label, ...]}.
        rows, cols:       grid size per page.
        thumbnail_size:   thumbnail edge length in pixels.
        label_choices:    optional list of label strings offered for
                          autocomplete; free text is still accepted.
        default_selected: whether checkboxes start checked. True suits the
                          common "whole cluster is type X, uncheck the
                          exceptions" workflow; set False if you'd rather pick
                          out a handful of images by hand.
        display_names:    optional per-image name shown instead of the
                          filename — either a dict {image_id: name} or a list
                          of names aligned with `image_ids` (same order, same
                          length). Defaults to the filename.
        name_font_size:   font size (px) for the name shown under each
                          thumbnail.

    Returns an ipywidgets.VBox. Either display it explicitly or let it
    auto-display as the last expression in a notebook cell. Labels are saved
    immediately on each "Add"/"Remove" click — there is no separate save step.
    """
    if not image_ids:
        raise ValueError("image_ids is empty — nothing to annotate.")

    name_map = _resolve_display_names(image_ids, display_names)
    existing_labels = load_labels(label_file)
    per_page = rows * cols
    n_pages = max(1, -(-len(image_ids) // per_page))  # ceil division

    state = {"page": 0}
    selected: dict[str, bool] = {img_id: default_selected for img_id in image_ids}
    checkboxes: dict[str, widgets.Checkbox] = {}

    out = widgets.Output()
    status = widgets.HTML()
    label_input = widgets.Combobox(
        placeholder="Type a label…",
        options=label_choices or [],
        description="Label:",
        ensure_option=False,
    )
    add_btn = widgets.Button(description="Add to selected", button_style="primary")
    remove_btn = widgets.Button(description="Remove from selected", button_style="danger")
    select_all_btn = widgets.Button(description="Select all (page)")
    select_none_btn = widgets.Button(description="Select none (page)")

    page_label = widgets.Label()
    first_btn = widgets.Button(description="<< First")
    prev_btn = widgets.Button(description="< Prev")
    next_btn = widgets.Button(description="Next >")
    last_btn = widgets.Button(description="Last >>")

    def _page_ids() -> list[str]:
        start = state["page"] * per_page
        return image_ids[start : start + per_page]

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
                    value=selected[img_id],
                    description="select",
                    indent=False,
                    layout=widgets.Layout(width=f"{thumbnail_size}px"),
                )

                def _on_change(change, img_id=img_id):
                    selected[img_id] = change["new"]

                cb.observe(_on_change, names="value")
                checkboxes[img_id] = cb

                current_labels = ", ".join(existing_labels.get(img_id, []))
                labels_html = widgets.HTML(
                    f"<div style='font-size:{name_font_size}px;color:#666;"
                    f"min-height:14px;text-align:center;'>{current_labels}</div>"
                )
                grid_cells.append(widgets.VBox([name_html, img_widget, cb, labels_html]))

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

    def _chosen_ids() -> list[str]:
        return [img_id for img_id, is_sel in selected.items() if is_sel]

    def _add(_btn) -> None:
        label = label_input.value.strip()
        if not label:
            status.value = "<span style='color:red;'>Enter a label first.</span>"
            return
        chosen = _chosen_ids()
        if not chosen:
            status.value = "<span style='color:red;'>No images selected.</span>"
            return
        updates = {}
        for img_id in chosen:
            current = set(existing_labels.get(img_id, []))
            current.add(label)
            existing_labels[img_id] = sorted(current)
            updates[img_id] = existing_labels[img_id]
        save_labels(updates, label_file)
        status.value = (
            f"<span style='color:green;'>Added '{label}' to {len(chosen)} images "
            f"→ {label_file}</span>"
        )
        _render()  # refresh so the new label shows under each thumbnail

    def _remove(_btn) -> None:
        label = label_input.value.strip()
        if not label:
            status.value = "<span style='color:red;'>Enter a label first.</span>"
            return
        chosen = _chosen_ids()
        if not chosen:
            status.value = "<span style='color:red;'>No images selected.</span>"
            return
        updates = {}
        n_changed = 0
        for img_id in chosen:
            current = set(existing_labels.get(img_id, []))
            if label in current:
                current.discard(label)
                n_changed += 1
            existing_labels[img_id] = sorted(current)
            updates[img_id] = existing_labels[img_id]
        save_labels(updates, label_file)
        status.value = (
            f"<span style='color:green;'>Removed '{label}' from {n_changed} images "
            f"→ {label_file}</span>"
        )
        _render()

    add_btn.on_click(_add)
    remove_btn.on_click(_remove)

    controls = widgets.HBox([first_btn, prev_btn, page_label, next_btn, last_btn])
    label_controls = widgets.HBox([label_input, add_btn, remove_btn])
    selection_controls = widgets.HBox([select_all_btn, select_none_btn])

    _render()

    return widgets.VBox([controls, label_controls, selection_controls, status, out])


def annotate_cluster(
    clustering: dict,
    cluster_id: int,
    label_file: str,
    display_names: list[str] | dict[str, str] | None = None,
    **kwargs,
) -> widgets.VBox:
    """
    Convenience wrapper: pull one cluster's image ids out of a clustering
    result and open the annotation grid for it directly.

    Args:
        clustering: a clustering result dict, e.g. from `cluster_embeddings`
                   or `cluster_inventories` (must have "image_ids"/"labels").
                   If it also has a "display_names" entry (a dict
                   {image_id: name}, or a list aligned with
                   `clustering["image_ids"]`) and `display_names` isn't passed
                   explicitly, that gets used automatically.
        cluster_id: which cluster to annotate (use -1 for outliers).
        label_file: see `annotate_image_grid`.
        display_names: explicit per-image display names; overrides anything
                   found on `clustering`. See `annotate_image_grid`.
        **kwargs:   forwarded to `annotate_image_grid`.
    """
    from archival_structures.stream_analysis.overview.clustering import get_cluster_members

    members = get_cluster_members(clustering)
    if cluster_id not in members:
        raise KeyError(
            f"No cluster {cluster_id} in this clustering result. "
            f"Available: {sorted(members.keys())}"
        )
    image_ids = members[cluster_id]

    if display_names is None and "display_names" in clustering:
        cluster_display_names = clustering["display_names"]
        if isinstance(cluster_display_names, dict):
            display_names = cluster_display_names
        else:
            # A list aligned with clustering["image_ids"] (the full set, not
            # just this cluster) — project it down to a dict so it can be
            # looked up per image regardless of which subset is shown here.
            display_names = dict(zip(clustering["image_ids"], cluster_display_names))

    return annotate_image_grid(image_ids, label_file, display_names=display_names, **kwargs)
