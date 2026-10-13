"""An ipywidgets-based scrolling filmstrip for annotating document boundaries between
*consecutive* pages in a page stream.

This is a different task from {func}`~archival_structures.stream_analysis.groundtruth.interactive_annotation.annotate_image_grid`:
that tool bulk-labels an *unordered* cluster of images with checkboxes ("this whole set is type
X"). This one is for the sequential-adjacency judgement behind Framing B document segmentation
(see `docs/challenges/document_segmentation.md#framing-b-pairwise-same-document-adjacency`): for
each pair of *consecutive* pages, does a document boundary fall between them? A checkbox grid has
no notion of "between", so it isn't reusable for this -- what's reused instead is the general
paginated-widget shape and the autosave pattern from `interactive_annotation.py`.

The filmstrip always shows several *consecutive* thumbnails around the page currently being
judged, not an isolated pair -- a page's neighbours often matter for judging whether it's really
a new document's start (see the worked example in
`docs/challenges/simulation_harness.md`'s Label Studio sketch, where an isolated pairwise view
was flagged as losing exactly this context).

Output is stored as a nullable list aligned with `image_ids` (length `len(image_ids) - 1`) so
partial progress across a long sequence survives being closed and reopened; `to_ground_truth_pairs`
converts a *complete* pass into the strict `list[bool]` shape
{func}`archival_structures.stream_analysis.corruption.metrics.true_same_document_pairs` already
produces from real ground truth, and `to_inventory_ground_truth` turns that into a fresh
{class}`~archival_structures.annotation.ground_truth.InventoryGroundTruth` "document" layer via
{func}`archival_structures.stream_analysis.corruption.metrics.pairs_to_groups`.
"""

from __future__ import annotations

import io
import json
import logging
import tempfile
from pathlib import Path

import ipywidgets as widgets
from IPython.display import display
from PIL import Image

logger = logging.getLogger(__name__)

try:
    from ipyevents import Event as _DomEvent
except ImportError:  # pragma: no cover - keyboard support degrades gracefully without it
    _DomEvent = None

_FULLRES_CACHE_DIR = Path(tempfile.gettempdir()) / "archival_structures_boundary_annotation"


# ---------------------------------------------------------------------------
# Image loading
# ---------------------------------------------------------------------------

def _load_square_thumbnail_bytes(path: str, size: int = 180) -> bytes | None:
    """Load an image, downscale to a size x size square (padded), return JPEG bytes -- same
    approach as `interactive_annotation._load_thumbnail_bytes`, kept separate here since the
    detail-pane loader below has different (non-padded) framing needs."""
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


def _load_detail_bytes(path: str, max_dim: int = 600) -> bytes | None:
    """Load an image, downscale to fit within max_dim x max_dim (aspect ratio preserved, no
    padding), return JPEG bytes -- used for the larger "quick look" detail pane."""
    try:
        img = Image.open(path).convert("RGB")
        img.thumbnail((max_dim, max_dim))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
        return buf.getvalue()
    except Exception as e:
        logger.warning(f"Could not load detail image for {path}: {e}")
        return None


def _browser_renderable_copy(path: str) -> Path | None:
    """A guaranteed-`.jpg`-extensioned copy of `path`'s full-resolution image, suitable for a
    `file://` link opened in a real browser tab.

    Some collections' files are misleadingly named: `NL-AsdSAA_89_3.1`'s `*.jp2` files are
    actually baseline JPEG bytes under a `.jp2` extension (confirmed with `file(1)` -- PIL opens
    them fine, since it sniffs content, but a browser navigating a `file://` link infers
    `image/jp2` -- true JPEG2000, which mainstream browsers don't render -- from the extension,
    regardless of what the bytes actually are). Rather than link directly to the source file,
    this always re-saves a `.jpg` copy to a temp cache dir first (keyed by the resolved source
    path, so repeat clicks on the same image reuse the cached copy instead of re-encoding), and
    returns that path instead -- correct regardless of a collection's on-disk naming.

    Returns `None` (rather than raising) if the source image can't be opened at all, so a single
    unreadable file degrades to a status message in the detail pane instead of crashing the
    widget.
    """
    try:
        resolved = Path(path).resolve()
        safe_name = f"{abs(hash(str(resolved)))}.jpg"
        out_path = _FULLRES_CACHE_DIR / safe_name
        if out_path.exists():
            return out_path
        img = Image.open(resolved).convert("RGB")
        _FULLRES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        img.save(out_path, format="JPEG", quality=95)
        return out_path
    except Exception as e:
        logger.warning(f"Could not create a browser-renderable copy of {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Label persistence
# ---------------------------------------------------------------------------

def load_boundary_labels(image_ids: list[str], label_file: str) -> list[bool | None]:
    """Load the `same_document` list (length `len(image_ids) - 1`) from `label_file`, or a
    fresh all-`None` (unannotated) list if the file doesn't exist yet.

    Raises `ValueError` if the file exists but its stored `image_ids` don't match the ones
    passed in -- reopening this widget against a different (or reordered) sequence would
    otherwise silently misalign every label with the wrong pair.
    """
    path = Path(label_file)
    if not path.exists():
        return [None] * max(0, len(image_ids) - 1)
    with open(path) as f:
        data = json.load(f)
    if data.get("image_ids") != image_ids:
        raise ValueError(
            f"{label_file} was saved for a different image_ids sequence "
            f"({len(data.get('image_ids', []))} images) than the {len(image_ids)} passed in -- "
            "labels would silently misalign if loaded against the wrong sequence."
        )
    return list(data["same_document"])


def save_boundary_labels(
    image_ids: list[str], same_document: list[bool | None], label_file: str,
) -> None:
    """Write `image_ids` and `same_document` to `label_file`. Writes to a temp file and renames
    over the target, so a crash mid-write can't corrupt the existing file -- same pattern as
    `interactive_annotation.save_labels`."""
    path = Path(label_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump({"image_ids": image_ids, "same_document": same_document}, f, indent=2)
    tmp_path.replace(path)


def to_ground_truth_pairs(same_document: list[bool | None]) -> list[bool]:
    """`same_document` with every entry resolved (no `None`s left) -- the exact shape
    {func}`archival_structures.stream_analysis.corruption.metrics.true_same_document_pairs`
    produces from real ground truth, ready to hand to
    {func}`~archival_structures.stream_analysis.corruption.metrics.pairs_to_groups` or any
    Framing-B scoring function.

    Raises `ValueError` naming how many gaps are still unmarked, rather than silently treating
    an incomplete pass as if every unmarked gap meant "same document".
    """
    n_unmarked = sum(1 for v in same_document if v is None)
    if n_unmarked:
        raise ValueError(
            f"{n_unmarked} of {len(same_document)} gaps are still unmarked -- "
            "finish annotating before converting to ground truth."
        )
    return list(same_document)  # type: ignore[return-value]


def to_inventory_ground_truth(
    image_ids: list[str],
    same_document: list[bool | None],
    inventory_id: str,
    ead_ref: dict[str, str],
    document_type: str = "mixed",
    doc_id_prefix: str = "doc",
):
    """A fresh {class}`~archival_structures.annotation.ground_truth.InventoryGroundTruth` with
    one `"document"` layer, built from a *complete* boundary-annotation pass: groups `image_ids`
    by the boundaries in `same_document` (via
    {func}`~archival_structures.stream_analysis.corruption.metrics.pairs_to_groups`) and wraps
    each group as an {class}`~archival_structures.annotation.ground_truth.AnnotationElement`
    with one whole-image `page_side="full"` {class}`~archival_structures.annotation.ground_truth.Span`
    per member.

    Raises `ValueError` (via `to_ground_truth_pairs`) if `same_document` isn't fully annotated
    yet.
    """
    from archival_structures.annotation.ground_truth import (
        AnnotationElement,
        InventoryGroundTruth,
        Span,
    )
    from archival_structures.stream_analysis.corruption.metrics import pairs_to_groups

    pairs = to_ground_truth_pairs(same_document)
    group_ids = pairs_to_groups(pairs)

    members_by_group: dict[int, list[str]] = {}
    for image_id, group_id in zip(image_ids, group_ids):
        members_by_group.setdefault(group_id, []).append(image_id)

    elements = [
        AnnotationElement(
            id=f"{doc_id_prefix}_{i + 1:04d}",
            type="document",
            spans=[Span(scan_id=image_id, page_side="full") for image_id in members],
        )
        for i, (_group_id, members) in enumerate(sorted(members_by_group.items()))
    ]

    return InventoryGroundTruth(
        inventory_id=inventory_id,
        ead_ref=ead_ref,
        document_type=document_type,
        layers={"document": elements},
    )


# ---------------------------------------------------------------------------
# Windowing (pure logic, kept separate from the widget for direct testability)
# ---------------------------------------------------------------------------

def compute_window(cursor: int, n_images: int, window_size: int) -> tuple[int, int]:
    """The `[win_start, win_end)` range of thumbnail indices to display so that the gap at
    `cursor` (between thumbnails `cursor` and `cursor + 1`) stays visible, roughly centred,
    clamped to the sequence's edges -- what makes the filmstrip scroll smoothly as `cursor`
    moves via the keyboard instead of jumping between fixed pages.
    """
    window_size = max(2, min(window_size, n_images))
    left_margin = window_size // 2
    win_start = cursor - left_margin + 1
    win_start = max(0, min(win_start, n_images - window_size))
    return win_start, win_start + window_size


def next_unmarked(same_document: list[bool | None], after: int) -> int | None:
    """The index of the first unmarked (`None`) gap strictly after `after`, wrapping around to
    the start of the sequence if none is found before the end; `None` if every gap is marked."""
    n = len(same_document)
    for offset in range(1, n + 1):
        idx = (after + offset) % n
        if same_document[idx] is None:
            return idx
    return None


# ---------------------------------------------------------------------------
# The widget
# ---------------------------------------------------------------------------

_STATE_STYLE = {
    None: ("?", "#999", "#f0f0f0"),
    True: ("SAME", "#fff", "#2e7d32"),
    False: ("BOUNDARY", "#fff", "#c62828"),
}


def annotate_page_boundaries(
    image_ids: list[str],
    label_file: str,
    window_size: int = 9,
    thumbnail_size: int = 180,
    detail_max_dim: int = 600,
    display_names: list[str] | dict[str, str] | None = None,
) -> widgets.VBox:
    """Scrolling-filmstrip widget for marking, for every adjacent pair of pages in `image_ids`,
    whether a document boundary falls between them.

    Mouse: click a gap's toggle button to cycle it through unmarked -> same document ->
    boundary -> unmarked; click a thumbnail to show it enlarged in the detail pane below the
    filmstrip, alongside an "Open full image" link that opens it in a new browser tab (via
    `_browser_renderable_copy`, so this works even for collections whose files are misleadingly
    named, like `NL-AsdSAA_89_3.1`'s `.jp2`-named JPEGs).

    Keyboard (needs the `ipyevents` package -- degrades to mouse-only with a status note if it
    isn't installed; click once into the filmstrip first to give it focus):

    - Left / Right arrow: move the active gap (highlighted with a blue outline) left/right.
    - '1': mark the active gap "same document" and advance to the next gap.
    - '2': mark the active gap "boundary" and advance to the next gap.
    - '0' / Backspace: clear the active gap's mark (does not advance).
    - 'u': jump to the next unmarked gap (wraps around; reports when everything is marked).

    Progress and every mark are saved immediately to `label_file` (see `save_boundary_labels`) --
    there is no separate save step, and reopening this widget on the same `image_ids` resumes
    exactly where a previous session left off.

    Args:
        image_ids: page thumbnails in stream order -- e.g. sorted scan filenames for one
            inventory number.
        label_file: JSON file to read existing progress from and write new marks to.
        window_size: how many consecutive thumbnails to show at once.
        thumbnail_size: filmstrip thumbnail edge length in pixels.
        detail_max_dim: detail-pane image's max width/height in pixels.
        display_names: optional per-image name shown instead of the filename -- see
            `interactive_annotation.annotate_image_grid` for the accepted shapes.
    """
    if len(image_ids) < 2:
        raise ValueError("image_ids needs at least 2 images to have any adjacent pairs.")

    name_map = _resolve_display_names(image_ids, display_names)
    same_document = load_boundary_labels(image_ids, label_file)
    n_images = len(image_ids)
    n_gaps = n_images - 1

    state = {"cursor": 0, "detail_idx": 0}

    filmstrip_out = widgets.Output()
    detail_out = widgets.Output()
    status = widgets.HTML()
    progress_label = widgets.Label()
    keyboard_hint = widgets.HTML(
        "<div style='color:#666;font-size:12px;'>"
        "Click the filmstrip once to focus it, then: "
        "&larr;/&rarr; move &middot; 1 = same document (advances) &middot; "
        "2 = boundary (advances) &middot; 0/Backspace = clear &middot; u = jump to next unmarked"
        "</div>"
        if _DomEvent is not None else
        "<div style='color:#b71c1c;font-size:12px;'>"
        "ipyevents isn't installed -- keyboard shortcuts are unavailable; use the gap buttons "
        "below to mark boundaries by mouse."
        "</div>"
    )

    def _save() -> None:
        save_boundary_labels(image_ids, same_document, label_file)

    def _render_progress() -> None:
        n_marked = sum(1 for v in same_document if v is not None)
        n_boundaries = sum(1 for v in same_document if v is False)
        progress_label.value = (
            f"{n_marked} / {n_gaps} gaps marked ({n_boundaries} boundaries so far) -- "
            f"active gap {state['cursor'] + 1} / {n_gaps}"
        )

    def _render_detail() -> None:
        image_id = image_ids[state["detail_idx"]]
        with detail_out:
            detail_out.clear_output(wait=True)
            detail_bytes = _load_detail_bytes(image_id, max_dim=detail_max_dim)
            children = [widgets.HTML(f"<b>{name_map[image_id]}</b>")]
            if detail_bytes:
                children.append(widgets.Image(value=detail_bytes, format="jpeg"))
            else:
                children.append(widgets.HTML("<i>could not load image</i>"))
            full_copy = _browser_renderable_copy(image_id)
            if full_copy is not None:
                children.append(widgets.HTML(
                    f'<a href="file://{full_copy}" target="_blank">Open full image &#8599;</a>'
                ))
            display(widgets.VBox(children))

    def _set_gap(idx: int, value: bool | None, advance: bool) -> None:
        same_document[idx] = value
        _save()
        if advance and idx < n_gaps - 1:
            state["cursor"] = idx + 1
        _render_filmstrip()

    def _make_gap_button(idx: int) -> widgets.Button:
        label, color, bg = _STATE_STYLE[same_document[idx]]
        btn = widgets.Button(
            description=label,
            layout=widgets.Layout(width="70px", height=f"{thumbnail_size}px"),
        )
        btn.style.button_color = bg
        btn.style.text_color = color
        if idx == state["cursor"]:
            btn.layout.border = "3px solid #1565c0"

        def _on_click(_btn, idx=idx):
            current = same_document[idx]
            next_value = {None: True, True: False, False: None}[current]
            state["cursor"] = idx
            _set_gap(idx, next_value, advance=False)

        btn.on_click(_on_click)
        return btn

    def _make_thumb_widget(idx: int) -> widgets.Widget:
        image_id = image_ids[idx]
        thumb_bytes = _load_square_thumbnail_bytes(image_id, size=thumbnail_size)
        if thumb_bytes:
            img_widget = widgets.Image(
                value=thumb_bytes, format="jpeg",
                width=f"{thumbnail_size}px", height=f"{thumbnail_size}px",
            )
        else:
            img_widget = widgets.HTML(
                f"<div style='width:{thumbnail_size}px;height:{thumbnail_size}px;"
                "background:#eee;text-align:center;'>missing</div>"
            )

        name_html = widgets.HTML(
            f"<div title='{image_id}' style='font-size:10px;color:#333;text-align:center;"
            f"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
            f"width:{thumbnail_size}px;'>{name_map[image_id]}</div>"
        )
        # A plain Button (native .on_click, no ipyevents involved) rather than a click listener
        # on the Image itself: Image isn't natively clickable in ipywidgets, and an ipyevents
        # click listener on a widget rendered inside an Output's dynamically-managed content
        # (as these thumbnails are, inside `filmstrip_out`) proved unreliable in practice -- a
        # Button is guaranteed to work regardless of environment.
        enlarge_btn = widgets.Button(
            description="🔍 view", layout=widgets.Layout(width=f"{thumbnail_size}px"),
        )

        def _on_enlarge(_btn, idx=idx):
            state["detail_idx"] = idx
            _render_detail()

        enlarge_btn.on_click(_on_enlarge)
        return widgets.VBox([name_html, img_widget, enlarge_btn])

    def _render_filmstrip() -> None:
        win_start, win_end = compute_window(state["cursor"], n_images, window_size)
        with filmstrip_out:
            filmstrip_out.clear_output(wait=True)
            cells: list[widgets.Widget] = []
            for idx in range(win_start, win_end):
                cells.append(_make_thumb_widget(idx))
                if idx < win_end - 1:
                    cells.append(_make_gap_button(idx))
            display(widgets.HBox(cells, layout=widgets.Layout(overflow="visible")))
        _render_progress()

    def _move_cursor(delta: int) -> None:
        state["cursor"] = max(0, min(n_gaps - 1, state["cursor"] + delta))
        _render_filmstrip()

    def _jump_unmarked() -> None:
        target = next_unmarked(same_document, state["cursor"])
        if target is None:
            status.value = "<span style='color:green;'>All gaps are marked.</span>"
            return
        status.value = ""
        state["cursor"] = target
        _render_filmstrip()

    def _on_keydown(event: dict) -> None:
        # this handler is registered for both 'click' and 'keydown' (see the Event below) --
        # the 'click' watcher's only purpose is to make ipyevents grab browser focus onto
        # `container` so that subsequent keydown events actually reach it, rather than falling
        # through to JupyterLab's own global keyboard shortcuts; it isn't itself an action here.
        if event.get("type") != "keydown":
            return
        key = event.get("key")
        if key == "ArrowRight":
            _move_cursor(1)
        elif key == "ArrowLeft":
            _move_cursor(-1)
        elif key == "1":
            _set_gap(state["cursor"], True, advance=True)
        elif key == "2":
            _set_gap(state["cursor"], False, advance=True)
        elif key in ("0", "Backspace"):
            _set_gap(state["cursor"], None, advance=False)
        elif key in ("u", "U"):
            _jump_unmarked()

    container = widgets.VBox([
        keyboard_hint,
        progress_label,
        filmstrip_out,
        widgets.HTML("<hr/>"),
        detail_out,
        status,
    ])

    if _DomEvent is not None:
        # 'click' must be watched alongside 'keydown' on the *same* Event/source -- ipyevents
        # only grabs browser focus for a source element in response to a mouse event on it (see
        # its own docs' one keydown example, which watches ['click', 'keydown', 'mouseenter',
        # 'touchmove'] together); a keydown-only watcher never receives focus at all, so keydown
        # events keep going to whatever already had focus (in JupyterLab, that falls through to
        # its own global keyboard shortcuts -- confirmed the hard way).
        keydown_listener = _DomEvent(
            source=container, watched_events=["click", "keydown"], prevent_default_action=True,
        )
        keydown_listener.on_dom_event(_on_keydown)
        container._boundary_annotation_keydown_listener = keydown_listener

    _render_filmstrip()
    _render_detail()

    return container


def _resolve_display_names(
    image_ids: list[str],
    display_names: list[str] | dict[str, str] | None,
) -> dict[str, str]:
    """Same contract as `interactive_annotation._resolve_display_names` -- duplicated locally
    rather than imported to keep this module usable on its own without pulling in the checkbox
    grid tool's dependencies."""
    if display_names is None:
        return {img_id: Path(img_id).name for img_id in image_ids}
    if isinstance(display_names, dict):
        return {img_id: display_names.get(img_id, Path(img_id).name) for img_id in image_ids}
    if len(display_names) != len(image_ids):
        raise ValueError(
            f"display_names has {len(display_names)} entries but image_ids has "
            f"{len(image_ids)} -- they must be the same length and in the same order."
        )
    return dict(zip(image_ids, display_names))
