#!/usr/bin/env python3
"""
crop_borders.py
===============

Remove black and/or white scanner borders from images of scanned/digitised
documents while keeping the result rectangular. Designed to be tolerant of
rough document edges, small tears, and missing corners.

Two cropping methods are supported:

  * "profile" (default) - Compute the fraction of "border-like" pixels in
    each row and column, then walk inward from each side until that fraction
    drops below a tolerance for several rows/columns in a row. Very robust
    to ragged edges; rectangular by construction.

  * "component" - Invert the border mask, find the largest connected
    non-border component, and use its bounding box (optionally shrunk
    inward by a few pixels). More sensitive to noise, but handles documents
    that don't fill most of the scan well.

Both methods share a common preprocessing pipeline:
    grayscale -> auto-detect border polarity -> threshold -> morphological
    cleanup -> mask.

Usage
-----
    # Single file
    python crop_borders.py input.jpg output.jpg

    # Whole directory (recursive); writes cropped copies to output_dir/
    python crop_borders.py ./scans ./scans_cropped

    # Use the connected-component method
    python crop_borders.py input.tif output.tif --method component

    # Force a polarity if auto-detection misbehaves
    python crop_borders.py input.png output.png --polarity black

Run with --help for the full list of tunables.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterator, Optional, Tuple

import cv2
import numpy as np


SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}

CropBox = Tuple[int, int, int, int]  # (top, bottom, left, right); slice as img[top:bottom, left:right]


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def to_gray(img: np.ndarray) -> np.ndarray:
    """Convert any 1/3/4-channel image to single-channel grayscale."""
    if img.ndim == 2:
        return img
    if img.shape[2] == 4:
        return cv2.cvtColor(img, cv2.COLOR_BGRA2GRAY)
    if img.shape[2] == 3:
        return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Fallback: take the first channel
    return img[:, :, 0]


def detect_border_polarity(
    gray: np.ndarray,
    frame_frac: float = 0.02,
    dark_value: int = 60,
    light_value: int = 200,
    presence_thresh: float = 0.30,
) -> str:
    """
    Inspect a thin frame around the image to decide whether the border is
    predominantly 'black', 'white', or 'both'.
    """
    h, w = gray.shape
    fh = max(1, int(h * frame_frac))
    fw = max(1, int(w * frame_frac))

    frame_pixels = np.concatenate([
        gray[:fh, :].ravel(),
        gray[-fh:, :].ravel(),
        gray[:, :fw].ravel(),
        gray[:, -fw:].ravel(),
    ])

    dark_frac = float(np.mean(frame_pixels < dark_value))
    light_frac = float(np.mean(frame_pixels > light_value))

    if dark_frac > presence_thresh and light_frac > presence_thresh:
        return "both"
    if dark_frac >= light_frac:
        return "black"
    return "white"


def build_border_mask(
    gray: np.ndarray,
    polarity: str,
    black_thresh: int = 30,
    white_thresh: int = 225,
) -> np.ndarray:
    """Return a uint8 mask: 255 = border-like pixel, 0 = content-like pixel."""
    mask = np.zeros_like(gray, dtype=np.uint8)
    if polarity in ("black", "both"):
        mask = cv2.bitwise_or(mask, ((gray < black_thresh).astype(np.uint8) * 255))
    if polarity in ("white", "both"):
        mask = cv2.bitwise_or(mask, ((gray > white_thresh).astype(np.uint8) * 255))
    return mask


def clean_mask(mask: np.ndarray, kernel_size: int = 3) -> np.ndarray:
    """Open then close to remove specks inside the document and to fill
    small gaps in the border (e.g. due to tears)."""
    if kernel_size <= 0:
        return mask
    k = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_size, kernel_size))
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, k)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, k)
    return mask


# ---------------------------------------------------------------------------
# Method 1: row/column projection profile
# ---------------------------------------------------------------------------

def _first_run_below(profile: np.ndarray, tolerance: float, consecutive: int, limit: int) -> int:
    """
    Walk forward from index 0. Return the index of the first row/column for
    which `consecutive` successive entries are below `tolerance`. If that
    never happens before reaching `limit + consecutive`, return `limit` as
    a safety fallback.
    """
    run = 0
    end = min(len(profile), limit + consecutive)
    for i in range(end):
        if profile[i] < tolerance:
            run += 1
            if run >= consecutive:
                return i - consecutive + 1
        else:
            run = 0
    return limit


def _last_run_below(profile: np.ndarray, tolerance: float, consecutive: int, limit: int) -> int:
    """
    Walk backward from the last index. Return the (exclusive) index just past
    the last row/column for which `consecutive` successive entries are below
    `tolerance`. If never found, return `limit` as a safety fallback.
    """
    run = 0
    n = len(profile)
    stop = max(limit - consecutive, -1)
    for i in range(n - 1, stop, -1):
        if profile[i] < tolerance:
            run += 1
            if run >= consecutive:
                return i + consecutive
        else:
            run = 0
    return limit


def crop_via_profile(
    mask: np.ndarray,
    tolerance: float = 0.5,
    consecutive: int = 5,
    max_crop_frac: float = 0.20,
) -> CropBox:
    """
    Determine a rectangular crop by walking inward from each side, looking
    for the first stable run of rows/columns whose border fraction drops
    below `tolerance`.
    """
    h, w = mask.shape
    border = mask.astype(np.float32) / 255.0

    row_profile = border.mean(axis=1)
    col_profile = border.mean(axis=0)

    max_top = int(h * max_crop_frac)
    max_bottom = h - int(h * max_crop_frac)
    max_left = int(w * max_crop_frac)
    max_right = w - int(w * max_crop_frac)

    top = _first_run_below(row_profile, tolerance, consecutive, max_top)
    bottom = _last_run_below(row_profile, tolerance, consecutive, max_bottom)
    left = _first_run_below(col_profile, tolerance, consecutive, max_left)
    right = _last_run_below(col_profile, tolerance, consecutive, max_right)

    # Last-resort sanity in case the entire image looked like border.
    if bottom <= top:
        top, bottom = 0, h
    if right <= left:
        left, right = 0, w

    return top, bottom, left, right


# ---------------------------------------------------------------------------
# Method 2: largest connected non-border component
# ---------------------------------------------------------------------------

def crop_via_largest_component(
    mask: np.ndarray,
    shrink: int = 2,
    max_crop_frac: float = 0.20,
) -> CropBox:
    """
    Invert the border mask, find the largest connected component (assumed to
    be the document body), and use its axis-aligned bounding box (optionally
    shrunk inward by `shrink` pixels). The result is then clamped so it
    never crops more than `max_crop_frac` of any dimension.
    """
    h, w = mask.shape
    content = cv2.bitwise_not(mask)

    num_labels, _, stats, _ = cv2.connectedComponentsWithStats(content, connectivity=8)
    if num_labels <= 1:
        return 0, h, 0, w

    # stats[0] is the background label (border). Pick the largest content blob.
    areas = stats[1:, cv2.CC_STAT_AREA]
    largest = 1 + int(np.argmax(areas))
    x = int(stats[largest, cv2.CC_STAT_LEFT])
    y = int(stats[largest, cv2.CC_STAT_TOP])
    cw = int(stats[largest, cv2.CC_STAT_WIDTH])
    ch = int(stats[largest, cv2.CC_STAT_HEIGHT])

    top, left = y, x
    bottom, right = y + ch, x + cw

    # Shrink inward to drop frayed edges that just barely got included.
    top += shrink
    left += shrink
    bottom -= shrink
    right -= shrink

    # Apply per-side safety caps: never crop more than max_crop_frac of a side.
    cap_top = int(h * max_crop_frac)
    cap_left = int(w * max_crop_frac)
    cap_bottom = h - int(h * max_crop_frac)
    cap_right = w - int(w * max_crop_frac)

    top = min(top, cap_top)
    left = min(left, cap_left)
    bottom = max(bottom, cap_bottom)
    right = max(right, cap_right)

    # Clamp to image bounds and guard against degenerate boxes.
    top = max(0, top)
    left = max(0, left)
    bottom = min(h, bottom)
    right = min(w, right)
    if bottom <= top:
        top, bottom = 0, h
    if right <= left:
        left, right = 0, w

    return top, bottom, left, right


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def crop_image(
    img: np.ndarray,
    method: str = "profile",
    polarity: Optional[str] = None,
    black_thresh: int = 30,
    white_thresh: int = 225,
    morph_kernel: int = 3,
    tolerance: float = 0.5,
    consecutive: int = 5,
    shrink: int = 2,
    max_crop_frac: float = 0.20,
) -> Tuple[np.ndarray, CropBox, str]:
    """
    Run the full pipeline on a single image.

    Returns
    -------
    cropped : np.ndarray
        The cropped image (same dtype/channel count as input).
    box : (top, bottom, left, right)
        Slice indices into the original image.
    polarity_used : str
        Which border polarity was used ('black', 'white', or 'both').
    """
    gray = to_gray(img)

    polarity_used = polarity if polarity is not None else detect_border_polarity(gray)
    mask = build_border_mask(gray, polarity_used, black_thresh, white_thresh)
    mask = clean_mask(mask, morph_kernel)

    if method == "profile":
        box = crop_via_profile(mask, tolerance, consecutive, max_crop_frac)
    elif method == "component":
        box = crop_via_largest_component(mask, shrink, max_crop_frac)
    else:
        raise ValueError(f"Unknown method: {method!r}")

    top, bottom, left, right = box
    cropped = img[top:bottom, left:right]
    return cropped, box, polarity_used


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def iter_images(path: Path) -> Iterator[Path]:
    """Yield image paths from a single file or recursively from a directory."""
    if path.is_file():
        yield path
    elif path.is_dir():
        for p in sorted(path.rglob("*")):
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
                yield p


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI argument parser for this script's `input`/`output`/cropping-method/threshold
    options (see the module docstring for the two cropping methods)."""
    p = argparse.ArgumentParser(
        description="Crop black/white scanner borders from document images.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("input", type=Path, help="Input image file or directory.")
    p.add_argument("output", type=Path,
                   help="Output file (if input is a file) or directory (if input is a directory).")
    p.add_argument("--method", choices=["profile", "component"], default="profile",
                   help="Cropping method.")
    p.add_argument("--polarity", choices=["auto", "black", "white", "both"], default="auto",
                   help="Border polarity. 'auto' inspects each image's frame.")
    p.add_argument("--black-thresh", type=int, default=30,
                   help="Pixels darker than this are 'black' border.")
    p.add_argument("--white-thresh", type=int, default=225,
                   help="Pixels brighter than this are 'white' border.")
    p.add_argument("--morph-kernel", type=int, default=3,
                   help="Morphological kernel size for cleanup; 0 disables.")
    p.add_argument("--tolerance", type=float, default=0.5,
                   help="(profile) Accept a row/col when its border fraction is below this.")
    p.add_argument("--consecutive", type=int, default=5,
                   help="(profile) Require this many consecutive accepted rows/cols.")
    p.add_argument("--shrink", type=int, default=2,
                   help="(component) Inward shrink in pixels around the bounding box.")
    p.add_argument("--max-crop-frac", type=float, default=0.20,
                   help="Per-side safety cap, as fraction of that dimension.")
    p.add_argument("--verbose", "-v", action="store_true", help="Print per-image summary.")
    return p


def main(argv: Optional[list] = None) -> int:
    """CLI entry point: crop borders from `args.input` (a file or directory of images) and
    write the result(s) to `args.output`. Returns a process exit code."""
    args = build_arg_parser().parse_args(argv)
    polarity = None if args.polarity == "auto" else args.polarity

    inputs = list(iter_images(args.input))
    if not inputs:
        print(f"No images found at {args.input}", file=sys.stderr)
        return 1

    is_batch = args.input.is_dir()
    if is_batch:
        args.output.mkdir(parents=True, exist_ok=True)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)

    n_ok = 0
    for in_path in inputs:
        img = cv2.imread(str(in_path), cv2.IMREAD_UNCHANGED)
        if img is None:
            print(f"  ! could not read {in_path}", file=sys.stderr)
            continue
        try:
            cropped, box, polarity_used = crop_image(
                img,
                method=args.method,
                polarity=polarity,
                black_thresh=args.black_thresh,
                white_thresh=args.white_thresh,
                morph_kernel=args.morph_kernel,
                tolerance=args.tolerance,
                consecutive=args.consecutive,
                shrink=args.shrink,
                max_crop_frac=args.max_crop_frac,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  ! failed on {in_path}: {e}", file=sys.stderr)
            continue

        out_path = args.output / in_path.name if is_batch else args.output
        out_path.parent.mkdir(parents=True, exist_ok=True)
        ok = cv2.imwrite(str(out_path), cropped)
        if not ok:
            print(f"  ! could not write {out_path}", file=sys.stderr)
            continue

        n_ok += 1
        if args.verbose:
            h, w = img.shape[:2]
            t, b, l, r = box
            print(
                f"{in_path.name}: {w}x{h} -> {r-l}x{b-t}  "
                f"crop=(top={t},bot={h-b},left={l},right={w-r})  polarity={polarity_used}"
            )

    if args.verbose:
        print(f"Done. Wrote {n_ok}/{len(inputs)} images.")
    return 0 if n_ok > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
