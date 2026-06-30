"""
Image loading utilities shared across both pipeline parts.
"""

import json
import logging
from pathlib import Path

import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)


def collect_image_paths(input_dir: str, extensions: list[str]) -> list[Path]:
    """Recursively collect all image paths with given extensions."""
    input_path = Path(input_dir)
    paths = []
    for ext in extensions:
        paths.extend(input_path.rglob(f"*.{ext}"))
        paths.extend(input_path.rglob(f"*.{ext.upper()}"))
    paths = sorted(set(paths))
    # DEBUG, not INFO: this is called once per directory by callers that may
    # scan thousands of directories (e.g. discover_inventories). One INFO
    # line per call at that scale can flood Jupyter's iopub channel.
    logger.debug(f"Found {len(paths)} images in {input_dir}")
    return paths


def load_image(path: Path, target_size: tuple[int, int] | None = None) -> np.ndarray | None:
    """
    Load an image as an RGB numpy array.
    Optionally resize to target_size (width, height).
    Returns None on failure.
    """
    try:
        img = Image.open(path).convert("RGB")
        if target_size is not None:
            img = img.resize(target_size, Image.LANCZOS)
        return np.array(img)
    except Exception as e:
        logger.warning(f"Could not load {path}: {e}")
        return None


def load_image_ids(image_ids_file: str) -> list[str]:
    with open(image_ids_file) as f:
        return json.load(f)


def save_image_ids(image_ids: list[str], image_ids_file: str) -> None:
    Path(image_ids_file).parent.mkdir(parents=True, exist_ok=True)
    with open(image_ids_file, "w") as f:
        json.dump(image_ids, f)


def paths_to_ids(paths: list[Path]) -> list[str]:
    """Convert paths to stable string IDs (relative path as string)."""
    return [str(p) for p in paths]
