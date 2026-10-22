"""Shared helpers for the phase-09 planning and step scripts (run from the AS root with
`.venv/bin/python -I`). A layer base is a dir holding `hand/` and `print/`, each with one
`*_text_layer_v*` root (e.g. `data/text_layer_v2_dev/15ir-a`)."""
import gzip
from pathlib import Path

import pandas as pd

DEV = Path("data/text_layer_v2_dev")
HAND_INV = "3349 3350 3351 3352 3353 3354 3771 3772 3773 3821 3822 3823 11182 11281".split()
PRINT_INV = ["455", "456"]
REPUBLIC_DIR = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/"
                    "paragraphs/NL-HaNA_1.10.94")


def layer_root(base, sub):
    """The single `*_text_layer_v*` root under `<base>/<sub>`."""
    roots = sorted((Path(base) / sub).glob("*_text_layer_v*"))
    if len(roots) != 1:
        raise SystemExit(f"expected one *_text_layer_v* root under {Path(base) / sub}, got {roots}")
    return roots[0]


def inventories(sub):
    return HAND_INV if sub == "hand" else PRINT_INV


def read(path, usecols=None):
    """A layer TSV (gz or plain) as str columns; '' for empty cells."""
    return pd.read_csv(path, sep="\t", quoting=3, dtype=str, keep_default_na=False,
                       usecols=usecols)


def raw_bytes(path):
    path = Path(path)
    return gzip.open(path, "rb").read() if path.suffix == ".gz" else path.read_bytes()


def baseline_y(baseline, y, h):
    """Mean y of the baseline points ("x,y x,y ..."), else the box centre (as `_line_sort_key`)."""
    if baseline and baseline.strip():
        pts = [tuple(map(float, p.split(","))) for p in baseline.split()]
        return sum(p[1] for p in pts) / len(pts)
    return int(y) + int(h) / 2
