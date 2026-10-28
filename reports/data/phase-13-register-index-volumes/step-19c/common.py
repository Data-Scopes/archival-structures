"""Shared loaders for step 19c scripts."""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
PLAN = Path(__file__).resolve().parents[1] / "planning"
CLONES = {"15y-b": ("19c-a", "hand_15y-b.txt"), "15z-b": ("19c-z", "hand_15z-b.txt")}


def layer_dir(clone):
    return ROOT / "data" / "text_layer_v2_dev" / clone / "1.01.02" / "NL-HaNA_1.01.02_text_layer_v4"


def load_pages():
    """All layout/pages rows of the two clones (hand inventories), columns as strings plus inv."""
    out = []
    for clone, lst in CLONES.values():
        for inv in (PLAN / lst).read_text().split():
            f = layer_dir(clone) / "layout" / "pages" / f"{inv}.tsv.gz"
            d = pd.read_csv(f, sep="\t", dtype=str, keep_default_na=False)
            d.insert(0, "inv", inv)
            d["clone"] = clone
            out.append(d)
    return pd.concat(out, ignore_index=True)
