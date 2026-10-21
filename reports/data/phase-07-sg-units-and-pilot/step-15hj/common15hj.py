"""Shared helpers for the step 15hj acceptance and error-analysis scripts."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "step-15g"))
from common import ALL_INVS, LAYERS, PRINT, layer_root  # noqa: E402,F401

LAYER = Path("data/text_layer_v2_dev/15hj-a")
HERE = Path(__file__).parent


def root(inv):
    return layer_root(LAYER, inv)


def wbc_set(inv):
    from archival_structures.text_layer.word_breaks import read_word_break_sets
    sets, _ = read_word_break_sets(root(inv))
    return sets[inv]


def right_first(seg_text, lead, wbcs):
    """First character of a segment's text after the dropped lead and any leading break chars."""
    return seg_text[lead:].lstrip("".join(wbcs))[:1]
