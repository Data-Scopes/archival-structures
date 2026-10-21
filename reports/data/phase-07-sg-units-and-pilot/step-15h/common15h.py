"""Shared helpers for the step 15h scripts."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "step-15g"))
from common import ALL_INVS, LAYERS, PRINT, layer_root  # noqa: E402,F401

LAYER = Path("data/text_layer_v2_dev/15h-a")
LOTA = Path("/Users/Shared/claude_projects/data_scopes/language-of-the-archive")
HERE = Path(__file__).parent


def root(inv):
    return layer_root(LAYER, inv)
