"""Shared helpers for the step 15g acceptance and error-analysis scripts."""

from pathlib import Path

from archival_structures.text_layer.io import read_tsv_gz

PARENT = Path("data/text_layer_v2_dev")
LAYERS = {
    "hand_tar": ("NL-HaNA_1.01.02_text_layer_v2", ["3349", "3350", "3351", "3352", "3353", "3354"]),
    "hand_local": ("NL-HaNA_1.01.02_text_layer_v2",
                   ["3771", "3772", "3773", "3821", "3822", "3823", "11182", "11281"]),
    "print": ("NL-HaNA_1.10.94_text_layer_v2", ["455", "456"]),
}
PRINT = {"455", "456"}
ALL_INVS = [inv for _, (_, invs) in LAYERS.items() for inv in invs]


def layer_root(layer_dir, inv):
    for sub, (name, invs) in LAYERS.items():
        if inv in invs:
            return Path(layer_dir) / sub / name
    raise KeyError(inv)


def lines_by_id(root, inv, scan_filter=None):
    out = {}
    for r in read_tsv_gz(root / "lines" / f"{inv}.lines.tsv.gz"):
        if scan_filter is None or scan_filter in r["scan_id"]:
            out[(r["scan_id"], r["line_id"])] = r
    return out


def seg_text(line, seg):
    return line["text"][int(seg["char_start"]):int(seg["char_end"])]
