"""Transparent gzip-aware file opening."""

import gzip


def get_open_func(filename: str):
    """`gzip.open` if `filename` ends in `.gz`, else the builtin `open`."""
    return gzip.open if filename.endswith('.gz') else open


def open_file(filename: str, mode: str):
    """Open `filename` with `gzip.open` or the builtin `open`, whichever its extension calls
    for (`get_open_func`)."""
    open_func = get_open_func(filename)
    return open_func(filename, mode)
