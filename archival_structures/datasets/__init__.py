"""Ground-truth annotation schema (`annotations.py`) and test-fixture data
(`images_transcriptions.py`) for this package.

`test_pairs` and `PACKAGE_DIR` are loaded lazily (PEP 562) because `images_transcriptions`
needs the optional notebook/vision dependencies (IPython, ipywidgets, PIL).
"""

_LAZY = {"test_pairs", "PACKAGE_DIR"}


def __getattr__(name):
    if name in _LAZY:
        from . import images_transcriptions
        value = getattr(images_transcriptions, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | _LAZY)
