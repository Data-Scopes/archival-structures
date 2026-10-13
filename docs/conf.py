# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# -- Project information -----------------------------------------------------

project = "archival-structures"
copyright = "2026, Marijn Koolen"
author = "Marijn Koolen"
release = "0.2.0"

# -- General configuration ----------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "myst_parser",
    "nbsphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# autodoc/napoleon: most docstrings in this codebase are plain prose rather than strict
# Google/NumPy style, but napoleon's section parsing (Parameters/Returns/...) is still useful
# where docstrings do use it (e.g. archival_structures/clustering/colour_clustering.py), and
# harmless elsewhere.
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_rtype = False

# lets `[text](other-page.md#some-heading)`-style links resolve against auto-generated heading
# anchors in other pages, not just headings with an explicit `(target)=` label
myst_heading_anchors = 3

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

# Heavy/optional runtime dependencies that aren't needed to *build* the docs -- mocked out so
# autodoc can import modules that use them without requiring a full install.
autodoc_mock_imports = [
    "cv2",
    "torch",
    "transformers",
    "hdbscan",
    "umap",
    "ipywidgets",
    "ipycanvas",
    "skimage",
    "fuzzy_search",
    "pagexml",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
}

# nbsphinx: render the demo notebooks directly from notebooks/demo/ rather than copying them
nbsphinx_execute = "never"  # the notebooks need real data/a kernel; render saved outputs as-is

# `sphinx-build -t ead_parser <files...>` (docs/Makefile's `ead-parser` target) builds just the
# EAD-parser working docs. nbsphinx registers an `html-collect-pages` handler that expects
# every notebook it knows about to have been written to the output tree, so it errors out on a
# partial build -- drop it, and the notebooks with it, for this subset.
if tags.has("ead_parser"):  # noqa: F821 -- `tags` is injected by Sphinx
    extensions.remove("nbsphinx")
    exclude_patterns += ["notebooks/**", "**.ipynb"]
    # index.md's notebook toctrees then point at excluded documents, which is expected here
    suppress_warnings = ["toc.excluded"]

# -- Options for HTML output --------------------------------------------------

html_theme = "furo"
html_static_path = ["_static"] if Path(__file__).parent.joinpath("_static").exists() else []
