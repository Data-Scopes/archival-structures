# EAD parser rewrite

A development workspace for testing and rewriting
{mod}`archival_structures.parsers.ead_parser` against EAD finding aids from a range of
archives, and extending it to the document- and page-level descriptions found in some
Stadsarchief Amsterdam (`NL-AsdSAA`) EADs.

- [Plan](plan.md) -- the staged plan, with the target data model and per-step deliverables.
- [Status](status.md) -- decisions taken and the current state of each step.
- Reports -- one per step, each with a data and error analysis over the test corpus.

Build these pages to HTML with:

```bash
poetry install --with docs   # once -- furo/myst-parser/nbsphinx live in the optional group
cd docs
make ead-parser        # just these pages, into _build/ead-parser/
make ead-parser-open   # ... and open the plan in a browser
```

The `Makefile` picks up `../.venv/bin/sphinx-build` in preference to whatever `sphinx-build` is
first on `PATH`, since a system-wide Sphinx will not have the theme or the Markdown parser these
pages need. Override with `make SPHINXBUILD=... ead-parser` if you keep the docs environment
somewhere else.

```{toctree}
:maxdepth: 2

plan
status
reports/index
```
