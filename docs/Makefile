# Minimal makefile for Sphinx documentation

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SOURCEDIR     = .
BUILDDIR      = _build

# Prefer the project's own virtualenv over whatever `sphinx-build` happens to come first on
# PATH. These docs need `furo`, `myst_parser` and `nbsphinx` from the optional `docs` dependency
# group; a system-wide Sphinx (e.g. Homebrew's) has none of them and fails with
# `ThemeError: no theme named 'furo' found`. Still overridable: `make SPHINXBUILD=... html`.
VENV_SPHINXBUILD := $(wildcard $(CURDIR)/../.venv/bin/sphinx-build)
ifneq ($(VENV_SPHINXBUILD),)
SPHINXBUILD   ?= $(VENV_SPHINXBUILD)
else
SPHINXBUILD   ?= sphinx-build
endif

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile ead-parser ead-parser-open

# EAD parser rewrite working docs (plan, status, step reports) on their own, so they can be
# rebuilt and read without waiting for the notebook pages in a full `make html`.
EADPARSERDIR = $(BUILDDIR)/ead-parser

ead-parser:
	@$(SPHINXBUILD) -b html -t ead_parser "$(SOURCEDIR)" "$(EADPARSERDIR)" $(SPHINXOPTS) $(O) \
		ead_parser/index.md ead_parser/plan.md ead_parser/status.md \
		ead_parser/reports/index.md ead_parser/reports/step-*.md
	@echo
	@echo "EAD parser docs built: $(EADPARSERDIR)/ead_parser/index.html"

ead-parser-open: ead-parser
	@open "$(EADPARSERDIR)/ead_parser/plan.html"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
