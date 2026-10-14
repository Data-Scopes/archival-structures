# Common development tasks. Releases are handled by scripts/release.py; see its docstring.
#
#   make version                  show version, latest tag, and whether a bump is needed
#   make check-tags               verify every v* tag matches its pyproject.toml version
#   make bump-patch               0.4.0 -> 0.4.1 (also bump-minor, bump-major)
#   make bump VERSION=1.2.3       set an explicit version
#   make release                  run tests, commit the bump, create the tag (local only)
#   make release-push             ... and push the branch and the tag
#   make release-github           ... and push, and create a GitHub release (needs gh auth)
#   make release-dry-run          print the release steps without running them
#   make test                     run the test suite
#   make docs                     build the Sphinx docs
#   make reports                  build reports/*.md to HTML in reports/_build/ (needs pandoc)
#   make reports-open             ... and open the reports index in a browser

PYTHON ?= .venv/bin/python
RELEASE = $(PYTHON) scripts/release.py

.PHONY: version check-tags bump bump-patch bump-minor bump-major release release-push \
	release-github release-dry-run test docs reports reports-open

version:
	@$(RELEASE) version

check-tags:
	@$(RELEASE) check

bump:
	@test -n "$(VERSION)" || { echo "usage: make bump VERSION=X.Y.Z"; exit 2; }
	@$(RELEASE) bump $(VERSION)

bump-patch bump-minor bump-major:
	@$(RELEASE) bump $(subst bump-,,$@)

release:
	@$(RELEASE) release

release-push:
	@$(RELEASE) release --push

release-github:
	@$(RELEASE) release --push --github

release-dry-run:
	@$(RELEASE) release --dry-run --skip-tests

test:
	$(PYTHON) -m pytest -q -p no:cacheprovider

docs:
	$(MAKE) -C docs html

REPORTS_DIR   = reports
REPORTS_BUILD = $(REPORTS_DIR)/_build

reports:
	@command -v pandoc >/dev/null 2>&1 || { echo "pandoc not found (brew install pandoc)"; exit 2; }
	@mkdir -p $(REPORTS_BUILD)
	@set -e; for f in $(REPORTS_DIR)/*.md; do \
	  n=$$(basename "$$f" .md); \
	  pandoc "$$f" --from gfm --to html5 --standalone --toc --toc-depth=3 \
	    --metadata pagetitle="$$n" \
	    --lua-filter $(REPORTS_DIR)/_tools/md-links.lua \
	    --css $(REPORTS_DIR)/_tools/report.css --embed-resources \
	    --resource-path=.:$(REPORTS_DIR) --fail-if-warnings \
	    -o $(REPORTS_BUILD)/$$n.html; \
	  echo "  $(REPORTS_BUILD)/$$n.html"; \
	done
	@chmod -R g+w $(REPORTS_BUILD)

reports-open: reports
	open $(REPORTS_BUILD)/index.html
