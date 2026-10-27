# Open user actions (AS)

Open items only. Answer under **Response**. Resolved items move to
`docs/USER_ACTIONS_resolved.md`.

### UA-007 `PageXMLPage.stats` still raises before pagexml-tools 0.9.1 (2026-10-20, from phase-07 planning)
- **Action:** **Edit** `/Users/marijnkoolen/Code/Huygens/pagexml-tools/pagexml/model/pagexml_document_model.py`,
  `PageXMLPage.stats` (L1176–1196 at commit `16a51d0`). Replace
  ```python
              "text_regions": len(self.num_text_regions),
              "table_regions": len(self.num_table_regions)
  ```
  with
  ```python
              "text_regions": self.num_text_regions,
              "table_regions": self.num_table_regions
  ```
  and, in the `if self.extra:` branch, replace `sum(tr for tr in self.extra if isinstance(tr,
  PageXMLTextRegion))` with `sum(1 for tr in self.extra if isinstance(tr, PageXMLTextRegion))`
  (the same for `PageXMLTableRegion`). Then release 0.9.1 and fill in **Response**.
  Checked at `16a51d0` (2026-10-20): `stats` raises `TypeError: object of type 'int' has no
  len()` on a page with columns, with `text_regions` or with `extra`, because `num_text_regions`
  is already an int (L392–393). AS's print splitter calls `page.stats['lines']`
  (`text_layer/line_split.py:1059`), so the print extract would crash under this copy. The other
  six fixes of UA-005 are in place (F07-P.9).
- **Options:** (a) fix and release 0.9.1; (b) fix it later.
  Recommended: (a).
- **Default if unanswered:** AS stays on the 0.9.0 wheel with its workarounds; step 15i skips
  the pin.
- **Blocks:** only the pin in step 15i (step 0). Steps 15fq–15hj run on 0.9.0 either way.
- **Response:** a, fixed locally on `bug_fixes` branch but waiting for release 0.9.1.

### UA-032 Install AS editable into NLP's venv, so NLP can switch to the phase-11 ports (2026-10-27, from phase-11 planning)
- **Action:** **Run**, in `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`:
  ```
  PYTHONNOUSERSITE=1 venv/bin/python -m pip install -e /Users/marijnkoolen/Code/Huygens/archival-structures
  ```
  then `chmod -R g+w venv/lib/python3.14/site-packages` (the agent account must be able to read and update it).
  NLP's venv holds a non-editable copy of AS **0.13.0** (no `direct_url.json`), so NLP can't see the phase-11 names
  (`text_extent` options, `segment_dp`, `textiling_depth_scores`, `separator_sheets`). NLP's own
  `docs/plan_model_alternatives/CONTEXT.md` L269–270 says AS is installed editable; that note is stale.
- **Options:** (a) editable install now (recommended: NLP's shims for C10/C12 and its `early_end_*` scripts can then
  use AS directly); (b) install the next release (`pip install <AS>@v0.16.0`) after you tag it; (c) leave NLP on 0.13.0.
- **Default if unanswered:** (c). NLP keeps its own copies; phase 11 is unaffected (its checks ran NLP's interpreter
  with `PYTHONPATH=<AS>`, without changing NLP's venv).
- **Blocks:** a later NLP shim step (NLP `unsupervised_segmentation.py` and `separator_sheets.py` → re-exports).
- **Response:**

### UA-033 `make docs` fails: no Sphinx in `.venv` (2026-10-27, from phase-11 planning)
- **Action:** **Run**, in `/Users/marijnkoolen/Code/Huygens/archival-structures`:
  ```
  .venv/bin/python -m pip install sphinx furo myst-parser nbsphinx && chmod -R g+w .venv
  ```
  `docs/Makefile` prefers `.venv/bin/sphinx-build`; there is none, so it falls back to `/opt/homebrew`'s sphinx, which
  fails with `ModuleNotFoundError: No module named 'pygments'`. `docs/conf.py` also loads `nbsphinx` (dropped when missing, L89); check `extensions` for any other package
  the build needs (e.g. `myst-nb`) and add it to the command.
- **Options:** (a) install as above (recommended); (b) fix the Homebrew Python instead; (c) leave it.
- **Default if unanswered:** (c). The docs are not built; phase-11 step 18d's pages are checked as Markdown only.
- **Blocks:** nothing in phase 11; building the Sphinx site.
- **Response:**
