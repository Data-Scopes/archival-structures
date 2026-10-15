# Step 13: pagexml-tools 0.8.2: pins and environments

> **Phase:** 05, Text layer. **Depends on:** 15c, and **the user's pagexml-tools 0.8.2 release**.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Amendment (Opus, 2026-10-14, after the release): the release is **0.9.0**, not 0.8.2

Read every "0.8.2" below as **0.9.0**. These facts were checked before this session:
- PyPI has `pagexml-tools 0.9.0`. The checkout `/Users/marijnkoolen/Code/Huygens/pagexml-tools`
  is on `master` at `c112a28`, after `3e751c8` ("set version to 0.9.0") and `afc9706` ("Fix
  column_parser bug"), and contains `7b2b721`. There is no local `0.9.0` tag, so use `3e751c8`
  and `HEAD` in the error-analysis diff, against `d1f0838` (0.8.1).
- 0.9.0 declares `fuzzy-search >=3.0.0,<4.0.0`, `orjson ^3.11.7`, `lxml 6.1.0` (exact), and
  `numpy`, `scipy`, `shapely`, `py7zr 1.1.3` among others. `pip install --dry-run
  "pagexml-tools==0.9.0"` in AS `.venv` would install fuzzy-search 3.0.4, lxml 6.1.0 (now 5.4.0),
  nltk 3.10.3 (now 3.9.2), py7zr 1.1.3 (now 0.20.8), pyppmd 1.3.1, pytest 9.1.1 (now 8.4.2).
  That is accepted. Record the before and after versions in the report, then let the full AS
  suite judge it. The AS pins become `pagexml-tools (>=0.9.0,<1.0.0)` and
  `fuzzy-search (>=3.0.0,<4.0.0)`. Leave the `lxml` pin alone; it already allows 6.x.
- **The nested-region bug is not fixed in 0.9.0** (`set += set` is still at L447). The fixture
  test still fails to parse `_0003.xml`. Rename it to `test_nested_regions_fail_in_pagexml`,
  update its comment (still failing in 0.9.0; to be reported upstream), and record it as an
  open-issue finding.
- **NLP and LotA:** both import pagexml from the checkout (`pagexml.__version__ == "0.9.0"`), and
  `pagexml.helper.pagexml_helper` imports. Their **dist-info metadata is stale**:
  `importlib.metadata.version("pagexml-tools")` gives `0.8.0`. For NLP this comes from
  `/opt/homebrew/lib/python3.11/site-packages/pagexml_tools-0.8.0.dist-info`, which the session
  can't write; for LotA it comes from `<LotA>/venv/.../pagexml_tools-0.8.0.dist-info`. Don't
  reinstall anything there. Criterion 1 is met through "the checkout is at the release commit"
  plus `pagexml.__version__`. Record the stale metadata as a finding: manifests built in those
  environments would record `pagexml_tools.version` 0.8.0. In STATUS.md, list the user's fix:
  `pip install --no-deps -e /Users/marijnkoolen/Code/Huygens/pagexml-tools` with each of the two
  interpreters.
- The provenance test is `test_pagexml_tools_is_at_least_0_9_0`, and the section and block
  titles say 0.9.0. The report anchor becomes `#step-13-pagexml-tools-090`.

## Context

The VOC text layer v1 was built against a pagexml-tools checkout at `0f39fad` with four modified
files. Planning (2026-10-14) found that those modifications were import-only (`orjson`, and the
fuzzy-search-3 path `fuzzy_search.analysis.similarity`), later committed as `7b2b721` on the
branch `fix/vertical_distance_baseline_gap`. PyPI 0.8.1 has the same text logic, but imports
`fuzzy_search.similarity`, which exists only in fuzzy-search 2.x. AS's `.venv` (0.8.1 +
fuzzy-search 2.5.0) therefore works. NLP's and LotA's environments (fuzzy-search 3.0.4) fail as
soon as the joiner or the word-break statistics import `pagexml.helper.pagexml_helper`:
- LotA's `tests/test_derive_voc.py` can't be collected;
- NLP's `tests/test_voc_text_layer.py` collects 29 tests, but 23 fail at run time.

The user has now released 0.8.2 from that branch. This step pins AS to it, proves the output is
unchanged, and checks that all three environments import the joiner.

## Inputs

- `<NLP>` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`, interpreter
  `<NLP>/venv/bin/python`. It is built with `--system-site-packages`: pagexml, fuzzy-search and AS
  come from `/opt/homebrew/lib/python3.11/site-packages`, and pagexml is an editable link to the
  checkout `/Users/marijnkoolen/Code/Huygens/pagexml-tools`.
- `<LotA>` = `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`, interpreter
  `<LotA>/venv/bin/python`, which has an editable pagexml-tools from the same checkout and
  fuzzy-search 3.0.4. **Read `<LotA>/CLAUDE.md` and `<NLP>/CLAUDE.md` first.** This step edits
  neither sibling.
- Sessions run as `claude-agent`. If an import fails only in the session, check
  `site.getusersitepackages()`; run sibling interpreters with `PYTHONNOUSERSITE=1` and report
  both results.
- AS `pyproject.toml` `[project] dependencies` today: `"pagexml-tools (>=0.8.0,<1.0.0)"`,
  `"fuzzy-search (>=2.5.0,<4.0.0)"`. AS itself uses `fuzzy_search.tokenization.token.Tokenizer`
  (`archival_structures/analysis/text_analysis.py` L19), which exists in 2.x and 3.x.
- The 15c reproduction command and the reference `<REF>` = `<NLP>/output/voc_text_layer/voc_text_layer_v1/`
  (see step 15c criterion 1 in `plan/phase-05-text-layer/step-15c-build-driver.md`, which you may
  read for the exact command).
- The fixture test `tests/test_text_layer_extract.py::test_nested_regions_fail_in_pagexml_0_8_1`
  (step 15). In 0.8.1, `get_all_text_regions` does `set += set` (`pagexml_document_model.py`
  L447). The user was asked to fix this in 0.8.2.

## Deliverables

| File | Change |
|---|---|
| AS `.venv` | `pagexml-tools==0.8.2` installed (below) |
| `pyproject.toml` | `"pagexml-tools (>=0.8.2,<1.0.0)"`; the fuzzy-search lower bound raised to what 0.8.2 declares, keeping `<4.0.0` |
| `tests/test_text_layer_extract.py` | only if 0.8.2 parses nested regions (below) |
| `tests/test_text_layer_provenance.py` | `test_pagexml_tools_is_at_least_0_8_2` |
| `docs/text_layer.md` | update the "known pagexml-tools limits" paragraph to 0.8.2 |
| `docs/shared_library/changelog.md` | `### Step 13: pagexml-tools 0.8.2` under *Unreleased*, with Edit |
| `reports/phase-05-text-layer.md` | `### Step 13` section, findings `F05-13.n` |

## Behaviour to implement

1. **Record before:** in each of the three environments, record `importlib.metadata.version`
   of `pagexml-tools` and `fuzzy-search`, `pagexml.__file__`, and whether
   `import pagexml.helper.pagexml_helper` and
   `from pagexml.analysis.text_stats import LineCharAnalyser` succeed.
2. **AS `.venv`:** if it doesn't have 0.8.2, run `.venv/bin/python -m pip install "pagexml-tools==0.8.2"` (`.venv/bin/pip` does not exist, F05-15c brief defect)
   and nothing else (pip may upgrade fuzzy-search to satisfy it). Print
   `importlib.metadata.requires("pagexml-tools")` and set the pins from it.
3. **Siblings:** don't install anything in NLP or LotA. Their pagexml is the editable checkout.
   If it doesn't contain the release yet (the import check of item 1 still fails, or
   `git -C <checkout> log -1` isn't the release commit), **stop and ask** the user to check out
   the release in the checkout.
4. **Nested regions:** run the fixture test.
   - If it still passes (0.8.2 still fails on nesting), leave it and record that.
   - If 0.8.2 parses nested regions: rename the test to `test_nested_regions_are_extracted`.
     Assert what the extract produces for file `_0003.xml`, measured, not guessed: region rows
     `n1` and `n1a` in document order, `counts["nested_text_regions"] == 1` and
     `counts["text_regions_with_children"] == 1`. Update the status counts for 0 parse errors
     (`n_errors` 0, `n_scans` 4, `n_rows` 6, `n_lines` 7). Verify each by running, and quote
     the before and after in the report.

## Constraints

- **Protected:** `<REF>` (sha256 of every file) and `<NLP>/data/voc_text_layer_v1/` (size and
  mtime under `wbc/`, plus `manifest.json` and `status.tsv`). Every output goes into the
  scratchpad.
- Never `git commit`, not in pagexml-tools either; never bump AS's version; `chmod g+w` new
  files. Don't run `poetry lock` (the user does).
- If the reproduction (criterion 2) changes **any** byte, stop and ask. That would mean 0.8.2
  changed text output, which is a finding to report, not a test to relax.

## Tests

- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_analysis_text_analysis.py tests/test_packaging_core_imports.py`,
  then the full suite (count in `STATUS.md`).
- NLP, from `<NLP>`: `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_voc_text_layer.py`
  → **29 passed** (still the original NLP module; step 15b makes it a shim). Also
  `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_evaluation.py tests/test_semi_markov.py`
  → **21 passed** (keep this order).
- LotA, from `<LotA>`: `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_derive_voc.py`
  should now collect (11 test functions) and run. Report the result. Then the full suite
  `venv/bin/python -m pytest -q -p no:cacheprovider` (before: 316 passed with
  `--ignore=tests/test_derive_voc.py`).

## Acceptance criteria

1. In all three environments `import pagexml.helper.pagexml_helper` and `LineCharAnalyser`
   succeed, and pagexml-tools reports 0.8.2 (or the checkout is at the release commit).
2. **Same output under 0.8.2:** the 15c CLI run on the 22 annotated inventories into a fresh
   scratch dir, compared with `compare_layers`/`compare_run_files` against `<REF>`:
   **157/157**.
3. Pins updated; `tests/test_text_layer_provenance.py` asserts ≥ 0.8.2.
4. AS suite green. NLP 29 + 21 passed. LotA `test_derive_voc.py` collects. Report its
   result and the full-suite count; a failing LotA test is a finding for the user, not
   something to fix here.
5. Protected hashes unchanged.

## Error analysis

- Diff installed 0.8.1 and 0.8.2 (`git -C /Users/marijnkoolen/Code/Huygens/pagexml-tools diff
  <0.8.1 commit d1f0838> <release commit> --stat -- pagexml/`, plus the full diff of
  `parser.py`, `helper/pagexml_helper.py`, `helper/text_helper.py`, `analysis/text_stats.py`,
  `model/pagexml_document_model.py`). Classify every hunk as import-only, a bug fix, or a
  behaviour change. Explain why criterion 2 is or isn't expected to hold for the 6,892 inventories
  not rebuilt.
- If LotA's `test_derive_voc.py` fails: read each failure and say whether it comes from 0.8.2 or
  is older.
- Expected findings: environments fixed (result), output unchanged under 0.8.2 (result), the
  nested-region status (behaviour-change or open-issue), and the LotA test result.

## Do not touch

- NLP and LotA files and environments; the pagexml-tools checkout (read-only `git` commands only).

## When done

1. Add `### Step 13: pagexml-tools 0.8.2` to `reports/phase-05-text-layer.md` (findings
   `F05-13.n`, `draft`); run `make reports`.
2. Append the block below to `plan/phase-05-text-layer/PROGRESS.md`.
3. Overwrite `STATUS.md`: 13 done, next 15b. Under *Facts later steps need*: the NLP test
   result, the AS suite count, the fuzzy-search version. Add "`poetry lock`" to the user's open
   items.

```markdown
### Step 13: pagexml-tools 0.8.2
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Environments:** <before → after, per environment>
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-05-text-layer.md` § Step 13](../../reports/phase-05-text-layer.md#step-13-pagexml-tools-082);
  findings F05-13.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
