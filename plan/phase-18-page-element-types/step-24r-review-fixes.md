# Step 24r: Phase-18 review fixes

> **Model:** Sonnet. **Written by:** the phase-18 review (Opus 5.5, 2026-11-17).
> **Read:** this brief and `plan/phase-18-page-element-types/STATUS.md`. Nothing else unless named here.
> **AS root:** `/Users/Shared/claude_projects/data_scopes/archival-structures` (all paths below are relative to it).
> **Budget:** about 30 minutes. Three small fixes. No gold is read for measurement and no bar is re-run.

## Rules (from `CLAUDE.md`, copied in)

- Use `.venv/bin/python`. Never `git commit`, `git stash`, `git checkout`, `git reset`; if git reports "dubious
  ownership", do not change git config (use `git -c safe.directory=<AS root> diff`).
- `chmod g+w` every file and directory you create. Check write access to a directory before writing there.
- Never run release targets (`make bump-*`, `make release*`). Never write under `data/annotations/` or on any external drive.
- No `rm -rf`, `nohup`, `&` or `$TMPDIR`. zsh; BSD `sed -i ''`. Jobs with more than one worker go through
  `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 24r ...`
  (none is needed here).
- Do not change any finding's status in the report, the *Summary*, *Review* or *Follow-ups* sections, or
  `reports/index.md`: those belong to the review.
- Do not edit `reports/data/phase-18-page-element-types/step-24d/eval_elements.py` or any file the 24d stages wrote
  (`*_lines.tsv`, `tables/*`, `cases_*`, `select.json`, `test_done.json`): they are the record of the one test run.

## Fix 1: `bulk_tagging` knows the `placement:` namespace (F18-R.10)

**Defect.** `archival_structures/datasets/bulk_tagging.py` l.41–48:

```python
from archival_structures.datasets.vocabulary import (
    CARRIER, CARRIER_TYPES, DOCTYPE, GENERIC, GENERIC_TYPES, NAMESPACES, POSITION,
    POSITION_TYPES, Tag, is_known_tag, make_tag,
)
...
_NAMESPACE_TYPES = {CARRIER: CARRIER_TYPES, GENERIC: GENERIC_TYPES, POSITION: POSITION_TYPES}
```

Phase 18 added `PLACEMENT = 'placement'` and `PLACEMENT_TYPES` (keys `main`, `margin`, `top`, `bottom`,
`interlinear`, each `frozenset()`) to `archival_structures/datasets/vocabulary.py`, and `NAMESPACES` now has five
entries. The widget's namespace dropdown (l.211, `options=list(NAMESPACES)`) therefore offers `placement`, but
`type_options_for_namespace('placement')` returns `[]`.

**Change.**
1. Add `PLACEMENT, PLACEMENT_TYPES` to that import.
2. `_NAMESPACE_TYPES = {CARRIER: CARRIER_TYPES, GENERIC: GENERIC_TYPES, POSITION: POSITION_TYPES, PLACEMENT: PLACEMENT_TYPES}`.
3. In the docstring of `type_options_for_namespace` (l.52–54), replace "for `carrier:`/`generic:`/`position:`" with
   "for `carrier:`/`generic:`/`position:`/`placement:`".

**Tests** (append to `tests/test_datasets_bulk_tagging.py`; it already imports `type_options_for_namespace` and
`subtype_options_for_type` from `archival_structures.datasets.bulk_tagging` and uses `unittest.TestCase`):
- in `class TestTypeOptionsForNamespace`: `test_placement_returns_placement_types`:
  `self.assertEqual(['bottom', 'interlinear', 'main', 'margin', 'top'], type_options_for_namespace('placement'))`;
- in `class TestSubtypeOptionsForType`: `test_placement_type_has_no_subtypes`:
  `self.assertEqual([], subtype_options_for_type('placement', 'margin'))`;
- a test that every namespace in `NAMESPACES` except `doctype` gives a non-empty list
  (`from archival_structures.datasets.vocabulary import NAMESPACES, DOCTYPE`), so the next namespace cannot be missed.

## Fix 2: VOC-only region-tag tables (F18-R.7)

**Defect.** `step-24d/eval_elements.py` l.250 selects "VOC" lines with `if r["region_tag"]`, but every line has a tag
(`Text` in SG and 2.10.01, `untagged` in the other archives), so `step-24d/tables/region_tags_voc.tsv` and
`tables/dev_region_tags_voc.tsv` pool all ten strata. Leave those files and the script as they are (record of the run).

**Change.** New directory `reports/data/phase-18-page-element-types/step-24r/` (`chmod g+w`) with a script
`region_tags_voc.py` (stdlib `csv` only, run with `.venv/bin/python -I` from the AS root). Inputs, read with
`csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)`:
`reports/data/phase-18-page-element-types/step-24d/test_lines.tsv` and `.../step-24d/dev_lines.tsv`. Columns used:
`stratum`, `region_tag`, `call_region_type`, `call_region_placement`, `gold_family`, `placement`, `unclear`.
Keep rows with `stratum == "voc"` only. For each of `test` and `dev` write `step-24r/region_tags_voc_<split>.tsv`
(tab-separated, header `table gold tag_call n`) with three blocks, each sorted by (gold, tag_call):
- `table = region_type`: `gold = gold_family`, `tag_call = call_region_type or "none"`, counts;
- `table = region_placement`: `gold = placement`, `tag_call = call_region_placement or "none"`, counts;
- `table = raw_tag`: `gold = gold_family`, `tag_call = region_tag` (the provider tag itself), counts.
Count all rows (unclear included), as `eval_elements.py` did. `chmod 0o664` the outputs.

**Acceptance (numbers the review measured).** Test, `raw_tag` block: `list` 69 lines = `paragraph` 67, `catch-word` 1,
`untagged` 1; `table` 154 = `paragraph` 153, `untagged` 1; `running_text` 67 = `paragraph` 64, `untagged` 3. Test VOC rows
in all: 325. Dev, `raw_tag`: `list` 51 all `paragraph`; `running_text` 90 all `paragraph`. If a number differs, stop and
record it under *Open questions* in `STATUS.md`.

## Fix 3: `listlike` module docstring renders in the docs (REVIEW finding 3)

**Defect.** `archival_structures/analysis/listlike.py` l.1–40 is the module docstring. Lines 9–39 are the original
shared-tools docstring, which is not reStructuredText; `docs/page_elements.md` renders it with `automodule`, and
Sphinx reports 5 warnings/errors for `listlike.py:docstring of archival_structures.analysis.listlike`.

**Change.** Docstring only; no code line changes.
- l.7: `Original docstring follows.` → `Original docstring follows::`
- Indent every non-empty line from l.9 up to and including l.39 (the line before the closing `"""` on l.40) by four
  spaces; leave empty lines empty. Keep l.8 empty (reST needs the blank line after `::`).

**Check.**
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_listlike.py` passes (the equivalence tests
  compare functions, not docstrings).
- `.venv/bin/python -I -m archival_structures.analysis.listlike --selftest` ends with `listlike selftest ok`.
- Sphinx: `.venv/bin/python -m sphinx -q -E -b html docs <scratchpad>/docs_24r 2>&1 | grep -c 'listlike.py:docstring'`
  prints `0` (use your session scratchpad directory, not the repository; the build takes a few minutes; other
  pre-existing warnings are not yours).

## Changelog

With Edit (never rewrite the file), in `docs/shared_library/changelog.md` under `## Unreleased`, after the
`### Step 24a: …` entry, add:

```
### Step 24r: Phase-18 review fixes

- `datasets.bulk_tagging`: `type_options_for_namespace('placement')` returns the placement types (was `[]`); the
  namespace map includes `placement:`. Additive.
- `analysis.listlike`: module docstring only (the original docstring is a literal block); no behaviour change.
- No sibling imports affected.
```

## Tests and acceptance

1. `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_datasets_bulk_tagging.py tests/test_analysis_listlike.py`
   passes, with the three new tests.
2. Full suite: `.venv/bin/python -m pytest -q -p no:cacheprovider`. Expect 1,460 passed (1,457 + 3 new), 11 skipped.
   `tests/test_text_layer_provenance.py::test_package_info_archival_structures_has_git_head` fails if git refuses the repo
   ("dubious ownership"); then re-run that file with
   `GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0=/Users/Shared/claude_projects/data_scopes/archival-structures`
   in the environment and count it as passed if it passes there.
3. Fix 2 acceptance numbers match.
4. Fix 3 Sphinx count is 0.

## Error analysis

Small: read the two new VOC tables and say in the report which provider tags the VOC list, table and margin lines get,
and whether the provider `marginalia` tag finds the VOC test margin lines (the review counted 1 margin-placement line
outside `unclear` on the VOC test pages, so say that it cannot be judged on this sample).

## Write-up

- Report `reports/phase-18-page-element-types.md`: insert a section `### Step 24r: Review fixes` directly before the
  line `## Review` (after the Step 24d section): what changed (three fixes), the test results, the two VOC tables'
  key numbers, and "no new findings" (or draft findings `F18-24r.n` with rows in the *Findings* table, status `draft`).
  Run `make reports` and check it gives no warning.
- `plan/phase-18-page-element-types/PROGRESS.md`: append a block `### Step 24r: Review fixes` (session, files, tests,
  acceptance, deviations, brief defects).
- `plan/phase-18-page-element-types/STATUS.md`: overwrite (40 lines max): 24r done, release v0.23.0 recommended to the
  user, phase 19 next (UA-073).

## Do not touch

`data/annotations/`; any sibling project; shared-tools; the frozen gold under `step-24c/gold/`; any 24d output;
`reports/index.md`; the report's *Summary*, *Review*, *Follow-ups*; `docs/USER_ACTIONS*.md`.
