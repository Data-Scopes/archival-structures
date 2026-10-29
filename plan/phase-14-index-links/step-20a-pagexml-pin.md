# Step 20a: pagexml-tools 0.9.1 pin, data link

> **Phase:** 14, Index → resolution links. **Depends on:** nothing for parts 1–5; **UA-048** (sibling venvs) for part 6.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

AS has run on pagexml-tools 0.9.0 with workarounds since phase 07. The user released **0.9.1** (on PyPI; checked
2026-10-29: `CHILD_PROPERTIES` includes `'extra'`, and `PageXMLPage.stats` uses `self.num_text_regions` directly). This
step installs and pins it, drops the workarounds that 0.9.1 makes unnecessary (each only if its test passes without
it), and proves with a regression build that no v5 table changes. It also links `republic_segmentation_v1` under
`data/` as the shared-data rules require (step 20e reads it). Published layers are never rebuilt (D17): if 0.9.1
changes a table, stop (part 4).

## Inputs

- `pyproject.toml` L25: `    "pagexml-tools (>=0.9.0,<1.0.0)",`.
- `archival_structures/text_layer/provenance.py` L6 `MIN_PAGEXML_TOOLS = (0, 9, 0)`; `check_pagexml_tools_version` L46–47.
- `tests/test_text_layer_provenance.py` L11–17:
  ```python
  def test_check_pagexml_tools_version_uses_minimum():
      assert prov.MIN_PAGEXML_TOOLS == (0, 9, 0)
      assert prov.check_pagexml_tools_version()

  def test_pagexml_tools_is_at_least_0_9_0():
      assert prov.check_min_version("pagexml-tools", (0, 9, 0))
  ```
- `archival_structures/analysis/page_structure.py`, `assign_column_roles` (anchor `def assign_column_roles`), L825–832:
  ```python
      if page_coords is None:  # PageXMLPage derives coords from its children, and raises on none
          page_coords = hull_coords(columns) if columns else pdm.Coords([(0, 0)])
      page = pdm.PageXMLPage(doc_id=page_id, metadata=dict(metadata or {}), coords=page_coords,
                             columns=main, extra=extra)
      pdm.set_parentage(page)
      page.set_as_parent(extra)  # set_parentage skips `extra` (pagexml-tools 0.9.0)
      for region in extra:
          region.set_as_parent(region.lines)
  ```
  and its module docstring L29–31 (every region goes in `page.columns` or `page.extra`; `set_parentage` skips `extra`).
- Tests on these: `tests/test_analysis_page_structure.py::test_page_parentage` (L184; asserts `r.parent is page` for
  `page.extra` and `l.parent is note` for the lines of extra regions), `::test_empty_page_builds` (L194,
  `assign_column_roles([])`, goes through the coords fallback); `tests/test_text_layer_line_split.py::
  test_page_split_columns_sides` (L176–186; goes through `page.stats`); `tests/test_text_layer_extract.py` L3 ("pins
  pagexml-tools 0.9.0 behaviour") and L93–94 (nested regions fail to parse).
- Keep, not workarounds: `text_layer/layout.py` `_page_coords` (L73–78; real page geometry), `text_layer/line_split.py`
  `_page_text_regions` (L1045–1053; copies 0.7.x behaviour on purpose), `line_split.py` L1059 `page.stats['lines']`.
- Text that says 0.9.0: `text_layer/build.py` L13, L19 (docstrings); `docs/text_layer.md` L70 ("pulls in
  `pagexml-tools>=0.9.0`") and section `## Known pagexml-tools 0.9.0 limits (unchanged since 0.8.1)` (L812: nested
  regions fail to parse, fixture `tests/fixtures/text_layer/pagexml/9999A/NL-HaNA_1.04.02_9999A_0003.xml`; a region with
  neither Coords nor lines is dropped).
- Regression build (checked at planning 2026-10-29 with `--check-only`: 231 inventories, 43–70 min with 4 to 2
  workers, 40,862 MiB free). The output dir `data/text_layer_v2_dev/20a-pin/` exists (holds the check-only `run_v4.log`):
  ```
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 \
    --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 \
    --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 \
    --src 1.01.02=data/text_layer_v5_19e_src15z/NL-HaNA_1.01.02 \
    --work-dir data/text_layer_v5_19e_inputs --out-dir data/text_layer_v2_dev/20a-pin --want 4 --min 2
  ```
  It is the command that built the reference `data/text_layer_v2_dev/19e-a` (protected; never write there). It calls
  coreshare itself. It re-creates the symlinks in `data/text_layer_v5_19e_inputs/1.01.02` (same targets).
- Comparer (run from the AS root): `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/compare.py
  data/text_layer_v2_dev/19e-a data/text_layer_v2_dev/20a-pin --sub-map 1.01.02:1.01.02 --out F.json`. It compares
  every file by relative path (decompressed bytes; `.json` without `seconds`/`elapsed*` keys), skips `manifest.json`,
  `progress.log`, `status.tsv`, and prints `== sub: N equal, N differ, N missing, …` then each differing file.
- Shared data: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/`;
  catalogue `/Users/Shared/claude_projects/data_scopes/shared-data/README.md`, row starting
  `` | `NL-HaNA_1.01.02/republic_segmentation_v1/` ``, last column now
  `` republic-classification (`data/republic_segmentation_v1`; check only, UA-097 b) ``.
- Sibling suites (part 6): NLP `cd /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection &&
  PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests` (last: 66 passed); LotA `cd
  /Users/Shared/claude_projects/data_scopes/language-of-the-archive && PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q
  -p no:cacheprovider` (last: 381 passed, 1 xfailed). NLP's `tests/test_voc_text_layer.py:234`
  (`test_pagexml_tools_version_is_at_least_080`) calls AS's `check_pagexml_tools_version`, so it fails while a sibling
  venv still reports 0.9.0. Read each sibling's `CLAUDE.md` before running anything there; edit nothing there.

## Deliverables

| File | Change |
|---|---|
| `.venv` | pagexml-tools 0.9.1 (`.venv/bin/python -m pip install 'pagexml-tools==0.9.1'`; then `chmod -R g+w .venv/lib/python3.14/site-packages/pagexml*`) |
| `pyproject.toml` | `"pagexml-tools (>=0.9.1,<1.0.0)",` |
| `text_layer/provenance.py`, `tests/test_text_layer_provenance.py` | `MIN_PAGEXML_TOOLS = (0, 9, 1)`; tests assert `(0, 9, 1)` (rename the second to `test_pagexml_tools_is_at_least_0_9_1`) |
| `analysis/page_structure.py` | drop workarounds per part 2; docstring true for 0.9.1 |
| `text_layer/build.py`, `docs/text_layer.md` | 0.9.0 → 0.9.1 where it states the requirement; the *Known limits* section re-checked on 0.9.1 (part 3) |
| `data/republic_segmentation_v1` | symlink (part 5) |
| shared-data `README.md` | AS added to the row's *used by* (Edit) |
| `docs/shared_library/changelog.md` | `### Step 20a: pagexml-tools 0.9.1` under *Unreleased* (Edit; create the heading if missing) |
| `reports/data/phase-14-index-links/step-20a/` | `protect_before.txt`, `protect_after.txt`, `compare.json`, `notes.md` |
| `reports/phase-14-index-links.md` | `### Step 20a: …` section, draft findings `F14-20a.n` |

## Behaviour to implement

**Part 0, protect.** `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
reports/data/phase-14-index-links/step-20a/protect_before.txt --skip 20a-` (`--skip` takes dev-dir name prefixes;
it hashes all of shared-data, LotA `data/ gold/ output/`, NLP `output/voc_text_layer` and every other dev layer; format
`size=N MTIME PATH` plus sha256 of 8 source archives). Re-run to `protect_after.txt` at the end and `diff` the two.
Allowed differences: `shared-data/README.md` (your *used by* edit) and shared-data paths of other projects (expected;
name them). Anything else → stop and report.

**Part 1, install and pin.** Install, `chmod`, check `.venv/bin/python -c "import importlib.metadata as m;
print(m.version('pagexml-tools'))"` prints `0.9.1`. Edit `pyproject.toml`, `provenance.py`, the two tests. Do not touch
`poetry.lock` (the user's open precondition `poetry lock`); say in the report that it still records 0.9.0.

**Part 2, workarounds, one at a time**, each followed by the named tests:
1. Delete L830–832 (`page.set_as_parent(extra)` and the loop). Run `test_page_parentage`. Passes → keep the deletion;
   fails → restore and record why (finding).
2. Replace L825–826 by passing `coords=page_coords` as given (None allowed). Run `test_empty_page_builds` and
   `tests/test_analysis_page_structure.py`. Passes → keep; any failure (e.g. `IndexError` in `parse_derived_coords`,
   F07-15fp.11) → restore and record.
3. Update the module docstring (L29–31) to what is true now.
4. Grep `tests/` for `stats`, `text_regions=`, `set_parentage`; run `tests/test_text_layer_line_split.py` and
   `tests/test_text_layer_extract.py`. If the nested-region tests (L93–94) fail because 0.9.1 parses nested regions,
   **stop before editing them**: that changes extract output; report it and wait (a UA, P13).

**Part 3, docs.** Re-check the two *Known limits* on 0.9.1 with the 9999A fixture and a region without Coords and lines
(a 10-line script in `step-20a/`); rename the section to `## Known pagexml-tools 0.9.1 limits` and state the result.

**Part 4, regression build.** Run the full AS suite first (expect 1,369 collected: 1,358 passed, 11 skipped, before
any test you add). Run the build command above in the foreground (timeout: give the tool its maximum and re-run the
same command to resume if it is cut; finished inventories are skipped). `chmod -R g+w data/text_layer_v2_dev/20a-pin`.
Then the comparer with `--out reports/data/phase-14-index-links/step-20a/compare.json`. **Pass:** 0 differing and 0
missing files except files whose only difference is a recorded pagexml-tools version string; read every differing
file and name it in `notes.md`. Any differing table (`*.tsv.gz`) → stop, report the files, the inventories, 5 example
rows, and write a UA (Decide: keep 0.9.1 and rebuild v5 as v6 later, or revert the pin). Also the run's `audit.json`:
`n_fail_judged` 0 and `n_pass` 4,192 as in `19e-a`.

**Part 5, data link.** `ln -s /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/republic_segmentation_v1
data/republic_segmentation_v1`. Check `ls data/republic_segmentation_v1/lines | head -3`. Edit the shared-data row's last
column to `` republic-classification (`data/republic_segmentation_v1`; check only, UA-097 b); AS (`data/republic_segmentation_v1`;
scoring only, phase 14) ``. Never write through the link.

**Part 6, siblings** (needs UA-048 answered and done: grep `docs/USER_ACTIONS.md` and
`docs/USER_ACTIONS_resolved.md` for `UA-048`). Check each sibling venv: `PYTHONNOUSERSITE=1 venv/bin/python -c "import
importlib.metadata as m; print(m.version('pagexml-tools'))"` → `0.9.1`. Then both suites. If UA-048 is still open:
do parts 0–5, set the step `partial` in `STATUS.md` with "part 6 waits for UA-048", and stop.

## Constraints

- Never `git commit`; never bump the version; `chmod g+w` new files; `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`.
- Do not edit any `text_layer/` stage module, `layout.py`, `line_split.py`, the siblings, `19e-a`, or published layers.
- Shared files (report, `STATUS.md`, `PROGRESS.md`, changelog, `USER_ACTIONS.md`) with Edit only: step 20c runs in
  parallel.

## Tests

- Changed: the two provenance tests. Run the files named in part 2, then the full suite:
  `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,358 passed, 11 skipped (no new tests in this step).

## Acceptance criteria

1. `.venv` reports 0.9.1; pin and `MIN_PAGEXML_TOOLS` (0, 9, 1); each workaround dropped or kept with its test result.
2. Full AS suite: 1,358 passed, 11 skipped.
3. `compare.py 19e-a 20a-pin`: every table equal; any other difference named and explained; audit 0 judged, 4,192 pass.
4. `diff protect_before.txt protect_after.txt`: only the allowed paths (part 0).
5. Link resolves; *used by* edited.
6. NLP 66 passed and LotA 381 passed + 1 xfailed under 0.9.1 (or `partial`, waiting for UA-048).

The user decides when the step is done.

## Error analysis

Read every file the comparer lists as differing (all of them, if any) and every test that changed result after a
workaround was removed. Findings: which workarounds 0.9.1 retired (count), whether any output changed, the state of the
two *Known limits*.

## Do not touch

`data/text_layer_v2_dev/19e-a`, every other dev layer, `shared-data/` except the one README cell, the siblings' code,
`poetry.lock`, the external drive.

## When done

1. Report: `### Step 20a: pagexml-tools 0.9.1 pin, data link` in `reports/phase-14-index-links.md` (draft findings
   `F14-20a.n`); `make reports` clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 20a: pagexml-tools 0.9.1 pin, data link
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `MIN_PAGEXML_TOOLS` (0, 9, 1); requirement pagexml-tools >= 0.9.1
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-14-index-links.md` § Step 20a](../../reports/phase-14-index-links.md#step-20a-pagexml-tools-091-pin-data-link); findings F14-20a.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```

3. Overwrite the phase `STATUS.md`: 20a state; facts for 20b (sibling versions) and 20e (the link path).
