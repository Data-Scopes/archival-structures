# Phase 05 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Phase planning
- **Session:** Opus, 2026-10-14. No code changed.
  - The phase directory was created from `plan/templates/`.
  - `PLAN.md` was seeded from `upstream_inventory.md` C14, C15 and §4 steps 13–15, and planned in
    the same session.
- **Read:**
  - the sources in `PLAN.md`;
  - NLP `voc_text_layer.py` and `build_voc_text_layer.py` (the ranges listed there);
  - NLP, LotA, pagexml-tools and the SG data through two Explore subagents;
  - the AS packaging test, `pyproject.toml` sections, the changelog head, the docs toctree.
- **Briefs written:** 14, 15, 15c, 13, 15b. The inventory's step 15 is split into a library
  part (15) and a driver part (15c), plus the NLP side (15b).
- **User decisions:**
  - split v2 and the SG preset into the next phase (P1);
  - the user releases pagexml-tools 0.8.2 (P2);
  - acceptance against the local 22 build, A/B, order-insensitive v1, plus the user's v2.1
    sample (P3);
  - the user moves the data after 15b (P4).
- **Planning decisions:** P5–P12 in `PLAN.md` (verbatim port, AS names, rule stays a dict,
  `ScanId`/`InventoryNumber` hand-off, manifest superset, NLP wrapper, no v2 in the move,
  8 workers).
- **Checked by prototype** (scratchpad; sibling files untouched; scripts kept in
  `reports/data/phase-05-text-layer/planning/`):
  - 22 local inventories rebuilt with NLP's module under AS `.venv`: 157/157 identical to NLP's
    local build (221 s);
  - `decide_wbc` on v1 stats: 2/2 byte-identical; v1 lines → v1 parts 8/8;
  - 34 rebuilds vs v1: same content, different order;
  - pagexml 0.8.1 = `0f39fad` (text logic); nested regions crash the parser;
  - v1 extract stats: 0 parse errors, 0 mismatches, 0 nested regions.
- **Findings:** F05-P.1–P.7 in the report's *Planning* section.
- **Report:** `reports/phase-05-text-layer.md` created (draft).
- **Also this session (user request):** `docs/findings.md` gained a section *Sequence decoding,
  scan ids and page numbering* with the phase-04 findings. Sphinx build: no warnings from it.

### Step 14: text-layer I/O and word-break detector
- **Session:** Sonnet, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/text_layer/{__init__,io,word_breaks}.py`,
  `tests/test_text_layer_io.py`, `tests/test_text_layer_word_breaks.py`,
  `tests/test_packaging_core_imports.py`, `docs/shared_library/changelog.md`,
  `reports/phase-05-text-layer.md`, `reports/data/phase-05-text-layer/step-14/`
- **Public API:** `archival_structures.text_layer.io.*`, `.word_breaks.*` (new)
- **Tests:** targeted `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_io.py tests/test_text_layer_word_breaks.py tests/test_packaging_core_imports.py` -> 22 passed; full suite 908 passed (890 before)
- **Acceptance:** 1. decision files 2/2 identical (default `¬„`, 1,461 differ, 172 whole_default); 2. stats rewrite identical; 3. parts 52/52 identical; 4. sort order equal on 6,914; 5. suite green, v1 files unchanged
- **Report:** [`reports/phase-05-text-layer.md` § Step 14](../../reports/phase-05-text-layer.md#step-14-text-layer-io-and-word-break-detector);
  findings F05-14.1–4
- **Error analysis:** 1,461 deviations: 717 add a char (672 `=`), 590 drop `„` only; 4 of 10 sampled plausible, 6 drop a genuine mark narrowly failing a threshold. 684 rows near-threshold could flip at +-10%.
- **Deviations from brief:** none
- **Brief defects:** `read_region_lines` docstring (ported) misdescribes the return shape: `(regions_meta row dict, texts)`.
- **Open questions for the user:** none

### Step 15: text-layer library
- **Session:** Sonnet, 2026-10-14. **Status:** partial (acceptance 3 not met; question open)
- **Files changed:** `archival_structures/text_layer/{extract,join,provenance,presets}.py`,
  `tests/test_text_layer_{join,extract,provenance}.py`, `tests/test_packaging_core_imports.py`,
  `tests/fixtures/text_layer/pagexml/9999A/*.xml`, `docs/shared_library/changelog.md`,
  `reports/phase-05-text-layer.md`, `reports/data/phase-05-text-layer/step-15/{check,ea}.py`
- **Public API:** `text_layer.extract/join/provenance/presets` (new)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_packaging_core_imports.py` → 67 passed; full suite 953 passed
- **Acceptance:** 1. 22 inventories 157/157 byte-identical (39 s, 8 workers); 2. A/B 140/140 equal (40 s); 3. scan-id check NOT met: 98 of 4,798,666 ids in 3 inventories (10090, 1186, 9817) fail `scan_id_matches`; 4. tests green, protected hashes unchanged
- **Report:** [`reports/phase-05-text-layer.md` § Step 15](../../reports/phase-05-text-layer.md#step-15-text-layer-library);
  findings F05-15.1–5
- **Error analysis:** 42 inventories vs v1: line multisets equal; line order differs in 12,834/313,209 regions (all 12,173 text differences); v1 order right in 9/10 sampled and paragraph regions top-to-bottom in 97.2% of scans vs 51.2% rebuilt
- **Deviations from brief:** none (the port was not adjusted to the scan-id numbers)
- **Brief defects:** acceptance 3 expects 0 mismatches; EA counts quoted for 34 inventories but 42 asked; NLP function names for the A/B not listed; `join_inventory` range ends L843
- **Open questions for the user:** should `ScanId` (or `scan_id_matches`) accept 2-digit numbers (10090), a letter suffix (1186 `0571A`) and `P01` (9817), or are these 98 ids accepted as known mismatches?
- **Addendum (user decision):** `scan_id_matches` uses `ScanId.try_parse(digits=None)` plus the `_{inv}_` fallback for non-parsing ids. Re-run: targeted 73 passed, full 959 passed; criterion 1 157/157, criterion 3 0 of 4,798,666 mismatching, protected state unchanged. Status now **done**; open question closed. Finding F05-15.3 is now a data-property.

### Step 15c: text-layer driver and CLI
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done
- **Files changed:** `archival_structures/text_layer/{build,compare}.py`, `pyproject.toml`
  (`[project.scripts]`), `tests/test_text_layer_{build,compare}.py`,
  `tests/test_packaging_core_imports.py`, `docs/text_layer.md`, `docs/index.md`,
  `docs/shared_library/changelog.md`, `reports/phase-05-text-layer.md`,
  `reports/data/phase-05-text-layer/step-15c/{check.py,protect.py,recipe.sh}`. Install run once:
  `.venv/bin/python -m pip install --no-deps -e .` (`.venv/bin/pip` does not exist).
- **Public API:** `text_layer.build.main`, `as-build-text-layer`, `text_layer.compare.*` (new)
- **Tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py tests/test_packaging_core_imports.py` -> 89 passed; full suite -> 975 passed
- **Acceptance:** 1: 157/157 (110 TSV byte-equal, 44 JSON equal, 3/3), 34.6 s with 8 workers. 2: manifest superset, new - ref = {archival_structures, preset}, layer-level values equal. 3: 0 to do in every stage, mtimes unchanged, 0.4 s. 4: v2.1 recipe on 1120,2555,8834 exit 0 (15/15), 18 s. 5: 13 NLP flags + `--preset`; estimate 0.5 min. 6: tests green, 7,079 protected files unchanged.
- **Report:** [`reports/phase-05-text-layer.md` § Step 15c](../../reports/phase-05-text-layer.md#step-15c-text-layer-driver-and-cli);
  findings F05-15c.1–4
- **Error analysis:** manifest diff explained key by key (git_head, pagexml_tools, python, argv/params, timestamps); estimate 30.8 s vs 34.6 s measured (ratio 1.12); a rerun still rewrites the manifest stage entries (as NLP).
- **Deviations from brief:** none
- **Brief defects:** `.venv/bin/pip` missing (used `python -m pip`); `compare` imports pandas lazily because the no-pagexml packaging test also forbids pandas; "byte-equal" holds for 135 of 157 (the 22 `extract_stats` differ in `source`); see report.
- **Open questions for the user:** none

### Step 13: pagexml-tools 0.9.0
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done
- **Files changed:** `pyproject.toml`, `tests/test_text_layer_provenance.py`, `tests/test_text_layer_extract.py`,
  `tests/test_analysis_neighbourhood_analysis.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md`,
  `reports/phase-05-text-layer.md`, `reports/data/phase-05-text-layer/step-13/envcheck.py`
- **Environments:** AS `.venv` pagexml-tools 0.8.1 -> 0.9.0, fuzzy-search 2.5.0 -> 3.0.4 (lxml 6.1.0, nltk 3.10.3, py7zr 1.1.3, pytest 9.1.1, pyppmd 1.3.1); NLP and LotA unchanged (code 0.9.0 from checkout, metadata 0.8.0)
- **Tests:** AS targeted -> 100 passed; AS full -> 976 passed; NLP 29 + 21 passed; LotA `test_derive_voc.py` 11 passed, full 327 passed
- **Acceptance:** 1 imports OK x3, `__version__` 0.9.0 x3. 2: 157/157 (110/44/3), 35.9 s. 3 pins and provenance test done. 4 all green. 5 7,079 protected files, 0 differences
- **Report:** [`reports/phase-05-text-layer.md` § Step 13](../../reports/phase-05-text-layer.md#step-13-pagexml-tools-090);
  findings F05-13.1-5
- **Error analysis:** 0.8.1 -> 0.9.0 diff: 9 import-only/cosmetic, 3 bug fixes, 2 behaviour changes (derived columns), none on the AS text path
- **Deviations from brief:** one extra test edit (neighbourhood test pinned the old `vertical_distance` bug)
- **Brief defects:** still said 0.8.2 outside the amendment; did not name the neighbourhood test; sibling CLAUDE.md not read (no sibling edits)
- **Open questions for the user:** none

### Step 15b: NLP text-layer shim
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done
- **Files changed:** NLP: `src/startpage/voc_text_layer.py` (shim), `scripts/build_voc_text_layer.py` (wrapper), `scripts/verify_as_text_layer.py` (new), `output/as-phase-05-originals/{src/voc_text_layer.py,scripts/build_voc_text_layer.py}` (copies). AS: `docs/shared_library/changelog.md`, `reports/phase-05-text-layer.md`, `plan/phase-05-text-layer/{PROGRESS,STATUS}.md`
- **Public API:** none in AS; NLP names unchanged (62/62 top-level names resolve)
- **Tests:** NLP `test_voc_text_layer.py` 29 passed; `test_evaluation.py test_semi_markov.py` 21 passed; `validate_voc_text_layer.py --help` exit 0; AS full 976 passed
- **Acceptance:** verify script exit 0: stored 157/157, A/B 157/157 + manifest, decide 2/2, names 17/17, protected 41,692 files unchanged; builds 32.7 s (current) and 33.6 s (originals), 8 workers. Shim has 4 `def`s.
- **Report:** [`reports/phase-05-text-layer.md` § Step 15b](../../reports/phase-05-text-layer.md#step-15b-nlp-text-layer-shim);
  findings F05-15b.1-3
- **Error analysis:** manifest diffs are only new keys, timestamps, git_head, out_dir/preset in argv/params ("other" empty); progress.log lines equal as multisets (only completion order differs)
- **Deviations from brief:** none
- **Brief defects:** runner for originals must set `sys.argv`; wbc_per_inventory columns taken from `build.stage_wbc`; `rm -rf` of scratch dirs is denied
- **Open questions for the user:** none

### Orchestrator follow-up (Opus, 2026-10-14/15)
- Ran steps 14, 15, 15c, 13 and 15b as Sonnet subagents and re-ran each step's acceptance
  checks independently: all reproduced.
- Step 15 scan-id check: the user chose ScanId (any digit width) with a fallback to NLP's
  `_{inv}_` test for non-parseable ids (F05-15.3).
- The release was pagexml-tools 0.9.0, not 0.8.2: an amendment was added to the step 13 brief,
  and a note to the step 15b brief.
- `docs/text_layer.md` and the `build.py` handoff docstring were fixed:
  - `pip install archival-structures` installs PyPI 0.8.0, which has no text layer, so install
    from the checkout;
  - they now explain that `as-build-text-layer` is a console script in the venv's `bin/`;
  - the recipe uses `<VENV>/bin/...`;
  - the nested-region example file is named.
- `chmod g+w` was applied to about 2,000 files that the step 13/15c pip installs had created in
  `.venv` without group write. They had blocked the user's own `pip install -e`.
- **P3d (user):** the v2.1 sample of 20 inventories against v1 gave `IDENTICAL (100
  comparisons)`.
- For the review: `compare.summarise` prints `content_equal 0` for the JSON kinds (NA counted
  as False). That is cosmetic but can be misread.

### Phase review
- **Session:** Opus 5.5, 2026-10-15 (fresh session). **Verdict:** accept with follow-ups.
- **Re-run** (one sequential script, `reports/data/phase-05-text-layer/review/run_all.sh`):
  - step 14 2/2, 52/52, 6,914;
  - step 15 157/157, 140/140, scan ids 0 of 4,798,666;
  - 15c CLI 157/157 (36.6 s), manifest superset, rerun no-op, recipe 15/15;
  - NLP verify ALL PASS on the second run (the first failed check 5 because the user's D5 move
    ran at the same moment);
  - NLP 29 + 21, LotA 327; protected 7,079 entries unchanged.
- **AS suite not reproducible as logged.** The user's reinstall on 2026-10-15 left pytest
  without `py.py` and removed the `vision` extra. With the shim from the scratchpad: 737
  passed, 2 failed (pandas 3.0.6, outside the text layer), 13 modules uncollectable.
  Text-layer tests 101 passed.
- **Findings:** 28 checked (27 confirmed, F05-14.4 revised); F05-R.1–R.7 added.
  - P3d recorded as R.1;
  - pandas 3 is R.2; stale AS dist-info in the siblings is R.3 (manifests say 0.1.0);
  - the systematic `„` misses are R.4: 446 of 696 fail only `end_rel_frac`;
  - the subset `wbc` overwrite hazard is R.5; `MIN_PAGEXML_TOOLS` 0.8.0 is R.6;
  - the outputs are the same under pandas 2.3.3 and 3.0.6 (R.7).
- **One-liners applied:**
  - the `neighbourhood_analysis.line_vertical_gap` docstring (fixed upstream in 0.9.0);
  - in the changelog, "not yet switched" → "switched in step 15b", plus step-13 notes on
    pagexml-tools 0.9.0 behaviour changes and on `MIN_PAGEXML_TOOLS`;
  - `plan/templates/STEP.md`: agent-shell rules, the pin-change grep, and the comparison mode
    per acceptance criterion.
- **Written:** `REVIEW.md`; `step-15r-review-fixes.md`; the report's Summary, Review and
  Follow-ups (status final); 35 rows in `reports/index.md`; `make reports` clean; the PLAN
  hand-off; the `plan/PLAN.md` row; `STATUS.md`; `COMMIT_MSG.txt`.
- **Release:** after 15r, v0.9.0 (minor).

### Step 15r: phase 05 review fixes
- **Session:** Sonnet 5.5, 2026-10-15. **Status:** done
- **Files changed:** AS: `archival_structures/analysis/sequence_patterns.py`, `archival_structures/text_layer/{provenance,build}.py`, `tests/{test_analysis_sequence_patterns,test_datasets_globalise_index,test_text_layer_provenance}.py`, `docs/text_layer.md`, `docs/shared_library/changelog.md`, `reports/phase-05-text-layer.md`, `plan/phase-05-text-layer/{PROGRESS,STATUS}.md`
- **Public API:** `segment_into_elements` also treats NaN/`pd.NA` as unlabelled; `MIN_PAGEXML_TOOLS` is (0, 9, 0) (changelog Step 15r)
- **Tests:** AS 799 passed, 2 skipped (torch), 0 failed, 6 collection errors (matplotlib x4, ipywidgets x2); NLP 29 + 21 passed
- **Acceptance:** CLI on 22 inventories exit 0, `TOTAL 157 / 157`; NLP verify `ALL PASS`
- **Report:** [`reports/phase-05-text-layer.md` addendum](../../reports/phase-05-text-layer.md#addendum-step-15r-review-fixes); no new findings
- **Error analysis:** pandas 3.0.6 stores `None` in a str Series as `nan`; old and new tests both give labels body/nan/body and 2 elements (pre-15r code: 3, with a spurious `nan` element); pagexml-tools 0.9.0 in all three environments
- **Deviations from brief:** none
- **Brief defects:** "976 passed, 0 errors" unreachable (6 uncollectable modules; 799 passed); over-long docstring line rewrapped
- **Open questions for the user:** none
- **Review (Opus, same session):** reran the AS suite (799/2/6, reproduces) and the old/new
  segmentation; corrected the addendum's pre-fix element count (3, not 1), the
  7→6 collection-error explanation and the extra name (`[notebook]`); rewrapped
  `docs/text_layer.md` L109.
