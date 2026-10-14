# Phase 02 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Seeded
- **Session:** Opus, 2026-10-14 (the session that set up `plan/`)
- `PLAN.md` seeded from `upstream_inventory.md` C2–C5, §4 and §6. No sibling code read.
- User decision recorded: LotA's `lb_adjusted` column is retired (P1).

### Phase planning
- **Session:** Opus, 2026-10-14. No code changed.
- **Read:** the sources listed in `PLAN.md`; AS `ead_digital`, `ead_text`, `ead_tree`,
  `ead_fields`, `ead_views` directly; LotA through two Explore subagents (current line ranges,
  signatures, tests).
- **Briefs written:** 03b, 03c (new step), 04, 05, 06.
- **Open questions settled:**
  - LotA outputs were never regenerated after step 2 (scope notes 2026-10-03, titles
    2026-10-08). This became step 03c.
  - No per-repository mechanism map (P7).
  - `classify_shape` moves (P8).
  - `hierarchy.py`/`model/node.py` match changelog L49–51.
  - CLI commands and timings are recorded in `PLAN.md`.
- **User decisions:**
  - baselines in `output/baselines/{pre,post}-as-step-2/` via a new step 03c (P4);
  - `target_archive` is serialised as a 5th reference field only when set (P5).
- **Planning decisions:**
  - `target_archive` None also when the code equals the current archive (P6);
  - `ead_views` gets an `archive_id` argument (P9);
  - `split_unittitle_dates` returns per-direct-child spans, and LotA's regex date fallback is
    deleted (P10).
- **Checked by prototype (scratchpad only):**
  - The new NA inventory regex gives the expected matches on 8 cases. It also showed that the
    old regex swallows `1.11.01.01` as an inventory token in mixed lists (hazard 4).
  - Putting control-character markers into an lxml tree fails. A marker-in-walk design holds
    the `split_unittitle_dates` invariant on 22,197 SG + 14,505 VOC titles; the counts are in
    brief 06.
- **Baselines measured:** AS suite 582 passed (97 s); LotA `test_hierarchy` +
  `test_digitisation` 22 passed (23 s). LotA `test_derive_voc.py` still fails to import.
- **Side findings:**
  - LotA `ArchiveNode`'s docstring overstates its equality (03b fixes the docstring);
  - `_segments` ignores the `<lb/>`-before-punctuation rule (ported verbatim; review
    follow-up).

### Step 03b: LotA switches to AS node ids
- **Session:** Opus 5.5 (not Sonnet; the user started this session on Opus), 2026-10-14.
  **Status:** done, pending the user's sign-off
- **Files changed:**
  - AS: `archival_structures/parsers/ead_views.py`, `tests/test_parsers_ead_views.py`,
    `docs/shared_library/changelog.md` (Step 3b entry, added with Edit);
  - LotA: `archival_language/ead/hierarchy.py`, `archival_language/model/node.py`,
    `STATUS.md` (appended).
  - The originals are backed up in this session's scratchpad (`orig/`).
- **Public API:** `ead_views.to_frame(finding_aid, level, archive_id: Optional[str] = None)` and
  `ead_views.unit_of_description_frame(finding_aid, archive_id: Optional[str] = None)`. `None`
  means `finding_aid.default_archive_id`, which is today's behaviour. The private helpers
  `_frame_from_components`/`_row_from_component` take `archive_id` too. LotA `ArchiveTree`
  and `ArchiveNode` public surface unchanged; the private `_nodes_by_component_id` is renamed
  `_nodes_by_component`.
- **Tests:**
  - AS targeted (`test_parsers_ead_views.py` + `test_parsers_ead_node_id.py`): 25 passed.
  - AS full suite: **584 passed** (101 s).
  - LotA `--ignore=tests/test_derive_voc.py`: **320 passed** (56 s), `test_hierarchy` included.
- **Acceptance:**
  1. Snapshot before/after of all 9 archives (pickles in scratchpad `before/`, `after/`).
     `frame()[["node_id","citation"]]` gives identical rows in the same order, and the full
     frame (all columns, same column order and index) is identical for the 8 non-empty
     archives. Rows: SG 17,937; VOC 16,447; NL-AsdSAA_5075 32,224; NL-HaNA_2.10.01 4,683;
     US-CtY-P_ypm.pbar.000001 1,426; NL-ZlCO_0266 386; NL-AsdSAA_89 150; GB-3624_oh 3;
     NL-HtBHIC_SVO_1595_1649 0. No old `frame()` raised. The one difference is on BHIC's
     0-row frame: the `node_id` dtype went from `str` (an artefact of the old `.map` on an
     empty Series) to `object`, which matches that frame's `citation` column. Non-empty frames
     are `str` both before and after.
  2. Walk node ids are identical for all 9 archives (so are `parent_id` and `ancestor_ids`,
     checked as well).
  3. AS 584 passed and LotA 320 passed.
  4. The grep returns nothing (exit 1).
- **Error analysis:**
  - `default_archive_id` differs from the LotA id for 7 of 9 archives. Only SG and VOC match.
    BHIC→`SVO_1595_1649` (0 rows), US-CtY-P→`ypm.pbar.000001`, GB-3624_oh→`gb3624-oh`,
    NL-AsdSAA_89→`89`, NL-HaNA_2.10.01→`2.10.01`, NL-AsdSAA_5075→`5075`,
    NL-ZlCO_0266→`0266`. For each I compared 3 rows (first, middle, last), e.g.
    `89:5.0` vs `NL-AsdSAA_89:5.0` and `gb3624-oh:8.9.2.3` vs `GB-3624_oh:8.9.2.3`. In every
    case the suffixes match and only the prefix differs. Without the new argument, the
    `frame()` membership check would raise for all 7.
  - Title check on 10 seeded random rows per archive (`random_state=42`; 3 for GB, 0 for
    BHIC): 73 rows, 0 mismatches.
- **Deviations from brief:**
  - The session ran on Opus, not Sonnet.
  - The old join also had `frame = frame.copy()`, which I removed with it: the AS frame is
    freshly built, so the copy is not needed.
  - `test_default_archive_id_unchanged` maps frame rows back to components through `citation`,
    because the frame carries no `index_path`.
- **Brief defects:**
  - `hierarchy.py` line numbers in `frame()` were off by 4–5. The citation join is L141–144
    (4 lines including `.copy()`), not L136–138. The unmatched check is L146–151 and the
    duplicate check L152–157. The other line numbers were right.
  - The brief did not mention that BHIC's frame is empty (0 rows) or the empty-frame dtype
    effect.
- **Open questions for the user:**
  - Is the `str`→`object` dtype change on BHIC's empty `frame()` acceptable? (My view: yes.
    There are no rows, and it matches the other columns.)
  - LotA `model/node.py`'s module docstring (L3–7) still says upstream provides no node
    identity, that `EADComponent` is unhashable and that `unit_of_description_frame` keys on
    `id(component)`. The brief only named the class docstring, so I left this alone. Should
    it be updated in a later step?

### Planning amendment: phase reports
- **Session:** Opus, 2026-10-14 (the planning session, continued). No code changed.
- **User request:** a reports directory with a written report per phase, including the error
  analysis, in which findings are easy to look up. Reports in Markdown, with an HTML converter
  run from a Makefile target.
- **Protocol changes:**
  - `plan/README.md`: the `reports/` layout, finding IDs `F<phase>-<step|P|R>.<n>`, rules 8–9,
    budgets, `reports/index.md` shared and append-only;
  - new `plan/templates/REPORT.md`;
  - `STEP.md`, `REVIEW.md` and `PHASE_PLAN.md` templates and `PROMPTS.md` updated.
- **Phase 02:**
  - new step 03d (`step-03d-reports-tooling.md`). It builds `make reports` (pandoc 3.12.1, a
    Lua filter for `.md` → `.html` links, embedded CSS, output in the gitignored
    `reports/_build/`), creates `reports/index.md` and the phase-02 report, and backfills
    planning findings F02-P.1–6 and 03b's section from this file. The recipe and filter were
    tested in a scratchpad.
  - Briefs 03c, 04, 05, 06 now have a report deliverable, a *When done* step and a
    `Report:` line in their PROGRESS block.
  - 03c's LotA `report/` file is replaced by the AS phase report.
  - `PLAN.md`: P11, the 03d row, owned files, checklist.
- **Size:** brief 05 is now 286 lines (budget ~250); not split, because its parts don't
  separate cleanly.

### Step 03d: reports/ directory and make reports
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done
- **Files changed:** `Makefile` (targets `reports`, `reports-open`; header and `.PHONY`),
  `.gitignore` (`reports/_build/`), `reports/_tools/md-links.lua`, `reports/_tools/report.css`,
  `reports/index.md`, `reports/phase-02-ead-for-lota.md`.
- **Public API:** none
- **Tests:**
  1. `make reports`: exit 0, printed `reports/_build/index.html` and
     `reports/_build/phase-02-ead-for-lota.html`; no warnings.
  2. `index.html` hrefs include `phase-02-ead-for-lota.html` (no `.md`).
  3. Plan link is `../../plan/phase-02-ead-for-lota/PLAN.md`; every `href="#…"` in the phase
     report has a matching `id=`; 2 `<style` matches (CSS embedded).
  4. `PATH=/usr/bin:/bin make reports`: "pandoc not found (brew install pandoc)", exit 2.
- **Acceptance:** 1. all 4 checks pass. 2. 6 planning findings (F02-P.1–6) and 5 03b findings
  (F02-03b.1–5), each with ID, kind, tags, status `draft` and a section link. 3. no package
  code changed; the suite was not run.
- **Report:** reports/phase-02-ead-for-lota.md (planning findings F02-P.1–6, step 03b F02-03b.1–5)
- **Deviations from brief:** none. The `.gitignore` entry has a comment header line.
- **Brief defects:** the session prompt asked for a section in
  `reports/step-03d-reports-tooling.md`, but the brief says 03d gets no report section
  (infrastructure, `PROGRESS.md` only), so none was written. I followed the brief.
- **Open questions for the user:** none. Error-analysis read of the built HTML as a reader:
  each finding is one click from the index (index → phase report → section link). The TOC at
  depth 3 lists the step sections. The findings table is wide (6 columns) and scrolls
  horizontally on narrow screens.

### Step 04: C5 DigitisationSummary.has_evidence
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done (pending user sign-off)
- **Files changed:** AS `archival_structures/parsers/ead_digital.py`, `tests/test_parsers_ead_digital.py`,
  `docs/shared_library/changelog.md`, `reports/phase-02-ead-for-lota.md`; LotA
  `archival_language/ead/digitisation.py`, `archival_language/corpus/coverage.py`, `STATUS.md`
- **Public API:** `ead_digital.MECHANISM_NAMES`; `DigitisationSummary.has_evidence(mechanisms: Collection[str]) -> bool`;
  `NodeDigitisation.summary` (last field, `compare=False`, `repr=False`)
- **Tests:** AS `.venv/bin/python -m pytest -q -p no:cacheprovider` → 589 passed; LotA targeted → 22 passed;
  LotA full (`--ignore=tests/test_derive_voc.py`) → 320 passed
- **Acceptance:** 1. 5 tests added, pass. 2. LotA digitisation + text_coverage pass unchanged (22, brief said 10+11).
  3. `branch_coverage` and 3 coverage frames for SG and VOC `DataFrame.equals` before/after (coverage frames
  used an empty quality frame, so `inventory_text_coverage` had 0 rows). 4. no `_has_evidence` left.
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 04; findings F02-04.1–6
- **Error analysis:** all 7,511 `is_digitised`-without-evidence units across 9 archives are handle-only
  (SG 7,079; ZlCO 338; VOC 84; HaNA 10). SG `has_evidence` share 60.5%.
- **Deviations from brief:** none
- **Brief defects:** no coverage-frame input was named, so I used the empty `inventory_quality` frame from
  `test_text_coverage.py`; the brief's 10 + 11 test counts are really 22.
- **Open questions for the user:** none

### Step 03c: LotA output baselines after AS step 2
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done (pending user sign-off)
- **Files changed:** LotA `output/baselines/` (new), `archival_language/cli/diff_outputs.py`, `tests/test_diff_outputs.py`, `STATUS.md`; AS `reports/phase-02-ead-for-lota.md`, `reports/data/phase-02-ead-for-lota/03c-*.tsv`
- **Public API:** none in AS; LotA `archival_language.cli.diff_outputs.diff_frames`
- **Tests:** LotA `tests/test_diff_outputs.py` → 3 passed; full (`--ignore=tests/test_derive_voc.py`) → 323 passed
- **Acceptance:** 1. pre copies `cmp`-identical; post holds 2 + 9 + census + token-kinds; wall times 3.8 s (scope notes), 19.7 s (titles). 2. scope notes: identical keys, 0 changed cells (the predicted changes do not occur in SG/VOC). 3. SG + VOC titles: 23 / 12 / 11 / 2 / 0 reproduce. 4. other 7 archives: 0 changed cells. 5. census: 4 cells. 6. green.
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 03c; findings F02-03c.1–6
- **Error analysis:** all 23 changed titles read, plus 10 unchanged; no SG/VOC `<scopecontent>` has `<lb/>` glue or child-edge words, hence 0 scope-note changes. 3 SG titles gain a space before a closing quote.
- **Deviations from brief:** none
- **Brief defects:** the brief's step-2 scope-note prediction (`derdenSeptember`, `Eems . Met`) does not occur in the notes; the `Eems . Met` case is a title.
- **Open questions for the user:** should AS `element_text` add quotes to the no-space-before set (F02-03c.4)?

- **Step 03c follow-up (user decision, 2026-10-14):** AS `ead_text._NO_SPACE_BEFORE` now includes `' " ’ ”`; test `test_lb_before_quote_is_dropped`; changelog entry *Step 3c follow-up*. Effect: 3 SG titles change, nothing else. New baseline `output/baselines/post-quote-rule/` for steps 05 and 06. AS 590 passed. Finding F02-03c.4 is now `bug-fixed`. Step 03c closed by the user.

### Step 05: C3 scope-note parser into AS, with cross-archive references
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done (pending user sign-off)
- **Files changed:** AS `archival_structures/parsers/ead_scope_notes.py` (new), `tests/test_parsers_ead_scope_notes.py` (new), `docs/shared_library/changelog.md`, `reports/phase-02-ead-for-lota.md`; LotA `ead/scopecontent.py`, `ead/scopecontent_survey.py`, `cli/parse_scopecontent.py`, `tests/test_scopecontent.py`, `output/scope-notes/*.tsv.gz` (regenerated), `STATUS.md`
- **Public API:** `ead_scope_notes`: `ScopeNote`, `ScopeReference` (+`target_archive`), `ShapeRule`, `NA_SHAPE_RULES`, `classify_shape(text, tags, rules)`, `ReferencePatterns`, `NA_REFERENCE_PATTERNS`, `descendant_tags`, `parse_scope_notes(component, archive_id, *, source_path, tags, shape_rules, patterns)`, `iter_scope_notes`, `scope_notes_frame`
- **Tests:** AS `.venv/bin/python -m pytest -q -p no:cacheprovider` → 606 passed; LotA `venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py` → 317 passed
- **Acceptance:** 1 yes (16 new, 606 total). 2 yes (317 = 323 - 6). 3 keys identical; only `references` changed (SG 1, VOC 90), `n_references` 0; 91 suffix-added, 0 swallowed-code. 4 notes with a cross-archive ref: VOC 90, SG 1; targets `1.11.01.01` 83, `2.01.27.01` 6, `1.01.02` 1 (VOC), `1.10.110` 1 (SG). 5 yes.
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 05; findings F02-05.1–6
- **Error analysis:** read 31 of 91 changed rows (all suffixes correct), all 2 unchanged rows with `Aanwinsten`/an archive code (no real miss). No archive code was swallowed in SG or VOC.
- **Deviations from brief:** none.
- **Brief defects:** the brief says to diff against `post-as-step-2/` but STATUS says `post-quote-rule/`; both give identical scope-note diffs. The "second kind" of change (swallowed code, F02-P.2) has no instance in the data.
- **Open questions for the user:** none
- **Follow-up 2026-10-14 (user request):** `_segments` applies the `<lb/>`-before-punctuation rule (F02-05.7); AS 607 tests; SG/VOC `segments` 0 changed cells.

### Step 06: C4 split_unittitle_dates; lb_adjusted retired
- **Session:** Sonnet 5.5, 2026-10-14. **Status:** done (pending user sign-off)
- **Files changed:** AS `archival_structures/parsers/ead_text.py`, `parsers/ead_fields.py`, `tests/test_parsers_ead_fields.py`, `docs/shared_library/changelog.md`, `reports/phase-02-ead-for-lota.md`; LotA `archival_language/facets/title_norm.py`, `cli/normalise_titles.py`, `tests/test_title_norm.py`, `tests/test_title_vocab.py`, `output/titles/*` (regenerated by `normalise_titles`), `STATUS.md`
- **Public API:** `ead_fields.split_unittitle_dates(unittitle_element) -> (prose, spans)`; private `ead_text._SPAN_START/_SPAN_END/_raw_marked_text`; removed from LotA: `NormalisedTitle.lb_adjusted`, the `lb_adjusted` column, census `n_lb_adjusted`
- **Tests:** AS `.venv/bin/python -m pytest -q -p no:cacheprovider` → 618 passed; LotA `venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py` → 316 passed
- **Acceptance:** 1 yes (11 new, 618). 2 yes (316 = 317 - 1; `test_title_norm.py` 22 tests). 3 vs `post-quote-rule/titles/`: keys identical, `lb_adjusted` only in OLD, 0 changed cells in all 9. 4 census 0 differing cells, `token-kinds-by-level.tsv` `cmp`-identical. 5 buckets (locatable/fallback/not/empty): SG 22,126/0/0/7, VOC 14,498/0/0/7, HaNA 2.10.01 3,503/0/0/2, others 0; spans excised 23,076 / 14,692 / 3,503. 6 grep empty.
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 06; findings F02-06.1–7
- **Error analysis:** read 71 titles (30 random, 10 with `<lb/>`+`<unitdate>`, 15 multi-child, 16 `None` span): no wrong cut; blemishes (left-over separators, year twice) come from the source.
- **Deviations from brief:** `test_split_fallback_inserts_space_and_rekinds` also patches `ead_fields.split_unittitle_dates` (it faked glue via `title_norm.element_text` only). Kept `_collapse` and `_insert_space_nth` (still used).
- **Brief defects:** acceptance 3 names `post-as-step-2/titles/`, but STATUS (and baselines README) say to diff against `post-quote-rule/`; I used the latter. The brief quotes the old `_NO_SPACE_BEFORE` class without the quotes added in step 03c's follow-up; I kept the current class and added the marker prefix. The `_split_fallback` test dependency above was not mentioned.
- **Open questions for the user:** none

### Phase review
- **Session:** Opus 5.5, 2026-10-14. **Verdict:** accept with follow-ups; no review-fixes brief.
- **Re-run:**
  - AS 618 passed, LotA 316 passed.
  - Fresh regeneration of all LotA outputs (scratchpad), diffed against the canonical files:
    0 cells.
  - Independent pandas diffs reproduce 03c, the quote rule, 05 and 06 to the cell.
  - Step 04 counts reproduced.
  - 03b confirmed indirectly: the node-id sequence of the 9 title outputs equals
    `pre-as-step-2/` (116,058 nodes).
- **Error analysis:**
  - read all 26 changed title rows (03c and quote rule) and 34 of the 91 changed notes (05);
  - checked all 40,207 NA title cuts mechanically and read 36 of them;
  - grouped SG's handle-only units by series.
  - Findings F02-R.1–9. 4 step findings revised (F02-P.2, 03c.6, 04.5, 06.5), 33 confirmed.
- **Files changed:**
  - `plan/phase-02-ead-for-lota/{REVIEW.md (new), PLAN.md (checklist, hand-off), STATUS.md}`;
  - `plan/PLAN.md`;
  - `plan/README.md` (rule 10), `plan/templates/STEP.md`, `plan/PROMPTS.md`;
  - `reports/phase-02-ead-for-lota.md` (final; also fixed the 7 broken step-06 anchors) and
    `reports/index.md` (46 rows);
  - `docs/shared_library/changelog.md` (`v0.5.0` heading, new *Unreleased (after v0.5.0)*, P6
    bullet);
  - LotA `STATUS.md` (appended).
- **No code changed.**
