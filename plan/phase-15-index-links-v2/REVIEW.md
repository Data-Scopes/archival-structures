# Phase 15 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-30. The same session orchestrated the step sessions (Sonnet subagents) and then
  reviewed their work.
- **Verdict:** **accept with follow-ups.**
  - Every logged number reproduces, and the code does what the briefs say.
  - v1 rebuilds byte-identically.
  - The three failed 21c bars (L recall, A, E) are fair negative results of pre-registered rules, not implementation
    faults.
  - No code defect; no review-fixes step is needed. Two text defects were fixed in the review (docs F15-R.7, report
    F15-R.3), and 21c's findings were moved into the report's *Findings* table.
  - Recommendation: do not publish v2 (UA-057).

## Checklist

- [x] **Acceptance re-run** (table below), from my own scripts (the scratchpad `rev_barF.py` and `rev_21c.py`), and
      from fresh builds of v1 and v2 into the scratchpad. The step dirs were not written. Every number matches.
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,402 passed, 11 skipped (1,381 before
      the phase; +7 in 21b, +9 in 21c, +5 in 21d). No sibling touched.
- [x] **Error analysis** read individual cases in every step and is in the report, one section per step. The reviewer
      read:
  - 2 gold sides;
  - the A1 entries of S01 and S04;
  - the bar-F miss;
  - 2 sampled links (3280, 3283), which changed one classification (F15-R.3).
- [x] **Phase report final:**
  - 37 findings, each confirmed or revised;
  - the 21c findings moved into the main table;
  - findings appended to `reports/index.md` and the phase row updated;
  - `make reports` clean.
- [x] **Changelog:** *Unreleased* has 21b, 21c and 21d. Each matches the diff:
  - the new `FolioMapParams` fields come after the old ones with v1 defaults, and `FolioTarget.opening` and the
    `FolioMap` diagnostics are trailing fields;
  - `LocatorParams.sub_entry_punct`/`left_boundary`, L5–L7, `SG_AMOUNT_BEFORE_V2`, `SG_ENTRY_END`, the line/entry API,
    `volume_links_v2`, `V2_CONFIG`, `ENTRY_COLUMNS`, `LINK_COLUMNS_V2` and `--version`;
  - "no sibling imports affected" is right (no sibling imports either module).
- [x] **No archive knowledge in core:** patterns and thresholds are named parameters (`FOLIO_MAP_V2`, `SG_*` constants,
      `LocatorParams`, `assemble_entries` keywords). `V2_CONFIG` names SG presets, as v1's `SG_LOCATOR_PARAMS` does.
      The bar numbers are in the builder script, not the library.
- [x] **Leakage** (F15-R.6):
  - `select.json` 09:57:24 UTC, dev rows only (`eval_links.load_gold` filters by split on read);
  - test gold hashed 10:18:01 UTC (`hashes.txt`; `shasum -c` OK on all 5 files);
  - 21b `test_done.json` 10:22:11 and 21c 10:22:16, once each;
  - `grep -rn "step-21a\|line_gold\|target_gold" archival_structures scripts` → nothing; silver scoring only in 21d.
- [x] **21a gold sound:**
  - entry ids are consistent (`validate_gold.py` OK);
  - no reserves were needed (29/29 sides `index`);
  - targets were drawn per side by the seeded rule after the side was read. The test draw was run by the orchestrator
    after all 16 test sides were written and before any test target was opened.
- [x] **v1 regression:** `21d-v1` = `20r-a` (21d's `regress.txt`). The reviewer's fresh v1 build = `20r-a` in 123
      files and `volumes.tsv`.
- [x] **Brief defects** answered below.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 21a | sides / lines / entries / locators | 29 / 1,251 / 595 / 851; embedded 21 (25) | `summary_output.txt`; `validate_gold.py` OK; hashes OK | yes |
| 21a | usable targets | test 63, dev 26 | 63 / 26 (filter in `rev_barF.py`) | yes |
| 21b | v1 tables = 20r-a | 41/41 | fresh v1 build = 20r-a (folio_map included) | yes |
| 21b | LOO v1 / v2 | 21,010/21,334; 20,952/21,176 | `loo.tsv` (planner numbers; prototype agrees) | yes |
| 21b | bar F | v2 62/63, v1 62/63; dev 26/26 | 62/63, 62/63; 26/26 | yes |
| 21c | dev grid, selection | A0 + L7, F1 0.8303 | `select.json` before the test lock | yes |
| 21c | L P / R | 404/412, 404/472 | 404/412, 404/472 | yes |
| 21c | A | 355/398 | 355/398 | yes |
| 21c | E | 52/63 | 52/63 | yes |
| 21c | v1 baseline P / R / A | 407/421, 407/472, 358/401 | same | yes |
| 21d | v1 rebuild | = 20r-a | = 20r-a (123 files + volumes.tsv) | yes |
| 21d | v2 dev build | 41 done; 72,558 / 100,019 / 99,997 | fresh build: same counts, 164 files byte-equal to `21d-a` | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | observation | `reports/data/phase-15-index-links-v2/step-21a/` | The gold format could not hold entries embedded in merged lines (21; F15-R.2). It was amended mid-step, before any test side was read; 21c scores them as neutral for links. | done in the phase (orchestrator) |
| 2 | defect (text) | `reports/phase-15-index-links-v2.md`, F15-21d.5 | 3283 N=593 was called a wrong scan; it is right (F15-R.3). | fixed in review |
| 3 | defect (docs) | `docs/index_links.md`, *Rules (v2)* | L5/L6 were described wrongly (F15-R.7), and the opening rationale cited an unsupported "2%". | fixed in review |
| 4 | debt | `step-21a/show_scan.py` | It lists folio numbers only on short lines or at the end of the first/last line. Numbers split across items (`Waerop 597` + `3`) still mislead a reader. Tool only. | none (tool of a closed step) |
| 5 | observation | `index_links.py` `read_side_lines` | It reads the whole `lines` table per call; fine for one volume at a time (A0 does not call it). | none |
| 6 | observation | `PROGRESS.md` | 21a sessions 2 and 4 wrote no PROGRESS block (their facts are in STATUS and the session-7 block). | none |
| 7 | observation | `index_links.py` L7 | The range filter drops the gold locators 1780, 1443 and 1182: garbled or beyond the volume, never locatable. | none (known limit) |

## Error analysis read by the reviewer

- **Gold.**
  - S11 (3539) and S05 (3421), the first 32 lines each: all labels, entry ids and locators check against the text.
    The difficult cases are handled by the conventions:
    - k2/k4 of S11 (a right entry started in a merged line, `S` on its own line);
    - `1e. 562.` (562 only);
    - `32.` as furniture.
  - The 9 pre-freeze edits are legitimate.
- **Map.**
  - T034, 3274: the misread run is the cause, so 21b's class holds.
  - 3280 N=334: 0378 prints 332, 0379 `353`, 0380 `… November 331` (for 334) and 0381 335. Interpolation puts 334 on
    0381, one scan late. G3 gives `medium` (v1: `high`). This is the intended behaviour.
  - 3283 N=593: 0618 591, 0619 592, 0620 `Waerop 597` + `3`, 0621 `591` (for 594), 0622 595, 0623 596. The map's 0620
    is right, so 21d's class is revised.
- **Entries.**
  - A1 on S01: `Oraignen.` (H) is joined to entry 1; `L Proe vide Vranckrijck,` and `Lit: N.` are joined to the next
    entry.
  - A1 on S04: `Napels.` and `Savoyen.` (H) start the next entries. One of them carries 21 locators, which explains
    S04's 24/80 links under A1 against 78/80 under A0.
  - 21c's categories (heading attached 67, non-S start 45, merged 27) hold up.
  - On test (post hoc, after the lock), A2 + L7 confirms the pattern (F15-R.5).
- **Layer.** The 5 largest per-volume locator changes (3623, 3712, 3611, 3630, 3587) are L7 range/left-boundary/amount
  drops and G2 ties, as 21d says. Entries are identical to v1 in all 41 volumes.

## Brief quality

Defects reported by the sessions, and the response:

- **21a, no way to record an entry embedded in a merged line.** The orchestrator amended the 21a/21c briefs during the
  phase (`embedded_entries.tsv`, neutral for links). For the templates, a side-level gold brief must define what
  happens to text that has no line of its own.
- **21a, `draw_targets.py` drew every undrawn side, including test sides still being read.** Session 2 added
  `--split`. For the templates, a draw that runs mid-step must be restricted to the split that is complete.
- **21a, `show_scan.py` missed numbers at the end of a long line**, which led to 8 `neighbours` labels. Fixed before
  the freeze, label only. For the templates, a page-number tool brief says where the numbers can stand.
- **21a, minor:**
  - `scans_opened` was undefined;
  - there was no `content` value for a `none` target;
  - 1,263 "layout lines" against 1,251 non-empty text lines;
  - multi-line headings, page-continued entries and mid-line starts inside one column needed conventions.

  The sessions settled them consistently (PROGRESS session 7 lists them).
- **21b:** the `content_sides` format was unspecified, and the G3 sample pool includes G1/G2 changes. Both were
  answered in the report.
- **21c:**
  - "previous entry" for a numbers-only line after a heading was ambiguous (the session read it as the previous
    non-heading entry);
  - `n_neutral` counting was undefined;
  - the treatment of reserves was undefined (none used).
  - The heading rule's cost was larger than the brief suggested, but the brief documented the known case.
- **21d:**
  - the expectation "none located" for the 92 dropped `high` links was wrong (21 stay as `low` within ±6);
  - `process()` gained a `version` argument.
- **Orchestration** (new this phase: one Opus session ran Sonnet subagents in parallel):
  - STATUS.md must be edited per row, not overwritten, while steps run in parallel;
  - a grep-based wait on a STATUS phrase fired on the waiting session's own row. Use a sentinel file instead.

## Updates applied

- [x] Phase report `reports/phase-15-index-links-v2.md`:
  - *Summary*, *Review* and *Follow-ups* written;
  - every finding is `confirmed` or `revised`;
  - status set to *final*.
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated; `plan/CONTEXT.md` updated
- [x] phase `STATUS.md` overwritten
- [x] sibling notes: none (no sibling touched)
- [x] Release recommendation: **v0.20.0** (minor). It contains:
  - folio map v2 (`FOLIO_MAP_V2`);
  - locator variants L5–L7;
  - line-level entry assembly;
  - `volume_links_v2` and the builder's `--version v2`.

  All of it is backwards compatible.
