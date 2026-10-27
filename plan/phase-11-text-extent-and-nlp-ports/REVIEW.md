# Phase 11 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-27 (same session as planning; the user asked for plan, implementation and review in
  one session; implementation ran as four Sonnet subagents)
- **Verdict:** accept with follow-ups

## Checklist

- [x] **Acceptance re-run by the reviewer** (table below): 18b and 18c compare scripts re-run into the scratchpad,
  18a's `recheck_report84.py` re-run from scratch (all 22 zips re-parsed, single process). All numbers equal.
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1301 passed, 11 skipped (1241 before the
  phase; +9 text_extent, +23 unsupervised segmentation, +28 separator sheets incl. the review's one). The phase's three
  files: 82 collected. **NLP:** `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider` → 66 passed; no
  NLP file is newer than the phase plan (`find … -newer PLAN.md` outside `venv/`: none).
- [x] **Error analysis** read individual cases in every step (report sections 18a–18c); the reviewer's own reading is
  below.
- [x] **Report final**, findings in `reports/index.md`, `make reports` clean.
- [x] **Changelog**: `## Unreleased` has 18a, 18b, 18c with the affected NLP imports; checked against `git diff`
  (additive only; the one output change, the empty-frame columns of `compute_corpus_text_extents`, is listed; the
  review added the sorted-sheets note).
- [x] **No archive knowledge in AS core:** `BODY_REGION_TYPES_VOC`, `VOC_SHEET_PATTERN`, `VOC_MAX_SHEET_CHARS` are
  named, never defaults of a filter (`region_types=None`); the DP constants keep NLP's values with the label-statistics
  caveat in the code comment.
- [x] **Leakage:** no label is read by `text_extent`, the segmenters or the sheet detector. Labels appear only in the
  re-check and error-analysis scripts. The DP's two constants are label-derived corpus statistics (stated in code and
  in `leakage_and_splits.md`).
- [x] **`region_types=None` identical** (P6): `test_region_types_none_is_identity`, and the all-lines re-check
  reproduces NLP's AS-0.13.0 extents (max |Δ| 1.1e-16, CSV round trip).
- [x] **Existing signatures unchanged:** `git diff` of the three modules shows additions only (kwargs appended after
  the existing ones).
- [x] **Brief defects** answered (below).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 18a | extents, both modes, 20,912 pages, max \|Δ\| | 20,912/20,912; ≤ 1.11e-16 | 20,912/20,912; ≤ 1.11e-16 (identical `recheck.tsv`) | yes |
| 18a | all-lines class = NLP `text_extent` | 19,836/19,836 | 19,836/19,836 | yes |
| 18a | early_end → END, all / body | 3,112 0.13978 0.31545 / 2,762 0.17053 0.34155 | identical | yes |
| 18a | 8 gap bins | 8/8 | identical `bins.tsv` | yes |
| 18b | calibrated weight; DP 44 lists; TT 22; 41,810 rows; rate 22 | 0.0293559853524414; 0 mismatches | same, 0 mismatches | yes |
| 18c | 169 sheets; 12 `apply_rule` runs; 20,912 next ids | 0 mismatches | same, 0 mismatches | yes |
| 18d | names import; index row + toctree; `make reports` | exit 0; L24, L38; clean | same | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (fixed in review) | `analysis/separator_sheets.py` `apply_rule` loop | `apply_rule` iterated the `sheets` set: with two consecutive sheets the result depended on set order, i.e. on `PYTHONHASHSEED` (NLP has the same latent bug). 0 consecutive sheets in the 22 inventories, so the AS = NLP check was unaffected | one-liner: `for inv, pid in sorted(sheets)`; test `test_consecutive_sheets_order_independent` (passes under seeds 1–3); changelog line; F11-R.3 |
| 2 | debt | `subsequence_detection.py` `textiling_depth_scores` | `np.convolve(mode="same")` zero-pads: on NLP's 22 sequences, 21 inventories get spurious starts at page 1 **and** at the last page (42 of 1,981 TextTiling starts; none labelled). Kept for parity (P2) | documented in `docs/stream_analysis.md`; F11-R.2; fix (edge padding) only together with the NLP shim and an NLP re-run |
| 3 | debt | `docs/challenges/leakage_and_splits.md` L39–40 | linked `../../reports/…`, outside the Sphinx tree (a broken link once `make docs` works) | fixed in review: plain path |
| 4 | debt | `docs/shared_library/upstream_inventory.md` C10 row; `docs/cross-project-review.md` L93–95 | the inventory called the convention "the same" and the granularity rule settled; the review document says Framing A is AS's default (F11-18d.1) | inventory: dated *Done* row added in review. The cross-project review is a dated record, overridden by its own §0 decisions 9–10: left as is |
| 5 | observation | step 18a report, *Conclusion* | "the all-lines early_end flag is largely a catch-word effect" overstates: of 1,039 flags the body variant drops (pages with body lines), 550 have a catch-word bottom; of 3,112 flags, 1,904 survive | revised in the report (F11-18a.2 revised) |
| 6 | observation | `separator_sheets.apply_rule` | an unknown convention raises only when a sheet of that inventory is in `df`; int dict keys silently mean "none" (inventories are compared as `str`) | none (docstring says `str`); listed for the shim step |

## Error analysis read by the reviewer

- **18a, catch-words (F11-R.1).** From `class_all/class_body.tsv.gz` and `ea_changed_types.tsv`: among the early_end
  flags the body variant changes, pages whose all-lines bottom is a **catch-word** are rarely document ends: END rate
  0.025 among the 550 dropped flags and 0.113 among the 585 gained ones (447 of those 585 are `INSIDE`), against 0.188
  and 0.278 for the other dropped and gained flags. A catch-word announces the next page's first word, so the document
  continues; removing catch-words from the margin turns those pages into early_end. Catch-word presence is a better
  anti-END cue than a margin input. This is a reading of the changed pages only (1,897 of 20,912), not a corpus rate.
  The step's own error analysis (types that set the margin, the non-monotone gap) holds up; its conclusion was revised
  (finding 5).
- **18b, TextTiling edges (F11-R.2):** counted on all 22 inventories (script in the scratchpad, NLP interpreter with
  `PYTHONPATH=<AS>`): starts at page 1 in 21, at the last page in 21, labelled starts at those positions 0. The step's
  2555 reading (10 DP starts: 4 separator sheets, 3 dated entry heads, 2 empty, 1 signature fragment) is consistent with
  `dp_starts_2555.tsv`.
- **18c:** checked that no detected sheet is followed by another sheet (0 of 169), which is why finding 1 did not show
  in the equivalence check. The near-miss reading (5 likely missed of 174, judged from text only) is plausible; images
  were not read, by the reviewer either.
- **18d:** read the three pages end to end. The hypothesis section's evidence line "mostly separator sheets and dated
  entry heads" over-generalised a sample of 10; reworded to the counts. The page cites only names that import.

## Brief quality

- 18a: "pages without lines are absent from `page_extents*.csv`" was wrong (1,076 rows with `n_lines == 0`); the
  step handled it. Cause: planning measured row counts but not the empty-row share. Template rule already exists
  ("say whether an expectation was measured"); the planner stated it as measured without checking `n_lines == 0`.
- 18b: the brief expected "constant input → only `[0]`" from TextTiling; the zero padding makes that false for a
  non-zero constant. The step tested the verbatim behaviour and documented it. Lesson: a brief that predicts behaviour
  of ported code must run that prediction once at planning, as CONTEXT §5 already says for commands.
- 18b: macOS `zcat` does not read `.gz` files without a `.Z` suffix; use `gzip -dc` or pandas. Added to CONTEXT §5.

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; every finding confirmed, revised or withdrawn;
      status *final*
- [x] `reports/index.md`: the phase row and its findings appended
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed (the one defect was a one-liner, fixed with a test)
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated; `plan/CONTEXT.md` updated
- [x] phase `STATUS.md` overwritten
- [x] Release recommendation: **v0.16.0** (minor: new public API in three modules, all additive; one behaviour fix
      in an empty-input frame)
