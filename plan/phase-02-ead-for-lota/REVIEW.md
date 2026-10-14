# Phase 02 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-14
- **Verdict:** accept with follow-ups. No code defect was found, so no review-fixes brief is
  needed. Two documentation defects were fixed in this session (changelog heading and the P6
  note, finding F02-R.9).

## Checklist

- [x] **Acceptance re-run.** Every step criterion was re-measured; see the table below. The
  exceptions are 03b's and 04's before snapshots, which no longer exist. F02-R.2 (node-id
  sequence against `pre-as-step-2/`) and code reading stand in for them.
- [x] **Suites.** AS `.venv/bin/python -m pytest -q -p no:cacheprovider`: **618 passed** (100 s),
  with the real-data tests run, not skipped. LotA `venv/bin/python -m pytest -q -p
  no:cacheprovider --ignore=tests/test_derive_voc.py`: **316 passed** (56 s).
- [x] **Output diffs read row by row.** 03c: all 21 SG and 2 VOC changed rows, plus the 3
  quote-rule rows. 05: 34 of 91 (1 SG, 33 VOC), and all 91 checked mechanically to be
  suffix-only. 06 has 0 changed cells, so the reviewer read 36 random cuts and 14 left-over
  separators instead, and checked all 40,207 spans mechanically. See the report's *Review*.
- [x] **Changelog** has Step 3b, Step 4, Step 3c follow-up, Step 5 and Step 6, each listing the
  LotA imports. Fixed here: the *Unreleased* heading (v0.5.0 had been tagged with steps 1–3 still
  under *Unreleased (after v0.4.1)*), and a bullet on P6's string equality.
- [x] **LotA copies deleted.** A grep of `archival_language/` and `tests/` for `_node_id`
  (definition), `_has_evidence`, `_structural_date_spans`, `_structural_text_and_prose`,
  `lb_adjusted`, `_DATE_SENTINEL` and `_retag_ranges` finds no definitions or uses; only
  `root_node_id` names match. `re.compile` in `ead/scopecontent.py` finds nothing. No `id(`
  join is left in `hierarchy.py`/`model/node.py`. `hierarchy.py:138` builds `frame()` from
  `unit_of_description_frame(..., archive_id=self.archive_id)`.
- [x] **No NA regex hardcoded in AS core (P3).** In `ead_scope_notes.py`, the NA regex pieces
  (`_MONTH`, `_NA_ARCHIVE_CODE`, `_NA_CUE`, `_INV_SEPARATOR`) are used only to build
  `NA_REFERENCE_PATTERNS` (L216–236). `classify_shape` (L159) is one generic loop over `rules`.
  `_inventory_references` reads the optional group through `groupdict().get("archive")`, so it
  works with patterns that lack the group (tested).
- [x] **`is_digitised` unchanged (P2).** `ead_digital.py:237–241` is still
  `bool(self.mechanisms)`; only the docstring was added.
- [x] **Baselines.** `output/baselines/README.md` exists. `pre-as-step-2/`, `post-as-step-2/`
  and `post-quote-rule/` each hold 2 scope-note files and 9 title files plus the census and
  token kinds. Their diffs reproduce the logged numbers (F02-R.1).
- [x] **Report.** It has one section per step (03b, 03c, 04, 05, 06), each with an error
  analysis that read individual cases. 03d has none by its brief. All 37 step and planning
  findings are set: 33 confirmed, 4 revised. *Summary*, *Review* and *Follow-ups* are written,
  and the status is *final*.
- [x] **`reports/index.md`** has the 46 phase findings appended, and `make reports` is clean.
- [x] **`Brief defects`** are answered below.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 03b | `frame()`/walk node ids identical on 9 archives | identical | the `node_id` sequence of all 9 title outputs (116,058 nodes) equals `pre-as-step-2/`, which the old `_node_id` produced | yes (indirect) |
| 03b | AS / LotA suites | 584 / 320 | 618 / 316 at phase end | yes (later steps added and moved tests) |
| 03c | titles SG+VOC `lb_adjusted`/`original`/`text`/`date_span`/`date_locator` | 23 / 12 / 11 / 2 / 0 | 23 / 12 / 11 / 2 / 0 | yes |
| 03c | scope notes pre vs post | 0 cells | 0 cells | yes |
| 03c | other 7 archives; census | 0 cells; 4 cells | 0 cells; 4 cells (`token-kinds-by-level.tsv` also differs, from the same 6 punct tokens; not logged) | yes |
| 03c follow-up | post-as-step-2 vs post-quote-rule | 3 SG titles | 3 SG titles (`5.2.0.4.2.69`, `5.2.4.85`, `5.2.4.186`), all closing quotes | yes |
| 04 | SG `has_evidence` share; handle-only units | 60.5%; SG 7,079, VOC 84, HaNA 10, ZlCO 338 | 60.5% (10,857/17,937); 7,079 / 84 / 10 / 338; AsdSAA_5075 0 | yes |
| 05 | scope-notes diff vs `post-quote-rule/` | `references` SG 1, VOC 90; all else 0 | same; all 91 are suffix-only | yes |
| 05 | targets | `1.11.01.01` 83, `2.01.27.01` 6, `1.01.02` 1, `1.10.110` 1 | same (AS parse) | yes |
| 05 | Aanwinsten/code notes without a reference | 2 | 2 (VOC `0.7.1.12.11.10`, SG `1.1.1`) | yes |
| 06 | titles diff vs `post-quote-rule/` | 0 cells, only `lb_adjusted` gone | same on all 9; census only `n_lb_adjusted` gone; token kinds equal | yes |
| 06 | date buckets | SG 22,126/0/0/7, VOC 14,498/0/0/7, HaNA 3,503/0/0/2 | same (census) | yes |
| 06 | real-data invariant | 22,197 / 14,505, 7 / 7 `None` | same, and 0 span mismatches over 40,207 titles | yes |
| all | canonical outputs reproducible from the current code | — | fresh regeneration: 0 changed cells in all 13 files | yes |

## Findings

Most serious first. **Defect** means must fix (it goes into a review-fixes brief), **debt**
means recorded and deferred, and **observation** means no action.

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | debt | `archival_structures/parsers/ead_scope_notes.py:267` | P6 compares the written archive code with `archive_id` by string equality. This is right for SG/VOC (LotA ids `1.01.02`/`1.04.02`), but under a prefixed id (`NL-HaNA_2.10.01`) a self-reference becomes cross-archive. 0 real cases today (F02-R.6) | Follow-up: an `own_codes` parameter or a code-suffix comparison, before LotA parses scope notes of a prefixed-id archive. Noted in the changelog |
| 2 | debt | `archival_structures/parsers/ead_fields.py:304–305` | `split_unittitle_dates` guards its invariants with `assert`, which `python -O` strips, so a violated invariant would silently give wrong spans. The first `assert` also walks the element a second time through `element_text` | Low priority. Replace with an explicit `if … raise ValueError` when the module is next touched |
| 3 | debt | LotA `facets/title_norm.py:381` (behaviour) | `prose` keeps the separators that a cut date leaves behind: 45% of SG titles end in `,`/`;`/`:` (F02-R.4). This is unchanged by the phase, but `prose` now comes from AS | User decision; would change LotA output |
| 4 | debt | LotA `facets/title_norm.py:66`, `cli/normalise_titles.py:59,195` | The `date` token kind and the `n_tokens_date` census column are always 0, left over from the retagging path that step 06 deleted | Remove in a LotA cleanup; changes census columns |
| 5 | defect (fixed) | `docs/shared_library/changelog.md:7` | v0.5.0 was tagged with steps 1–3 still under *Unreleased (after v0.4.1)*, so the phase-02 entries sat in the same section as released ones | Fixed: new *Unreleased (after v0.5.0)* heading and a `## v0.5.0 (2026-10-14)` heading above Step 2 |
| 6 | observation | `ead_fields.py:44`, `ead_scope_notes.py:33` | Both modules import private names from `ead_text` (`_SPAN_START/_END`, `_raw_marked_text`, `_LB_MARK`, `_NO_SPACE_BEFORE`). Intra-package and deliberate (one rule, one regex), but a change to `ead_text`'s private walk must re-run both test modules | None |
| 7 | observation | `ead_scope_notes.py:216–225` | Cue-less cross-archive notes (`zie Aanwinsten, 1.11.01.01 (1893 29r)`) yield no reference (1 VOC note, F02-05.6) | None now (extract, do not interpret) |
| 8 | observation | LotA `NL-HtBHIC_SVO_1595_1649` | 0 unit-of-description rows against 28,008 title nodes, so its `daogrp` evidence declaration is never exercised (F02-03b.3) | LotA follow-up |

## Error analysis read by the reviewer

The details are in the report's *Review* section. In summary:

- **03c.** All 23 changed title rows and the 3 quote-rule rows were read as character diffs.
  The step's analysis holds, except that F02-03c.6 called all 10 SG `original` changes glue
  fixes; 3 are the quote regression (revised).
- **05.** 34 changed notes were read, and all 91 were checked to be suffix-only. Every target
  read matches the written code. The suspicious-looking `4582` is in the source.
- **06.** All 40,207 NA titles were checked mechanically (0 span mismatches, 0 nested-only
  dates), and 36 random cuts and 14 left-over separators were read. No wrong cut was found.
  F02-06.5 understated how common orphaned separators are (revised with counts).
- **04.** The counts were reproduced. Grouping SG's handle-only units by series shows they come
  in whole series (F02-R.3), which the step's 25-row sample could not show.
- **Leakage.** Not applicable: the phase has no metric, split or fitted statistic.

The steps' own analyses read real cases and were honest about their limits (04's sample and
05's synthetic-only coverage of F02-P.2). The weakest, 04's 25 rows from one category, is
backed by the series grouping.

## Brief quality

| Step | Brief defect logged | Cause | Change made |
|---|---|---|---|
| 03b | `hierarchy.py` line numbers off by 4–5; BHIC's empty frame not mentioned | line numbers drift; empty inputs not listed | `STEP.md` *Inputs*: a grep-able anchor next to line numbers, and inputs known to be empty |
| 03d | the session prompt asked for a report section, the brief said none | `PROMPTS.md` prompt 2 was unconditional | prompt 2 now says "unless the brief says the step has none" |
| 03c | step 2's scope-note prediction (`derdenSeptember`, `Eems . Met`) did not occur in the notes | prediction carried over from the inventory without checking the notes | none: a prediction that fails is a finding (F02-03c.2), not a brief defect |
| 04 | no coverage-frame input named; the "10 + 11" test count was really 22 | input for the before/after check missing; counts read rather than collected | `STEP.md`: name the input for every before/after comparison; counts from `pytest --collect-only -q` |
| 05 | brief says `post-as-step-2/`, `STATUS.md` says `post-quote-rule/`; the "second kind" of change had no instance | the 03c follow-up changed the baseline after the brief was written | `plan/README.md` rule 10 (propagate mid-phase changes into briefs not yet run); `STEP.md` names the baseline "as currently named in `STATUS.md`" |
| 06 | same baseline mismatch; the quoted `_NO_SPACE_BEFORE` lacked the quotes; the `_split_fallback` test dependency was not mentioned | same as 05; plus a test that patches a function the step replaced | rule 10 covers the first two. The third is a one-off |

## Updates applied

- [x] Phase report `reports/phase-02-ead-for-lota.md`: *Summary*, *Review* and *Follow-ups*
      written; every finding `confirmed` or `revised`; status *final*.
- [x] `reports/index.md`: the phase's 46 findings appended (with Edit), and its phase row
      updated.
- [x] `make reports` runs without warnings; links checked in `reports/_build/`. Pandoc does not
      warn about a missing anchor. The 7 step-06 finding links pointed at
      `#step-06-c4-split_unittitle_dates-lb_adjusted-retired`, but the heading's id has `-in-as-`,
      so all 7 were fixed in the report and the index. All anchors in both built files now
      resolve.
- [x] Review-fixes brief not needed: no code defect. The debts are follow-ups.
- [x] `docs/shared_library/changelog.md` is complete for this phase, after the heading fix and
      the P6 bullet.
- [x] Phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated.
- [x] Phase `STATUS.md` overwritten.
- [x] LotA `STATUS.md`: review entry appended (LotA's convention is to append updates).
- [x] Release recommendation for the user: **v0.6.0** (`make bump-minor`). It contains steps 3b,
      4, the 3c follow-up, 5 and 6: one new module, new public API, and a behaviour change in
      `element_text`.
