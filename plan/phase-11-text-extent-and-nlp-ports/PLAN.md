# Phase 11: Text extent options and NLP ports (optional)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).

## Goal

AS's `analysis/text_extent` gains NLP's two requested options (UA-014 (a)): `top`/`bot` over chosen region types and
continuous margin-gap columns, both additive, and the NLP report/84 numbers reproduce through them. NLP's unsupervised
segmentation (C10) and separator-sheet rule (C12) live in AS with tests, shown equal to NLP's originals on NLP's own
data. The C18 docs exist: a leakage-and-splits page and the granularity rule, stated as a hypothesis (review decision
9). NLP's shims follow once NLP installs AS editable (UA-032); they are not part of this phase.

## Owns

- `archival_structures/analysis/text_extent.py` (additive), `tests/test_analysis_text_extent.py`;
- `archival_structures/stream_analysis/overview/subsequence_detection.py` (additive), `stream_analysis/__init__.py`
  (`_LAZY_EXPORTS` entries only), `tests/test_subsequence_detection_unsupervised.py` (new);
- `archival_structures/analysis/separator_sheets.py` (new), `tests/test_analysis_separator_sheets.py` (new);
- `docs/challenges/` (new `leakage_and_splits.md`, `index.md`, `document_segmentation.md`), `docs/stream_analysis.md`
  (the *Subsequence detection* section);
- `reports/phase-11-text-extent-and-nlp-ports.md`, `reports/data/phase-11-text-extent-and-nlp-ports/`.

Nothing in NLP or LotA is edited. NLP's interpreter runs read-only checks with `PYTHONPATH=<AS root>`. Phase 12's
remaining step (15y, P21 pins) owns `text_layer/` and `analysis/hand_print.py`; this phase does not touch them.

## Sources

- `plan/PLAN.md` row 11; `plan/CONTEXT.md` §4 (sibling requests, C10/C12/C18 notes).
- `docs/shared_library/upstream_inventory.md` C10 L324–333, C12 L348–356, C18 L423–432, §4 row 18 L462.
- `docs/from-nlp-start-page-detection.md` §1 L7–60 (the verdict and the two requests); UA-014 in
  `docs/USER_ACTIONS_resolved.md`.
- `docs/cross-project-review.md` §0 decisions 9–10 L21–55, §1 item 3 L81–91 (granularity rule).
- NLP (read by an Explore agent on 2026-10-27): `scripts/early_end_extract_body.py` L29–55, `early_end_supplement.py`
  L35–37, 54–56, 75–78, `early_end_analysis.py` L78–88, 155; `src/startpage/unsupervised_segmentation.py` (327 lines);
  `src/startpage/separator_sheets.py` (64 lines); NLP `CLAUDE.md` L33–62 (evaluation protocol), L116–132 (leakage).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 18a | `text_extent` region types and margin gaps | nothing | NLP `page_extents.csv`/`page_extents_body.csv` reproduced on all 20,912 pages; early_end → END 3112 / 0.1398 / 0.3154 (all) and 2762 / 0.1705 / 0.3415 (body); bot-gap bins = `supp_end_rate_by_bot_bin.csv` | `step-18a-text-extent-options.md` |
| 18b | C10: DP and TextTiling segmentation | nothing | new tests incl. brute-force DP optimality; AS = NLP on the 22 inventories (weight 0.0293559853524414, 41,810/41,810 stored change points) | `step-18b-unsupervised-segmentation.md` |
| 18c | C12: separator-sheet rule | nothing | new tests; AS = NLP: 169 sheets on 20,912 page texts, `apply_rule` arrays equal for every convention | `step-18c-separator-sheets.md` |
| 18d | C18 docs: leakage and splits, granularity hypothesis | 18a–18c | the pages exist, cite AS APIs that exist (import check), `make reports` clean | `step-18d-c18-docs.md` |

## Why this order

18a–18c touch disjoint modules and run in parallel (P9). 18d documents what 18a–18c built (C10 is documented as a
section-level tool), so it runs last.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | Inventory step 18 is split into 18a–18d (letter suffixes, README numbering rule) | one brief each | four independent pieces, each reviewable alone |
| P2 | C10 goes into `subsequence_detection.py` (the inventory's destination) but keeps NLP's return convention: **segment start indices, always including 0**. Two converters, `starts_to_boundaries` and `boundaries_to_starts`, link it to the module's gap-index convention | port verbatim, quirks included; docstrings state the convention; no behaviour change | the inventory's claim of "the same convention" is wrong (CONTEXT §4). Keeping NLP's convention makes the later NLP shim a pure re-export with identical outputs; converters avoid a silent off-by-one |
| P3 | No NLP edits in this phase. NLP's AS is a non-editable 0.13.0 copy, so a shim importing new names would break NLP. Equivalence checks run with `cd NLP && PYTHONNOUSERSITE=1 PYTHONPATH=<AS root> venv/bin/python …` (checked 2026-10-27: imports the AS working tree; pagexml-tools 0.9.0 in both venvs) | never `pip install` into NLP's venv; never write under NLP | NLP keeps working; the shims come after UA-032 |
| P4 | Region type of a line = NLP's `_rtype`: the direct parent region's `type`; a list with more than one element gives its last element, a one-element list gives `"text_region"`, a string is returned as is, no parent gives `None` | copy the rule verbatim into `line_region_type` | the report/84 body numbers must reproduce; pagexml-tools gives `['text_region', 'paragraph']` for typed regions and `'text_region'` for untyped ones |
| P5 | Margin gaps are raw: `top_gap = top − stable['top']`, `bot_gap = bot − stable['bot']`, no tolerance subtracted. The class keeps its own comparison (`bot > stable_bot + bot_tol`) | never derive the class from the gap | a consumer sets its own threshold (NLP request 2); float rounding at the boundary could differ |
| P6 | The default `region_types=None` keeps every current output identical | test it on a fixture | additive (UA-014 (a)) |
| P7 | C12 module `analysis/separator_sheets.py`; NLP's regex and the 12-character cap are named VOC defaults (`VOC_SHEET_PATTERN`, `VOC_MAX_SHEET_CHARS`), passed as parameters; the next page id is computed with C13 `ScanId` and keeps the digit count | no hard-coded VOC pattern in the functions | inventory C12: "the regex as a parameter", C13 for scan ids |
| P8 | Phase-08 candidates F08-R.2 (re-shoot recall below J 0.6) and F08-17b.2 (a classifier run under `page_duplication`) stay out | follow-ups | not in row 11; they need a read or an NLP classifier run, not an AS port |
| P9 | 18a–18c run as parallel Sonnet subagents of one session (the user asked for plan, implementation and review in one session on 2026-10-27). Shared files have per-step placeholders: changelog `<!-- 18x -->`, `PROGRESS.md` `<!-- 18x block -->`, the report's empty step sections | a step edits only its own placeholder or section, with Edit; it edits only its own row of `STATUS.md` | concurrent edits must not be lost (README *Parallel phases*) |
| P10 | The granularity rule is documented as a **hypothesis** to test (review §0 decision 9), and Framing B is the default (decision 10) | 18d wording | user decision 2026-10-13 |

## Hazards

- NLP `page_table.csv` has 20,905 labelled pages, `page_extents*.csv` 20,912 (7 unlabelled pages); join on `scanname`.
- The END target is `end_last` (equal to NLP's `label_class == 'END' | (end_last & start)`, 1,379 pages; checked).
- In NLP's body variant the stable margins come from the body extents, over pages with ≥ 1 body line; pages without
  body lines are never flagged.
- `textiling_depth_scores` on an empty input returns `(np.array([]), [0])` by operator precedence; `segment_dp`'s `[]`
  branch is unreachable. Port as is (P2).
- `make docs` fails before this phase (`/opt/homebrew` sphinx lacks `pygments`; `.venv` has no sphinx): UA-033. 18d
  does not depend on it.
- Multi-worker runs go through `coreshare.py run` (18a's PageXML parse).

## Open questions for planning

All settled (P1–P10). UA-031 (a) started this phase.

## Review checklist

- [ ] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [ ] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; NLP suite unchanged (66 collected; no NLP edits).
- [ ] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report, one section per step.
- [ ] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [ ] Every public API change is in `docs/shared_library/changelog.md`, with the affected
      sibling imports (NLP: `early_end_*` scripts, `unsupervised_segmentation`, `separator_sheets`).
- [ ] No archive-specific knowledge hardcoded in AS core: region types, regex and caps are parameters with named defaults.
- [ ] Leakage: no label enters `text_extent`, DP calibration or the separator detection (labels only in the checks).
- [ ] `git diff` shows no change to existing signatures; `region_types=None` output identical (P6).
- [ ] `Brief defects` entries answered: templates or briefs fixed.

## Hand-off

Reviewed 2026-10-27 (accept with follow-ups; [REVIEW](REVIEW.md)). Later phases can assume:
- `text_extent`: `region_types` (default `None`, parent-region type by `line_region_type`), `margin_gaps`,
  `text_extent_table`; report/84 reproduces through them;
- `subsequence_detection`: `segment_dp`, `textiling_depth_scores`, `calibrate_length_penalty_weight`,
  `predicted_start_rate`, `MAX_SEGMENT_LENGTH` (NLP-verbatim, start indices incl. 0, TextTiling edge starts F11-R.2),
  `starts_to_boundaries`, `boundaries_to_starts`; lazily exported from `archival_structures.stream_analysis`;
- `analysis/separator_sheets.py`: VOC defaults named; sorted sheet order (F11-R.3);
- docs: `docs/challenges/leakage_and_splits.md`, the granularity hypothesis in `document_segmentation.md`.
NLP still imports its own copies; the shim step follows UA-032. Release: v0.16.0 recommended (not yet tagged).
