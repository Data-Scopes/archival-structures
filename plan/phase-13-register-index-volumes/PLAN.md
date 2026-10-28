# Phase 13: Register and index volumes (text layer v5)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).

## Goal

Hand units on index pages are index entries, and no hand unit interleaves the lines of two text columns. Concretely:
(1) the hand layout splits a PageXML region that holds two side-by-side columns into column pieces in row order (every
hand region, P2); (2) the layout flags **index page sides** from the PageXML alone (two columns + entry shape, with
contiguity fill), tested against the inventory GT and against reads (P1); (3) on flagged sides the hand unit rule cuts
at entry starts (hanging outdent, leading date, the previous line's page reference). The rules ship as a new layer
version `text_layer_v5` for 1.01.02 (D17), with a dev build of every local volume, a run kit and UAs for the full run
and a pilot. Each rule has a pre-registered bar; a failed bar is a negative result and that rule stays out of v5.

## Owns

- `archival_structures/analysis/page_structure.py` (new `split_regions_into_columns`, `LayoutParams` fields
  `hand_column_split`, `index_sides`), `archival_structures/analysis/index_pages.py` (new),
  `archival_structures/text_layer/layout.py` (column split call, `layout/pages/` table),
  `archival_structures/text_layer/units.py` (`UnitParams.index_entry`, index cues), `text_layer/presets.py` (v5),
  `scripts/run_text_layer_v4_full.py` (`--version`), `docs/text_layer.md` (*Text layer v5*), their tests.
- Data: `data/text_layer_v2_dev/19*` (clones and the v5 dev build), `data/text_layer_v5_19e_inputs/`,
  `reports/data/phase-13-register-index-volumes/`.
- No sibling edits. LotA reads the published layers only; a v5 pilot is a UA (19e).

## Sources

- `plan/CONTEXT.md` §1–§4 (v4 pipeline, accepted deviations, data facts, the register/index follow-up).
- `reports/phase-12-hand-print-box-height.md` *Review of steps 15y and 15z* (L1107–1131): F12-R.21, F12-R.22; the review
  script `reports/data/phase-12-hand-print-box-height/review/review_15yz.py` `cmd_interleave_hand` (L338–364).
- `plan/phase-12-hand-print-box-height/REVIEW.md` L355–392 (index volumes, column interleaving).
- `ground_truth/NL-HaNA_1.01.02/inventory_metadata-NL-HaNA_1.01.02.json` (user, 2026-10-28): 674 inventories, `sections`
  with `page_type` `index_page` (492 sections in 485 inventories), `resolution_page`, `respect_page`.
- Planning outputs: `reports/data/phase-13-register-index-volumes/planning/` (`draw_reads.py` and its tables,
  `detector_grid.py`, `colsplit_prototype.py`, `hand_15y-b.txt`, `hand_15z-b.txt`, `interleave_v4_hand.tsv`).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 19a | Gold reads: index sides and entry starts | nothing | 60 side verdicts, entry labels on 30 sides (reserves by rule), blind check page + UA for the user | `step-19a-gold-reads.md` |
| 19b | Hand column split (layout) | nothing | v1–v4 byte-identical; bar B1 ≥ 0.90 right of 40 read splits, ≤ 2 wrong of the 20 non-index; interleave share reported | `step-19b-hand-column-split.md` |
| 19c | Index-side detector | 19a, 19b | features = planning table; grid selection reproduces V05 (P 0.9808, R 0.9297); D1 on GT test: lines P ≥ 0.95, R ≥ 0.90; D2 on reads: accuracy ≥ 0.90, ≤ 2 FP in S3 | `step-19c-index-side-detector.md` |
| 19d | Index entry unit rule | 19a, 19c | variant chosen on dev gold; test gold P ≥ 0.90, R ≥ 0.85, F1 > v4 hand | `step-19d-index-entry-units.md` |
| 19f | Column order v2 (reorder; review follow-up, UA-042) | 19b, 19c | pieces mode = 19b; bar B2 ≥ 0.90 of 40 fresh reads, ≤ 2 wrong of 20 `other` → **failed 35/39** (UA-043) | `step-19f-column-order-v2.md` |
| 19g | Index entry rule v2 (review follow-up, UA-042) | 19a, 19c, 19d | fresh gold (20 sides) before code; X1–X4 chosen on the 30 spent sides; entry bar v2 once → **passed** (X4 = `SG_INDEX_ENTRY_V5B`) | `step-19g-index-entry-v2.md` |
| 19e | v5 preset, dev build, audit, run kit (re-planned 2026-10-28) | 19c, 19g; UA-039 answered; UA-043 answered or its default | v5 (detector + V5B; reorder only if UA-043 (b)) of the 231 local volumes; print = v4; hand units = `19g-*`; audit 0 judged; docs; UAs for full run and pilot | `step-19e-v5-build-and-run-kit.md` |

## Why this order

19a and 19b run in parallel: the gold is read before any rule exists (the planner drew the lists), and the column split
needs no gold. 19c comes after 19b because both edit `layout.py`/`page_structure.py`, and its clone starts from 19b's.
19d needs the gold (19a) and the flags and pieces (19c). 19e assembles whatever passed. A failed bar removes only its
own rule from v5 (P12).

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Index entry rule per page side**, detected from the PageXML (no volume list, no EAD title rule in the build). The inventory GT is used to fit and test the detector, never as a build input | 19c/19d | User, 2026-10-28 (planning question 1). Index sections sit at the front of every *tweede serie* half-year register (3471: scans 0003–0056; GT: pages 7–54 in 3254) and fill the index volumes |
| P2 | **Column split for every hand region** (not only index sides), with a geometric guard; off (`None`) = v1–v4 byte-identical | 19b | User, 2026-10-28 (question 2). Body regions also interleave a margin summary column with the main text (3471 scans 0069, 0450, 0466) and bilingual insets (3192) |
| P3 | **Gold by agent text reads** (19a) before any phase-13 rule exists; a **blind check page** of 30 side labels for the user; the check gates 19e, not the bars | 19a | User, 2026-10-28 (question 3); F12-15z.7: text reads agreed with images on 175/176 scans |
| P4 | **HTR gutter merges** (one line holding both columns): count only; known limit in docs | 19a notes `merged`; 19b/19d count | User, 2026-10-28 (question 4). Planning: lines ≥ 1.6 × the median width are 1,382 of 92,452 on two-column sides of 7 volumes; about half are lemma headings |
| P5 | **Layer version:** `sg_1.01.02_v5` = `sg_1.01.02_v4` + `mode_overrides["hand"]` (layout `hand_column_split`, `index_sides`; unit `index_entry`); name `NL-HaNA_1.01.02_text_layer_v5`; 1.10.94 has no v5 (no hand volumes). New params fields are omitted from `to_dict` when `None` | 19b–19e | D17: published layers are never rebuilt. Omitting the fields keeps v1–v4 `params.json` and the staleness check (`v2_todo`) unchanged |
| P6 | **Column split rule** (fixed): region lines with text; `W` = width of their union, `mh` = median `h`; candidates = lines with `w ≤ 0.65·W`, sorted by `x`; split x = the `x` after the largest gap between consecutive candidate `x` (ties: leftmost); require gap ≥ 0.25·W; left = `x+w ≤ sx + 0.5·mh`, right = (not left) and `x ≥ sx − 0.5·mh`, else span; require ≥ 3 left and ≥ 3 right lines, span ≤ 0.34 of lines, median width of each side ≥ 0.3·W, y-overlap of the two sides ≥ 2·mh. Pieces: walk lines in `_line_sort_key` order; a span line closes the current block (left piece, right piece) and joins a span piece; lines of each piece row-sorted. Pieces carry `column_split_from` (change `split_at_column`) | 19b copies it; no tuning | Prototype at planning (`planning/colsplit_prototype.py`, raw PageXML regions): 3471 165 of 5,857 regions, 3222 2 → 0 with the span guard (fragmented lines) |
| P7 | **Bar B1** (19b): 40 split regions (seed 20261117): 20 on index-like sides (planning features: `col_share ≥ 0.25` and `max(end_num, date_start) ≥ 0.15`), 20 elsewhere; verdicts `right` / `wrong` / `unclear` / `unreadable input`; pass iff right ≥ 0.90 of right+wrong and wrong ≤ 2 among the 20 elsewhere; > 8 excluded → stop. Interleave share (F12-R.22 metric) reported, no bar | 19b | Baseline: 2,612 of 399,714 main_text units (0.65%) in the 125 local hand volumes (`planning/interleave_v4_hand.tsv`) |
| P8 | **Detector features** per layout page side (lines with text): `col_share` (P16 measure on lines with `len(text) ≥ 20`, ≥ 15 such lines, else unmeasured), `end_num`, `date_start` (lines with ≥ 8 stripped chars); rule `col_share ≥ c1 and max(end_num, date_start) ≥ c2`, optional `or (n_text8 ≥ 5 and max ≥ c3)`, then gap fill `g`, drop runs < `m`. Grid V01–V12: c1 0.25, c2 0.15, c3 ∈ {off, 0.3}, g ∈ {0, 4, 8}, m ∈ {1, 3}. Selection on the GT **fit** volumes: among variants with line-weighted P ≥ 0.95 the highest R (ties: lower V); none → highest P. Positive = `index_page` or `respect_page` | 19c | Planning (fit half only): V05 (c3 off, g 8, m 1) P 0.9808, R 0.9297. The planner saw pooled numbers over all 42 GT volumes before drawing the split (F13-P.5) |
| P9 | **Bars D1, D2** (19c): D1 on the GT **test** volumes (once): line-weighted P ≥ 0.95 and R ≥ 0.90. D2 on the 60 read sides of 19a: accuracy ≥ 0.90 over `index`/`not index` verdicts, ≤ 2 false positives in S3 (S3h+S3a); > 10 excluded → stop | 19c | Fixed 2026-10-28 before any test number was computed |
| P10 | **GT page numbers:** verso = 2·scan − 2, recto = 2·scan − 1 (REPUBLIC convention; 3254's index section 7–54 = scans 0004–0027). 19c checks it by reading the sides around 5 section ends before using any GT number | 19c stops if the mapping is off | The planning boundary check was inconclusive (offsets −3..0 within 0.05 agreement on 94–118 sides) |
| P11 | **Entry cues** on flagged sides, per main segment `i ≥ 1` of a region piece: `outdent` (`(x[i-1] − x[i]) / med_h ≥ 0.5`), `date_start` (segment text matches the SG date-start pattern), `end_ref_prev` (previous segment text ends in a number), `short_prev` (as print, frac 0.1); `header` as now. Variants E1 outdent; E2 + date_start; E3 + end_ref_prev; E4 + short_prev; E5 the print rules (`SG_PRINT_UNIT_PARAMS`). Selection: max F1 on the 10 dev sides (ties: lower E) | 19d | F12-R.21: print `short_prev` cuts fit entries better than hand regions; planning reads of 3579/3471: entries start with a hanging outdent of about one line height |
| P12 | **Entry bar** (19d, on the 20 test sides, once): micro P ≥ 0.90, R ≥ 0.85 over gold `S`/`C` lines, and F1 > the v4 hand build's F1 on the same sides. A `date_header` unit directly followed by a `main_text` unit in the same layout column counts as one start (its first line). Fail → negative result; v5 ships without the entry rule (UA) | 19d | Fixed 2026-10-28 |
| P13 | **Stop rules:** a failed bar → negative result, no tuning, report, UA on what v5 ships. B1 fail: 19c–19e wait for the UA. D1/D2 fail: 19d still runs (component test on gold sides), the entry rule cannot ship without a detector. Types are unchanged (lemma headings typed `marginalia`, date-only lines `date_header`: counted, F13-P.6) | all | Phase 10/12 practice |
| P14 | **Dev data:** clones (`cp -cR`) of `15y-b` and `15z-b` for 19b–19d, built only for the 125 hand volumes (`hand_15y-b.txt` 111, `hand_15z-b.txt` 14); 19e builds v5 fresh from the 231 local sources (15i-src 14, staged 182, staged_15z 35 = 37 minus 3463, 9486, which are in staged) | 19b–19e | Clones avoid re-extracting; v5 needs its own layer root |
| P15 | **Bar B2** (19f, review 2026-10-28): reorder mode (start-x classes, wide lines at their row in the left run); 40 fresh regions (seed 20261125, B1's 40 excluded), same thresholds as B1 | 19f | Failed 35/39 (F13-19f.2); the review found the fail fair (F13-R.10). Ships only if UA-043 says (b) |
| P16 | **Entry bar v2** (19g, review 2026-10-28): X1–X4 selected on the 30 spent sides; one test on 20 fresh sides read before any code (reserves by group); P ≥ 0.90, R ≥ 0.85, F1 > v4 hand | 19g | Passed: X4 P 0.9530, R 0.8974 (F13-19g.1); test clean (F13-R.9). Every later entry test needs new fresh gold |
| P17 | **v5 contents** (review of 19f/19g): hand override = `index_sides` `SG_INDEX_SIDES_V5` + `index_entry` `SG_INDEX_ENTRY_V5B`; `hand_column_split` only with UA-043 (b). The entry rule does not depend on the reorder (F13-R.11) | 19e | Ship exactly what passed (P13) |

## Hazards

- **CLI params reach both modes.** In a mode run `--layout-params`/`--unit-params` are merged into the hand **and** print
  params (`build.Run.mode_params`). On the clones only the hand inventories are built (`--inventories-file`), so print
  outputs stay v4; `params.json` then records the override for print too (dev clones only).
- **`to_dict` drift:** a new `LayoutParams`/`UnitParams` field that appears in `to_dict` when unset changes every
  `params.json` and makes `v2_todo` rebuild whole layers. Omit when `None` (as `drop_cap_lines`, 15x).
- **`{}` is falsy:** test `is not None`, never truthiness, for the new dict fields.
- **Hashing:** `LayoutParams` is frozen; a dict field needs `dataclasses.field(default=None, hash=False)`.
- **Gutter-merged lines** become span lines; a merge in the middle of a column block cuts both columns there (P4).
- **Line fragments** (one written line in two boxes side by side, 3222_0031) look like columns; the span guard removed
  the two planning cases; the B1 read checks the rest.
- **Reader bias:** the 19a reader sees the inventory number; strata are not shown by the read tool. Index pages are easy
  to tell from text; the user check measures it.
- **Duplicate tars** 3463, 9486 in two staged dirs (19e): never link a tar twice.
- **The agent shell:** no `rm -rf`, `nohup`, `&`; zsh; new dir per run; `chmod g+w`; coreshare for > 1 worker.

## Open questions for planning

Settled 2026-10-28: scope (P1), split scope (P2), gold (P3), gutter merges (P4) by the user; version (P5), rules and bars
(P6–P12) by the planner.

## Review checklist

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; sibling tests with the
      sibling's interpreter (LotA suite unchanged: 381 passed + 1 xfailed on 2026-10-25).
- [x] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report (`reports/phase-13-register-index-volumes.md`), one section per step.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Every public API change is in `docs/shared_library/changelog.md`, with the affected
      sibling imports.
- [x] No archive-specific knowledge hardcoded in AS core: date patterns and thresholds are parameters with named
      defaults; SG values live in `presets.py`.
- [x] Leakage: the detector is selected on GT fit volumes only and tested once on GT test volumes; the entry variant is
      chosen on dev sides only; no feature reads a label.
- [x] v1–v4 byte-identical (params and tables) with the new fields unset; v5 print inventories equal to v4.
- [x] Gold quality: the user's blind check against 19a's labels.
- [x] Optional silver check (added 2026-10-28, `docs/from-republic-segmentation.md`): page sides that hold
      `republic_segmentation_v1` resolution lines are not index pages, so a flagged one is a detector false positive on
      volumes outside the inventory GT (3349+ registers). Scoring only; needs a Parquet reader (pyarrow is not in `.venv`).
- [x] `Brief defects` entries answered: templates or briefs fixed.

## Hand-off

Filled in at the final review (Opus, 2026-10-28). Release: **v0.18.0** (recommended; the user bumps).
- **Text layer v5** = `sg_1.01.02_v5` (`presets.SG_1_01_02_V5`): v4 + hand override (`index_sides` `SG_INDEX_SIDES_V5`,
  `index_entry` `SG_INDEX_ENTRY_V5B`); no column split or reorder; 1.10.94 has no v5. Print volumes and everything upstream of
  `units` in hand volumes equal v4. Dev build `data/text_layer_v2_dev/19e-a` (231 volumes; protected). Run kit:
  `scripts/run_text_layer_v4_full.py --version v5`; its final audit is the 19e copy (v5 checks). Full run: UA-045; pilot: UA-046.
- **Library (all off by default, `to_dict` omits them when `None`):** `analysis.index_pages`, `LayoutParams.index_sides`
  (→ `layout/pages/`), `LayoutParams.hand_column_split` (`pieces`/`reorder`, both failed their bars), `UnitParams.index_entry`
  (`no_cut_pattern`, `main_types`), unit methods `hand:index_start`, `hand:index:<cues>`.
- **Numbers to start from:** detector D1 P 0.9711 R 0.9490, D2 54/60 (52/57 on the user-judged sides); entry rule fresh
  P 0.9530 R 0.8974; 8,873 of 115,274 hand sides flagged (41% by gap fill); `main_text` on flagged sides 80,344 → 162,021;
  interleave 2,612 → 728 (722 on unflagged sides); false positives 54 known sides, 0.18% of hand units (F13-R.13).
- **Spent:** both entry gold sets (19a 30 sides, 19g 20 sides), the 19a side reads, the B1 and B2 regions. A later entry,
  column or detector test needs fresh reads (`plan/CONTEXT.md` §5).
- **Open (no step):** detector false-positive guard (F13-R.13), column order v3 (F13-R.10), `marginalia`-typed entry starts
  (F13-R.12), continuation cuts after amounts (F13-19g.4), one-column/garbled indexes (F13-19c.5), the index definition for
  later gold (F13-R.15).
