# Phase 10 progress

Append-only. One block per session (planning, step, review), newest last, ~40 lines each.
Implementation sessions append here but don't read it unless their brief names a block.

### Planning (Opus 5.5, 2026-10-23)
- **Seeded** from UA-015's response ("Full-run scope later, with (c) in phase 10 planning") and three user
  answers: phase 10 = hand/print only, the optional items go to phase 11 (P1); "any mixture at any level … as
  few assumptions as possible" (P2); add a known-gap note to the published pilots now (done: v3 hand manifest
  `known_gaps`, shared-data catalogue rows for v2 and v3 hand, AS `docs/text_layer.md` path tables, LotA
  `docs/from-archival-structures.md` §9 *Correction*).
- **Explore 1 (hand/print in AS):** preset field differences and where each is read (in `PLAN.md` *Sources* and the
  15r/15s briefs); no PageXML signal for print in 1.01.02 (F10-P.2); 3771–3823 printed by four signals (F10-P.1);
  EAD "gedrukt" groups; 181 text-layer tests, the preset-pinning ones named in the PLAN hazards.
- **Explore 2 (phase-11 items):** C10 conventions differ (AS gap index vs NLP start index incl. 0; the inventory's
  C10 row is wrong on this, and "below majority" holds only for Pk/WindowDiff); C12 64 lines, no tests, nothing
  similar in AS; `text_extent` has no region-type filter or margin-gap column (21 tests); no leakage/granularity
  page in `docs/challenges/` (`check_nested_split` exists); NLP runs a **non-editable AS 0.13.0** install. Recorded
  in `plan/CONTEXT.md` §4 for phase 11.
- **Measured here:** TSV density per region on the 14 dev inventories (F10-P.3); EAD silver groups with TSV coverage
  (F10-P.4); REPUBLIC `print_paragraphs_v1` ids vs 3771 layer ids (F10-P.5); `legacy_region_text` is the TSV join.
- **Design changes during planning:** v4 split filter = print tag **and** print mode (a mode-only filter would change
  455); `n_chars` uses `legacy_region_text` to match the TSVs; hand regions ignore `UnitParams`, so v4 passes the
  print unit params everywhere.
- **Written:** `PLAN.md` (P1–P9), briefs 15p–15t, `STATUS.md`, report skeleton with F10-P.1–P.5;
  `plan/PLAN.md` rows 10–11; `plan/CONTEXT.md` §1 and §4.

### Step 15p: hand/print census (Sonnet, 2026-10-23)
- **Done:** `census.py` (self-test ok), full census of 9,122 TSVs (0 failures; 20.6 M regions, 2.39 M scans), thresholds
  lo = 0.4219, hi = 0.4411 (swapped: hand p95 0.4411 > print p05 0.4219; dev percentiles equal the planning numbers),
  `analyze.py`, `samples.py`, five samples read (238 regions), `staging.tsv`, UA-017.
- **Stop rule fired (negative result):** silver `print` -> `print` 172/239 = 72.0%, silver `hand` -> `hand` 464/548 =
  84.7% (dev excluded); both < 90%. Rule not changed. Failures: dense neat hand labelled `print`/`mixed` (19 + 65
  net-resoluties volumes; all sampled text handwritten) and printed plakkaten labelled `mixed` (67 of 76; sampled text
  printed). Dev volumes: 7 hand, 3350 `mixed` (0.17), 6 print.
- **Corpus:** chars 82.4% hand / 15.7% print regions; volumes hand 8,358, mixed 525, print 223, uncertain 85.
  Real odd print: English newspaper pages in 7461-7463, 8611.
- **Report:** `reports/phase-10-hand-print-per-region.md` section Step 15p, F10-15p.1-8 (draft); `make reports` clean.
- **Next:** 15q must be re-planned by Opus before any session runs it (density alone failed the fixed acceptance).

### Review of 15p (Opus 5.5, 2026-10-23)
- **Verdict:** sound; the stop rule fired correctly and nothing was tuned. Re-computed the silver crosstab from
  `inventories.tsv` (dev excluded): hand 464/65/19 (hand/mixed/print), print 172/67/0, as reported. No leakage:
  thresholds come from dev percentiles only; silver is used for evaluation only and excludes dev.
- **Per series (review):** gedrukte resoluties 99/99 and nouvelles 64/64 `print`; plakkaten 9/76 (median print
  char share 0.84, q10–q90 0.75–0.90); eerste serie ordinaris 94/123 and tweede serie net-resoluties 366/421 `hand`.
- **Error analysis checked:** verdict files tally as reported (s1 29 hand + 1 junk; s4 59 hand, 29 print); read
  sampled snippets. Text-only verdicts are a limit (all text is Loghi HTR); S→B and d→b confusions in the
  plakkaten snippets fit print. Accepted.
- **Brief defects** (9,191 keys, suffixed-inventory fallback, two `mixed` scan definitions, sample sizes) recorded
  by the step; none changes the result.
- **Decision for the user:** UA-018 (re-plan 15q on line geometry / EAD volume modes / close the phase). 15q–15t
  not run.

### Re-plan of 15q (Opus 5.5, 2026-10-23)
- **Inputs:** UA-018 = (a); UA-017 = done. The 12 tars were unreadable (`-rw--w----`, the UA's `chmod g+w` was wrong).
  The user fixed it, and all 12 now open. UA-017/018 moved to `USER_ACTIONS_resolved.md`. UA-007 stays open
  (fixed on `bug_fixes`, waiting for release 0.9.1).
- **Measured before writing a brief** (dev layer `15m-a` only, no silver): nine line-geometry features per region.
  Only `hrel` (box h / baseline pitch) separates (AUC 0.913; others 0.50–0.71; LR 0.885). Page cut 1.53 from dev:
  hand volumes 0.00–0.19 `print` pages, print 0.91–0.98, so 3 of 8 hand volumes are `mixed` under P3's 0.9/0.1 rule
  in sample. 455/456 at 1.52/1.57. Report *Planning*, F10-P.6.
- **Drawn:** random silver sample of 60 volumes stratified by series (`planning-15q/silver_sample.tsv`), for UA-020.
- **Not written:** a new 15q brief. Whether the volume rule stays 0.9/0.1 (UA-019 (a)), becomes one mode per volume
  by page majority (b), or the phase takes EAD modes (c) or closes (d) is the user's call; each gives a different brief.
- **Next:** after UA-019, an Opus session writes 15q (and 15r–15s if (b) changes P2/P7) from STATUS *Facts*.

### Re-plan after UA-019 (b) (Opus 5.5, 2026-10-23)
- **Inputs:** UA-019 = (b), one mode per inventory from its PageXML (given in chat); UA-020 = copied. The 60 tars are
  `-rw--w----` (unreadable for the agent) → UA-021. UA-019/020 moved to `USER_ACTIONS_resolved.md`.
- **Decisions changed** (phase `PLAN.md`): P2 (inventory is the unit; region/scan labels diagnostic), P3 (the `hrel`
  rule and its threshold procedure, fixed before any silver geometry), P5 (`mode/` tables, no new v3 columns), P7 (all
  rules per inventory), new P10 (stop rule: stratum-weighted agreement ≥ 0.90 per silver class on the 60-volume sample)
  and P11 (1.10.94 falls back to fixed `print` if 455/456 fail the frozen rule).
- **Code facts checked:** the six rule fields that differ between the v3 presets (by `asdict`); the five `build.py`
  read sites; `fit_split_model`'s pre-pass (L1396–1410) as the shared pre-pass; `extract_scan_v2` never splits;
  `15m-a` sources and argv; the v3 run script already launches through coreshare.
- **Briefs:** new `step-15q-volume-mode-classifier.md`, `step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`
  (old 15q–15t marked VOID). Phase now 15p + 3 steps; `layout.py`, `types.py`, `units.py`, `join_v2.py` no longer touched.
- **Report:** re-plan paragraph extended, title "per volume", step sections 15q–15s; `plan/PLAN.md` row 10,
  `reports/index.md` row, `plan/CONTEXT.md` L120 (15t → 15s).
- **Next:** 15q (Sonnet).

### Step 15q, part A frozen (Sonnet, 2026-10-23)
- Dev pre-pass (14 inventories, 0 parse failures) and `fit` done; acceptance 2 (3349 lines: 92,152 rows identical) and 3
  (t_lo/t_hi 1.529/1.534, 9,048 labelled scans, all 14 dev volumes in their class) checked before any other set.
- **Frozen:** `reports/data/phase-10-hand-print-per-region/step-15q/thresholds.json`, sha256 `72cc1c042c010cd925c5897184d3e5f8b3d82736d6785d723e382209c22049f3`
  (t_print 1.5344738, t_hand 1.5287525 unrounded; t_lo 1.529, t_hi 1.534). No threshold, feature or rule changes after this point.
- Parts B, C, D run after this block.

### Step 15q (Sonnet, 2026-10-23): STOP RULE FIRED
- **Built:** `archival_structures/analysis/hand_print.py` (+ `volume_mode_from_features`), `tests/test_analysis_hand_print.py` (7 tests;
  suite 1,213 -> 1,220 collected, 1,209 passed, 11 skipped), changelog entry under *Unreleased*, `step-15q/validate.py` and two
  error-analysis helpers. Report section: [Step 15q](../../reports/phase-10-hand-print-per-region.md#step-15q-volume-mode-classifier), F10-15q.1-7.
- **Part A:** acceptance 2 (92,152/92,152 rows) and 3 hold (1.529/1.534, 9,048 scans, 14/14 volumes); 3354 share 0.1627 vs 0.157
  (re-plan used unrounded t_lo; brief defect). `thresholds.json` sha256 `72cc1c042c010cd925c5897184d3e5f8b3d82736d6785d723e382209c22049f3` (frozen
  before B/C).
- **Part B:** 455 `hand` (share 0.187), 456 `hand` (0.097): P11 applies, 1.10.94 takes `{"method": "fixed", "mode": "print"}`.
- **Part C:** weighted agreement hand 0.768, print 0.893 (< 0.90): fails. Tweede serie 14/20, nouvelles 6/10; others 10/10.
- **Part D / protection:** table in the report; protect before/after identical.
- **Next:** phase review (prompt 3). 15r and 15s are not to run.

### Review of 15q (Opus 5.5, 2026-10-23)
- **Verdict:** sound; the stop rule fired correctly and nothing was tuned. 15r and 15s not run (P10).
- **Freeze order checked:** `thresholds.json` (read-only, 13:52, sha256 `72cc1c04…49f3` = PROGRESS) predates
  `transfer_*` (13:53) and `sample_*`/`staged_*` (14:00). Thresholds come from the 14 dev volumes only (no 455/456,
  no silver): no leakage.
- **Re-computed:** stop rule from `sample_volumes.tsv` + `silver_sample.tsv`: hand 0.7678, print 0.8929, per-stratum
  10/10, 14/20, 10/10, 6/10, 10/10, as reported. Independent re-implementation of P3 (own aggregation over
  `extract_scan_v2` rows, not `hand_print.py`) on 3303, 12109, 3126 from the tars: print/hand/uncertain counts and
  shares identical (0.5130, 0.4163, 0.2143). The only difference: my `unlabelled` counts are higher (scans with no
  featured region at all are absent from `volume_mode`'s `scans`), diagnostic only. Region ids are scan-prefixed
  (grouping by `region_id` is safe). Full suite: 1,209 passed, 11 skipped.
- **Robustness of the verdict:** the hand side alone fails (0.768; tweede serie Wilson upper 0.85 < 0.90), so the
  open *nouvelles* label question (F10-15q.6) cannot change the outcome. Relabelling silver after seeing results
  would be post hoc anyway; the report's line "a reason for a visual check before the phase review judges the stop
  rule" should be reworded at phase review. Plakkaten pass 10/10, but every share is 0.55–0.68 (fragile pass).
- **Direction of the failure:** the feature is inverted across material: printed 455/456 at shares 0.19/0.10
  (`hand`), handwritten 3590 at 0.97 (`print`). `hrel` measures leading, not script (F10-15q.2, .4).
- **For the phase review:** F10-15q.4 is tagged `voc`, check the tag; F10-P.6's 1.52/1.57 for 455/456 vs 1.72/1.80
  here (different aggregate, brief defect) needs a correction note on F10-P.6. `volume_mode_from_features` is in the
  changelog entry.
- UA-021 resolved (moved). Next: phase review (prompt 3, new Opus session).

### Phase review (Opus 5.5, 2026-10-23)
- **Verdict:** accept as a negative result (`REVIEW.md`). No review-fixes step; one doc fix in review
  (`hand_print.py` docstring: *Status: negative result*; it said the mode "is used downstream").
- **Re-run:** 15q acceptance 1–6 (table in `REVIEW.md`); AS suite 1,209 passed / 11 skipped; LotA 368 passed, 1 xfailed.
- **Own error analysis:** box height and pitch per volume show `hrel` mixes line spacing (3590: pitch 137 px) with the
  model's box convention (455/456: boxes 76–82 px at the dev print pitch) (F10-R.1); column interleave rejected as a
  cause (F10-R.2, 40 scans × 5 volumes); exploratory box height separates all 76 labelled volumes, 65–86 against
  89.5–173 px (F10-R.3, post hoc); *nouvelles* label probably right (F10-R.4); the sample is spent (F10-R.5).
  Scripts and table under `reports/data/phase-10-hand-print-per-region/review/`.
- **Findings:** P.6, 15q.2, .4, .6 revised; the rest confirmed; F10-R.1–5 added; all appended to `reports/index.md`.
- **Updated:** report (final), `reports/index.md`, phase `PLAN.md` (checklist, hand-off), `plan/PLAN.md` row 10,
  `plan/CONTEXT.md` (§1, §3, §4, §5; still over budget, flagged), `STATUS.md`; UA-022 in `docs/USER_ACTIONS.md`.
- **Release:** optional v0.15.0 (the unused `hand_print` module); nothing in the siblings needs it.
- **Next:** UA-022.
