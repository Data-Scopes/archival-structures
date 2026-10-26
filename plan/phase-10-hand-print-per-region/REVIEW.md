# Phase 10 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-23. The same session ran 15q as a Sonnet subagent (prompt 2) and checked it
  (`PROGRESS.md` *Review of 15q*) before this review.
- **Verdict:** **accept as a negative result.** Both pre-registered tests fired (15p density, 15q `hrel`), nothing
  was tuned after a freeze, and the numbers reproduce. 15r and 15s were not run (P10). No code defects. The
  `hand_print.py` docstring overstated the rule; fixed in review (one edit). No review-fixes step. How the hand/print
  work continues is UA-022.

## Checklist

- [x] **Acceptance re-run** (table below). 15p was re-checked by its own review (`PROGRESS.md`); 15q by me.
- [x] **Suites:** AS `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,209 passed, 11 skipped (1,220
  collected; 1,213 before 15q). LotA `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider` → 368
  passed, 1 xfailed. No sibling code changed; LotA got a docs note at planning (§9 *Correction*).
- [x] **Error analysis** read individual cases in both steps (15p: 238 regions, five seeded samples; 15q: 6 + 4 + 2
  failed or boundary volumes, 455/456, 30 dev scans, 30 Part-D scans; verdicts in `verdicts.tsv`). The 15q
  *explanation* of the failure does not hold up (F10-R.1); the cases read do.
- [x] **Report** final; findings in `reports/index.md`; `make reports` clean.
- [x] **Changelog:** `## Unreleased` → `### Step 15q` lists the new module and every public name
  (`VolumeModeParams`, `region_geometry`, `region_features`, `scan_value`, `scan_label`, `fit_thresholds`,
  `volume_mode`, `volume_mode_from_features`, three constants), "affected sibling imports: none". No `LayerPreset`
  field or `mode/` table was added (15r did not run), so the PLAN item on those is void.
- [x] **No archive knowledge in `hand_print.py`:** thresholds are required fields of `VolumeModeParams`; no SG
  inventory, path or preset in the module (`grep -n "1\.01\|SG\|3771" archival_structures/analysis/hand_print.py`
  → only the docstring's finding IDs). It imports neither pandas nor `text_layer`.
- [x] **Leakage:** `thresholds.json` (read-only, 13:52:16, sha256 `72cc1c04…49f3`, as recorded in `PROGRESS.md`
  before part B) lists only the 14 dev inventories; `transfer_*` were written at 13:53, `sample_*`/`staged_*` at
  14:00. Silver was used once, for the stop rule. 15p: thresholds from dev percentiles only (its review).
- [x] **v1–v3 presets and dev layers reproduce:** no `text_layer/` file changed in this phase (`git diff --stat`
  shows none); 15q's protect hashes before/after are identical (99,445 lines). `15r-a` does not exist (void item).
- [x] **Brief defects** answered (below).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 15q | 1. tests | 7 new; 1,209 passed, 11 skipped | same (after the docstring edit too) | yes |
| 15q | 2. 3349 pre-pass rows = `15m-a` lines on 5 columns | 92,152 / 92,152 | 92,152 / 92,152 | yes |
| 15q | 3. thresholds, labelled scans, dev classes | 1.529 / 1.534; 9,048; 14/14 | from `thresholds.json` and `dev_eval_volumes.tsv`: same; 3354 0.1627 vs 0.157 (brief defect) | yes |
| 15q | 3. implementation | — | own re-implementation of P3 (`review/indep_hrel.py`) from the tars on 3303, 12109, 3126: print/hand/uncertain counts and shares identical (0.5130, 0.4163, 0.2143) | yes |
| 15q | 4. P11 | 455 `hand` 0.187, 456 `hand` 0.097 | `transfer_volumes.tsv`: same | yes |
| 15q | 5. stop rule | hand 0.768, print 0.893; fail | from `sample_volumes.tsv` × `silver_sample.tsv`: 0.7678, 0.8929; 0 parse failures, 60/60 `geometry` | yes |
| 15q | 6. Part D, protection | table; identical | `staged_volumes.tsv` matches the report table; `protect_*.txt` identical | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | observation | report, Step 15q *Conclusion* (1)–(2); F10-15q.2, .4 | The step's explanation of the failure is inverted: 3590's low `hrel` comes from wide line spacing (pitch 137 px against 80–94 in the dev hand volumes; box height a normal 157), not from "tight" handwriting; 455/456's high `hrel` comes from taller boxes (76–82 px against 67–69 for 3771/3822) at the **same** pitch (47–48), not from "wide leading" | F10-R.1; F10-15q.2 and .4 revised |
| 2 | observation | F10-P.6 | Two-column interleaving is not the cause of the 455/456 shift (column-wise `hrel` equals region-wise within 0.03; 1–7% two-column regions in 40 scans of each of 455, 456, 3771, 3349, 3590). F10-P.6's 1.52 / 1.57 for 455/456 does not reproduce under the 15q definition (1.63 / 1.86 region median, 1.72 / 1.80 scan median) | F10-R.2; F10-P.6 revised |
| 3 | observation | `review/h_by_volume.tsv` | Exploratory, post hoc: absolute line-box height (char-weighted median per volume) separates all 76 labelled volumes (print 65–86 px, hand 89.5–173), incl. 455/456 and the four failing *nouvelles* | F10-R.3; a lead for UA-022 (a), not a result |
| 4 | observation | F10-15q.6 | The *nouvelles* `print` label is probably right: density (15p, 64/64 `print`) and box height (83–86 px) agree with it; the `hrel` failure is the feature's | F10-R.4; F10-15q.6 revised |
| 5 | debt | `archival_structures/analysis/hand_print.py:184` | `volume_mode`'s `scans` (hence `n_scans`) contains only scans that have line rows; a scan without text lines is absent, so `n_scans` is lower than the scan count of the inventory (3126: 363 against 369). Diagnostic only; `validate.py` takes the scan count from the pre-pass | document if the module is ever used |
| 6 | debt | `tests/test_analysis_hand_print.py:55` | `assert lo == 1.235 or hi == 1.235` is weak (it does not pin which threshold rounds) | leave; the module is unused |
| 7 | observation (fixed) | `hand_print.py` docstring | Said the volume mode "is used downstream" and that printed lines "are set tighter"; neither holds | fixed in review: a *Status: negative result* paragraph |
| 8 | method | — | The 60-volume silver sample is spent for any geometry rule: 15q and this review have seen its geometry. A new test needs a new random sample that excludes these 60, the 12 UA-017 volumes and dev | F10-R.5; in UA-022 (a) |

## Error analysis read by the reviewer

- **Implementation:** an independent re-implementation of the P3 rule on three sample volumes (one failing hand
  volume, one failing *nouvelles*, one passing eerste serie) gives identical counts; the failures are not bugs.
- **Mechanism:** raw `h_med` and `pitch_med` (regions ≥ 100 chars) for 3349, 11182, 3771, 3822, 3590, 3572, 12109,
  12197, 455, 456 (F10-R.1). `hrel` mixes two things that vary independently of script: a scribe's line spacing
  and the segmentation model's box convention.
- **Column interleave:** 40 random scans (seed 16) each of 455, 456, 3771, 3349, 3590: rejected as a cause (F10-R.2).
- **Box height across all 88 evaluated volumes** (F10-R.3). Margin is thin (print max 86.0 = 12135, hand min 89.5 =
  3492), and size depends on scan resolution (F10-15p.7). Among the staged volumes, 11930 (`print` 1.0 by `hrel`)
  is 138.5 px (hand-sized; 15p called it `hand`), 3463 86.5 (print-sized; 15p `print`), 4617 97 (in between).
- **The step's text reads** (verbatim snippets in the report) are consistent with these numbers; only the stated
  cause was wrong. The step's suggestion to settle the *nouvelles* label "before the phase review judges the stop
  rule" is not needed: the hand side fails alone (tweede serie Wilson upper bound 0.85 < 0.90), and relabelling
  silver after seeing results would be post hoc.

## Brief quality

- **Reference shares computed at unrounded thresholds** (15q, 3354 +0.0057): a brief that freezes rounded thresholds
  must compute its reference numbers with the same rounded values. Added to `plan/CONTEXT.md` §5.
- **`fit` and `evaluate --set dev` both wrote `dev_*`:** brief naming collision; the step renamed. Recurring kind
  (output names per subcommand); §5 note "give every subcommand its own output prefix".
- **No source for scan order:** the step used sorted file names, which is what `iter_pagexml` yields. Fine.
- **`volume_mode` takes line rows, the script caches region features:** `volume_mode_from_features` added; additive
  and in the changelog. Fine.
- **F10-P.6's 455/456 numbers:** a planning measurement with a different aggregate was quoted as if it were the
  rule's (F10-R.2). §5: "quote a planning number only with the exact definition the step will use".
- **Explanations of a geometry failure** were written from the ratio alone; §5: "when a ratio feature fails, report
  its numerator and denominator before naming a cause".

## Updates applied

- [x] Phase report `reports/phase-10-hand-print-per-region.md`: *Summary*, *Review* and *Follow-ups* written; every
      finding `confirmed` or `revised`; status *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed (the one doc fix was made in review)
- [x] `docs/shared_library/changelog.md` complete for this phase
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated; `plan/CONTEXT.md` updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: none needed (no sibling code; LotA's §9 *Correction* note stands)
- [x] Release recommendation: in `STATUS.md` and the chat (optional v0.15.0: the unused `hand_print` module)
