# Phase 12 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-25. 15u and 15v had their own reviews (`PROGRESS.md`); this review covers the phase
  and step 15w, which had none. Scripts: `reports/data/phase-12-hand-print-box-height/review/` (`review_15w.py`,
  `column_share.py`, `dropcap_design.py`, `draw_reads.py` and their outputs).
- **Verdict:** **accept with follow-ups; one defect goes to step 15wr.** The box-height rule, the mode decision, the
  per-inventory build and the audit work as specified and every acceptance number reproduces. But the v4 print rules
  include the merged-line splitter, and in the single-column print series of 1.01.02 (*nouvelles*, *plakkaten*,
  *Processtukken*) it cuts about 1.06 million ordinary lines in two and sends the halves to different layout columns
  (F12-R.4). 15w's reads missed it, the audit cannot see it, and `docs/text_layer.md` describes it as column
  interleaving. 15wr adds a split gate measured from the PageXML; 15x (UA-026 (b)) runs after it.
- **Report status:** *final*, with post-review addenda for 15wr and 15x still to come, as in phase 07 (15ir) and
  phase 09 (15n/15o). The steps add their own sections, and their reviews set the statuses of the new findings.

## Checklist

- [x] **15u acceptance** (fit values, `cut.json` sha256 and timestamp, stop rule): re-run by the 15u review
  (`review/review_15u.py`, 0 mismatches over 80 volumes). Today: `shasum -a 256 step-15u/cut.json` → `ce58c333…ccb572`,
  unchanged. 15w's pre-pass reproduces the 168 reference values exactly (below), which re-checks the definition.
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,235 passed, 11 skipped (1,246 collected),
  as logged. **LotA:** `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider` → 381 passed, 1 xfailed.
  No sibling code changed.
- [x] **Error analysis read individual volumes and scans** in every step (15u 95 + 18 scans, 15v 30 crosswalk rows,
  45 splits, merges and units and 10 pages, 15w 147 + 160 scans and 20 units; verdict files). I re-read cases
  myself (below). 15w's split reads are missing (F12-R.4), and its conclusions on 3463 and on the nouvelles are revised.
- [x] **Phase report** final (with addenda pending), findings in `reports/index.md`, `make reports` clean.
- [x] **Changelog:** `## Unreleased` lists 15q, 15u, 15v, 15w. Checked against the code: every new public name in
  `hand_print.py` (`BoxHeightParams`, `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`,
  `BOX_HEIGHT_METHODS`), `presets.py` (`mode_presets`, `volume_mode`, `MODE_RULE_FIELDS`, `BOX_HEIGHT_V4`,
  `inventory_preset`, two presets), `line_split.prepass_rows`, the four `modes.py` functions and the v4 `params.json`
  form are listed. Not listed (minor, observation 6): the `modes.py` constants `SIDE_MIN_CHARS`, `MODE_SCAN_COLUMNS`,
  `MODE_REGION_COLUMNS`, and `build.Run.mode_run/preset_for/mode_params` (internal). Sibling imports: none, correct
  (LotA imports no `text_layer` v4 name; it reads no `params.json` or `mode/`).
- [x] **No archive knowledge in `hand_print.py`** (`grep -n "1\.01\|SG\|3771\|87.75"` → nothing; the cut is a required
  field) or in `modes.py` (the cut comes from `preset.volume_mode`). The cut lives in `presets.BOX_HEIGHT_V4`.
- [x] **Leakage:** no test or diagnostic volume in the fit (15u review); `cut.json` mtime before the test pre-pass;
  15w uses only the frozen cut. My split gate for 15wr (F12-R.4) is measured on the 87 print-mode volumes it will
  be applied to. It describes column layout and is not fitted to a label. The gap is wide (single-column ≤ 0.225,
  two-column ≥ 0.496), and 15wr's bar reads scans drawn now, before the gate exists.
- [x] **v1–v3 presets reproduce:** `test_v1_v3_presets_unchanged` (field-for-field); `build.py` non-mode branches keep
  their old expressions (diff read). `15v-a` hand and 455/456 equal `15m-a` (15v review). `15w-a` = `15v-a` on the 16 shared
  volumes: re-run `review_15w.py` → 1.01.02 258 equal / 33 differ / 6 missing, 1.10.94 55 / 2 / 0, the logged numbers.
  **168/168 values** equal, by my own join.
- [x] **Crosswalk bar (P12)** applied as fixed, reading settled in the 15v review (unusable either way). **15w read
  lists (P14)** applied: 51 band volumes (39 + 456 + 11 reserves) and the two out-of-series `print` volumes.
  **Audit** re-run: `15w-a` 3,412 pass / 75 fail, all 75 reported shares, 0 judged; `15m-a` 178 / 4, the same four;
  `--self-test` ok.
- [x] **Brief defects** answered (below).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 15w | 1. tests | 1,235 passed, 11 skipped (1,246) | same | yes |
| 15w | 2. reference values | 168/168 | 168/168 equal, 0 unequal, 0 missing (`review_15w.py`, own join) | yes |
| 15w | 2. modes | 111 hand, 85 print, 2 fixed | same; silver print 83 → 83 `print`, silver hand 78 → 77 + 1 (3463), none 35 → 34 + 1 (9486) | yes |
| 15w | 2. `15w-a` = `15v-a` (16) | 258 / 33 / 6; 55 / 2 / 0 | same (`review/compare16_review.json`) | yes |
| 15w | 3. audit `15w-a` | 3,412 pass, 75 fail (reported) | same, 0 judged | yes |
| 15w | 3. audit `15m-a` | 178 / 4 (3351, 3823, 455, 456) | same four, same shares | yes |
| 15w | 5. protected data | identical except shared-data (other projects) | `protect2_*` identical; `protect_*` differs only in `shared-data/` (`README.md`, `republic_ner_2026-04-11/`, `.DS_Store`) | yes |
| 15u | `cut.json` | sha256 `ce58c333…` | same | yes |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 (F12-R.4) | defect | `text_layer/line_split.py:1189` `derive_split_params`; `presets.py` `_V4_MODE_PRESETS` | The print rules carry the merged-line splitter, which assumes two columns per page side. In the single-column print series it splits ordinary lines: *nouvelles* 885,967 splits (median 70.9 per scan, about 43% of lines), *plakkaten* 169,527 (median 15.9 per scan), 9486 4,745 (47% of lines); *gedrukte resoluties* 617 in 31 volumes. 46 of 46 sampled splits in 8 volumes cut one continuous sentence ("Le Collonel Rendall, qui ‖ avoit éte Gouverneur des Barbades &"). The halves go to separate layout columns, so units hold half-lines (12088_0428). 15w's "interleaved columns" (12088 units 0064, 0630) are this. A volume-level feature from the PageXML separates the layouts: the share of lines that start past 0.4 of their side's text span is 0.496–0.501 in the 31 *gedrukte resoluties*, 0.005–0.225 in the 53 others | step 15wr (split gate) |
| 2 (F12-R.5) | observation | F12-15w.5 | The four handwritten volumes near the cut are all **index** volumes (EAD: 3463 "1740 index", 3478 "1745 index", 3492 "1748 index 1e halfjaar", 4617 "Index"). All 14 local index volumes lie at 86.5–137.0 px; no non-index hand volume is below 100.0. The 1.01.02 EAD has 251 index-like titles (127 in the *tweede serie*, 115 in the *secrete* registers). A normal-tail estimate from the 11 index volumes that were not selected for being odd (mean 116.7, sd 16.2) puts about 9 of the 251 on the `print` side in a full run (exploratory). 3463 and 4617 were selected for UA-017 because they were contradictory, so 15w's "one in four" overstates the rate | F12-15w.5 revised; UA-028 |
| 3 (F12-R.6) | observation | `text_layer/units.py:137` (print cues) | Drop-cap cuts dominate the print cuts of the *gedrukte resoluties*. In 5 volumes (3771, 3822, 3769, 3805, 3864), cuts on the `indent` cue alone, 1 or 2 main lines after the previous cut or the region start, are 23,991 of 53,464 cuts (44.9%; d=1 16,710, d=2 7,281, d=3 367). Only 53 of the 4,687 cuts at *WAER op* / *Dat* / *IS na* lines are of that kind (1.1%) | the 15x rule design |
| 4 (F12-R.7) | method | `step-15w/audit.py` | The structural audit (3,289 non-share invariants) passes on a layer in which about a million lines are cut wrongly: ids, offsets and value sets say nothing about whether the text reads. A build that moves rules to new series needs reads of each stage's output per series | `plan/CONTEXT.md` §5 |
| 5 | debt | `scripts/run_text_layer_v4_full.py` cost constants | The print rate (4.17e-7 worker-s per byte, 3 × hand) was measured with the wrong splits. The bigram splitter on ~1M lines is most of it | re-measured in 15wr |
| 6 | observation | `docs/shared_library/changelog.md` 15v | `modes.py` constants and `build.Run` helpers are not listed. They are internal or self-describing, and no sibling uses them | none |
| 7 | observation (accepted deviation) | `step-15w/audit.py:238` | The lower-case merge share stays in the invariant list with `reported: true` and is left out of `n_fail_judged`. The brief asked both for "reported, never pass/fail" and for `15m-a` to reproduce 178/4, which needs the pass/fail value. The flag satisfies both | accepted; `CONTEXT.md` §2 |
| 8 | observation | `docs/text_layer.md` v4 *Known issue* | The paragraph puts the 12088 problem down to column interleaving | 15wr rewrites it |

## Error analysis read by the reviewer

- **Splits in the print series (F12-R.4).** `split_stats` of all 85 print-mode 1.01.02 volumes (counts above). I read
  random split pairs (seeds 41, 43): 12088 ×8, 12166 ×8, 12109, 12122, 12207, 12225, 12184, 9486 ×5 each. In 46 of 46
  the two halves form one continuous sentence. Examples: 12122 "Met eene voor drie ‖ daagen alhier ingeloopen
  Estafette", 12184 "Vijf en twintigh mergen elf ‖ roeden Landts", 9486 "de onvoorziene ‖ overgave van Breda". On
  page 12088_0428 every left half is 764 px wide (`default_line_width` of the model): "wesen vereyscht, men op
  gisteren den" opens one unit, and its continuation "Heer Bronswyck-Zellischen" opens the unit of the "second
  column". `split_model/12088.json` takes 764 px as the column width of an 1,800 px text span. 15w's two "bad unit
  boundaries" in 12088 are these half-lines, so F12-15w.7's "no bad split" is wrong.
- **Column layout** (`review/column_share.py` on the pre-split lines rebuilt from `15w-a`): pooled share of lines
  starting past 0.4 of the span. *Gedrukte resoluties* 0.496–0.501 (31), 455 0.461, 3463 0.415, *plakkaten*
  0.026–0.225 (27), *nouvelles* 0.005–0.064 (25), 9486 0.007, 456 0.164. 456 is two-column but most of its lines are
  merged, so the gate is for 1.01.02 only. Table: `review/column_share_pooled.tsv`.
- **Near-cut handwritten volumes (F12-R.5).** EAD titles from `data/metadata/eads/NL-HaNA_1.01.02.xml`. All four are
  index volumes, and neighbouring non-index volumes of the same years lie at 146–151 px (3459, 3471, 3487, 3493). 3463's
  print build: 25.5 units per scan, median main unit 2 lines, 106 bigram splits. Its text is degraded but readable,
  as for any hand volume cut too finely.
- **Drop-cap pattern (F12-R.6).** Page 3771_0535 in full (cue values per line): the drop-cap paragraph starts flush or
  slightly indented, and its next one or two lines sit 1.1–1.7 line heights to the right of the column edge, beside
  the initial. Line 3 then returns flush. The other cut types on that page (*IS na …* on a new line, attendance lists
  by `short_prev`) are right.
- **15w's reads otherwise hold.** The band verdicts (50/51) agree with the EAD (the printed ones are nouvelles/plakkaten
  years). The minority-scan verdicts match 15u's pattern. The lower-case merge reads are plausible (capitalised nouns).

## Brief quality

- **15w (1): UA IDs** "next free UA-024, UA-025" while UA-026/027 existed. Fine as reserved IDs. The 15x brief
  names UA-028+ and says to check both UA files.
- **15w (2): band "39 + 456"** without the reserves. The brief's own rule gives 51, and the step applied it. Fine.
- **15w (3): UA-026 answered mid-step.** The step documented it, as rule 10 asks.
- **15w (4): `compare.py` compares whole trees.** The wrapper `compare16.py` is fine. The 15wr/15x briefs give a
  restricted comparer command (per-inventory list).
- **15w (5): "reported" vs 178/4.** Contradictory brief. The flag is accepted (finding 7).
- **New, from this review:** 15w's error analysis read units of the print series but no split lines, and its brief
  did not ask for them. Recurring kind (phase 06: hand candidate rates "near 0"): when rules move to a new series,
  the brief must name a read of every stage's output that can fail there (splits, layout, units). Added to
  `plan/CONTEXT.md` §5, with F12-R.7.

## Updates applied

- [x] Phase report: *Summary*, *Review*, *Follow-ups* written; every finding `confirmed`, `revised` or `withdrawn`;
      status *final* (post-review addenda 15wr and 15x to come)
- [x] `reports/index.md`: the phase's findings appended (with Edit), phase row updated
- [x] `make reports` runs without warnings
- [x] Review-fixes brief written: `step-15wr-review-fixes.md` (split gate, F12-R.4)
- [x] Step 15x brief written: `step-15x-drop-cap-units.md` (UA-026 (b); depends on 15wr)
- [x] `docs/shared_library/changelog.md` complete for 15q–15w (observation 6)
- [x] phase `PLAN.md` *Hand-off*, steps, P16–P19; `plan/PLAN.md` row; `plan/CONTEXT.md` updated
- [x] phase `STATUS.md` overwritten
- [x] UA-028 (index volumes in the full run); review notes added to UA-024 and UA-025 (wait for 15wr/15x)
- [x] Sibling notes: none needed (LotA uses no v4)
- [x] Release recommendation: hold v0.15.0 until 15wr and 15x are reviewed (in `STATUS.md` and the hand-off)

## Step 15wr review (2026-10-26)

- **Session:** Opus 5.5, a new session (15wr ran as Sonnet 5.5, 2026-10-25/26).
- **Verdict:** **accept.** The split gate is implemented as P16 and the brief say. It fixes F12-R.4 in `15wr-a` and
  changes nothing outside the 53 gated volumes. The read bar (P17) holds on a full re-read. No fixes brief is needed.

| Check | Re-run by the reviewer | Step's claim |
|---|---|---|
| AS suite | 1,237 passed, 11 skipped (before and after the review's test addition) | same |
| LotA suite (`PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q`) | 381 passed, 1 xfailed | not run (no change) |
| gate decisions (`mode/<inv>.json`) | 111 hand (no gate), 32 split (min share 0.4152, 3463), 53 gated (max 0.2258, 12225), 0 `share: null`; 1.10.94 no gate | same |
| shares vs review `pooled_mid_start` | max gap 0.0076 (12162: 0.0234 vs 0.031) | report: 0.0002; F12-15wr.1: 0.001 (both wrong, `gate.tsv` itself shows 0.0076) |
| own decompressed-byte compare vs `15w-a` | 1.01.02: hand 2,109 equal; split 640 equal + 32 differ (all `mode/<inv>.json`, only by `split_gate`); gated 106 equal, 901 differ, 106 only in `15w-a` (`split_model`, `split_stats`); corpus 10 differ. 1.10.94: 44 equal, `*/stats.json` differ by `seconds` only | same |
| corpus `wbc` | per-inventory rows equal outside the 53; `default_set`, `pinned` unchanged; pooled shares change | same |
| `split_from` in the 53 gated volumes | 0 of 1,947,176 lines | same |
| audit `step-15w/audit.py --layer 15wr-a` | 3,593 pass, 32 fail (reported), 0 judged; v4 21 invariants ok; `--self-test` ok | same |
| protected hashes | `protect2` before = after = now; `protect` differs only in LotA `gold/06-terms-1.01.02.json` (another project) | same |

**Read in the code** (`modes.py`, `presets.py`, `tests/test_text_layer_modes.py`, `step-15w/audit.py`, run script):
- `column_start_share` is the brief's definition: rows need `min_chars` of `text_raw` and a box; per `page_side`, 5th
  percentile of `x` and 95th of `x + w` (`numpy.percentile`, linear); a side counts with `min_rows` rows; `(None,
  n_kept)` when none does. Rows without `page_side` would form their own group, but every line of the 198 volumes has
  `left` or `right` (checked on 8 volumes, both series).
- The gate is computed only when the resolved preset splits and the preset carries `split_gate`, before `write_mode`.
  So hand volumes and 1.10.94 write the same mode file as before, and `share is None` keeps the splitter, so a volume
  too small to measure cannot be gated. No test reached that branch: the fixture is a `hand` volume and `_fixed` drops
  the gate. **Fixed in review:** `test_extract_split_gate` now has an unmeasurable case (`min_chars` 10⁶, `min_share`
  1.1), which must split and record `{"share": null, "n_rows": 0, "min_share": 1.1, "split": true}`. The modes file
  passes 9 of 9, and the test count is unchanged.
- `stage_units` still picks print units from the resolved preset's `merged_line_split`, so gated volumes keep the print
  layout, types and unit rules. 15x relies on this.
- The audit's new invariants match the brief: a missing `split_gate` counts as split, and `split` is checked against
  `share is None or share >= min_share`.
- **Observations, no action:** `mode/<inv>.json` records `min_share` but not `min_chars`, `min_rows`, `mid_frac` (the
  brief's schema; the manifest records the preset name, and v4 is unpublished, P20). The run script's 1.10.94 output
  constant stays 0.0888 while its comment gives the new 0.0873 (1.10.94 output is unchanged on content, so this is
  noise). UA-024's 15w evidence still names interleaved *nouvelles* columns, which were the split defect; a review note
  now says so.

**Error analysis read by the reviewer:**
- *Bar units in full* (the step read the first 14–16 lines): all 20 units of `step-15wr/reads_out.txt`, 9–54 lines each.
  All 20 read in order: hyphenated words join across lines, sentences run on, and no unit has a half line or a
  second column. In 9486, two 54-line testimony units read through. 12226 (the step's `unclear`) is a plakkaat in
  garbled OCR, but its word breaks join ("han Ben- / 16 Detember 1757", "syest- / sien", "Raet- / nen"), so I judge it
  in order. The pre-registered count stays 19/20; the bar holds either way. F12-15wr.2 is confirmed.
- *12187* (the random line that was never split): "van niemandt eenige ignorantie stonme te", whole in both builds. The
  step sampled all lines of the gated volumes, not only split ones. I drew 5 lines from split lines only (seed 20261030):
  12182, 12118, 12184, 12133, 12166. In all five the `15w-a` halves cut one phrase ("Gouverneur gearresteert, dos ‖ op
  het aancoonen"), and in `15wr-a` the line is whole.
- *3463* (share 0.4152, split): 106 splits on 57 scans. In a sample of 12 (seed 1), 8 cut one phrase ("Rug: Pissars,
  om Ocbroy tot ‖ het opregten", "Hooftman ‖ en Ontfanger", "last ‖ en valgeld"), none is clearly right, and 4 are
  unclear. The index pages have two columns, so the gate correctly leaves the splitter on. The error is the mode
  (F12-R.5), and the P21 pin to `hand` removes the splits (F12-R.9).
- *The 32 split volumes:* no per-inventory file changed except `mode/<inv>.json`, so the splitter output is unchanged.

**Brief defects the step listed:** (1) fixture lines < 20 chars: the test's `min_chars: 1` keeps the test's intent;
no conclusion changes. (2) `split_from` rows are two per split: the report now gives both counts, and F12-R.4's
885,967 is a count of splits, so it stands. (3) `seconds` in `stats.json`: `compare.py` already drops it; the 15x brief now
says so. (4) Partial reads: re-read in full above, no change; the 15x brief now asks for full-unit read files. The
defect kind is "read only the head of a printed read file". It is close to F12-R.7 and is not added to `CONTEXT.md` §5
as a new kind.

**New findings:** F12-R.8 (re-run reproduces), F12-R.9 (3463 splits wrong; mode error, P21), F12-R.10 (half lines had
removed `-` from the derived WBC sets of 31 of the 53 gated volumes; the join uses the pinned set). Statuses:
F12-15wr.2, .4 confirmed; F12-15wr.1 (share gap), .3 (row total 2,120,486) revised.

**15x brief** checked against `15wr-a` and corrected in place (Rule 10): presets anchors after the inserted
`SPLIT_GATE_V4` (L184/185/188/204); the `gate.tsv` layout and `gated.txt`; units per scan in `15wr-a`; 3463 as a count
only; the build command copied in, instead of "as 15wr's step 2"; the protect commands written out with `15wr-a`;
the test baseline 1,248 / 1,237 / 11; the `seconds` note and equal `mode/` files in acceptance 2; the `15wr-a` audit
numbers; full-unit read files; "split-gated" in the diagnostic UA made precise (`split_gate.split` true). Bars A/B/C,
`drop_cap_lines`, seeds and `reads_15x.tsv` (20 scans, file of 2026-10-25 21:08, before 15wr ran) unchanged.
**15x may start now.**

## Step 15x review (2026-10-26)

- **Session:** Opus 5.5, a new session (15x ran as Sonnet 5.5, 2026-10-26). Script:
  `reports/data/phase-12-hand-print-box-height/review/review_15x.py` (`compare`, `bars`, `extra`, `lost`; written
  independently of `step-15x/bars.py`), outputs `review/suppressed_15x_review.tsv`, `reads_15x_extra_20261031.txt`,
  `reads_15x_lost_20261032.txt`, verdicts `verdicts_15x_review.tsv`.
- **Verdict:** **accept.** The rule is implemented as P18 and the brief say; the three pre-registered bars reproduce
  from my own code, and my reads confirm them. No code defect, no fixes brief. Two one-line fixes in review (changelog,
  *Known issues*). UA-029's recommendation changes after 60 more reads (below); the decision stays the user's.

| Check | Re-run by the reviewer | Step's claim |
|---|---|---|
| AS suite | 1,251 collected, 1,240 passed, 11 skipped; units + modes files 27 passed | same |
| LotA suite (`PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q`) | 381 passed, 1 xfailed | not run (no change) |
| own decompressed-byte compare vs `15wr-a` (JSON without `seconds`/`elapsed*`) | 1.01.02 hand 2,109 equal; print 1,084 equal, 595 differ = 85 × 7 (`units/{units,segments,doc_id_map}`, `wbc/parts`, `join/{segments,stats,units}`); `mode/` 594 of 594 equal (198 × 3); 1.10.94 57 of 57 equal; corpus: `units/params.json` (print gains `"drop_cap_lines": 2`), `units/stats.json`, `wbc/corpus_default*.json` (only `pooled` statistics), `wbc/wbc_stats.tsv`; `default_set`, `pinned`, `wbc_per_inventory*.tsv` equal | same |
| v1–v3 presets | `test_v1_v3_presets_unchanged` passes; 1.10.94 `units/params.json` equal (no `drop_cap_lines`) | same |
| suppressed cuts (`starts(15wr-a) − starts(15x-a)`, 85 volumes) | 179,412 (resoluties 126,228, nouvelles 34,385, plakkaten 18,572, 3463 165, 9486 62); no new start anywhere; every suppressed start lies inside a `main_text` unit | same |
| bar A sample | 210 on the 20 scans; `Random(20261029).sample(sorted, 60)` = `sampleA.tsv` exactly; verdicts 59/1 | same |
| bar B | 37,897 / 38,176 = 0.9927 on segment text and on whole-line text alike; my lost list = `barB_lost.tsv` | same |
| bar C | `pages.tsv` 19 better, 1 same | same |
| diagnostic draws | `Random(20261028).sample(sorted, 10)` per series = the 20 `diag-*` rows of `verdicts.tsv` | same |
| audit `step-15w/audit.py --layer 15x-a` | 3,592 pass, 33 fail (reported), 0 judged | same |
| protected `protect2` | re-hashed now = `protect2_after.txt` | same |

**Read in the code** (`git diff` of `units.py`, `presets.py`, both test files):
- `split_region`: `last` starts at 0 (the region's first main line) and moves only on a kept cut; a cut is dropped only
  when `cues == ["indent"]`, `drop_cap_lines` is set and `i - last <= drop_cap_lines`. `short_prev`, `gap`, `header` and
  any combination always cut. With `None` the loop is the old one. Hand mode does not reach the branch.
- `UnitParams.to_dict` deletes the key only when `None`; `from_dict` accepts it, so `params.json` round-trips.
  `dataclasses.asdict(UnitParams)` still has the key (only `to_dict` omits it).
- `inventory_preset` copies the six `MODE_RULE_FIELDS`, validates the override keys of the requested mode
  (`ValueError`), replaces whole fields (hence `{**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}`), and returns
  `mode_presets=None, volume_mode=None, mode_overrides=None`. A preset without `mode_presets` returns early and ignores
  `mode_overrides`; no preset does that (observation, no action).
- `build.Run.mode_params` merges a CLI `--unit-params` over both modes, so `drop_cap_lines` can also be set from the CLI.
- Tests cover the brief's cases (`[1, 1, 4]` → `[6]`, `[3, 3]`, `indent+short_prev`, a drop after a `short_prev` cut,
  `to_dict`/`from_dict`, the overrides and the bad key).

**Error analysis read by the reviewer:**
- *Bar A, 21 cuts and 5 pages in full* (3769 0416, 3795 0360, 3816 0355, 3816 0460, 3849 0257; all units of both
  builds): 20 right, 1 wrong (3849 0257 "En hier op gedelibereert zynde", the step's `wrong`). All 5 pages `better`.
  On 3795 0360 the merged "Geteeckent, / C. Voet van Winsen." is one signature, so I count it right, not a defect.
- *All 210 suppressed cuts on the 20 scans* (not only the 60): I screened every cut whose line starts upper-case or
  follows a line ending in `.`/`:`/`;` (95) and read them. Wrong: 3849 0257 (above), 3816 0355 "WAAR op gedelibereert"
  after the junk line "4080", 3828 0195 "Signee," after "Fait à la Haye le 23 Mars 1773."; unclear: 3774 0281 "En
  prenant connoissance de cette cause". So 206 of 210 right (0.981), the same as the sample (F12-R.12).
- *Bar B lost starts* (30 of 279, seed 20261032): 10 are the decision of a short pass entry ("OP de Requeste van …, /
  Koopman te Amsterdam. / IS na voorgaande deliberatie goedgevonden …"): request and decision become one unit, which
  is still one entry; 8 are a decision glued to the 1–2-line tail of the previous entry at a page or column top ("loopende
  maand, houdende advertentie. / WAAR op geen resolutie is gevallen."); 7 are *Dat …* sub-clauses of a request glued to the
  previous 1–2-line clause (one of them, 3821 0408 "eindelyk / Dat de Carielins", is a right merge); 5 follow a junk or
  number line ("4080", "010", "13", "60"), so the clause stays apart from the previous entry and the merge is harmless.
  F12-15x.3 is revised with these categories.
- *nouvelles/plakkaten, 30 more suppressed cuts each* (seed 20261031, fixed before drawing; the 10 diagnostic cuts
  excluded): *plakkaten* 21 right, 1 wrong (12170 "W1H." article number glued to the page number "44"), 8 unreadable
  input (table junk, garbled HTR, interleaved margin columns); *nouvelles* 23 right, 4 wrong (12090 "Il y a
  continuellement …" after a broken line, 12109 "Waar mede, &c." closing formula, 12114 "Getee- / kent" signature after
  the dateline, 12140 "Art. 1."), 3 unreadable. With the diagnostic: readable precision *plakkaten* 27/28 (0.96),
  *nouvelles* 30/36 (0.83). The pre-registered diagnostic result (6/10, 7/10) stands; the extra reads change its reading
  (F12-R.13).
- *12118's merge share (0.9541 → 0.9496)*: 377 new word-break merges, 0 lost; 317 lower-case, 60 not. Of the 60, about
  33 are letter parts that the rule pulled into the previous unit and the join then glued across a break mark ("Getee|
  Waar meede, &c.", "ingekoomen|Waar meede", "Dat. 25 October", "Rec. 31", "Hoog Mogende Heeren,"), 12 are capitalised
  nouns after a real word break ("GuldenVlies", "KoninglykeHoogheid"), 15 junk. So the drop is caused by the rule's wrong
  merges of letter parts in the *nouvelles* (F12-R.14), not by noise.

**Brief defects the step listed:** (1) "units per page" undefined: the step used all unit types per scan, the 15wr
basis; no conclusion depends on it. (2) The 7 explained files per volume match acceptance 2. (3) The audit baseline 32
→ 33: traced above (F12-R.14). (4) The "45% fewer cuts" expectation was on another basis (cuts in five volumes); the
3771 figure (46.05 → 35.21, design 45.5 → about 35) confirms it. New defect kind: **`unclear` counted as `wrong` mixes
input quality with rule error**; with junk input it fired the plakkaten trigger (F12-R.15, `CONTEXT.md` §5).

**New findings:** F12-R.11 (re-run reproduces), R.12 (all 210 bar-A-scan cuts: 0.981), R.13 (single-column series:
the rule helps; *nouvelles* letter parts are its error), R.14 (12118 cause), R.15 (method: an `unreadable` verdict).
Statuses: F12-15x.1, .2 confirmed; .3, .4 revised.

**Fixed in review:** changelog entry gains the `asdict` note; `docs/text_layer.md` *Known issues* gives the review's
precision per series. Review notes in UA-029 (sharpened options, recommendation (a)), UA-024, UA-025.

## Steps 15y and 15z review (2026-10-27)

- **Session:** Opus 5.5, a new session (15y and 15z ran as Sonnet 5.5; the drive-rebuild check and UA-037/038 ran as Opus,
  2026-10-27). Scripts, written apart from the steps' own: `reports/data/phase-12-hand-print-box-height/review/review_15yz.py`
  (`throughput`, `compare15y`, `repro15z`, `summary`, `interleave`, `interleave_hand`) and `stale_cases_15yz.py` (pytest, four
  staleness cases); outputs `throughput_15yz_review.json`, `interleave_15yz_review.tsv`, `interleave_hand_15x-a_review.tsv`.
- **Verdict:** **accept with follow-ups; the fixes go to step 15yr** (`step-15yr-review-fixes.md`, Sonnet). The pins, the
  pin rule, the in-place rebuild and the drive rebuild are right, and every acceptance number reproduces. One defect in the new
  staleness logic: a failed upstream row counts as "newer", so a failed forced extract leads to a rebuild from stale lines
  (F12-R.19). The throughput model's extract gap is not worker inefficiency: the extract rows leave out the mode pre-pass, about
  half of each call (F12-R.17), and its in-sample serial errors hold by construction (F12-R.18). The audit has two keying
  defects: modes and counts by inventory (F12-15z.9), and done-ness by any done row (F12-R.20). The stager never deletes
  (F12-15z.10). The two whole-layer scans are one function: `input_bytes` decompresses every tar, once in a no-op `--estimate`
  and once at the start of the build (F12-15z.11 revised). Error analysis: on index volumes the `hand` pin fixes the label but
  not the text. The hand rules give one unit per PageXML region and keep two-column regions in XML order (F12-R.21, F12-R.22).

| Check | Re-run by the reviewer | Step's claim |
|---|---|---|
| AS suite | 1,322 collected: 1,311 passed, 11 skipped | same |
| LotA suite (`PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider`) | 381 passed, 1 xfailed | not run |
| 15y acc. 2: own decompressed compare `15x-a` vs `15y-b` (JSON without `seconds`, `elapsed*`, `source`) | 1.01.02: 3,772 equal, 27 differ (17 of 3463 + 10 corpus: `layout|types|units/stats.json`, 3 `corpus_default*.json`, 3 `wbc_per_inventory*.tsv`, `wbc_stats.tsv`), only in `15x-a` `split_model/3463.json`, `split_stats/3463.json`; 1.10.94 57/57 equal | same |
| 15y acc. 3: `15y-a` vs `15y-b`, files `3463.*` | 19/19 equal, same file set; `mode/3463.json` `hand`/`pinned`, `box_height_mode` `print`, 86.5, no `split_gate` | same |
| 15y acc. 4: resume no-op, on an APFS clone of `15y-b` | `0 to do` in all six stages; `size mtime` list of 3,590 files unchanged; `status.tsv` 1,183 rows before and after; 30.5 s, of which about 29 s the sizing of 196 tars | same (259 s with the audit) |
| 15y acc. 5: `archive_summary` of the staged full run (after the drive rebuild) | (9,122, 0) and (2, 0) | same |
| 15z acc. 1: `15z-a` `mode/` vs the staged full run | 37/37 `value`, gate share and measured mode equal | same |
| 15z acc. 2: the pin rule re-applied to `verdicts.tsv` | 185 rows (103 / 73 / 7 unclear / 2 table); 14 pin `hand`, 2 pin `print`, 1 mixed, 20 as built; `MODE_PINS_V4` = rule (16 incl. 3463) | same |
| 15z acc. 3: own compare `15z-a` vs `15z-b` | nothing outside the pinned volumes but the same 10 corpus files; 22 split files only in `15z-a` | same |
| drive rebuild: staged 16 vs `15z-b` | every common file equal (`source` aside); in the staged copy only: 24 `split_model`/`split_stats` files of 12 hand pins | same (F12-15z.8, .10) |
| 15z resolution (F12-15z.6) | per-volume median `page_w` 0.90–1.07 and `page_h` 0.94–1.06 of the all-volume median (5,180 x 4,070) for the 35; *plakkaten* 3,920 x 2,550 and 4,070 x 2,660 | group medians 0.98–1.04 (revised wording) |
| user check (F12-15z.7) | `user_check.tsv`: 102 + 73 agree, 1 `printed` → handwritten, 7 doubtful → 5 + 2 handwritten, 2 tables → handwritten | same |

**Read in the code** (`git diff` of `modes.py`, `presets.py`, `build.py`, `units.py`, the run script, `step-15w/audit.py`, tests):
- `decide_mode`: the pin is applied after the method branch, so a `fixed` preset also records the measured result; the gate then
  runs on the pinned mode (`extract_inventory_v2_modes` L134–145); a bad pin raises. Stale split files are unlinked before the
  split extract writes new ones, so a print → print rebuild with `record_splits=False` also loses an old `.splits.json` (intended).
- `is_current`: compares `_seq` with the latest row of the upstream stage **whatever its status** (L289–294). A `failed` upstream
  row therefore makes a stage redo, from the old inputs (F12-R.19). `stage_layout` checks only that the input files exist.
- `v2_todo` compares `params.json` before writing it; the units document includes `republic_dir`, which depends on the
  inventories of the call. A subset `--stage units` with `--republic-dir` could flip it and make the next full run redo all
  units. No v4 run does that (1.01.02 has no crosswalk; 1.10.94 has two volumes with one). Observation, no action.
- Run script: `--rebuild` checks are complete; a rerun after a crash repeats the forced extract of F (minutes), which the
  docstring does not say. The docstring and `docs/text_layer.md` say the other volumes' sources "are not read". They are: the
  second command's `build.main` lists every source and sizes it with `io.input_bytes`, which decompresses each `.tar.gz`
  (`io.py:152`), and the wbc stage exits if a layer inventory is missing from the sources.
- `--estimate` runs `build.main --estimate`, which for both v4 presets prints "has no throughput fit; no prediction"
  after the same full decompression (`run_v4.log`, 3 times): 9 minutes for nothing per archive and run.
- `extract_v2.extract_inventory_v2` times itself (L184, L243); `extract_inventory_v2_modes` calls it after `prepass_rows`, so the
  status `seconds` of a v4 extract row leave out the pre-pass, the mode decision and the split-model fit (F12-R.17).
- Audit: `done_inventories` keeps a stage as done if any row was `done` (the same bug as debt 2, F12-R.20); `modes`/`counts`
  keyed by inventory (F12-15z.9). The self-test swaps `PRESETS["sg_1.01.02_v4"]` without `try/finally`.
- Changelog *Unreleased* matches the API: `decide_mode(..., inv=None)`, `volume_mode["pins"]`, `MODE_PINS_V4`, `V2_UPSTREAM`,
  `is_current`, `v2_todo`, `_seq`, stale-file removal, script signatures. Two sentences went stale after UA-038 ("the published
  pilot keeps 3463 `print`", "the published v4 pilot is not rebuilt"); fixed in review, as is the same sentence in
  `docs/text_layer.md` and its "not read" claim.

**Staleness cases 15y did not cover** (`stale_cases_15yz.py`, the SG fixture, the split gate opened):
- *B, the reverse pin* (`hand` build, then pinned `print` with the splitter): forced extract + `--stage all` equals a fresh
  print build in all 21 files; `split_model` written. Correct.
- *C, a crash after layout in a rebuild* (forced extract, `--stage layout`, then `--stage all`): types, units, wbc and join
  are redone, layout is not; equal to a fresh build. Correct.
- *D, two volumes, one rebuilt*: only 9999 gets new rows; all 21 files of 9998 keep their mtimes. Correct.
- *A, the forced extract fails after the pre-pass* (simulated `OSError` in `extract_inventory_v2`): `mode/9999.json` already says
  `hand`/`pinned`, `lines/` are still the print build's. `--stage all` (extract still failing) then logs `0 already done, 1 to
  do` for layout, types, units, wbc and join, rebuilds them with the **hand** rules on the print build's split lines and records
  them `done`; `units/units/9999` and `join/` change; the last rows read extract `failed`, everything else `done`. Exit code 1 and
  the run summary flag it, and the run script stops after a failed first command. But a plain build CLI resume, and the audit
  (F12-R.20), treat the hybrid as built (F12-R.19). After the cause is fixed, a plain `--stage all` restores a build equal to a
  fresh one.

**Error analysis read by the reviewer:**
- *Throughput, in and out of sample* (`review_15yz.py throughput`; the 16 rebuilt rows left out). OLS with an intercept
  reproduces the summed seconds exactly (in-sample sum error 0.0000% for layout, types, units, join), and a serial stage's span is
  its summed seconds, so the step's 0.1–0.4% are not a test. Honest tests: a random split half (seed 20261105) predicts the other
  half's sums within 0.15% (layout, types, units, wbc, join) and +4.8% (extract); a fit without the 196 dev volumes predicts their
  full-run seconds at −15.0% (extract), −5.7% (layout), −5.4% (units), +0.9% (types, wbc), −0.9% (join): the 196 are 52% print
  by bytes against 12% in the run, so the pooled fit runs low on print-heavy subsets. Per volume the median absolute error is
  6.5–15% (extract 40%). `15x-a` is not out of sample in volumes (its 196 are in the fit); it tests another machine and disk.
  Per volume, `15x-a`'s seconds are 0.93–1.13 of the full run's.
- *The extract gap.* Timing `extract_inventory_v2_modes` locally on 9448 (print), 3436 (hand pin) and 12476 (print): whole call
  8.4 / 3.7 / 3.0 s, row `seconds` 4.47 / 1.90 / 1.54, pre-pass alone 4.1 / 1.8 / 1.4 s, so 47–49% of each call is not
  recorded. The full run's extract (10.99 recorded worker-hours in 2.84 h, no gap over 1.2 min between rows) could hold at most
  7 × 2.84 = 19.9 worker-hours, so the unrecorded part was at most 0.81 x the recorded one there (locally 0.88–0.95 x). The
  7 workers were busy; F12-15y.3's "worker efficiency 0.55–0.65" is the missing pre-pass (F12-R.17). wbc's gap is not its
  decision tail (39 s from the last part to the decision line); with 0.17 s tasks the pool's per-task overhead is the likely cause
  (not measured).
- *Whole-layer time* (`run_v4.log`): sizing scans of 8.8–9.4 min, six times over the three runs (`--estimate`, then the start of
  `--stage all`); the audit 59.7 min (full run) and 59.0 min (rebuild) for 9,124 inventories; the wbc decision 39–47 s.
- *Index volumes, print build next to the hand pin* (`review_15yz.py units`; one scan per volume, seed `20261105-<inv>`, all
  main_text units of both builds in full): 3463_0091 print better (print cuts "12 december. miss: Directeuren …" after the short
  line "Holl: ov:", hand merges it into the 13-line unit), 3439_0142 print better (48 vs 36 units; print's `short_prev` cuts at
  "26. Nov. Miss:", "19 feb:", "24 Maart.", "7 nov.", "17 noy" are entry starts; its 3 wrong cuts are fragments, "vinfra 25 nov:",
  "n aem ij n1 300", "127 Junyl"), 3469_0042 mixed (print wrongly cuts "over het arrest …" off its entry but cuts "5 mij", "11 meij"
  right; hand merges three dated entries), 3475_0155 print better (31 vs 19; the hand build's 27-line unit interleaves two columns:
  "8. Meij. voorschrijven … Metta Maria / 25 Januarij. Miss: greftien, over het gestrende schip, de / galeij, aan greflein, om"),
  3489_0056 same. 3 print better, 1 mixed, 1 same, 0 hand better. Volume-wide, 97–99% of the hand main_text starts are
  `hand:region` (one unit per PageXML region); the print rules add `short_prev` and `indent` cuts. So the under-segmentation of
  F12-15y.5 and F12-15z.3 is the hand unit rule, and the print *unit* rules fit index entries better than the hand ones.
- *Column interleaving* (`interleave`): a main_text unit with at least 3 jumps of > 800 px between consecutive lines that are
  both ≥ 500 px wide. 14 hand-pinned volumes: print builds 29 of 40,496 units (0.07%), hand builds 360 of 25,984 (1.39%); 5 of 6 read
  (lax test, seed 20261106) are two text columns of one PageXML region in XML order. 3442: "27 oc: Miss: Mag: van dantzig … /
  22 Sept: Vier dienders voor t wegheeren / ambassadeur van Rusland. 17 nov: Rapp:". The 6th is a two-column name table (12297).
  The print layout sorts the lines of unchanged regions and splits at the column; the hand layout keeps XML order
  (`docs/text_layer.md`, known limit). The same test on the 111 hand volumes of `15x-a`: 2,252 of 373,730 units (0.60%), 30
  volumes ≥ 1%, led by *tweede serie* registers and indexes (4617, 3471, 3493, 3459, 3579, 3546); 6 of 6 read (seed 20261108)
  interleave two columns ("28. Augr. Miss. Stadht. Kien; plaatsen van / van eenen Os van Aaile naar Bakel 20. Sept:", 3579). A lax
  test (2 jumps, any width) flags 3.9%, but most of those are a right-aligned "Waerop" or a margin start in one column (8 read),
  so 0.60% is the defensible lower bound (F12-R.22). This affects every published hand layer (v2–v4), not only the pins.

**Statuses.** Confirmed: F12-15y.1, .2; F12-15z.1, .2, .3, .5, .7, .8, .9, .10. Revised: F12-15y.3 (the gap is the unrecorded
pre-pass; in-sample serial errors are by construction), F12-15y.4 (per-mode extract rates leave out the pre-pass), F12-15y.5 (the cause
is the region-based hand unit rule; the hand layout also interleaves columns), F12-15z.4 (by F12-15z.7), F12-15z.6 (per-volume ratios
0.90–1.07), F12-15z.11 (one function, two calls; the audit hour is not modelled either). None withdrawn. New: F12-R.16–R.22.

**Brief defects** (15y: five, 15z: seven, all listed in the report) change no conclusion. Two are the planner's: the 15y brief
promised that the other volumes' sources are not read, and the 15z brief listed `„`/`=` as handwritten cues, which hold only in
narrow columns (F12-15z.7). New defect kinds for `CONTEXT.md` §5: **validate a fitted model on held-out units, not on the
sums it was fitted to**; **check what a timing column measures before modelling it** (`seconds` began after the pre-pass).

**Fixed in review:** the two stale changelog sentences; `docs/text_layer.md` (*Pinned volumes*: the pilot has 3463 pinned; *Rebuilding
pinned volumes*: every source must be present and is sized). Everything else goes to 15yr.

**Updates applied:** report (statuses, F12-R.16–R.22, *Review of steps 15y and 15z*, follow-ups, summary addendum), `reports/index.md`,
phase `PLAN.md` (15yr row, P30, hand-off), `plan/PLAN.md`, `plan/CONTEXT.md` §1, §4, §5, `STATUS.md`, `PROGRESS.md`.

## Step 15yr review (2026-10-28)

- **Session:** Opus 5.5, the session that wrote the brief (15yr ran as a Sonnet 5.5 subagent of it, 2026-10-28).
- **Verdict:** **accept**, with one fix made in review (`done_inventories`). No new brief.

| Check | Re-run by the reviewer | Step's claim |
|---|---|---|
| AS suite | 1,334 collected: 1,323 passed, 11 skipped | same |
| audit `--self-test` | ok, incl. `two`, `fail` and my new `partial` case | ok (`two`, `fail`) |
| audit of `15y-b` (after my fix) | 3,592 / 33 / 0 judged; 198 keys `<archive>/<inv>`; `modes` identical to `step-15yr/audit_15y-b.json` | same |
| no-op `--stage all` on a fresh APFS clone of `15yr-a` | 1.16 s; `[sizes] 188 from status.tsv, 8 measured`; `0 to do` everywhere; 1,189 rows before and after | 1.35 s |
| 3463 extract rows in `15yr-a` | 10.345, 2.166 (15y), 4.333 s (15yr, whole call) | 4.333 vs 2.166 |
| `stale_cases_15yz.py` | A: only the failed extract row, every later stage blocked, only `mode/9999.json` changes, after the fix equal to a fresh build; B, C, D unchanged and correct | case A as a test |
| 15x-a extract, 1.01.02 only | new fit 0.415 h vs 0.372 h (+12%); old 0.228 h | 0.50 h (+33%, both archives) |

**Read in the code:**
- `upstream_ok` walks the `V2_UPSTREAM` chain to the end. The brief said one level. The step's reason holds: after a failed extract the
  mode file is new, so `join`'s lead marks change and join would rerun on the old units, although units' own upstream row is done.
  Accepted as a deviation.
- `v2_todo` removes blocked inventories before the params check, so "parameters changed: redoing all" never touches a blocked one.
  The join gate is evaluated after `current` (harmless). `recorded_sizes` reads the last done extract row with integer sizes, and
  `main` reuses it only for a file source with an equal stored size. Dirs are always measured; v1 is unchanged.
- `modes.extract_inventory_v2_modes`: `seconds` covers the pre-pass, the decision, the gate and the extract; `extract_stats` is unchanged.
- Run script: `build_cmds` returns the command list; `unsized` mirrors the build's reuse rule through `build.recorded_sizes`; the
  log line splits stages and whole-layer time. The docstring's "not read" sentence is replaced as briefed.
- Stager: `scheduled_deletions` covers only `LAYER_DIRS` trees (skipping `wbc/parts`) and per-inventory files of the layer's wanted
  inventories; it never touches `ROOT_FILES`, `LAYER_FILES` or `params.json`. A missing source dir would schedule a whole mirrored dir;
  the 100-file cap stops that (exit 1, nothing deleted). With the default `--inventories` it should remove exactly the 24 stale split
  files of the staged copy (F12-15z.10); the 16 pinned volumes' other files stay, since only the six parse-error volumes are wanted.
- **Defect, fixed in review:** the new `done_inventories` required only the stages seen *for that inventory*, so an inventory with an
  extract row alone counted as built. The old code required the layer's full stage count. An audit after a run that stopped after
  extract would then have read missing tables. Now the inventory's stages must equal the stages seen in the sub-layer, and all must be
  `done`. Membership uses a dict, not a list (73k rows × 9k inventories). The self-test has a `partial` case that fails on the
  step's version.
- **Validation error:** `validate.py` adds 1.10.94's extract (455/456, 4 workers) to the 15x-a figure, but 0.372 h is the 1.01.02
  span. For 1.01.02 alone: +12%. Recorded seconds × group factors give 5,324 worker-s (0.37 h on 4 workers, the measured span), and the
  pooled fit gives 5,971. So the remaining error is the mix: one pooled factor of 1.82 against 1.32 for the split print volumes, which
  are 52% of `15x-a`. For the full layer's mix the fit is +6%. A per-mode extract fit would serve print-heavy subsets; it is not
  needed for whole-layer planning.
- The changelog and `docs/text_layer.md` were edited by string replacement, not Edit (rule 3). The diff shows only the intended
  additions, so nothing was lost.

**Statuses:** F12-15yr.1, .2, .3, .6 confirmed; .4, .5 revised. **Brief defects** listed by the step (the ambiguous "15y-b row" of 3463;
the old-code rebuild prediction being stages only; `$B` not word-splitting in zsh) change no result. The last is a planner's
defect: give zsh commands as arrays or written out (`CONTEXT.md` §5 already says "write the prefix out").
