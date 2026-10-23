# Phase 09 report: Text layer v3 preparation

> **Status:** final (reviewed 2026-10-22).
> **Phase plan:** [`plan/phase-09-text-layer-v3-prep/PLAN.md`](../plan/phase-09-text-layer-v3-prep/PLAN.md).
> **Code state:** AS unreleased after v0.13.0 (recommended: v0.14.0); LotA docs only, 2026-10-22.
> **Last updated:** 2026-10-22, by step 15m (Sonnet 5.5).

## Summary

Phase 09 fixed the phase-07 follow-ups in the text-layer code and readied the full SG run
`text_layer_v3`. Nothing is published. The reference is dev layer `15m-a` (16 pilot inventories, v3
presets), and the full run waits for the user to stage inputs (UA-015). No sibling code changed; LotA
has a note on what v3 changes for its reader (§8).
- **Print reading order** (15j): same-row swaps 600 → 13 (455) and 1,382 → 14 (456); 130 jump regions
  → 0. All of 47 reorders read are right or ambiguous; none is worse (F09-15j.1–.3).
- **Page numbers** (15k): 188 off-chain numbers that fit the chain stay `page_number`. Print bottom-band
  quire numbers (367) and off-chain years (37) become `other`. The other bands stay: their numbers are
  mostly misread page numbers (F09-15k.2), and hand-bottom waits on crops (F09-R.5).
- **Continuation marks** (15l): hand `, `/`,,` (70) and 455's `J` (2,172, error ≤ 0.2%, F09-R.4) are
  joined away. The upper-case guard is a negative result (24/40 and 20/40 wrong, bar 27); the
  repeated mark read as a capital explains it (F09-15l.5).
- **Crosswalk and REPUBLIC** (15k): 513 `end` ids map to the right half (20 of 20 read). The library no
  longer names LotA's REPUBLIC path.
- **116 rows:** share 0.526 / bound 0.560 by the static verdicts; 0.543 / 0.569 if rows 23 and 188 are
  relabelled as read (F09-R.3).

## Findings

One row per finding. The ID is `F09-<step>.<n>`. Planning findings are `F09-P.<n>`, the review's
are `F09-R.<n>`. IDs never change and are never reused. A finding that turns out to be wrong is
marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F09-P.1 | Of 1,886 merged print layout regions in `15ir-a`, 55 hold a pair of pieces that the interleave guard blocks, bridged by a third piece (455 54, 456 1). Of the 12 read, 10 small pieces are end-of-line or paragraph-end fragments of the column text ("den.", "doen.", "Signé,") and 2 have no text. A transitive guard would cut them off, so the bridge is not fixed | negative-result | sg, print, layout | confirmed | [Planning](#planning) |
| F09-P.2 | Print baselines are less than 15 px apart within a printed row and about 48 px between rows (box h about 75 px). A row sort with a tolerance of 0.2 × median h would cut same-row swaps in sorted pieces from 561 to 12 (455) and from 1,380 to 14 (456), with no new jumps. A tolerance of 0.5 × h merges rows: 246 regions in 456 get new jumps | method | sg, print, layout, reading-order | confirmed | [Planning](#planning) |
| F09-P.3 | `15ir-a` has 3,293 off-chain `pn_line` segments (hand 2,464, print 829). 188 lie strictly between the chain numbers of the pages around them (hand 184, print 4); the 8 of those read are real page numbers that lost the candidate slot to another band number ("4767"). Top-band off-chain lines also include misread page numbers outside the gap ("552 )" between 550 and 552) | data-property | sg, types, page-numbers | confirmed | [Planning](#planning) |
| F09-P.4 | In 455, 2,175 of 38,965 merges (5.6%) have a right side `J` + lower case; 6 of 6 read are the repeated break mark ("hou\|Jdende", "Colle\|Jgie"). 456 has 32: of 12 read, 7 are real J-words ("Junii", "Jurisdictie"), 3 look like the mark ("exami\|Jneren") and 2 are unclear | data-property | sg, print, join, ocr | confirmed | [Planning](#planning) |
| F09-P.5 | 553 REPUBLIC ids mapped `split_original` are a paragraph's `line_end` (455 135 + 8 also `line_start`, 456 378 + 32). In all 10 read (455), the paragraph ends in the last half, but the crosswalk maps to the first | open-issue | sg, print, crosswalk | confirmed | [Planning](#planning) |
| F09-P.6 | A row sort of hand `unchanged` regions would reorder 2–7% of them (3349 236 of 12,090; 3352 372 of 11,152; 3772 752 of 11,435; 11182 229 of 5,468). None were read, so hand keeps its XML order | data-property | sg, hand, layout | confirmed | [Planning](#planning) |
| F09-P.7 | Only the 16 pilot inventories (14 hand, 455 and 456) have PageXML locally, and REPUBLIC paragraphs exist only for 455 and 456, in LotA `data/`. The full `text_layer_v3` run needs staging first | data-property | sg, text-layer | confirmed | [Planning](#planning) |
| F09-15j.1 | The row-tolerant sort (0.2 x median h) cuts same-row swaps in sorted print pieces from 561 to 12 (455) and from 1,380 to 14 (456), with 0 jump regions, matching the planning simulation exactly | result | sg, print, layout, reading-order | confirmed | [Step 15j](#step-15j-row-tolerant-line-order) |
| F09-15j.2 | Sorting `unchanged` print regions removes all 130 jump regions in 455 (456 had none); 182 (455) and 7 (456) regions are flagged `lines_resorted`. Of the 27 resorted regions read (20 of 455, all 7 of 456), 25 are now right, 2 ambiguous (digit pair, OCR-garbled index), none worse | result | sg, print, layout | confirmed | [Step 15j](#step-15j-row-tolerant-line-order) |
| F09-15j.3 | Of 20 reordered pieces/merges read (Random(71)), 19 are right and 1 ambiguous (a table of amounts); none worse. The typical fix is a short fragment ("doen.", a page number, a date) that now reads left to right on its row | result | sg, print, layout | confirmed | [Step 15j](#step-15j-row-tolerant-line-order) |
| F09-15j.4 | The 26 swaps left in sorted classes (455 12, 456 14) are 8 header rows (page number about 15 px above the date line), 11 numeric-column cells and 7 running-text fragments at a row end; the baseline gap is at or just over the tolerance, which is under 15 px where the median box h is under about 75 px. Not tuned | negative-result | sg, print, layout | confirmed | [Step 15j](#step-15j-row-tolerant-line-order) |
| F09-15j.5 | The new order changes unit segmentation: print `join/units` 455 28,005 to 27,956 (-49), 456 24,710 to 24,515 (-195), 0 id collisions; 673 (455) and 1,798 (456) unit texts exist only in the old layer. Hand output differs from `15ir-a` only by the added columns | behaviour-change | sg, print, units | confirmed | [Step 15j](#step-15j-row-tolerant-line-order) |
| F09-15k.1 | Bars for the off-chain retype (P6), read on the 137-row planning sample: only print-bottom passes (19 of 20 not page numbers, 1 unclear); hand-top 6, hand-bottom 12, print-top 3 not_pn all fail the bar of 16, and the 37 off-chain years pass (37 of 37) | result | sg, print, hand, types | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15k.2 | Negative result: the top-band off-chain numbers are mostly this page's own number read wrongly (print-top: 12 of 20 pn, "( 598 )" between 197 and 199 = 198; hand-top 8 pn + 6 unclear), so they stay `page_number`; the print-bottom ones are quire numbers ("31" beside "(209 )") and became `other` (367 segments) | negative-result | sg, print, types | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15k.3 | `rule:pn_line_gap` keeps 188 segments as page numbers (hand 184, print 4), as planned; 18 of the 20 gap rows read are real page numbers, 2 unclear (no neighbour context), 0 wrong | result | sg, types | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15k.4 | The `role` column and the `end` rule move all 513 `split_original\|end` ids (455 135, 456 378) to the last half; of 20 read (seed 61) all 20 paragraph ends lie in the mapped half | result | sg, print, crosswalk | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15k.5 | `claimable` built per scan gives identical units and cuts the 455 units stage from 9.7 s to 4.2 s (456 5.2 s, unchanged) | result | sg, print, units | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15k.6 | A year piece after a word ("Maart 1718", segment "1718") is only a year by its segment text, not the line text; the retype tests the segment text (the brief said line text: 33 instead of the planned 34 hand years) | method | sg, hand, types | confirmed | [Step 15k](#step-15k-page-number-types-crosswalk-role-units) |
| F09-15l.1 | `J` bar adopted for 455: 30 of 30 read `J`+lower right sides are the repeated hyphen ("sul\|Jlen", "Ver\|Jgaderinge"); `join_lead_marks_lower={"455": "J"}`. 455 `J`+lower 2,172 to 0, lower-case right-side share 0.892 to 0.948 | result | sg, print, join | confirmed | [Step 15l](#step-15l-continuation-marks-and-the-upper-case-guard) |
| F09-15l.2 | 456 is not pinned: of 32 `J`+lower right sides 23 are real words or names ("Junii", "Januarii", "Jacob"), 2 are the mark, 7 unclear; the per-inventory pin is needed because the same character is a break mark in 455 and a letter in 456 | result | sg, print, join | confirmed | [Step 15l](#step-15l-continuation-marks-and-the-upper-case-guard) |
| F09-15l.3 | The extended comma rule stays: 67 of 70 hand `, ` / `,,` rows are the repeated break mark (bar 63; 1 real comma, 2 garbled OCR). Hand `comma_sp` + `comma2` 70 to 0; 3 more right sides now start upper-case (exposed by the strip) | result | sg, hand, join | confirmed | [Step 15l](#step-15l-continuation-marks-and-the-upper-case-guard) |
| F09-15l.4 | Negative result: `g_upper_right` is not adopted in either sub-layer; upper-case merges are wrong in 24 of 40 (hand) and 20 of 40 (print), bar 27. Guard stays off; code and tests are in place | negative-result | sg, hand, print, join | confirmed | [Step 15l](#step-15l-continuation-marks-and-the-upper-case-guard) |
| F09-15l.5 | What the `right` upper-case merges share: a stray capital that is the repeated break mark read as a letter ("Secreta\|Pris", "aenge\|Ystelt", "Ver\|Vgaderinge", "Sup\|Pliant", "Decem\|Vcember"): 10 of 40 hand, 16 of 40 print. A guard on a capital cannot tell them from compounds ("West\|Vrieslandt"); a candidate is a mark-after-hyphen rule for capitals that repeat the letter pair, or a lexicon check. Not built | method | sg, join | confirmed | [Step 15l](#step-15l-continuation-marks-and-the-upper-case-guard) |
| F09-15m.1 | Phase totals against the pilot code (`15ir-a` to `15m-a`): print same-row swap pairs 455 600 to 13 and 456 1,382 to 14, backward-jump regions 130 to 0, 189 regions resorted, 188 `rule:pn_line_gap`, 367 print numbers and 37 years retyped `other`, hand `, ` / `,,` merges 70 to 0, 455 `J`+lower merges 2,175 to 0, 513 crosswalk ids to the last half | result | sg, print, hand, phase | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.2 | The integrated build `15m-a` (v3 presets, from scratch) equals the chained layer `15l-a` in all 288 tables (hand 237, print 51), 0 differ; the v3 presets differ from v2 only in `name` and `description` | result | sg, presets | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.3 | The 116-row evidence does not move: share free 0.526, bound 0.560 on both layers, 0 verdicts change; 8 rows change cells, and by reading rows 23 and 188 would be `fixed` (share 63/116 = 0.543, bound 0.569) if re-labelled, which the script does not do (its verdicts are hand-read and static) | result | sg, rows116, lota | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.4 | The audit passes 178 of 182 on both layers; the 4 failures are the lower-case merge shares (3351 0.9415, 3823 0.9414, 455 0.9477 (was 0.892), 456 0.932); 455 misses 0.95 by 0.002 and the rest is the stray-capital and real-word merges | result | sg, audit | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.5 | Of 20 changed `join/units` texts read (`Random(101)` over 1,640), 13 are the 455 `J` mark (all better), 6 are the row sort (all better by hyphen pairs that now join: "toeko-" + "men.", "Cel." + "derlandt."), 1 neutral; 0 worse | result | sg, print, join | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.6 | Pin: pagexml-tools 0.9.1 is not on PyPI on 2026-10-22 (latest 0.9.0), so nothing is pinned and the 0.9.0 workarounds stay; UA-007 stays open (its Response says the fix waits for the release) | open-issue | pagexml-tools | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15m.7 | The full run needs 8 hand inventories not in `shared-data` (only 3349-3354 are staged), the print inventories beyond 455/456 and REPUBLIC paragraphs outside LotA's `data/`; the run kit is smoke-tested (`--limit 1`, exit 0) and the staging is UA-015 | open-issue | sg, run-kit | confirmed | [Step 15m](#step-15m-v3-presets-integrated-dev-build-run-kit) |
| F09-15n.1 | `scripts/run_text_layer_v3_full.py` reproduces `15m-a` on the local pilot sources (`15n-a`): `compare.py` hand 215 equal / 22 differ, print 49 / 2, and every difference is a source path (`scans/` `member` of the 8 directory inventories, `extract_stats` `source` of all 16), which now names the input link; audit 178 / 182 as `15m-a` | result | sg, run-kit | confirmed | [Step 15n](#step-15n-hand-off-script-for-the-full-text_layer_v3-run) |
| F09-15n.2 | The step's time prediction (pilot worker-seconds per input byte / `--want`) was 4.6x too low: one inventory is one task (455 alone about 185 s), coreshare granted 5 and 4 workers, and the hand pilot mixes flat XML dirs with tars, so bytes undercount tars about 4x. Fixed in review: per XML byte (archives x 4.16), range max(worker-s / workers, largest inventory) to 3x that; pilot actuals hand 2.6 min and print 4.8 min fall in the predicted 1-3 and 4-12 min | method | run-kit, estimate | confirmed | [Step 15n](#step-15n-hand-off-script-for-the-full-text_layer_v3-run) |
| F09-15n.3 | `audit.py` (step 15m) is pilot-specific: fixed inventory lists, sources read from `15i-src`, the four lower-case share failures; on the full run it can crash or fail, and the script logs that and goes on | open-issue | audit, run-kit | confirmed | [Step 15n](#step-15n-hand-off-script-for-the-full-text_layer_v3-run) |
| F09-15n.4 | The UA-016 clean-up broke 11 local-data tests (9 image tests read `data/thumbs`, 2 opening-detection tests read `data/PageXML/<archive>/` that are now tars). They now skip with a pointer to the audit; suite 1,202 passed, 11 skipped | open-issue | tests, disk | confirmed | [Step 15n](#step-15n-hand-off-script-for-the-full-text_layer_v3-run) |
| F09-15o.1 | The `text_layer_v3` pilot is published to shared-data (hand 305 MB, print 84 MB); the audit of the published copies reproduces `15n-a` exactly: 178 pass, 4 fail, same invariants, same `counts`, and the v2 dirs and dev layers `15m-a`/`15n-a` are byte-identical before and after | result | sg, publication | confirmed | [Step 15o](#step-15o-v3-pilot-publication) |
| F09-15o.2 | The 8 archive sha256s equal the ones in the published v2 manifests, so v3 reads the same PageXML as v2; hand 3771-3773, 3821-3823, 11182, 11281 are directories identified by file count only, not by hash (the v2 manifests have the same gap) | data-property | sg, provenance | confirmed | [Step 15o](#step-15o-v3-pilot-publication) |
| F09-15o.3 | The first rows of 455 (scan 0003, a title page) are OCR junk ("LINULA", "HOOOI MOCICC ICCTCIDACCH"), typed `running_header`; the main-text rows of scan 0100 read in order within a column with no column interleave, but show residual OCR noise ("0Is ter", "sou-cie van de") | data-property | sg, print, ocr | confirmed | [Step 15o](#step-15o-v3-pilot-publication) |
| F09-R.1 | The review re-run reproduces the phase: AS 1,205 and NLP 66 passed; v1 `TOTAL 157 / 157`; `15m-a` = `15l-a` (0 of 288 tables differ); same-row swaps 13 / 14, 0 jump regions; 188 gap, 367 + 37 retyped; audit 178 / 182 | result | sg, phase | confirmed | [Review](#review) |
| F09-R.2 | Joining `units/republic_lines` of `15ir-a` and `15m-a` on `republic_line_id`, only the 513 `split_original` rows with role `end` change; the "plus 30 `unmatched` rows" in the 15m Part 3 table is not reproduced | method | sg, crosswalk | confirmed | [Review](#review) |
| F09-R.3 | Read in the layer, 2 of the 116 rows are now free of their artefact. Row 23's print text "vanJder" is "vander". Row 188's print tokens are found exact, in reading order. Relabelled, the share free would be 63/116 = 0.543 (bound 0.569); the static verdicts give 0.526 / 0.560. LotA re-checks after re-alignment (D19) | result | sg, rows116, lota | confirmed | [Review](#review) |
| F09-R.4 | The 455 `J` rule's error rate is at most 0.2%: of the 2,172 stripped right sides, at most 3 are real J-words ("Jacob", "Jacht", "Jonel"). The most frequent are marks ("Jden" 160, "Jde" 105, "Jdende" 30) | result | sg, print, join | confirmed | [Review](#review) |
| F09-R.5 | Hand-bottom off-chain numbers: all 12 decidable sample rows are not page numbers. The bar of 16 missed only because 8 rows sit on pages without a chain number nearby and are `unclear`. About 890 such segments keep `page_number` until crops decide them | open-issue | sg, hand, types | confirmed | [Review](#review) |
| F09-R.6 | UA-015's staging command was wrong: it copied symlinked flat directories into `shared-data`, whose hand sources are `<inv>/page/*.xml` tars. It is replaced by a `tar -s` loop. Tested on 11182, a build from the tar equals `15m-a` in every table except the archive `member` path | bug-fixed | sg, run-kit | confirmed | [Review](#review) |
| F09-R.7 | The 455 lower-case share rising from 0.892 to 0.948 is mechanical: the audit share counts exactly the right sides the `J` rule removes. The evidence for the rule is the reading (F09-15l.1, F09-15m.5, F09-R.4) | method | sg, audit | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`,
  `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Measured on 2026-10-21 on `data/text_layer_v2_dev/15ir-a` (the phase-07 code after 15ir), with the
scripts in [`data/phase-09-text-layer-v3-prep/planning/`](data/phase-09-text-layer-v3-prep/planning/README.md).
They read layers and write only their own outputs.

| Check | Result |
|---|---|
| Print line order (`order.py --simulate`) | Same-row swaps in sorted pieces: 455 561, 456 1,380. Simulated row sort: 12 and 14, 0 new jumps; 238 and 863 regions reordered. `unchanged`: 455 has 130 regions with a jump; the sort would reorder 182 (456: 7) and remove every jump |
| Baseline steps between consecutive sorted lines of a region (`order.py` data; regions ≥ 3 lines) | 5th–90th percentile 41–53 px. Under 15 px in 0.7% (455) and 1.6% (456) of steps: same-row fragments. Median box h 75 px (455) and 76.5 px (456) |
| Interleave bridges (`bridge.py`) | 55 of 1,886 merged print regions; 12 read (F09-P.1) |
| Page-number segments (`pn_offchain.py`) | Confirmed `rule:pn_line`: hand 6,637, print 2,189. Off-chain: hand 2,464, print 829. Gap-consistent: 188. Non-gap years: 37. Sample `pn_sample_planning.tsv` (seed 41) |
| Merge right sides (`merges.py`) | Upper case: hand 4,219, print 4,447. `J`+lower: 455 2,175, 456 32, hand 34 (names). `, ` + letter: hand 65; `,,`: hand 5. Digits: 0 (`g_digit_right` guards them) |
| Crosswalk (`crosswalk.py`) | `split_original` by role: 455 start 46 / end 135 / both 8; 456 102 / 378 / 32 |

**User decisions (2026-10-21):**
- the slug, and the phase ends with a dev layer and a run kit, with no publication (P1);
- print `unchanged` regions are sorted and flagged, hand keeps its order (P2);
- the `g_upper_right` bar is 27 of 40 per sub-layer (P3).

**Planner decisions:** P4–P12 in the phase plan. They include a row tolerance of 0.2 × median h,
not tuned; per-band bars for retyping off-chain numbers; a per-inventory `J` mark; and the
REPUBLIC path removed from the library.

**Findings:** F09-P.1–F09-P.7.

### Step 15j: row-tolerant line order

**What changed.** Brief: [`step-15j-row-tolerant-line-order.md`](../plan/phase-09-text-layer-v3-prep/step-15j-row-tolerant-line-order.md).
`LayoutParams` gets `line_sort="row"`, `row_tol_frac=0.2` and `sort_unchanged_lines=False`; new
`sort_lines_by_row` / `sort_lines`; merges and column-split pieces are sorted by printed row; the print
preset (`SG_PRINT_LAYOUT_PARAMS`) also sorts `unchanged` regions and flags them in the new
`layout/regions` column `lines_resorted` (scan column and stat `n_regions_resorted`). Hand keeps XML
order. Dev layer `data/text_layer_v2_dev/15j-a/{hand,print}`, compared with `15ir-a`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1 Builds | exit 0, 0 failures, sanity pass | both `failures=0`, no `--ignore-sanity` |
| 2 Same-row swaps, sorted pieces, 455 | 561 to <= 25 (sim. 12) | 561 to 12 (1 + 5 + 6) |
| 2 Same-row swaps, sorted pieces, 456 | 1,380 to <= 40 (sim. 14) | 1,380 to 14 (0 + 0 + 14) |
| 2 Jump regions in sorted classes | 0 | 0 |
| 2 Sorted regions whose order changed | about 238 (455), 863 (456) | 240 (455), 892 (456) |
| 3 `unchanged` jump regions 455 / 456 | 130 to 0 / 0 to 0 | 130 to 0 / 0 |
| 3 `lines_resorted` 455 / 456 | 182 +- 20 / 7 +- 3 | 182 / 7 |
| 3 `n_regions_resorted` = column count | equal | 182 / 7, equal |
| 4 Hand | only added columns / new keys | `layout/regions`, `layout/scans`: only added all-0 column; `params.json`, `stats.json`: new keys only; `layout/lines`, `types/ units/ join/ wbc/` identical |
| 5 Units 455 / 456 | report | 28,005 to 27,956 / 24,710 to 24,515; `n_id_collisions` 0 / 0 |
| 5 `join/units` texts that differ | report | only in old: 673 / 1,798; only in new: 624 / 1,603 (multiset of scan, text) |
| 6 AS suite | green | 1,172 passed (1,172 collected; was 1,166) |
| 6 v1 check | TOTAL 157 / 157 | TOTAL 157 / 157 |
| 6 NLP suite | green | 66 passed |
| 6 Protected hashes | unchanged | `diff` of before / after empty |

Print `unchanged` swap pairs also fell: 455 39 to 1, 456 2 to 0.

**Error analysis.** Script `reports/data/phase-09-text-layer-v3-prep/step-15j/order_diff.py`; dumps
(`sample_sorted.txt`, `sample_unchanged_455.txt`, `unchanged_456.txt`, `remaining_swaps.txt`),
`counts.json` and the verdicts [`ea_verdicts.tsv`](data/phase-09-text-layer-v3-prep/step-15j/ea_verdicts.tsv)
sit beside it. Each dump shows the old and new order of the moved window with baseline y, box x and the
first 40 characters.

- *What was read:* 20 changed regions of the sorted classes (`Random(71)` over the 1,132 changed ones),
  20 resorted `unchanged` regions of 455 (`Random(72)`), all 7 of 456, and all 26 remaining swap pairs
  in the sorted classes. Verdicts were by reading the order against the text, not against images.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | right, sorted pieces | 19 / 20 | "doen." (x 472, y 1903) now before "drde ende Copye van de Requeste oock" (x 1068, y 1896) |
  | ambiguous, sorted pieces | 1 / 20 | table of amounts: "1700. . 1701. en 1702-" before "1012-10-" |
  | worse | 0 | |
  | right, `unchanged` | 25 / 27 | 455 page 0702: 14 lines that jumped back to the start of the region are back in y order |
  | ambiguous, `unchanged` | 2 / 27 | digits "0" (x 4295) and "2" (x 4244), 3.5 px apart; OCR-garbled index lines |
  | remaining swap: header row | 8 / 26 | "( 1378. )" (x 1262, y 378.7) before "Den 20. December." (x 466, y 393.5) |
  | remaining swap: numeric column cell | 11 / 26 | "wijdt" (x 1218) before "op 35" (x 525) in a table of timber sizes |
  | remaining swap: running-text fragment | 7 / 26 | "de te hebben." (x 816) between two lines at x 440 |

- *Conclusion:* the row sort does what it was meant to do. Almost all reorders put a short
  right-hand or left-hand fragment at its place on the row, and for `unchanged` regions the old XML jumps
  (a signature block or marginal line stored at the end) disappear. No reorder made a region worse.
  What remains are rows whose baselines differ by about 15 px: page-number/date headers and table cells,
  where the tolerance (0.2 x median box h) is just below the gap. The tolerance was not tuned (a larger
  one merges rows, F09-P.2), so these stay open. The ambiguous cases are tables, where left-to-right
  order by x is the only available rule.

**Findings:** F09-15j.1 - F09-15j.5.

**Open issues.** The 26 remaining swaps (header rows, table cells). The change shifts unit counts
(-49 / -195), so 15k and later compare against `15j-a`, not `15ir-a`.

### Step 15k: page-number types, crosswalk role, units

**What changed.** `TypeParams` gets `offchain_retype`, `offchain_other_bands`, `offchain_years_other`; `type_inventory` decides each
`rule:pn_line_offchain` segment (gap, year, band) after the 15ir relabel. Both SG presets switch it on, with the outcomes
of Part 1 below. `units.py`: `read_republic_roles`, `role` column and `end` rule in `republic_crosswalk`,
`claimable` per scan, `REPUBLIC_PARAGRAPHS_DIR` removed (`stage_units` warns and skips the crosswalk without
`--republic-dir`). Brief: [`step-15k-types-units-followups.md`](../plan/phase-09-text-layer-v3-prep/step-15k-types-units-followups.md).
Layer `data/text_layer_v2_dev/15k-a` (a first build with a line-text year test is kept as `15k-a-first`, see F09-15k.6).

**Part 1: bar outcomes (read before building).** Verdicts for all 137 rows of the planning sample are in
`reports/data/phase-09-text-layer-v3-prep/step-15k/pn_verdicts.tsv`; unclear counts as `pn`.

| Stratum | pn | not_pn | unclear | Bar | Outcome |
|---|---|---|---|---|---|
| hand-top | 8 | 6 | 6 | not_pn >= 16 | kept `page_number` |
| hand-bottom | 0 | 12 | 8 | not_pn >= 16 | kept `page_number` |
| print-top | 12 | 3 | 5 | not_pn >= 16 | kept `page_number` |
| print-bottom | 0 | 19 | 1 | not_pn >= 16 | **`other`** (`offchain_other_bands=("bottom",)`) |
| year (37, pooled) | 0 | 37 | 0 | not_pn >= 30 | **`other`** (`offchain_years_other=True`, both presets) |
| gap (20) | 18 | 0 | 2 | none | kept (18 of 20 pn, 2 unclear) |

Hand-bottom is 12 not_pn + 8 unclear: the unclear rows have no context (a lone "591", "76.", "2"), so the bar
was not met by 4 rows. Not tuned.

**Results (15k-a against 15j-a).**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Builds | exit 0, 0 failures, sanity | hand and print `failures=0` |
| 2. Drift of `pn_offchain.py` on 15j-a | none for hand | none: hand off-chain 2,464, gap 184, non-gap years 34; print 829, gap 4, years 3 |
| 3. `rule:pn_line_gap` | 188 | 188 (hand 184, print 4) |
| 3. `other` by band | print bottom 366 (planning) | 367 (455 194, 456 173): the planning band is y < page_h/2, the code's is the 3 topmost lines; 1 line differs |
| 3. years | 37 | 37 (hand 34, print 3; 455); no other `line_type` / `type_method` change, no change in other columns |
| 4. Units | only retyped units differ | `unit_type` only: 455 197, hand 26 units (11182 4, 3349 9, 3350 2, 3352 4, 3353 1, 3823 6), print 456 0; 1 split (hand 11281 scan 0053: year "1718" and page number "6." were one `page_number` unit) and 1 join (456 scan 0509: a retyped number joined an `other` run, 24,515 to 24,514); `join/` differs in the same units; 455 `seconds` 9.7 to 4.2 (<= 5) |
| 5. Crosswalk | match x role as planning; 513 `line_id` changes | identical counts (455 `split_original|end` 135, 456 378); exactly these 513 rows change `line_id`, each to the last half (checked against `split_from` siblings) |
| 6. Suites | green | AS 1,179 passed (was 1,172; +7 tests); NLP 66 passed; v1 TOTAL 157 / 157; protected diff empty |

Hash-protected trees unchanged (`15k-protected-{before,after}.txt` identical).

**Error analysis.**

- *What was read:* all 137 sample rows (Part 1); 10 retyped segments per stratum of the built layer (`Random(81)`,
  `step-15k/sample_retyped.py`, `sample_retyped.txt`); 20 crosswalk rows (10 per inventory, `crosswalk.py --seed 61`,
  `crosswalk_sample.tsv`).
- *Categories (Part 1):*

  | Category | Count | Example (verbatim) |
  |---|---|---|
  | hand-top: own page number, misread | 8 | "( 930 )" with next page "( 901 )" |
  | hand-top: not a number of this page | 6 | "4768" on a page whose number is "( 190 )" |
  | print-top: own number with a leading digit or OCR error | 12 | "1919 )" after "(918 )"; "( 598 )" between 197 and 199 |
  | print-bottom: quire / signature number | 19 | "31" under "(209 )"; "130" under "1161 )" |
  | years: 1703., 1704., 1718 ... | 37 | "1703." with next page "104." |
  | gap: this page's number | 18 | "( 551 )" between "( 550 )" and "( 552 )" |

- *Retyped segments in the build:* year (10 read): 10 of 10 are years at the foot of a page (8 beside no or another page
  number, "1768." beside "( 86 )"); print band (10 read): 8 consistent with quire / signature numbers
  (111, 130, 120, 151, 91, 121, 52, 361. under pages 1001-1315), 2 unclear ("19." and "1" with no page number nearby).
  The rule did what the verdicts predicted.
- *Crosswalk (20 read):* 20 right: the paragraph's last words are in the mapped (second) half, usually after the
  `||` where our split falls inside the sentence ("... Hoogh Mogende || Guarnisoen seerqualijck hadden getracteert, ende").
- *Conclusion:* a retype by band is only safe where the band holds mostly quire numbers (print bottom). The top
  band mixes in the page's own number, misread by OCR; those are mostly right as `page_number` and a numeric or
  chain-based check would be needed to separate them (not done). The gap rule keeps what the chain lost.

**Findings:** F09-15k.1 - F09-15k.6.

**Open issues.** Off-chain top-band numbers that are not this page's number (hand-top 6 of 20, e.g. "4768") stay
`page_number`; 455 `split_original|both` ids (8) and 456 (32) map to the first half. The hand-bottom verdicts need
context the sample lacks.

### Step 15l: continuation marks and the upper-case guard

**What changed.** `join_unit` / `join_inventory_v2` get `lead_marks_lower` and `upper_guard`; the `lead_marks` strip is now
`[marks]{1,2} ?` before a letter. `LayerPreset.join_lead_marks_lower` (per inventory) and `join_upper_guard`; `stage_join`
resolves the pin per inventory and re-joins when stats differ. `SG_PRINT_V2` pins `{"455": "J"}`; the guard stays off in
both presets. Brief: [`step-15l-join-followups.md`](../plan/phase-09-text-layer-v3-prep/step-15l-join-followups.md).
Layer `data/text_layer_v2_dev/15l-a`.

**Part 1: bar outcomes (read before building).** `merges.py --sample 40 --seed 51` on `15k-a`; 215 rows with verdicts in
`reports/data/phase-09-text-layer-v3-prep/step-15l/merges_verdicts.tsv`. Counts on `15k-a` equal planning except print
`upper` (455 2,036, was 2,043; 456 2,411, was 2,404; 15j drift) and 455 `J`+lower 2,172 (was 2,175).

| Stratum | mark / wrong | word / right | unclear (+ real_comma) | Bar | Outcome |
|---|---|---|---|---|---|
| 455 `J`+lower (30) | 30 | 0 | 0 | mark >= 27 | **adopted**, `{"455": "J"}` |
| 456 `J`+lower (32, no bar) | 2 | 23 | 7 | stop if mark >= 27 | not near; unpinned |
| hand `, ` / `,,` (70) | 67 | 0 | 2 + 1 real_comma | mark >= 63 | **rule stays** |
| hand `upper` (40) | 24 wrong | 10 right | 6 | wrong >= 27 | guard **off** |
| print `upper` (40) | 20 wrong | 16 right | 4 | wrong >= 27 | guard **off** |

The 3 print 456 `comma_sp` rows (not part of any bar) were all unclear.

**Results (15l-a against 15k-a).**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Builds | exit 0, 0 failures, sanity, 0 offset violations | hand and print `failures=0`, `sanity_pass` true, `n_offset_violations` 0 in every `join/stats` |
| 2. Hand commas | `comma_sp` + `comma2` 70 to 0 | 65 + 5 to 0; print `comma_sp` unchanged (3) |
| 3. 455 `J` | `J`+lower to 0, lower share 0.892 to about 0.948 | 2,172 to 0; share 0.8920 to 0.9477; 456 unchanged (32 `J`+lower, share 0.9320) |
| 4. Guard | guard off: nothing changes | merges unchanged in every inventory (hand 215,277, 455 38,981, 456 40,898); 0 `g_upper_right`; hand `upper` +3 (4,219 to 4,222) because a `, ` / `,,` + capital right side is now seen after the strip |
| 5. Other tables | `scans/ lines/ layout/ types/ units/ wbc/` identical | identical (`compare.py`); `join/` differs in 70 hand merges (7 inventories, 28 files incl. 14 stats) and print 455 only (1,213 units, 2,172 segments); 456 only its stats keys |
| 6. Suites | green | AS 1,204 passed (was 1,179; +25 tests); NLP 66 passed; v1 TOTAL 157 / 157; protected diff empty |

**Error analysis.**

- *What was read:* all 215 sample rows (Part 1, judged from the 30-character context); 10 changed hand and 10 changed
  print 455 spans of the built layer (`Random(91)`, diff of `join/units` text against `15k-a`).
- *`J` (455):* 30 of 30 are a word broken at the hyphen, with `J` the repeated mark: "sul|Jlen", "Ma|Jgistrats",
  "Valcke|Jnier", "Septem|Jber". 456: real words and names ("con|Jurisdictie", "Junii", "Januarii", "Jacob", "Johan"),
  one line-order error ("Plenipo|Janssen, tentiaris"), 2 marks ("exami|Jneren", "Resoo|Jlutie"), 7 unclear ("ver|Joght",
  "Ja|Juarii").
- *Commas (hand, 70):* 67 are a word cut in Latin or Dutch ("fue|, rant", "Sta|, tuta", "Woen|, sel", "vyf|,, tien"); the
  rest: "tu|, Dat" (real comma, new sentence) and two OCR-garbled lines ("nabnoo|, daan").
- *Upper-case, wrong (hand 24, print 20):* compounds "West|Vrieslandt" (hand 3, print 2), titles "Lieutenant|Collonel",
  "Groot|Brittannien", "Hoofd|Officieren", line-order or sentence starts "Amsterdam|April", "toekoe|Is ter
  Vergaderinge", "dili|WAAR op".
- *Upper-case, right (hand 10, print 16):* the same stray-capital mark as `J`, now another letter: "Secreta|Pris" (4 of
  the hand rows), "ge|Knuppelt", "aenge|Ystelt", "Ver|Vgaderinge", "Sup|Pliant", "Decem|Vcember", "Pro|Pvincie".
- *Built layer (10 + 10 read):* all 20 changed spans give the intended word: "in, colae" to "incolae", "Woen, sel" to
  "Woensel", "d'execu,,tion" to "d'execution", "wasgeJbraght" to "wasgebraght", "AdJmiraliteyt" to "Admiraliteyt".
- *Conclusion:* the repeated-mark pattern is real and strong in the hand `,` and print-455 `J` cases and is a pinned,
  per-inventory fact. A capital-based guard does not separate right from wrong: 40 percent of the print and 25 percent
  of the hand upper-case merges are the same mark read as a capital letter, which the guard would split.

**Findings:** F09-15l.1 - F09-15l.5.

**Open issues.** The stray-capital mark ("Secreta|Pris") is left in the text (hand about 10 of 40, print about 16 of 40 of
the upper-case merges). A rule that drops a capital when the lower-case remainder continues the word would need a lexicon
check; not built.

### Step 15m: v3 presets, integrated dev build, run kit

**What changed.** `presets.py`: `SG_HAND_V3`, `SG_PRINT_V3` (copies of the v2 presets with `name` and `description`
changed), registered as `sg_1.01.02_v3` and `sg_1.10.94_v3`; test `test_v3_presets_equal_v2_but_name`. No other code
changed. Docs: `docs/text_layer.md` *Text layer v3 (phase 09)* and the status of the *Known issues*; LotA
`docs/from-archival-structures.md` section 8 (no LotA code); UA-015. Layers `data/text_layer_v2_dev/15m-a` and
`15m-smoke`. Brief: [`step-15m-v3-build-and-run-kit.md`](../plan/phase-09-text-layer-v3-prep/step-15m-v3-build-and-run-kit.md).

**Part 0, pin.** `pip index versions pagexml-tools` on 2026-10-22: latest 0.9.0 (0.9.1 not on PyPI). Nothing pinned, no
`15m-pin`, UA-007 stays open.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| Builds | exit 0, 0 failures, `sanity_pass` | hand and print exit 0, `failures=0`, `sanity_pass` true in both manifests |
| `compare.py 15l-a 15m-a` | 0 differing tables | hand 237 equal, print 51 equal, 0 differ. By `diff -rq`, besides the skipped `manifest.json`, `progress.log`, `status.tsv`, the `seconds` values in `layout/`, `types/` and `units/stats.json` differ (run time); the layer directory name differs (`_v2` to `_v3`) |
| Audit | listed | 178 / 182 on `15ir-a` and `15m-a`; the same 4 fail |
| 116 rows | listed | free 0.526, bound 0.560 on both; 0 of 116 predictions differ; 8 rows differ in cells |
| Smoke | exit 0 | hand `--limit 1` (3349) and print `--limit 1` (455) exit 0; `--estimate` runs (no throughput fit for the SG presets, so no time) |
| Protected / v1 | empty diff / 157 of 157 | empty / `TOTAL 157 / 157` |
| Suites | green | AS 1,205 passed (was 1,204; +1); NLP 66 passed |

**Part 3, phase measurement, v2 dev (`15ir-a`) to v3 dev (`15m-a`).** Scripts and outputs in
`reports/data/phase-09-text-layer-v3-prep/step-15m/` (`audit.py`, `rows116.py` with the root glob `*_text_layer_v*`;
`order|pn_offchain|merges|crosswalk_<layer>.json|txt`).

| Measure | `15ir-a` | `15m-a` |
|---|---|---|
| Same-row swap pairs, 455: merged / merged+split / split_at_column / unchanged | 74 / 222 / 265 / 39 (600) | 1 / 5 / 6 / 1 (13) |
| Same-row swap pairs, 456: merged / merged+split / split_at_column / unchanged | 1 / 76 / 1,303 / 2 (1,382) | 0 / 0 / 14 / 0 (14) |
| Jump regions (> 50 px back), 455 `unchanged` (all other classes 0 in both layers; 456 0) | 130 | 0 |
| `lines_resorted` (regions) | 0 (no column) | 189 (455 182, 456 7; all `unchanged`) |
| `rule:pn_line_gap` segments | 0 | 188 (hand 184, print 4) |
| Retyped `other` (print numbers; years) | 0; 0 | print 367 (366 bottom, 1 top); `rule:year_offchain` hand 34, print 3 |
| Crosswalk `split_original` with role `end` (match count) | 455 135, 456 378 | same; `line_id` moved on 513 of them (455 135, 456 378), plus 30 `unmatched` rows (7, 23) whose `line_id` also changed *(review: not reproduced; joined on `republic_line_id` only the 513 `end` rows differ, F09-R.2)* |
| Merges with a comma-space right side (`comma_sp` + `comma2`) | hand 65 + 5, print 3 | hand 0, print 3 |
| Merges with `J`+lower right side | hand 34, 455 2,175, 456 32 | hand 34, 455 0, 456 32 |
| Upper-case right-side merges | hand 4,219; 455 2,043; 456 2,404 | hand 4,222; 455 2,036; 456 2,411 |
| Boundary kind `g_upper_right` | 0 | 0 (guard not adopted) |
| Units 455 / 456 (`n_id_collisions`) | 28,005 (0) / 24,710 (0) | 27,956 (0) / 24,514 (0) |
| Hand units | 181,455 | 181,456 |
| Audit invariants passed | 178 / 182 | 178 / 182 |
| 116 rows: share free / bound | 0.526 / 0.560 | 0.526 / 0.560 |

The print merge totals move (79,645 to 79,879; 455 38,965 to 38,981, 456 40,680 to 40,898) because the row sort changes
which lines meet at a column end. Of the 4 audit failures only the numbers moved: 455 0.8917 to 0.9477, 456 0.9318 to
0.9320, 3351 0.9415 (same), 3823 0.9407 to 0.9414; the hand shares of 3772, 3773, 3821, 3822, 11182 rise by 0.0001-0.001.

**Error analysis.**

- *What was read:* (1) all 8 of the 116 rows whose cells differ from `15ir-a` (0 changed prediction, so the brief's
  fallback of rows with changed text applies: these are the rows whose print or hand match changed), `rows116_15ir-a.tsv`
  against `rows116_15m-a.tsv`; (2) 20 of the 1,640 `join/units` rows whose text differs (hand 26, print 1,614; units
  present in both layers), `Random(101)`, file `step-15m/units_text_sample.txt`; for the four order cases the layout lines
  of the region were printed in both layers.
- *The 8 rows:*

  | Row | Change | Cause | Closer to the scan? |
  |---|---|---|---|
  | 66, 88, 290 | print match "byreJductie" / "werJden" / "ipsascomJmoratus" becomes "byreductie" / "werden" / "ipsascommoratus", ratio 0.88 to 0.833, 0.936 to 0.913, 0.965 to 0.952 | 15l `J` mark | yes: the stray `J` is gone; the ratio falls only because the gold print string still has it ("byre - Jductie") |
  | 23 | print match "vanJder" is gone (found fuzzy to none) | 15l `J` mark: the unit now reads "vander" and no longer matches the gold string | yes, the hand word is "vander" (verdict `persists_htr_ocr` is now stale: by reading it would be `fixed`) |
  | 188 | found none to exact (ratio 1.0): "torsijne narichtinge. daed in behoorlijcke forma" | 15j row sort: the print lines are in reading order, the string is now contiguous (verdict `needs_realignment` is stale: by reading `fixed`) | yes |
  | 8, 275 | `print_n_candidate_units` 59 to 58, 6 to 5 | 15k/15j unit changes (counts only) | neutral |
  | 250 | candidates 90 to 18, scope `scan_units` to `paragraph_units` | 15k crosswalk/unit change (a paragraph unit now matches) | neutral |

  (The 8 rows: 66, 88, 290, 23, 188, 8, 275, 250.)
- *The 20 texts:* 

  | Cause | better | worse | neutral | Example (verbatim, shortened) |
  |---|---|---|---|---|
  | 15l `J` mark (455) | 13 | 0 | 0 | "den tienJden deser" to "den tienden deser" (455_0272) |
  | 15j order (455, 456) | 6 | 0 | 1 | 456_0472 "toeko-" / "nabnoten ona van" / "men. ae ohan g-" now "toeko-" + "men. ae ohan g-" ("toekomen."); 456_0130 "Cel." + "derlandt." (was "Cel." + "een"); 455_0195 "Utrecht ... voor-" + "gedragen" |
  | 15k units | 0 | 0 | 0 | none in the sample |

  The neutral one is 455_0313: a piece of text ("Keyserlijcke Majesteyt. Waer op gedelibereert") moved to the front;
  whether the new order is right needs the scan. 455_0195 trades "Gedeputeerdete | Utrecht" for "voor- | gedragen";
  the 3-line region is probably an interleaved column fragment and neither order is certain.
- *Conclusion:* every change in the sampled texts and rows is either the removed `J` mark or a reading-order repair, and
  none is worse. Mid-line `J`s that are not at a merge point stay ("twee Jduysendt", "Jexaminer"; the mark rule only acts
  at merges). Of the 1,640 changed units 1,361 + 253 are print (15j order and the `J` mark) and 26 hand (commas). The audit
  and the 116-row numbers do not move, because the 116-row verdicts are static, read in 15i.

**Findings:** F09-15m.1 - F09-15m.7.

**Open issues.** The 116-row verdicts of rows 23 and 188 are stale (see above); relabelling is the phase review's call.
455's lower-case share is 0.9477, 0.002 below the target. UA-015 (staging) and UA-007 (0.9.1) are open.

### Step 15n: hand-off script for the full text_layer_v3 run

*Post-review addendum (2026-10-22): Sonnet implemented it and Opus reviewed it in the same session.* The local disk cannot hold all SG PageXML
(UA-015, UA-016), so the user runs the full build with inputs and output on an external drive. The script links the
PageXML of several source dirs into one local input dir (the first source wins), then checks:
- the dirs and subset files;
- the REPUBLIC coverage;
- the free space against the predicted output.

It then runs `--estimate` and the build through `coreshare`, and finally the audit. It adds nothing to the
library; the build's `--republic-dir` help no longer names LotA's dir, a default that 15k removed.

**Acceptance** (local pilot sources):
- `--check-only` finds hand 14, print 2, REPUBLIC 2/2;
- `--limit 1` exits 0;
- the full run `15n-a` equals `15m-a` but for source paths (F09-15n.1).

**Review.**
- The prediction was rewritten (F09-15n.2).
- `scripts/run_text_layer_v3_full.py` was un-ignored in `.gitignore`.
- The 11 failing tests were traced to the disk clean-up, not to this step, and now skip (F09-15n.4).

**Findings:** F09-15n.1 - F09-15n.4.

### Step 15o: v3 pilot publication

The user authorised publication of the v3 code's output on the local pilot inventories (UA-015 a, restricted). The
dev layer `15n-a` was copied (`cp -R`) to `shared-data/NL-HaNA_1.01.02/text_layer_v3/` (hand, 14 inventories, 305 MB) and
`shared-data/NL-HaNA_1.10.94/text_layer_v3/` (print, 455 and 456, 84 MB), without `progress.log` and `status.tsv`, and made
group-writable. `publish_manifest.py` added a `publication` key to both manifests (producer, AS 0.14.0 at `fa5cc63`,
audit summary, counts, the 116-row prediction, known gaps, schema pointer, sources with sha256 or file count; print also
the REPUBLIC paragraph files with sha256). The shared-data README (2 rows, 1 changelog line), LotA
`docs/from-archival-structures.md` (§9 and a pointer in §8), AS `docs/text_layer.md` and
`docs/from-language-of-the-archive.md` (§7 *Done* line) were edited. No library code changed.

**Acceptance.**

| # | Criterion | Result |
|---|---|---|
| 1 | Both copies exist, no `progress.log`/`status.tsv`, group-writable | pass (checked with `find ! -perm -g+w`: none) |
| 2 | Published audit 178/4, same 4 invariants, `counts` = `15n-a` | pass: 178 pass, 4 fail (3351 0.9415, 3823 0.9414, 455 0.9477, 456 0.932); `invariants`, `counts`, `word_break_stat`, `corpus_default` equal `15n-a/audit.json`; re-run after the manifest edit gives the same |
| 3 | `publication` in both manifests with all fields; 8 hashes match v2 | pass (the script exits on a mismatch; none) |
| 4 | Protected hashes unchanged | pass: 880 files, `diff` empty |
| 5 | README, LotA §9 and §8 pointer, AS docs | written |

**Error analysis.** Read from the published copies. `join/units`, 3349, first 3 rows (scan 0004, `index` units):
"Mot hey Ryk Actws na ha Hee Rym Hohnarms de 25 Nvembe 1619 Penig ontvangen den 20 Januarij 1840." (handwriting
recognition noise, as in v2); "Heren"; "Aalberts 321.". 455, first 3 rows (scan 0003, a title page): "LINULA"
(`running_header`), "der Resolutien van de" (`main_text`), "HOOOI MOCICC ICCTCIDACCH" (`running_header`): decorative
type, so OCR junk; nothing to read for order. Because those rows say little about reading order, I also read the first
main-text units of scan 0100. 3349: "te visiteren, examineren, ende van alles alhier ter vergaderinge rapport te doen." then
"Is ter vergaderinge gelesen de requeste van Herman van Quickelenburgh, ..."; a readable resolution in order. 455 (13 main
units): "strcteurs tot Maestricht, of die van den Brabantschen Gereghte aldaer te gelasten en tebevelen, ...", then "0Is ter
Vergaderinge gelesen de Requeste van MriCoenraadt van Heussen, Bailliuw der vrye Heerlijckheden ...", then a unit from the
second column ("en naer Sluys zeylende, geplundert, ...") and one from the third ("in sijne qualiteyt tegens die twee
Gevangenen sou-cie van de hebben te procederen, ..."). Each unit stays inside one column; none shows a line from a
neighbouring column. Residual OCR noise remains ("0Is", "MriCoenraadt", "sou-cie van de"), as expected from the source.
The `doc_id_map` of 3349 for the first `hand_doc_id` of `rows116_15m-a.tsv` for inventory 3349
(`..._3349_0076-text_region-3487-2074-1842-998`): `link` `same_id`, `old_status` `identical`, 11 segments, 454 characters;
the next one (`..._0133-text_region-3031-134-1505-1751`) is `trimmed` and has a `part` row to a second unit
(`..._0133-text_region-4325-134-95-151`, 1 segment, 3 characters). The 02d id format of the brief was not used: the column
is `hand_doc_id`, whose values are region ids, not 02d numbers.

**Findings:** F09-15o.1 - F09-15o.3.

**Review (Opus 5.5, 2026-10-23).** Accepted. Checked the published `publication` keys, the README rows and LotA §9 against
`audit_published.json`. One fix in review: the brief asked for `built_from` kind `dir`, but the v2 manifests use
`directory`; the 8 hand directory rows in `shared-data/NL-HaNA_1.01.02/text_layer_v3/manifest.json` and
`publish_manifest.py` now say `directory` (tables untouched). F09-15o.2 holds for all 8 directory inventories (3771-3773,
3821-3823, 11182, 11281), not 6.

## Review

Phase review (Opus, 2026-10-22). The code review is in
[`REVIEW.md`](../plan/phase-09-text-layer-v3-prep/REVIEW.md). Verdict: **accept with follow-ups**. There are
no code defects. The one doc defect, the UA-015 staging command, was fixed and tested in review. No
review-fixes step.

**Re-run** (F09-R.1):
- AS suite 1,205 passed, NLP 66; v1 `TOTAL 157 / 157` (`09R-v1check`);
- `compare.py 15l-a 15m-a`: 0 of 288 tables differ;
- `order.py`, `pn_offchain.py`, `merges.py` and the crosswalk on the step layers reproduce every
  logged number. The one exception is the 30 `unmatched` crosswalk rows in the 15m table, which don't
  reproduce (F09-R.2);
- audit 178 / 182 with the same four lower-case shares below 0.95.

**Leakage and circularity.** Nothing is fitted. Each bar was fixed in a brief before its sample was
read, and verdict files hold every row. The 455 lower-case share is not independent evidence for the `J`
rule, because the audit counts what the rule removes (F09-R.7). The reading is the evidence: 30/30 in
15l, 13/13 in 15m, and the token list over all 2,172 strips (F09-R.4).

**Own error analysis.**
- *Rows 23 and 188* of the 116 now read as fixed (F09-R.3). The verdict file is not edited; LotA
  re-checks after re-alignment (D19).
- *Off-chain verdicts:* I read all 40 hand-band rows; the verdicts follow the context columns. In
  hand-bottom all 12 decidable rows are not page numbers (F09-R.5).
- *15j remaining swaps* (4 read) are header rows and margin numbers, as the step classified them.
- *UA-015:* the staging command was wrong and is replaced by a `tar -s` loop, tested by a build on 11182
  (F09-R.6).

All step findings are confirmed. Two need a qualifier:
- F09-15m.3: the 116-row share is unchanged only because the verdicts are static (F09-R.3).
- F09-15k.1: hand-bottom missed its bar on unclear rows, not on page numbers (F09-R.5).

**Candidates for `docs/findings.md`** (not edited):
- F09-P.4 / F09-15l.1: a repeated hyphen OCR'd as `J` in one print volume only;
- F09-15l.5: a repeated break mark read as a capital letter defeats an upper-case join guard;
- F09-15k.2: off-chain top-band "page numbers" are mostly misreads of the page's own number.

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| UA-015 | Stage the inputs (hand tars, print PageXML, REPUBLIC paragraphs as a `shared-data` dataset), then build `text_layer_v3` with the run kit and publish it | `docs/USER_ACTIONS.md` UA-015; `plan/CONTEXT.md` §4 |
| F09-R.5 | Hand-bottom off-chain numbers: verdicts from crops for the 8 `unclear` rows (no chain nearby), then re-apply the bar of 16 | `plan/CONTEXT.md` §4 |
| REVIEW finding 6 | `_pn_candidate`: pick the band number that fits the chain (the 188 gap rows lost the slot to "4767"-like numbers) | `plan/CONTEXT.md` §4 |
| F09-15l.5 | A mark rule for capitals (repeated break mark read as "P", "Y", "V"), then re-test `g_upper_right` against the same bar | `plan/CONTEXT.md` §4 |
| F09-15j.4 | 26 same-row swaps left (header rows, numeric cells); no action unless they matter downstream | report only |
| F09-15m (brief defect) | Throughput fit for the SG presets (`--estimate` gives no time); fit it from the full run | `plan/CONTEXT.md` §4 |
| F09-15n.3 | A full-run audit (the step-15m audit is pilot-specific): invariants without fixed inventory lists | `plan/CONTEXT.md` §4 |
| UA-007 | pagexml-tools 0.9.1 pin and dropping the 0.9.0 workarounds | `docs/USER_ACTIONS.md` |
| F09-R.3 | Rows 23 and 188 read as fixed; the ≥ 80% check (UA-001) stays with LotA after re-alignment | LotA (D19) |

## Reproduce

Planning, from the AS root (`P=reports/data/phase-09-text-layer-v3-prep/planning`, `L=data/text_layer_v2_dev/15ir-a`):

```
.venv/bin/python -I $P/order.py --layer $L --simulate --out $P/order_15ir-a_print.json
.venv/bin/python -I $P/bridge.py --layer $L --out $P/bridge_15ir-a.tsv
.venv/bin/python -I $P/pn_offchain.py --layer $L --sample 20 --seed 41 --out $P/pn_sample_planning.tsv --json $P/pn_15ir-a.json
.venv/bin/python -I $P/merges.py --layer $L --json $P/merges_15ir-a.json --sample-out $P/merges_sample_planning.tsv
.venv/bin/python -I $P/crosswalk.py --layer $L --json $P/crosswalk_15ir-a.json --sample-out $P/crosswalk_sample_planning.tsv
```

Step 15j, from the AS root (`O=data/text_layer_v2_dev`, `P=reports/data/phase-09-text-layer-v3-prep`):

```
.venv/bin/python -I $P/planning/order.py --layer $O/15j-a --sub print --inv 455,456
.venv/bin/python -I $P/planning/compare.py $O/15ir-a $O/15j-a
.venv/bin/python -I $P/step-15j/order_diff.py --old $O/15ir-a --new $O/15j-a --out $P/step-15j
```

Step 15k, from the AS root (`P=reports/data/phase-09-text-layer-v3-prep`):

```
.venv/bin/python -I $P/planning/pn_offchain.py --layer data/text_layer_v2_dev/15j-a --json $P/step-15k/pn_15j-a.json
.venv/bin/python -I $P/step-15k/tally.py data/text_layer_v2_dev/15j-a data/text_layer_v2_dev/15k-a
.venv/bin/python -I $P/planning/compare.py data/text_layer_v2_dev/15j-a data/text_layer_v2_dev/15k-a
.venv/bin/python -I $P/step-15k/sample_retyped.py
.venv/bin/python -I $P/planning/crosswalk.py --layer data/text_layer_v2_dev/15k-a --sample-out $P/step-15k/crosswalk_sample.tsv --seed 61
```

Step 15l, from the AS root (`P=reports/data/phase-09-text-layer-v3-prep`):

```
.venv/bin/python -I $P/planning/merges.py --layer data/text_layer_v2_dev/15k-a --json $P/step-15l/merges_15k-a.json --sample-out $P/step-15l/merges_verdicts.tsv --sample 40 --seed 51
.venv/bin/python -I $P/planning/merges.py --layer data/text_layer_v2_dev/15l-a --json $P/step-15l/merges_15l-a.json
.venv/bin/python -I $P/planning/compare.py data/text_layer_v2_dev/15k-a data/text_layer_v2_dev/15l-a --out $P/step-15l/compare_15k-a_15l-a.json
```

Step 15m, from the AS root (`S=reports/data/phase-09-text-layer-v3-prep/step-15m`):

```
# builds: see the brief (15m-a, presets sg_1.01.02_v3 / sg_1.10.94_v3), then for L in 15ir-a 15m-a:
.venv/bin/python -I $S/audit.py --layer data/text_layer_v2_dev/$L --out $S/audit_$L.json
.venv/bin/python -I $S/rows116.py --layer data/text_layer_v2_dev/$L --out $S/rows116_$L.tsv   # needs rows116_verdicts.tsv beside it
.venv/bin/python -I $P/order.py --layer data/text_layer_v2_dev/$L --out $S/order_$L.json       # order.py has --out, not --json
.venv/bin/python -I $P/pn_offchain.py|merges.py|crosswalk.py --layer data/text_layer_v2_dev/$L --json $S/<name>_$L.json
```
