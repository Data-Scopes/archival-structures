# Phase 13 report: Register and index volumes (text layer v5)

> **Status:** final (reviewed 2026-10-28).
> **Phase plan:** [`plan/phase-13-register-index-volumes/PLAN.md`](../plan/phase-13-register-index-volumes/PLAN.md).
> **Code state:** AS unreleased after v0.17.0 (v0.18.0 recommended); no sibling changes.
> **Last updated:** 2026-10-28, by the final phase review (Opus 5.5).

## Summary

Text layer v5 of 1.01.02 cuts hand units on index pages at the entries. It is v4 plus two rules that passed bars fixed in
advance: an index-side detector that reads only the PageXML (two columns and entry shape, gap fill 8; GT test lines P 0.9711,
R 0.9490; 54/60 read sides) and an index entry rule (outdent, date, page reference, short line; fresh gold P 0.9530, R 0.8974,
F1 0.9244 against 0.5009 for v4 hand units). Three rules failed their bars and stay out: the column split (B1 0.763), the
first entry rule (E5) and the column reorder (B2 35/39). The dev build of the 231 local volumes equals v4 in all print
volumes and everywhere upstream of the units in the hand volumes; `main_text` units on the 8,873 flagged sides double and
interleaved units fall from 2,612 to 728. Detector false positives (rosters, accounts, letters) re-cut 0.18% of the hand units
without losing text. The full run (UA-045) and a pilot (UA-046) wait for the user.

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F13-P.1 | Index pages are page sides with two entry columns: in the 125 local hand volumes, 39–91% of the measured sides of the index volumes and 2–9% of the *tweede serie* half-year registers (their front section, e.g. 3471 scans 0003–0056) have a column-start share ≥ 0.3; the minuten (46, 4445, 3991, …) are two-column too (12–64%) but have 0–1 sides with entry shape (`max(end_num, date_start) ≥ 0.15`) where the index volumes have 58/66–123/125 | data-property | sg, index, layout | confirmed | [Planning](#planning) |
| F13-P.2 | The inventory GT (`inventory_metadata-NL-HaNA_1.01.02.json`) has `index_page` sections in 485 of 674 inventories; 42 local hand volumes are in it, 19 with an index section (1,437 labelled index sides of 48,418) | data-property | sg, ground-truth, index | confirmed | [Planning](#planning) |
| F13-P.3 | In v4, 2,612 of 399,714 main_text units (0.65%) of the 125 local hand volumes interleave two columns (phase-12 metric); the causes read at planning are index columns in one PageXML region, a margin summary column beside the main text in one region (3471 body scans), bilingual insets (3192) | open-issue | sg, layout, hand, line-order | confirmed | [Planning](#planning) |
| F13-P.4 | A geometric split of hand regions (largest gap between line starts ≥ 0.25 of the region width, ≥ 3 lines per side, side-by-side overlap) splits 165 of 5,857 regions in 3471 and 0–113 in the other volumes tried; fragments of one written line in two boxes (3222_0031) look like columns and are removed by a span guard (span lines ≤ 1/3) | method | sg, layout, hand | confirmed | [Planning](#planning) |
| F13-P.5 | A side rule `col_share ≥ 0.25 and max(end_num, date_start) ≥ 0.15` with a contiguity fill of 8 sides reaches line-weighted P 0.9808 and R 0.9297 on the 22 GT fit volumes (V05 of a 12-variant grid); the planner saw pooled in-sample numbers over all 42 GT volumes before drawing the fit/test split, then fixed the grid | method | sg, index, detector | confirmed | [Planning](#planning) |
| F13-P.6 | On index pages the v4 types stage types lemma headings as `marginalia` and date-only entry lines as `date_header`, so a dated entry starts with a separate `date_header` unit (3579_0173: 8 `date_header` units, 16 `hand:region` main units) | data-property | sg, types, index | confirmed | [Planning](#planning) |
| F13-19a.1 | Reading 60 sides from text alone gave 28 `index` and 32 `not index` (0 `unclear`, 0 `unreadable`): S1 index volumes 20/20 index, S2 *tweede serie* front scans 7/20, S3h hard hand volumes 1/10, S3a other hand volumes 0/10 | result | sg, index, gold | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.2 | The 13 non-index S2 sides are 10 minuten with marginal lemma words, 2 resolution texts and 1 letter; the only S3h index side (46_0515) is an alphabetical repertory of the 17th-century volume 46, the other 9 S3h sides are running text or minuten | data-property | sg, index, minuten | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.3 | The 30 entry sides show two entry-start styles that follow the groups: 16 of 16 G1+G2 sides are page-reference indexes (an entry ends in page numbers, the next entry starts after them), 9 of 9 G3 sides and 2 of 5 G4 sides start with a date, 2 G4 sides put the date at the end of the entry and 1 G4 side is a page-reference index | data-property | sg, index, entry-start | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.4 | Over the 671 `S` lines (hand tally), 238 (35.5%) start with a date, 348 (51.9%) follow a line ending in a number without starting with a date, 44 (6.6%) are only recognisable by starting further left than the line above, and 41 (6.1%) have none of the three cues | result | sg, index, entry-start | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.5 | The gold has 1,590 lines on 30 index sides (dev 10 sides / 492 lines, test 20 / 1,098): 671 `S`, 810 `C`, 100 `H`, 9 `?` (0.57%); the mean entry is 2.21 lines (G1 1.49, G2 2.52, G3 2.53, G4 2.78) and all 8 reserves were left unused | result | sg, index, gold | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.6 | Gutter merges are rare but uneven: 21 lines (1.3% of 1,590) in 7 of 30 sides, 0.70 per side, 8 of them in 3630_0031 and 6 in 3459_0036, where the two columns of a register page share one text line | data-property | sg, layout, index | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19a.7 | Layout damage that no entry rule can see: orphan reference lists at the page top (3107_0046), date columns on separate lines (12297_0019), stray marginal digits and words, and entries whose first line was lost to HTR (3522_0010 col 2) give 100 `H` lines (6.3%) and a handful of unlabellable fragments | open-issue | sg, index, htr | confirmed | [Step 19a](#step-19a-gold-reads-index-sides-and-entry-starts) |
| F13-19b.1 | The geometric hand column split lowers hand interleave from 2,612 to 980 of the main_text units (0.65% to 0.24%) but adds 10,637 units (+2.7%) because 2,760 regions become 16,134 pieces | result | sg, layout, hand, line-order | confirmed | [Step 19b](#step-19b-hand-column-split) |
| F13-19b.2 | Bar B1 fails: 29 right and 9 wrong of 38 judged (0.763 < 0.90) and 4 wrong among the 20 not-index-like cases (> 2); UA-040 decides whether v5 ships the split | negative-result | sg, layout, hand | confirmed | [Step 19b](#step-19b-hand-column-split) |
| F13-19b.3 | The wrong cases come from extent-based span classification (wide ordinary lines reach the other column and strand 1-line pieces: 5 cases) and from piece order after the layout column grouping (4 cases) | open-issue | sg, layout, hand | confirmed | [Step 19b](#step-19b-hand-column-split) |
| F13-19b.4 | Of 10 remaining interleaved units 5 are real column regions kept whole by a guard (gap with an inflated W, min_lines, median width, overlap, span) and 5 are metric false positives (indent, amounts, tables); none is only a gutter merge | data-property | sg, layout, hand | confirmed | [Step 19b](#step-19b-hand-column-split) |
| F13-19b.5 | The split yields 1,418 split regions on index-like sides and 1,342 elsewhere (minuten, margin-column body pages); margin pieces come before the main text in 7 of 12 read margin cases and after it in 2, decided by the layout column grouping | data-property | sg, layout, hand | confirmed | [Step 19b](#step-19b-hand-column-split) |
| F13-19c.1 | The detector grid reproduces on the layout tables of the v4 hand clones: 115,274 feature rows equal the planning table, the 12 grid lines are identical, V05 (`col_share >= 0.25`, shape >= 0.15, gap 8, min_run 1) is selected | method | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19c.2 | Bar D1 passes on the 20 GT test volumes: lines P 0.9711 and R 0.9490 (sides tp 675, fp 12, fn 64); fit was P 0.9808, R 0.9297 | result | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19c.3 | Bar D2 passes on the bar itself: 54 of 60 read sides right (0.9000, bar 0.90), 1 false positive in S3h + S3a (bar 2); the 5 false negatives are S1 index sides (short or garbled HTR, or dates the pattern misses) | result | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19c.4 | False positives are a name roster with page references (4831: 2 rule sides, 8 more by gap fill) and account pages with amounts (3320): the gap fill of 8 turns a 2-side error into 10 | open-issue | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19c.5 | False negatives are one-column-per-side indexes (3127: 8 of 20 sampled; 4617 style: 3), sparse sides with col_share unmeasured (7) and two GT-labelled blank or title pages; the shape-only arm (V07-V12) would catch many at 3-6 points of fit P and was not selected | open-issue | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19c.6 | In the 125 hand volumes the detector flags 18 volumes in 3349-3600 by more than half of their sides (index volumes) and the front section of 24 registers (first flagged scan 2-6, front run ends at scan 22-61); four garbled index volumes (3498, 3520, 3523, 3525) have 15 of 830 sides flagged | data-property | sg, index, detector | confirmed | [Step 19c](#step-19c-index-side-detector) |
| F13-19d.1 | Chosen on 10 dev sides, E5 (the print rule, `indent` + `short_prev`, on flagged sides) has the best F1 (0.833; E1-E4 0.770, 0.814, 0.816, 0.820); the v4 hand units reach P 0.925, R 0.657, F1 0.768 | result | sg, index, units | confirmed | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-19d.2 | Entry bar fails on the 20 test sides: E5 P 0.8683, R 0.7092, F1 0.7807 (bar P >= 0.90, R >= 0.85); F1 beats v4 hand (P 0.9741, R 0.4502, F1 0.6158) but precision falls 0.97 to 0.87 and recall is still short; UA-041 decides | negative-result | sg, index, units | confirmed | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-19d.3 | On the 4 test sides of the pinned print volumes E5 reaches F1 0.810 (P 0.865, R 0.762) against the v4 print build's 0.872 (P 0.944, R 0.810) and v4 hand's 0.680: the rule closes about two thirds of the hand-print gap there | result | sg, index, units | confirmed | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-19d.4 | Of 40 read test errors (200 in all: 54 FP, 108 FN, 38 FN without a main unit), 26 sit where two columns are interleaved in one layout region and the previous main line in region order is on the other column, 11 are entry lines typed `marginalia`/`other`, 2 have only a reference at the end of the line above, 1 is a gutter-merged line; none is a hanging-indent subtlety. *Revised by the review:* 14 of the 26 sit in one-column layout regions (cue failures), so 12 of 40 are interleaving; the 11 no-unit lines are 10 typed `index` (v4 `rule:index_page`, 3349_0037) and 1 `marginalia`; over all 200 errors 58 are in mixed regions (F13-R.1, F13-R.5) | open-issue | sg, index, units, layout | revised | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-19d.5 | E5 raises main_text units on flagged sides from 80,344 to 133,218 (+65.8%: `a` 55,505 to 91,531, `z` 24,839 to 41,687) on 8,873 flagged sides; the 676,744 units on unflagged pages are identical (rows, segment rows, unit order) | behaviour-change | sg, index, units | confirmed | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-19d.6 | Dev picks differ from group fit: E5 wins G1 and G3 on dev, but on test its recall is 0.64-0.77 in every group (G1 0.695, G2 0.644, G3 0.766, G4 0.696), so the entry cues do not transfer through the interleaved region order whatever the index style. *Revised by the review:* the group recalls hold, the cause does not: G3 has 19 of its 411 test gold lines in a mixed region (3466_0138) and loses recall on date starts E5 has no cue for, G1 on one-line entries after a page reference (F13-R.2) | result | sg, index, units | revised | [Step 19d](#step-19d-index-entry-unit-rule) |
| F13-R.1 | 19d's error categories overstate interleaving: 14 of the 26 "interleave" verdicts are in one-column layout regions; over all 200 E5 test errors 58 (29%) are in regions mixing two columns, 38 (19%) in no main unit and 104 (52%) in one-column regions | method | sg, index, units, error-analysis | confirmed | [Review](#review) |
| F13-R.2 | E5's shortfall is its cue set: in one-column regions it reaches P 0.915, R 0.734; 42 of its 75 FN there follow a line ending in a number and 24 start with a date (cues E5 lacks), and 21 of its 29 FP are `indent` firing on hanging-indent continuation lines | negative-result | sg, index, units | confirmed | [Review](#review) |
| F13-R.3 | A column fix alone cannot lift E5 over the entry bar: with every S line of the mixed regions found R ≤ 0.775 (389/502); a geometric re-split gives P 0.903, R 0.723 | result | sg, index, units, layout | confirmed | [Review](#review) |
| F13-R.4 | The dev choice of E5 was within noise (E3–E5 dev F1 0.816–0.833 on 10 sides); post hoc on test E3 and E4 reach F1 0.859; with a no-cut guard before number-only lines and the columns re-ordered, E3 reaches P 0.940, R 0.882 pooled over the 30 spent gold sides (diagnostic, not a bar) | result | sg, index, units | confirmed | [Review](#review) |
| F13-R.5 | v4's types stage already types whole index pages `index` (`rule:index_page`): 963 sides, 17,845 segments in the 125 hand volumes, 408 sides / 8,763 segments on detector-flagged sides; those lines are in no main unit (30 of 19d's 38 no-unit FN) | data-property | sg, index, types | confirmed | [Review](#review) |
| F13-R.6 | D1 is robust to the planner's pooled look but rests on 10 informative test volumes: volume-bootstrap 90% interval P 0.917–1.000, R 0.895–0.980; test P/R flat (0.970–0.973 / 0.926–0.954) over the neighbouring thresholds | method | sg, index, detector | confirmed | [Review](#review) |
| F13-R.7 | B1 failed on the P6 rule, not the code: extent-based span lines and span pieces that bridge both column hulls in the page-level column grouping; a start-x rule qualifies all 40 B1 regions with 0–4 wide lines (9 in the bilingual case), but in 13 of 40 the right column starts higher, so the columns must stay in one region in sequence to keep the order | method | sg, layout, hand | confirmed | [Review](#review) |
| F13-R.8 | The 19a gold is consistent with its rules (10 of 810 `C` lines start with a date, 7 of them a documented date column), but entry labels come from one reader and have no independent check | method | sg, index, gold | confirmed | [Review](#review) |
| F13-19f.1 | Re-ordering the lines of two-column hand regions (start-x rule, one region kept) lowers hand interleave from 2,612 to 843 of the main_text units (0.65% to 0.21%, below 19b's 980) and adds no region: 3,066 regions in 108 of 125 volumes are re-ordered (1,696 on detector-flagged sides, 1,370 elsewhere); main_text units change by -285 (399,714 to 399,429, 31 inventories) | result | sg, layout, hand, line-order | confirmed | [Step 19f](#step-19f-column-order-v2) |
| F13-19f.2 | Bar B2 fails narrowly: 35 right, 4 wrong, 1 unclear of 40 read regions, right/(right+wrong) = 35/39 = 0.897 (< 0.90); wrong among the 20 `other` cases = 2 (bar <= 2, pass). Flagged 18/2/0, other 17/2/1. One case moved to `right` would give 0.90; UA-043 decides | negative-result | sg, layout, hand | confirmed | [Step 19f](#step-19f-column-order-v2) |
| F13-19f.3 | The 4 wrong cases have three causes: 2 are fragments of the same written lines that the metric and the rule take for columns (25: rows of left and right halves; 30: seven word fragments), 1 is a same-row continuation sent to the right run (16), 1 is a centred heading pulled into the right run because the largest gap fell inside the heading (10) | open-issue | sg, layout, hand | confirmed | [Step 19f](#step-19f-column-order-v2) |
| F13-19f.4 | In 1,340 of 3,066 re-ordered regions (43.7%) the right text starts above the left text, so left-then-right is a choice there; 3,004 (98%) hold at least one wide line (`W`, 4,833 of 73,777 lines = 6.6%), mostly the first heading or a gutter merge, and 388 hold three or more; a `W` line carries a half of the other column's text (cases 22, 28, 33, 35-39), so the right run alone is not always continuous | data-property | sg, layout, hand | confirmed | [Step 19f](#step-19f-column-order-v2) |
| F13-19f.5 | Of 10 remaining interleaved units, 3 sit in regions kept whole by the wide-line guard (`wide > 0.34`, gutter-merge heavy: a margin column merged into the body lines), 3 by a size guard (`min_lines`/narrow candidates, 4-6 lines), 3 by the median-width guard on the right side, and 1 is a three-cluster page (wide lines, an inner indented column, a right column) where only the rightmost gap is used | data-property | sg, layout, hand | confirmed | [Step 19f](#step-19f-column-order-v2) |
| F13-19g.1 | Entry bar v2 passes on 20 fresh index sides (868 lines, read before any code ran): X4 (outdent, date_start, end_ref_prev, short_prev; no-cut guard; `index`-typed lines as mains) has P 0.9530, R 0.8974, F1 0.9244 against v4 hand F1 0.5009 (R 0.336), E5 0.6277 and, on the 2 print sides, v4 print 0.857 | result | sg, hand, index | confirmed | [Step 19g](#step-19g-index-entry-rule-v2) |
| F13-19g.2 | Selection on the 30 spent sides: X1-X4 F1 0.8795, 0.9027, 0.8805, 0.9040. Adding `index` to the mains is the larger gain (no-unit FN 45 to 15, R +0.04); `short_prev` trades P (-0.02 to -0.03) for R (+0.03) | result | sg, hand, index | confirmed | [Step 19g](#step-19g-index-entry-rule-v2) |
| F13-19g.3 | 31 of the 44 fresh FN are lines no entry rule reaches: all typed `marginalia` (15 in `extra`-role regions, 16 in `main`-role regions: one-line regions or lines inside a main region); of the 13 FN that are in a unit, 12 were read: 10 entries with no cue (letter references `Letter R.`, `1 dito`, OCR months `ang`), 1 in a two-column region and 1 start swallowed by the date_header scoring convention. *Revised by the review:* the 31 cluster in 4 volumes (3127 10 on two sides, 3265 8 on one side, 3280 5, 3283 2); the lines sit in `extra`-role column regions and one-line regions of fragmented sides, so the cause is upstream of the types stage too, and typing them as mains is not free: with `marginalia` added to `main_types` the spent fresh sides give P 0.890, R 0.977 (diagnostic, F13-R.12) | open-issue | sg, types, hand | revised | [Step 19g](#step-19g-index-entry-rule-v2) |
| F13-19g.4 | 19 fresh FP: 9 in regions that span both columns (merged lines, interleaved region order; checked in `layout/regions`), 5 `short_prev` cuts of a cross-reference line, 4 `end_ref_prev` cuts after an amount or day number (`f150`, `30000.`, `22:`), 1 `date_start` on a date that closes an entry | data-property | sg, layout, hand | confirmed | [Step 19g](#step-19g-index-entry-rule-v2) |
| F13-19g.5 | Units: main_text units on flagged sides double (80,344 to 162,021 in 125 inventories), `index` units on flagged sides go 2,789 to 0, and unflagged pages are identical in unit rows, segment rows and unit order (676,744 units) | behaviour-change | sg, hand, index | confirmed | [Step 19g](#step-19g-index-entry-rule-v2) |
| F13-19e.1 | The v5 dev build (231 volumes, 60 min, 0 failed rows) equals v4 in all 106 print volumes and, in the 125 hand volumes, in every file except `units/`, `join/` and `wbc/parts` (86 volumes with flagged sides); `units/` equals the 19g clones for all 125; `layout/pages` equals the 19c clones | result | sg, layout, units | confirmed | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-19e.2 | End to end with the production units: 19a sides P 0.8768 R 0.9329 (30 sides), 19g sides P 0.9530 R 0.8974 (20 sides), the numbers of the clones; 8,873 of 115,274 sides are flagged, 41% of them by gap fill; `main_text` units on flagged sides 80,344 to 162,021 | result | sg, hand, index | confirmed | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-19e.3 | Of 10 v5 units on flagged sides of unseen volumes, 3 are one entry, 3 two entries, 3 part of an entry and 1 a heading (none a non-index); the two-entry and part units come from cuts missed after a reference or amount and from `marginalia`-typed entry starts, not from new causes | data-property | sg, hand, index | confirmed | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-19e.4 | Silver check on 27 hand volumes: 18 of 34,428 resolution-page sides are flagged (0.05%; 8 by rule, 10 by gap fill), in 4831, 3315, 3320, 3307; the 5 read are regiment rosters and account pages whose lines end in numbers, like index entries | result | sg, hand, index, silver | confirmed | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-19e.5 | Interleaved `main_text` units in the hand volumes fall from 2,612 of 399,714 (0.65%) to 728 of 481,391 (0.15%) in v5 (19f's reorder: 843 of 399,429); the 728 left were not analysed. *Revised by the review:* the share falls partly because the denominator grows by 81,677 units; the comparable count is 2,612 → 728, and 722 of the 728 are on unflagged sides (unchanged from v4), so on flagged sides interleaving goes 1,890 → 6 (review, *Remaining interleave*) | behaviour-change | sg, hand, layout | revised | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-19e.6 | Detector false positives cost units but no text: 4831's roster pages (14 to 358 units on 10 sides), a garbled scan of 11930 (27 junk units) and account pages are cut at entry cues; in 5 gap-filled units read, 1 was on such a page. *Revised by the review:* quantified over every flagged side known not to be an index page: 54 sides in 18 of 125 volumes (also letters, running text, account series 8706, 3199, 11182), `main_text` units 228 → 1,091 plus 207 v4 `index` units re-cut, 0.18% of the hand `main_text` units (F13-R.13) | open-issue | sg, hand, index | revised | [Step 19e](#step-19e-v5-preset-dev-build-audit-and-run-kit) |
| F13-R.9 | 19g's fresh test is sound: the 20 sides were labelled and hashed (11:02 UTC) before any code changed or ran on them (first code edit 11:02:36, one fresh run 11:04:00, from the session transcript), its 19 volumes share none with the 30 spent volumes, the reserves follow the brief, and the logged P/R recompute from the production units with 0 of 868 cells different; the entry labels have no human check | method | sg, index, units, gold | confirmed | [Review](#review) |
| F13-R.10 | B2's failure is a fair negative: the `unclear` case that would lift the ratio to 0.900 (23) is not `right` under the rubric (the text opens at the top of the right column), and 2 `right` calls (22, 07) pass only because a `W` line may carry the other column's text; the two failure kinds need a row-pairing guard (halves of written lines, a minimum line width for word fragments) and a split x taken from the band where both columns overlap (a centred heading, a same-row continuation) | negative-result | sg, layout, hand | confirmed | [Review](#review) |
| F13-R.11 | The entry rule does not depend on the column reorder: on the spent fresh sides X4 on the 19f layout gives P 0.9552, R 0.8951, F1 0.9242 against 0.9244 on the v4 layout (2 sides change by one line); 7 of the 9 "two-column region" FP sit in wide regions of 3–8 lines that the reorder's guards keep whole (diagnostic, no bar) | result | sg, index, units, layout | confirmed | [Review](#review) |
| F13-R.12 | The 31 no-unit FN of X4 cluster in 4 volumes (25 of 31: 3127, 3265, 3280, 3283), in `extra`-role column regions and one-line regions typed `marginalia`; adding `marginalia` to `main_types` lifts fresh R to 0.977 but drops P to 0.890 (FP 19 to 52) and selection P to 0.872, so it needs its own design and fresh gold (diagnostic) | open-issue | sg, index, units, types | confirmed | [Review](#review) |
| F13-R.13 | Detector false positives in the v5 dev build: 54 flagged sides are known not to be index pages (GT resolution pages, silver negatives, reads, and the flags of 11 non-index volumes read by the review), in 18 of the 125 hand volumes (30 by the side rule, 24 by gap fill): rosters (4831, 8287), accounts (8706, 3199, 3307, 3315, 3320, 11182), letters and running text (359, 695, 751, 882, 3221, 4170, 11220, 11281), garbled scans (11930, 3525). On them `main_text` units go 228 → 1,091, 207 v4 `index` units are re-cut into mains and 99 v4 units are split: 0.6% of the flagged sides, 0.18% of the hand `main_text` units; no text is lost or moved. Outside the registers the rate is about 1 flagged side per 1,000 sides (28 of 25,158 sides in the 37 volumes outside the GT volumes and the half-year registers 3349–3600) | open-issue | sg, hand, index, detector | confirmed | [Review](#review) |
| F13-R.14 | Gap fill is mostly right: of 20 gap-filled sides drawn from volumes outside the GT and the entry gold (seed 20261130; pool 658 sides in 17 volumes), 17 are index pages, 2 hold only a page number (one unit, no harm) and 1 is an account page (8706); it recovers one-column dated-summary register indexes (3463, 3472) whose sides the side rule cannot measure (`col_share` unmeasured or 0) | result | sg, index, detector | confirmed | [Review](#review) |
| F13-R.15 | The user's 3 `unsure` sides of the blind check (UA-039) are a definition question, not a disagreement: a dated summary index under a respect term without page numbers (17, 3712), the same in garbled HTR (23, 3498), and a 6-line side whose locators the HTR lost (54, 3489). D2 over the 57 sides the user judged: 52/57 = 0.912 (bar 0.90) | method | sg, index, gold | confirmed | [Review](#review) |
| F13-R.16 | The run kit's final audit was the v4 (15w) audit, so a full v5 run would have reported 0 judged failures without the v5 checks; fixed in the review by pointing `AUDIT` at the 19e copy (same checks for v4; 223 s vs 224 s on the 231-volume dev build) | bug-fixed | sg, audit, run-kit | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

**What was measured** (2026-10-28, Opus 5.5; scripts in
[`data/phase-13-register-index-volumes/planning/`](data/phase-13-register-index-volumes/planning/)). Side features of
all 115,274 page sides of the 125 local hand volumes of `15y-b`/`15z-b` (`draw_reads.py`, `side_features.tsv`); the GT
fit/test split (`gt_split.tsv`, seed 20261113); the read lists for step 19a (`reads_sides.tsv`, seed 20261114;
`reads_entries.tsv`, seed 20261115); the detector grid on the fit half (`detector_grid.py fit`); the v4 interleave
baseline (`interleave_v4_hand.tsv`); a prototype of the column split on raw PageXML regions
(`colsplit_prototype.py`).

**Index pages.** Two kinds of volume hold them: the index volumes (*tweede serie* 1703–1769 indexes, the hand-pinned
3433–3489, the secrete indexes 4617/4623/4792, the repertorium 3712) and the half-year registers, whose first 40–70
scans are an index of the half year. Entries hang (the first line about one line height left of its continuation), start
with a date in the chronological indexes (`17. April. Ry. voor den Considl de lon …`, 3579) and end in a page reference
in the register indexes (`Reqt:. P: Roos … Gerid. 41.`, 3471). Per-volume medians on the two-column sides: end-number
share 0.22–0.73 in register indexes, 0.01–0.05 in chronological indexes, whose date-start share is 0.24–0.37
(F13-P.1).

**Detector grid** (`detector_grid.py fit`, line-weighted over GT-labelled sides of the 22 fit volumes):

| Variant | shape-only arm | gap fill | min run | P | R |
|---|---|---|---|---|---|
| V01 | off | 0 | 1 | 0.9841 | 0.8383 |
| V03 | off | 4 | 1 | 0.9798 | 0.8835 |
| V05 | off | 8 | 1 | 0.9808 | 0.9297 |
| V06 | off | 8 | 3 | 0.9857 | 0.9197 |
| V09 | 0.3 | 4 | 1 | 0.9225 | 0.9663 |
| V11 | 0.3 | 8 | 1 | 0.9177 | 0.9860 |

The misses without gap fill are sparse index pages (fewer than 15 lines of ≥ 20 chars: 464 of the 1,437 index sides
over all 42 volumes) and single-column index books (4617, 4623, 4792). The test half was not computed (step 19c, bar D1).

**Decisions** P1–P14 are in the phase plan; the user settled the scope (per page side), the split scope (every hand
region), the gold (agent reads + a blind check) and the gutter merges (count only).

### Step 19a: gold reads (index sides and entry starts)

**What changed.** No library code. The step read 60 page sides (verdict `index` / `not index`) and, on the 30
planned entry sides, labelled every listed line `S` / `C` / `H` / `?`, before any detector or entry rule exists.
Every side was read by the session from the listing of `show_side.py` (text only, no stratum, split or
prediction shown); no label comes from a rule or script. `append_labels.py` only writes the typed labels to the TSV.
A blind check page lets the user check 30 of the 60 side labels against the scans (UA-039). Brief:
[`step-19a-gold-reads.md`](../plan/phase-13-register-index-volumes/step-19a-gold-reads.md). Files in
`reports/data/phase-13-register-index-volumes/step-19a/`: `side_verdicts.tsv`, `entry_gold.tsv`, `entry_sides.tsv`,
`s_start_reading.tsv` (hand tallies behind the start-cue table), `show_side.py`, `append_labels.py`,
`build_check_page.py`, `check_page.html`, `check_items.json`, `summary.py`, `unused_reserve_reads.tsv`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. `side_verdicts.tsv` | 60 rows, ids 1–60 once, 4 verdict values; flag if more than 10 `unclear`/`unreadable` | 60 rows; 28 `index`, 32 `not index`, 0 `unclear`, 0 `unreadable input` (D2 is not at risk from the gold) |
| 2. `entry_sides.tsv` | at least 28 `index` sides, at least 9 in `dev`, 18 in `test` | 30 `index` sides: 10 `dev`, 20 `test`; no reserve used |
| 3. `entry_gold.tsv` | one row per listed line, labels `S/C/H/?`, `n_S ≥ 1` per side | 1,590 rows = the sum of the 30 `n_lines`; minimum `n_S` per side 6 |
| 4. check page | 30 items (10/10/10), blind, links, copy buttons, JS syntax checked, UA written | 30 items (S1 10, S2 10, S3h+S3a 10; seed 20261116), no agent verdict in the HTML, `new Function` syntax check `syntax ok`, UA-039 |

Side verdicts per stratum (stratum read from `reads_sides.tsv` only after the verdicts were written):

| Stratum | index | not index | unclear | unreadable |
|---|---|---|---|---|
| S1 index volumes | 20 | 0 | 0 | 0 |
| S2 *tweede serie* scans ≤ 0100 | 7 | 13 | 0 | 0 |
| S3h other hand volumes (hard) | 1 | 9 | 0 | 0 |
| S3a other hand volumes (any) | 0 | 10 | 0 | 0 |
| all | 28 | 32 | 0 | 0 |

`kind` (leading phrase): register index 16, resolution text 15, minuten 11, chronological index 8, alphabetical
repertory 4, letter 4, list 1, treaty text 1.

Entry gold per group (G1 GT register index sections, G2 late register front sections, G3 index volumes, G4 other
index books):

| Group | Sides | Lines | S | C | H | ? | Mean entry (lines) |
|---|---|---|---|---|---|---|---|
| G1 | 8 | 362 | 225 | 110 | 24 | 3 | 1.49 |
| G2 | 8 | 382 | 141 | 215 | 23 | 3 | 2.52 |
| G3 | 9 | 624 | 233 | 357 | 33 | 1 | 2.53 |
| G4 | 5 | 222 | 72 | 128 | 20 | 2 | 2.78 |
| all | 30 | 1,590 | 671 | 810 | 100 | 9 | 2.21 |

By split: `dev` 10 sides, 492 lines (S 169, C 284, H 36, ? 3); `test` 20 sides, 1,098 lines (S 502, C 526, H 64, ? 6).
Every one of the 30 `read` sides was an index side, so none of the 8 reserves was used (see Deviations below).

How entries start (hand tally over the 671 `S` lines, from `s_start_reading.tsv`; the cascade takes a line to its first
matching cue):

| Cue | Lines | Share |
|---|---|---|
| starts with a date | 238 | 35.5% |
| previous line ends in a number (not date-start) | 348 | 51.9% |
| only: starts further left than the line above (about 25 px or more) | 44 | 6.6% |
| none of these | 41 | 6.1% |
| (previous line ends in a number, counted regardless of the date cue) | 377 | 56.2% |

Lines labelled `merged`: 21 (1.3% of 1,590) in 7 of 30 sides (0.70 per side): 3630_0031 8, 3459_0036 6,
3308_0006 2, 3522_0010 2, 3254_0009 1, 3428_0040 1, 3466_0138 1.

`summary.py` output (`.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19a/summary.py`):

```
== Side verdicts per stratum ==
stratum	index	not index	unclear	unreadable input	total
S1	20	0	0	0	20
S2	7	13	0	0	20
S3a	0	10	0	0	10
S3h	1	9	0	0	10
all	28	32	0	0	60
unclear + unreadable: 0 of 60

== Kind counts (text before first parenthesis or 'under') ==
 16  register index
 15  resolution text
 11  minuten
  8  chronological index
  4  alphabetical repertory
  4  letter
  1  list
  1  treaty text

== Entry sides ==
listed: {('dev', 'read'): 10, ('dev', 'reserve'): 4, ('test', 'read'): 20, ('test', 'reserve'): 4}
sides labelled: 30 | reserves used: 0
index sides per split: {'dev': 10, 'test': 20}
not-index entry sides: 0

== Entry gold per group ==
group	sides	lines	S	C	H	?	mean_entry_len(lines)
G1	8	362	225	110	24	3	1.49
G2	8	382	141	215	23	3	2.52
G3	9	624	233	357	33	1	2.53
G4	5	222	72	128	20	2	2.78
all	30	1590	671	810	100	9	2.21
share of ? : 0.566% | min n_S per side: 6
dev lines 492 {'H': 36, 'S': 169, 'C': 284, '?': 3}
test lines 1098 {'S': 502, 'C': 526, '?': 6, 'H': 64}

lines labelled merged: 21 in 7 of 30 sides; per side: {'6': 2, '9': 1, '11': 6, '13': 2, '16': 1, '17': 8, '26': 1}
merged per side (all sides): 0.70

== How entries start (hand tally over all S lines) ==
n_S                       671  100.0%
date_start                238  35.5%
prev_ends_number_any      377  56.2%
prev_ends_number_excl     348  51.9%
further_left_excl         44  6.6%
none_excl                 41  6.1%
(excl = exclusive cascade date > previous line ends in number > further left (dx <= -25 px) > none)
```

**Error analysis.**

- *What was read:* all 60 sides (listing by `read_id`, no stratum visible) and all 1,590 lines of the 30 entry sides,
  each side in full; nothing was sampled. Entry-start cues were tallied by reading the 671 `S` lines together with the
  tail of the line above (and the indentation difference as a reading aid).
- *Categories (hard cases with a decision):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Side verdict: structure without readable references | 1 | 3498_0027 (read 2): `Zelen`, `Vrieslan`, `Haet en leeden`, `7. fantnr. Resolibtie staet en lenden consent in den Saat„` ... marginal province words with dated consent entries; garbled HTR, called `index` (low confidence) |
  | Side verdict: dated list without page references | 1 | 3579_0038 (read 53): `19. Jann: de Hr. van Cals, wege. Holl: in de Admt. ...`, `7. Febr: W. Ryssen, als Schepen te Veulo.`; called `index` (dated one-line entries under the heading `Commissien`), the borderline of "list without a reference per entry" |
  | Side verdict: minuten with marginal lemma words | 11 | 3421_0070 (read 45): `Hoeuft` / `pasport` / `Admt. t'amsterdam` in the margin of `Opde requeste van Matthijs Hoeuft ... Is na voorgaende deliberatie goedgevonden ende verstaen`; called `not index` (running text) |
  | Entry line with two columns merged | 21 | 3459_0036 line 5: `de resolutie van den 5e. Decemb: op de reqte. van de Regenten van Hoog. Op de reqte van de provisoren van`; labelled `C` (first part), with entry starts hidden mid-line |
  | Orphan or stray fragment (notes: stray, orphan, marginal, date column, lost, catchword) | 30 | 3107_0046 line 1: `92: 9: 94: 93: 46: 17:` (reference lists above the first entry, owner unclear): labelled `C`; 12297_0019 lines 13, 15: `16 July 1722`, `11 Jan: 1757.` (date column on its own lines): labelled `C` |
  | Entry start without any cue | 41 | 3428_0040 line 2: `Vernieuwinge van het Plac, Advis vanden Raad van` (two entries merged across the gutter, no number or date before it) |

- *Conclusion:* the two start styles are separable by group, not by side type: the register and late-front indexes
  of G1 and G2 are ended by page numbers (the cue "previous line ends in a number" covers 301 of their 366 `S` lines,
  82%; the rest follow headings, `vide` cross-references or are only indented), the G3 index volumes are chronological
  summaries with a date at the start (all 9 sides), and G4 mixes both plus two sides with the date at the end. A single
  cue cannot cover the gold (date 35.5%, number 51.9%), but the pair covers 586 of 671 (87.3%) and indentation adds 6.6%.
  Gutter merges are few but they cluster in register pages where HTR joined the two columns into one text line
  (3630_0031, 3459_0036); for those pages a merged line hides an entry start, which the rule will count as a miss
  by phase decision. The `?` share is 0.57%, too small to move precision or recall.

**Findings:** F13-19a.1 to F13-19a.7.

**Deviations.** Two reserve sides (read_id 10 = 3303_0031 and 20 = 3438_0042) were read and labelled by mistake while
working through the read list in order. Their rows are not in `entry_gold.tsv` and not counted; they are kept in
`unused_reserve_reads.tsv` (85 lines, for the record only). They are not drawn into the gold because no `read` side
was replaced.

**Brief defects.** `show_side.py` and the brief's `n_lines` agree for all 60 + 30 sides, so no defect there. Gaps in
the brief that needed a decision: (1) "label by its first part" for gutter merges does not say what to do when the
first part is a heading tail followed by an entry start (3630_0031 line 2; labelled `S`); (2) an undated sub-entry
(`Reg: Weduwe Marten, om een half jaer tractement`) is not covered by "own date or own page reference"; labelled `S`
when it is a separate document; (3) the cue table of the error analysis does not say whether the cues are exclusive;
both views are given above.

**Open issues.** UA-039 (the blind check), which blocks step 19e only.

### Step 19b: hand column split

**What changed.** `LayoutParams.hand_column_split` (default `None`, left out of `to_dict` when `None`),
`HAND_COLUMN_SPLIT_DEFAULTS` and `page_structure.split_regions_into_columns`, called from `layout_scan` right after the
`split_on_line_field` block. 6 new tests (5 in `tests/test_analysis_page_structure.py`, 1 in `tests/test_text_layer_layout.py`);
suite 1,340 collected, 1,329 passed, 11 skipped. Nothing is published, no preset sets the field.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 47 of 47 in the two modules; full suite 1,329 passed, 11 skipped (1,334 + 6 collected) |
| 2 | off = unchanged | `layout/params.json` bytes and `layout/{scans,regions,lines}` of 3471, 3579, 46 equal to `15y-b` (decompressed) |
| 3 | interleave baseline | `15y-b`/`15z-b`: 399,714 units, 2,612 interleaved (0.6535%), per volume identical to `interleave_v4_hand.tsv`; with the split (`19b-a`, `19b-z`): 410,351 units, 980 interleaved (0.2388%) |
| 4 | **bar B1** | **fail**: right 29, wrong 9, unclear 1, unreadable 1 of 40; right/(right+wrong) = 29/38 = 0.763 (< 0.90); wrong among the 20 not-index-like = 4 (> 2). Judgeable (2 unclear/unreadable <= 8). UA-040 |
| 5 | duplicate `layout_region_id` per scan | 0 in the 125 rebuilt inventories |
| 6 | protected lists | `protect_before/after.txt` (125,008 files) and `protect2_before/after.txt` (50,900 files) identical; no `shared-data/` change in the window |

Dev layers: `data/text_layer_v2_dev/19b-a` (111 hand inventories of `15y-b`), `19b-z` (14 of `15z-b`), `19b-v4check`
(off-check); clones of `15y-b`/`15z-b`, only the hand inventories rebuilt (layout, types, units).

**Split statistics** (`step-19b/split_stats.tsv`). 2,760 regions split in 105 of the 125 volumes into 16,134 pieces (5.8 per region),
4,181 of them span pieces (hull wider than 0.65 of the split region; approximate), 66,911 lines in split regions. 1,418 of the
regions are on index-like sides, 1,342 elsewhere (minuten and body pages with a margin column). Largest: 3487 (207 regions), 3471
(168; the raw-region prototype had 165 of 5,857), 3493 (166). main_text units grow by 10,637 (+2.7%): the extra pieces become extra
`hand:region` units. Interleave per volume: 3471 122 to 27, 3623 103 to 25, 3507 100 to 27, 3459 88 to 17.

**B1 verdicts** (`b1_verdicts.tsv`, `b1_reads.txt`; seed 20261117, 20 of 1,418 index-like and 20 of 1,342 other, read by me in text).
Rule I applied (the brief's definition left it open): `right` = columns separated, and within a column the pieces follow in text
order with extra piece boundaries only at genuinely merged or heading lines; block order (left, right, merged line, left, right)
counts as right because step 6 of the brief designs it. `wrong` = (a) the order in the layout is neither column-wise nor
block-wise (a column interrupted by the other column's piece, a stranded tail before the head), or (b) an ordinary, non-merged
line is classed as span and strands 1-2-line pieces in a column.

| side class | right | wrong | unclear | unreadable |
|---|---|---|---|---|
| index-like (20) | 15 | 5 | 0 | 0 |
| other (20) | 14 | 4 | 1 | 1 |

Kinds (first word of `kind`): index columns 23 (17 right, 6 wrong: 02, 08, 11, 13, 17, 38), margin column beside the main text 13 (10 right, 2 wrong: 23, 25; 1 unreadable: 26), minuten half-page 2 (1 right, 1 wrong: 40), bilingual or letter columns 2 (1 right: 36; 1 unclear: 27). All 9 `wrong` cases (short):
- 02 (3280_0027): order merged head, right column, left column; the head's continuation (left) comes last.
- 08 (3445_0099): 35 pieces from 45 lines. A stray fragment `uij.` at x=1716 pulls `sx` left, so ordinary left lines (x 554-672, w ~1200,
  ending at ~1760) count as span and stand alone: `554 | 19 Maert Miss: Colyear r: de meenigvuldige`.
- 11 (3351_0135): 13 pieces; `474 | 9 feb: s en Coninck bij missive pegslt, sine heer en heer vaan` (w1292, ordinary) is a 1-line span piece.
- 13 (3471_0025): heading, right column (9 lines), then the left column (3 lines).
- 17 (3433_0052): 33 pieces from 96 lines; layout column 0 mixes right-column top lines with left-column merged lines.
- 23 (3459_0568): the last margin line `gelragt` is piece 0, ahead of the main text; the rest of the margin comes after it.
- 25 (3522_0591): margin in 4 pieces and 1-line main pieces (`Is ter vergaderinge geleeren de Requeste van`, `Decken en Capittel ...`).
- 38 (3193_0177): right column pieces at y 1257-1743 come before the pieces at y 554-937.
- 40 (3199_0162): order full-width, right column, one left line, left column, left tail.
Not `wrong`, worth noting: 10 and 12 each have one ordinary line stranded as a 1-line span piece, with the order kept; 39 puts the
merged heading after the left column; 27 (bilingual, 3349_0577) has 9 of 27 lines merged Dutch+French and stays mixed (`unclear`);
26 (3520_0004) is garbled throughout (`unreadable input`). Margin column placement: the margin pieces come before the main text in
7 of the 12 margin cases (21, 22, 24, 29, 30, 35, 37) and after it in 2 (32, and 23 in part); the layout column assignment decides,
the split does not.

**Why it fails.** (1) Span classification is by line extent: HTR boxes of ordinary left lines are wide and reach past the right
column's first x (`x + w > sx + tol`), so they are classed span; with an outlier line `sx` moves left and most of a column turns
span (08, 11, 17, 38). Each span line closes the block, so the column breaks into 1-line pieces. (2) Piece order is decided after
the split by the existing column grouping, which puts a right-column piece in the same layout column as the merged head and
reorders by y (02, 13, 23, 40). No threshold change was tried (negative result, no tuning).

**Remaining interleaved units** (`remaining_reads.txt`, seed 20261118, 10 of 980). Not one is a gutter merge alone. Five are real
column regions kept whole by a guard: 3507_0039 (gap guard; `W` = 4,768 inflated by an outlier box), 3623_0018 (6 lines, 5 narrow
candidates, `min_lines`), 3354_0096 (median width of the left date column below 0.3 W), 3579_0157 (vertical overlap 213 < 2 h),
3217_0622 (14 of 30 span lines > 0.34). Five are not columns, metric false positives: 46_0335 (one text column with an indented
block of wide lines), 3212_0323 (indented text with side notes), 11930_0003 (text plus amount column, 2 right lines), 11436_0177
(a table of regiments), 11182_0529 (a table with number columns).

**Findings.**

1. The split lowers hand interleave from 2,612 to 980 units (0.65% to 0.24%) but raises main_text units by 10,637 (+2.7%).
2. It splits 2,760 regions in 105 of 125 volumes, 1,418 on index-like sides and 1,342 elsewhere, into 16,134 pieces (5.8 per region).
3. Bar B1 fails: 29 right, 9 wrong (0.763), 4 wrong among 20 not-index-like cases; UA-040 asks whether v5 ships the split.
4. The failures are extent-based span classification (ordinary wide lines strand 1-line pieces: 08, 11, 17, 25, 38) and the
   piece order after the layout column grouping (02, 13, 23, 40).
5. Of 10 remaining interleaved units, 5 are real columns kept whole by a guard and 5 are metric false positives (indent, amounts, tables); none is only a gutter merge.
6. Margin-column splits put the margin summary before the main text in 7 of 12 read cases and after it in 2.

**Brief defects.** (1) `interleave.py` must take the inventories from the `hand_*.txt` lists, not from `mode/*.json == hand`:
3463 is `hand` in both `15y-b` and `15z-b` (listed only in `hand_15y-b.txt`), which gives 126 rows, 401,816 units, 2,639 interleaved instead of 399,714 / 2,612.
(2) "span pieces" for `split_stats` is not defined by the layout tables; I used a hull wider than 0.65 of the split region.
(3) The B1 definition of `wrong` does not cover fragmenting or piece order; the rule above is mine. (4) `b1_reads.txt` is 2,535 lines for 40 cases (regions of up to 116 lines); sampling is as specified.

**Open issues.** UA-040 (does v5 ship the split); blocks 19c-19e as the brief says.

### Step 19c: index-side detector

**What changed.** New module `archival_structures/analysis/index_pages.py` (`INDEX_SIDES_DEFAULTS`, `side_features`, `side_rule`,
`fill_runs`), `LayoutParams.index_sides` (default `None`, left out of `to_dict` when `None`), `text_layer/layout.py`
(`PAGE_COLUMNS`; `layout/pages/<inv>.tsv.gz` only when the field is set; `n_index_sides` in the stats), and two preset constants
(`SG_INDEX_DATE_START`, `SG_INDEX_SIDES_V5`; no preset uses them yet). 8 new tests (7 in `tests/test_analysis_index_pages.py`, 1 in
`tests/test_text_layer_layout.py`); suite 1,348 collected, 1,337 passed, 11 skipped. **Planner change** (STATUS, after B1 failed): the
clones come from the v4 layers (`15y-b` to `19c-a`, `15z-b` to `19c-z`) and `--layout-params` is `{"index_sides": SIDES}` without
`hand_column_split`; only the 125 hand inventories were rebuilt (`layout`, `types`, `units`), each through coreshare, e.g.
`as-build-text-layer --preset sg_1.01.02_v4 --out-dir data/text_layer_v2_dev/19c-a/1.01.02 --stage layout --inventories-file planning/hand_15y-b.txt --layout-params '{"index_sides": <SG_INDEX_SIDES_V5>}' --workers {workers}`
(`SG_INDEX_SIDES_V5` as JSON; the `types` and `units` stages take no params).

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 55 of 55 in the three modules; full suite 1,337 passed, 11 skipped |
| 2a | feature check | 115,274 `layout/pages` rows equal to `planning/side_features.tsv` in all six feature columns (`features_check.py`) |
| 2b | mapping check (P10) | 5 of 5 `consistent` (3107, 3126, 3254, 3256, 3265): the index ends at GT page `end` and the sides after it are empty or stray tokens (3265: "Finis indicis Anni" on p91, resolutions from p93) |
| 2c | grid on `fit` | the 12 lines equal the planning block; selection V05 (P 0.9808, R 0.9297), equal to the run config |
| 3 | **D1** (20 test volumes, run once) | **pass**: lines P 0.9711, R 0.9490; sides tp 675, fp 12, fn 64 |
| 4 | **D2** (60 read sides) | **pass, on the bar**: accuracy 54/60 = 0.9000 (60 judged, 0 left out); 1 false positive in `S3h` + `S3a` (bar: at most 2); 5 false negatives, all in `S1` |
| 6 | v1-v4 unchanged | `LayoutParams().to_dict()` has no `index_sides`; protected lists (`protect_*.txt`, `protect2_*.txt`) identical before and after, no `shared-data/` change |

D2 per stratum: S1 15 of 20, S2 20 of 20, S3a 10 of 10, S3h 9 of 10. D1 and D2 are the bars of the P8 rule; V05 was fixed on the
fit volumes before the test (selection rule: highest R among P >= 0.95).

**Grid on the 22 GT fit volumes** (`select_fit.txt`; lines P/R, sides tp/fp/fn):

| V | shape-only arm | gap | min_run | P | R | tp / fp / fn |
|---|---|---|---|---|---|---|
| V01 | none | 0 | 1 | 0.9841 | 0.8383 | 395 / 9 / 287 |
| V02 | none | 0 | 3 | 0.9954 | 0.7349 | 339 / 3 / 343 |
| V03 | none | 4 | 1 | 0.9798 | 0.8835 | 461 / 13 / 221 |
| V04 | none | 4 | 3 | 0.9842 | 0.8336 | 442 / 11 / 240 |
| **V05** | none | 8 | 1 | **0.9808** | **0.9297** | 551 / 13 / 131 |
| V06 | none | 8 | 3 | 0.9857 | 0.9197 | 547 / 11 / 135 |
| V07 | 0.3 | 0 | 1 | 0.9302 | 0.9408 | 521 / 49 / 161 |
| V08 | 0.3 | 0 | 3 | 0.9603 | 0.8547 | 457 / 28 / 225 |
| V09 | 0.3 | 4 | 1 | 0.9225 | 0.9663 | 619 / 58 / 63 |
| V10 | 0.3 | 4 | 3 | 0.9444 | 0.9472 | 614 / 44 / 68 |
| V11 | 0.3 | 8 | 1 | 0.9177 | 0.9860 | 652 / 63 / 30 |
| V12 | 0.3 | 8 | 3 | 0.9374 | 0.9860 | 652 / 51 / 30 |

On the test volumes V05 has lower P (0.9711 vs 0.9808 on fit) and higher R (0.9490 vs 0.9297); no tuning was done on either set.

**Error analysis** (`errors_reads.txt`, `error_verdicts.tsv`, `error_sample.tsv`; read with `show_any.py`, 8-28 lines per side).
Sample: all 12 false-positive test sides; 20 of the 64 false-negative test sides (weighted by `n_lines`, seed 20261120); the 6 D2
disagreements.

| Category | Count | Where |
|---|---|---|
| FP gap fill across a non-index side | 8 | 4831 scans 0020-0023 (single-column officer roster, each name followed by "89, 1264."), between two rule sides |
| FP list with page references | 2 | 4831 0019 recto, 0024 verso: same roster, `rule` fires (col_share 0.32-0.33) |
| FP list with figures | 2 | 3320 0337 recto/verso: accounts with amounts ("13: 4:") in a right column |
| FN single-column dense index page | 8 | 3127 (one column of short entries ending in page numbers, end_num 0.80-0.97, col_share 0-0.19) |
| FN sparse index page, col_share unmeasured | 7 | 3127, 3274, 3290: 17-34 lines, fewer than 15 lines of 20 or more characters, end_num 0.38-0.91 |
| FN single-column index book (4617 style) | 3 | 4617 0003, 0085, 0089: chronological summary under a lemma, dates mid-line |
| FN GT label: blank or title page | 2 | 3127 0006 verso (5 stray tokens), 3290 0004 recto (title page "Index van ... resolutien") |

Verbatim: 4831_0020 recto "Jan Henrick van Gurtsen, Lt. Coll 89. 1264"; 3127_0016 recto "Hans Fonteine 127:" (12 lines, col_share
None); 4617_0085 verso "consent van Vriesland febr: 26." D2 disagreements: FN 3498_0027, 3520_0033, 3525_0043 (short, garbled
sides, no usable dates: "8aupsty aegst"), FN 3498_0128 (65 lines of dated entries but the HTR garbles the months, date_start
0.033), FN 3712_0273 (four text columns with mid-line dates, shape 0.135 just under 0.15), FP 751_0305 (1714 pass resolution in
two columns, col_share 0.56, end_num 0.17).

**Conclusion.** The rule works where an index has two columns and entry-final numbers or entry-initial dates. What it misses is
(a) one-column-per-side indexes (3127: 8 of 20 sampled FN, plus the 4617 style), (b) sparse sides, and (c) pages with garbled
HTR. The `c3` shape-only arm (V07-V12) would catch many of (a) and (b) but costs 3 to 6 points of P on fit (the grid above); it
was not selected and not tuned. What it falsely flags is a name list with page references (4831) and account pages (3320); the
gap fill turns 2 such sides into 10.

**Detected index sides of the 125 hand volumes** (`flagged_by_volume.tsv`, `front_sections.tsv`, `flagged.txt`). Groups are by
inventory number, not by a list of volume types: the brief names no source for "minuten", so they are inside the "other" rows.

| Group | Volumes | Sides flagged / all | Lines flagged / all |
|---|---|---|---|
| GT volumes (42; 22 with flags) | 42 | 1,251 / 48,471 | 46,833 / 2,202,877 |
| 3349-3600, index volumes (>= 50% of sides flagged) | 18 | 4,999 / 5,682 | 185,215 / 194,131 |
| 3349-3600, registers with a front section (>= 800 sides) | 24 | 1,922 / 35,133 | 92,462 / 1,410,235 |
| 3349-3600, small volumes with almost no flags (3498, 3520, 3523, 3525) | 4 | 15 / 830 | 459 / 18,918 |
| other < 3349 | 10 | 58 / 4,397 | 5,084 / 156,770 |
| other > 3600 | 27 | 628 / 20,761 | 33,945 / 672,762 |

In the 24 registers the first flagged scan is 2-6 and the front run ends at scan 22-61 (median 45); 4 of the 24 have flagged
sides after the front run (3349: 17, 3350: 9, 3352: 13, 3353: 11, the last up to scan 372). The four small volumes (3498, 3520,
3523, 3525; about 200 sides each) are index volumes by 19a's D2 reads (3498_0027, 3520_0033, 3525_0043 are all labelled `index`),
but only 15 of 830 sides are flagged: the detector does not find garbled-HTR index volumes.

**Findings.**

1. The grid reproduces on the layout tables: 115,274 feature rows equal, the 12 planning lines equal, V05 selected.
2. D1 passes on the 20 GT test volumes: lines P 0.9711, R 0.9490 (sides tp 675, fp 12, fn 64).
3. D2 passes on the bar: 54 of 60 sides (0.9000), 1 false positive in `S3h` + `S3a`; all 5 false negatives are `S1` index sides.
4. False positives are a roster with page references (4831) and account pages (3320); the gap fill makes 8 of 12.
5. False negatives are mostly one-column indexes (3127, 4617), sparse sides and garbled HTR.
6. Over the 125 hand volumes 18 index volumes are flagged by more than 50% of their sides and 24 registers by their front section (scan 2-6 to 22-61); four garbled index volumes are not found.

**Brief defects.** (1) The brief gives no definition of the volume groups (minuten, "other") and no list of index volumes; I grouped
by inventory number. (2) The `select.py` labels come from `planning/side_features.tsv`'s `gt_label` (not recomputed from the GT
JSON), as `detector_grid.py` does. (3) "Weighted sample" for the 20 FN sides is not specified further; sequential draws without
replacement with `random.Random(20261120)` over the sides sorted by inv, scan, page. (4) The `layout` stage rerun needed no
`--force`. (5) "Protect lists: replace 19b-a/19b-z" was read as: protect `19b-a`, `19b-z` too (no differences).

**Open issues.** none for 19c; 19d and 19e wait for UA-040 (and 19e for UA-039).

### Step 19d: index entry unit rule

**What changed.** `UnitParams.index_entry` (off by default; omitted from `to_dict` when `None`), `INDEX_CUES`, `INDEX_ENTRY_DEFAULTS`,
`entry_cues`, the `index_side` argument of `split_region` and `build_units_page` (hand mode only; methods `hand:index_start`,
`hand:index:<cues>`), segment `text` in `unit_inventory`, the read of `layout/pages` when `index_entry` is set (`FileNotFoundError`
otherwise), and `presets.SG_INDEX_ENTRY_V5` (variant E5; no preset uses it). 7 new tests (`tests/test_text_layer_units.py`, 17 to 24);
suite 1,355 collected (1,348 + 7), 1,344 passed, 11 skipped. Base: the v4 hand layout (19b's split failed, UA-040 unanswered), so
"split only" equals "v4 hand" (checked: identical numbers). Clones `19d-a`, `19d-z` from `19c-a`, `19c-z` (`--stage units
--unit-params '{"index_entry": <SG_INDEX_ENTRY_V5>}'`, coreshare). Gold: the 10 dev and 20 test sides of 19a; 4 test sides of the pinned
print volumes (3436, 3439, 3442, 3466) and 2 dev sides (3433, 3469) for the print baseline.

**Dev (10 sides, 492 lines; 169 S).**

| System | P | R | F1 | tp / fp / fn |
|---|---|---|---|---|
| E1 outdent | 0.8243 | 0.7219 | 0.7697 | 122 / 26 / 47 |
| E2 + date_start | 0.8242 | 0.8047 | 0.8144 | 136 / 29 / 33 |
| E3 + end_ref_prev | 0.7725 | 0.8639 | 0.8156 | 146 / 43 / 23 |
| E4 + short_prev | 0.7416 | 0.9172 | 0.8201 | 155 / 54 / 14 |
| **E5 indent + short_prev** | 0.8101 | 0.8580 | **0.8333** | 145 / 34 / 24 |
| v4 hand = split only | 0.9250 | 0.6568 | 0.7682 | 111 / 9 / 58 |
| v4 print (2 sides, E5 on them 0.9703) | 0.9608 | 0.9608 | 0.9608 | 49 / 2 / 2 |

7 of the 58 hand FN lines are in no main_text/date_header unit (typed otherwise); the rule cannot reach them. Choice (highest F1,
ties to the lower E): **E5**.

**Test (20 sides, 1,098 lines; 502 S; run once).**

| System | P | R | F1 | tp / fp / fn (no unit) |
|---|---|---|---|---|
| **E5** | 0.8683 | 0.7092 | 0.7807 | 356 / 54 / 146 (38) |
| v4 hand = split only | 0.9741 | 0.4502 | 0.6158 | 226 / 6 / 276 (38) |
| v4 print, 4 sides | 0.9444 | 0.8095 | 0.8718 | 102 / 6 / 24 (1) |
| E5 on the same 4 sides | 0.8649 | 0.7619 | 0.8101 | 96 / 15 / 30 (5) |
| v4 hand on the same 4 sides | 0.9706 | 0.5238 | 0.6804 | 66 / 2 / 60 (5) |

By group, test, E5 P / R / F1 (v4 hand F1): G1 0.890 / 0.695 / 0.781 (0.478), G2 0.844 / 0.644 / 0.730 (0.688), G3 0.885 / 0.766 /
0.821 (0.689), G4 0.796 / 0.696 / 0.743 (0.628). On dev E5 had R 0.90 / 0.80 / 0.97 / 0.44 in G1-G4 (3, 3, 3, 1 sides).

**Acceptance.** 1 tests green (full suite 1,344 passed). 2 dev numbers and choice above. 3 **entry bar fails**: P 0.8683 < 0.90, R 0.7092
< 0.85; only "F1 > v4 hand F1" holds (0.7807 > 0.6158). No tuning, no second test; **UA-041**. 4 flagged sides: main_text units `a`
55,505 to 91,531, `z` 24,839 to 41,687 (8,873 flagged sides in all; +65.8%); `unit_method` on flagged sides: `hand:index_start` 12,920 +
6,411, `hand:index:short_prev` 17,678 + 10,187, `hand:index:indent` 11,272 + 4,441, `hand:index:indent+short_prev` 7,076 + 2,220, the
rest with `header` (538+640, 214+113, 187+86, 209+246), `hand:region` (one-piece regions) 41,437 + 17,343, `hand:run` unchanged. **Units on
unflagged pages identical**: 0 differing pages of 676,744 units (all columns but the scan-level `unit_order`, the segment rows and the
unit-id order). Protected lists (`protect_*.txt`, `protect2_*.txt`) identical before and after; no `shared-data/` change.

**Error analysis.** All 200 test errors of E5 (54 FP, 108 FN, 38 FN in no main unit) were listed; 40 were sampled
(`random.Random(20261121)` over the list sorted by read and k) and read with two lines either side (`error_reads.txt`,
`error_verdicts.tsv`):

| Category (40 read) | n |
|---|---|
| two columns interleaved in one region: the previous main line in region order is on the other column | 26 (21 FN, 5 FP) |
| entry start typed `marginalia`/`other` (no main unit; 3349_0037 alone 10) | 11 |
| entry with no indent/short cue, only a reference at the end of the line above | 2 |
| gutter-merged line (3254_0009 k1 w=2025) | 1 |
| hanging indent below 0.5 h; no outdent/date/reference; cut at a continuation inside an entry; column split error (19b) as a separate class; gold ambiguity | 0 |

The interleave is the 19b problem seen through the entry rule: in 3254_0009 the layout lines 0..19 run k1, k2, k3, k45, k4, k5, k6,
k46, k7, ..., so every left-column line's predecessor is a right-column line, `short_prev` compares the wrong pair and `indent` is
measured against a column left edge pooled over two columns. Verbatim: `k9 "Pillegave voorden Jongen Prince 227."` follows (in region
order) `k47`, not the short line `k8 "uijtvoer van Levraij 222."`; `FP k57 "234, 461."` is a number-only continuation cut after
the other column's line. In 3256_0009 and 3126_0013 the two columns sit at x about 2.5k and 3.8k in one region (7 + 3 FN). The few
non-interleaved FN are lines after a full-width line that ends in a reference (`3107_0046 k17`, preceded by `... 154:`), which
`end_ref_prev` (E3/E4) would catch at the price of precision. Groups: G3 (date-start indexes) fits best (R 0.77, P 0.89); G1/G2
page-reference indexes lose most recall; on dev E5 beat E1-E4 in G1 and G3 and lost in G4 (one side). E5 differs from E3/E4 on dev
by dropping the date and reference cues, which dev G3 did not need (R 0.97). Conclusion: the entry cues are not the limit, the line
order within the region is; a column fix (UA-040 c) before the entry rule would be the pre-registered way to test it again.

**Findings.** F13-19d.1-6 (table above).

**Brief defects.** (1) "19b-*" and "split pieces" had to be read as the v4 hand regions (task note), so "split only" equals "v4 hand"
and is reported once. (2) The brief gives no pairing rule for sides without gold lines in a unit (counted as `fn_nounit`, as written)
and does not say whether a `date_header` start can be a TP on a date line: it can (the pair is one entry started by the date line).
(3) "the 3 lines around it" was read as two before and two after. (4) The pinned print baseline covers only the 6 gold sides of
3433, 3436, 3439, 3442, 3466, 3469 (3445, 3472, 3475, 3489, 3463 have no gold side), so it is a 2 + 4 side diagnostic. (5) The
`protect.py --skip 19b 19c 19d` form leaves `19c-*` out of the hash list, so they were added through the `find` list as the brief says.
(6) The error sample of 40 over the pooled FP + FN list gives only 5 FP; the FP side is thinly read.

**Open issues.** UA-041 (ship the entry rule or not); UA-040 and UA-039 still open.

### Step 19e: v5 preset, dev build, audit and run kit

**Gate.** UA-039 (a): `check_verdicts.json` against `side_verdicts.tsv`, 30 items: 27 judged items by both, **27 agree (1.000)**, no
disagreement; 3 `unsure` (read_ids 17, 23, 54; the agent said `index` on all three) are left out, so the side gold stands but those
three are unconfirmed. UA-043 (a): **branch A**, no column reorder. UA-044 (a): pyarrow 26.0.0 was in `.venv`; the silver check ran. All
three UAs moved to `docs/USER_ACTIONS_resolved.md`. Suite: 1,368 collected, 1,357 passed, 11 skipped.

**What changed.** `presets.SG_1_01_02_V5` = `SG_1_01_02_V4` with `mode_overrides["hand"]` = `layout_params` {`SG_HAND_LAYOUT_PARAMS`,
`index_sides`: `SG_INDEX_SIDES_V5`} and `unit_params` {`index_entry`: `SG_INDEX_ENTRY_V5B`}; `PRESETS["sg_1.01.02_v5"]`. The run script
takes `--version {v4,v5}` (`ARCHIVES_BY_VERSION`, `ARCHIVES` kept; `link_archive(..., version)`; label `<version>-<archive>`;
`--version v5` with 1.10.94 is an argparse error). Tests: `test_v5_preset`, `test_version_v5`, `test_version_v5_rejects_1_10_94`.
`docs/text_layer.md` has *Text layer v5 (phase 13)*; the changelog has the step entry; UA-045 (full run) and UA-046 (pilot) are written.

**Build.** `19e-a`: 231 inventories (`--check-only` found 231, no duplicate; 125 hand, 106 print), `--want 4 --min 2` granted 4, wall
**60 min** (11:44:30 to 12:44:32 UTC; predicted 30 to 70 min), every stage done for 231, 0 failed rows, 2,723 MiB. The run script's own
v4 audit: 4,192 pass, 47 reported fail (lower-case merge shares), 0 judged.

**Compare with v4** (`step-19e/compare_v4.json`, per inventory and file; reference 15z-b for 37 volumes, else 15y-b).
- Print (106): equal in every per-inventory file (the 6 `scans` differences are the `member` column only, below).
- Hand (125): equal in `lines`, `regions_meta`, `mode`, `extract_stats`, `layout/{scans,regions,lines}`, `types/*`; `layout/pages/` new
  (125); `units/units`, `units/segments`, `units/doc_id_map`, `join/*` differ in 86 volumes (the others have no flagged side), `wbc/parts`
  in 83. `units/units` and `units/segments` are **equal to `19g-a`/`19g-z` for all 125**.
- `scans` differs in 8 volumes (3771-3773, 3821-3823, 11182, 11281): only the `member` column, the source path of the 14 directory
  sources (`text_layer_v5_19e_inputs` against `text_layer_v4_full_inputs`). Not a rule change.
- Layer-level files (params, stats, manifest, status, progress, corpus wbc): all differ or are new (listed in the JSON), not judged.

**Audit** (`audit_19e-a.json`): 4,196 pass, 47 fail, **0 judged failures**; the 47 are the reported lower-case merge shares (the same
kind as v4). V5 checks, all pass: every hand inventory has `layout/pages`, no print inventory has one; no duplicate `layout_region_id`
in a scan; every `hand:index*` unit sits on a side with `index_side == "1"`; no `index` unit on a flagged side. 8,873 sides flagged.

**Metrics** (`step-19e/metrics_*.json`).
- (b) Flagged sides (125 volumes, 115,274 sides, 4,655,693 lines): 8,873 sides (7.7%), 363,998 lines (7.8%); by the side rule 5,225, by
  gap fill 3,648 (41%). `layout/pages` equals the 19c clones for all 125. By group: GT volumes (42) 1,251 of 48,471 sides (888 rule, 363
  gap fill; 22 volumes with flags); half-year registers 3349-3600 (46) 6,936 of 41,645 (3,811 / 3,125; all 46 have flags); other < 3349
  (10) 58 of 4,397; other > 3600 (27) 628 of 20,761 (468 / 160; 13 volumes).
- (c) `main_text` units on flagged sides **80,344 (v4) to 162,021 (v5)** as expected; elsewhere 319,370 both; `index` units on flagged sides
  2,789 to 0. `unit_method` on flagged sides: v4 `hand:region` 77,185, `hand:run` 40,683, `hand:region_cut_at_header` 3,159; v5 `hand:index_start`
  22,096, `hand:index:<cue>+...` 81,162 over 30 combinations (most frequent `end_ref_prev` 12,722, `short_prev` 11,801, `short_prev+end_ref_prev` 11,624,
  `outdent` 10,593, `date_start` 10,103), plus `hand:region` 58,763 and `hand:run` 37,894 for the non-main types.
- (a) Interleave: **728 of 481,391** `main_text` units (0.15%) in v5, equal to the 19g clones (the same script gives 2,612 of 399,714 for v4 and 843 of
  399,429 for 19f). The brief's expected "2,612 of 399,714" is v4's number (brief defect); the denominator changes because the units on flagged
  sides double.
- (d) End to end on the 50 spent gold sides with the production units (no bar): 19a sides (30) **P 0.8768, R 0.9329**, F1 0.9040 (tp 626, fp 88,
  fn 45); 19g sides (20) **P 0.9530, R 0.8974**, F1 0.9244 (tp 385, fp 19, fn 44). Both equal the expected numbers, so the build equals the clones.

**Silver check** (UA-044 a; `silver.py`, `silver.json`, `silver_sides.tsv`). 27 of the 125 hand volumes have a `republic_segmentation_v1`
partition. A side with at least 5 lines of resolution unit types is a silver negative: **34,428 sides, 18 flagged (0.05%)** (8 by the side rule, 10 by
gap fill) in four volumes: 4831 (10), 3315 (5), 3320 (2), 3307 (1). Read in the text (seed 20261129, 5 of 18): 4831 verso 22, 20 and recto 23 are
regiment rosters (each line ends in the same page-like pair `89, 1264`), 3307 verso 333 and 3315 recto 488 are account pages (lines ending in
amounts `2507: 19: 11`); none is an index page, so the 18 are real false positives, all of two kinds (rosters, accounts) that share the
index cues (a number at the line end). The silver layer has known limits ("13 of 20 random sessions read were wrong"); it is used for resolution
pages only. No bar.

**Error analysis** (`error_reads.txt`, `error_reads.py`).
- 10 units on flagged sides, volumes outside both gold sets (seed 20261122, pool 37,208): one entry 3 (3472 "22 Januarij. Reg: Schelberg ...",
  3350 "Duijst van Doorhout ... 108, 218.", 3611 "Beneficien, vid: Canoniksdyen."), two entries 3 (3587 scan 6: the tail of "'t Collegie in Zeeland. 76."
  plus "Reqte van W: C: H. van Sijnden ... 476."; 3394 "Land Graef van Hessen / Philipstad 636."; 3587 scan 9 tail plus "Reqte van St. P: Snoets"), part of an
  entry 3 (3623 "ien Raad van Staaten."; 3353 "herstellinge van het schip Vigos, 7, 97."; 3623 ",deratien van D'iecteuren"), heading 1 (3463
  "Turhsche pasporten"), not an index 0. The two-entry units are the cuts missed after a short reference or amount (F13-19g.4); the part units are
  continuation fragments (the first line of the entry sits in a `marginalia` unit or is cut at the amount).
- 5 units on gap-filled sides (seed 20261123, pool 6,460): index pages 4 (3546 "24. Novm. Nederburg Eed gedaan." and "Commisen generaal"; 3590 "geappobeert 908
  Reqte"; 3463 "17. September. Rapport en Res: 2000 gl: ..."), a roster page 1 (4831 "Gajus van Jeltinga89, 1264."). Harm from the gap fill in this sample: 1 of 5, and it is
  the known roster.
- The 3 volumes with the largest relative change in `main_text` units on flagged sides, one flagged side each: 11930 (0 to 27, 1 side; a garbled scan
  of unreadable HTR, the side is not an index: 27 junk units of a page that was junk before), 4831 (14 to 358 on 10 sides; scan 19 recto is a roster,
  every `89, 1264` line is cut as an entry: harm, no loss of text), 3265 (195 to 2,983 on 84 sides; scan 4 recto is the first page of the real
  index, cut correctly into about 20 entries).

**Findings.** F13-19e.1-6 (rows in the table above).

**Brief defects.** (1) Expected interleave "A: 2,612 of 399,714" is v4's number; v5 gives 728 of 481,391, equal to the 19g clones (checked with the same
script on 15y-b/15z-b, 19g-a/z, 19f-a/z). (2) `--check-only` needs `--out-dir` to exist (made it). (3) The `scans` table's `member` column holds the source path,
so a v5 build from other input links differs from v4 there; the brief's "equal in `scans/`" holds only for the file contents apart from that column.
(4) Metric (b)'s "index volumes, registers' front sections, others" has no list in the brief; 19c's grouping (`flagged.py`) was used. (5) The brief says the v5
unit_method for the first unit is `hand:index_start`; confirmed, but `hand:index:header` also occurs (957). (6) The run script's final audit is the v4 15w audit
(it does not know the v5 checks); the 19e copy adds them.

### Step 19f: column order v2

**What changed.** `HAND_COLUMN_SPLIT_DEFAULTS` gains `"mode": "pieces"` (19b, the default, unchanged) and `"span_reach_frac": 0.5`;
mode `"reorder"` keeps a two-column hand region whole and re-orders its lines, left column then right column, each line classed by
where it starts (`x < sx - tol` left), with wide lines (`x + w > sx + 0.5 * median right width`) kept at their row in the left run.
Steps 1-3 of `_column_pieces` moved into a helper `_column_geometry` used by both modes; the new function is
`_reorder_lines`/`_reorder_regions_by_column`, the new region carries `column_reorder_from`. `LayoutParams.__post_init__` rejects a
`mode` outside `("pieces", "reorder")`; `layout._change` maps `column_reorder_from` to `column_reordered`;
`presets.SG_HAND_COLUMN_ORDER_V5` holds the reorder values (no preset uses it). 7 new tests (6 in
`tests/test_analysis_page_structure.py`, 1 in `tests/test_text_layer_layout.py`); suite 1,362 collected, 1,351 passed, 11 skipped.
Nothing is published. Layout params of the clones (`step-19f/layout_params.json`): `hand_column_split` = the reorder dict
(`min_lines 3, min_gap_frac 0.25, max_col_frac 0.65, min_col_w_frac 0.3, tol_h 0.5, min_overlap_h 2.0, max_span_frac 0.34, mode
"reorder", span_reach_frac 0.5`) and `index_sides` = `SG_INDEX_SIDES_V5`.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 55 of 55 in the two modules; full suite 1,351 passed, 11 skipped (1,355 + 7 = 1,362 collected) |
| 2 | pieces mode unchanged | on `19f-piecescheck` (3471, 3579) `layout/{regions,lines}` equal to `19b-a` (decompressed); `LayoutParams().to_dict()` has no `hand_column_split` (test) |
| 3 | interleave and units | v4 baseline reproduces: 399,714 units, 2,612 interleaved (0.6535%); `19f-a`/`19f-z`: 399,429 units, 843 interleaved (0.2111%); 19b: 980. `19c-*`: 399,714 units, so -285 units (-0.07%); 31 of 125 inventories differ, by +1 to -48 units (12297 -48, 3475 -47, 3579 -30, 3445 -16); layout region rows 707,748 in both, so no region is added; the unit differences come from the changed line order |
| 4 | **bar B2** | **fail (narrowly)**: right 35, wrong 4, unclear 1 of 40; right/(right+wrong) = 35/39 = 0.897 (< 0.90); wrong among the 20 `other` = 2 (bar <= 2). Judgeable (1 unclear <= 8). UA-043 |
| 5 | duplicate `layout_region_id` per scan | 0 in the 125 rebuilt inventories |
| 6 | protected lists | `protect_before/after.txt` (clones skipped) differ only in `shared-data/`: `README.md` modified, `NL-HaNA_1.01.02/republic_segmentation_v1` (3,105 files) and `NL-HaNA_1.10.94/republic_segmentation_v1` (18) added by another project; `protect2_before/after.txt` (59,476 files: 15y-b, 15z-b, 19b-a, 19c-a, 19c-z, v4_full) identical |

Dev layers: `data/text_layer_v2_dev/19f-a` (111 hand inventories of `19c-a`), `19f-z` (14 of `19c-z`), `19f-piecescheck` (off-check, 3471 and
3579 only). Clones of `19c-a`/`19c-z`; only the hand inventories rebuilt (layout, types, units).

**Statistics** (`step-19f/reorder_stats.tsv`, `stats.py`). 3,066 regions are re-ordered in 108 of the 125 volumes (1,696 on
detector-flagged sides, 1,370 elsewhere; largest 3487 with 212, 3471 176, 3493 172, 3507 150, 3456 148), 73,777 lines in them.
4,833 lines are wide (6.6%); 62 regions have none, 2,042 one, 574 two, 388 three or more. In 1,340 regions (43.7%) the right text
starts higher than the left.  The pieces mode split 2,760 regions (19b).

**B2 verdicts** (`b2_verdicts.tsv`, `b2_reads.txt`, `b2_cases.tsv`; seed 20261125: the 3,066 regions minus the 40 B1 source regions
leave 1,674 flagged and 1,352 other; 20 + 20 drawn before reading; read by me in the text, no script judged). Rule applied as fixed in the
brief; two readings I had to settle: a `W` line that holds half of the other column's text is accepted at its row in the left run,
and a case is `unclear` only when neither left-then-right nor right-then-left reads continuously.

| side class | right | wrong | unclear | unreadable |
|---|---|---|---|---|
| flagged (20) | 18 | 2 | 0 | 0 |
| other (20) | 17 | 2 | 1 | 0 |

Kinds: index columns 15 (13 right, 2 wrong), margin column beside the main text 12 (all right), two text columns 10 (9 right, 1
unclear), parallel languages 1 (right), fragments of the same lines 2 (both wrong). Both rules of B2 checked: ratio 0.897 fails, `other`
wrong 2 passes.

The 4 `wrong` cases (shortened):
- 10 (3579_0184, flagged): the centred heading `Vlaanderen / Sas van Gent.` (x 1289-1371, y 118-207) is classed right because the
  largest start-x gap falls between 579 and 1289; the new order is left lines, heading, right lines, so the heading comes after the left text.
  Borderline: the heading belongs to neither column.
- 16 (3472_0189, flagged): `sous, ssebréé, Vleugels,` (x 3563, y 1589) continues the left line `18 Junij. op de Heeren Matthas,` (ends at
  3582, y 1596) in the same row, but starts right of the gap and joins the right run.
- 25 (11182_0670, other): left (x~900, w~540) and right (x~1860, w~660) lines are the two halves of the same written lines, row-aligned
  (`Boven dit alles. .` + `iestegenstaende dequoti„`, `satie vant gemae` + `int Eijlant van Cadsant`); the XML order was right and is broken. 64 lines.
- 30 (11436_0256, other): seven word fragments of width 110-126 (`Lar`, `hout`, `hout`, `2`, `aet`, `Rogg`, `ogge.`); a metric false positive
  that passes every guard; row order lost.
The `unclear` case, 23 (882_0162, other): one sentence listing deputies in two columns; the first right-column line `Zijnde ter vergaderinge
gecompareerd` (y 106) should come before the left list, and a town list at x~3000 (y 1997-2347) belongs beside the right text; neither
column order reads continuously. Counted as `right` the ratio would be 36/40 = 0.900. Two `right` calls are weak: 22 (12444_0184) has 6
of its 22 lines wide, so the body text is split between `W` and the right run, and 07 (3493_0011) lost its first right-column line to the
sibling piece in the page-gutter split (not the reorder's doing). I checked the other index and margin cases against the text for the
same row-aligned-halves pattern as in 25 (e.g. 21, 26, 27, 32, 33, 35-39 are row-aligned but each column reads on its own, a margin summary beside
the body) and found fragments of the same lines only in 25 and 30.

**Remaining interleaved units** (`remaining_reads.txt`, `remaining_reads_layout.txt`; seed 20261126; the guard recomputed on the
*layout* region the unit's lines sit in, since the source region can span both pages of a scan). 3 sit in regions kept whole by the wide-line
guard (units 03, 06, 10: a margin column merged into the body lines, 9-36 wide lines), 3 by a size guard (02, 04, 09: 4-6 lines), 3 by the
median-width guard on the right side (05, 07, 08: index columns with short right lines), 1 is a three-cluster page (unit 01): a region
re-ordered after the page-gutter split, whose long left lines, inner indented continuation lines (x 2723-2776) and right column (x 3812+)
make the largest gap fall to the right of the inner lines, so the hanging indent still alternates (metric false positive). None is a pure
gutter merge.

**Error analysis.** Read: 40 B2 regions (2,127 lines in `b2_reads.txt`) and 10 remaining units. Where the rule works, it works well:
35 of 40 regions are index, margin-summary or text columns that now read column by column (18 of the 20 flagged). The
failures are of two kinds: halves of the same lines taken for columns (2, both `other`), a line near the gap that belongs to the other
side or to neither (2, both `flagged`). The reorder keeps one region and one unit per region, so the 19b piece-order failures (4 of 9 wrong)
are gone, and so are the stranded 1-line span pieces (5 of 9), but the failures that remain are in the geometry that decides left and right.

**Conclusion.** A negative result by the pre-registered bar, by one case (0.897 against 0.90). Interleave falls further than with the split
(843 against 980 of about 400,000 units) with 285 fewer units where the split added 10,637; B2's wrong cases are few and of two kinds that
the guards could be tightened against (a centred heading inside the gap; row-aligned halves), but the bar is not tuned: the cases
are reported as read. UA-043 asks whether v5 ships the reorder. "19g base" is therefore the v4-layout clones `19c-a`/`19c-z` unless UA-043 says otherwise.

**Open issues.** UA-043 (ship the reorder or not); UA-039 still open.

### Step 19g: index entry rule v2

**What changed.** `INDEX_ENTRY_DEFAULTS` gains `"no_cut_pattern": None` and `"main_types": ["main_text"]` (old configs behave as before).
`entry_cues`: a main line that fully matches `no_cut_pattern` fires no cue except `header` (a page-reference list that ran over,
`358, 540, 546.`, never starts an entry). `split_region` in entry mode turns every segment whose `line_type` is in `main_types`
into `main_text` before the runs are grouped, so the lines v4 typed `index` (`rule:index_page`) on flagged sides join the mains.
`presets.SG_INDEX_NO_CUT = r"[\d\s.,:;]+"` and `SG_INDEX_ENTRY_V5B` (the chosen variant). 3 new tests; suite 1,365 collected, 1,354 passed,
11 skipped. Nothing is published; no preset uses V5B yet. Built on the v4-layout clones `19c-a`/`19c-z` (STATUS "19g base"; 19f's B2 failed) as
`19g-a`, `19g-z` with `--stage units --force --unit-params '{"index_entry": SG_INDEX_ENTRY_V5B}'` (`step-19g/unit_params.json`).

**Fresh gold (Part A, read before any code ran on the fresh sides).** 20 `read` sides read in `read_id` order from `show_side.py`
listings, no rule or script labels. 108 (G1) is running text (accounts), not index; 109 (G3) is unreadable input (most words garbled);
they were replaced by the reserves 121 (G1) and 125 (G3). 20 `index` sides, 868 lines: **S 429, C 340, H 95, ? 4**. Files
`step-19g/fresh_gold.tsv`, `fresh_sides.tsv` (sha256 in `PROGRESS.md`). Conventions applied: a line holding a second entry that starts
mid-line is labelled by its first part (`C`, note `midline_start`, 7 lines); merged gutter lines are labelled by their first part (`merged`,
12 lines); a ref-only line that sorts before its lemma is `C` (`ref_before_lemma`); undated sub-entries with their own page reference are `S`.

**Selection (30 spent 19a sides, X1-X4 and baselines; micro).**

| System | P | R | F1 | FP | FN (no unit) |
|---|---|---|---|---|---|
| X1 outdent + date_start + end_ref_prev, mains `main_text` | 0.8951 | 0.8644 | 0.8795 | 68 | 91 (45) |
| X2 as X1, mains `main_text` + `index` | 0.8994 | 0.9061 | 0.9027 | 68 | 63 (15) |
| X3 X1 + short_prev | 0.8715 | 0.8897 | 0.8805 | 88 | 74 (45) |
| **X4** X3, mains `main_text` + `index` | 0.8768 | 0.9329 | **0.9040** | 88 | 45 (15) |
| E5 (19d) | 0.8506 | 0.7466 | 0.7952 | 88 | 170 (45) |
| v4 hand = BASE units | 0.9574 | 0.5022 | 0.6588 | 15 | 334 (45) |
| v4 print (the print-pinned volumes' sides: `15z-a`/`15x-a`) | 0.9497 | 0.8531 | 0.8988 | 8 | 26 (1) |

X4 has the highest F1 (X2 0.9027 is 0.0013 behind), so the rule chooses X4. Adding `index` to the mains is the larger gain (R +0.04 from X1 to X2
and from X3 to X4, no unit 45 to 15); `short_prev` trades precision for recall (P -0.02 to -0.03, R +0.03).

**Fresh test (20 sides, once, X4).** All 20 sides are flagged in BASE `layout/pages`.

| System | P | R | F1 | TP | FP | FN (no unit) |
|---|---|---|---|---|---|---|
| **X4** | **0.9530** | **0.8974** | **0.9244** | 385 | 19 | 44 (31) |
| E5 | 0.8398 | 0.5012 | 0.6277 | 215 | 41 | 214 (85) |
| v4 hand = BASE units | 0.9863 | 0.3357 | 0.5009 | 144 | 2 | 285 (85) |
| v4 print (2 sides, 3445 and 3489) | 1.0000 | 0.7500 | 0.8571 | 18 | 0 | 6 (1) |
| X4 on those 2 sides | 0.9565 | 0.9167 | 0.9362 | 22 | 1 | 2 (1) |

By group (P / R): G1 0.970 / 0.866, G2 0.933 / 0.980, G3 0.958 / 0.883, G4 0.954 / 0.872 (E5: G4 0.679 / 0.376).

**Entry bar v2: passed.** P 0.9530 >= 0.90, R 0.8974 >= 0.85, F1 0.9244 > v4 hand F1 0.5009 (same sides). The test ran once (`test_done.json`).

**Units (Part D; `units_stats.py`).** main_text units on flagged sides: 19c-a 55,505 to 19g-a 111,062 (111 inventories, 5,602 flagged
sides); 19c-z 24,839 to 50,959 (14 inventories, 3,271 flagged sides); total 80,344 to 162,021. Units typed `index` on flagged sides: 2,573 and 216
before, 0 after. **Elsewhere unchanged:** 0 unflagged pages differ in unit rows (without `unit_order`), segment rows or unit-id order
(674,610 and 2,134 units). Protected lists: `protect_before` and `protect_after` are identical (no change by other projects in the
`shared-data/` tree during the run) and the hash list of `15y-b`, `15z-b`, `15z-a`, `15x-a`, `19c-a`, `19c-z`, `data/text_layer_v4_full` is unchanged.
(The "before" lists were taken after the read-only selection and test runs, which write only into `step-19g/`.)

**Error analysis.** Read: all 19 FP and 40 of 44 FN (`random.Random(20261127)` over the sorted list), each with two lines before and after in
region order and the region id (`step-19g/error_reads.txt`). Layout causes were checked in BASE `layout/regions` (`layout_check.py`): the regions of
reads 110, 114, 119, 120, 121 and 115 have a width of 2,234-2,410 px, i.e. both columns; the region of read 112 has w 1,111, one column, so those FP
are not column errors.

| Category | FP (19) | FN (44; 40 read) |
|---|---|---|
| Two columns in one region (merged line or wide region; interleaved region order) | 9 (110 k29; 114 k18, 20, 21, 22, 44; 119 k22; 121 k17, k51) | 1 read (120 k61: a date_header from the other column swallows the start) |
| Line in no main unit | - | 31 (28 read): all typed `marginalia`; 15 in `extra`-role regions, 16 in `main`-role regions (one-line regions, or lines inside a region that also holds main lines) |
| Entry with no cue | - | 10 read (112: 6 lemmas after a line ending in a letter reference, `Letter R.`, `Lett. W.`; 101 `1 dito`; 104 `S: en H: Hope`; 113 after `Maerts:`; 125 `16 ang:`, an OCR month) |
| Continuation cut: `short_prev` | 5 (112 k26, 72, 74, 76; 118 k7) | - |
| Continuation cut: `end_ref_prev` on a non-reference number | 4 (104 `f150`; 116 `30000.`; 120 `22:`; 115 stray `stants`) | - |
| Continuation cut: `date_start` (a date closing the entry) | 1 (113 k6) | - |
| Start suppressed by the scoring rule after a `date_header` unit | - | 1 (119 k16) |

114 k18 is gold ambiguity as well (a merged gutter line labelled by its first part, `C`, whose second part starts an entry).
Examples. FP `short_prev`: `Liquidatie van de Provincien,` then `Zee Rekenkamer Letter R.` (112, one-column region): a short line followed by
a cross-reference in the same entry. FP `end_ref_prev`: `... Corps van 30000.` then `Russen op de Fronlieren ...` (116). FN no cue:
`Del Mina, Zio West. Ind. Lett. W.` then `Middelburgh in Zeeland.. 106.` (112): the previous line ends in a letter, not a number.
FN no unit: `Attentaten van Bisschop 753:`, `affgesanten van Stadt 719:`, `audientie propositie 719:` (121, one `extra` region, all typed `marginalia`).

**Conclusion.** X4 passes the bar on 20 fresh sides read before any code ran: P 0.953, R 0.897. Most of the remaining recall loss (31 of 44 FN) is
lines typed `marginalia` that no entry rule reaches: the next lever is the types stage on flagged sides, not the cues. Most of the precision loss is in
regions that span both columns (9 of 19) and in continuations after a short line or an amount (10 of 19). The selection numbers (P 0.877, R 0.933) and the
fresh numbers (P 0.953, R 0.897) differ in both directions; the two gold sets were read by different sessions with slightly different conventions
(here mid-line starts are `C`, which lowers R), so a gap of this size is not read as a difference in the rule.
Caveats: 20 sides from 19 volumes; one reader made the gold (UA-039, a blind check of the 19a labels, is still open); the BASE has the v4 layout, so the column
failures are those of 19c, and 19f's reorder was not used.

**Open issues.** UA-043 (ship the reorder or not); UA-039 still open. Follow-up for the 19e re-plan: typing of `marginalia` lines on flagged sides (no UA yet; it needs a design first).

## Review

**Review of steps 19a–19d** (Opus 5.5, 2026-10-28; step 19e pending, so this report stays *draft*). Code review:
[`REVIEW.md`](../plan/phase-13-register-index-volumes/REVIEW.md). Every logged acceptance number reproduces (19a
summary, 19b interleave counts with equal per-volume files, 19c grid, D1 and D2, 19d dev and test with equal per-line
files); the suite gives 1,344 passed, 11 skipped. Scripts and tables of this section are in
[`data/phase-13-register-index-volumes/review/`](data/phase-13-register-index-volumes/review/).

**19d: interleave or cues?** Each test gold line was placed in its layout region of the 19c clone, and each line in a
column by 2-means over the side's line starts (`entry_interleave_vs_cues.py`). A region is *mixed* when it holds at
least 2 main lines of each column. Only 5 of the 20 test sides have a mixed region, and those regions hold 78 of the 502
`S` lines (15.5%) and 59 of the 526 `C` lines. All 200 E5 errors, by place and cause:

| Where | FN | FP | Cause (counts) |
|---|---|---|---|
| mixed region | 33 | 25 | the previous line in region order is on the other column |
| no main unit | 38 | – | 30 lines typed `index` by v4's `rule:index_page` (all of 3349_0037), 8 typed `marginalia` |
| one-column region | 75 | 29 | FN: 42 follow a line ending in a number, 24 start with a date, 9 neither; FP: 21 `indent` on a continuation line, 3 `short_prev`, 5 region or index starts |

E5 on one-column regions alone: P 0.9147, R 0.7335. The recall loss is the cue set: E5 has neither `end_ref_prev` nor
`date_start`, and in register indexes of one-line entries (`Cornelia Daetheler 329.` / `Jan Demcken 332, 356.`,
3256_0009) no line is short against the region's median right edge, so `short_prev` never fires. The precision loss is
`indent`, the print paragraph cue: index entries hang, so it fires on the indented continuation (`Nederlanders 314,
328, 355.` / `358, 540, 546.`). 19d's error sample put 26 of 40 errors under "interleave", but 14 of those 26 are in
one-column regions (reads 5, 7, 15, 25, 27; 3256_0009 has 14 regions, one per column block). The reader saw the two x
ranges of the side in gold order, not the region boundaries (F13-R.1). A column fix alone cannot lift E5 over the bar:
with every `S` line of the mixed regions found, R ≤ 389/502 = 0.775. The reviewer's re-split of the mixed regions
gives E5 P 0.9030, R 0.7231 (F13-R.3).

*Post-hoc variants (diagnostic only; these runs spend the 19d test gold).* E3 and E4 score F1 0.859 on test against
E5's 0.781. On dev, E3–E5 were 0.816, 0.820 and 0.833 over 10 sides, so the choice was within noise (F13-R.4). Pooled
over all 30 gold sides (`guard_estimate.py`; *guard* = no cut before a line of only digits and punctuation, the
overflow of a page-reference list):

| Variant | layout | P | R | F1 |
|---|---|---|---|---|
| E5 (19d) | v4 | 0.8506 | 0.7466 | 0.7952 |
| E3 + guard | v4 | 0.8964 | 0.8644 | 0.8801 |
| E3 + guard | columns re-ordered in the region | 0.9397 | 0.8823 | 0.9101 |
| E4 + guard | columns re-split into pieces | 0.9335 | 0.9001 | 0.9165 |

So the cues are the main lever and the column fix adds about 0.04 of precision. With the `index`-typed lines brought
in, a redesigned rule plausibly clears P ≥ 0.90 and R ≥ 0.85. This has to be shown on fresh gold (step 19g; sides drawn
in `review/fresh_entry_sides.tsv`, seed 20261124).

**v4 already has an index type** (F13-R.5). The v4 types stage types every line of a page `index` when at least 50% of
its lines are short and at least 30% end in a number (`types._is_index_page`). In the 125 hand volumes that covers 963
sides (17,845 segments), 408 of them on detector-flagged sides (8,763 segments, mostly the fronts of 3349–3353, 3368,
3428). Those lines form `index` runs, not main units, so neither v4 hand units nor the entry rule cut them. This is the
"typed marginalia/other" class of 19d.

**19b.** The code follows P6 step by step, so B1 failed on the rule. Extent-based classification makes ordinary left
lines that reach past the gutter into span lines (08, 11, 17, 25, 38). Each span piece then spans both column hulls,
so `group_regions_into_columns` puts all the pieces of a region into one layout column and orders them by top. In case
13 the right column (top 1871) comes before the left (top 1875). A start-x rule on the same 40 regions
(`split_v2_on_b1.py`) qualifies all 40 with 0–4 wide lines each (9 in the bilingual case 27), against 19b's 13–35 pieces in the fragment cases. In
13 of the 40, though, the right column starts above the left. A piece-based v2 would still misorder those unless the
columns stay in one region in sequence (reorder). The 19f brief pre-registers that design (F13-R.7). Would it pass a
fresh B1? The 9 wrong cases come from the two mechanisms the reorder removes by construction. The remaining risk is
regions that are not two columns (tables, indented blocks), which 19b already counted as `right`/`unclear`. So a pass
is plausible but not shown.

**19c.** D1 holds, with a caveat on its evidence. Only 10 of the 20 test volumes have positives or false flags, and
4831 alone holds all 562 false-positive lines. A bootstrap over volumes gives a 90% interval of P 0.917–1.000 and R
0.895–0.980. On test, P and R stay at 0.970–0.973 and 0.926–0.954 over `col_share` ∈ {0.20, 0.25, 0.30} × shape ∈
{0.10, 0.15, 0.20}, so the planner's pooled look before the split (F13-P.5) cannot have bought the pass (F13-R.6).
D2 sits exactly on its bar (54/60): one changed side label in UA-039's check would move it, so the check matters.
All 30 entry gold sides are flagged by the detector, so 19d's "index_side=True" evaluation equals the production path.

**19a.** Labels follow the brief: 10 of 810 `C` lines start with a date (7 are the 12297 date column of F13-19a.7)
against 198 of 671 `S` lines. Spot reads of six test sides found no inconsistency. Dated follow-ups in chronological
indexes (3439 `9 oct 6m.`) are `S` by the "own date" rule; that is a convention, and 19g keeps it. Entry labels come
from one reader and have no independent check (UA-039 covers side labels) (F13-R.8).

**Process.** Running 19c on the v4 clones after B1 failed was sound: the detector reads only line geometry and text,
which the split leaves unchanged. Running 19d before UA-040 was answered went against P13. It gave useful evidence, but
it spent the 20 test sides, and the review's diagnostics spend them further: every later entry test needs fresh gold.
19b's `git stash`/`pop` while 19a worked in the same tree did no harm, but it could have lost a parallel session's
edits. The 19f/19g briefs forbid it.

**Findings of the review:** F13-R.1 to F13-R.8 in the *Findings* table.

**Review of steps 19f and 19g** (Opus 5.5, 2026-10-28; addendum, 19e still pending). Code review:
[`REVIEW.md`](../plan/phase-13-register-index-volumes/REVIEW.md), section *Steps 19f and 19g review*. Script:
[`review/review_19fg.py`](data/phase-13-register-index-volumes/review/review_19fg.py) (read-only; its subcommands are
named below). Every logged number reproduces: the suite (1,354 passed, 11 skipped), 19f's pieces-mode equality on 3471
and 3579, the interleave count (843 of 399,429, files `cmp`-equal), 707,748 layout regions before and after (no region
added, no duplicate id), 3,066 reordered regions (1,696 flagged), the B2 counts; 19g's selection table (`g_select`,
`select_lines.tsv` `cmp`-equal), the fresh test and the unchanged units on unflagged pages (`g_unflagged`, 676,744). The
fresh test was **not** re-run: `g_fresh` recomputes it from the production units of `19g-a`/`19g-z` and from
`build_units_page` on `19c`, and both agree with `fresh_lines.tsv` in all 868 cells (P 0.9530, R 0.8974, F1 0.9244).
19f and 19g findings are confirmed, except F13-19g.3 (revised).

*Is the 19g test clean?* (F13-R.9) The 19g session transcript gives the order: the 22 listings were printed
10:59–11:01 UTC, the labels written as literals and hashed at 11:02, the first code edit came at 11:02:36, `select` at
11:03:40 and the single `fresh --variant X4` at 11:04:00. The listing tool shows only line geometry and text. The 19
fresh volumes share no volume with the 30 spent sides, so the cues and the no-cut guard, which the earlier review
designed on those 30 sides, met new volumes. The reserves replaced 108 and 109 by the brief's rule. Three fresh sides
(106, 113, 117) read against their listings follow 19a's rubric. The gold was read fast (22 sides in about three minutes)
by one model reader, and no human checks entry labels, so the gold's quality rests on these spot reads.

*Is B2 a fair negative?* (F13-R.10) Yes. The verdicts follow the fixed rubric. The one case that would move the ratio to
the bar, 23 (`unclear`), is a list of deputies whose text opens at the top of the *right* column (`Zijnde ter
vergaderinge gecompareerd`), with a town list in the lower left that belongs to the right text: no left-then-right order
reads, so it is not `right`. Two `right` calls are lenient: 22 has 6 of 22 lines that merge a margin summary with body
text, so the body is torn between the `W` lines and the right run, and 07 has the right column's first entry inside a
`W` line. The rubric allows both; a stricter one would lower the ratio. The two failure kinds point to two guards, which
a later rule would need to test on fresh reads:

| Failure kind | Cases | What would catch it (not measured) |
|---|---|---|
| halves of the same written lines | 25 (row-paired left and right halves, flush left right edges at x≈1440), 30 (seven word fragments, w 110–126) | a row-pairing test (most left lines have a right partner in the same row *and* the left right edges are flush), and a minimum median line width in line heights |
| a line near the gap that belongs to neither or to the other side | 10 (centred heading `Vlaanderen / Sas van Gent.` sets the split x), 16 (`sous, ssebréé, Vleugels,` continues the left line in the same row; the left line ends at 3582, the right one starts at 3563) | the split x from lines inside the band where both columns overlap (a heading above both goes first), and a right line whose left row-mate reaches its start is a continuation |

*Does the entry rule need the reorder?* (F13-R.11) No. As a diagnostic on the spent fresh sides (no bar), X4 on the
19f reorder layout (`g_reorder`) gives P 0.9552, R 0.8951, F1 0.9242, against 0.9244 on the v4 layout; only 120 (one
TP lost) and 121 (one FP gone) change. All 20 sides are flagged in both layouts. 19g's nine FP "in regions that span both
columns" are real, but 7 of them sit in wide regions of 3–8 lines (110: 3 lines, 114: 7, 119: 8) where a gutter merge
or a heading joins a few lines of each column; the reorder needs at least 3 lines per side and keeps them whole. In 119
the regions of both columns are in one layout column, a page-level grouping problem. So UA-043 decides only the
reorder, for the 0.4% of hand units outside index entries that it un-interleaves.

*Where are the missed starts?* (F13-R.12) Counted from `19c`'s `types/segments` (`g_fn`): all 31 no-unit FN are
typed `marginalia`, and 25 of them are in 4 volumes: 3127 (10, sides 102 and 115), 3265 (8, side 121), 3280 (5),
3283 (2). In 121 the left column is cut into regions that the layout gives the `extra` role, beside a 40-line region
spanning both columns; in 117 they are one-line regions. Adding `marginalia` to `main_types` (`g_marg`, diagnostic)
gives fresh P 0.8896, R 0.9767 (FP 19 to 52) and selection P 0.8721, R 0.9553. Lemma headings and marginal notes then
start units, so this needs a design of its own (for example only in `main`-role regions) and new fresh gold.

*Side verdicts as a by-product.* Of the 22 detector-flagged fresh sides 19g read, 20 were index pages, 1 running text
(108) and 1 unreadable (109). That is independent support for the detector's precision on flagged sides, separate from
19a's labels.

**Findings of the review (19f/19g):** F13-R.9 to F13-R.12.

**Review of step 19e and phase close** (Opus 5.5, 2026-10-28). Code review:
[`REVIEW.md`](../plan/phase-13-register-index-volumes/REVIEW.md), section *Step 19e review and phase close*. Script:
[`review/review_19e.py`](data/phase-13-register-index-volumes/review/review_19e.py), outputs in
[`review/19e/`](data/phase-13-register-index-volumes/review/19e/). Every 19e number reproduces: the suite (1,357 passed, 11
skipped), `--check-only --version v5` (231 inventories), `compare_v4.json`, the audit (0 judged, the 4 v5 checks pass), all
`metrics_*` files and the silver files (`cmp`-equal; the scripts were run from copies writing into the scratchpad). The
UA-045 commands parse (`parse_args` only, with placeholder paths). F13-19e.1–.4 are confirmed, .5 and .6 revised.

*How much do the detector's false positives cost?* (F13-R.13) The entry rule cuts every flagged side, so a false positive
re-cuts a page that is not an index. Every flagged side that is known not to be an index page was collected: GT
`resolution_page` sides in the 42 GT volumes, the 18 silver negatives, the `not index`/`unreadable` reads of 19a and 19g, and
the flagged sides of 11 non-index volumes that the review read (first lines of each side; 11182 scan 0529 recto, a table of
years and folios, was left out). That gives 54 sides in 18 volumes, 30 flagged by the side rule and 24 by gap fill:

| Kind | Volumes (flagged sides) | What the entry rule does there |
|---|---|---|
| rosters with page-like pairs | 4831 (10), 8287 (1) | every `name 89, 1264.` line becomes a unit (v4 typed most of them `index`) |
| accounts | 8706 (15), 3199 (6), 3315 (5), 3320 (2), 3307 (1), 11182 (4) | lines ending in amounts (`54016: 3: 0`) are cut one by one |
| letters, running text | 359, 695, 751, 882, 3221, 4170, 11220, 11281 (1 each) | cuts at short lines and outdents, mostly where a paragraph starts anyway |
| garbled scans | 11930, 3525 (1 each) | junk units of a junk page |

On these 54 sides `main_text` units go from 228 to 1,091, 207 v4 `index` units are re-cut into mains and 99 of the 228 v4
units are split; 1,992 lines are involved. That is 0.6% of the 8,873 flagged sides and 0.18% of the 481,391 hand `main_text`
units. No text is lost or moved: the lines, their order and the layout are v4's. It is a lower bound, since only the GT
volumes are fully labelled; reads of flagged sides outside the GT give 4 non-index of 66 (19a 1 of 24, 19g 2 of 22, gap fill
1 of 20), so up to a few hundred such sides in the dev build. Outside the GT volumes and the half-year registers the
detector flags 28 of 25,158 sides falsely (about 1 in 1,000), mostly isolated sides; the series with many (8706 accounts,
4831 rosters) share the index cue, a number at the end of most lines. **Judgement:** acceptable for a v5 pilot and the full
run, as a documented limit (`docs/text_layer.md` names it). A guard (a repeated end pair as in 4831, amount-shaped line
ends, or no isolated side in a volume without a run of flagged sides) needs its own bar on fresh reads; it is a follow-up,
not a fix.

*Is the gap fill right?* (F13-R.14) Gap fill flags 3,648 of the 8,873 sides; 2,626 of them are in the index volumes of the
entry gold, 363 in GT volumes and 659 elsewhere. Of 20 gap-filled sides drawn from 658 of those 659 (one was a 19a read) (17 volumes, seed 20261130;
[`gapfill_verdicts.tsv`](data/phase-13-register-index-volumes/review/19e/gapfill_verdicts.tsv)), 17 are index pages (12
dated summary indexes like `16 Februarij. Prop: Utrecht om ...` under a lemma, 5 page-reference indexes), 2 hold only a
page number, and 1 is an account page (8706 scan 0173 recto, ration accounts cut line by line). With 19e's sample (4 of 5)
the gap fill is right on 21 of 25 and harmful on 2. It matters most for the one-column dated summary indexes of 3463 and
3472: their sides have no measurable column share, so the side rule fires only now and then and the fill covers the rest.

*What do the user's `unsure` sides say?* (F13-R.15) The three sides the user marked `unsure` are 17 (3712 scan 0026 verso:
lemma *Gemeene Middelen*, dated summaries, no page numbers; "I think these are index entries for the main term (also called
'respect') ... organised by date"), 23 (3498 scan 0128 verso: the same style under *Spagne*, garbled HTR; "maybe index
entries organised by date?") and 54 (3489 scan 0045 recto: six lines, *Denemarken* / *Resident Coijmans* / *Conventie over de
Achterstallen*, "I see no referencing locator"). None is a disagreement; all three ask what counts as an index entry. The
rubric took a date as a locator (the resolution of that day) and judged a side by its structure when the HTR lost the
locators. The user's notes point at a labelling question for later work: (1) write the definition into the rubric and the
check page (lemma or respect term + locator, where a locator is a page number *or* a date; structure decides when locators
are lost); (2) the two index styles (page-reference vs dated summary, F13-19a.3) may deserve a per-side label, since their
entry starts differ and downstream readers may want to tell them apart. D2 does not hinge on them: over the 57 sides the user
judged, 52 are right (0.912; 23 is one of the detector's misses).

*Run kit.* (F13-R.16) The full run's final audit was the 15w audit, which does not know the v5 invariants. The review
pointed `AUDIT` at the 19e copy (identical checks on a v4 layer, plus the v5 checks on a `*_v5` layer; both took about
223 s on the 231-volume build), so UA-045 needs no extra step. The UA's smoke command also needs `<drive>/layer_v5_smoke`
to exist (reviewer note on UA-045).

*Remaining interleave.* 722 of the 728 interleaved `main_text` units in v5 sit on unflagged sides, where the units equal v4
(margin summaries beside body text, minuten); on flagged sides interleaving falls from 1,890 to 6. The reorder (UA-043,
not shipped) is the rule for the rest.

**Findings of the review (19e):** F13-R.13 to F13-R.16.

**Candidates for `docs/findings.md`** (not edited; for someone reusing AS): F13-P.1 with F13-19c.2 and F13-R.6 (index page
sides can be found from PageXML geometry and line ends alone: two columns plus entry shape, GT test P 0.97, R 0.95);
F13-19g.1 with F13-19a.3 (index entries start at a hanging outdent, a leading date or after a page reference; a print
paragraph rule fails there, F13-R.2); F13-R.5 (v4 already types whole index pages `index`, so such lines are in no main
unit unless a rule brings them in); F13-R.13 with F13-19e.4 (the index cues also fire on rosters and accounts: lines that
end in numbers); F13-R.9 (gold is spent once a variant is scored on it; label and hash fresh gold before any code runs);
F13-19f.2 with F13-R.10 (column order inside one hand region is a negative result so far: start-x reordering fails on
halves of written lines and on headings in the gap).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F13-19b.2, F13-R.7, F13-19f.2 | Column order v2 (reorder): done in 19f; B2 failed (35/39); not shipped | UA-043 (a), resolved |
| F13-19f.3, F13-R.10, F13-19e.5 | Column order v3: a row-pairing guard (halves of written lines, word fragments) and a split x from the band where both columns overlap (headings, same-row continuations); a new pre-registered read bar on fresh regions. It is the rule for the 722 interleaved units left on unflagged sides | open, no step (a later phase; ships only as a new layer version) |
| F13-19d.2, F13-R.2–R.5 | Entry rule v2: done in 19g; bar passed on fresh gold (X4 = `SG_INDEX_ENTRY_V5B`) | done: in v5 (19e) |
| F13-R.13, F13-19e.6, F13-19c.4 | Detector false-positive guard: rosters with a repeated end pair (4831), account pages with amount-shaped line ends (8706, 3199, 3315), isolated flagged sides in volumes without a run; needs a pre-registered bar on fresh reads, including resolution and account pages | open, no step (before or after the full run; a guard changes `layout/pages` and units, so it is a new layer version once v5 is published) |
| F13-19g.3, F13-R.12 | Entry starts in `marginalia`-typed lines (31 of 44 fresh FN, 4 volumes): `extra`-role column regions and one-line regions on index sides; `marginalia` as mains costs P (0.890) | open, no step (needs a design and new fresh gold) |
| F13-19g.4 | Continuation cuts after an amount or a short cross-reference line (10 of 19 fresh FP) | open, no step |
| F13-R.15 | Index definition for later gold: a locator is a page number or a date; structure decides when the HTR lost the locators; consider a per-side index style (page-reference vs dated summary) | open, no step (copy into the next gold rubric and check page) |
| F13-19c.5 | One-column indexes (3127, 4617) and garbled index volumes (3498, 3520, 3523, 3525) are not detected | open, no step |
| step 19e | Full v5 run of 1.01.02 on the drive; v5 pilot in `shared-data/` | UA-045, UA-046 |
| F13-R.6, F13-R.15 | Blind check of side labels | UA-039 (a), done: 27/27 agree, 3 `unsure` |
| PLAN review checklist | Silver check against `republic_segmentation_v1` | done in 19e (F13-19e.4); AS reads it by absolute path, not through a link under `data/` (CONTEXT §5) |

## Reproduce

```
# planning (reads only; from the AS root)
.venv/bin/python -I reports/data/phase-13-register-index-volumes/planning/draw_reads.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/planning/detector_grid.py fit
```

```
# step 19c (from the AS root; clones 19c-a, 19c-z built as in the section above)
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/features_check.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/select.py fit
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/select.py test   # once; refuses a second run
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/d2.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/errors_sample.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19c/flagged.py
```

```
# step 19d (from the AS root; clones 19d-a, 19d-z built from 19c-a, 19c-z with --stage units --unit-params '{"index_entry": SG_INDEX_ENTRY_V5}')
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/entry_eval.py dev
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/entry_eval.py test --variant E5   # once
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/units_stats.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19d/errors_sample.py
```

```
# step 19g (from the AS root; clones 19g-a, 19g-z built from 19c-a, 19c-z with --stage units --force --unit-params step-19g/unit_params.json)
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/write_fresh_gold.py   # rewrites the fresh gold from the hand labels: do not run again
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/entry_eval_v2.py select
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/entry_eval_v2.py fresh --variant X4   # once; refuses a second run
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/units_stats.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/errors_sample.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19g/layout_check.py
```

```
# review of 19f/19g (from the AS root; read-only on every layer; OUT = any new directory)
R=reports/data/phase-13-register-index-volumes/review/review_19fg.py
.venv/bin/python -I $R OUT f_counts      # 19f region rows, reordered regions, units
.venv/bin/python -I $R OUT g_select      # 19g selection into OUT
.venv/bin/python -I $R OUT g_fresh       # fresh test recomputed from the units (does not run the guarded script)
.venv/bin/python -I $R OUT g_reorder     # diagnostic: X4 on the 19f layout
.venv/bin/python -I $R OUT g_fn          # FN line types by side and volume
.venv/bin/python -I $R OUT g_marg        # diagnostic: X4 with marginalia as mains
.venv/bin/python -I $R OUT g_unflagged   # units on unflagged pages, 19g against 19c
```

```
# step 19e (from the AS root; build as in the section above, layer data/text_layer_v2_dev/19e-a; the scripts overwrite their step files)
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/compare_v4.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer data/text_layer_v2_dev/19e-a --out OUT/audit.json
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/metrics.py a   # then b, c, d
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/silver.py
.venv/bin/python -I reports/data/phase-13-register-index-volumes/step-19e/error_reads.py
```

```
# final review (from the AS root; read-only on every layer; OUT = any new directory)
R=reports/data/phase-13-register-index-volumes/review/review_19e.py
.venv/bin/python -I $R OUT damage                     # flagged sides known not to be index pages, v4 vs v5 units
.venv/bin/python -I $R OUT gapfill 20                 # 20 gap-filled sides outside GT and entry gold (seed 20261130)
.venv/bin/python -I $R OUT side NL-HaNA_1.01.02_8706_0173 recto   # one side in layout order, v5 unit starts marked
```
