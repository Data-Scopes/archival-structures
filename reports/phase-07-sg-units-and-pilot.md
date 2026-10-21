# Phase 07 report: SG units, types and the pilot layer

> **Status:** final (phase review, 2026-10-21).
> **Phase plan:** [`plan/phase-07-sg-units-and-pilot/PLAN.md`](../plan/phase-07-sg-units-and-pilot/PLAN.md).
> **Code state:** AS unreleased after v0.10.0 (working tree at 73e1517 + phase-07 changes); LotA `docs/from-archival-structures.md` §6 and `shared-data` `text_layer_v2` written in 15i Part B.
> **Last updated:** 2026-10-21, by the phase review (Opus).

## Summary

From phase 06's v2 lines, AS now builds the units LotA aligns on: page layout (15fp–15fq2), line
segments with nine types (15g, 15g2), units with a legacy `doc_id` map and a REPUBLIC crosswalk (15h),
and unit text with per-line offsets (15hj, 15hj2, 15i2). The pilot for hand 1.01.02 (14 inventories)
and print 455/456 is published in `shared-data` as `text_layer_v2` (`coverage: partial`). Key results:
offsets are exact (F07-15hj.1) and every legacy id is mapped (F07-15h.1), but region merges failed
for hand and are off (F07-15fq.3). The ≥ 80% acceptance target is predicted out of reach (0.526, bound
0.560): 40 of the 51 artefact rows are transcription glue or HTR/OCR errors (F07-15i.3, .4). The review
found five defects in the published layer (F07-R.1–R.5); the largest are scrambled print line order in
455 (F07-R.1) and a stray continuation comma in 1.8k hand words (F07-R.2). Step 15ir fixes them in a
dev layer.

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F07-P.1 | Hand 1.01.02 (3351, 3354, 3771, 11182 checked) and print 456 have only `structure {type:Text}` on every TextRegion, so there are no layout labels; print 455 has Laypa types | data-property | sg, layout, region-types | confirmed | [Planning](#planning) |
| F07-P.2 | In 455, most typed regions are empty: only 6 of 204 `date`, 111 of 1,149 `header` and 122 of 934 `page-number` regions carry a TextLine | data-property | sg, print, region-types | confirmed | [Planning](#planning) |
| F07-P.3 | Sibling regions with lines overlap (bbox ≥ 0.5 of the smaller) on 78/219 scans of 3351, 124/579 of 3771, 320/730 of 456 and 704/721 of 455, where `index`×`resolution` pairs dominate (4,350) | data-property | sg, layout, overlap | confirmed | [Planning](#planning) |
| F07-P.4 | republic's `merge_overlapping_text_regions` runs unchanged under pagexml-tools 0.9.0 and keeps every line; regions go 4,146 → 4,017 (3351, 199 scans) and 1,604 → 645 (455, 120 scans) | result | sg, layout, republic-port | confirmed | [Planning](#planning) |
| F07-P.5 | AS's `classify_inventory_structure` labels every SG scan `odd_shaped` (inventory `mixed`), because 2 covers in 219 scans fall below `min_share=0.15` | open-issue | sg, opening-detection | confirmed | [Planning](#planning) |
| F07-P.6 | On landscape hand scans, AS's `get_separation_point` and the widest central zero-coverage gap differ by > 50 px on 127/199 (3351) and 243/298 (11182) scans, against 25/297 in print 456, so the `page_w/2` page side is suspect in hand | open-issue | sg, page-split | confirmed | [Planning](#planning) |
| F07-P.7 | pagexml-tools 0.9.0: `PageXMLPage` ignores its own `text_regions` (and `.stats` raises KeyError on them), `set_parentage` skips `extra`, `merge_columns` sets `doc_id` not `id`, `add_to_pagexml` writes no columns, `copy_column` drops tables | open-issue | pagexml-tools | confirmed | [Planning](#planning) |
| F07-P.8 | republic's date-header detection is not portable to SG: it needs a neural `line_class`, `date`/`attendance` region types and REPUBLIC inventory metadata, none of which SG has | method | sg, date-headers, republic-port | confirmed | [Planning](#planning) |
| F07-P.9 | The user's pagexml-tools branch `bug_fixes` (`99df1e3`, `16a51d0`) fixes six of the seven traps of F07-P.7/F07-15fp.11, but `PageXMLPage.stats` raises `TypeError` on every page (`len()` of an int), and AS's print splitter calls it (`line_split.py:1059`) | open-issue | pagexml-tools | confirmed | [Planning](#planning) |
| F07-P.10 | In the hand minutes the HTR often merged a page's date header, first text line and page number into one TextLine ("28. Junij verthoonen, … 909."): the top line ends in a number on 35–39% of pages in 3349–3353 (96–97% of recto ones in the page-number chain) and starts with a date on 142–497 pages per inventory | data-property | sg, hand, types, htr | confirmed | [Planning](#planning) |
| F07-P.11 | A gutter at the verso's main-text right edge (guarded) fixes all 14 margin-column cut-offs and the whole-page miss among 15fp's 60 read page-split scans, and changes 5 of 44 scans judged right (all 11182/11281, a judgement it puts in doubt) | method | sg, hand, page-split | confirmed | [Planning](#planning) |
| F07-P.12 | The share of openings with a zero-coverage gap separates the layouts: 0.984–1.0 in 3354, 3771–3823, 455 and 456, and 0.072–0.874 in 3349–3353, 11182 and 11281 | data-property | sg, page-split | confirmed | [Planning](#planning) |
| F07-P.13 | A text-pattern furniture rule (page number, date, year; ≤ 2 lines, ≤ 30 characters) flags the furniture member in all 14 hand merges 15fp read as "furniture absorbed", and one of the 4 interleaved margin notes ("1. Meij") | method | sg, layout, merge | confirmed | [Planning](#planning) |
| F07-P.14 | The ≥ 80% target (UA-001) looks out of reach on this layer: of the 51 artefact rows, about 13 notes name glued print words (repair off), 4 are misaligned (LotA re-aligns), and many are HTR/OCR errors; AS can address about 9 (hand furniture, hyphens, a column merge) plus some of the 16 `segmentation` rows (planning estimate from the notes) | open-issue | sg, lota, acceptance | confirmed | [Planning](#planning) |
| F07-15fp.1 | The layout stage keeps every input line exactly once, gives unique and (for unchanged regions) byte-identical region ids, and leaves 0 overlapping pairs on all 16 inventories; the planning overlap counts reproduce exactly (78, 124, 320, 704 scans) | result | sg, layout | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.2 | Hand-read merge precision is below the 0.90 bar: hand 23/28 = 0.82 when page furniture absorbed at a column edge counts as right (9/28 when not), print 4/30 = 0.13 | negative-result | sg, layout, merge | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.3 | Laypa's print regions are page-level blocks whose XML line order already interleaves the two text columns: 1,403 of 2,044 unchanged 456 regions span both columns, and only 30 of 2,169 print merges joined two columns themselves, so print units must split on the line `column` field | data-property | sg, print, layout, units | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.4 | In hand 3349–3353 the recto's margin column abuts the verso text, so the gutter often has no gap (823/887 openings of 3350 use `min_coverage`) and the split cuts the recto's margin notes off to the verso in 11 of 30 scans where `page` ≠ `page_side` | negative-result | sg, hand, page-split | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.5 | `min_coverage` can miss the gutter by a whole page (sep ≈ 0.34·`page_w` in 3350_0851/0856/0859/0886), which puts the verso's text on the recto; 1 of 30 sampled `min_coverage` scans, 3 of 4 checked of 19 flagged in 3350 | open-issue | sg, hand, page-split | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.6 | The fitted gutter of hand SG is off the page centre (`gutter_rel` 0.436 in 3351 to 0.557 in 11182), while 3771–3823 and print sit at 0.498–0.502 with a gap on every opening | data-property | sg, page-split | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.7 | Of 60 hand-read `extra` regions, 56 are margin notes, headings, page numbers, catch words or noise and 4 are main text (323 of 1,047 extra characters); `main` holds 94.6–99.8% of the characters per inventory | result | sg, layout, roles | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.8 | 3821–3823 are largely printed registers in narrow sub-columns (keyword, summary, date, page); the role rule puts keyword, date and page in `extra`, giving 11 extra regions per page and 163 pages with ≥ 10 in 3821 | data-property | sg, registers, roles | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.9 | 455's reduction from 9,343 to 2,953 regions comes from two overlapping Laypa label layers: 1,251 of 1,784 merges join an `index`- and a `resolution`-tagged region, and 1,058 of 1,426 pages end with one main region | data-property | sg, print, overlap | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.10 | 455's index (register) part is scans 0003–0005 only; Laypa's `index` tag sits on resolution text throughout the volume, so the `index`×`resolution` overlap (F07-P.3) is not an index-part effect | data-property | sg, print, region-types | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.11 | pagexml-tools 0.9.0 `PageXMLPage(coords=None)` with no children raises `IndexError` in `parse_derived_coords`; the layout passes the page box | open-issue | pagexml-tools | confirmed | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fq.1 | The main-edge gutter reproduces the planning counts exactly (lines moved against 15fp: 343, 1,615, 4, 14, 52, 236, 268 in 3349, 3350, 3351, 3352, 3353, 11182, 11281; 0 in the 9 gap inventories) and moves recto margin notes only: 30 of 30 read changed scans are right, and it fixes 14/14 cut-offs and the whole-page miss of 15fp's dev set | result | sg, hand, page-split | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.2 | 15fp's judgement "gutter right" is wrong for the 5 scans of 11182/11281 whose lines move (and the undecided one): each moved line is a recto margin note, as the symmetry rule predicts (verso has its own left margin column; recto text starts at 0.595 in 11182_0524) | result | sg, hand, page-split | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.3 | Hand merge precision stays far below the 0.90 bar after the furniture rule and the guard: 12 right of 23 decided (0.52; 7 ambiguous), so the fallback ran and the hand layer has no merges (15fq-b); the errors are interleaved columns (5), row fragments sorted before their line (5) and a margin item (1) | negative-result | sg, hand, layout, merge | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.4 | After the column split, print merges are mostly right: 28 of 30 (0.93, 95% interval about 0.79–0.98); the typical merge puts the lines of two source regions of one column back in y order. The two errors: a page number absorbed ("1 673 )") and one region with both columns | result | sg, print, layout, merge | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.5 | Criterion 6 is missed: 145 print layout regions (36 in 455, 109 in 456) still hold lines of both columns, because the merge joins a column-1 piece to an overlapping column-2 piece (title pages and pages where a wide line carries a column label); in 456 that is 109 of 205 merges | negative-result | sg, print, layout, units | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.6 | The text-pattern furniture rule flags 31,524 regions (27,906 hand, 3,618 print) and excluded no body text in 30 read: 19 are page numbers, running years or header dates, 11 are numeric or date margin items (kept as their own regions, as intended); 0 merged regions have a furniture member | result | sg, layout, furniture | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.7 | The interleave guard blocks 2,274 pairs (227 hand, 2,047 print) and is right 16 of 30 read (5 wrong, 9 ambiguous): it keeps empty-text noise, page numbers and margin summaries out, but also line-end fragments of print ("den.", "len.", "doen.") that continue the sentence. *Revised by the review:* the 2,274 count is inflated, because a blocked relation is counted once per merge pass under each new merged id (F07-R.5); the hand-read 16 / 5 / 9 stands | negative-result | sg, layout, merge | revised | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.8 | The print column split cuts 3,973 regions (2,171 in 455, 1,802 in 456); 17 of 20 read are column-pure and in reading order, 2 are one-line empty-text noise parts, 1 is a merged double column. Three of the 17 keep a running-head line out of y order, because the order is the source order. *Revised by the review:* the source order is a real defect in 455: 416 `split_at_column` regions jump back > 50 px, and 335 units read across the column (F07-R.1) | result | sg, print, layout | revised | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15fq.9 | In hand text the HTR often puts the last word of a line ("den", "de", "den.") in its own region at the line's row; a y-sort places it before the line it ends (5 of 11 wrong hand merges), and the guard blocks others of the same kind | data-property | sg, hand, htr, layout | confirmed | [Step 15fq](#step-15fq-layout-fixes-hand-gutter-print-column-split-furniture-interleave-guard) |
| F07-15g.1 | Every one of 1,644,839 non-blank lines in the 16 inventories has at least one segment (1,668,542 segments; 23,703 extra from the three in-line rules), with no overlaps and texts that rejoin to the line text | result | sg,types,r4 | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.2 | All six R4 test rows of the 02d sheet reproduce (rows 50, 52, 55, 134, 19, 9): the date header is its own segment, the page number is split off the top line, marginal notes are `marginalia`; the brief's first-space cut did not reproduce row 19 ("Augt."), the space nearest the main left edge does | result | sg,types,r4,lota | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.3 | One main-text reference per page mistypes two-column pages: 24.6% of 3771's segments and 27.1% of 455's were `marginalia` (33,638 and 47,100) until each main column got its own left edge, then 1.3% and 1.1% (1,781 and 1,848); in print the line `column` field must replace the layout column, because 145 layout regions hold both columns (UA-008): 456's `margin_prefix` cuts fall from 4,574 to 394 | method | sg,types,print,layout | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.4 | Type precision (seed 17, 20 per type): page_number 19/20 (0.95), date_header 13/13 decided (7 more are date entries on 3821-3823 register pages), running_header 20/20, catch_word 20/20, signature_mark 20/20, index 19/19, other 20/20, and marginalia only 7/13 (0.54; 6 ambiguous) | result | sg,types,r4 | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.5 | `rule:margin_prefix` is a negative result: 18,983 lines are cut (1.1%) and 2 of 9 decided cuts are right (0.22); `rule:date_prefix` (1,745 lines) and `rule:pn_suffix` (2,975 lines) are 10/10 right each | negative-result | sg,types,hand | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.6 | The page-number chain finds a number on 72% (3349) to 98% (456) of rectos; 12,155 `pn_line` pages split 10,363 in-chain and 1,792 (14.7%) off-chain, and a running year read with a wrong digit ("2767", "4716.") is typed `page_number` in 1 of 20. *Revised by the review:* the "in-chain" `rule:pn_line` count includes about 1,414 band lines that were never chain candidates (F07-R.4) | data-property | sg,types,page-numbers | revised | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.7 | The index experiment survives: 212 pages are typed `index` (registers and name lists in 3349-3353 and 3821-3823), 14 of 15 decided pages are right (0.93, bar 0.80), so `index_rule` stays on; the check for missed register pages found 1 candidate, a prose page | result | sg,types,index | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.8 | Furniture left in `main_text` in the band lines: 0 of 20 in hand and 3 of 20 in print, all page numbers read with extra OCR digits ("11323 )", "111475 )", "1 605 )"); date headers misread ("Den 13. Joun.", "Dont 22. Maert.") also stay `main_text`/`marginalia` | data-property | sg,types,print,ocr | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.9 | 3,688 segments are `other` (single characters and digits); 7 of 20 read are drop capitals ("N" of "Ntfangen", "K", "R") that belong to the next line's word, so the unit text loses a first letter | data-property | sg,types,htr | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15g.10 | Laypa's tags in 455 predict types badly: 138,691 of 140,701 segments in `index`-tagged lines are `main_text`, but `page-number`-tagged lines are 110 of 129 `page_number` and `catch-word` lines 832 of 1,117 `catch_word` | data-property | sg,types,print,laypa | confirmed | [Step 15g](#step-15g-line-and-region-types) |
| F07-15fq2.1 | Criterion 6 now holds: 0 print layout regions carry both `column` values (was 36 in 455 and 109 in 456). The 145 mixed regions are replaced by 303 new ones (81 and 222), 0 other regions are lost or new, and every new region's sources lie within one old mixed region; the guard blocks 62 (455) and 113 (456) pairs. *Revised by the review:* 62 / 113 count relations once per merge pass and are overstated (F07-R.5); the 0 mixed regions stand | result | sg, print, layout, merge | revised | [Step 15fq2](#step-15fq2-column-merge-guard) |
| F07-15fq2.2 | Of 30 formerly mixed regions read, 27 are right, 3 ambiguous (a title page, a garbled page, a 4-line tail piece) and 0 wrong; both print merge errors of 15fq (`455_0345`, `456_0098`) are fixed. The page-number line that carried the other column's label (11 of 15 in 455) is now its own region | result | sg, print, layout, merge | confirmed | [Step 15fq2](#step-15fq2-column-merge-guard) |
| F07-15fq2.3 | The guard leaves 146 overlapping column-conflicting pairs that are neither furniture nor interleave-blocked (39 and 107); over all column-conflicting overlaps (198), 113 are two full columns whose hulls overlap and 78 have a side of at most 3 lines, 56 of those not page numbers: mostly line-end fragments the HTR placed at a line's row and labelled with the other column ("den.", "sen.", "doen."). They were joined to their line before; now they stand alone | negative-result | sg, print, layout, htr | confirmed | [Step 15fq2](#step-15fq2-column-merge-guard) |
| F07-15fq2.4 | 15g's print types barely move: 53 (455) and 106 (456) of 348,457 segments change type, `main_text` share 0.9921 and 0.9923 (was 0.992); 103 of 456's changes are `marginalia` to `main_text` (lines no longer in an `extra` role region) and 45 of 455's are page-number lines ("1 985 )") from `main_text` to `marginalia`; hand types are identical | result | sg, print, types | confirmed | [Step 15fq2](#step-15fq2-column-merge-guard) |
| F07-15g2.1 | Keeping only month-only margin cuts removes 18,552 of 18,975 `rule:margin_prefix` cuts (423 stay, all `date_header`); `marginalia` segments fall from 82,935 to 64,487 (-18,448) and nothing else moves by more than 69; 0 unchanged lines differ | result | sg,types,hand | confirmed | [Step 15g2](#step-15g2-margin-cut-month-only) |
| F07-15g2.2 | The cost of (b) is small: of 30 formerly cut lines read (seed 22), 18 are right as main text, 10 right as marginalia, 2 ambiguous and 0 wrong; most removed prefixes are the first words of attendance-list lines | result | sg,types,hand | confirmed | [Step 15g2](#step-15g2-margin-cut-month-only) |
| F07-15g2.3 | The two cuts 15g judged right (3353_0318, 3349_0380) are lost (now one `main_text` line), and 26 lines gain a `pn_suffix` split ("1 657": `page_number` + `page_number`) because they are no longer cut first | negative-result | sg,types,page-numbers | confirmed | [Step 15g2](#step-15g2-margin-cut-month-only) |
| F07-15h.1 | R5 holds on all 16 inventories: every one of the unchanged non-empty layout regions (0 exceptions) keeps its legacy `doc_id` as a `same_id` unit; the 300 `hand_doc_id`s of the 02d sheet are all `old_doc_id`s (214 `identical`, 86 `trimmed`, 0 `changed`) | result | sg,units,r5 | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.2 | Hand legacy regions mostly survive: of 154,021, 129,686 are `identical` (84.2%), 21,523 `trimmed` (furniture or margin notes pulled out), 37 `changed`, 2,775 empty; print does not: of 9,343 non-empty 455 regions 4,344 are `identical`, 499 `trimmed`, 4,500 `changed` (48%), and in 456 1,031 / 124 / 1,854 (of 3,009), because the layout split the columns and merged the pieces | result | sg,units,hand,print | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.3 | The print paragraph cues fitted on 455 (even scans) reach dev F1 0.784 (P 0.724, R 0.855) at `indent_frac=0.5, short_frac=0.1, gap_factor=None`, but test F1 0.698 (P 0.688, R 0.708): 455 odd scans 0.789, 456 0.655. The best point is at the grid edge and `header` alone gives F1 0.02. *Revised by the review:* no leakage into the cues, but the `split_half` crosswalk class that builds 40% of 456's reference was added after looking at 456 (test), so the 456 test F1 is not fully blind | result | sg,units,print,republic | revised | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.4 | REPUBLIC is itself the main error source in print: of 20 false negatives read on test, 14 are REPUBLIC paragraph starts mid-sentence (lowercase or mid-word continuations, at column or page tops in 456), and only 6 are real starts the cues miss (no indent, full previous line); of 20 false positives, 17 are cue errors (continuations over the 0.5-line indent, presence-list short lines, OCR noise) and 3 plausible or reference artefacts | negative-result | sg,units,print,republic | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.5 | The crosswalk maps 99.9% of REPUBLIC line ids (455: 27,033 exact, 1,864 near, 189 split_original, 55 split_half, 7 unmatched; 456: 20,192, 13, 512, 13,848, 23). The brief's rules leave 13,871 of 456's 34,588 ids unmatched, because REPUBLIC splits merged print lines itself and its halves differ from ours by 10-30 px in width; the added `split_half` rule (IoU >= 0.5 with one of our halves) recovers them | result | sg,units,print,republic | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.6 | Print units are small: median main unit is 2 lines (hand: 8-10), and 9,335 of 21,731 (455) and 7,804 of 19,763 (456) main units are one segment, of which 841 and 1,398 have at most 6 characters (line-end fragments, drop capitals, single presence-list names); the any-cue rule cuts every short presence-list line | negative-result | sg,units,print | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.7 | Row 80 of the 02d sheet is fixed: "Noch wert alhier gebragt het extrs:" (unit_order 24) and "in te huuren." (21) are in different `hand:region` units of 3353_0754, not adjacent | result | sg,units,hand,lota | confirmed | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15h.8 | New unit ids collide with a taken id (a unit, layout region or legacy region of the scan) 58 times in 455 and 32 in 456 and never in hand; the brief expected 0. All are print units whose bbox equals a legacy region's box that is not one of the units; the id then gets `w+1`. *Revised by the review:* 13 of 455's 58 hit a layout-region or unit id; the other 45 and all 32 of 456 are legacy twins that hold every line of that region and could keep its id, so 77 regions map `changed` needlessly (F07-R.3) | open-issue | sg,units,ids | revised | [Step 15h](#step-15h-units-reading-order-and-the-doc_id-mapping) |
| F07-15hj.1 | Unit text carries exact offsets: over all 234,581 units and 1,650,009 segments of the 16 inventories 0 offset violations (also against `lines.text[line_offset:...]`), `join_unit` equals `join_region` in 0 of 234,581 units, and for the 129,686 hand units whose legacy region is `identical` the guard-free join equals the legacy region text in 0 failures | result | sg,join,offsets,r1 | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.2 | The `„`-robust statistic is not adopted: default and robust decisions are identical in all 16 inventories, so the labelled-sample test was not run (spec step 2). Robust raises `end_rel_frac` of `„` by 0.0-13.8 (largest in 11182: 40.9 to 54.7, 1,514 of 6,008 `„` are in line-start runs or between digits) but every `„` already clears 5.0; the misses (3351, 3354) are on `join_vocab_frac` | negative-result | sg,wbc,robust,f05-r4 | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.3 | The SG sanity anchors fail on pooled counts for hand_local (`:` passes the rule) and print (`.`); hand_tar passes. The derived sets are `-=„` for most hand inventories, `-.=„` for 3822 and 455, `-:=„` for 11281; `join` needed `--ignore-sanity`. Read samples: `.` right 11 of 20 (455) and 4 of 10 (3822), `:` right 9 of 10 (11281, the scribe's hyphen), `=` right 7 of 10 | open-issue | sg,wbc,sanity,ua-010 | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.4 | The word-break rule misses `„` in 3351 and 3354 (11,039 and 9,841 lines; `join_vocab_frac` 0.12 and 0.08, vocabulary too small): both get an empty set, so their 350 `„` line ends stay unmerged; 12 of 12 read are genuine breaks ("satis„" + "militio" is OCR-garbled, the rest clean) | negative-result | sg,wbc,hand | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.5 | Share of merges whose right side starts lower case (target >= 0.95, reported): hand overall 0.954 (215,071 of 225,534); per inventory 0.990 (3349) to 0.699 (11182); below 0.95: 3822 0.779 (0.957 without `.`), 11182 0.699, 3823 0.941; print 0.901 (455 0.876, 456 0.931). *Revised by the review:* 11182's low share is mostly not wrong merges: 1,305 of its 1,416 non-lower-case right sides are a `,`, the scribe's repeated break mark at the line start (lower or punctuation 0.977); the comma stays inside the word (F07-R.2) | result | sg,join,wbc | revised | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.6 | 316,301 merges, 85 of which remove two break chars (`--`); 0 digit-digit merges (149 `g_digit` boundaries kept apart). 17,128 merges (5.4%) have an upper-case or digit right side, and 15 of 20 read are wrong (a sentence or paragraph start after a hyphen, or a hyphenated compound "Chur-Saxische"), so a `g_upper_right` guard is worth testing; it needs a change to `join.py` (not allowed in 15hj) | open-issue | sg,join,guards | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.7 | The guards (4,418 guarded boundaries, 2,515 `g_digit_left`, 1,534 `g_digit_right`) are right in 11 of 20 read, wrong in 6 and unclear in 3; all 6 wrong are genuine breaks whose letters the HTR read as digits ("a1-" + "daar", "a5-" + "tervolgens", "Con„" + "101") | data-property | sg,join,guards,htr | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj.8 | 5,562 of 41,494 print main units (13%) end in `-`; the word continues in the next unit or page, and units never cross (10 listed in `ea_verdicts.tsv` for 15i, 3 of them one-line fragments) | data-property | sg,print,units,join | confirmed | [Step 15hj](#step-15hj-word-breaks-and-unit-text-with-offsets) |
| F07-15hj2.1 | Pinning the SG word-break sets (`-=„`, 11281 `-:=„`) makes `sanity_pass` true in all three sub-layers and `join` runs without `--ignore-sanity`; the unpinned pooled rule still fails for hand_local and print (`pooled_rule_sanity_pass` false). The 15hj-b tables outside `wbc/` decision files and `join/` are byte-identical to 15hj-a | result | sg,wbc,sanity,ua-010 | confirmed | [Step 15hj2](#step-15hj2-pinned-wbc-sets) |
| F07-15hj2.2 | Removing `.` from the sets: 3822 goes from 21,486 to 10,929 merges and the lower-case right-side share from 0.779 to 0.957; 455 from 50,164 to 38,932 and from 0.877 to 0.891. Of 10 `.` merges read in each, the removal is right in 6 of 10 (455) and 7 of 10 (3822); the 7 wrong removals are genuine breaks ("on."+"fangen", "hon."+"derd", "Bur."+"germeelter") | result | sg,wbc,print,hand | confirmed | [Step 15hj2](#step-15hj2-pinned-wbc-sets) |
| F07-15hj2.3 | In hand_tar the pin adds 1,121 merges on newly pinned chars (`=` 821, `„` 297, `-` 3); the `=` merges have a lower-case right-side share of only 0.74 (611 of 821), the `„` merges 0.95. Of 20 read, 11 are real breaks, 5 glue two words ("aen=" + "haer", "missiven=" + "vanden") and 4 are unclear: in these volumes `=` often marks compounds or is a stray mark | result | sg,wbc,hand,precision | confirmed | [Step 15hj2](#step-15hj2-pinned-wbc-sets) |
| F07-15hj2.4 | 3351 and 3354, which the rule left with an empty set, now have 174 and 126 merges (lower-case share 0.943 and 0.968), 3351 mostly `„` (171); in 11281 the pinned `:` keeps 5,216 merges, 5 of 5 read right | result | sg,wbc,hand | confirmed | [Step 15hj2](#step-15hj2-pinned-wbc-sets) |
| F07-15i.1 | The pilot builds in one pass (hand 14 inventories 2:31, print 2 inventories 4:44, 8 workers, exit 0) and reproduces 15hj-b byte for byte in every table of `scans/ regions_meta/ lines/ layout/ types/ units/ join/` except the `member`/`source` path of the 8 local hand inventories (now the symlink path under `15i-src`); `wbc_set` is identical, only `char_status` strings differ (`-:default/pinned_in` for hand_tar) | result | sg,pilot,reproducibility | confirmed | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i.2 | Audit: 176 of 182 invariants pass; the 6 failures are the reported lower-case right-side share of merges below 0.95 (3351 0.943, 3823 0.941, 11182 0.699, 11281 0.946, 455 0.891, 456 0.931), all known from 15hj/15hj2 (upper-case and digit right sides need a `join.py` guard). *Revised by the review:* in 11182 (and partly 3352, 11281) the cause is a line-initial `,` continuation mark, not upper case (F07-R.2). All 300 02d `hand_doc_id`s are in `doc_id_map`; LotA gold 02-/02c- cover none of the pilot inventories | result | sg,pilot,audit,join | revised | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i.3 | 116 rows: hand tokens found for 111, print tokens for 110 (3 of them only in a wider scan search). Predicted share free `(unchanged_free + fixed) / 116` = 61/116 = 0.526 (55 + 6), upper bound with `needs_realignment` 0.560, against the target 0.80 and the old baseline 0.56. The 80% target is not reachable on this layer | result | sg,pilot,rows116,ua-001 | confirmed | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i.4 | Of the 51 artefact rows 40 persist: 26 `persists_glue` (missing whitespace in print or hand; repair is off, UA-003) and 14 `persists_htr_ocr`; only 6 are `fixed`, 4 `needs_realignment`, 1 `unclear` (a Dutch/French column merge in one unit). The layer's gains are unit boundaries (furniture, marginalia, stray lines, over-long paragraphs separated), not token repair | result | sg,pilot,rows116,glue | confirmed | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i.5 | The 10 non-artefact rows predicted `changed` are mostly improvements: 6 print token strings lose a line-end hyphen plus stray capital ("byre - Jductie" -> "byreJductie") and 3 hand strings lose a page number that sat inside the text ("incon, 823. formiteijt", rows 55, 52, 19); LotA re-labels them | result | sg,pilot,rows116 | confirmed | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i.6 | `unchanged_free` is a weak test for short strings: 6 of the 55 have a token of at most 3 characters ("et", "28", "off"), which occurs anywhere in the candidate units; the real check is LotA's re-alignment | open-issue | sg,pilot,rows116,method | confirmed | [Step 15i](#step-15i-pilot-build-audit-116-row-evidence-and-publication) |
| F07-15i2.1 | Dropping `=` from the pinned set of 3349, 3350, 3351, 3353, 3354 removes 820 merges in 3349/3350/3353 (276, 226, 318; 818 of them at `=`) and 3 in 3351; 3354 loses none (its `=` line ends were all guarded). `-` and `„` merges are unchanged apart from 1 `„` merge in 3349. Lower-case right-side share of the three inventories rises from 0.9806 to 0.9887 (hand_tar: 0.9754 to 0.9819) | result | sg,pilot,word-breaks,ua-011 | confirmed | [Step 15i2](#step-15i2-hand_tar-pin-without-) |
| F07-15i2.2 | The change is local: scans, regions_meta, lines, layout, types and units are byte-identical to `15i-a` in all 14 hand inventories, `join/` differs only in the 5 inventories whose pin changed (not in 3352 or the 8 hand_local), print is byte-identical, the audit passes the same 176 of 182, and the 116-row evidence is unchanged (0 of 116 rows differ; share free 0.526, bound 0.560) | result | sg,pilot,reproducibility,rows116 | confirmed | [Step 15i2](#step-15i2-hand_tar-pin-without-) |
| F07-15i2.3 | Cost of option (b): of 20 removed `=` merges (3349, 3350, 3353, Random(24)) 11 are right to split (two words, e.g. "Pottebackers"+"Aerde", "aen"+"haer"), 6 are wrong to split (compounds broken again: "Lant|zaten", "Scheeps|vaert", "Transport|schepen"), 3 unclear. Fewer errors than the 11/20 right-to-merge of 15hj2 (5 wrong, 4 unclear), on a different sample and a single reader | result | sg,pilot,word-breaks,error-analysis | confirmed | [Step 15i2](#step-15i2-hand_tar-pin-without-) |
| F07-15i2.4 | Brief defect carried as a fact: 3351 and 3354 do have `=` line ends (3 merges in 3351; 3 `=` ends in 3354, one `g_digit_right` guarded boundary that moved), so their `join/` changes slightly although the brief expected none | open-issue | sg,pilot,brief | confirmed | [Step 15i2](#step-15i2-hand_tar-pin-without-) |
| F07-R.1 | Column-split print pieces keep the XML line order, so units read across the column: in 455, 546 layout regions jump back > 50 px (416 `split_at_column`, 127 `unchanged`) and 335 of 28,079 units do (456: 160, mostly drop capitals); rows 153, 185 and 275 of the 116 sit in such units | open-issue | sg, print, layout, units, reading-order | confirmed | [Review](#review) |
| F07-R.2 | The hand scribes repeat the break mark at the start of the next line, which the HTR reads as `,`: 1,305 of 4,699 merges in 11182 (336 in 3352, 170 in 11281) leave a comma inside the word ("gero,ijeert"); with punctuation counted 11182's right-side share is 0.977, not 0.699 | data-property | sg, hand, join, htr | confirmed | [Review](#review) |
| F07-R.3 | Units reserve every legacy id, so a unit that is the twin of a legacy region (same box, all its lines) gets `w+1`: 45 of 58 collisions in 455 and 32 of 32 in 456; those 77 regions map `changed` instead of keeping their id | open-issue | sg, print, units, r5 | confirmed | [Review](#review) |
| F07-R.4 | Page-number tags: 1,414 band lines that were never chain candidates keep the in-chain tag `rule:pn_line`, about 87 date-prefix remainders become `page_number` without the chain, and rule 1 splits 19 whole date lines with a year ("28. Junij 1650.") | open-issue | sg, types, page-numbers | confirmed | [Review](#review) |
| F07-R.5 | The layout's `n_guard_blocked` / `n_column_blocked` count a blocked relation once per merge pass under each new merged id (one relation gives 3 entries in a probe), so 1,322 / 62 (455) and 673 / 113 (456) are overstated | open-issue | sg, layout, merge | confirmed | [Review](#review) |
| F07-R.6 | No leakage in the 15h cue fit: features are per-region geometry, the grid runs on 455 even scans only, test is scored once, and units are built before REPUBLIC is read; the `split_half` reference class was, however, designed after looking at 456 (test) | method | sg, units, print, leakage | confirmed | [Review](#review) |
| F07-R.7 | The review re-run reproduces the phase: AS 1,101 passed, NLP 56, LotA 367 (+1 xfail), v1 `TOTAL 157 / 157`, audit 176 / 182 with the same 6 failures, 0 of 116 × 33 row cells differ, and the published copies equal `15i-b` apart from the `publication` key | result | sg, pilot, reproducibility | confirmed | [Review](#review) |
| F07-R.8 | The published `text_layer_v2` was built at git head 73e1517 with 8 dirty files and records `as_version` 0.9.0; its code has no commit until the user commits the phase-07 tree | open-issue | sg, pilot, provenance | confirmed | [Review](#review) |
| F07-R.9 | The suite stays green with the page-number chain, `merge=False`, the furniture `exclude` or the pin application broken: no test exercises them | open-issue | tests, types, layout, wbc | confirmed | [Review](#review) |

## Steps

### Planning

Measured on 2026-10-18 with scratch scripts that stream the PageXML (regex for counts, and
pagexml-tools 0.9.0 for the runs). The inputs were the shared-data archives for 3351, 3354
and 455/456, and the AS-local directories for 3771 and 11182. The scripts are not in the repo;
step 15fp reproduces the numbers from the v2 tables.

- **Region types (F07-P.1, P.2).** 455 has 140,094 TextRegions, typed as follows:
  - `resolution` 91,098; `index` 37,320; `separator` 4,674; `catch-word` 2,485;
  - untyped 1,261; `header` 1,149; `signature-mark` 969; `page-number` 934; `date` 204.

  Most are empty noise boxes. Scan 0037, for example, has 147 regions, of which 131 have no
  line. Of the 189 overlapping pairs in that scan, 3 share an overlapping line.
- **Overlap (F07-P.3).** Overlap was counted only between regions that carry lines, because
  empty boxes inflate it: with the empty boxes included, 455 has 130,695 `index`×`resolution`
  pairs. As the user noted, the index part of a resolution book is where regions overlap most.
- **Original code (F07-P.4).** Republic's merge ran on 120 scans per inventory (199 in 3351),
  with all regions with lines in one column, and raised no errors. The other counts:
  - 3771: 2,509 → 2,456;
  - 456: 461 → 396.

  pagexml-tools `split_scan_pages_with_separation_point` also ran cleanly. On raw scans,
  though, it builds one column per empty region: 4,658 verso columns on 120 pages of 455.
- **Openings and gutter (F07-P.5, P.6).**
  - SG scans are landscape spreads, except about two portrait covers per volume.
  - In 3351, the widest central gap has a median offset of −173 px from `page_w/2`, ranging
    from −477 to +437. In 3771 it is +0.5, in 456 +12 and in 455 +0.25.
  - Neither separation method is validated; 15fp does that.
- **What was not ported (F07-P.8).** Republic's `line_class` comes from `NeuralLineClassifier`
  (LSTM + GysBERT), not from the PageXML. Its date classifier and mapper need `date`
  regions, `inventory_num` on every line and the inventory's year range.

**Phase planning (2026-10-20).** Measured with scratch scripts on the 15fp tables, and on LotA's
gold sheet (read only). The scripts are not in the repo; 15fq and 15g re-measure the numbers.
- **pagexml-tools (F07-P.9).** The local copy was run without installing it (`PYTHONPATH`, no
  bytecode). `PageXMLPage.stats` raised `TypeError: object of type 'int' has no len()` on a page
  with columns, with `text_regions` and with `extra`, at `16a51d0`. The page split, parentage,
  `merge_columns`, `add_to_pagexml`, copy and transform fixes are in place.
- **Merged top lines (F07-P.10).** These lines resolve 5 of the 7 R4 test rows. Row 50 is
  3349_0945: "28. Junij verthoonen, als het sullen requireren. 909." (x 0.560–0.903, h 168 against
  about 130 for a text line). Row 134 is 3349_0601: "21. April Is ter vergaderinge gelesen de
  reqte. 565.". Rows 52, 55 and 19 are alike. Line types alone can't separate them, so 15g types
  segments. The page-number chain (`longest_increasing_chain` over one candidate per page) keeps
  635 of 657 recto suffix numbers in 3349, 639/684 in 3350, 652/677 in 3352, 579/642 in 3353 and
  441/472 in 11182. Row 80 is a reading-order case: its bottom-of-page line
  (3353_0754, y 0.917) followed "in te huuren." (y 0.423) in LotA's region stream.
- **Gutter (F07-P.11, P.12).** Rule: long lines are those with `w/page_w ≥ 0.6 ×` the inventory
  median. The separation is `min(p95 of the verso long lines' right edges, p05 of the recto long
  lines' left edges)`, with the sides split at `gutter_rel`. It needs ≥ 5 long lines per side and
  must lie within ±0.15·`page_w` of `gutter_rel`. Lines whose page changes against 15fp: 3349 343,
  3350 1,615 (most of them in the run of whole-page misses 0851–0886), 3351 4, 3352 14, 3353 52,
  11182 236, 11281 268. Without the switch of F07-P.12, it would also move 125–235 lines of
  register tables in 3821–3823 (the gutter moves to 0.41–0.47), which is why those inventories
  keep the gap rule.
- **Furniture (F07-P.13).** With patterns only, merges with a flagged member are: 3349 30/101,
  3350 28/107, 3771 134/205, 11281 420/550, 455 654/1,784, 456 216/385. Adding a position rule
  (top or bottom 6% of the page's text) also flagged genuine short last lines ("Schepen",
  "vallen."), so only the bottom-band catch word is kept.
- **116 rows (F07-P.14).** Labels: spelling 29, abbreviation 22, print_ocr_error 21, segmentation
  16, number 9, hand_htr_error 7, editorial 5, misaligned 4, unclear 3. The notes of rows 19, 52
  and 55 (labelled spelling or abbreviation) also name a page number merged into the text line.

### Step 15fp: page layout (page split, region merge, columns, roles)

**What changed.** Brief:
[`step-15fp-page-layout.md`](../plan/phase-07-sg-units-and-pilot/step-15fp-page-layout.md).
- New `analysis/page_structure.py` holds the generic code on pdm objects. It ports republic's
  overlapping-region merge (made deterministic and repeated to a fixed point) and its
  column criterion, and adds the gutter fit and page split.
- New `text_layer/layout.py` is the rows ↔ pdm adapter. It writes the `layout/` tables.
- New v2 stage `as-build-text-layer --stage layout`.
- Schemas: `docs/text_layer.md` *Layout tables (step 15fp)*.
- Run on copies of the phase-06 layers, under
  `data/text_layer_v2_dev/15fp-a/{hand_tar,hand_local,print}/`. The input tables are unchanged.

**Results.** Acceptance criteria 1, 2, 3 (0 pairs) and 6 hold on all 16 inventories:
- every input `(scan_id, line_id)` appears exactly once in `layout/lines`;
- every `layout_region_id` is unique per inventory;
- every `unchanged` row has `layout_region_id == source_region_ids`;
- after the layout, no two layout regions on a page satisfy `regions_overlap(·,·,0.5)`;
- the suite gives 1023 passed (1010 + 13 new);
- the protected hashes are identical (`15fp-protected-{before,after}.txt`).

Criteria 3–5 are reported per inventory below, from
[`acceptance.json`](data/phase-07-sg-units-and-pilot/step-15fp/acceptance.json):

| Inv | Lines | Regions in → out | Merges | Gutter splits | Passes | gap / min_cov / none | `gutter_rel` | Lines crossing (scans > 0) | Lines page ≠ `page_side` (scans) | Main char share | Extra / page |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 3349 | 92,152 | 12,093 → 11,978 | 101 | 9 | 3 | 337 / 620 / 6 | 0.536 | 1,087 (53.0%) | 426 (164) | 0.982 | 2.76 |
| 3350 | 84,789 | 9,710 → 9,661 | 107 | 85 | 2 | 64 / 823 / 7 | 0.486 | 2,567 (87.2%) | 1,996 (363) | 0.986 | 2.13 |
| 3351 | 12,598 | 4,144 → 3,994 | 112 | 0 | 3 | 173 / 25 / 21 | 0.436 | 33 (11.4%) | 2 (2) | 0.949 | 2.05 |
| 3352 | 96,844 | 11,152 → 11,031 | 105 | 2 | 2 | 660 / 363 / 5 | 0.523 | 426 (24.3%) | 107 (88) | 0.985 | 2.18 |
| 3353 | 85,134 | 10,600 → 10,488 | 101 | 1 | 2 | 635 / 279 / 8 | 0.511 | 350 (21.0%) | 49 (24) | 0.984 | 2.23 |
| 3354 | 11,161 | 3,775 → 3,691 | 61 | 1 | 3 | 186 / 3 / 28 | 0.487 | 4 (1.4%) | 0 | 0.946 | 2.21 |
| 3771 | 139,321 | 11,696 → 11,474 | 205 | 0 | 2 | 572 / 0 / 7 | 0.500 | 0 | 0 | 0.995 | 2.83 |
| 3772 | 143,929 | 11,435 → 11,236 | 186 | 0 | 2 | 587 / 0 / 8 | 0.500 | 0 | 0 | 0.995 | 2.34 |
| 3773 | 147,639 | 11,649 → 11,409 | 226 | 0 | 2 | 603 / 0 / 5 | 0.502 | 0 | 0 | 0.995 | 2.32 |
| 3821 | 119,045 | 17,447 → 17,284 | 122 | 0 | 3 | 434 / 0 / 6 | 0.499 | 0 | 0 | 0.980 | 11.78 |
| 3822 | 124,679 | 17,383 → 17,193 | 148 | 0 | 3 | 458 / 0 / 5 | 0.499 | 0 | 0 | 0.982 | 10.90 |
| 3823 | 121,664 | 16,775 → 16,567 | 171 | 0 | 3 | 436 / 0 / 6 | 0.498 | 0 | 0 | 0.981 | 11.39 |
| 11182 | 57,713 | 5,473 → 5,281 | 182 | 9 | 2 | 555 / 193 / 7 | 0.557 | 406 (21.1%) | 237 (122) | 0.985 | 0.94 |
| 11281 | 78,109 | 7,914 → 7,391 | 550 | 41 | 1 | 770 / 349 / 20 | 0.552 | 741 (27.0%) | 411 (271) | 0.982 | 0.81 |
| 455 | 173,515 | 9,343 → 2,953 | 1,784 | 0 | 5 | 715 / 0 / 6 | 0.500 | 0 | 1 (1) | 0.997 | 0.58 |
| 456 | 175,474 | 3,009 → 2,429 | 385 | 1 | 3 | 725 / 0 / 5 | 0.502 | 0 | 0 | 0.998 | 0.39 |

How to read the table:
- *Regions* count regions with at least one line with text. There are 2–15 empty ones per
  hand inventory and 130k in 455; they get `layout_role=empty`.
- *none* counts the 2 portrait covers per inventory and the openings without text.
- *Passes* is the largest number of merge passes on one page.
- *Main char share* is the share of `main` characters in main + extra.
- *Extra / page* is per page with text.

Overlap before the layout, as the number of scans with at least one overlapping sibling pair
(regions with ≥ 1 line). The first two columns use the planning criterion, bbox intersection
≥ 0.5 of the smaller box. The third uses `regions_overlap(0.5)` on line hulls, which is what
the merge uses:

| Inv | Scans with lines | Region boxes | Line hulls | `regions_overlap`, hulls |
|---|---|---|---|---|
| 3349 / 3350 / 3351 | 957 / 887 / 201 | 88 / 98 / 78 | 88 / 98 / 78 | 92 / 100 / 80 |
| 3352 / 3353 / 3354 | 1,023 / 914 / 190 | 101 / 88 / 45 | 101 / 88 / 45 | 102 / 94 / 46 |
| 3771 / 3772 / 3773 | 573 / 587 / 603 | 124 / 115 / 127 | 124 / 115 / 127 | 152 / 143 / 171 |
| 3821 / 3822 / 3823 | 435 / 460 / 436 | 96 / 101 / 137 | 96 / 101 / 137 | 103 / 118 / 149 |
| 11182 / 11281 | 749 / 1,119 | 141 / 495 | 141 / 495 | 159 / 509 |
| 455 / 456 | 715 / 727 | 704 / 320 | 711 / 320 | 711 / 323 |

The planning numerators reproduce exactly: 78 (3351), 124 (3771), 320 (456) and 704 (455).
The planning denominators were all scans. Region boxes and line hulls differ only in 455.

**The 455 reduction (9,343 → 2,953).**
- 8,174 of the 9,343 regions with text (87%) end up in one of 1,784 merges. A merge has 4.6
  members on average, at most 17.
- 1,251 of the merges join an `index`-tagged and a `resolution`-tagged region. Laypa laid two
  overlapping label layers over the same text block. The merges also absorb 937 `catch-word`
  and 1,002 untagged fragments.
- After the layout, 1,058 of 455's 1,426 pages have exactly one `main` region.
- 456 has only `Text` regions: 385 merges, with 2.5 members on average.
- The reduction is real, but it does not mean the units got better: see the merge sample and
  F07-15fp.3.

**Error analysis.**

*Merge sets.*
- *What was read:* 60 merged layout regions, sampled with `random.Random(15)`
  (`ea.py merges`):
  - 30 of the 2,377 hand merges;
  - all 3 merges on 455's index pages. These are scans 0003–0005, the only scans with ≥ 10
    lines containing "pag.";
  - 27 of the other 2,166 print merges.
- For each set I read the member boxes and the merged line sequence, with the member of every
  line.
- *Categories:*

  | Category | Hand | Print | Example (verbatim, shortened) |
  |---|---|---|---|
  | right: reading order kept, nothing separate joined | 9 | 1 | 11281_0174 "…sijn mij met een goed aental" + "Schepen"; 3773_0420 "houdende ad-" / "vertentie." / "WAAR op geen resolutie is" / "gevallen." |
  | page furniture absorbed at the column's top or bottom (page number, running date or year, catch word), not interleaved | 14 | 3 | 3772_0074 "( 93" before "1717."; 3773_0151 "Den 26. Maart." first; 455_0037 catch word "Octo-" last |
  | wrong: margin note interleaved with the text | 4 | 0 | 3349_0662 "Op het gerapporteerde vande Heeren van 62" / **"1. Meij"** / "Gent, ende andere haer Ho:Mog: …"; 3822_0069 "voor het planien der" / **"1r92"** / "Meyboom" |
  | wrong: two columns joined | 1 | 26 | 455_0641 "houdende advertentie: Waer op geen Resolutie is" (col 2) / "hy Suppliant op sijn parol, en onder handtekeninge" (col 1) / "gevallen." (col 2); 3351_0047 register sub-columns "26 Jan: memr. …" / "22 fob: reqt. voorst…" |
  | ambiguous (not counted) | 2 | 0 | 3349_0474 interlinear "den" sorted before the first line; 3773_0006 "R." / "E" / "cob Schelius" |

- *Precision on the decided sets:*
  - hand: 23 / 28 = 0.82 if absorbed furniture counts as right, 9 / 28 = 0.32 if not;
  - print: 4 / 30 = 0.13;
  - all: 27 / 58 = 0.47.

  All are below the 0.90 bar, so this is a **negative result**. The threshold was not tuned.
- *Where the print failures come from.* All 26 wrong print sets already had a member whose
  lines lie in both text columns (the line `column` field has values 1 and 2). That member's
  XML line order alternates between the columns too: the 120 lines of region
  455_0641-…-414-312-1826-2931 make 117 jumps of > 600 px in input order, and 109 once sorted by
  y. So the merge does not cause the interleaving. Laypa's print regions are page-level blocks:
  - in 455, 1,434 of the 1,456 two-column merges contain a two-column member;
  - in 456, 368 of 376 do;
  - 1,403 of 456's 2,044 *unchanged* regions span both columns;
  - only 30 of the 2,169 print merges joined two columns that no member already spanned.
- *Hand.* The errors are short margin items (a date, a number, a label) that `regions_overlap`
  places inside a narrow text block, so the y-sort puts them between two lines of that block.
- *Conclusion.* In hand, the merge mostly attaches furniture and fragments at the edge of a
  column. In 4 of 28 sets it interleaves a margin item. In print the merge is not the problem:
  the regions are not columns. Units for print (15h) have to split on the line `column` field,
  not on regions.

*Page split.*
- *What was read:* two samples of 30 scans each, `random.Random(15)` (`ea.py pageside`, `ea.py
  mincov`). For each scan I read the coverage profile, every layout region with its x-range,
  page and role, and the lines within 8% of `separation_x`.
  - the first sample comes from the 1,035 openings where at least one line's `page` differs
    from the old `page_side`;
  - the second comes from the 2,655 scans with `split_method=min_coverage`.
- *Categories:*

  | Category | page ≠ `page_side` | `min_coverage` | Example |
  |---|---|---|---|
  | gutter right | 18 | 26 | 3350_0275, sep 0.477: the recto margin note "27. Augti" (0.469–0.523) is now on the recto; the old `page_side` put it on the verso |
  | cut off a margin column | 11 | 3 | 3349_0353, sep 0.593: the recto's margin notes "1. Maart", "Vittinghoff", "Hamilton" (0.515–0.593) go to the verso, while the recto text "Is ter vergaderinge gelesen de reqte 318." starts at 0.596 |
  | gutter missed by a whole page (not in the brief's list) | 0 | 1 | 3350_0856, sep 0.341: the verso main text ("Opde memorie vanden heere Palmquist, extrs:", x 0.343–0.592) goes to the recto |
  | cut through text | 0 | 0 | |
  | cover/single wrongly split | 0 | 0 | |
  | undecided | 1 | 0 | 11281_0292 "6.1." at 0.521–0.558, mid-height; whose it is is unclear |

  - In all 11 cut-offs of the first sample, the old `page_side` (`page_w/2`) had the notes on
    the right page. In the 18 right cases the new split fixes what `page_side` got wrong.
  - Twelve of the 14 cut-offs (both samples) are in 3349/3350 (3 and 9), one is in 3353 and
    one in 11182.
- *Why it fails.*
  - In the hand minutes (3349–3353) every page has its margin column on the left. The recto's
    margin notes are written right up against the end of the verso's lines. Example: in
    3350_0625 the verso text reaches 0.531 and the recto notes start at 0.528.
  - So there is often no zero-coverage gap at the gutter: 823 of 887 openings in 3350 and 620 of
    957 in 3349 fall back to `min_coverage`. Lines cross the separation in 87% and 53% of their
    scans.
  - The widest gap, and often the coverage minimum, lies between the recto's margin column and
    the recto's text. Line x-extents alone cannot tell a margin column from a page.
  - The whole-page miss happens when the window holds a sparse verso margin. In 3350, 19
    openings have one page with < 5% of the characters while the other page has two main
    columns. I checked 4 of them: 3 are misses (0851, 0859, 0886, sep 0.337–0.343) and 0005 is
    a real one-page opening.
  - Proxy for the cut-off: an `extra` region on the verso whose right edge lies within
    0.04·`page_w` of the separation. It flags 107/957 openings in 3349 (11%), 176/887 in 3350
    (20%), 15/1,023 in 3352 and 22/914 in 3353 (`split_proxies.py`). Its precision was not
    checked. It does not apply to 3771–3823 (other layout) or 11182/11281, where verso
    endorsements end at the gutter.
- *Conclusion.* The fitted gutter is right in 44 of the 60 scans read. But the gap rule is not
  safe for hand 3349–3353: it moves the recto's margin notes to the verso. A rule that knows
  the page layout (margin column left of the text on both pages, so the gutter lies just left
  of the recto's margin column) would be needed. That was not tried here.

*Roles.*
- *What was read:* 30 pages from the 15,112 pages with at least one `extra` region,
  `random.Random(15)` (`ea.py roles`). That gives 60 `extra` regions with 1,047 characters. I
  read every extra line, and the first and last lines of every main region.
- *Categories (extra regions):*

  | Category | Count | Example |
  |---|---|---|
  | margin note | 39 | 3352_0720 "Ouwerkerck", "versterkinge van / het guarnisoen. / van Limborg" |
  | header / date heading | 7 | 3772_0445 "Den 28. September."; 3352_0887 "7. Junij." |
  | page number | 7 | 3823_0159 "( 147 )"; 11281_0753 "735." |
  | catch word or signature mark | 2 | 456_0299 "dat", "7B" |
  | noise | 1 | 3350_0202 "8.A" |
  | **main text wrongly in `extra`** | 4 (323 chars) | 3349_0551 the signatories "L: S.H: v Bleijswijck." … "S. De Drews." (9 lines); 3771_0551 the line ends "tentie." / "len." and the attendance names "Wichers, Tamminga."; 3354_0058 a 5-line register sub-column "esdent Famars bede portual litt. P. .9." |

  - Outside the sample: 3821 has 163 pages with ≥ 10 `extra` regions. 3821_0047 recto is a
    printed register in four narrow sub-columns: keyword ("Marocco."), summary, date
    ("18 8110") and page ("476"). The role rule puts the keyword, date and page sub-columns in
    `extra`. This is why 3821–3823 have about 11 extra regions per page.
- *Conclusion.*
  - 56 of the 60 extra regions are what `extra` should hold. The 4 that are not hold 31% of
    the sample's extra characters (323 of 1,047). In each, main text forms its own narrow block:
    signatures, a list of names, or a short second sub-column.
  - Per inventory, `extra` holds 0.2–5.4% of the characters.
  - Registers (3821–3823, parts of 3351/3354) need their own rule.

**Findings:** F07-15fp.1–F07-15fp.11.

**Open issues.**
- The merge precision is below the bar (F07-15fp.2).
- Print units cannot come from regions (F07-15fp.3).
- The hand page split cuts off margin columns (F07-15fp.4, .5).
- Registers fall outside the role rule (F07-15fp.8).
- A new pagexml-tools trap: `PageXMLPage(coords=None)` with no columns, regions or extra
  raises `IndexError: point list cannot be empty`. `parse_derived_coords` is called on an
  empty list. The layout passes the page box instead (F07-15fp.11, for UA-005).

### Step 15fq: layout fixes (hand gutter, print column split, furniture, interleave guard)

**What changed.** Brief:
[`step-15fq-layout-fixes.md`](../plan/phase-07-sg-units-and-pilot/step-15fq-layout-fixes.md).
- `analysis/page_structure.py`: `LayoutParams` has 13 new fields and `LEGACY_15FP`;
  `inventory_line_width_rel`, `choose_gutter_rule`, `find_main_edge_x`,
  `split_regions_on_line_field`, `furniture_region_ids`; `find_separation_x(..., rule=...)`; the
  merge takes `exclude` and the interleave guard.
- `text_layer/layout.py`: per-page order page split, column split, furniture, merge, columns,
  roles; new table columns and `change` values; `presets.py` gets `layout_params` and the SG
  furniture patterns; `build.py` merges preset and `--layout-params`.
- Runs on copies of the phase-06 layers: `data/text_layer_v2_dev/15fq-a/` (main run, hand merges
  on), `15fq-legacy/` (`LEGACY_15FP`, regression) and, after the fallback, **`15fq-b/` = final**
  (hand merges off; print identical to 15fq-a). The `sg_1.01.02_v2` preset now sets `merge=False`;
  15fq-a is reproduced with `--layout-params '{"merge": true}'`.
- Fallback: hand merge precision is 0.52 (< 0.90), so the hand side was re-run with `merge=False`
  (decided by the user, 2026-10-20). Print precision is 0.93, so print keeps its merges.

**Results.** All 16 inventories, 1,663,766 lines.

| Criterion | Expected | Measured |
|---|---|---|
| 1. Conservation, unique ids, unchanged ids byte-identical | all | 16/16 inventories; 0 duplicate ids; 0 unchanged-id mismatches (15fq-a and 15fq-b) |
| 2. `15fq-legacy` = `15fp-a` on the 15fp columns (`merged+split` → `merged+split_at_gutter`) | equal | scans, regions and lines tables equal in 16/16 |
| 3. Gap inventories: `separation_x`, `split_method`, line `page` as 15fp | equal | 0 differences in 3354, 3771–3773, 3821–3823, 455, 456 |
| 4. Main-edge inventories: lines with a different `page` | planning numbers | 3349 343 (105 scans), 3350 1,615 (215), 3351 4 (4), 3352 14 (9), 3353 52 (15), 11182 236 (119), 11281 268 (135): exact |
| 5. Dev check: 15fp's 60 read page-split scans | 14/14 cut-offs, 3350_0856, 5 of 44 right | 14/14 change; 3350_0856 changes (40 lines); 5 of 44 "gutter right" change (11182_0709/0306/0538/0524, 11281_0923); the undecided 11281_0292 changes (1 line) |
| 6. Print: regions with both columns | 0 | **145** (455: 36, 456: 109); the same in 15fq-b. Missed (F07-15fq.5) |
| 7. Furniture: merged regions with a furniture member | 0 | 0 (31,524 furniture regions, none merged) |
| 7b. Residual overlapping pairs | each involves furniture or a guard-blocked pair | 15fq-a: 5,009 pairs, 3,400 furniture, 1,609 guard, 0 other. 15fq-b: 907 more pairs ("other") in hand, all from merges being off; print unchanged |
| 8. Suite green, protected hashes unchanged | | 1,035 passed (1,023 + 12); `15fq-protected-{before,after}.txt` identical (52,237 entries) |
| Roles (not re-read) | | `main` holds 94.6–99.8% of the characters per inventory; extra regions per page 1.0–11.96, within 0.06 of 15fp's values |

Per-inventory tables: [`acceptance.json`](data/phase-07-sg-units-and-pilot/step-15fq/acceptance.json)
(15fq-a) and `acceptance_15fq-b.json`.

**Error analysis.** Seed `random.Random(16)`; every scan of 15fp's `ea_verdicts.tsv` is excluded
except in the dev re-read. Verdicts:
[`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15fq/ea_verdicts.tsv) (60 merges, 50 page
split, 6 dev scans, 30 furniture, 20 column split, 30 guard).

- *Merge sets* (15fq-a, 30 hand of 827, 30 print of 1,924). Right = reading order kept and
  nothing joined that a reader keeps apart.

  | Side | Right | Wrong | Ambiguous | Precision (right / decided) |
  |---|---|---|---|---|
  | hand | 12 | 11 | 7 | 0.52 |
  | print | 28 | 2 | 0 | 0.93 |

  | Wrong, hand | Count | Example (verbatim, shortened) |
  |---|---|---|
  | two columns interleaved | 5 | 3354_0135: "28 junij. 1n. miss:. vi ker plus van 2 regementen" (x 3600) between "14 junij. heeft deselve sigh in een conferentie" and "beswaart op het preck" (x 2602) |
  | row fragment sorted before its line | 5 | 3349_0356: "den." (x 2874, y 2382) before "Ontfangen een missive vande Gecomden. inde" (y 2395) |
  | margin item interleaved | 1 | 3353_0024: "paspoorten inde provincien" / "niet valabel 601." between the names "Boelart 123." and "Boislart 130." |
  | wrong, print | 2 | 455_0345: "1 673 )" joined into column 2; 456_0098: both columns (61 + 61 lines) of one source region merged again |

  Right print merges all look alike: one column, the lines of two or three source regions
  alternating in y, for example 455_0350 (A/B alternating "werden geaccordeert, soo als haer
  geaccordeert" … "Beneficievoor harenSoon, mits blijvendeeen der-" …). Conclusion: on hand
  text the merge repairs too little and breaks too much, since margin items, column entries and
  line-end fragments all overlap the text and a y-sort cannot place them. In print, after the
  column split, the merge repairs the Laypa fragmentation.
- *Page split* (hand main-edge openings). 30 scans whose `page` differs from 15fp (20 from
  3349–3353, 10 from 11182/11281; pools 3349 102, 3350 204, 3351 4, 3352 9, 3353 14, 11182 114,
  11281 133) and 20 of 4,924 without a difference. All 30 changed scans are right: the moved
  lines are short notes and dates just right of the separation ("36. Julij", "Couck",
  "Elde o", "31. Meij", "Correspondentie / over het / Reguleren der / Contributien"), role
  `extra` or the recto's left main block. In none is a verso text line moved. 3351_0063 has a
  separation at 0.306·`page_w` (the volume has a narrow verso): its moved line is a recto
  header. The 20 unchanged scans show no verso-side cut-off; the verso lines nearest the
  separation are line-end words ("den", 2 cases) in role `main`. Conclusion: main-edge is right
  on every read case.
- *The 6 dev scans.* 11182_0709 ("D."), 11182_0306 ("admt. amsterdm."), 11182_0538 ("Sbosch.
  2."), 11182_0524 ("R: v. Brab: 6", x 0.502–0.567, recto text from 0.595, verso margin
  "Rade van Brabent." at its left), 11281_0923 (3 notes) and 11281_0292 ("6.1.") are recto
  margin notes. 15fp's verdict does not stand (F07-15fq.2).
- *Furniture* (30 of 31,049 flagged regions outside the dev set): 19 true page furniture
  ("( 186 )", "1717.", "Den 25 February"), 11 numeric or date margin items kept as own regions
  ("191" at y 0.84, "27 Dec." at x 0.23), 0 body text wrongly excluded. No catch word came up
  in the sample.
- *Print column split* (20 of 7,469): 17 column-pure; 2 `''` parts, each one line with empty
  text (456_0045, 456_0639); 1 not pure (456_0581, 105 lines, merged again). Running heads
  ("Den 20. Februarii.", y 255) sit second in three regions, because the line order is the
  source order.
- *Guard* (15 hand of 178 scans with a block, 15 print of 854; one random pair per scan):
  right 16, wrong 5, ambiguous 9. Right: empty-text noise (8), a page number "1 760)", margin
  summaries ("franse deserteurs / na Beijeren gaande"), labels. Wrong: print "den.", "len.",
  "reert.", "doen." continuing the sentence, and hand "tie."/"len.". Ambiguous: hand "den"
  fragments. Conclusion: the guard's target (margin items) works in hand, where merges are now
  off anyway; in print it helps against noise and costs line-end fragments.

**Findings:** F07-15fq.1–.9.

**Open issues.** Criterion 6 (145 print regions with both columns) is missed. The decision is
recorded in STATUS.md and as UA-008. The hand layer has no merges, so hand units come from
Laypa/HTR regions as they are (907 residual overlapping pairs, 51–106 per inventory).

### Step 15g: line and region types

**What changed.** Brief:
[`step-15g-line-region-types.md`](../plan/phase-07-sg-units-and-pilot/step-15g-line-region-types.md).
- New `archival_structures/text_layer/types.py` (`TypeParams`, `TYPES`, `segment_page`,
  `type_inventory`), `LayerPreset.types_params` with the SG patterns (`presets.py`), and
  `--stage types` / `--types-params` in `build.py`. Tables `types/segments/` and `types/regions/`,
  `types/stats.json`, `types/params.json`. Schemas in `docs/text_layer.md` (*Type tables*).
- Layer: `data/text_layer_v2_dev/15g-a/{hand_tar,hand_local,print}/` (a copy of `15fq-b` plus
  `types/`). The whole stage takes about 40 seconds.
- Deviations from the brief, all found in the error analysis or on the six R4 rows (see *Brief
  defects* in `PROGRESS.md`): (1) the margin-prefix cut uses the space nearest to the main left
  edge, within 0.04 `page_w`, not the first space past it; (2) per-column main left/centre when a
  page has several main columns (`col_min_char_frac` 0.25), keyed by the print `column` field
  where set; (3) a line that fullmatches `date_line`, `pn_line` or `year_line` is never cut by
  the margin rule; (4) a running year (`year_line`) in the top band is a `running_header`, not a
  `pn_line`; (5) `maert` added to the month list ("Den 9. Maert.").

**Results.** All 16 inventories, 1,644,839 text lines, 1,668,542 segments.

| Criterion | Expected | Measured |
|---|---|---|
| 1. Coverage and offsets | every non-blank line has >= 1 segment; no overlaps; texts rejoin | 16/16 inventories, 0 missing, 0 overlaps, 0 text mismatches (`acceptance.py`) |
| 2. R4 rows 50, 52, 55, 134, 19, 9 | expected segments | 6/6 (9 line checks incl. "Nieuwe jaren", "Diesbagh", "29. Augti.", "de Ruijter", "betalinge der", "Luijck"); every page number is in the chain |
| 3. Audit 3771_0100; `main_text` share | resolution text `main_text`, "( 152 )" `page_number`; 11182 the majority | 224 of 235 segments `main_text`; "( 152 )" `page_number`; "4716." (a running year) is also `page_number`. Character share 91.5% (3351) to 99.2% (455, 456); 11182: 98.0% (LotA's old labels: 98.1% of tokens) |
| 4. Per inventory | tables | below; 455 Laypa table in `acceptance_15g-a.json` |
| 5. Suite, protected hashes | green, identical | 1,045 passed; `15g-protected-{before,after}.txt` identical (52,638 entries) |

Segments per type, all inventories (share of characters): `main_text` 1,513,641 (97.84%),
`marginalia` 82,994 (1.13%), `date_header` 20,275 (0.43%), `index` 18,445 (0.38%),
`page_number` 15,130 (0.12%), `catch_word` 8,213 (0.05%), `running_header` 5,381 (0.04%),
`other` 3,688, `signature_mark` 775.

| Inv | Text lines | Segments | `main_text` chars | `page_number` | `date_header` | `marginalia` | `index` pages | Chain / candidates | Rectos with a number | Splits date / margin / pn |
|---|---|---|---|---|---|---|---|---|---|---|
| 3349 | 91,897 | 95,304 | 96.4% | 746 | 837 | 9,711 | 33 | 647 / 759 | 692 / 957 | 275 / 2,534 / 598 |
| 3350 | 84,627 | 88,492 | 97.1% | 814 | 636 | 8,782 | 24 | 747 / 854 | 773 / 886 | 151 / 3,082 / 632 |
| 3351 | 12,581 | 12,955 | 91.5% | 308 | 200 | 1,373 | 58 | 246 / 298 | 158 / 173 | 166 / 197 / 11 |
| 3352 | 96,495 | 100,329 | 96.8% | 876 | 839 | 8,826 | 38 | 787 / 893 | 828 / 1,023 | 438 / 2,752 / 644 |
| 3353 | 84,706 | 88,381 | 96.7% | 703 | 753 | 8,250 | 31 | 658 / 764 | 678 / 913 | 508 / 2,599 / 568 |
| 3354 | 11,146 | 11,418 | 92.4% | 268 | 209 | 1,075 | 47 | 239 / 260 | 137 / 164 | 166 / 102 / 4 |
| 3771 | 136,435 | 136,881 | 98.4% | 1,281 | 1,480 | 1,781 | 16 | 976 / 1,114 | 555 / 569 | 2 / 444 / 0 |
| 3772 | 141,492 | 141,735 | 98.9% | 1,217 | 1,546 | 1,356 | 0 | 1,043 / 1,129 | 563 / 587 | 6 / 237 / 0 |
| 3773 | 145,219 | 145,812 | 98.9% | 1,228 | 1,571 | 1,961 | 3 | 1,054 / 1,140 | 569 / 600 | 0 / 593 / 0 |
| 3821 | 116,595 | 117,168 | 96.5% | 945 | 2,934 | 8,784 | 17 | 603 / 770 | 396 / 435 | 3 / 570 / 0 |
| 3822 | 121,882 | 122,507 | 96.8% | 1,026 | 2,878 | 9,125 | 16 | 685 / 824 | 414 / 459 | 3 / 622 / 0 |
| 3823 | 118,407 | 118,917 | 96.5% | 983 | 2,729 | 9,558 | 16 | 511 / 751 | 376 / 436 | 1 / 507 / 2 |
| 11182 | 57,508 | 59,716 | 98.0% | 689 | 73 | 4,276 | 30 | 595 / 685 | 635 / 748 | 6 / 1,768 / 434 |
| 11281 | 78,030 | 80,463 | 97.6% | 988 | 185 | 5,321 | 10 | 765 / 947 | 887 / 1,106 | 17 / 2,392 / 24 |
| 455 | 173,398 | 173,645 | 99.2% | 1,469 | 1,657 | 1,848 | 0 | 995 / 1,271 | 628 / 714 | 2 / 190 / 55 |
| 456 | 174,421 | 174,819 | 99.2% | 1,589 | 1,748 | 967 | 3 | 1,277 / 1,419 | 710 / 722 | 1 / 394 / 3 |

Page-number candidates are `pn_line` or `pn_suffix` lines; 3349-3353 and 11182 mostly use the
suffix of the topmost line, the others the separate line (`stats.json` has the split). 3351 and
3354 have the lowest `main_text` share because they are short volumes with many margin notes
and ledger-like pages.

455, Laypa tag of the input region against the line type (segments; `acceptance_15g-a.json`):

| Laypa tag | `main_text` | `marginalia` | `page_number` | `catch_word` | `date_header` | other types |
|---|---|---|---|---|---|---|
| `index` | 138,691 | 329 | 566 | 9 | 716 | 390 |
| `resolution` | 27,852 | 829 | 392 | 8 | 897 | 185 |
| `catch-word` | 9 | 246 | 26 | 832 | 0 | 4 |
| `untagged` | 338 | 383 | 260 | 284 | 41 | 29 |
| `page-number` | 2 | 12 | 110 | 0 | 0 | 5 |
| `header` | 1 | 24 | 79 | 5 | 0 | 8 |
| `signature-mark` | 2 | 21 | 36 | 12 | 3 | 3 |

**Error analysis.** *What was read:* `random.Random(17)` over all 16 inventories, dumps by
`reports/data/phase-07-sg-units-and-pilot/step-15g/ea.py` with the previous and next line of
the page; verdicts in
[`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15g/ea_verdicts.tsv). The samples
were drawn on the final code. A first draw on an earlier build (page_number 15/18) found two
defects, fixed before the final draw: the margin rule cut page-number and date lines
("( 186 | )"), and one page reference mistyped the second text column; the sampling bar was
not changed. UA-008 effect: 109 mixed-column regions in 456 had 4,717 of their 9,676 column-1
segments typed `marginalia` with a page-level reference.

| Stratum (20 each unless noted) | Right | Wrong | Ambiguous | Precision |
|---|---|---|---|---|
| `page_number` | 19 | 1 ("2767", a running year) | 0 | 0.95 (bar 0.90) |
| `date_header` | 13 | 0 | 7 (dates in register pages: "7 July", "13 Oct.") | 1.00 of decided (bar 0.90) |
| `running_header` | 20 | 0 | 0 | 1.00 |
| `catch_word` | 20 | 0 | 0 | 1.00 |
| `signature_mark` | 20 | 0 | 0 | 1.00 |
| `marginalia` | 7 | 6 ("603", "378" register numbers; "Ho:Mo: vergaderinge | vande heer" main text in an `extra` region) | 7 | 0.54 |
| `index` (segments) | 19 | 0 | 1 | 1.00 |
| `other` | 20 | 0 | 0 | 1.00 (7 are drop capitals) |
| `date_prefix` splits (10) | 10 | 0 | 0 | 1.00 |
| `pn_suffix` splits (10) | 10 | 0 | 0 | 1.00 |
| `margin_prefix` splits (10) | 2 ("Praside | d'Heer van Kersbergen") | 7 ("Raad van | State.", "naar | 4o05 Mog.") | 1 | 0.22 |
| band lines typed `main_text`: hand (20) | 20 | 0 | 0 | |
| band lines typed `main_text`: print (20) | 17 | 3 page numbers with extra digits | 0 | |
| index pages (15 decided of 20) | 14 | 1 (prose with ragged short lines, 11281_0337) | 5 (accounts tables, one 4-line junk page) | 0.93 (bar 0.80) |

- *Conclusion:* the whole-line furniture rules (page number, running year, catch word,
  signature, date line) are reliable; the failures come from (a) geometry that assumes one text
  column (fixed, see F07-15g.3), (b) `extra` regions that hold register columns or text, which
  `rule:extra_role` calls `marginalia`, and (c) the margin-prefix cut, whose offset test
  (0.02 `page_w` left of the median edge) fires on ragged hand lines and on whole marginal notes.
  The date-prefix and page-number-suffix splits are exact. On 3821-3823 a "date_header" is also a
  register date entry; LotA's session-header logic should use only date headers outside
  `index` pages. Mixed-column print regions (UA-008) affect typing only through the reference
  edges, which the `column` field repairs; the region type of the 145 regions is `main_text` in
  143 and `index` in 1, so a column split in 15h loses nothing here.

**Findings:** F07-15g.1–.10.

**Open issues.** UA-009 (`margin_prefix`, recommended: keep only month-only prefixes).
UA-008 now has a response (b: a follow-up step 15fq2 for the print merge). If it runs, re-run
`--stage types` on the new print layout (about 15 seconds); the `column`-field handling then
changes nothing for the split regions. Row 80 is 15h's test.

### Step 15fq2: column merge guard

**What changed.** Brief:
[`step-15fq2-column-merge-guard.md`](../plan/phase-07-sg-units-and-pilot/step-15fq2-column-merge-guard.md)
(UA-008 (b)).
- `analysis/page_structure.py`: `LayoutParams.column_merge_guard` (default on; off in
  `LEGACY_15FP`). With `split_on_line_field` set, `get_overlapping_region_sets` joins regions with
  a union-find over the same edges and skips an edge when the union of the two components'
  non-empty `column` values has more than one element; skipped pairs go to `column_blocked`.
  Without the field the old DFS runs, so the result is unchanged.
- `text_layer/layout.py`: scans column `n_column_blocked` (last), summed in `layout/stats.json`.
  Docs: `docs/text_layer.md`; changelog entry *Step 15fq2*.
- Layers: `data/text_layer_v2_dev/15fq-c/{hand_tar,hand_local,print}/` (layout),
  `15fq2-noguard/print/` (guard off, regression), `15g-b/` (`15fq-c` plus `--stage types`).
  Hashes: `15fq2-protected-{before,after}.txt` (identical).
- Scripts and outputs: `reports/data/phase-07-sg-units-and-pilot/step-15fq2/` (`acceptance.py`,
  `ea.py`, `ea_residual.py`, `compare_types.py`, `compare_print.py`, `ea_verdicts.tsv`, JSON).

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | Conservation and ids, 16/16 | pass: 0 lost or doubled lines, 0 duplicate layout ids, 0 `unchanged` id mismatches |
| 2 | Hand equals `15fq-b` on the 15fq columns of the three tables, `n_column_blocked` 0 | pass in 14/14 |
| 3 | `15fq2-noguard/print` equals `15fq-b/print` | pass in 455 and 456 (scans, regions, lines) |
| 4 | Print regions with both columns | **0** in 455 and 456 (was 36 and 109) |
| 5 | Furniture merged: 0; residual pairs | 0 merged with a furniture member. Residual overlapping pairs (455 / 456): furniture 1,694 / 218, guard-blocked 769 / 659, column-blocked 39 / 107, other 0 / 0 |
| 6 | Locality | 81 new regions in 455, 222 in 456 (303); 36 and 109 lost (145 = the mixed regions, none else); 0 locality exceptions; `n_column_blocked` 62 / 113 |
| 7 | 15g re-run | hand `types/` equal in 14/14; print below; coverage ok, R4 rows 50, 52, 55, 134, 19, 9 reproduce; `main_text` char share 0.9921 (455), 0.9923 (456) |
| 8 | Suite, hashes | 1,050 passed (1,045 before, +5 new column-guard tests); protected hashes identical |

(Residual pairs for hand are not part of this criterion: hand has merges off, as in 15fq.)

15g re-run, print. Segment rows 173,645 to 173,646 (455) and 174,819 to 174,811 (456); region rows
8,651 to 8,696 and 4,610 to 4,723 (82 and 234 rows only in `15g-b`; 37 and 121 only in `15g-a`);
`layout_region_id` differs on 2,537 and 12,349 segments because the regions are new; the type
changes are 53 and 106. 455: `main_text` to `marginalia` 45, `other` to `marginalia` 6,
`marginalia` to `page_number` 1, `other` to `page_number` 1. 456: `marginalia` to `main_text` 103,
`main_text` to `marginalia` 2, `marginalia` to `other` 1. Per-type counts, 455 before to after:
marginalia 1,848 to 1,899, main_text 166,896 to 166,851, other 542 to 535, page_number 1,469 to
1,471; 456: marginalia 967 to 857, main_text 168,789 to 168,890, other 304 to 305. A small change,
as expected. Ten changed print segments read (seed 21): 7 better (456 prose lines `marginalia` to
`main_text`: "Schelde en andere onder het committimus gelegen,", "koorts niet was genesen, dat haer
Hoogh Mogende"), 3 neutral (page-number lines "1 985 )" and "11091 )", `main_text` to `marginalia`,
both wrong before and after; "5 21"), 0 worse.

**Error analysis.**
- *30 formerly mixed regions* (seed 21; 15 per inventory; `ea.py`, verdicts in `ea_verdicts.tsv`):
  27 right, 3 ambiguous, 0 wrong. All pieces are column-pure by construction; I checked that each
  piece holds the lines of one column in the source order (merged order is unchanged and a
  column piece keeps its lines' y order). In 14 of 15 sampled 455 regions and 13 of 15 in 456 the
  old region is now two regions: a column-1 body of 53 to 73 lines and a column-2 body of 29 to
  63 lines, or a 59-to-61-line body plus a one-line page number. Verbatim: `455_0272`: 59 lines
  of column 1 ("Den 11. Mey.", "van Vryenes, geschreven tot Meurs den achtsten") and a region
  with the one line "1 526 )" labelled column 2; `456_0122`: 62 + 61 lines. The `''` lines went
  to the neighbouring body (an OCR-junk first line "iamt ormu" in `455_0039`, two empty-text
  lines at the end of column 1 in `456_0662`). Ambiguous: `455_0027` (a 4-line tail piece "IX. 9.
  voorde Paerden..." is its own region), `456_0004` (title page, wide OCR lines, 3 + 9 lines),
  `456_0076` (garbled OCR page).
- *15fq's two print merge errors*: `455_0345` ("1 673 )" absorbed into column 2) is fixed (it is
  its own region with 61 pure column-2 lines in the body); `456_0098` (one 122-line region with
  both columns) is fixed (61 + 61 lines).
- *Column-blocked residuals* (`ea_residual.py`, 20 read, seed 21): 14 of 20 are two full
  columns (54 to 67 lines each) whose hulls overlap; both are pure, which is right. 6 have a
  one-line or two-line side: 2 page numbers ("1 616 )", "1 833 )") and 4 line-end fragments that
  continue the other column's last sentence ("den.", "sen.", "doen.", "hoort."). Over all 198
  overlapping column-conflicting pairs: 113 have both sides at 20 lines or more, 78 a side of at
  most 3 lines (22 page-number-like, 56 other, mostly fragments). The fragment carries the other
  column's label (the HTR region had it), so the rule keeps it apart: a lost merge, not a column
  error. 15hj's word-break join should expect a line-end fragment as its own unit; this is for
  15h/15hj, and not changed here (no threshold, per the brief).

**Findings:** F07-15fq2.1–.4.

**Open issues.** None for the user. 15h runs on `15g-b`. UA-009 stays open.

### Step 15g2: margin cut month-only

**What changed.** Brief:
[`step-15g2-margin-cut-month-only.md`](../plan/phase-07-sg-units-and-pilot/step-15g2-margin-cut-month-only.md)
(UA-009 (b)).
- `text_layer/types.py`: `TypeParams.margin_cut` (`"month_only"` default, `"all"` = 15g, `"off"`).
  Under `month_only` the rule-2 cut is made only when the prefix fullmatches `month_only`; other
  lines stay whole and go to the whole-line rules. Nothing else in `types.py` changed.
- Tests: `test_margin_prefix_other_not_cut` (renamed), `test_margin_cut_off`, `margin_cut` in
  `params.json` (1,051 tests pass, 1,050 before). Docs: `docs/text_layer.md`; changelog *Step 15g2*.
- Layers: `data/text_layer_v2_dev/15g-c/` (types layer for 15h; `15g-b` plus `--stage types`) and
  `15g2-all/` (`margin_cut="all"`, regression). Hashes: `15g2-protected-{before,after}.txt`.
- Scripts and outputs: `reports/data/phase-07-sg-units-and-pilot/step-15g2/` (`acceptance.py`,
  `acceptance.json`, `ea.py`, `ea_sample.txt`, `ea_verdicts.tsv`) and
  `step-15g/acceptance_15g-c.json`.

**Results against the acceptance criteria.**
1. **Regression:** `15g2-all` `types/{segments,regions}` equal `15g-b` in 32/32 tables (16 x 2).
2. **Cut set:** in `15g-c` every `rule:margin_prefix` segment is a `date_header` whose text
   fullmatches the month pattern (0 exceptions). 423 cuts, equal per inventory to the `15g-b`
   cuts with a `date_header` prefix (3350: 277, 3773: 43, 11281: 35, 3772: 16, 3821: 11, 3352
   and 3823: 10 each, 3822: 9, 3349: 6, 3353/3354: 2, 11182 and 456: 1, 0 in 3351, 3771, 455).
3. **Locality:** 0 of the unchanged lines differ. 18,552 lines changed, and all have no prefix
   piece. The brief expected 18,983 minus 423; 18,983 is 15g-a's count, and `15g-b` had 18,975
   (15fq2 changed 8), so 18,975 - 423 = 18,552 is the right figure. A `pn_suffix` piece stays where
   it was in every changed line that had one. In 26 changed lines a suffix appears that was not
   there before: the line is now whole, and the top-line suffix rule splits the number off ("1 657"
   was `marginalia` "1" + `page_number` "657", now `page_number` "1" + `page_number` "657"; both
   halves are page-number type). Segments: 1,668,535 to 1,650,009 (-18,526 = -18,552 + 26).
   Region rows differ in 8,625 of 190,640 (regions whose `region_type` char majority moved).
4. **15g acceptance on `15g-c`:** coverage 0 missing, overlap or mismatch in all 16 inventories;
   R4 rows 50, 52, 55, 55b, 134, 134b, 19, 9a, 9b all ok (row 19 keeps the month cut "Augt.").
   Per-type segments `15g-b` to `15g-c`: marginalia 82,935 to 64,487 (-18,448), main_text
   1,513,697 to 1,513,766 (+69), date_header 20,275 to 20,210 (-65), other 3,682 to 3,645 (-37),
   running_header 5,381 to 5,340 (-41), page_number 15,132 to 15,121 (-11), index 18,445 to 18,452
   (+7), catch_word and signature_mark unchanged. The small shifts besides marginalia come from
   lines that, once whole, meet a different whole-line rule (a prefix that was the page-number
   digits, a short line that now passes the band rules).
5. **Suite and hashes:** 1,051 passed. Protected hashes identical before and after.

**Error analysis** (seed 22; `ea.py`, texts and boxes from the tables). 30 formerly cut lines
(10 from 3349-3354, 10 from 3771-3823, 10 from 11182/11281/455/456; 10,969 / 2,884 / 4,699
candidates). Categories:

| Category | Count |
|---|---|
| right main (a main-text line that starts a little left) | 18 |
| right marginalia (the whole line is a margin note) | 10 |
| ambiguous | 2 |
| wrong (margin note plus main text now one segment) | 0 |

Examples, right main: "Praeside d'Heer van Wellandt", "Hogenhoeck, Heemskerck, Bors van
Waveren,", "is gevallen.", "Mijn Heeren; het schijnt, dat het aennaderen vanden heer". Right
marginalia (typed `marginalia` by `extra_role`/`page_margin` once whole): "Trouppes tot Zas van",
"Rec: 14. Februarij 1718.", "present de heeren Raden". Ambiguous: "520. monte les 42. jours a.." (a
number in front of a French line) and "1645 ten 19. soo in voors suppletie van de" (a year that may
be a margin note). The share of wrong is 0 of 30; with n = 30 the 95% upper bound is about 0.1,
so (b) gives up at most about one line in ten of the removed cuts, and in the sample nothing. The
sample found that almost every removed prefix is the first word or two of a main-text line that
starts a little left (names in attendance lists, "De", "is"), which is why 15g's cuts were wrong.
The two cuts 15g called right (3353_0318 "Praside | d'Heer van Kersbergen, 1704,", 3349_0380
"Magt. van Herstal | ende Ingezetenen van Herstal en wanderen") are now single `main_text` lines.
They were the only two real margin notes among the 9 decided in 15g, so (b) loses them: the cost
is those lines, which carry a margin note glued to main text.
Reading 3353_0318 shows its other nine formerly cut lines ("Prasentibus", "extrs:", "vande Lier,",
"Raadt", "Twee extrs:", "vander", "Zallick,", "De") are all attendance-list main text, so the
two "right" cuts were probably also list items cut by an uneven left edge; the 15g verdict on them
was a judgement from the table, not from an image.

**Findings:** F07-15g2.1-.3 (see the table).

**Open issues.** None for the user. 15h runs on `15g-c`.

### Step 15h: units, reading order and the doc_id mapping

**What changed.** Brief:
[`step-15h-units-and-mapping.md`](../plan/phase-07-sg-units-and-pilot/step-15h-units-and-mapping.md).
- New `text_layer/units.py` (`UnitParams`, `split_region`, `build_units_page`, `paragraph_features`,
  `cues_fired`, `doc_id_map_rows`, `republic_crosswalk`, `unit_inventory`); `LayerPreset.unit_params`;
  `build.py --stage units`, `--unit-params`, `--republic-dir`. `SG_PRINT_V2.unit_params` =
  `{indent_frac: 0.5, short_frac: 0.1, gap_factor: None}` (fitted below).
- Tests: `tests/test_text_layer_units.py` (9; suite 1,060 passed, 1,051 before). Docs:
  `docs/text_layer.md` (*Units and the doc_id mapping*); changelog *Step 15h*.
- Layer: `data/text_layer_v2_dev/15h-a/` (copy of `15g-c` plus `units/`); hashes
  `15h-protected-{before,after}.txt` (identical; `size mtime path`, sha256 for `shared-data`
  archives, over the earlier dev layers, `shared-data/`, LotA `data/`, `gold/`, `output/`).
- Scripts and outputs: `reports/data/phase-07-sg-units-and-pilot/step-15h/` (`acceptance.py`,
  `acceptance.json`, `fit_paragraphs.py`, `fit_paragraphs.json`, `pairs_{dev,test}.tsv.gz`,
  `ea.py`, `ea_sample.txt`, `ea_counts.json`, `ea_verdicts.tsv`, `protected_hash.py`).
- Brief deviation, one addition: the crosswalk has a fifth match class `split_half` (below).
  Hand `unit_method` is `hand:run`, `hand:region`, `hand:region_cut_at_header`; print is
  `print:run`, `print:region`, `print:region_start` and `print:<cues>`.

**Results against the acceptance criteria.**
1. **Coverage:** all 16 inventories: every `types/segments` row is in exactly one unit, `unit_id`
   is unique, one `unit_type` per unit. 181,472 hand units, 53,109 print units.
2. **R5:** 0 unchanged non-empty layout regions without a `same_id` row; every `regions_meta` row
   is in `doc_id_map`. `old_status` (legacy regions): hand (14 inventories) identical 129,686,
   trimmed 21,523, changed 37, empty 2,775; 455 identical 4,344, trimmed 499, changed 4,500,
   empty 130,751; 456 identical 1,031, trimmed 124, changed 1,854, empty 90. Per-inventory counts
   are in `acceptance.json` (`crit2`). Id collisions: 0 in hand, 58 (455) and 32 (456) in print.
3. **Stable keys:** 300 of 300 `hand_doc_id`s (300 distinct) are `old_doc_id`s: 214 `identical`,
   86 `trimmed`, 0 `changed`.
4. **Row 80:** the two units are both `main_text`, `hand:region`, in 3353_0754, unit_order 24
   ("Noch wert alhier…", region `3604-3785-1254-132`) and 21 ("in te huuren.", region
   `3617-1633-1232-221`): not adjacent.
5. **Crosswalk** (distinct `line_start`/`line_end` ids):

| | exact | near | split_original | split_half | unmatched |
|---|---|---|---|---|---|
| 455 | 27,033 | 1,864 | 189 | 55 | 7 |
| 456 | 20,192 | 13 | 512 | 13,848 | 23 |

   With the brief's four classes alone 456 has 13,871 unmatched. REPUBLIC splits the merged
   two-column print lines itself, and its halves have x, y and h equal to ours but a width 10-30 px
   off (REPUBLIC `2625-2341-900-79`, ours 921 wide). `split_half`: our line with a `split_from`
   and the best box IoU >= 0.5. Of the 13,871, 13,852 reach IoU 0.5 and 13,087 reach 0.8.
6. **Paragraphs** (reference: consecutive `main_text` pairs in a print layout region on scans with
   a mapped `line_start`; dev 80,596 pairs with 7,260 boundaries, test 245,779 with 23,416;
   `line_start`s not mapped: 2 of 8,337 dev, 12 of 27,381 test). Dev grid (27 points, F1):

| indent_frac \ short_frac | 0.1 | 0.2 | None |
|---|---|---|---|
| 0.5 | 0.779 / 0.780 / **0.784** | 0.768 / 0.768 / 0.773 | 0.630 / 0.630 / 0.634 |
| 1.0 | 0.762 / 0.763 / 0.767 | 0.739 / 0.739 / 0.744 | 0.296 / 0.296 / 0.297 |
| 1.5 | 0.749 / 0.749 / 0.754 | 0.723 / 0.723 / 0.727 | 0.207 / 0.206 / 0.206 |

   (cells: `gap_factor` 1.5 / 2.0 / None; the full grid is in `fit_paragraphs.json`.) Chosen (first
   best): `indent_frac=0.5, short_frac=0.1, gap_factor=None`: **dev** P 0.724, R 0.855, F1 0.784.
   **Test, reported once:** P 0.688, R 0.708, F1 0.698 (455 odd scans P 0.736, R 0.851, F1 0.789;
   456 P 0.664, R 0.646, F1 0.655). The `header` cue alone: dev F1 0.020, test 0.022. No bar.
   Units per type and inventory are in `acceptance.json` (`crit6`); in total hand: main_text
   86,744, marginalia 47,523, date_header 16,413, page_number 11,976, catch_word 5,901,
   running_header 5,272, index 4,297, other 2,726, signature_mark 620. Median main unit: 8-10
   lines in hand (2 in 3351 and 3354), 2 in print (both).
7. **Suite and hashes:** 1,060 passed; protected hashes identical before and after.

**Error analysis** (seed 18; `ea.py`; verdicts in `ea_verdicts.tsv`, samples in `ea_sample.txt`).

*Hand units, 30 `trimmed` regions (21,523 candidates).* 26 right as shown (the pulled-out units are
a date header, running header, page number, catch word, signature-like `other` or margin note, and
the main unit reads as one text), 1 right with a split margin note ("Confliet van" and
"jurisdictie." are two one-line `marginalia` units because a main line sits between), 1 doubtful,
and 3 whose pulled-out segments lie beyond the 16 shown (not read). The doubtful one: the year
"1717." typed `running_header` in the middle of 3772_0571's region cuts its main text into 1 + 6
segments. 0 wrong in the 27 read.

*Print paragraphs, test.* 20 false positives (of 7,502): continuation line over the 0.5-line indent
6 (e.g. "Provincie van Gelderlandt." after "...Gedeputeerde uyt de"), presence-list short lines that
REPUBLIC keeps in one paragraph 4 ("PRAESIDE," / "De Heer Gockinga."), OCR noise lines 4
("16103"), a column fragment 1, a header cue across a page break where the text continues 1,
reference artefact 1 (a `header` cue before "PRAESIDE,", where REPUBLIC's paragraph starts at the
non-main date line), plausible cue 2 (a drop-capital "W verstaen", a French fragment at indent 11)
and one fragment/reading-order case. The cue is wrong in 17, plausible or right in 3. 20 false
negatives (of 6,847): REPUBLIC cuts mid-sentence in 14 ("…om te vi-" | "siterenexamineren, en
van alles alhier ter Vergaderinge rapport te doen." as a paragraph of its own; lowercase starts),
real paragraph starts that the cues miss in 6 ("Is ter Vergaderinge gelesen de Requeste van",
"Ontfangen een Missive van den Raedt van State", "DE Resolutien eergisteren genomen", all after a
full line with a flush left edge). The 14 are in 456, where REPUBLIC's own line split put a
paragraph break at column tops; this is why 456 scores lower (0.655).
Residue (F07-15fq2.3): one-segment `main_text` units of at most 6 characters: 841 (455), 1,398
(456) of 9,335 and 7,804 one-segment main units.

*Ids, 20 `changed` legacy regions (6,391 in all, 6,354 of them print).* In each the map rows give
the units that hold the region's segments (`n_segments_shared` sums to the region's segments by
construction), so a reader finds the text again: yes in 20. By cause: 8 interleaved two-column
legacy regions (alternating lines go to two column units), 7 regions cut at a paragraph or header
boundary (a date header, a presence list of 14 one-line units), 5 a single line that the layout
put into another column's region ("10 P", "Den 7. Jutbiio", "ende van alles alhier ter
Vergaderinge rapport te").

*Crosswalk, 30 unmatched ids (7 + 23; all read).* 15: our line is unsplit and the REPUBLIC id is a
left or right fragment of it (same x, y, h, width 250-900 against our 1,100-1,800); 6: the same
line with a box offset over 5 px (REPUBLIC two lines where we have one, or skew); 4: REPUBLIC's
line is unsplit or our halves lie more than 5 px off in y; 3: small fragment boxes below IoU 0.5; 2:
no counterpart. A `contained` rule would recover the first 15; they are 0.06% of the ids.

**Findings:** F07-15h.1-.8 (see the table).

**Open issues.** None for the user. The 58 + 32 id collisions deviate from the brief's expectation
of 0 (F07-15h.8); the bumped ids are unique and `doc_id_map` is unaffected. The grid's best point is
at its edge (`indent_frac=0.5`, `short_frac=0.1`); per the brief it is not extended. 15hj and 15i
use the layer `15h-a`.

### Step 15hj: word breaks and unit text with offsets

**What changed.** Brief:
[`step-15hj-word-breaks-and-join.md`](../plan/phase-07-sg-units-and-pilot/step-15hj-word-breaks-and-join.md).
- New `text_layer/join_v2.py`: `join_unit(texts, wbcs, guards, both_ends_fix) -> (text, spans, boundaries)`,
  `read_unit_texts`, `write_word_break_stats_part_v2`, `join_inventory_v2`. `word_breaks.py` gets
  `robust_counts` and `word_break_stats(..., robust=False)` (additive). `LayerPreset` gets
  `word_break_rule`, `word_break_stat` (`default`), `wbc_sanity_in` (SG: `()`), `wbc_sanity_out`.
  `build.py`: v2 `wbc` and `join`, `--allow-subset-wbc` (v1 and v2), manifest `history`, and a v2
  `--stage all` that runs extract (+ wordfix) -> layout -> types -> units -> wbc -> join.
- Tests: `tests/test_text_layer_join_v2.py` (36), `test_subset_guard` and `test_manifest_history` in
  `test_text_layer_build.py`; `test_build_v2_cli_extract_and_refuses_join` is now
  `test_build_v2_cli_all_runs_the_chain` (the refusal it pinned is gone). Suite 1,098 passed (1,060
  before). Docs: `docs/text_layer.md` (*Unit text and offsets*, *Building a layer*); changelog *Step 15hj*.
- Layer: `data/text_layer_v2_dev/15hj-a/{hand_tar,hand_local,print}/` (copy of `15h-a` plus `wbc/` and
  `join/`; wbc is decided per layer root, so hand_tar (6 inventories) and hand_local (8) have their own
  corpus default). Joined with `--ignore-sanity` (below). Hashes `15hj-protected-{before,after}.txt`
  (`size mtime path`, 97,899 files, identical).
- Scripts and outputs: `reports/data/phase-07-sg-units-and-pilot/step-15hj/` (`acceptance.py`,
  `acceptance.json`, `ea.py`, `ea_sample.txt`, `ea_counts.json`, `ea_verdicts_build.py`,
  `ea_verdicts.tsv`, `common15hj.py`).
- `line_ranges` of `make_text_region_text` (pinned in `test_line_ranges_semantics`): a merged line's
  range stops before the dropped break char and the next range starts there; a plain line's range
  includes the separating space. `join_unit` takes the ranges per run, trims that space, and adds each
  run's start (runs are joined with one space).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1 Offsets | 0 violations | 0 in 1,650,009 segments of 234,581 units (text slice equals the segment after `lead_dropped` and the merged break char(s); `lines.text` at `line_offset` equals the slice; `""` after a merge, `" "` otherwise). 316,301 merges remove one break char in 316,216 cases and two (`--`) in 85; 0 keep it; 0 empty segments |
| 2 Equality with `join_region` | 0 differences | 0 of 234,581 units, and 0 against the text the stage wrote |
| 3 Legacy text (hand, `identical`) | 0 failures | 129,686 units tested, 0 failures |
| 4 Decisions | reported | default = robust in all 16 (table below); sanity: hand_tar True, hand_local False (`:`), print False (`.`); adoption: not adopted; digit-digit merges 0 |
| 5 Lower-case share, hand | >= 0.95 reported | 0.954 overall; per inventory in the table |
| 6 v1 | `TOTAL 157 / 157` | `TOTAL 157 / 157` (110 TSV byte-equal, 44 JSON equal, 3 run files); suite 1,098 passed; protected hashes identical |

Boundaries and decisions per inventory (`wbc` = the set in `wbc_per_inventory.tsv`, identical for
`default` and `robust`; `lower` = merges with a lower-case right side):

| Inv | wbc | units | segments | plain | merge | g_standalone | g_digit | g_digit_left | g_punct_left | g_digit_right | lower |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 3349 | `„` | 14150 | 92776 | 69964 | 8660 | 0 | 0 | 0 | 1 | 1 | 0.990 |
| 3350 | `„` | 11996 | 85687 | 66207 | 7478 | 0 | 0 | 0 | 4 | 2 | 0.986 |
| 3351 | `` | 4693 | 12758 | 8065 | 0 | 0 | 0 | 0 | 0 | 0 | - |
| 3352 | `=„` | 13357 | 97587 | 74252 | 9970 | 0 | 0 | 1 | 1 | 6 | 0.950 |
| 3353 | `„` | 12615 | 85784 | 65358 | 7808 | 0 | 0 | 0 | 1 | 2 | 0.989 |
| 3354 | `` | 4230 | 11318 | 7088 | 0 | 0 | 0 | 0 | 0 | 0 | - |
| 3771 | `-=„` | 15011 | 136437 | 78567 | 42734 | 21 | 5 | 14 | 1 | 84 | 0.987 |
| 3772 | `-=„` | 14843 | 141515 | 81167 | 45377 | 22 | 0 | 17 | 6 | 83 | 0.984 |
| 3773 | `-=„` | 15130 | 145262 | 83814 | 46171 | 10 | 0 | 16 | 5 | 116 | 0.985 |
| 3821 | `-=„` | 19824 | 116609 | 85544 | 11017 | 11 | 0 | 33 | 12 | 168 | 0.963 |
| 3822 | `-.=„` | 20200 | 121894 | 77657 | 21486 | 20 | 59 | 1956 | 4 | 512 | 0.779 (0.957 without `.`/`:`) |
| 3823 | `-=„` | 19832 | 118421 | 87415 | 10678 | 28 | 6 | 55 | 24 | 383 | 0.941 |
| 11182 | `-=„` | 6238 | 57949 | 46999 | 4699 | 1 | 2 | 0 | 0 | 10 | 0.699 |
| 11281 | `-:=„` | 9353 | 78106 | 59074 | 9456 | 1 | 12 | 190 | 3 | 17 | 0.946 (0.885 without `.`/`:`) |
| 455 | `-.=„` | 28079 | 173476 | 94918 | 50164 | 15 | 52 | 191 | 6 | 51 | 0.876 (0.891 without `.`/`:`) |
| 456 | `-=„` | 25030 | 174430 | 108620 | 40603 | 7 | 13 | 42 | 16 | 99 | 0.931 |

**`„`-robust statistic (F05-R.4).** The parts files carry `all_freq_robust` and
`end_rel_frac_robust` for every candidate. Decisions with the two statistics are equal in all 16
inventories (and the pooled rule sets and defaults are equal in the three layer roots), so the spec
keeps `default` and skips the 40-pair labelled sample: no pair was labelled and no threshold was
touched. For `„`, robust changes `end_rel_frac` from 40.9 to 54.7 (11182), 39.7 to 41.9 (3352),
43.3 to 43.4 (3349), and by under 0.1 in the other 13; all of them already clear 5.0. The `„` that
the rule misses (3351: `join_vocab_frac` 0.12, 3354: 0.08) fail on the vocabulary test, which the
robust statistic does not touch. `word_break_stat` stays `default` in both SG presets.

**Sanity anchors.** SG presets drop `¬` from `wbc_sanity_in` (no `¬` in the hand corpus), keeping
`:` and `.` as must-fail. They fail in hand_local (pooled rule set `-:=„`, from 11281 and its 5,216
`:` line ends) and in print (`-.=„`, from 455). The brief asks to report `sanity_pass`, not to
stop, and `join` is the stage that refuses (as in v1), so the layer was joined with `--ignore-sanity`
(recorded in the manifest) to measure criteria 1-5, and the choice of sets is UA-010.

**Error analysis.**

- *What was read:* `random.Random(19)` over all boundaries of the 16 inventories (`ea.py`, samples in
  `ea_sample.txt`, verdicts in `ea_verdicts.tsv`): 15 hand and 15 print `merge` boundaries; 20
  guarded boundaries; 20 of 17,128 `merge` boundaries with an upper-case or digit right side;
  extra samples of the merges on the suspect characters (20 `.` in 455, 10 `.` in 3822, 10 `:` in
  11281, 10 `=`); 10 print units whose last segment ends in `-`; 12 `„` line ends of 3351/3354 (not
  stored in the verdict file). The adoption sample was not needed.
- *Categories:*

  | Sample | Right | Wrong | Unclear | Example (verbatim) |
  |---|---|---|---|---|
  | hand `merge`, 15 | 15 | 0 | 0 | "dusda-" + "nige"; "me„" + "morie" |
  | print `merge`, 15 | 15 (2 `.` as OCR hyphen, 2 with OCR noise) | 0 | 0 | "saec." + "ken"; "geconsen-" + "Jteert." |
  | guarded, 20 | 11 | 6 | 3 | wrong: "a1-" + "daar,"; "Con„" + "101 de Hochepied" |
  | `merge` with upper/digit right, 20 | 4 | 15 | 1 | wrong: "neemen." + "WAAR op"; "Chur-" + "Saxische" |
  | `.` merges 455, 20 | 11 | 9 | 0 | right: "vol." + "gens"; wrong: "nadeel." + "lae Rumonde" |
  | `.` merges 3822, 10 | 4 (2 noisy) | 5 | 1 | wrong: "Mogende." + "WAAR op" |
  | `:` merges 11281, 10 | 9 | 0 | 1 | "ho:" + "mogende"; "gar:" + ":nisn" |
  | `=` merges, 10 | 7 (2 noisy) | 2 | 1 | wrong: "Muijl=" + "Esels" (compound) |
  | `„` line ends 3351/3354, 12 | 12 | 0 | 0 | "com„" + "missanisen" |
  | print units ending in `-`, 10 | listed | | | "afgewe-" (12 segs), "Heyn-" (1 seg) |

- *Conclusion:* in hand text a merge on `-` or `„` is almost always a real word break (15 of 15 hand and 15 of 15
  print read; 0.99 lower-case right sides in 3349, 3350 and 3353). The damage comes from three places.
  (1) Sets that contain `.` or `:`: `.` is the HTR's reading of a hyphen about half the time in
  print and 40% in 3822, and a sentence end the other half; 3822 drops to 0.779 lower-case and 455 to
  0.876 (UA-010). `:` is the scribe's hyphen only in 11281. (2) Hyphens before an upper-case word
  (a new sentence, or a hyphenated compound), 5.4% of merges and 75% wrong in the sample. (3) The
  digit guards fire on HTR digit-for-letter errors inside genuine breaks (6 of 20). 11182 (French
  and Dutch, 0.699) is the weakest inventory: `=` and `„` merges there are followed by upper-case
  words or comma-initial fragments in about 30% of cases; it was not read further.

**Findings:** F07-15hj.1 to F07-15hj.8.

**Brief defects.**
- The brief says to pass "a rule copy whose `end_rel_frac` column is the robust one"; a rule has no
  columns. The robust variant instead passes a stats frame whose `all_freq` and `end_rel_frac` are the
  robust columns, so the pooled sanity check also uses the robust frequency.
- The layer has three roots (hand_tar 6, hand_local 8, print 2), so `wbc` is decided per root, not
  over the 16 inventories; 15i's single hand layer of 14 will have a different corpus default.
- The brief does not say what to do when the sanity anchors fail (UA-010); `--ignore-sanity` was used
  for the dev layer.
- `test_build_v2_cli_extract_and_refuses_join` pinned the refusal the step removes; it was rewritten.
  The fixture for `test_v2_all_chain` (`tests/fixtures/text_layer/sg/9999`) has to be copied into a
  directory named `NL-HaNA_1.01.02_9999` to match the preset's source pattern.
- `classify_boundary` returns `None` for the char of a `plain` boundary; `join_unit` stores `""`.

**Open issues.** UA-010 (the WBC sets). A guard for upper-case right sides (F07-15hj.6) needs a change
to `join.py` and a v1 byte-identity check; it is not in this step.

### Step 15hj2: pinned WBC sets

**What changed.** Brief:
[`step-15hj2-pinned-wbc-sets.md`](../plan/phase-07-sg-units-and-pilot/step-15hj2-pinned-wbc-sets.md).
UA-010 (c): `LayerPreset.wbc_pinned`; `SG_HAND_V2` pins `-=„` (11281 `-:=„`), `SG_PRINT_V2` pins `-=„`.
`build.decide_v2` replaces the derived decision by the pin (both derived variants are still written), and
`sanity_pass` is evaluated on the pinned default set (the old value is kept as `pooled_rule_sanity_pass`).
Layer `data/text_layer_v2_dev/15hj-b/{hand_tar,hand_local,print}/` = copy of `15hj-a` with `wbc` and `join`
re-run (preset defaults, no `--ignore-sanity`). Scripts and JSON: `reports/data/phase-07-sg-units-and-pilot/step-15hj2/`
(`acceptance.py`, `deltas.py`, `ea.py`).

**Results against the acceptance criteria.**
1. `wbc_per_inventory.tsv` has `-=„` for every inventory except 11281 (`-:=„`), `mode` `pinned`;
   `sanity_pass` true in all three sub-layers (`pooled_rule_sanity_pass`: hand_tar true, hand_local false,
   print false); manifests: `wbc.pinned` true, `join.ignore_sanity` false. Met.
2. `scans/ regions_meta/ lines/ layout/ types/ units/ wbc/parts/` and `wbc/wbc_stats.tsv` are
   byte-identical to `15hj-a` in all three sub-layers (content hashes). Met.
3. Join checks on `15hj-b`: offset violations 0 (1,650,009 segments, 234,581 units), `join_unit` ==
   `join_region` 0 differences, legacy text 0 failures in 129,686 `identical` units, 0 digit-digit
   merges, 0 empty segments. Met.
4. Merges before -> after (lower-case right-side share). Unchanged, same share: 3771, 3772, 3773, 3821,
   3823, 11182 (0.699), 11281 (0.946), 456. Changed:

| inv | merges before | after | lower-case share before -> after | per-char change |
|---|---|---|---|---|
| 3349 | 8,660 | 8,936 | 0.990 -> 0.983 | `=` +275 (207 lower), `„` +1 |
| 3350 | 7,478 | 7,704 | 0.986 -> 0.980 | `=` +225 (170 lower), `„` +1 |
| 3351 | 0 | 174 | - -> 0.943 | `„` +171, `=` +3 |
| 3352 | 9,970 | 9,972 | 0.951 -> 0.950 | `-` +2 |
| 3353 | 7,808 | 8,127 | 0.989 -> 0.979 | `=` +318 (231 lower), `-` +1 |
| 3354 | 0 | 126 | - -> 0.968 | `„` +126 |
| 3822 | 21,486 | 10,929 | 0.779 -> 0.957 | `.` -10,551, `-` -6 |
| 455 | 50,164 | 38,932 | 0.877 -> 0.891 | `.` -11,231, `-` -1 |

   The expected 3822 (about 0.957) and 455 (about 0.891) reproduce. In hand_tar the added merges total
   1,121 (`=` 821 with 611 lower-case right sides = 0.74, `„` 297 with 283 = 0.95, `-` 3); `-` is
   nearly absent there. Lower-case share of the `=` merges is only 0.74.
5. v1 `TOTAL 157 / 157`; full suite 1,100 passed (1,098 at 15hj plus two new tests). Protected hashes
   (`15hj2-protected-{before,after}.txt`): every AS dev layer, `shared-data/`, NLP `output/` and LotA
   `data/`/`gold/` are unchanged; the only differences are 28 files under LotA `output/titles/` and
   `output/test-staged-fixture-2.5b/`, written by a concurrent LotA session (this step never writes there).
   `shared-data/line_class_preprocessed_page_json` is not readable by this account (permission denied
   in both hashes, as before). Met.

**Error analysis** (`random.Random(23)`; texts from `join/`; `ea.py`, `ea_sample.txt`, `ea_verdicts.tsv`).
- *New hand_tar merges* (20 of 1,121): right 11, wrong 5, unclear 4. Right: compounds and real breaks
  ("beurt=" + "schipper", "Crijgs=" + "articul", "Transport=" + "schepen", "dienaen„" + "gande").
  Wrong: two words glued ("aen=" + "haer", "missiven=" + "vanden", "Holstein=" + "Norburgh",
  "me=" + "commaniquer"). Unclear: garbled HTR ("teea„" + "oock"). Precision of decided cases 11/16.
- *`.` merges removed* (10 each): 455: removal right 6, wrong 4 ("on." + "fangen", "Sup." + "pliant",
  "Pal." + "land", "wer." + "den"); 3822: right 7, wrong 3 ("Kar." + "ren", "Bur." + "germeelter",
  "hon." + "derd"). The right removals are sentence ends ("Gelderland." + "Steenis") and abbreviations
  ("5e." + "meld", "Mog." + "gepresenteert").
- *11281 `:`* (5 of 5,216): 5 right ("chan:" + "delles", "geautho:" + "riseert", "res:" + "pect").

**Conclusion.** The pin does what UA-010 (c) intended and clears the anchors, but it is a trade, not a
free gain: removing `.` keeps two words apart correctly in about two thirds of the read cases and splits
a real word in about one third, and the new `=` merges in hand_tar are right in about two thirds of the
decided cases. The lower-case share hides this (0.98 for hand_tar, though 5 of 20 read are glued).

**Findings:** F07-15hj2.1 to F07-15hj2.4.

**Brief defects.**
- The changelog instruction says "below the last phase-07 entry"; the file is newest first, so the entry
  was put at the top of *Unreleased*, above the 15hj entry.
- The brief expects `-`/`=` merges to be added in hand_tar inventories "that had no `-` or `=`"; `-` is
  almost absent (3 merges), the added merges are `=`.
- The protected-hash format is not specified; `size mtime path` lines (as in `15hj-protected-*.txt`) from
  `find ... -exec stat -f`, sorted by path, over the same roots plus `data/text_layer_v2_dev/` minus the new
  layers.

**Open issues.** None for the user beyond UA-010, now resolved.

### Step 15i: pilot build, audit, 116-row evidence and publication

**What changed.** Brief:
[`step-15i-pilot-build-and-publish.md`](../plan/phase-07-sg-units-and-pilot/step-15i-pilot-build-and-publish.md),
Part A only (build, reproducibility, audit, R6 counts, 116-row evidence, crops); Part B (publication) waits for UA-011.
No library code changed. Scripts and data: `reports/data/phase-07-sg-units-and-pilot/step-15i/`
(`protect.py`, `repro.py`, `audit.py`, `rows116.py`, `rows116_verdicts.tsv`, `crops.py`, `audit.json`, `repro.json`,
`rows116.tsv`, `rows_extra.tsv`, `crops/index.html`).
- **Step 0 (pagexml-tools pin).** UA-007 is still open and PyPI lists 0.9.0 as the newest release, so the pin is skipped:
  pagexml-tools stays 0.9.0 (`MIN_PAGEXML_TOOLS`, `pyproject.toml` untouched).
- **Layers.** `data/text_layer_v2_dev/15i-a/hand/` (14 inventories from `15i-src/NL-HaNA_1.01.02/`, 6 tar symlinks and
  8 directory symlinks; `discover_inventories_v2` finds all 14) and `15i-a/print/` (shared-data zips). Both with
  `--stage all --workers 8`, no `--ignore-sanity`. Wall time: hand 2:31 (user 408 s), print 4:44 (user 509 s); exit 0,
  0 failures. One hand layer, so the WBC corpus default is pooled over 14 inventories (`default_set` `-=„`, `sanity_pass`
  true, `pooled_rule_sanity_pass` true); print: `pinned`, `derived_default_set` `-.=„`, `pooled_rule_sanity_pass` false,
  `sanity_pass` true on the pin. `word_break_stat` `default`.

**Results against the acceptance criteria.**

1. *Reproducibility* (`repro.py`, `repro.json`): 222 comparisons equal, 22 differ, all explained. Per inventory every
   decompressed table under `scans/ regions_meta/ lines/ layout/ types/ units/` and all of `join/` (tables and
   `join/stats`) equals `15hj-b` (hand_tar for 3349-3354, hand_local for the 8 others, print), with these exceptions:
   - `scans/<inv>` of the 8 local inventories: only the `member` column (the symlink path
     `data/text_layer_v2_dev/15i-src/...` instead of `data/PageXML/NL-HaNA/...`); the same string is in each
     `extract_stats/<inv>.json` `source` (14 inventories, tar ones now `15i-src/.../*.tar.gz`). Not a content change, but
     the published `member`/`source` values are dev paths (see findings F07-15i.1, for Part B).
   - `wbc/wbc_per_inventory.tsv`: the `wbc_set` column is identical for all 16; only `char_status` of the six hand_tar
     inventories differs: `-` was `pinned_in` and is now `default/pinned_in`, because the pooled derived default over
     14 inventories now contains `-`.
   Met.
2. *Audit* (`audit.py` -> `audit.json`; `--self-test` passes on the 9999 fixture): 176 of 182 invariants pass.
   Passing: every XML member (10,715 scans in total) has a scan row; every scan with status `ok` and text has >= 1 unit;
   every `regions_meta` row is in `doc_id_map`; every `unchanged` layout region keeps its id; `unit_id` unique per
   inventory; one header per table; value sets (`page`, `unit_type` = the 9 types, `old_status`, `link`, `change`,
   `layout_role`; `link=none` only on `empty` rows with `unit_id=""`); valid UTF-8, no CR, tab count = header's in
   every file; `n_segments` = rows in `join/segments`; offset invariant (0 violations, 1,650,009 segments);
   every segment box lies in its unit box (5 px); all 300 02d `hand_doc_id`s are in `doc_id_map`; LotA `gold/02-` and
   `02c-` region ids in pilot inventories: 0 and 0 (expected). *Fail*: the lower-case right-side share of merges (target
   0.95, reported, not blocking) is below 0.95 in 3351 (0.943), 3823 (0.941), 11182 (0.699), 11281 (0.946), 455 (0.891)
   and 456 (0.931): known since 15hj (17,128 merges have an upper-case or digit right side, 15 of 20 read wrong; a guard
   needs `join.py`), not a build error. Protected hashes (`15i-protected-{before,after}.txt`, same format as 15hj's;
   AS dev layers except `15i-*`, all of `shared-data/`, LotA `data/ gold/ output/`, NLP `output/voc_text_layer`): the
   only differences are the mtimes of two LotA `output/test-staged-fixture-2.5b/` files, written by a concurrent LotA
   session (this step writes nowhere there). Met, with the merge-share failures listed.
3. *R6 counts* (per inventory; `audit.json` `counts` has more: `scan_status`, `units_per_type`, `segments`, `merges`):
   whitespace repairs 0 everywhere (UA-003); `word_break_chars` `-=„` (11281 `-:=„`); `word_break_stat` `default`.

| inv | scans | no text | lines | legacy regions | layout regions (+empty) | units | old_status identical/trimmed/changed/empty | gutter | merge lower-case share |
|---|---|---|---|---|---|---|---|---|---|
| 3349 | 963 | 6 | 92,152 | 12,121 | 12,096 (+28) | 14,150 | 10933/1157/3/28 | main_edge | 0.983 |
| 3350 | 894 | 7 | 84,789 | 9,731 | 9,727 (+21) | 11,996 | 8486/1209/15/21 | main_edge | 0.980 |
| 3351 | 219 | 19 | 12,598 | 4,148 | 4,145 (+4) | 4,693 | 3920/223/1/4 | main_edge | 0.943 |
| 3352 | 1028 | 5 | 96,844 | 11,175 | 11,152 (+23) | 13,357 | 9912/1240/0/23 | main_edge | 0.950 |
| 3353 | 922 | 8 | 85,134 | 10,611 | 10,600 (+11) | 12,615 | 9481/1119/0/11 | main_edge | 0.979 |
| 3354 | 217 | 28 | 11,161 | 3,777 | 3,776 (+2) | 4,230 | 3599/175/1/2 | gap | 0.968 |
| 3771 | 579 | 7 | 139,321 | 11,894 | 11,696 (+198) | 15,011 | 8768/2928/0/198 | gap | 0.987 |
| 3772 | 595 | 8 | 143,929 | 11,570 | 11,435 (+135) | 14,843 | 8410/3025/0/135 | gap | 0.984 |
| 3773 | 608 | 5 | 147,639 | 11,773 | 11,649 (+124) | 15,130 | 8557/3092/0/124 | gap | 0.985 |
| 3821 | 440 | 5 | 119,045 | 18,032 | 17,447 (+585) | 19,824 | 15770/1677/0/585 | gap | 0.963 |
| 3822 | 463 | 3 | 124,679 | 18,117 | 17,383 (+734) | 20,200 | 15509/1874/0/734 | gap | 0.957 |
| 3823 | 442 | 6 | 121,664 | 17,625 | 16,775 (+850) | 19,832 | 14813/1962/0/850 | gap | 0.941 |
| 11182 | 755 | 6 | 57,713 | 5,512 | 5,478 (+39) | 6,238 | 4846/623/4/39 | main_edge | 0.699 |
| 11281 | 1139 | 20 | 78,109 | 7,935 | 7,930 (+21) | 9,353 | 6682/1219/13/21 | main_edge | 0.946 |
| 455 | 721 | 6 | 173,515 | 140,094 | 8,787 (+130,751) | 28,079 | 4344/499/4500/130751 | gap | 0.891 |
| 456 | 730 | 4 | 175,474 | 3,099 | 5,304 (+90) | 25,030 | 1031/124/1854/90 | gap | 0.931 |
| total | 10,715 | 143 | 1,663,766 | 297,214 | 165,380 | 234,581 | | | |

   Units per type (all): main_text 128,238; marginalia 49,422; date_header 19,807; page_number 14,991; catch_word 8,213;
   running_header 5,339; index 4,310; other 3,486; signature_mark 775. Split lines (print `split_stats`, bigram +
   hyphen): 455 11,504, 456 64,265. Known gaps: the 143 scans without text are 133 `no_regions` (no TextRegion; 3351 18, 3354 27, 11281 20, the others 3-8 each, none in 455) and 10 `no_words` (regions but no text line: 455 6, 3351 1, 3354 1, 3771 1, 456 1); 455's 130,751 empty layout regions are the empty Laypa regions of F06-15d.2 (130,737 legacy regions without lines),
   which is why its `old_status` has 130,751 `empty`.
4. *116 rows* (`rows116.py`, `rows116.tsv`). Hand tokens found for **111** of 116 (target 110) and print tokens for
   **110** (target 105). Misses (hand): rows 284 and 255 (hand tokens empty in the gold sheet), 19 (page number "247." no
   longer in the text), 9 and 80 (furniture, marginalia, or a line from elsewhere that the old region had joined: the
   string no longer exists, that is the fix). Misses (print): 133 ("vrykost -" is now "vrykosten": fuzzy 0.78 < 0.8),
   284 (print string is longer than the new unit), 134, 50 and 80 (print tokens empty), 188 (reordered: 0.72). Print
   strings of rows 8, 229 and 250 are found only after widening from the paragraph's units to all units of its scan(s)
   (`print_scope=scan_units`): the REPUBLIC paragraph span maps to units that miss the string.
   Predicted counts: unchanged_free 55, changed 10, and for the 51 artefact rows persists_glue 26, persists_htr_ocr 14,
   fixed 6, needs_realignment 4, unclear 1. **Predicted share free `(55 + 6) / 116` = 0.526; upper bound with
   `needs_realignment` 65/116 = 0.560; target 0.80, old baseline 0.56 (65/116, the non-artefact rows).** Flags: 5 rows
   with a print token of >= 18 letters, 2 with a hand span in a non-`main_text` unit, 0 print spans over two layout
   columns. Per label (predicted): print_ocr_error 21: glue 11, HTR/OCR 9, realign 1; segmentation 16: glue 14, HTR/OCR 2;
   hand_htr_error 7: fixed 3, glue 1, HTR/OCR 3; misaligned 4: fixed 1, realign 3; unclear 3: fixed 2, unclear 1.
5. *Crops* (`crops.py`): `crops/index.html` + 64 JPEGs (2.2 MB, largest 50 KB) for rows 39, 60, 241 and 30 random labelled
   rows (`Random(20)` over the other labelled rows); the boxes of 241 were opened and sit on the right line in hand and
   print. Row 60 is **unlabelled** in the gold sheet (not one of the 116); its evidence is in `rows_extra.tsv` (hand
   "hautes" and print "thaures" found exactly). Not fetched: the IIIF links.

**Error analysis.** All 51 artefact rows were read (note, old tokens, new matched text, unit types; verdicts and reasons in
`rows116_verdicts.tsv`), the 10 `changed` rows, and 20 `unchanged_free` rows (`Random(15)`). Reading, in the order of the
classes:
- *persists_glue* (26): both strings are found exactly and the print (or hand) still lacks the space, e.g. row 241 hand
  "het selve" vs print "hetselve", row 40 print "hetgepasseerdein delaetsteactie", row 91 hand "inde" vs print "in de". 2 of
  them (178 "toegesondengeworden,", 31 "procedureninstate ensurcheantie.") had their line-end hyphen merged by the pinned
  set, and the glue is what is left; 8 and 140 stay split at "rem. boursement" / "geresu- meertende" because `.` and a
  hyphen followed by a gap are not merged.
- *persists_htr_ocr* (14): character errors survive unchanged: "claudestin" vs "clandestin" (190), "Heerer" (216),
  "gerofineerde" (227), "laranVcon" (264) and "vanJder" (23), where the stray capital is the margin bleed that
  the old " - J" also showed.
- *fixed* (6): rows 134 ("21. April" now a `date_header` unit), 50 ("909" a `page_number` unit), 9 (date header and
  two marginalia now separate units; the HTR error "Zeelve" stays), 80 (the line from the bottom right no longer follows
  "huuren."), 229 (print "nnekon-" is its own unit before "Waerop gedelibereerdt zijnde,"), 284 (the print unit ends at
  "gevallen.").
- *needs_realignment* (4): 92 and 44 (spans differ in extent, text unchanged), 188 (print order now "daed in behoorlijcke
  forma torsijne narichtinge."), 133 (print "vrykosten" vs hand "vrij kost" + "en"). *unclear* (1): 250, one `main_text`
  unit in hand and in print still interleaves a Dutch and a French column.
- *`changed` rows* (10, none an artefact label): improvements by the hyphen merge (66, 105, 122, 28, 88: "byre - Jductie"
  -> "byreJductie"), page numbers removed from hand text (55, 52, 19), 150 print "voorschreveRequestege voeght," (a
  new split inside a glued word) and 255 (empty hand tokens in the gold sheet).
- *`unchanged_free` sample* (20): all 20 are the same text in the same `main_text` unit (e.g. 192 hand "van 300:
  Rijcxdaelders" vs print "vandriehondertrijckdaelders"; 22 "off" vs "of"), but 6 of the 55 have a token of <= 3 characters
  (rows 119, 22, 244, 125, 157, 5), found as a substring anywhere in the candidate units, so "still found" is weak
  evidence there (F07-15i.6).

**Conclusion.** The predicted share free is 0.526 (upper bound 0.560), well below 0.80, and slightly below the old
56%: 40 of the 51 artefact rows are untouched because the artefacts are in the transcription itself, not in the unit
boundaries. 26 are missing whitespace (glue repair off, UA-003) and 14 are character-level HTR/OCR errors; the new
layer changes the unit boundaries (furniture, marginalia, stray lines and over-long print paragraphs separated; the
hyphen merges), which fixes 6 rows and moves 4 to re-alignment. The 80% target is therefore not reachable on this layer
without a glue repair or a different treatment of those rows; the real check is LotA's re-alignment (hand-off 4). One
caveat for publication: the pinned `=` merges added in hand_tar (3349, 3350, 3353; 821 merges) were read right in only
11 of 20 cases (5 glue two words: F07-15hj2.3). This is raised in UA-011.

**Part B (publication, 2026-10-21, Sonnet; UA-012 answered a).** The layer published is `15i-b` (hand rebuilt by 15i2, print
byte-equal to Part A). Checks first: neither target existed, parents are real directories and writable. `cp -R` of
`15i-b/hand/…` to `shared-data/NL-HaNA_1.01.02/text_layer_v2/` (305 MB) and `15i-b/print/…` to
`shared-data/NL-HaNA_1.10.94/text_layer_v2/` (84 MB), without `progress.log` and `status.tsv`; `chmod -R g+w`. The
`publication` key was added to both `manifest.json` by `step-15i/publish_manifest.py` (14 and 2 inventories; sha256 of the 6 tar and
2 zip files re-checked and equal to the brief's; the 8 local hand inventories recorded as paths with file counts 580, 596, 609, 441, 464, 443,
756, 1,140). Written outside AS: shared-data `README.md` (2 catalogue rows, 1 changelog line), LotA `docs/from-archival-structures.md` (new §6),
nothing else. Protected-hash comparison (`step-15i/15i-protected-B-{before,after}.txt`): the only change is the `README.md` of
shared-data and the new files under the two `text_layer_v2/` dirs. Audit re-run on the published copies
(`step-15i/audit_published.json`, via symlinks named `*_text_layer_v2`; the audit globs that suffix): 176 of 182 pass, the same
6 merge-share failures; invariants and actual values identical to `step-15i2/audit.json`. Known gaps are in the manifest
(dev paths in `argv`/`member`/`source`; `as_version` 0.9.0 as recorded by the build, built on a dirty tree at git head 73e1517; glue not repaired; 6 merge-share
failures; partial coverage). Acceptance 5 met. Brief defects: `audit.py` finds a layer by `*_text_layer_v2`, so the published dir name needs symlinks to
audit it in place; the brief's "3351 and 3354 changed" is only true for 3351 (3 merges), per 15i2.

### Step 15i2: hand_tar pin without =

**What changed.** Brief: [`step-15i2-drop-eq-hand-tar.md`](../plan/phase-07-sg-units-and-pilot/step-15i2-drop-eq-hand-tar.md)
(UA-011 b). `SG_HAND_V2.wbc_pinned` now has `-„` for 3349, 3350, 3351, 3353 and 3354 (default `-=„`, 11281 `-:=„`,
3352 and the other 7 take the default); test `test_sg_hand_pins`; `docs/text_layer.md` and the changelog entry
*Step 15i2*. New layer `data/text_layer_v2_dev/15i-b/{hand,print}/`: hand rebuilt (`--stage all --workers 8`, exit
0, no `--ignore-sanity`, 0 failures), print copied unchanged from `15i-a/print`. Data and scripts:
`reports/data/phase-07-sg-units-and-pilot/step-15i2/` (`repro.py`, `repro.json`, `deltas.py`, `deltas.json`,
`removed_eq.tsv`, `ea_verdicts.tsv`, `audit.json`, `rows116.tsv`, `rows116_verdicts.tsv`, `protect.py`, `build.log`).

**Results against the acceptance criteria.**
1. Build exit 0; `wbc_per_inventory.tsv` `wbc_set`: `-„` for 3349, 3350, 3351, 3353, 3354, `-:=„` for 11281, `-=„` for
   the other 8; `corpus_default.json` `sanity_pass: true`.
2. Against `15i-a/hand` (decompressed bytes): `scans/ regions_meta/ lines/ layout/ types/ units/` identical in all 14;
   `join/` (units, segments, stats) identical in 3352 and the 8 hand_local (16 of 16 files equal per inventory) and
   different in 3349, 3350, 3353 **and also 3351 and 3354** (see brief defects: they have `=` line ends too; 3351 loses
   3 merges, 3354 changes 3 segment rows and 2 `ends_with_break_char` flags, 0 merges). Other differing files:
   `manifest.json`, `progress.log`, `status.tsv`, `wbc/*` (`corpus_default.json`, `wbc_per_inventory.tsv`: sets,
   `char_status`), and the layer-level `layout/types/units stats.json` (timings/paths). `15i-b/print` equals `15i-a/print`
   in all 54 files, byte for byte.
3. `=` merges and totals (all merges per inventory, `15i-a` to `15i-b`):

   | inv | merges a | merges b | removed | `=` merges a / b | lower-case share a | b |
   |---|---|---|---|---|---|---|
   | 3349 | 8,936 | 8,660 | 276 | 275 / 0 | 0.9829 | 0.9903 |
   | 3350 | 7,704 | 7,478 | 226 | 225 / 0 | 0.9799 | 0.9864 |
   | 3353 | 8,127 | 7,809 | 318 | 318 / 0 | 0.9788 | 0.9891 |
   | 3349+3350+3353 | 24,767 | 23,947 | 820 | 818 / 0 | 0.9806 | 0.9887 |
   | 3351 | 174 | 171 | 3 | 3 / 0 | 0.9425 | 0.9415 |
   | 3354 | 126 | 126 | 0 | 0 / 0 | 0.9683 | 0.9603 |
   | hand_tar (6, incl. 3352 unchanged) | 59,806 | 58,163 | 823 | | 0.9754 | 0.9819 |

   820 removed against 818 `=` merges: in 3349 one extra `„` merge (a boundary next to a removed `=` merge) is also gone.
   The `-` and `„` merges are otherwise unchanged. The 3354 share drops 0.9683 to 0.9603 (122 to 121 of 126) because
   the boundary guard that moved (`g_digit_right` to `plain`) changed one merge's right-hand character.
4. `audit.py --layer 15i-b`: 176 of 182 pass, the same 6 failures as Part A (merge lower-case share below 0.95 in 3351
   0.9415, 3823 0.9405, 11182 0.6987, 11281 0.946, 455 0.891, 456 0.9313; known, F07-15i.2). Every invariant that passed
   still passes; the only changed values are the merge shares above (3349, 3350, 3353 up, 3351, 3354 down slightly).
5. `rows116.py --layer 15i-b`: 116 rows, hand tokens found 111, print 110; **no row differs** from `step-15i/rows116.tsv`
   in any column (hand columns, `predicted`, `reason`), so Part A's verdicts are unchanged and `crops.py` was not rerun.
   Predicted share free 0.526 (61/116), upper bound 0.560 (as in 15i).
6. Full AS suite: 1,101 passed (1,100 + `test_sg_hand_pins`). Protected hashes (`data/text_layer_v2_dev/15i2-protected-
   {before,after}.txt`, 99,149 files, with `15i-a` included and only `15i-b`/`15i-src` excluded): no difference at all,
   LotA `output/` included.

**Error analysis.** 818 removed `=` merges (3349 275, 3350 225, 3353 318). Twenty drawn with `random.Random(24)`
(`ea_verdicts.tsv`, context of 30 characters either side, the left token's `=` is not shown). Verdict on the *removal*:
- right to split (two words; 11): "Pottebackers | Aerde", "Generael | Major", "aen | haer Ho:Mog:", "encore | a paijer",
  "Maj | Imperiale", "van Steijn | Callefelts", "Henrij | Chapelle", "paijs | de Liege", "Neusen | Ambacht",
  "vrouwen | Gasthuijsen", "Gedep | tot";
- wrong to split (a compound now broken; 6): "Lant | zaten", "Visch | schuijten", "Transport | schepen",
  "Opper | Stuirman", "Scheeps | vaert", "rent | Ampts" (rentampt);
- unclear (3): "dienst | vrundelijck", "cij | apres" (ci-apres), "tres | nestamment" (mangled).

The cost of (b) on this sample is 6 of 20 (30%) real words split again, against 11 of 20 wrong merges removed; the
15hj2 sample read 11 right, 5 wrong, 4 unclear for keeping the merges. The two readings agree that `=` is mixed, about
half or more of the merges being wrong. Pattern: `=` after a function word or abbreviation or before a proper
noun/capital is a hyphen-less join, `=` inside a compound after a noun stem is a true break. Both are caught only by
the right-side capital, which the pinned set cannot use. One reader, short context, so only the direction is
reliable. The capital-right guard (`join.py`, 17,128 such merges overall) would address both.

**Conclusion.** Option (b) is cheap and exact: 820 merges fewer in three inventories, 3 in a fourth, nothing else
changes in the layer, the audit and the 116-row evidence are identical, and the lower-case share of the three
inventories reaches 0.989. The price is about a third of the removed `=` boundaries being real compound breaks.
The brief's premise that 3351 and 3354 stay unchanged was wrong (F07-15i2.4). Publication waits for UA-012.


## Review

Phase review (Opus, 2026-10-21). The code review is in
[`REVIEW.md`](../plan/phase-07-sg-units-and-pilot/REVIEW.md). Verdict: accept with follow-ups; the
fixes are step 15ir ([brief](../plan/phase-07-sg-units-and-pilot/step-15ir-review-fixes.md)).

**Re-run.** AS suite 1,101 passed; NLP 56 and LotA 367 (+1 xfail) against the working tree; v1
`TOTAL 157 / 157`; `audit.py --layer 15i-b` 176 / 182 with the same 6 merge-share failures;
`rows116.py --layer 15i-b` equal to 15i2's table in all 116 × 33 cells; the published dirs equal
`15i-b` (only `manifest.json` differs, by `publication`). Offsets were re-derived for all 1.65 M segments
(0 mismatches). F07-R.7.

**Leakage.** The 15h cue fit is clean (F07-R.6): per-region geometry, dev-only grid, one test score,
REPUBLIC read after the units exist. The caveat is the `split_half` reference class, designed on 456
(test); F07-15h.3 is revised for it.

**Own error analysis.**
- *11182 merges* (all 1,416 non-lower-case right sides counted, 15 read): 1,305 start with `,`, and
  every one read is a continuation ("bin|,nen", "por|,tien", "geschie|,den"). The same holds in 3352 and
  11281 (6 read each). The merges are right, but the comma stays in the word (F07-R.2). F07-15hj.5 and
  F07-15i.2 are revised: the low share is not mostly upper-case right sides.
- *Print line order* (3 units read, all counted): `455_0010` reads y 2952 → 3102 → 1371 → … → 1322
  across the column. Counted over the layer: 335 units in 455 and 160 in 456 jump back > 50 px. In 455
  they come from 416 `split_at_column` and 127 `unchanged` regions (F07-R.1). Three of the 116 rows are
  in such units. F07-15fq.8 is revised.
- *Unit ids*: all 77 bumped units whose box is a legacy box hold every line of that legacy region
  (F07-R.3). F07-15h.8 is revised.
- *Types and counters*: the subagents' counts were re-derived (`rule:pn_line` 10,327, 19 whole date lines
  split, blocked counters 1,322 / 62 and 673 / 113). F07-R.4 and F07-R.5; F07-15g.6, F07-15fq.7 and
  F07-15fq2.1 are revised.
- The steps' own error analyses otherwise hold: each sample names its seed and has a verdict file, and
  the negative results are reported as such.

**Candidates for `docs/findings.md`** (not edited): F07-15fp.3 (Laypa print regions are page-level and
interleave columns), F07-P.10 (HTR merges the date header, text and page number into one top line),
F07-15fq.3 (overlap merging fails on SG hand: 0.52), F07-R.2 (line-initial `,` continuation mark),
F07-15hj2.2 (`.` as a word-break char is right only about half the time).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F07-R.1–R.5, REVIEW rows 6–10 | Code fixes, tests, docs, LotA known-issues note; dev layer `15ir-a` | [`step-15ir-review-fixes.md`](../plan/phase-07-sg-units-and-pilot/step-15ir-review-fixes.md) |
| F07-R.1–R.4 | Republish a fixed pilot, or carry the fixes into the full run | UA-013 (`docs/USER_ACTIONS.md`) |
| F07-R.8 | Commit the phase-07 tree and release v0.11.0 before 15ir | `REVIEW.md` *Release* |
| F07-15hj.6 | `g_upper_right` guard (needs `join.py`) | phase 08 planning (`plan/CONTEXT.md`) |
| REVIEW row 11 | Crosswalk role column (`line_end` ids map to the first half; 553) | phase 08 planning |
| REVIEW rows 12, 13 | Interleave guard bridged by a third region; bottom-band years typed `page_number` | phase 08 planning |
| REVIEW row 8 | Move the REPUBLIC default path out of the library | phase 08 planning |
| F07-15i.3 | The ≥ 80% acceptance (UA-001) on LotA's re-alignment | LotA (D19) |
| UA-007 | pagexml-tools 0.9.1 pin | `docs/USER_ACTIONS.md` |

## Reproduce

Step 15fp, from the AS root with `.venv/bin/python` (pagexml-tools 0.9.0). Copy the layers
first, then run:

```
D=data/text_layer_v2_dev
mkdir -p $D/15fp-a/{hand_tar,hand_local,print}
cp -R $D/15d-a/hand_tar/NL-HaNA_1.01.02_text_layer_v2 $D/15fp-a/hand_tar/
cp -R $D/15d-a/hand_local/NL-HaNA_1.01.02_text_layer_v2 $D/15fp-a/hand_local/
cp -R $D/15fr-a/print/NL-HaNA_1.10.94_text_layer_v2 $D/15fp-a/print/
.venv/bin/python -m archival_structures.text_layer.build --out-dir $D/15fp-a/hand_tar --preset sg_1.01.02_v2 --stage layout
.venv/bin/python -m archival_structures.text_layer.build --out-dir $D/15fp-a/hand_local --preset sg_1.01.02_v2 --stage layout
.venv/bin/python -m archival_structures.text_layer.build --out-dir $D/15fp-a/print --preset sg_1.10.94_v2 --stage layout
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/acceptance.py
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/split_proxies.py
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fp/ea.py merges OUT.txt   # also: pageside, mincov, roles
```

The hand verdicts are in
[`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15fp/ea_verdicts.tsv).

Step 15fq, from the AS root with `.venv/bin/python` (pagexml-tools 0.9.0). Copy the layers
first (`15fq-a`, `15fq-legacy`, `15fq-b`), then run the layout stage:

```
D=data/text_layer_v2_dev
for L in 15fq-a 15fq-legacy 15fq-b; do mkdir -p $D/$L/{hand_tar,hand_local,print}
  cp -R $D/15d-a/hand_tar/NL-HaNA_1.01.02_text_layer_v2 $D/$L/hand_tar/
  cp -R $D/15d-a/hand_local/NL-HaNA_1.01.02_text_layer_v2 $D/$L/hand_local/
  cp -R $D/15fr-a/print/NL-HaNA_1.10.94_text_layer_v2 $D/$L/print/; done
B=".venv/bin/as-build-text-layer --stage layout"
# 15fq-a: hand merges on (the preset now has merge=false)
$B --out-dir $D/15fq-a/hand_tar --preset sg_1.01.02_v2 --inventories 3349,3350,3351,3352,3353,3354 --layout-params '{"merge": true}'
$B --out-dir $D/15fq-a/hand_local --preset sg_1.01.02_v2 --inventories 3771,3772,3773,3821,3822,3823,11182,11281 --layout-params '{"merge": true}'
$B --out-dir $D/15fq-a/print --preset sg_1.10.94_v2 --inventories 455,456
# 15fq-b (final): preset defaults; 15fq-legacy: --layout-params = LEGACY_15FP.to_dict() as JSON
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq/acceptance.py [LAYER_DIR]
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15fq/ea.py merges OUT.txt  # also: pagesplit, devscans, furniture, colsplit, guard
```

The 15fq-a runs were made while the hand preset still had merges on; the `--layout-params`
override above reproduces them. Verdicts:
[`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15fq/ea_verdicts.tsv).

Step 15g, from the AS root with `.venv/bin/python`. Copy the 15fq-b layer, run the types stage,
then the acceptance and error-analysis scripts:

```
D=data/text_layer_v2_dev; mkdir -p $D/15g-a && cp -R $D/15fq-b/* $D/15g-a/
B=".venv/bin/as-build-text-layer --stage types"
$B --out-dir $D/15g-a/hand_tar --preset sg_1.01.02_v2
$B --out-dir $D/15g-a/hand_local --preset sg_1.01.02_v2
$B --out-dir $D/15g-a/print --preset sg_1.10.94_v2
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g/acceptance.py [LAYER_DIR]
.venv/bin/python reports/data/phase-07-sg-units-and-pilot/step-15g/ea.py type:page_number OUT.txt
# also type:<TYPE>, split:<RULE>, recall:hand, recall:print, indexpages
```

Verdicts: [`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15g/ea_verdicts.tsv).

Step 15hj, from the AS root (the v1 check is the brief's command; `$S` = `data/text_layer_v2_dev/15hj-v1check`):

```
D=data/text_layer_v2_dev; cp -R $D/15h-a $D/15hj-a
for x in "hand_tar sg_1.01.02_v2" "hand_local sg_1.01.02_v2" "print sg_1.10.94_v2"; do set -- $x
  .venv/bin/as-build-text-layer --out-dir $D/15hj-a/$1 --preset $2 --stage wbc --workers 4
  .venv/bin/as-build-text-layer --out-dir $D/15hj-a/$1 --preset $2 --stage join --workers 4 --ignore-sanity
done
.venv/bin/python -I reports/data/phase-07-sg-units-and-pilot/step-15hj/acceptance.py
.venv/bin/python -I reports/data/phase-07-sg-units-and-pilot/step-15hj/ea.py
.venv/bin/python -I reports/data/phase-07-sg-units-and-pilot/step-15hj/ea_verdicts_build.py
.venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S      # TOTAL 157 / 157
```

Verdicts: [`ea_verdicts.tsv`](data/phase-07-sg-units-and-pilot/step-15hj/ea_verdicts.tsv).
