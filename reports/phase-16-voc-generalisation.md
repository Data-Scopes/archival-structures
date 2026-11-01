# Phase 16 report: A second archive (1.04.02 VOC): what generalises

> **Status:** final (reviewed 2026-11-01).
> **Phase plan:** [`plan/phase-16-voc-generalisation/PLAN.md`](../plan/phase-16-voc-generalisation/PLAN.md).
> **Code state:** unreleased after v0.20.0 (release recommended as v0.21.0); no sibling code changes (22e appends one
> note to an NLP doc).
> **Last updated:** 2026-11-01, by the phase-16 review (Opus 5.5).

## Summary

Phase 16 ran the SG-tuned components on a second archive, the VOC (NL-HaNA 1.04.02), with the SG rules unchanged. It
then tried a small set of VOC options fixed at planning, each against the SG bar on gold the components were never
fitted to.

- **Text-layer build:** carries over unchanged. It built 567 inventories with `failures=0` and audit
  `n_fail_judged = 0`, with every volume `hand` (F16-22a.1, F16-22a.2).
- **Folio map:** fails. Leaf 0.608 (v1), 0.579 (v2) and 0.654 with Laypa page-number text, against 0.95 on 904
  Generale Missiven anchors. VOC numbers are tied, restart and gap, and the chains are noisy (LOO 0.884 against SG's
  0.986) (F16-22b.1/.2/.4/.8).
- **Index-side detector:** fails. Precision 0.019 and recall 0.016 unchanged; at best precision 0.067 and recall 0.393.
  The detector is built for SG's printed two-column indexes, while VOC tables of contents are one-column, and ledger
  pages look like index pages to it (F16-22c.1/.2/.4).
- **Index links:** fail even with gold TOC sides. E is 0.345 (L2) and 0.552 (L7); the VOC locator L8 reaches 0.644,
  well above the +3-leaf chance of about 0.24 but far below 0.90 (F16-22d.1/.3, F16-R.1). The shipped pipeline finds
  no front index in any VOC volume (F16-22d.8).
- **Six other components:** run without error but have no VOC gold (F16-22e.3).

What generalises is the geometry-and-plumbing layer. What does not is everything that encodes SG conventions: how
numbers are written and placed, what an index page looks like, and how long index runs are (F16-22e.2). The review
found and fixed one scorer defect (22r), which moved 22d by about one point (F16-R.1). Every bar reproduces (F16-R.2).
Code: one additive preset and three additive options, all off by default; SG outputs are unchanged.

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F16-P.1 | The Generale Missiven overview gives 1,841 distinct folio ↔ scan anchors in 545 inventories (1171 excluded). Scans per folio between a missive's begin and end have a median of 2.006 in `Enkel` (single-page) volumes and 1.007 in `Dubbel` (opening) volumes, as expected if folio numbers sit on rectos in both layouts. On v1 Laypa page-number anchors, `numbering_mode` calls 167 of 170 dev `Enkel` volumes `foliation` (about 2 scans per number) and 95 of 101 dev `Dubbel` volumes `pagination` (1 scan per number, i.e. foliated openings). **Revised at review:** `Dubbel` is not a scan layout; 57 `Dubbel` volumes are portrait single-page scans with the same 1.01 scans per folio, so there the GM folio behaves as a page number (F16-22a.3, F16-R.3) | data-property | voc, folio-map, gold | revised | [Planning](#planning) |
| F16-P.2 | A scan-level baseline (v1 Laypa `page-number` anchors, `page_numbering` defaults, LIS + interpolation) puts the GM gold scan in folio N's leaf for 0.647 of dev anchors (606/937; strict 0.505). Of 8 misses read: 3 have the same number on two consecutive `Dubbel` scans, 1 is an HTR misread (1302 for 1102), 1 has a prefix the pattern refuses ('sur 311'), and 3 have no number within 1–4 scans of the gold | data-property | voc, folio-map | confirmed | [Planning](#planning) |
| F16-P.3 | **Revised at review.** Of the 11 GM volume parts the planner listed as having the folio fall along scans, 6 do so in the frozen anchors (`gm_anchors.tsv`): gold errors 3 (1269, 2550, 2808), nested or duplicate registrations 2 (1696, 2394) and one numbering restart (1341: folio 1 at scan 855). The other 5 (1279, 1363, 1408, 1614, 3987) show no decrease, and the planning list cannot be reproduced (F16-22b.7, F16-R.5) | data-property | voc, folio-map, gold | revised | [Planning](#planning) |
| F16-P.4 | The SG v5 preset, with only the archive plumbing changed, ran on 8023 and 2775 with `failures=0` (18 s). Both volumes are `hand` (box height 212 and 172 px against the SG cut 87.75) and every side is `single`. The detector flagged 0 of 2775's 8 TOC scans and 3 other sides. 363 of 403 `page_number` segments in 2775 are `rule:pn_line_offchain` | result | voc, text-layer, index | confirmed | [Planning](#planning) |
| F16-P.5 | 21 of the 22 GLOBALISE inventories open with a table of contents (*Register der brieven/papieren*): 186 "Table of contents" scans, from 1 to 40 per volume, starting at scan 5–15. 2555 also has mid-volume TOC runs (621–625, 745–749). In the dev text the folio numbers sit in short lines of their own or as a leading range ("288 a 291.", "1960 tot 1962"), and entries often begin with a receipt date ("den 25 Maart") | data-property | voc, index | confirmed | [Planning](#planning) |
| F16-P.6 | VOC PageXML carries Laypa region tags (`page-number`, `header`, `marginalia`, `paragraph`, `catch-word`, `signature-mark`), which SG hand volumes lack | data-property | voc, pagexml | confirmed | [Planning](#planning) |
| F16-22a.1 | The SG v5 stages run unchanged on 567 VOC inventories (545 GM + 22 GLOBALISE) with `failures=0` and `n_fail_judged = 0` (10,069 pass, 148 fail, all reported: one check, "merges with lower-case right side" below 0.95 in 148 volumes, min 0.8404) | result | voc, text-layer, audit | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.2 | All 567 VOC volumes are `hand`, none `print`: box height min 109, median 177.5 (GM) and 181 (GLOBALISE), max 273.5, so the SG cut 87.75 is never close, although VOC scans are larger than SG ones (SG hand median 137) | data-property | voc, text-layer, mode | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.3 | The SG side rule agrees with the GM `layout` label in 483 of 545 volumes (88.6%): `Enkel` 340/343, `Dubbel` 143/202. Of 59 disagreeing `Dubbel` volumes, 57 have portrait scans (aspect 0.70–0.76) read as `single`: the label does not say "two pages per scan"; the 3 `Enkel` exceptions are landscape openings (1603, 2770, 3366) | data-property | voc, text-layer, sides, gold | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.4 | 342,451 `page_number` segments: 53.8% `pn_line`, 42.0% `pn_line_offchain` (SG hand 17.9%). In a 30-segment sample 27 are folio numbers and 2 paragraph numbers; of 20 Laypa page-number lines with no segment 9 are missed folio numbers and 5 are years. Laypa agreement P 0.650 R 0.825 | data-property | voc, text-layer, page-number | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.5 | `¬` ends 116,871 lines (0.66%) and is never joined (116,881 still in the join text; 6.64 per 1,000 lines) because the word-break set is `-=„`; the HTR reads the repeated mark at the next line start as `„` (4 of 10 read) | open-issue | voc, text-layer, word-breaks | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.6 | The SG v5 index flags fire on 122 sides in 17 of the 22 GLOBALISE volumes and hit 1 of the 186 gold TOC scans (TP 1, FP 121, FN 185) | negative-result | voc, index | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22a.7 | Segment mix differs from SG hand: `marginalia` 16.3% (SG 8.7%), `catch_word` 2.1% (0.1%), `other` 1.8% (0.1%); in 10 read sides 6 have the right unit order, 3 are interleaved or out of order, 1 is cut mid-sentence; marginalia and catch-words are placed by type, not position | data-property | voc, text-layer, units | confirmed | [Step 22a](#step-22a-the-voc-text-layer-with-the-sg-v5-rules-unchanged) |
| F16-22b.1 | Bar F (VOC) fails for the unchanged folio maps on the 273 test volumes: map v1 (V0, off, T) 550/904 = 0.608 (Wilson 0.576-0.640, strict 0.500) and map v2 (V2, off, T) 523/904 = 0.579 (0.546-0.610); target 0.95 (SG gold 62/63) | negative-result | voc, folio-map, bar-F | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.2 | The dev-selected cell (V0, off, TL), dev 647/929 = 0.696, reaches 591/904 = 0.654 (0.622-0.684, strict 0.535) on test, still far below 0.95; verdict `fails on VOC`. Per layout: Enkel 0.678, Dubbel 0.606, openings 0.698, single 0.637 | negative-result | voc, folio-map, bar-F | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.3 | Of the options, only Laypa `page-number` text (N) helps (+3.9 points dev, +4.6 test); `single_foliation` (S) changes nothing the leaf bar sees (-1 point dev: it only moves chain positions from sides to scans and the target back), and the v2 rules G1-G3 cost 3 points (G1 cuts the chain of 60 volumes and 139 test anchors get no answer) | negative-result | voc, folio-map, options | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.4 | Of 70 read misses the largest classes are tied or duplicated numbers 16 (the same number on 2-3 consecutive scans, or a repeated header such as 1037/1382/1465), restart or second numbering 10, gap 10 and gold error 9; the right place is printed only 78% of the time (463/597) | result | voc, folio-map, error-analysis | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.5 | 9 of the 70 read misses (13%) are gold errors where the printed number on the gold scan contradicts the gold folio, roughly 4-5% of all anchors (wide interval); the bar slack is 5%. A check page for the user is UA-059 | result | voc, gold-quality | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.6 | VOC GM volumes are mostly single-page scans foliated on every other scan: in 397 `single` volumes 89,339/99,093 = 90.2% of chain steps of one number are exactly 2 scans; in 146 `openings` volumes the median `recto_share` is 0.903 and only 74 reach the SG 0.9 foliation rule | result | voc, foliation, rectos | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.7 | The brief's restart criterion (an extra monotone run of >= 10 anchors) flags 509 of 545 volumes and is noise; with a compactness filter (post-hoc) 6 volumes remain (1085, 1542, 1718, 1816, 2004, 2061). Of the 11 decreasing-folio parts, 6 decrease in the frozen gold: gold error 3, nested or duplicate registration 2, numbering restart 1 (1341); the other 5 show no decrease (1363 has a local gold error, 4 unclear) | negative-result | voc, restarts, gold-quality | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22b.8 | Leave-one-out on the chain is 87,262/98,750 = 0.884 on dev (all volumes 0.878), against 0.986 on SG: the VOC chains are noisy, so interpolation from them is the weak point, not the printed hit | result | voc, folio-map, loo | confirmed | [Step 22b](#step-22b-the-folio-map-on-voc) |
| F16-22c.1 | Bar D (VOC) fails for both cells on the 10 test volumes (61 TOC scans). Unchanged SG detector: P 1/53 = 0.019 (Wilson 0.003–0.099), R 1/61 = 0.016 (0.003–0.087). Dev-selected cell `tmc8_som0.25_cue_gap2`: P 24/357 = 0.067 (0.046–0.098), R 24/61 = 0.393 (0.281–0.519). Verdict `fails on VOC`; no `VOC_INDEX_SIDES_V1` added | negative-result | voc, index, bar-D | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22c.2 | The SG features cannot see VOC TOCs. Of 117 dev TOC scans the unchanged detector finds 0 (FN 117: 57 one-column TOC with numbers or dates not at line ends or starts, 51 with folio numbers in short lines of their own or absent, 5 very short pages, 3 register title pages, 1 empty side). `col_share` is 0.0 on nearly all of them (single column), `end_num` 0.0–0.4 and `date_start` 0.0–0.2, mostly below `shape_min` | negative-result | voc, index | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22c.3 | The lever that moves the number most is `shape_only_min` (shape-only rule, no column needed), and it buys recall with false positives: dev F1 0.000 (SG) to 0.300 (best); of 32 cells 0.25 with `gap` 2 wins. The `VOC_TOC_CUE` date pattern adds 3 TP and 0 FP on dev (73 vs 70 TP, same FP 297); `gap` 2 instead of 8 cuts dev FP from 453 to 297 and loses 7 TP; `text_min_chars` 3 raises recall (87 TP) at 455 FP | result | voc, index | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22c.4 | False positives of the selected cell are ledger and register pages, not TOCs: on test 333 FP, of which 105 account pages (Transport, guilder amounts), 154 account pages with a "Van <place> onder dato ..." heading (2448 alone 165, 8834 94) and 74 other; on dev 297 FP (144 / 34 / 119). Account pages end in numbers and start with dates, which is what the shape features look for | negative-result | voc, index | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22c.5 | 2775 (planner-seen, outside the test bar): unchanged cell 0/3 precision, 0/8 recall; selected cell P 1/19 = 0.053 (0.009–0.246), R 1/8 = 0.125 (0.022–0.471) | negative-result | voc, index | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22c.6 | `front_index_run` (SG defaults) returns a run on 0 of 22 volumes with unchanged flags, 4 with the selected flags (1267, 1274, 2555, 1539) and 8 with oracle flags (1267, 1274, 1388, 1557, 2548, 2555, 1539, 2448). Oracle `None` in 14 volumes: `min_flagged` (fewer than 10 flagged sides before the gap) in all 14, with `min_after` (fewer than 300 sides after the run) in a further 6 short volumes (7923, 8697, 8023, 8237, 8276, 8284: 148–220 sides); the first TOC side is never the cause. 22d cannot rely on the SG front-run rule for VOC | open-issue | voc, index, front-run | confirmed | [Step 22c](#step-22c-the-index-side-detector-on-voc-tables-of-contents) |
| F16-22d.1 | Bar E (VOC) fails for all three cells on the test volumes (oracle TOC sides, 2775 apart; target 0.90): U1 (L2, map v1) 142/411 = 0.345 (Wilson 0.301-0.393), U2 (L7, map v2) 90/163 = 0.552 (0.476-0.626), selected C3 (L8, V0/off/`page-number`) 279/433 = 0.644 (0.598-0.688). Chance at +3 leaves is 0.187, 0.252 and 0.238. Verdict `fails on VOC` | negative-result | voc, index-links, bar-E | revised | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.2 | The SG locators pick up the wrong things on VOC TOCs: L7 finds 163 locators in 9 test volumes (none in 2775) and covers 103/478 = 0.215 of the starts; L2 finds 411, of which 204 are <= 31 (day numbers; 43 hit) and 31 are year-like (0 hit), and 37 (9%) fall outside the map | result | voc, index-links, locators | revised | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.3 | L8 (leading or standalone folio) triples coverage: C3 275/478 = 0.575 of the starts after the front TOC (U2 0.215, U1 0.272) and E rises 9 points over U2, well above chance (0.238) but far below 0.90; dev selection C3 F1 0.511 against C2 0.495 and U1 0.228 | result | voc, index-links, locators | revised | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.4 | The 22b map cell is not the lever for links: on dev L8 with the SG map (C2) gives E 522/947 = 0.551, with SEL (C3) 689/1,275 = 0.540 (more links); L7 with SEL (C1) 0.310 against U2 0.326 | negative-result | voc, index-links, folio-map | revised | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.5 | Of 30 test non-hits of C3 read: 13 not a folio (years, day of a date), 4 range end taken, 10 map errors (>= 2 leaves off), 3 unclear; of 30 dev: 7 not a folio, 2 range end, 2 HTR, 9 start on the leaf's neighbour, 6 map error, 2 subdocument without gold start, 2 unclear. Locator errors are 11/30 dev and 17/30 test, map-like errors 15/30 and 10/30 | result | voc, index-links, error-analysis | confirmed | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.6 | L8 has fixable defects seen in the reading: range connector "à" and " / ", ". . ." (763 à 773, 122 a 125 after "/", 5038. . . 5107) fall through to L7 and give the range end; "verso" after the range end (656 a 668 verso, 749 a 749 verso: 5 of the 15 dev lines without locator) is not matched; years after "A„o" and day numbers in dates (tot 14, gedateert 21) are taken | open-issue | voc, index-links, locators | confirmed | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.7 | Silver (OBP `folio_start` sets, 9 test volumes): C3 precision 287/365 = 0.786, recall 287/458 = 0.627; U2 84/154 = 0.545 and 84/458 = 0.183; U1 0.434 and 0.258. 2775 with C3: 47/75 and 47/52 | result | voc, index-links, silver | confirmed | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22d.8 | 2775 (planner-seen): C3 41/143 = 0.287 (0.219-0.366), coverage 33/52; U1 0/50, U2 no locators. End to end, the shipped pipeline (L2, the layer's own flags) finds no front index in any of the 22 volumes and gives 0 links | negative-result | voc, index-links, end-to-end | revised | [Step 22d](#step-22d-index-links-on-voc-tables-of-contents) |
| F16-22e.1 | Of the 10 SG components compared, 1 carries over unchanged (the text-layer build: 567 inventories, `failures=0`, `n_fail_judged = 0`), 3 fail on VOC against their bar (folio map F 0.608-0.654 vs 0.95; index-side detector P 0.019-0.067, R 0.016-0.393; index links E 0.345-0.644 vs 0.90) and 6 run with no VOC bar (mode, sides, line types, word breaks, units, front-index run). None passes only after reparameterisation | result | voc, generalisation | revised | [Step 22e](#step-22e-generalisation-table) |
| F16-22e.2 | The three failures share one cause: SG conventions in how numbers and index pages look (folios on every other scan with tied numbers; one-column handwritten TOCs with numbers and dates away from line ends; locators taking day numbers and years). **Revised at review:** the geometry-based rules (build, side layout, mode cut) ran and contradicted no evidence, but there is no VOC gold for them, so "did not fail" means "not tested" (F16-22e.3) | result | voc, generalisation | revised | [Step 22e](#step-22e-generalisation-table) |
| F16-22e.3 | Five of the six components that run have no VOC gold (mode, sides, line types, word breaks, units; the GM `Enkel`/`Dubbel` label is not side gold), and the front-index run is reported only: "runs" there means no error, not correct | open-issue | voc, generalisation, gold | confirmed | [Step 22e](#step-22e-generalisation-table) |
| F16-R.1 | 22d's gold loader kept the last `toc_gold.tsv` row per scan; 698 scans have 2-4 rows and 691 disagree on `start`, so starts were lost (dev starts to cover 953 → 996, test 462 → 478). Fixed in 22r: C3 test E 274/433 → 279/433 = 0.644 (U1 142/411, U2 90/163), dev C3 E 671 → 689 of 1,275; selection (C3) and verdict unchanged; none of the 60 read non-hits flips | bug-fixed | voc, index-links, gold, review | confirmed | [Review](#review) |
| F16-R.2 | Every bar reproduces from the reviewer's own runs: the 22a audit (all 18,996 keys equal, `n_fail_judged` 0); 22b 550 / 523 / 591 of 904 with an independent scorer; 22c from the layer's own flags (TP 1 of 53 predicted / 61 gold; selected 24 / 357 / 61); 22d after 22r. SG defaults are unchanged against `HEAD` copies on 3444: inputs equal, `locate(n)` equal for n = 0-1,499 under V0 and V2, L1-L7 locators differ on 0 of 9,166 units; AS suite 1,413 passed, 11 skipped | result | voc, review | confirmed | [Review](#review) |
| F16-R.3 | The worst folio-map stratum is `Dubbel` volumes with portrait single-page scans: leaf 19/66 = 0.288 (selected cell, test) against 0.693 for `Dubbel` openings and 0.676 for `Enkel` single pages. GM gives 1.01 scans per folio there (71 spans), so the numbers are page numbers; the misses read are restarts (7529: a second numbering from scan about 1216) and volume-start extrapolation (1122, 1132, 1245). The map does not use this numbering-mode signal | data-property | voc, folio-map, review | confirmed | [Review](#review) |
| F16-R.4 | GM folio gold carries about 4-5% errors (9 of 70 read misses, F16-22b.5, and 3 of the 6 decreasing parts), close to the 5% slack of a 0.95 bar: a future VOC folio bar needs a checked subset (UA-059 is a start) | method | voc, gold-quality, review | confirmed | [Review](#review) |
| F16-R.5 | Planning defects (the planner's): the all-single-page premise came from 2 smoke volumes (146 of 567 are openings; the 22b brief was corrected before it ran); `single_foliation` cannot move a scan-level leaf by construction; the restart criterion flags 509 of 545 volumes; 5 of the 11 decreasing parts are not reproducible; the gold row grain was not stated (F16-R.1). `plan/templates/STEP.md` now asks for the row grain and a planning run of every new option or criterion | method | planning, review | confirmed | [Review](#review) |
| F16-R.6 | Leakage checks pass: gold hashes equal planning (`a2b875d9…`, `095e7f31…`); `select.json` precedes `test_done.json` in 22b, 22c, 22d and 22r; no library code reads gold or silver; 2775 is outside every test bar; `VOC_TOC_CUE` and L8 were written from dev TOC text only. Row 10 of the table (`front_index_run`) is `runs, no bar` under P11 but finds a run in 0 of 22 volumes: in practice it does not carry over | result | voc, leakage, review | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

*Opus 5.5, 2026-10-31. Scripts and outputs: `reports/data/phase-16-voc-generalisation/planning/`.*

**Gold and splits.**

- **GM anchors.** `gm_split_and_proto.py` writes `gm_anchors.tsv` (sha256 `a2b875d9b06e9972…`): one row per
  distinct (inventory, folio, scan) from each missive's begin and end. 1171 is excluded, because the gold names
  `1171_*` scans and the drive has only `1171A`/`1171B`.
  - Split by inventory, stratified by scan layout, seed 20261031.
  - dev: 937 anchors in 272 inventories; test: 904 in 273.
- **TOC gold.** `toc_split_and_peek.py` writes `toc_gold.tsv` (sha256 `095e7f3198ac3542…`): one row per scan of the
  22 GLOBALISE inventories, with `toc` and `start`. dev 11 volumes / 117 TOC scans; test 11 / 69.
  - 2775 (test) is planner-seen and stays out of the test bars.

**Dev-only prototype** (`gm_proto_dev.tsv`): F16-P.1, F16-P.2. **Smoke run** of the 22a preset: F16-P.4.

**Decisions.** See the phase plan, P1–P13. The VOC bars keep the SG thresholds:
- F ≥ 0.95 (leaf);
- D: scan P ≥ 0.95, R ≥ 0.90;
- E ≥ 0.90 (leaf holds a GLOBALISE start).

### Step 22a: The VOC text layer with the SG v5 rules unchanged

**What changed.** New preset `voc_1.04.02_v5sg` (`VOC_1_04_02_V5SG`, `VOC_SOURCE_PATTERN`) in
`archival_structures/text_layer/presets.py`: SG v5 with the archive plumbing changed and the inventory-keyed settings
dropped (`wbc_pinned` inventory keys, `volume_mode["pins"]`; `join_lead_marks_lower` was already `None`). Additive;
changelog entry *Step 22a* under *Unreleased*; `docs/text_layer.md` paragraph. Tests: `tests/test_text_layer_voc_preset.py`
(5 tests). Layer built over the drive PageXML with `--stage all`, 2 workers (coreshare granted 2: the machine load was
27–42 on 10 cores for about 1.5 h, so the job queued; the build itself took 1 h 51 min), root
`data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/` (link to the drive, 6.0 GB). Scripts and tables:
`reports/data/phase-16-voc-generalisation/step-22a/`. Brief: `plan/phase-16-voc-generalisation/step-22a-voc-text-layer.md`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | new + full suite green | 5 new passed; full suite 1,407 passed, 11 skipped (1,402 before) |
| 2. Coverage | all inventories built, `failures=0` | **567** built, not 568 (see Brief defects: 545 GM + 22 GLOBALISE); `failures=0`; 567 `mode/*.json`, 567 `join/units/*.tsv.gz` |
| 3. Audit unchanged | `n_fail_judged == 0` | `n_fail_judged = 0`; 10,069 pass, 148 fail, all 148 *reported*, all one check (below) |
| 4. Six tables | written and summarised | `modes`, `sides`, `types`, `laypa_pn`, `breaks`, `index_flags` `.tsv` + `summary.json` |
| 5. Protect diff | explained | `protect_before.txt` vs `protect_after.txt` identical (180,292 lines; no change in shared-data, LotA/NLP data or `text_layer_v2_dev`) |

**Audit.** The one failing check is "merges with lower-case right side (share, target >= 0.95, reported)": 148 of 567
volumes fall below 0.95 (range 0.8404 for 7534, 426 merges, to 0.9496 for 3947; most are 0.91–0.95). It is not
judged for SG either. Classification: an SG assumption (the join assumes that the line after a break mark starts
lower-case; the VOC HTR starts it with `„` (below), and capitals occur inside words). No preset bug, so no rebuild.

**Measurements.**

- **Mode (`modes.tsv`).** All 567 volumes are `hand`; there are no `print` volumes. Box height (px): GM (545) min 109,
  quartiles 160 / 177.5 / 194.75, max 273.5; GLOBALISE (22) min 131, quartiles 172.4 / 181 / 197.5, max 231. The SG
  cut is 87.75, so no VOC volume is within 20 px of it. SG hand volumes (125 in the v5 layer) have 77 / 137 / 182
  (min / median / max): VOC scans are larger than SG ones, but the cut is far below both. `scan_minority_share` > 0 in
  235 volumes (max 0.123): mixed scan sizes inside volumes exist but never flip a mode. 2775: 172, 8023: 212, as in
  the smoke run.
- **Sides (`sides.tsv`).** Against the GM `layout` label, 483 of 545 volumes agree (88.6%): `Enkel` 340 of 343
  (99.1%), `Dubbel` 143 of 202 (70.8%). The 62 disagreeing volumes: 57 `Dubbel` volumes that are ≥ 90% `single`, 2
  `Dubbel` mixed (7529: 1,368 single, 3 verso, 210 recto; 1432), 3 `Enkel` volumes that are ≥ 90% `verso`+`recto`
  (1603, 2770, 3366).
- **Page numbers (`types.tsv`).** Pooled `page_number` segments 342,451 (1.94% of segments; SG hand 1.15%).
  Methods: `pn_line` 184,162 (53.8%), `pn_line_offchain` 143,812 (42.0%), `pn_line_gap` 13,209 (3.9%), `pn_suffix` 1,268
  (0.4%). SG hand reference (125 volumes): 36,584 / 9,648 (17.9%) / 1,085 / 6,627 (12.3%). Other types, share of
  segments VOC vs SG: `main_text` 76.7 vs 88.6%, `marginalia` 16.3 vs 8.7%, `catch_word` 2.1 vs 0.1%, `other` 1.8 vs
  0.1%, `index` 0.9 vs 0.4%.
- **Laypa agreement (`laypa_pn.tsv`).** Line has a `page_number` segment vs line is in a Laypa `page-number` region:
  TP 222,378, FP 119,702, FN 47,323: P 0.650, R 0.825 (agreement, not gold).
- **Breaks (`breaks.tsv`).** Of 17,614,738 non-empty lines, line-final: `„` 1,424,959 (8.1%), `:` 360,492, `-`
  290,121, `¬` 116,871 (0.66%), `=` 41,267. In the join output, 116,881 `¬` are still followed by a space or the
  unit end: **6.64 per 1,000 lines, i.e. `¬` is never joined** (the corpus default set is `-=„` and the pinned default
  `-=„` takes precedence over the pooled rule, which wanted `¬`+`„`).
- **Index flags (`index_flags.tsv`).** Scan level, 22 GLOBALISE volumes: gold TOC scans 186; flagged sides (`index_side
  == "1"`) 121 + 1 = 122; **TP 1, FP 121, FN 185** (the single TP is in 8284). The flags fire in 17 of the 22
  volumes (most in 8834: 21, 1547: 19, 1557: 19, 2448: 15); none in 1388, 2555, 3142, 8023, 8276, 8697. No conclusion
  here (22c owns the detector).

**Error analysis.** Scripts: `analyse.py` (seed 20261031), output `step-22a/samples.txt`.

- *Print volumes:* 0 exist; nothing to read. The question "is any VOC volume printed" is answered by the line texts
  in the samples below: all handwritten-HTR text (`Coffij`, `VEd=le Achtbaerheden`).
- *Sides (the 10 first disagreeing volumes read, scan sizes of 3 scans each):*

  | Category | Count | Example |
  |---|---|---|
  | `Dubbel` label, portrait scans, one page per scan | 8 | 1122: 3 scans 2925x4167, 3176x4270, 3260x4216 (aspect 0.70–0.76) |
  | `Enkel` label, landscape openings photographed as one scan | 2 | 1603: 5811x4467, 5875x4467, 6048x4644 (aspect 1.3); 2770: 7725x5442 |

  Later disagreeing `Dubbel` volumes (7528: median aspect 0.76; 7545: 0.73; 1241: 0.70) are portrait too. The agreeing
  `Dubbel` volumes are landscape. So `Dubbel` in the GM overview does not imply a two-page scan; the SG side rule,
  which reads the scan geometry, is right for these volumes and the label is the unreliable one. 2 of 3 `Enkel`
  exceptions are landscape openings the SG rule splits into verso and recto.
- *Page numbers (30 `page_number` segments, over all volumes):* 27 folio numbers (e.g. `1228.`, `870`, `31`, `(27`
  with a stray bracket), 2 numbers in another role (`§ 102.`, `§60`: paragraph numbers), 1 uncertain (`5476`), 0 noise.
  Of the 15 `pn_line_offchain` segments in the sample, 12 are folio numbers, 2 paragraph numbers and 1 uncertain, so the
  dominant offchain method marks real folio numbers in about 80% of this small sample.
  Laypa `page-number` lines with no segment (20): 9 folio numbers the rules missed (`488`, `70`, `393`, `4026.`,
  `1109a.`, `132 X`, `- - . „ 100.`, `1297`, `1328`), 5 years (`1756` twice, `1769`, `1773`, `1660`), 1 register
  number (`No. 8.`), 5 noise/empty (``, `.`, `...dat`, `f`, `voorschoten`). So about half the Laypa-only lines are
  correctly left out (years, noise) and about half are missed folio numbers.
- *Units (10 GM sides with >= 10 lines; read from heads of `units/units` and `join/units`, 110 characters each):*

  | Category | Count | Example |
  |---|---|---|
  | reading order right | 6 | 2150/0134: one 23-line body, one marginalia, one catch-word |
  | interleaved or out of order | 3 | 1950/0333: 16-line body at y=487 listed after bodies at y=1377 and y=2430, and the page number `671` appears twice; 1119/2232: a 29-line unit from y=261 overlaps a unit at y=1599; 1188/1193: marginalia scraps `ant`, `rable`, `lieven` are line ends of the body |
  | cut mid-sentence | 1 | 2615/0620: body split in two units and a 3-line piece of it (`dit Iaar importeeren ruim 60 tonnen`) typed as marginalia |

  Causes in VOC terms: marginalia (typed `marginalia` in 16% of segments, many single words at line ends) and
  catch-words (`naar`, `bij`, `komt`) become units of their own placed by type rather than by position; wide body
  paragraphs and letters with an inset salutation are separate regions. This is a small judged-by-head sample;
  it says the SG unit rules run without errors, not that the VOC order is right.
- *Word breaks (10 of 11,115 `¬`-ending lines in 60 random volumes):* `¬` kept in the joined text in 10 of 10
  (`Conver¬ den`, `proce¬ „deeren`, `repatri¬ „eerende`). In 4 of 10 the next line starts with `„` (`„deeren`, `„eerende`,
  `„gen`, `„`): the HTR reads the repeated break mark at the start of the next line as `„`; SG's `join_lead_marks` is
  `","`, so the mark is not dropped either. The lines are not joinable by the present rules.
- *Conclusion:* the SG v5 stages run on VOC without a failure and pass the judged audit; what they get wrong is
  visible in the marks and sides, not in crashes.

**Findings:** F16-22a.1 to F16-22a.7.

**Brief defects.**

- The brief says 546 GM inventories and a total of 568. `gm_split.tsv` has 547 lines including the header, so 546 GM
  rows; minus `1171` that is **545**, and the total is **567** (545 + 22). Everything above is 567.
- `coreshare.py` granted 2 workers, not 6, and nothing in the brief says the build cannot be resized after a start. The
  `--expected-min 90` estimate was exceeded (111 min).
- `$TMPDIR` is unset in the agent shell, so the brief's log-redirect needed a scratchpad path.

**Open issues.** None blocking. The `¬`/`„` break handling (F16-22a.5) and the Dubbel label (F16-22a.3) are inputs
for the later steps and the review; no rule was changed here.

### Step 22b: The folio map on VOC

**What changed.** Two opt-in parameters, defaults unchanged: `FolioMapParams.single_foliation` / `single_slope` (S: a
volume whose numbered sides are all `single` scans and whose chain rises at least 1.5 scans per number is `foliation`;
the back of a target may then be the next `single` side) and `read_volume_inputs(..., number_tags=())` (N: also read the
text of lines in regions with a given `regions_meta` tag, here Laypa `page-number`). 4 new tests (3 in
`test_analysis_folio_map.py`, 1 in `test_analysis_index_links.py`); `read_volume_inputs` on SG inv 3444 is an equal object
before and after. Paragraph in `docs/index_links.md`, changelog entry *Step 22b*. Scripts and tables:
`reports/data/phase-16-voc-generalisation/step-22b/` (`eval_gm.py` with stages `dev`, `test`, `archive`, `context`;
`classify_misses.py`; `build_check_page.py`). Single process (no coreshare). Brief:
`plan/phase-16-voc-generalisation/step-22b-folio-map-voc.md`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests, SG object | green; object unchanged | 4 new passed; full suite 1,411 passed, 11 skipped; SG 3444 equal |
| 2. Dev grid | 8 rows, 937 anchors minus unreadable; `select.json` before `test_done.json` | 8 rows over 929 readable anchors (8 gold scans not in the scans table); `select.json` 2026-11-01 before `test_done.json` |
| 3. Bar F (VOC) | test leaf share >= 0.95, 3 cells | **fails in all three** (table below); verdict **fails on VOC** |
| 4. Archive questions | rectos, restarts, LOO, 11 parts | below |

**Dev grid** (929 readable dev anchors; leaf share, strict share in brackets; full table `dev_grid.tsv`):

| M | S | N | leaf | strict | Enkel leaf | Dubbel leaf | openings leaf | single leaf |
|---|---|---|---|---|---|---|---|---|
| V0 | off | T | 0.657 (0.534) | | 0.672 | 0.627 | 0.681 | 0.648 |
| **V0** | **off** | **TL** | **0.696 (0.568)** | | 0.721 | 0.646 | 0.707 | 0.693 |
| V0 | on | T | 0.646 (0.514) | | 0.655 | 0.627 | 0.681 | 0.634 |
| V0 | on | TL | 0.687 (0.551) | | 0.707 | 0.646 | 0.707 | 0.680 |
| V2 | off | T | 0.623 (0.505) | | 0.647 | 0.575 | 0.616 | 0.626 |
| V2 | off | TL | 0.659 (0.536) | | 0.686 | 0.604 | 0.655 | 0.660 |
| V2 | on | T | 0.609 (0.486) | | 0.626 | 0.575 | 0.616 | 0.607 |
| V2 | on | TL | 0.646 (0.519) | | 0.667 | 0.604 | 0.655 | 0.643 |

Selected: **(V0, off, TL)**, the unique maximum (no tie within 0.002). `select.json` carries the cell and the dev numbers.

**Bar F on test** (904 anchors in 273 volumes, 0 unreadable; Wilson 95%; the three cells, one run):

| Cell | Leaf | Wilson | Strict | Enkel leaf | Dubbel leaf | openings leaf | single leaf |
|---|---|---|---|---|---|---|---|
| (V0, off, T), unchanged map v1 | 550/904 = 0.608 | 0.576-0.640 | 452 = 0.500 | 381/594 = 0.641 | 169/310 = 0.545 | 154/248 = 0.621 | 396/656 = 0.604 |
| (V2, off, T), unchanged map v2 | 523/904 = 0.579 | 0.546-0.610 | 438 = 0.485 | 361/594 = 0.608 | 162/310 = 0.523 | 149/248 = 0.601 | 374/656 = 0.570 |
| **(V0, off, TL), selected** | **591/904 = 0.654** | **0.622-0.684** | 484 = 0.535 | 403/594 = 0.678 | 188/310 = 0.606 | 173/248 = 0.698 | 418/656 = 0.637 |

Per `method`, selected cell (leaf): printed 463/597 = 0.776 (0.740-0.807); interpolated 111/257 = 0.432; extrapolated 17/37 =
0.459; none 0/13. Unchanged v2: printed 378/460 = 0.822, interpolated 116/244, none 0/139 (65 volumes; in 60 of them G1 cut the
chain at a jump and the folio lies outside the kept piece); unchanged v1: printed 431/561 = 0.768, interpolated 102/292, none 0/14. Per-layout Wilson intervals are
in `test_done.json`. The side layout is `openings` if verso+recto >= 0.5 of the sides in 22a's `sides.tsv` (146 volumes
with anchors) else `single` (399); the GM label is not a scan layout.

**Archive questions** (545 volumes; `volumes.tsv`, `loo.tsv`, `restarts.tsv`, `decreasing_parts.tsv`):

- **Rectos.** 146 `openings` volumes: `recto_share` from (V2, off, T) has quantiles 5/25/50/75/95 = 0.72/0.84/0.90/0.94/0.97;
  74 reach 0.9 (mode `foliation`), 72 do not (`pagination`). 397 `single` volumes: of the consecutive chain points one number
  apart, 89,339/99,093 = 0.902 are exactly 2 scans apart (341 volumes at >= 0.5): numbers sit on every other single-page
  scan, i.e. foliation on single-page scans. The SG rule (share of rectos) cannot see it, S can, but gains nothing (below).
- **Restarts.** The brief's criterion (an extra run of >= 10 anchors after `main_chain`) flags **509 of 545 volumes**
  (1,553 runs): it measures misread noise, not restarts (runs span the whole volume at density 0.03). A post-hoc
  compactness filter (0.5 <= scans/number <= 3 and >= 0.2 anchors per scan, `coherent` column) leaves 6 volumes:
  1085, 1542, 1718, 1816, 2004, 2061 (runs of 121-278 anchors). Not read further.
- **LOO** over the chain of the selected cell: dev volumes 87,262/98,750 = **0.8837** (Wilson 0.882-0.886, 272 volumes);
  all volumes 169,641/193,305 = 0.8776. SG: 0.9858.
- **The 11 decreasing-folio parts** (`decreasing_parts.tsv`): only 6 show a decrease in the frozen gold (the other 5 have none
  under any ordering I tried: by scan, by missive id, end < begin; the planning definition could not be recovered).
  Gold error 4, of which 1363 has no decrease (1269: 185 begin 764 at scan 1542, print 718 there; 1363: folio 44 at 132, print puts it at 130; 2550: 565 end 757
  at 798, print 773; 2808: 648 end 714 at 456, print 733); nested or duplicate registration 2 (1696: 358 folios 1-22 overlap
  357's numbering from 16, and 356's 403-409 sit at 410-416 while print puts 396-399 at 445-448; 2394: 577 end 1101 and 542
  end 1345 both at scan 624, print 1344 on 623); numbering restart 1 (1341: 234 begins at folio 1 on scan 855 after 687 on
  716, no number printed there); unclear 4 (1279, 1408, 1614, 3987: no decrease found).

**Error analysis.** 40 dev misses of the selected cell (seed 20261031) and 30 test misses (seeded sample of the 313; the
brief says "every miss up to 30", the first 30 in file order would have been 5 volumes) were read with the page-number texts
(segment and Laypa) on the gold scan +-3 and around the predicted scan (`dev_misses_context.txt`, `test_misses_context.txt`;
classes and notes in `misses_read.tsv`).

| Category | dev (40) | test (30) | Example |
|---|---|---|---|
| tied or duplicated number | 10 | 6 | 2970: `76` printed on scans 87 and 88; 2552: `1275` on 20 and 21; 2361: `1037` repeated on 75-79 |
| restart or second numbering | 5 | 5 | 1603: early series 1169-1193 on scans 13-19, chain starts at 240; 2612: `47 48 49` at 211-215 and 63-65 |
| no number near the gold (gap) | 5 | 5 | 1066: `3` on 23, `4` expected on 25 is blank |
| gold error | 3 | 6 | 1917: gold scan prints `1531`, gold says 1530; 2018: gold scan prints `94`, gold says 95 |
| several numbers on one side (first match wins) | 3 | 4 | 3826: gold scan `0`, `3679`, `Seert`; the map takes `0` |
| side/layout offset | 5 | 2 | 1936: `2585` on 633, `2586` on 635, gold 634 (the scan before) |
| extrapolation beyond the chain | 3 | 1 | folio 1 with no number before the chain |
| HTR misread | 3 | 1 | 3559: `1302` for 1102 on the gold scan |
| unclear | 3 | 0 | |
| front matter wrongly excluded | 0 | 0 | |

Reading the 70: a printed number is the right place only 78% of the time (463/597 on test); the other 22% of
`printed` answers are tied numbers (the same number on two or three consecutive scans, so the chain keeps one), a
repeated page header taken for a folio (`1037`, `1382`, `1465`), or a gold scan that the print contradicts. Misses
where the map says `printed` at +1 or +2 scans from gold (53 of the 134 `printed` misses) mostly carry the number
on the next recto: the gold scan is the unnumbered scan before. 9 of the 70 read misses (13%) are gold errors, so the
gold errors are about 13% of the 35% misses, i.e. roughly 4-5% of all anchors (a 70-case estimate, wide interval), the same size as the 5% slack of the bar. Because the second text of a side is often the true number
(`2, 61, 1, 3`; `389, 8, 387`), taking the first matching text per side loses anchors, and N adds more texts per
side. N still helps on the whole: +4.6 points on test, because Laypa `page-number` regions recover numbers the segment
typing missed (22a: 42% of segments are `pn_line_offchain`).

**User check.** 9 misses read as gold errors (>= 5): `check_page.html` (UA-059, blocks nothing) asks which scan carries folio N.

**Brief defects.** (1) The brief's restart criterion flags 509 of 545 volumes; it cannot find restarts. (2) Five of the
"11 decreasing" parts have no decrease in the frozen gold; the definition is in no file. (3) `S` changes only the mode and the
`back` of a target; leaf, `front` and the scan chain are identical to pagination on `single` scans, so S cannot move the
leaf bar by design (its only effect is the scan-vs-side position, 0 vs -1 points). (4) "Every test miss, up to 30" does not say which 30.
(5) `volumes.tsv` of the brief is not defined; I wrote one row per inventory with side layout, anchors, modes and chain slope.

**Findings (draft).** F16-22b.1 to F16-22b.8 in the table above.

### Step 22c: The index-side detector on VOC tables of contents

**What changed.** No library code. Scripts and tables in `reports/data/phase-16-voc-generalisation/step-22c/`
(`eval_toc.py` with stages `replica`, `dev`, `test`, `cases`; `cases_label.py`). The SG v5 index-side detector was
scored at scan level (a scan is *index* if any side is flagged) against the 186 gold TOC scans of the 22 GLOBALISE
inventories, with the frozen dev/test split; 2775 is reported apart from the test bar (10 volumes, 61 TOC scans).

**Replica (criterion 1).** `layout._write_pages_table` with `SG_INDEX_SIDES_V5` reproduces `rule` and `index_side` of
the layer's `layout/pages` row for row in all 22 volumes (`replica_check.tsv`: 0 differences; the in-script flag path
also matches). All 22 volumes are `hand`, so none counts as missed for lack of `layout/pages`.

**Grid and selection (criterion 2).** 32 cells on the 11 dev volumes (117 TOC scans), `dev_grid.tsv`. Unchanged SG cell:
TP 0, FP 68, FN 117. Best: `tmc8_som0.25_cue_gap2` (`text_min_chars` 8, `shape_only_min` 0.25, `VOC_TOC_CUE`, `gap` 2):
TP 73, FP 297, FN 44, P 0.197, R 0.624, F1 0.300; no other cell within 0.005. `select.json` was written
2026-11-01T02:22:56, `test_done.json` after it (02:23:03); the test split was run once.

| Dev cell | TP | FP | FN | P | R | F1 |
|---|---|---|---|---|---|---|
| unchanged (SG) | 0 | 68 | 117 | 0.000 | 0.000 | 0.000 |
| `tmc8_som0.25_sg_gap2` (no cue) | 70 | 297 | 47 | 0.191 | 0.598 | 0.289 |
| **selected** `tmc8_som0.25_cue_gap2` | 73 | 297 | 44 | 0.197 | 0.624 | 0.300 |
| `tmc8_som0.15_cue_gap2` | 96 | 542 | 21 | 0.151 | 0.821 | 0.254 |
| `tmc3_som0.25_cue_gap2` | 87 | 455 | 30 | 0.161 | 0.744 | 0.264 |

**Bar D on VOC (criterion 3), 10 test volumes, scan level** (`test_done.json`, `scan_eval.tsv`):

| Cell | TP | FP | FN | P (Wilson 95%) | R (Wilson 95%) | Bar D |
|---|---|---|---|---|---|---|
| unchanged SG | 1 | 52 | 60 | 1/53 = 0.019 (0.003–0.099) | 1/61 = 0.016 (0.003–0.087) | fail |
| selected | 24 | 333 | 37 | 24/357 = 0.067 (0.046–0.098) | 24/61 = 0.393 (0.281–0.519) | fail |
| 2775, unchanged (outside bar) | 0 | 3 | 8 | 0/3 | 0/8 | n/a |
| 2775, selected (outside bar) | 1 | 18 | 7 | 1/19 = 0.053 (0.009–0.246) | 1/8 = 0.125 (0.022–0.471) | n/a |

Bar: P >= 0.95 and R >= 0.90. **Verdict: `fails on VOC`.** Per volume (selected, TP/FP/FN): 1539 15/21/1, 1120 3/13/0,
8834 3/94/1, 2448 0/165/21, 1529 1/19/5, 3891 0/1/4. No `VOC_INDEX_SIDES_V1` or `VOC_TOC_CUE` was added to `presets.py`
(condition of the brief not met); no changelog entry, no new test.

**Front runs (criterion 4), reported only** (`front_runs.tsv`, `front_index_run` SG defaults, 22 volumes): unchanged 0
runs; selected 4 (1267, 1274, 2555, 1539); oracle 8 (1267, 1274, 1388, 1557, 2548, 2555, 1539, 2448). Oracle `None`
reasons: `min_flagged` in all 14 (fewer than 10 oracle sides before the 20-side gap: 0–8 counted; GLOBALISE TOCs have 1–40 scans, most of them short), with
`min_after` as well in 6 short volumes (7923, 8697, 8023, 8237, 8276, 8284: 148–220 sides in total). `max_first`
and `max_share` never fire, and a 20-side gap exists everywhere. Step 22d uses oracle TOC sides, so it is not affected.

**Error analysis.** Read: the dev FN of the selected cell (30 of 44, seed 20261031), dev FP (30 of 297), the unchanged
cell's dev FN (15 of 117), test FN (30 of 37) and test FP (30 of 333); 5 lines of text and the features each
(`cases_read.tsv`, 135 rows). The categories were fixed after reading the samples and then applied by rule to every
FN/FP (`categories.json`, `cases_label.py`); the TSV categories are rule labels, checked against the text read.

| FN category (selected cell) | dev (44) | test (37) | Example |
|---|---|---|---|
| one-column TOC, numbers/dates not at line ends or starts | 14 | 16 | 1388_0024 "Copie ola van belofte door den calicoulans ..." (`end_num` 0.06) |
| folio numbers in own short lines, or column absent | 21 | 18 | 2548 "Javas Oost Cust / A„o 1741 / den 25:' Maart / P„r 't schip ..." (per-ship lists); 2448 "Pag: / Persia / A„o 1738" |
| very short page (<= 5 lines) | 5 | 0 | 1547_0014 (5 lines) |
| title/heading page of the register | 3 | 2 | 1557_0009 "Register der brieven"; 1529_0007 |
| layout lost the side (no lines) | 1 | 1 | 2555_0616, 2448_0008 |

Unchanged cell, dev FN (117): 57 one-column, 51 numbers apart, 5 short, 3 title, 1 empty. The TOCs are one column
(`col_share` 0.0 or None) and `end_num` is 0.0–0.4, so the column route never fires and the shape-only route
needs `shape_only_min` 0.25 or lower. Volumes 2548 and 2448 have TOC pages that list ships and dates with the folio
column in separate or missing lines; no line-end rule can see those.

| FP category (selected cell) | dev (297) | test (333) | Example |
|---|---|---|---|
| account/ledger page (Transport, guilder amounts) | 144 | 105 | 1274_0635 "„ 28249:11: 9. / ƒ592559:17:5 / p:r transport ..."; 1547_0676 |
| account page with a "Van <place> onder dato ..." heading | 34 | 154 | 2448_0593 "Van Persi onder dato 31 xber 1737"; 8834_0367 "Van Cormandel onder dato 3:' Meij" |
| other (letter text, roster, number column) | 119 | 74 | 1267_0629 (letter prose, page number "310"); 1274_1764 (roster "folio der boecken") |

None of the 60 FP read was a TOC page the gold missed. Account pages end in numbers and often begin with a date,
exactly what `end_num` and `date_start` reward, and `gap` 2 plus the 0.25 shape-only threshold flags them by the
hundred (2448: 165 FP, 8834: 94 FP). The `VOC_TOC_CUE` pattern was written on TOC text and matches ledger text equally.
The only precise cell by volume is 1539 (TP 15 of 16 TOC scans) where the TOC does have entries ending in folio ranges.

**Conclusion.** The SG index-side detector does not generalise to VOC TOCs: its columnar-with-numbers-or-dates model
does not describe a one-column register with folio numbers set apart, and loosening it to find the TOCs floods the
output with account pages. A VOC TOC detector would need other evidence (position in the volume, "Register" title,
a run of short pages after the first scans, a count of folio numbers on the side); that is a new avenue, not a
retuning, and was not tried here.

**Findings:** F16-22c.1 to F16-22c.6.

**Deviations from the brief.** None. Brief defects: none blocking. The SG default `min_after` 300 cannot be met in
volumes of 148–220 sides, which the brief's front-run section did not anticipate.

**Open issues.** None blocking. 22d proceeds with oracle TOC sides as planned.

### Step 22d: Index links on VOC tables of contents

**What changed.** Library: `LocatorParams` gains `lead_pattern` and `standalone_pattern`; locator variant `L8` and
`VOC_LOCATOR_PARAMS` in `archival_structures/analysis/index_links.py` (additive; two tests; L1-L7 untouched: for SG inv 3444
the links of `volume_links` with L1-L7 and of `volume_links_v2` hash identically before and after, sha256 `30c9319dc2142311...`,
5,035,724 bytes of JSON). Scripts and tables in `reports/data/phase-16-voc-generalisation/step-22d/` (`eval_links.py` with
stages `dev`, `test`, `errors`; `label_cases.py`). Entries are read from the gold TOC sides (oracle, as 22c failed): `main_text`
units for the SG locators, every line of the sides for L8. A link is right when the leaf (the scan the map gives and the next
one) holds a gold start. Dev 11 volumes (193-1,275 links per cell), test 9 volumes (8023 has no locators; 2775 apart).

**Cells and selection (criterion 2).** Five cells on dev (`dev_grid.tsv`): U1 (L2, V0, T), U2 (L7, V2, T), C1 (L7, SEL),
C2 (L8 lines, V2, T), C3 (L8 lines, SEL = V0, off, `page-number`). SEL differs from U2, so no cell was dropped.

| Cell | links | E (hits/links) | located | coverage | chance (+3) | F1 |
|---|---|---|---|---|---|---|
| U2 | 193 | 63/193 = 0.326 | 1.000 | 58/996 = 0.058 | 0.301 | 0.099 |
| U1 | 997 | 244/997 = 0.245 | 0.763 | 212/996 = 0.213 | 0.198 | 0.228 |
| C1 | 255 | 79/255 = 0.310 | 1.000 | 68/996 = 0.068 | 0.263 | 0.112 |
| C2 | 947 | 522/947 = 0.551 | 0.998 | 447/996 = 0.449 | 0.305 | 0.495 |
| **C3** | 1,275 | 689/1,275 = 0.540 | 0.998 | 482/996 = 0.484 | 0.267 | **0.511** |

Selected C3 (F1 0.5106, next C2 0.4948, outside the 0.005 tie band); `select.json` is time-stamped before `test_done.json`.

**Bar E on test (criterion 3; once; 2775 apart).** Target E >= 0.90.

| Cell | E | Wilson 95% | located | coverage | chance (+3) | bar |
|---|---|---|---|---|---|---|
| U1 | 142/411 = 0.345 | 0.301-0.393 | 374/411 = 0.910 | 130/478 = 0.272 | 77/411 = 0.187 | fails |
| U2 | 90/163 = 0.552 | 0.476-0.626 | 163/163 | 103/478 = 0.215 | 41/163 = 0.252 | fails |
| C3 (selected) | 279/433 = 0.644 | 0.598-0.688 | 433/433 | 275/478 = 0.575 | 103/433 = 0.238 | fails |

Verdict **`fails on VOC`**. 2775 (planner-seen): U1 0/50 (0 of 52 starts covered), U2 no locators, C3 41/143 = 0.287
(0.219-0.366), coverage 33/52, chance 22/143. Per volume (C3 hits/links): 1120 42/70, 1529 19/35, 1539 114/125, 2448 40/98,
3891 5/19, 8237 14/18, 8276 12/17, 8284 9/19, 8834 24/32; 1539 carries a quarter of the links.

**Review fix (22r).** `toc_gold.tsv` has one row per GLOBALISE CSV row, not per scan: 698 scans have two or more rows and
691 of them disagree on `start`. `load_gold()` in `eval_links.py` kept the last row of a scan, so a start row followed by a
non-start row lost its start; this deflated E, coverage and chance in every cell, on dev and test. The scorer now takes the
max over a scan's rows (the selection rule and cells are unchanged; the old outputs are in `step-22d/superseded_22r/`).
C3 test E moved from 274/433 to 279/433 (0.633 to 0.644), dev C3 E from 671/1,275 to 689/1,275. Selection (C3) and verdict
(`fails on VOC`) are unchanged; silver is byte-identical; the end-to-end result is unchanged. The numbers in this section
are those of the rerun.

**Silver (OBP `folio_start` set per volume, test, 9 volumes with locators; `silver.tsv`).** Pooled over volumes: C3 precision
287/365 = 0.786, recall 287/458 = 0.627; U2 84/154 = 0.545 and 84/458 = 0.183; U1 118/272 = 0.434 and 118/458 = 0.258. 2775: C3
47/75 and 47/52 = 0.904; U2 has no locators. The silver has 4-375 rows per volume; precision here is against a complete-ish
set, so it is a looser check than E.

**End to end (criterion 4; `end_to_end.tsv`).** The shipped pipeline (`volume_links` with L2 and the layer's own flags) finds
no front index in any of the 22 volumes and so gives 0 links (consistent with F16-22a.6 and F16-22c.6).

**Error analysis.** Read (before the 22r fix, so drawn from 604 dev and 159 test non-hits; under the fixed gold they are 586 and 154): 30 dev non-hits of C3 (seed 20261031), 30 test non-hits (seed 20261031) and
15 of 207 dev lines without an L8 locator but with an OBP folio in the text (`errors_dump.txt`, `cases_read.tsv`). The
sample, not the full set, is classified; categories are my reading of the line and the offset to the nearest gold start.

| Category | dev 30 | test 30 |
|---|---|---|
| not a folio (year, day of a date) | 7 | 13 |
| range end taken as the locator (locator error) | 2 | 4 |
| HTR misread | 2 | 0 |
| right folio, start on the leaf's neighbour (offset +-1) | 9 | 0 |
| right-looking folio, wrong scan (map error, offset >= 2) | 6 | 10 |
| subdocument the gold has no start for | 2 | 0 |
| unclear | 2 | 3 |

Under the fixed gold none of the 60 read non-hits becomes a hit (`cases_flipped_22r.tsv` is empty), so every count above
stands. The 5 test hits and 18 dev hits gained by the fix are among the non-hits that were not read.

- Locator errors (not a folio + range end + HTR) are 11/30 dev and 17/30 test; map-like errors 15/30 and 10/30. Examples
  of not-a-folio: "dato 14 Nov. 1670." (1274, the year), "tot 14" (2448), "A„o 1738" (2448, the year; 4 of the 30 test
  rows are this heading or its neighbours). Example of the map: 2448 "774" located at scan 794, 205 leaves from the nearest
  start (interpolated on a noisy chain; F16-22b.8).
- Range-end errors come from connectors L8's lead pattern does not know: "763 à 773." and "780à 782." (1120, `à`),
  "122 a 125." after " / " (1539) and "5038. . . 5107." (3891, dots as connector): L8 falls to L7, which takes the last number.
- The 15 lines without locator: 5 are real misses, all in 1557, from "verso" after the range end ("„ 656 a 668 verso",
  "„ 749 a 749 verso"; the lead pattern needs `\b` after the second number but not a "verso" after it, and L7 refuses a
  lowercase word); 2 HTR or noise ("1Wingurla", "a3"); 8 are dates whose digits coincide with an OBP folio.
- The 9 offset-1 dev misses (e.g. 1267 "„ 543 a 544", 2548 "„38") have the start on the leaf's neighbour: a verso or
  run-on, or a one-scan map error; the scan decides, which I did not check.
- On the 2775 and test volumes the gold is not the weak point; two dev rows are subdocuments (2555 register of names) the
  GLOBALISE gold has no start for.

**Conclusion.** Bar E fails on VOC for every cell. The SG locators barely work on VOC TOCs: L7 finds 163 locators in 9 test
volumes (none in 2775) and covers 22% of the starts, L2 finds many but half are day numbers (204 of 411 are <= 31, 43 hit; 31
year-like, 0 hit). The VOC locator L8 triples coverage (0.57) and raises E to 0.644, well above the +3 chance (0.238) but far
from 0.90; the chain map is not the lever on dev (C2 0.551 against C3 0.540; C1 0.310 against U2 0.326). Half of the sampled
failures on test are locator errors that a better pattern could fix (dates, years, range connectors, "verso"); the other half
are map errors that 22b already measured (0.654 leaf). Not tried: restricting locators to numbers that increase along the
TOC, which would remove day numbers and years, nor using the scan for verso starts.

**Findings:** F16-22d.1 to F16-22d.8.

**Deviations from the brief.** (1) `dev_links.tsv` (extra file with all dev links of every cell, for reading). (2) The
"15 lines with no locator where the OBP has a folio on that page's range" was read as: lines of dev TOC sides with no L8
locator whose digits equal an OBP `folio_start` of the volume, because the brief does not define the page's range. 
Brief defects: the range alternation of the pattern lacks "à" and the dots; "verso" after the
range end is not allowed.

**Open issues.** None blocking.

### Step 22e: Generalisation table

**What changed.** No code. This section compiles the 22a-22d results into one table (component by archive, verdict), and
two notes outside the report were written: the three *used by* cells of the VOC rows in `ground_truth/README.md`, and
one dated section in NLP `docs/from-archival-structures.md`. Brief: `plan/phase-16-voc-generalisation/step-22e-generalisation-table.md`.
Every number below comes from the 22a-22d sections and findings above; none was recomputed. The table is also in
`reports/data/phase-16-voc-generalisation/step-22e/generalisation.tsv`. The layer covers 567 inventories.

**The table.** Verdicts of rows 7-9 are copied from the steps' own verdict lines.

| # | component | module / parameter | SG evidence | VOC verdict | VOC evidence | SG-specific part |
|---|---|---|---|---|---|---|
| 1 | text-layer build (extract -> join) | `archival_structures.text_layer`, preset `voc_1.04.02_v5sg` | SG full v5 run, audit `n_fail_judged = 0` | unchanged | 567 inventories built, `failures=0`, `n_fail_judged = 0`; 148 volumes below the reported (not judged) lower-case-merge share 0.95, min 0.8404 (F16-22a.1) | none for the build; the join's assumption that a line after a break mark starts lower-case does not hold (F16-22a.1, F16-22a.5) |
| 2 | volume mode by line-box height (cut 87.75 px) | text-layer `volume_mode`, cut 87.75 px | F12-15u.1 (60/60 fresh test volumes agree with silver), F12-R.5 (misses are handwritten index volumes) | runs, no bar | All 567 volumes `hand`, box height min 109, median 177.5 (GM) / 181 (GLOBALISE), max 273.5; no VOC mode gold exists (F16-22a.2) | scan scale: VOC scans are larger than SG ones (SG hand median 137), but the cut lies far below both, so it never decides anything |
| 3 | side layout (opening vs single page) | text-layer side rule on scan geometry | 22a sides agreement with the GM `Enkel`/`Dubbel` labels | runs, no bar | Agrees with the GM label in 483/545 volumes (88.6%): `Enkel` 340/343, `Dubbel` 143/202; 57 of 59 `Dubbel` disagreements are portrait single-page scans, so the label is the unreliable side (F16-22a.3); the label is not mode or side gold | none; the rule reads scan geometry. The GM `Dubbel` label does not mean two pages per scan |
| 4 | line types, page-number types | text-layer segment types (`pn_line`, `pn_line_offchain`, ...) | 22a `types.tsv`, `laypa_pn.tsv` (SG hand reference: 125 volumes) | runs, no bar | 342,451 `page_number` segments, 42.0% `pn_line_offchain` (SG hand 17.9%); 27 of 30 sampled are folio numbers; Laypa agreement P 0.650 R 0.825, not gold (F16-22a.4, F16-22a.7) | method mix: numbers sit off the SG page-number chain; marginalia 16.3% (SG 8.7%) and catch-words 2.1% (0.1%) are VOC layout |
| 5 | word-break set (pinned `-=„`) | text-layer `wbc_pinned` default `-=„` | 22a `breaks.tsv` | runs, no bar | `¬` ends 116,871 lines (0.66%) and is never joined (6.64 per 1,000 lines); `„` ends 8.1%; 4 of 10 read have `„` at the next line start (F16-22a.5) | the break marks: VOC HTR uses `¬` and repeats the mark as `„` on the next line |
| 6 | units (hand: region-based) | text-layer units, hand mode | 22a units read | runs, no bar | 10 sides read: 6 right order, 3 interleaved or out of order, 1 cut mid-sentence; marginalia and catch-words become units placed by type (F16-22a.7) | region-order assumptions: marginalia and catch-words placed by type, not position |
| 7 | folio map (v1, v2) | `analysis.folio_map`, `FolioMapParams`; map v1 (V0), v2 (V2) | phase 14 F 33/35, phase 15 F 62/63; 22b bar F | fails on VOC | Bar F (target 0.95): v1 550/904 = 0.608, v2 523/904 = 0.579, selected (V0, off, TL) 591/904 = 0.654 (F16-22b.1, F16-22b.2); LOO on the chain dev 0.884 (SG 0.986) (F16-22b.8) | largest read class: tied or duplicated numbers, 16 of 70 misses (F16-22b.4); also numbers on every other single-page scan (F16-22b.6) and the SG v2 rules G1-G3 (F16-22b.3) |
| 8 | index-side detector (v5 `index_sides`) | text-layer `index_sides` flags (`shape`, `col_share`, `end_num`, `date_start`) | phase 13 D1 P 0.971 R 0.949; 22c bar D | fails on VOC | Bar D (P >= 0.95, R >= 0.90, 10 test volumes): unchanged P 1/53 = 0.019, R 1/61 = 0.016; selected `tmc8_som0.25_cue_gap2` P 24/357 = 0.067, R 24/61 = 0.393 (F16-22c.1); no `VOC_INDEX_SIDES_V1` | largest read class: one-column TOCs with numbers or dates not at line ends or starts, 57 of 117 dev FN (F16-22c.2); false positives of the selected cell are ledger pages (F16-22c.4) |
| 9 | index links (locators L2/L7 + map) | `analysis.index_links`; U1 (L2, map v1), U2 (L7, map v2); VOC-only L8 (additive) | phase 15 L P 0.981, E 0.825 (failed); 22d bar E | fails on VOC | Bar E (target 0.90, oracle TOC sides): U1 142/411 = 0.345, U2 90/163 = 0.552, selected C3 (L8, V0/off/`page-number`) 279/433 = 0.644 (F16-22d.1); coverage 0.272/0.215/0.575 (F16-22d.3) | locators: day numbers and years taken as folios, 204 of 411 L2 locators <= 31 (F16-22d.2); in the read non-hits locator errors and map errors are about equal (11 and 15 of 30 dev, 17 and 10 of 30 test; test largest: not a folio, 13) (F16-22d.5) |
| 10 | front-index run (`front_index_run` defaults) | `front_index_run` SG defaults (`min_flagged` 10, `min_after` 300, gap 20) | 22c `front_runs.tsv` (reported only; no VOC bar) | runs, no bar | Run found in 0/22 volumes with unchanged flags, 4 with selected flags, 8 with oracle flags; `min_flagged` is the cause in all 14 oracle misses, `min_after` in 6 short volumes (F16-22c.6); end to end 0/22 (F16-22d.8) | `min_flagged` 10 and `min_after` 300 assume long SG index runs; GLOBALISE TOCs have 1-40 scans and some volumes have 148-220 sides |

**What carried over and what did not.**

- Geometry-based rules carried over (review: no VOC gold for them, so not tested; F16-22e.2): the text-layer build and its audit pass unchanged on 567 volumes (F16-22a.1), and the side rule on scan geometry is right where the GM label is not (F16-22a.3). The mode cut never decides anything because all volumes are `hand` (F16-22a.2).
- Numbering rules carried over only in part: the page-number types run but 42% sit off the SG chain (F16-22a.4), and the folio map reaches 0.608-0.654 against 0.95 (F16-22b.1, F16-22b.2) because folios are on every other scan and numbers are tied or repeated (F16-22b.4, F16-22b.6).
- SG conventions: the printed-index style (numbers at line ends, columns) behind the detector does not match one-column handwritten TOCs (F16-22c.2), and its false positives are ledger pages (F16-22c.4).
- SG conventions in the locators: L2 and L7 take day numbers and years (F16-22d.2); L8 triples coverage but stays at 0.644 (F16-22d.3).
- SG scale and run length: the front-run thresholds assume long index runs (F16-22c.6).
- SG break marks: `-=„` leaves `¬` unjoined (F16-22a.5).

**Error analysis.** No new reading. Each verdict was checked against the step's own verdict line and numbers: rows 7, 8 and 9
match 22b, 22c and 22d ("fails on VOC" in all three); rows 1-6 match 22a (the audit is the only row with a bar, and it
passed). The largest error class of each `fails on VOC` row is in the `SG-specific part` column, from the steps' own
category counts. **Disagreements between table and steps: none.** Notes: (a) row 10 and rows 2-6 had no VOC bar, so the
verdict is `runs, no bar` even where the numbers are poor (row 10: 0 of 22 volumes with a run on unchanged flags); the
review may want to treat row 10 as failing. (b) 22d's per-layout reading differs between dev and test (locator errors
11/30 dev, 17/30 test; map errors 15/30, 10/30), so row 9 names no single largest class. (c) "SG evidence" for rows 7-9
(phase 14 F 33/35, phase 15 F 62/63, phase 13 D1, phase 15 L/E) is taken from the brief, not from this report; their
finding IDs are not in this report.

**Gaps.** No VOC gold for mode, sides, line types, word breaks or units, so rows 2-6 have no precision or recall. The
SG-side numbers for rows 1, 3-6 are references in 22a (e.g. SG hand 17.9% `pn_line_offchain`), not bars. No number was
missing for rows 7-9.

**Findings:** F16-22e.1 to F16-22e.3.

**Brief defects.** None blocking. The brief's row 10 ("front-index run") has no verdict rule for a component with
a "reported only" criterion; `runs, no bar` was used. The GM row in `ground_truth/README.md` had "none yet" in *used by*;
it was replaced by the AS text (the rest of the cell is unchanged).

**Open issues.** None blocking.

## Review

*Opus 5.5, 2026-11-01. Code review in [`plan/phase-16-voc-generalisation/REVIEW.md`](../plan/phase-16-voc-generalisation/REVIEW.md).
Reviewer scripts are in the session scratchpad: re-runs only, nothing written into the step dirs. The planner of this
phase also reviewed it, so planning defects are named as such (F16-R.5).*

**Re-runs (F16-R.2).** I re-ran every acceptance number:
- the 22a audit, into the scratchpad;
- 22b's three test cells, with my own loop over `read_volume_inputs`, `front_index_run`, `build_folio_map` and
  `locate`;
- 22c's unchanged cell, from the layer's `layout/pages` flags;
- 22d's leaf hits, from `toc_gold.tsv`.

To show the SG defaults are unchanged, I loaded `git show HEAD:` copies of `folio_map.py` and `index_links.py` next to
the current ones and compared them on 3444. Everything matches except 22d.

**22d defect (F16-R.1).**
- My leaf check disagreed on 5 C3 test rows (for example 1120 folio 459 → scan 534). The leaf's second scan, 0535,
  has two gold rows, START then NONE, and `load_gold()` kept the last.
- A fixed copy in the scratchpad gave the same dev selection, so a Sonnet session (22r) applied the one-line fix and
  re-ran dev and test in place. The old outputs are in `step-22d/superseded_22r/`.
- This is a scorer fix, not a second look at test. The cells, the selection rule and the selected cell are the same.
- The read cases stand: none flips.

**Folio map (22b).**
- The code adds `single_foliation` as a guarded branch and keeps the old paths byte-equal. The option is inert on
  the leaf bar (F16-22b.3), a planning defect (F16-R.5).
- I read the worst stratum, `Dubbel` volumes with single portrait pages (leaf 0.288), and the miss classes hold
  (F16-R.3).
- 22b's decreasing-part classes are backed by the printed numbers it quotes. For example, 1341 has no number printed
  on scans 849–858 before folio 1 at scan 855. F16-P.3 is revised to match.
- The GM gold noise is near the bar's slack (F16-R.4).

**Detector (22c).** The replica check is exact, and so is my recomputation from layer flags. The FP categories were
assigned by rules written after reading 60 pages; the section says so. No `VOC_INDEX_SIDES_V1` was added, as the bar
requires.

**Links (22d).**
- `_extract_l8` does what the brief specifies: standalone, then a leading range, then L7, then the range filter.
- The gaps it shows (`à`, dots, "verso" after the range end) are brief-level pattern gaps, recorded as F16-22d.6.
- On 2775, C3 does better on the OBP silver than on GLOBALISE starts (47/52 against 41/143). The silver is a looser
  set (F16-22d.7).

**Table (22e).**
- Verdicts follow P11, and rows 7–9 copy the steps.
- I revised F16-22e.2: "did not fail" for rows without gold means "not tested".
- Row 10 stays `runs, no bar` but does not carry over in practice (F16-R.6).
- The NLP note and the three `ground_truth/README.md` cells are correct.

**Leakage (F16-R.6).** The checks pass.

**Candidates for `docs/findings.md`** (not edited): F16-22a.3 (the GM `Enkel`/`Dubbel` label is not a scan layout),
F16-22b.5 with F16-R.4 (GM folio gold has about 4–5% errors), F16-R.1 (GLOBALISE segmentation CSVs have several rows
per scan: collapse before scoring), and F16-22e.2 (what carries from SG to VOC).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F16-22a.5 | A VOC break set with `¬` (a new layer version, D17) | UA-060 (a); `plan/CONTEXT.md` §4 |
| F16-22b.4, F16-22b.5, F16-R.3, F16-R.4 | A VOC folio map that uses the side layout and numbering mode, restarts and volume starts, with a bar on a checked GM subset | UA-059 (check page), UA-060 (a); CONTEXT §4 |
| F16-22c.2, F16-22c.4, F16-22c.6 | A one-column TOC detector with a ledger guard; `front_index_run` thresholds scaled to volume length | UA-060 (a); CONTEXT §4 |
| F16-22d.6 | L8 v2 (`à`, dots, "verso" after the range, numbers increasing along the TOC) with a bar on fresh TOC reads | UA-060 (a); CONTEXT §4 |
| F16-22a.7 | VOC unit order (marginalia, catch-words) | CONTEXT §4 |
| F16-R.1 | 22d gold grain | done in 22r |
| F16-R.5 | Brief rules: gold row grain; a planning run of every new option | `plan/templates/STEP.md`, CONTEXT §5 |

## Reproduce

Planning (from the AS root):

```
.venv/bin/python reports/data/phase-16-voc-generalisation/planning/gm_split_and_proto.py
.venv/bin/python reports/data/phase-16-voc-generalisation/planning/toc_split_and_peek.py
```

Steps (from the AS root; T7_Data mounted; the 22b-22d test stages assert they run once, so move `select.json` and
`test_done.json` aside first to re-run):

```
D=reports/data/phase-16-voc-generalisation
.venv/bin/python $D/step-22a/stage_inputs.py
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures \
  --label 22a --want 6 --min 2 --wait -- \
  .venv/bin/as-build-text-layer --preset voc_1.04.02_v5sg --pagexml-dir data/text_layer_voc_inputs/1.04.02 \
  --out-dir /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/22a-a/1.04.02 \
  --stage all --workers '{workers}'
.venv/bin/python reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer data/text_layer_voc_22a --out $D/step-22a/audit.json
.venv/bin/python $D/step-22a/measure.py && .venv/bin/python $D/step-22a/analyse.py
for s in dev test archive context; do .venv/bin/python $D/step-22b/eval_gm.py --stage $s; done
for s in replica dev test cases; do .venv/bin/python $D/step-22c/eval_toc.py $s; done
for s in dev test; do .venv/bin/python $D/step-22d/eval_links.py --stage $s; done   # `errors` re-samples; the read sample is pre-22r
```
