# Phase 17 report: VOC rules v1

> **Status:** final (reviewed 2026-11-01; 23c test stage reviewed in an addendum 2026-11-02).
> **Phase plan:** [`plan/phase-17-voc-rules-v1/PLAN.md`](../plan/phase-17-voc-rules-v1/PLAN.md).
> **Code state:** unreleased after v0.21.0 (v0.22.0 recommended); no sibling code changed.
> **Last updated:** 2026-11-02, by the review addendum for the 23c test (Opus 5.5).

## Summary

Phase 17 wrote VOC rules for the four components that failed in phase 16. It tested each once, against a bar fixed
before any read, on gold from 24 VOC volumes no earlier phase had touched. **No bar passed, and no VOC
constant was added.**

| Component | Step | Bar | Fresh result | Against phase 16 |
|---|---|---|---|---|
| `¬` word break (preset `voc_1.04.02_v6`) | 23b | ≥ 57/60 joins right | 56/60 (reviewer: 55/60): fails | 106,569 more merges; upstream stages identical |
| Folio map options | 23c | F ≥ 0.95 leaf | 79/109 = 0.725 (Wilson 0.634-0.800): fails | BASE (22b) 81/109 = 0.743; McNemar b 2, c 0, p 0.5 |
| One-column TOC detector | 23d | P ≥ 0.95, R ≥ 0.90 | P 0.583, R 0.276: fails | PH16 P 0.706, R 0.244; McNemar p 0.147 |
| Locators L9g + monotone filter | 23e | L P ≥ 0.95, R ≥ 0.90; E ≥ 0.90 | P 0.556, R 0.400; E 0.529 (9/17): fail | C3 P 0.333, R 0.360, E 0.269 |

- **What the fresh volumes show.** 21 of 24 open with a register TOC (F17-23a.1). 42% of TOC pages carry no folios:
  they are numbered enclosure lists (F17-23a.2). Only 15% of register lines name a folio (F17-23a.4).
- **TOC detector (23d).** Dev recall halved on unseen volumes. The fresh misses are dated summary registers and
  enclosure lists, which the line features cannot see (F17-23d.3, F17-R.1).
- **Locators (23e).** The losses are entry numbers read as folios on pages that carry no folios (F17-23e.2). The map is
  not the limit (E_map 0.857). A page-level "this page carries folios" cue is the open rule (F17-23e.5). With 25 gold
  folios, bars L and E were not decidable (F17-R.2).
- **Folio map (23c).** On 109 folios the user read on the page images, the selected options are no better than 22b's map
  (the dev gain on GM does not carry over), and both miss a quarter. 18 of the 30 misses are map-side: ties and a second
  numbering in the volume that the map answers from instead of the numbering around the scan (F17-23c.10).
- **`¬` (23b).** It is a hyphen in about 92% of joins. The rest are scribes' line-end fillers after a complete word or an
  abbreviation (F17-23b.5, F17-R.3). The v6 layer is kept on the drive.

## Findings
| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F17-P.1 | 4,394 OBP inventories have PageXML on the drive and lie outside phase 16's 567. 24 test volumes and 16 reserves were drawn from them (seed 20261101), reading only file names and sizes (zip median 23 MB, as in the 567) | method | voc, gold, split | confirmed | [Planning](#planning) |
| F17-P.2 | With `¬` added to the pinned break set (`-=„¬`), 856 of 870 `¬`-ended lines in 6 phase-16 volumes merge (13 `g_digit_right`, 1 `g_standalone`). 176 of the 870 next lines start with `„`, which the existing `both_ends_fix` drops. The pooled word-break rule already put `¬` in the derived set of 418 inventories | data-property | voc, text-layer, word-breaks | confirmed | [Planning](#planning) |
| F17-P.3 | On the phase-16 GM anchors the prototype harness reproduces 22b (dev 647/929, test 591/904). The five folio-map options move the leaf hits by −1 to +16 (dev) and 0 to +22 (test); all together reach 679/929 = 0.731 and 625/904 = 0.691. Candidate choice by chain (pick) is the largest single gain | result | voc, folio-map, planning | confirmed | [Planning](#planning) |
| F17-P.4 | Against the scan where the map puts folio N's printed number, GM end anchors often sit on the next scan (single-page printed ends: 110 at +1 against 257 at 0), and 33 single-page begin anchors sit 2 scans before. GM's scan for "folio N" is not always the scan where N is written; phase 17 tests on numbers read on the image | data-property | voc, gold, folio-map | confirmed | [Planning](#planning) |
| F17-P.5 | A one-column TOC rule (folio lines, leading or trailing ranges, dated entries, a register title) limited to the first 60 scans gives P 0.688, R 0.570, F1 0.624 on the 22 GLOBALISE volumes (TP 106, FP 48, FN 80), against 22c's best dev F1 0.300. Without the window F1 is 0.32. Inside the window the ledger guard changes nothing. FN concentrate in 2548 (35), 2448 (19) and 2555 (17) | result | voc, index, toc, planning | confirmed | [Planning](#planning) |
| F17-P.6 | With those flags, `front_index_run` finds a front run in 7 of 22 GLOBALISE volumes under the SG defaults, and in 21 of 22 with `min_flagged` 1, `max_first` 60 and `min_after` min(300, ⌈0.5·n⌉). The 22nd, 8023, has no TOC | result | voc, front-run, planning | confirmed | [Planning](#planning) |
| F17-P.7 | With the 22b map on the 22 GLOBALISE TOCs, E and coverage are 0.545 / 0.518 for L8, 0.606 / 0.636 for L9 (connectors, trailing ranges), 0.641 / 0.635 for L9g (date and year guards) and 0.816 / 0.568 for L9g with the monotone filter. The "verso" misses of F16-22d.6 are trailing ranges, which L8 has no arm for | result | voc, index-links, locators, planning | confirmed | [Planning](#planning) |
| F17-23a.1 | 21 of the 24 fresh sample volumes (88%) have a front register TOC (23 have a toc page of any type); the 3 without hold a daghregister (1390) or only a title page (2222, 2487). The 12-volume target is met without reserves | data-property | voc, gold, toc | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23a.2 | Readers labelled numbered enclosure lists (no folio numbers) inconsistently (`toc/register` 284, `other_list` 47, `no` 4 pages); one rule (a list of the volume's documents is a register, with or without folios) changed 51 of 1,579 labels. 143 of 344 toc rows (42%) are `loc=no`, mostly enclosure lists, so detector scores need a with and without split | data-property | voc, toc, gold | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23a.3 | TOC pages per volume with a toc: min 1, median 9, max 87; they start at scan 3 to 11. 2282's summary register runs from scan 17 to about 110, past the 100-scan read cap, and points into resolutions that start at scan 111 | data-property | voc, toc | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23a.4 | Of 168 sampled register lines, only 25 (15%) name a folio on the line itself; 141 name none and 2 are unreadable. Readers had taken 36 before the rule that a line names only the numbers written on it removed 10 whose folio stands on a neighbouring line of the entry. Recall for L is measured over 25 lines in 13 volumes | data-property | voc, locators, gold | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23a.5 | Of 25 gold targets, 21 are found (13 sure, 8 likely) and 4 not_found. Three of the not_found are the 2538 register, which names folios 2358, 2496 and 2833 beyond the volume's printed folios (about 2,235): multi-volume registers. 2 gold folios (2954 409, 3793 3330) are HTR misreads of the line text, recorded as `htr=` notes | data-property | voc, links, gold | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23a.6 | Agent labelling is reliable on the TOC task: the 30-scan self-check agrees 30/30 (7 of the 15 `no` draws in scans 30-60, no skimming seen), and 2 of 1,579 labels are `unclear`. The weak points are rules the brief left open (enclosure lists, bare numbers, folios on neighbouring lines), not reading errors | method | voc, gold, agents | confirmed | [Step 23a](#step-23a-fresh-gold) |
| F17-23b.1 | **The join bar is missed: 56 of 60 judged `¬` joins are right** (0.933, Wilson 0.841-0.974; bar 57). The 4 wrong joins glue two separate words after a spurious or abbreviation `¬` ("huij¬" + "op", "momo¬" + "Witsen", "aen¬" + "'t geen", "maij¬" + "nu"); 5 further joins are unclear. Negative result; the layer is kept, no retuning. **Revised at review:** k = 47 ("bij nae¬" + "oijt" → "naeoijt", two words) is wrong, not unclear, so 55 of 60 (F17-R.3); the verdict is unchanged | negative-result | voc, text-layer, word-breaks | revised | [Step 23b](#step-23b-voc-break-set-with-) |
| F17-23b.2 | Adding `¬` changes only the join stage: 0 of 5,670 upstream files differ from 22a, and 68,534 of 4,593,380 units change, all with a `¬` in the 22a text. `¬` in unit texts falls from 116,920 to 10,329: 9,525 at the end of a unit (nothing to join), 779 guarded (703 `g_digit_right`), 25 inside a line, 0 other. Merges rise from 1,038,099 to 1,144,668 | result | voc, text-layer, word-breaks | confirmed | [Step 23b](#step-23b-voc-break-set-with-) |
| F17-23b.3 | The digit guard on `¬` is right on VOC: in 20 sampled guarded `¬` boundaries 0 should have joined with the next line. 17 are before a number (accounts, quantities, dates: "tverc¬ 33¾"), 2 after a lone mark; 4 are words whose continuation sits two lines later behind an interleaved number ("Gezond¬ 10 heijd"), which no adjacent-line join can repair | result | voc, text-layer, word-breaks | confirmed | [Step 23b](#step-23b-voc-break-set-with-) |
| F17-23b.4 | Audit: `n_fail_judged = 0`; the only differing keys are `n_pass`/`n_fail` (10,069/148 to 10,074/143), `counts` (word_break_chars `-=„` to `-=¬„`, merges, merge_lower_share) and 17 per-inventory reported-only lower-case-merge checks that flip (6 pass to fail, 11 fail to pass). The pooled lower-case share of merges is 0.9655 (22a) against 0.9651 (v6): the 106,569 new merges are as lower-case-continued as the old | result | voc, text-layer, audit | confirmed | [Step 23b](#step-23b-voc-break-set-with-) |
| F17-23b.5 | A glued join is the characteristic error: all 4 wrong joins and 1 of 10 sampled changed units ("Jopin¬" + "soe" in inventory 1107) are a `¬` that follows a complete word or an abbreviation, so the next line's first word is fused to it. `both_ends_fix` and the guards cannot see this; the share is about 7% of judged joins (4 of 60). **Revised at review:** 5 of 60 with k = 47 (about 8%) | data-property | voc, word-breaks, error-analysis | revised | [Step 23b](#step-23b-voc-break-set-with-) |
| F17-23c.1 | The dev grid (48 cells, 1,833 corrected GM anchors) selects pick_by_chain + tie_keep first + clean_tol 3 (pieces off, extrapolate 6): leaf 1311 = 0.715 against BASE 1245 = 0.679 (+66), strict 1057 against 1018, leave-one-out 0.884 against 0.878. The best cell (all five options, extrapolate 30) has 1313. Bar F (0.95) is far off on dev; test waiting on UA-061 | result | voc, folio-map | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.2 | Alone against BASE, pick gains 36 leaf hits (1281; 55 fixes, 19 breaks), tie first 10 (56 fixes, 46 breaks), clean 3 (30 fixes, 27 breaks), pieces 1 and extrapolate 30 1. Pick and tie first together gain 45 (1290), pick, tie first and clean 66: the options interact, and pick is the only one that clearly pays | result | voc, folio-map | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.3 | Where a number is printed on several consecutive scans, which copy holds the gold depends on the case: in the sampled 15 breaks, 9 are ties where the GM anchor sits on a later copy and "first" picks an earlier one; in the 15 sampled fixes, 6 are ties where the anchor sits on the first copy or between the copies. "First" beats "last" on dev (1255 against 1248 alone) but the choice is a convention of the GM anchors, not a property of the print | negative-result | voc, folio-map, gold | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.4 | Chain cleaning (clean_tol 3) is a wash: 30 fixes and 27 breaks alone. Its breaks drop correct interior points when the neighbours are sparse or noisy (4 of 15 sampled breaks). It only helps leave-one-out (0.878 to 0.887 alone) | negative-result | voc, folio-map | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.5 | Pieces (secondary chains outside the main span) and extrapolate 30 do nothing on the GM anchors (+1 each, 2 anchors use `piece_*`). Restarts and volume-start extrapolation are not what limits the map on GM | negative-result | voc, folio-map | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.6 | Of the 15 sampled fixes, 10 are real (a second candidate, a repeated header such as 966, a tie, or a bad chain start), 3 are convention-dependent ties (the anchor sits between or on the first copy), 1 is lucky (a 4-fold tie), 1 has a doubtful gold (inv 2425). The GM gap to 0.95 stays: most dev misses are anchors that sit 1 to 3 scans off the printed number (F17-P.4), which no map option changes | result | voc, folio-map, error-analysis | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.7 | **Bar F fails on the image gold.** SEL leaf 79/109 = 0.725 (Wilson 0.634-0.800), strict 78 (0.716); BASE 81/109 = 0.743 (0.654-0.816), strict 81. Openings 17/23 both; single 62/86 (BASE 64/86). 109 `number` anchors, all main, none layered. No `VOC_FOLIO_MAP_V1` constant was added. | result | voc, folio-map, bar-f | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.8 | SEL does not improve on BASE on the image gold: exact McNemar b = 2 (BASE only), c = 0, p = 0.5. The dev gain on GM (+66 leaf, 0.679 to 0.715) does not carry over; the two anchors that differ are both ties where `tie_keep=first` takes the wrong copy (2954 folio 386, 3793 folio 3465). | result | voc, folio-map | confirmed | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.9 | BASE on the image gold, 81/109 = 0.743 (Wilson 0.654-0.816), is no worse than on the GM anchors (0.679, inside the interval). The volumes and anchor kinds differ, so this is not a paired comparison, but gold read where the number is written still leaves a quarter of folios missed: GM's begin/end convention (F17-P.4) is not what keeps the map far from 0.95 | result | voc, folio-map | revised | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.10 | Of the 30 SEL misses, 18 are map-side: tied or duplicated number 10, restart or second numbering 5 (3 of them have no number at the gold scan in the HTR but the neighbours carry the local numbering, e.g. 2167 folio 69: 67 at 1137, 71 at 1141, map answers 89 from another numbering), several numbers on a side 2, a one-opening offset 1. HTR wrong digit at the gold scan 8 (2 on golds the user marked unsure), unclear 4. 7 misses have no prediction. A map that prefers the numbering local to a scan's neighbours is the open rule; SEL's options address none of these | result | voc, folio-map, error-analysis | revised | [Step 23c](#step-23c-voc-folio-map) |
| F17-23c.11 | 131 of 240 image-gold items have no folio number, but 111 of the 124 single-page ones are even scans, the versos of foliated leaves, unnumbered by design. Scans that should carry a folio and do not: 20 of 240 (8%), in 12 volumes, at most 5 per volume (4325). The foliation, not missing numbers, explains the `none` share | data-property | voc, folio-map, gold | revised | [Step 23c](#step-23c-voc-folio-map) |
| F17-23d.1 | **Bar D fails on the fresh labels.** SEL (the unchanged defaults: window 60, toc_min 0.2, no amount guard, gap 4, title on) gets P 0.583 (95/163, Wilson 0.506-0.656) and R 0.276 (95/344, 0.232-0.326), F1 0.375, against P 0.95 and R 0.90. Dev recall was 0.570 and dev precision 0.688, so recall halves on unseen volumes. No retuning, no `VOC_TOC_SIDES_V1` | negative-result | voc, toc, bar-d | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23d.2 | PH16 (22c's cell) gets P 0.706 (84/119, 0.619-0.780) and R 0.244 (84/344, 0.202-0.292), F1 0.363. SEL against PH16 on per-scan correctness over 1,404 scans: b 94, c 116, exact two-sided p 0.147, so neither is better. The two cells find different pages: SEL finds the `loc=no` enclosure lists and title pages (37/143 and 6/11, PH16 2/143 and 0/11), PH16 the `loc=yes` registers better (82/201 against 58/201) | result | voc, toc | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23d.3 | Of the 249 SEL false negatives, only 49 lie beyond the 60-scan window (40 of them in 2282's register, which runs to scan 110). The other 200 are in-window pages the feature set cannot see: dated summary registers with no folio numbers (2167, 2538, 8466, 8875), numbered enclosure lists (1631, 2047, 4325), and long entries that end in a single locator after dot leaders (2282, 8875). Recall is 0.289 on `loc=yes` and 0.259 on `loc=no` | data-property | voc, toc | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23d.4 | Of the 68 SEL false positives, 35 are one volume (2487): prose with paragraph numbers (`1130.`) on lines of their own, which the folio-line pattern reads as folios (fol 0.1-0.27). Others: a "Register" or "inhoud" word in a letter (title cue), and near-empty sides (n 0-1) turned on by the gap fill. Only 1 of 30 sampled FP is a ledger | data-property | voc, toc | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23d.5 | The amount guard is inert: 1 side blocked on the 22 dev volumes, 0 on the 24 fresh volumes, and the one sampled ledger FP (4325 scan 55) has amt 0.03. The front window already keeps ledgers out; dev grid cells with and without the guard differ by 0 TP and 0 FP | negative-result | voc, toc | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23d.6 | `front_index_run` with `VOC_RUN` (min_flagged 1, max_first 60, gap 20, min_after_frac 0.5) finds a run in 21/22 dev and 23/24 fresh volumes, against 7/22 and 4/24 under the SG defaults. A run is not a right run: on fresh volumes it often rests on 1-5 flagged sides (2282: `2-6/7` for a register at scans 17-110; 8466: `1-7/8`), so the count measures only that the thresholds no longer reject | result | voc, toc, front-run | confirmed | [Step 23d](#step-23d-one-column-toc-detector) |
| F17-23e.1 | **Bars L and E fail on the fresh gold.** SEL (L9g + mono, 22b map; dev F1 0.670, E 0.816) gets L P 0.556 (10/18, Wilson 0.337-0.754) and R 0.400 (10/25, 0.234-0.593) against 0.95 and 0.90, and E 0.529 (9/17, 0.310-0.738) against 0.90. C3 (L8) gets P 0.333, R 0.360, E 0.269 (7/26). With 25 gold folios the intervals are wide, but no part of them reaches a bar | result | voc, locator, links | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.2 | The loss is the locator's entry-number false positives, not the map. All 9 predicted numbers that name a gold folio hit the right leaf (E 9/9 among them) and E_map is 18/21 = 0.857 (sure targets 10/13); all 8 E misses are numbers that are not folios: item numbers at line start ("No 14.", "12: Copia ...", "401") on pages that carry no folios (6 of 8 on `loc=no` pages), plus one second number of "388 288". L precision on `loc=yes` lines is 10/12 = 0.833, on `loc=no` lines 0/6 | result | voc, locator | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.3 | Recall is low because few sampled lines (25 of 166) name a folio and 15 of those 25 are missed: 5 by pattern gaps (four-dot connector "3382. . . . 3384", `„` as connector in "447 „ „ 450", a lead number followed by dashes and text; 2 volumes, 1631 and 3793), 5 by the range filter (2282's map range 877-2491 excludes folios 4 and 710; 2538's three gold folios lie beyond the volume), 3 dropped by the monotone filter, 2 HTR misreads (2954, 3793) | result | voc, locator | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.4 | The monotone filter's dev gain does not hold on fresh TOCs as a recall measure: it dropped 10 locators on the 25-folio sample, 3 of them gold (773, 388, 45) and 7 not (entry, day or year numbers such as 14, 603, 1740). Without it SEL would be P 13/28 = 0.464, R 13/25 = 0.520 (post-hoc, from `mono_dropped`); its precision gain is real but small on this sample | negative-result | voc, locator, mono | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.5 | Only 25 of 166 readable sampled lines (15%) name a folio, and all 25 are on `loc=yes` pages (81 sampled lines); 85 sampled lines on `loc=no` pages hold none, and SEL puts a number on 6 of them (all false). A locator needs a page-level cue (does this page carry folios) before it reads a leading number; the L9 patterns are line-level only | open-issue | voc, locator, toc | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.6 | End to end (23d's SEL detector, `VOC_RUN`, then SEL locators on the predicted sides) 66 of the 166 sampled lines lie on predicted sides (16 with a gold folio) and give E 8/8 (Wilson 0.676-1.000) and R 9/25; the detector's misses cost recall, its hits do not cost precision on these few links. Silver: SEL 461/642 = 0.718 precision and 461/1088 = 0.424 recall against the OBP `folio_start`, C3 0.523 and 0.413 | result | voc, links, end-to-end | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-23e.7 | The dev selection did not choose the 23c map: the cell with SEL22B differs less from C3 and is within 0.005 of the best (L9g+mono: 0.6699 with SEL22B, 0.6722 with SEL23C). On the dev 22 volumes the 23c map adds 5 coverage hits (872 vs 867) and no E change (901/1104 both); on fresh gold the map is not the limit (F17-23e.2) | result | voc, folio-map | confirmed | [Step 23e](#step-23e-locator-l9-and-voc-links) |
| F17-R.1 | Bar D's failure does not depend on the gold definition. On `loc=yes` toc pages SEL recalls 58/201 = 0.29 (PH16 82/201 = 0.41); without the 87-page 2282 register 55/115 = 0.48 (PH16 72/115 = 0.63); over all toc pages without 2282 and 2167 86/203 = 0.42 (PH16 60/203 = 0.30). SEL's gain over PH16 comes from enclosure lists (`loc=no`); on registers that carry folios PH16 is better | result | voc, toc, bar-d, review | confirmed | [Review](#review) |
| F17-R.2 | The fresh link gold was too small to decide bars L and E: 8 lines per register TOC gave 25 gold folios, because 85% of sampled lines name none (F17-23a.4). Even 25/25 has a Wilson lower bound of 0.867. A decisive test samples lines on `loc=yes` pages and sizes the draw on the expected yield (CONTEXT §5) | method | voc, gold, locators, review | confirmed | [Review](#review) |
| F17-R.3 | The reviewer reads `¬` join k = 47 ("bij nae¬" + "oijt" → "naeoijt") as wrong, giving 55 of 60. All 5 wrong joins are a `¬` after a complete word or an abbreviation: VOC scribes use `¬` as a line-end filler as well as a hyphen, so a lexicon or next-word check is the candidate rule, on fresh reads | result | voc, text-layer, word-breaks, review | confirmed | [Review](#review) |
| F17-R.4 | The orchestrating (reviewing) session ran the 23a readers and set the label rules for enclosure lists (i–iv), bare numbers, neighbouring-line folios and HTR misreads (a–c) after the reads and before any phase-17 code ran on the fresh volumes; 51 toc labels and 25 link-line edits, all logged. They are definitional and applied to every row: no leakage, but the gold is not reader-only | method | voc, gold, agents, review | confirmed | [Review](#review) |
| F17-R.5 | 9,525 `¬` remain at unit ends in v6 (92% of the 10,329 left): the word continues in the next unit across a page or region break. Joining them needs VOC unit order (F16-22a.7), not another guard | open-issue | voc, text-layer, word-breaks, review | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

*Opus 5.5, 2026-11-01. Scripts: `reports/data/phase-17-voc-rules-v1/planning/` (probes read phase-16 data only).*

- **Gold.** All phase-16 gold is dev data in this phase. Every bar is tested once on 23a's fresh gold from 24 new
  volumes (`fresh_volumes.tsv`, F17-P.1):
  - folio anchors read by the user on page-top crops (UA-061);
  - TOC labels, locators and link targets read by agents.
- **Bars** (fixed before any read):
  - F ≥ 0.95 (leaf);
  - D: scan P ≥ 0.95, R ≥ 0.90;
  - L: P ≥ 0.95, R ≥ 0.90;
  - E ≥ 0.90;
  - `¬` joins ≥ 57/60.
  - F and D also get a McNemar test against the phase-16 configuration.
- **Probes:** F17-P.2 to F17-P.7. Every option the briefs define moved its dev measure at planning. None comes near
  its bar, so negative test results are likely, and the paired comparisons will say whether anything improved.

### Step 23a: Fresh gold

**Status: agent gold frozen** (`step-23a/gold/agent_gold.done`, 2026-11-01T16:05:07Z). **Folio gold waits on UA-061** (the user
reads 240 crops; `summarise_folio.py` then writes `gold/folio_gold.tsv` and `folio_gold.done`). `D` = `reports/data/phase-17-voc-rules-v1/step-23a/`.

**What was built.** The 40 fresh volumes (24 sample, 16 reserve; reserves not needed) were staged (`stage_inputs.py`, disjoint from the
567, asserted) and built with `voc_1.04.02_v5sg` into `data/text_layer_voc_23a` (a link to
`/Volumes/T7_Data/.../text_layer_voc/23a-a`): `failures=0`, audit 715 pass, 14 fail, `n_fail_judged = 0` (the 14 are reported-only
"merges with lower-case right side" shares below 0.95, e.g. 4333 0.923, 8149 0.916). `protect_voc`: only the 23a layer and, after the
freeze, the 5 gold files are new (the final diff adds `gold/{SHA256SUMS,agent_gold.done,link_lines.tsv,link_targets.tsv,toc_labels.tsv}`);
`protect.py --skip 23`: no change. Reader inputs: `dump.py` writes `dumps/<inv>_front.txt` (scans 1-100) and `<inv>_numbers.tsv` for all
40; `show.py --inv X --from A --to B` prints scans from the whole volume. The folio page (`folio_page.html`, 240 crops, 24 volumes x 10
scans; 210 single-page and 30 opening-layout items) is blind (grep for `pred` and for 3 sampled line texts: 0 hits; JS parses).
`summarise_folio.py` was tested on a fake verdict file only.

**Orchestration note.** The brief says this step's session spawns the readers. It did not: the orchestrating session ran them, 12 TOC
readers on 2 volumes each (the brief said at most 4) and 7 target readers on 3 volumes each. This session merged and checked. Readers
r01-r12 (TOC) and t01-t07 (targets) are recorded per row. Raw reads are in `D/reads/` and were never edited.

**Brief amendments (orchestrator decisions, after the readers had run).**
1. *TOC harmonisation* (the readers labelled enclosure lists inconsistently; `reads/harmonise.tsv`, 51 changes). Rule (i): a page that is
   wholly or largely a list of this volume's documents, enumerated one by one, is `toc/register` with or without folio numbers; a
   `title` page stays `title`. Rule (ii): a short list (about 10 items or fewer) inside a letter's running text, on a page that is
   otherwise letter text, is `no`. Rule (iii): `other_list` only for lists whose items are not documents. Rule (iv): 2282's summary
   register is checked against the volume. Applied: (iii) 47 rows (2167 scans 19, 21-60; 8840 scans 7-12: `other_list` to `register`);
   (i) 3 rows (7694 scans 26, 27, 83: `no` to `toc/register`; scan 84 stays `no`); (ii) 1 row (2538 scan 42: `toc` to `no`, 2 list
   items in 8 of 26 lines); (iv) 0 changes: 2282's "Register en korte Daementreckinge ... van de Resolutien" (entries with trailing
   numbers 1-874+) points into the volume, because "Resoolitie genomen in Rade van Mallabaar" starts at scan 111 and the entries
   summarise it. So 2282 scans 17-100 stay `toc/register`. Because 7694's last toc moved from 82 to 83, its region grew to scan 93,
   which no reader had read; the gap row (`no`, letter text) is in `reads/extra_labels.tsv`, reader `r03+23a`.
2. *`loc=` prefix.* Every `toc` row's `note` starts `loc=yes;` if the page carries folio or page numbers that point into the volume,
   else `loc=no;` (title pages and enclosure lists with item numbers only are `no`). Set per volume and scan range from the readers'
   notes and spot reads (`reads/loc.tsv`). 201 of the 344 toc rows are `loc=yes`, 143 `loc=no`. This is for 23d to stratify; it is a
   judgement, not a measurement.
3. *Link-line consistency* (`reads/links_consistency.tsv`, 25 edits to 14 lines). Rule (a), bare-number lines: the number counts when
   it stands in the folio column of a `loc=yes` page and lies in the volume's folio range; item numbers, the register page's own
   number and running heads give nothing. Rule (b): a line names only the numbers written on it; a folio written on a neighbouring line
   of the entry belongs to that line. Rule (c): a gold folio N that is not a digit string in the line text keeps the reader's value and
   gets `htr=<digits as written>` appended to `note`; 23e scores L on both.

**Results against the acceptance criteria.**
- Criterion 1: 40 staged, `failures=0`, `n_fail_judged = 0`. Met.
- Criterion 2: `toc_labels.tsv` has 1,579 rows (24 volumes, each complete over 1..max(60, last toc+10) capped at 100). Labels: no 1,060,
  no_text 173, toc 344, unclear 2 (0.13%). `list_type` of the toc rows: raw register 284, other_list 47, title 11; final register 333,
  title 11. Self-check 30/30 (15 `toc`, 15 `no`, 17 volumes; the ids were written before looking; every one agreed). 7 of the 15 `no`
  draws fall in scans 30-60, so the skimming worry did not show. Met.
- Criterion 3: 21 of the 24 sample volumes have a front register TOC (a `toc/register` scan among scans 1-60); 23 have a `toc` of any
  type. No reserve was read. Met (target 12).
- Criterion 4: `link_lines.tsv` has 168 rows (8 for each of the 21 TOC volumes; the smallest candidate pools are 2954 with 15 and 8029
  with 33), all with a `status`. After the consistency pass 25 lines name a folio, 141 name none (`ok`), 2 are `unreadable`.
  `link_targets.tsv` has 25 rows, one per gold folio: found 21 (sure 13, likely 8), not_found 4 (all `likely`). Met.
- Criterion 5: the page has 240 items, the blind grep is clean, the JS parses, UA-061 is written. Met.
- Criterion 6: hashes below. Met.

| File | sha256 |
|---|---|
| `gold/toc_labels.tsv` | `6f66ce8e329afd8527883a51cd8d657a5a8705bcfe4513f863f182f42c054d49` |
| `gold/link_lines.tsv` | `ce48293a26b5582e3417c15fe49219c160e2277449fffb99faace614af84235a` |
| `gold/link_targets.tsv` | `fbe9a053b7de5c13220c5d368ed3fd303afcf3714f7e52b3994a84ce6e757ee6` |
| `folio_sample.tsv` | `ade42b010265a7e20b54042a71fcf74ca5e5228dc361cffe004330005f2d5d86` |

**Error analysis.**
- *Self-check:* 30 of 30 agree, so there are no disagreements to list. Reader and I used the same dumps, so this measures reader noise
  and not shared blind spots. The 15 `toc` draws came from 9 volumes and the 15 `no` draws from 11 volumes.
- *Harmonisation:* I re-read 16 candidate pages in show.py before changing anything (2167 scans 19-22, 40, 60; 8840 scans 3, 7, 12;
  7694 scans 26, 27, 83, 84; 2538 scan 42; 8875 scan 41; 8813 scan 19; 7978 scans 9-11, 66). Example of (iii): 2167 scan 22 "N=o 52
  Extract missive sub dato 20:en feb ... 53 twee Copia translaat mandaat" is an enclosure list of 39 consecutive pages; calling it
  `other_list` would hide it from every rule that wants registers. Counter-example kept: 8813 scan 19 has a 6-item list in about
  half of the 38 lines, followed by a letter. I kept it `toc` as the last page of a list run; under rule (ii) alone it would flip. A
  threshold on the share of list lines (about 50%) would be a cleaner rule.
- *`not_found` targets (4 of 4):* 2538 folios 2358, 2496 and 2833: the register names folios beyond the volume (the volume has 2,296
  scans and printed folios reach about 2,235), so the register spans several volumes. 1723 folio 1330: the Carel catalogue title
  is in a table that the HTR dropped; neighbouring numbers 1324 (scan 589) and 1329 (scan 609) are found.
- *`likely` targets (8 found):* 1631 447 and 502 (a missive heading not seen), 2047 folio 5 (not printed; extrapolated from 8 at scan 31
  and 10 at scan 35), 2167 842 (number at the foot of the page), 2379 789, 3793 3198, 3330, 3382 (sender not visible).
- *`unreadable` lines (2 of 2):* 3793 scan 23 "3864." (a lone garbled number between 3353 and 3404; there is no such page), and 7694 scan 5
  "den 19 d:o per den Briefken van den Gesaghebber Cornelis van Kiel" (the entry's folio is lost in HTR).
- *Consistency pass, per rule (25 edits):* (b) 20 edits on 10 lines (gold folio and note): 1556 scans 15-17, 1723 scans 14 and 16, 2047
  scans 10-11, 8466 scan 6. Example: 1556 scan 15 "dito vanden koninck Toloko en de Ternaetse Rijcx" had folio 408, written on the next line
  ("408 a 408 ver."). (a) 3 edits: 2379 scan 15 "603" dropped (inside a `loc=no` enclosure list), 2282 scan 17 "4." kept with a note
  (folio column of the summary; the page's own number is 1975/23). (c) 2 notes: 2954 "109 709 — 841" read 409 (`htr=109`); 3793 "3380 ... 3373"
  read 3330 (`htr=3380`). Cases the orchestrator listed that did not match the data: 3793 "3058" is not a sampled bare line (the
  sampled line is the entry's text line, and 3058 stands alone four lines up; it stays empty under rule b); 2379 "173." is in the
  target evidence only, not in the line text ("„ 773."); 2047's Cananor line has no inline "300." (it stands on the previous line).
  Rule (b) removed 10 of 36 folios, the single largest effect.
- *`list_type` and TOC lengths:* final 333 register, 11 title, 0 other_list. TOC pages per volume over the 23 volumes with a toc: min 1,
  median 9, max 87 (2282). First toc scan: 3 to 11 (median 7; 11 of 23 volumes start at scan 5 or earlier); last toc scan: 4 to 100
  (median about 24); 2282 (7-10 and 17-100) and 2167 (7-60) are the long ones. Pages with a `loc=no` note: 143 of 344.
- *Volumes without a front register TOC (3):* 1390 is a daghregister (scan 5 is its title page, scan 9 its text; no list in scans 1-60);
  2222 has only a title page at scan 4 ("Register der Brieven & Papieren van Ceijlon, F:o 425 tot 913") and then letters; 2487 has only
  a title page at scan 7 ("... van Batavia, fo 4628-5050") and then letters.

**Data properties.**
- 21 of 24 sampled volumes (88%) have a front register TOC, 23 of 24 have a toc page of some kind.
- 143 of 168 sampled register lines (85%) name no folio on the line itself (141 empty, 2 unreadable). Reasons, from the notes: the line
  is a continuation line of a multi-line entry (the folio stands on the entry's last line), an enclosure list with item numbers, a
  title or a date. This is the value to use when scoring L: recall is measured over 25 lines, and only 13 volumes have one.
- The 2538 register names folios beyond the volume (3 of its 3 gold lines with folios past 2,235; see not_found).
- 2282's TOC runs past the 100-scan read cap: the summary register ends near scan 110, so scans 101-110 are unlabelled, and no link
  line lies after scan 85.
- 143 toc pages (42%) carry no locators (`loc=no`): these are enclosure lists with item numbers (2167: 41 pages, 3260: 18). Detector
  scoring (D) should be reported with and without them.

**Findings.** F17-23a.1 to F17-23a.6 (table above). Scripts: `D/{stage_inputs,dump,show,folio_sample,build_folio_page,summarise_folio,merge_toc,sample_lines,merge_links}.py`.
Brief defects are listed below.

**Brief defects.**
- `read_volume_inputs` always opens `layout/pages/` and `types/segments/`, which the brief's constraints forbid readers to see.
  `dump.py` uses only `.sides` and `.numbers` and drops `flags`.
- The brief does not say which text column the dumps use (`text`, not `text_raw`), nor how to read "front register TOC" (label
  `toc` with list_type `register`; 21 on that reading, 23 on any toc).
- The brief assumes this session spawns the readers; it did not, and readers per volume differ (2 and 3 against at most 4).
- A target reader prefixed one line id with `line_` (2954 scan 5 "388 288"); `merge_links.py` normalises it.
- `zcat` fails on `.gz` on macOS; `gzcat` works.

### Step 23b: VOC break set with ¬

**What changed.** New preset `voc_1.04.02_v6` (`VOC_1_04_02_V6`: v5sg with `wbc_pinned = {"default": "-=„¬"}`), 4 new tests, a rebuild of
the 567 inventories into `data/text_layer_voc_23b/1.04.02/NL-HaNA_1.04.02_text_layer_v6/` (a link to the drive,
`…/text_layer_voc/23b-a`). Build: `failures=0`. No library change apart from the preset. Data and scripts:
`reports/data/phase-17-voc-rules-v1/step-23b/`.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | new tests pass, suite green | 4 new tests pass; suite 1,443 collected; one run had `test_analysis_token_analysis::test_content_words_uses_non_content_words` fail while other sessions ran in parallel, and it passes alone (12 passed) |
| 2 | 567 built, audit `n_fail_judged = 0` | met: 567, `failures=0`; audit 10,074 pass, 143 fail, 0 judged. Keys that differ from 22a: see F17-23b.4 (`n_pass` 10,069 to 10,074, `n_fail` 148 to 143, `counts`, `layer`) |
| 3 | compare.json | (a) 0 of 5,670 files differ; (b) 68,534 changed units, 0 without `¬` in the 22a text; (c) other = 0 |
| 4 | bar: at least 57 of 60 `right` | **missed: 56 of 60 = 0.933, Wilson 95% 0.841-0.974** |
| 5 | protect pairs clean | `protect.py --skip 23` identical; `protect_voc.py` differs only in the new `data/text_layer_voc_23a/` and `step-23a/gold/` paths of the parallel step 23a |

Remaining `¬` in v6 join texts (22a: 116,920 in unit texts; the brief's 116,881 counts differently):

| class | n |
|---|---|
| `unit_end` (last segment of the unit) | 9,525 |
| `guarded` (703 `g_digit_right`, 40 `g_digit_left`, 21 `g_punct_left`, 15 `g_standalone`) | 779 |
| `not_line_final` (23 `plain`, 1 `g_punct_left`, 1 none) | 25 |
| `other` | 0 |
| total | 10,329 |

**Error analysis.** Read: 65 of the 80 sampled merge boundaries (seed 20261101, uniform over 22a-eligible v6 merges whose left line ends in
`¬`; read in order until 60 were judged, k = 1-65; 66-80 are reserves and not scored), all 20 sampled guarded boundaries, and 10 changed
units. Verdicts in `joins_read.tsv`, `guarded_read.tsv`, `changed_sample.tsv`.

Join verdicts (k = 1-65): 56 right, 4 wrong, 5 unclear (skipped; the scored 60 end at k = 65).

| wrong k | left | right | joined | why |
|---|---|---|---|---|
| 29 | "een huij¬" | "op de oostzijde staet" | "huijop de oostzijde" | two words, `¬` after a complete word |
| 33 | "Tobillo momo¬" | "Witsen" | "momoWitsen" | two words (name), stray `¬` |
| 57 | "dat zij aen¬" | "'t geen wij Echter" | "aen't geen" | two words, `¬` after a complete word |
| 61 | "Keyserlycke maij¬" | "nu comen te vertrecken" | "maijnu comen" | abbreviation (majesteit?) glued to the next word |

| unclear k | why |
|---|---|
| 19 | Portuguese-looking right line: "Noodtcem" or "Noodt cem" |
| 47 | "bij nae¬ oijt": "naeoijt" or "na nooit" |
| 50 | "subs¬ dato": "subsdato" or "sub dato" |
| 52 | right line itself ends in `¬` ("ge¬" / "ge¬" / "negen": "gegenegen", a doubled "ge") |
| 56 | HTR garble ("ese be¬ se cons") |

A `„` at the start of the next line was dropped correctly in all 9 scored cases that had one (k = 12, 14, 20, 31, 32, 39, 46, 60, 62). The right joins include long, hyphen-like splits of Dutch words ("Nage¬ / len", "gead¬ / viseert") and Portuguese
("valcan¬ / cames"). I also looked at reserves 66-80 (13 right, 1 unclear at k = 69, 1 wrong at k = 75: "freguatten¬" + "na volbragte" to "freguattenna", the same glued pattern), which are not scored: the bar was fixed on the first 60.

Guarded boundaries (20): 17 digit, 2 standalone mark, 1 digit-left; none should have joined with the next line. Typical: "tverc¬ 33¾", "touw¬ 2: man 12", "Janu¬ 1642 stijlo novo",
"Hoog ¬ Edelheid". In 4 (k = 9, 10, 12, 15: "ver¬ 400. strekt", "Comptoir¬ 2:6:-:- kens", "geree¬ 32:10. 6. 44. 4. kend",
"Gezond¬ 10 heijd") the word's second half sits on the line after an interleaved number: guarded joining is right, but the word stays broken.
The digit guard on `¬` is right for VOC, and the cost, a word left broken, is a reading-order matter.

Changed units (10, `changed_sample.tsv`): 9 correct joins ("gespro¬ ken" to "gesproken", "afge¬ veerdight", "voorko¬ „men" to "voorkomen",
"prefe¬ rerende"); 1 glued ("Jopin¬" + "soe uwe Majt" to "Jopinsoe", inventory 1107).

**Conclusion.** `¬` as a break mark is correct in about 93% of eligible joins, and the layer adds 106,569 merges. The remaining error is
a `¬` that follows a complete word or an abbreviation, which no existing guard sees; a lexicon or a "next line starts with a lower-case
function word" check might catch some, but that is not tested here (no retuning, per the brief). The audit moves only in join-related
keys.

### Step 23c: VOC folio map

**Status: dev and test done** (test run once, 2026-11-02T16:26:54Z; gold sha256 7411576...a7af verified before the run).

**What changed.** `FolioMapParams` gets `pick_by_chain`, `tie_keep` (+`tie_window`), `clean_tol`, `pieces` (+`piece_min`,
`piece_slope`), all off; `FolioMap.pieces`; `locate` falls back to the pieces when the main result is None or
`extrapolated`. Six new tests (29 in the module). Nothing else changed. `sg_check.json`: on SG inventory 3444, 0 differences
against `git show HEAD` in the chain, mode and `locate(0..1499)` under `FolioMapParams()` and `FOLIO_MAP_V2`.

**Dev set-up.** All 1,841 phase-16 GM anchors are dev; the 7 user corrections of 22b are applied (`dev_anchors.tsv`); 8 anchors
whose scan is not in the layer are dropped, 1,833 are scored. BASE on the uncorrected rows reproduces 22b: dev split 647/929,
test split 591/904. With the corrections BASE is 649/929 and 596/904. 48 cells, number tags `("page-number",)`. Selection
rule: max leaf share; within 0.002 of the max, fewest non-default options, then extrapolate 6.

**Dev results (leaf = gold in {front scan, next scan}).**

| Cell | non-default | leaf | share | strict | leave-one-out |
|---|---|---|---|---|---|
| all five, extrapolate 30 (`p1_tfirst_c3_g1_e30`) | 5 | 1313 | 0.7163 | 1059 | 0.884 |
| pick, first, clean, extrapolate 30 | 4 | 1312 | 0.7158 | 1058 | 0.884 |
| pick, first, clean, pieces | 4 | 1312 | 0.7158 | 1058 | 0.884 |
| **SEL** pick, first, clean (`p1_tfirst_c3_g0_e6`) | 3 | **1311** | **0.7152** | 1057 | 0.884 (179,594/203,113) |
| pick, no tie rule, clean, pieces, extrapolate 30 | 4 | 1295 | 0.7065 | 1056 | 0.886 |
| BASE (`p0_t0_c0_g0_e6`) | 0 | 1245 | 0.6792 | 1018 | 0.878 (169,641/193,305) |

SEL by side layout: openings 0.746 (BASE 0.704), single 0.704 (BASE 0.670). By GM kind: begin 616 of 918 (BASE 604), end 695
of 915 (BASE 641). By split (corrected): dev split 681 of 929 (BASE 649), test split 630 of 904 (BASE 596). Single options
against BASE: see F17-23c.2. The top four cells are within 2 hits; the selection rule picks the simplest.

**Test.** Run once on the user's image gold (`step-23a/gold/folio_gold.tsv`, 240 items): 109 `number` items are the anchors (all
`main`, none layered; 131 `none` items are not anchors). Leaf = gold scan in {front scan, next scan}. `select.json` (15:37:13Z, 1 Nov)
precedes `test_done.json` (16:26:54Z, 2 Nov).

| Cell | n | leaf | share | Wilson 95% | strict |
|---|---|---|---|---|---|
| BASE (`p0_t0_c0_g0_e6`) | 109 | 81 | 0.743 | 0.654-0.816 | 81 (0.743) |
| **SEL** (`p1_tfirst_c3_g0_e6`) | 109 | 79 | **0.725** | 0.634-0.800 | 78 (0.716) |

Strata (leaf hits, BASE / SEL): openings 17/23 and 17/23; single 64/86 and 62/86; kind main 81/109 and 79/109. By method of the
prediction: printed 70/84 and 70/86, interpolated 11/17 and 9/15, extrapolated 0/1 and 0/1, none (no scan found) 0/7 and 0/7.
Exact McNemar of SEL against BASE: b (BASE only) = 2, c (SEL only) = 0, p = 0.5. **Verdict: bar F (0.95) missed**, by 0.225
for SEL; the interval tops out at 0.800. This is a negative result: no retuning, and no `VOC_FOLIO_MAP_V1` constant was added. SEL does
not beat BASE on the image gold: its dev gain (+66 of 1,833) does not appear (-2 of 109, not significant either way). BASE
fresh (0.743, Wilson 0.654-0.816) is above its GM score (0.679) but the interval contains it, so the image gold does not
confirm a gap between the map and the image-truth: the GM miss rate (32%) is not larger than the image-gold miss rate, so GM's
convention is not what costs the map its score, and the map is far from 0.95; 28 of 109 BASE anchors (26%) miss, and 7 of them have no prediction at all.

**Test error analysis.** All 30 SEL misses read (`test_misses_read.tsv`, `context_test.txt`: page-number texts on gold +-3 scans and
around the prediction; the user's notes in `folio_gold.tsv` used where present).

| Class | SEL misses (30) | Examples (inv, folio, gold scan -> predicted scan) |
|---|---|---|
| HTR misread at the gold scan (review: was "or blank", 12) | 8 | 3793 f3661, 309 -> none (HTR 3067); 7978 f195, 155 -> none (HTR 145.); 1631 f935, 462 and f930, 457 (HTR 435, 430: 9 read as 4); 2222 f125, 6 (HTR 425.); 2954 f758, 799 -> 759 (HTR 778); 2954 f528, 393 and 2167 f33, 1101 (user unsure of the gold) |
| tied or duplicated number | 10 | 8840 f73, 81 -> 189 (73 on 81, 83, 189); 8466 f39, 251 -> 55 and f59, 273 -> 75; 7978 f44, 250 -> 403; 2167 f415, 1481 -> 435; 1631 f319, 339 -> 344 (319 printed twice); 2487 f160, 984 and f112, 644 -> none |
| several numbers on a side | 2 | 2487 f4657, 75 -> 115 (two numberings); 1723 f1184, 275 -> 267 |
| restart or second numbering (review: was 2) | 5 | 7978 f63, 267 -> 419 (extrapolated); 7978 f4, 360 -> 278; and three the step had as "blank at the gold scan": 2167 f69, 1139 -> 89 (67 at 1137, 71 at 1141), 7694 f31, 115 -> 39 (33 at 117), 8840 f39, 47 -> 155 (39 at 155): the gold scan's own number is missing in the HTR, its neighbours carry the local numbering, and the map answers from another numbering in the volume |
| unclear (gold or both disagree) | 4 | 7694 f57, 137 (neighbours say 143); 2047 f1, 396 (user: could be a speck); 1081 f202, 421 -> 423; 1631 f199, 157 -> 215 |
| side or layout offset (review: was 0) | 1 | 4325 f675, 333 -> 334 (gold blank in the HTR; 674 at 332, 676 at 335; interpolation lands one opening late) |
| gap, extrapolation, layered sheet | 0 | none of the misses; the one extrapolation miss is a restart |

Misses concentrate in a few volumes: 1631 (4; openings), 7978 (4), 2487, 2167, 2954 (3 each). The 8 HTR cases are a wrong digit
at the gold scan (4/9, 1/4, 5/4 confusions the user's notes also flag; 2 of them on golds the user marked unsure). Ties are the
largest map-side class: when a number appears on two distant scans (a second numbering or a repeated page), the map picks one;
neither "first" nor the chain choice can tell which one the folio is on without the image. Seven of the 30 misses have no scan
found at all, so no option of the map reaches them.

*Review (2026-11-02):* the step first classed 4 misses as "blank at the gold scan" (HTR). In three of them the neighbours carry
the local numbering (2167: 67 at 1137, 71 at 1141 around gold 1139), so plain interpolation would hit; the map misses because it
answers from another numbering that also contains the folio. They are restarts, and the fourth is a one-opening offset. Map-side
classes (ties 10, restarts 5, several numbers 2, offset 1) are 18 of 30, so a map that prefers the numbering local to its
neighbours is a real open rule; the HTR-only share is 8 of 30.

**BASE/SEL discordant anchors (2, both BASE right, SEL wrong).** `2954-0017` (folio 386 printed on 15 and 17; `tie_keep=first`
takes 15, BASE 17 is the gold) and `3793-1105` (3465 on 1103 and 1105; SEL 1103, gold 1105). Both are `tie_keep=first`. On the image
gold the tie rule is a coin toss that happened to fall against SEL twice; no anchor was fixed by any option. This agrees with the
dev finding that "first" only moves which copy the GM convention uses (F17-23c.3).

**None items.** 131 of 240 sampled items are `none` (no folio number): single-page volumes 124 of 210 (59%), openings volumes 7
of 30 (23%). They cluster in volumes: 9 of 10 in 1184 and 1390, 8 in 1081, 1723, 2379, against 1 in 1631 and 2222 (the openings
volumes) and 3 in 2047, 2954, 8466. *Review (2026-11-02):* in single-page volumes 111 of the 124 `none` items are even-numbered
scans, the versos of foliated leaves, which carry no number by design (the leaf measure counts them with their recto). Unnumbered
scans that should carry a folio are 13 single-page and 7 openings items (20 of 240, 8%), spread over 12 volumes (at most 5, in
4325). The many `none` items are the foliation, not a gap in it, and they cap nothing.

**Dev error analysis.** SEL fixes 135 anchors and breaks 69 against BASE. I read 15 of each (seed 20261101;
`misses_read.tsv`, `context_dev.txt`: page-number texts on gold ± 3 scans and around both predictions).

| Class | Fixes (15) | Breaks (15) |
|---|---|---|
| tied number | 5 (one a 4-fold tie, luck) | 9 |
| several numbers on a side | 6 | 1 |
| noisy chain, gap, cleaning | 2 | 4 |
| volume-start extrapolation | 2 | 0 |
| doubtful gold | 0 | 1 |

Fixes by "pick" are real: `1750` folio 945 sits on scan 757 as the second text (`['966', '945']`, 966 is a repeated header),
`3780` folio 6237 gets a chain of 154 points instead of 54 from the second candidates, `1085` folio 31 sits behind `['33.', '31.']`.
Fixes by "first" are mixed: `1188` folio 172 (`172.` at 367, repeat at 373) is real, but `1075` folio 102 (102 at 253 and 255, gold
254) and `1076` folio 26 are hits only because the gold sits between the copies. The breaks are the mirror image: `2940` folio 91
(91 at 195 and 197, gold 197), `3068` folio 1041 (861 and 863, gold 863), `2612`, `3970`, `3819`: the GM anchor sits on the later copy.
Cleaning breaks drop a correct interior point (`3948` folio 2432: neighbours 2633 and 2629 are noisy; `3561`, `3558`, `1117`: sparse
chains). `2425` folio 3175 is a fix by luck: scan 513, the gold, prints 3173. Conclusion: "pick" is a real improvement; "first" and
"clean" trade fixes for breaks at about the same rate and mainly move which copy of a repeated number the gold uses.

**Draft findings.** F17-23c.1 to F17-23c.6 (dev, on GM) and F17-23c.7 to F17-23c.11 (test, image gold); see the table.

### Step 23d: One-column TOC detector

**What changed.** New `index_pages.toc_side_features`, `toc_side_rule`, `toc_sides` and `TOC_SIDES_DEFAULTS`; `front_index_run` gains `min_after_frac` (default behaviour unchanged, checked against `HEAD` on 22 GLOBALISE flag lists and 3,000 random lists). Brief: [`step-23d-voc-toc-detector.md`](../plan/phase-17-voc-rules-v1/step-23d-voc-toc-detector.md). Harness and outputs: `reports/data/phase-17-voc-rules-v1/step-23d/` (`eval_toc17.py`). Bar D fails, so `VOC_TOC_SIDES_V1` and `VOC_FRONT_RUN_V1` were not added.

**Results.** Fresh labels: 1,404 scans in 24 volumes (344 `toc`, 1,060 `no`); 173 `no_text` and 2 `unclear` are left out.

| Criterion | Expected | Measured |
|---|---|---|
| Planning cell reproduces | TP 106 / FP 48 / FN 80 | 106 / 48 / 80 |
| `front_index_run` default unchanged | equal | 22/22 volumes, 0/3,000 random lists differ |
| SEL select time before test | yes | select 15:29:08Z, test 16:08:24Z |
| Bar D, SEL P | >= 0.95 | 0.583 (95/163; Wilson 0.506-0.656) |
| Bar D, SEL R | >= 0.90 | 0.276 (95/344; Wilson 0.232-0.326) |
| PH16 P / R | reported | 0.706 (84/119; 0.619-0.780) / 0.244 (84/344; 0.202-0.292) |
| McNemar SEL vs PH16 | reported | b 94, c 116, p 0.147 |
| Front runs, VOC_RUN / SG defaults | reported | dev 21/22 and 7/22; fresh 23/24 and 4/24 |

Dev grid (96 cells + PH16, `dev_grid.tsv`): best dev F1 0.6272 (window 45, toc_min 0.2, gap 4, title on, with or without the guard); SEL by the tie rule is the defaults, F1 0.6235; PH16 dev F1 0.212. SEL on fresh: F1 0.375. Recall by `list_type`: register 89/333 (PH16 84), title 6/11 (PH16 0). Recall by 23a's `loc` stratum: `loc=yes` 58/201 (PH16 82), `loc=no` 37/143 (PH16 2). Per-volume counts are in `test_done.json`: 2282 (4 TP, 83 FN), 2167 (5 TP, 49 FN) and 2487 (35 FP) dominate.

**Error analysis.**

- *What was read:* dev, 15 FN and 15 FP of 80 and 48 (seed 20261101); test, 30 FN of 249 and 30 FP of 68 (seed 20261101), first 5 lines and features each (`cases_dump.txt`, categories in `cases_read.tsv`). Population counts for the window and the `loc` strata come from `scan_eval.tsv`. Sample category counts below.

  | Category (test sample) | Count | Example |
  |---|---|---|
  | FN single locator at line end (long entries, dot leaders) | 8 | 2282_0034 `wegens de verlope wevers ... - - -. „ 223.` |
  | FN TOC beyond the window (2282, scan 62+) | 6 | 2282_0081 `kruijtkelders, oorzaek van het bederven ... . . „` |
  | FN dated summary register, no folios | 8 | 2167_0031 `Van Ceijlon onder dato 23:e Januarij 1730.` |
  | FN register with folio numbers apart | 3 | 2379_0008 `Pag: / Amboina / A:o 1736 / ontfangen` |
  | FN numbered enclosure list, no folios | 3 | 4325_0011 `A - No 5 Copia Authentieq Brieff onder den` |
  | FN resolution extract (prose-like) | 2 | 3260_0024 `gegeven, en ter resolutie van den` |
  | FP letter or prose (paragraph numbers, "register"/"inhoud" in text) | 15 | 2487_0021 `1638 / 1130. / En ten dien fine sal moeten in aan` |
  | FP near-empty side or front matter (gap fill) | 7 | 2282_0011 `4` (n 0) |
  | FP account or ledger | 1 | 4325_0055 `Montant de l'autre part ƒ19608: 14:—` |
  | FP other | 1 | 1184_0054 `1651 / adij 30: Januarij p„r` |

  Dev sample: FN 12 register with folio numbers apart (2448, 2548), 3 other; FP 6 letter or prose with a "Register" heading, 6 front matter or gap fill, 3 numbered or dated lists (see `cases_read.tsv`).
- *Guard:* blocks 1 side on dev, 0 on fresh (`guard_blocked_sides_fresh` in `test_done.json`).
- *Conclusion:* the detector reads the printed shape "folio range or date at the line start or end". The fresh registers are mostly something else: summaries with no folios on the line, enclosure lists, or entries ending in one locator. The gold's `loc=no` pages are 42% of the positives and a folio-based feature cannot find most of them by construction. The false positives come from prose whose lines are bare numbers and from the title cue, not from ledgers, so the amount guard is not what limits precision. A detector with a P and R above the bar needs a different signal (page-level layout and line-length profile, or position relative to the first resolution), not another cell of this grid. Why recall drops from 0.570 on dev to 0.276 on fresh volumes was not analysed per volume beyond the counts above (2282 and 2167 hold 132 of the 249 FN); the dev gold also allows up to 4 gold rows per scan collapsed with max, the fresh gold does not.

**Findings:** F17-23d.1 to F17-23d.6.

**Open issues.** The `front_index_run` constant `VOC_FRONT_RUN_V1` waits on a TOC detector that passes bar D; 23e must not assume one. `toc_side_features` accepts any `cfg` key from `TOC_SIDES_DEFAULTS`; no sibling uses it.

### Step 23e: Locator L9 and VOC links

**What changed.** `index_links.py`: `LocatorParams` gains `trail_pattern`, `trail_num_pattern`, `guard_before`, `guard_after`; `VOC_LOCATOR_PARAMS["L9"]` (connectors `à`, ` / `, 2-3 dots, `verso`, trailing ranges, a lone number after dashes) and `["L9g"]` (date and year guards); `monotone_mask`. L8 is byte-equal (`l8check.json`: 7,715 TOC lines of the 22 dev volumes, 0 differences against `git show HEAD`). Tests: `test_l9_connectors`, `test_l9_trailing_range_verso`, `test_l9g_guards`, `test_l8_unchanged`, `test_monotone_mask`. Harness: `reports/data/phase-17-voc-rules-v1/step-23e/eval_links17.py`.

**Dev (22 GLOBALISE volumes, document starts as scoring-only targets).** The six SEL22B rows of the planning prototype reproduce exactly (`c3_reproduction.tsv`, e.g. L8 1009/1851 and 790/1526; L9g + mono 901/1104 and 867/1526). The grid has 12 cells because SEL23C differs from SEL22B (`dev_grid.tsv`):

| Variant | mono | map | E (hits/links) | coverage | F1 |
|---|---|---|---|---|---|
| L8 (C3) | off | SEL22B | 1009/1851 = 0.545 | 790/1526 = 0.518 | 0.531 |
| L9 | off | SEL22B | 1239/2045 = 0.606 | 971 = 0.636 | 0.621 |
| L9g | off | SEL22B | 1199/1870 = 0.641 | 969 = 0.635 | 0.638 |
| L9 | on | SEL22B | 898/1112 = 0.808 | 866 = 0.567 | 0.667 |
| **L9g** | **on** | **SEL22B** | 901/1104 = 0.816 | 867 = 0.568 | **0.670** (SEL) |
| L9g | on | SEL23C | 901/1104 = 0.816 | 872 = 0.571 | 0.672 (best) |
| L8 | on | SEL22B / SEL23C | 683/918 / 681/918 | 689 / 691 | 0.562 / 0.562 |
| L8, L9, L9g | off | SEL23C | 993/1842, 1226/2035, 1184/1867 | 794, 978, 976 | 0.530, 0.621, 0.637 |

SEL is the cell within 0.005 of the best F1 with the fewest differences from C3 (2: variant, mono): L9g + mono + SEL22B. The 23c map was not chosen (F17-23e.7). `select.json` (16:12:09 UTC) precedes `test_done.json` (16:12:22).

**Results on the fresh gold** (21 volumes, 166 readable sampled lines of 168; 2 unreadable; 25 gold folios on 25 lines; Wilson 95%; the intervals are wide):

| Criterion | Bar | SEL (L9g + mono, SEL22B) | C3 (L8, SEL22B) |
|---|---|---|---|
| L precision | 0.95 | 10/18 = 0.556 (0.337-0.754) | 9/27 = 0.333 (0.186-0.522) |
| L recall | 0.90 | 10/25 = 0.400 (0.234-0.593) | 9/25 = 0.360 (0.203-0.555) |
| E | 0.90 | 9/17 = 0.529 (0.310-0.738) | 7/26 = 0.269 (0.137-0.461) |
| E, sure targets only | | 6/14 = 0.429 (0.214-0.674) | 4/23 = 0.174 (0.070-0.371) |
| E_map (gold folios with a found target) | | 18/21 = 0.857 (0.654-0.950); sure 10/13 | the same map |
| Exclusions (target not_found) | | 1 | 1 |

Both bars fail. With the `htr=` value substituted for the gold folio on the two HTR lines (2954, 3793), L is unchanged (TP 10, R 0.400): neither HTR string was predicted. By stratum (SEL): `loc=yes` pages hold 81 sampled lines (all 25 gold folios) and `loc=no` pages 85 (none); P on `loc=yes` 10/12 = 0.833, R 10/25; on `loc=no` 0 of 6 predicted numbers are right. End to end (23d's SEL cfg, `VOC_RUN`, SEL locators on predicted sides): a run was found in 21/21 volumes; 66 sampled lines lie on predicted sides (16 with a gold folio), E 8/8 (Wilson 0.676-1.000), P 9/9, R 9/25. Silver (OBP `folio_start`, 21 volumes, oracle sides): SEL 461/642 = 0.718 precision, 461/1088 = 0.424 recall; C3 449/858 = 0.523 and 0.413.

**Error analysis** (all cases are small enough to read; see `cases_read.tsv`, `errors_dump.txt`).
- *E misses (8 of 8 read):* not a folio 7 (item numbers at line start: "No 14.", "N„o 8.", "16. Originelen Eijsch", "12: Copia brieven", "22: Copia Actens", bare "401", "N: o 25."; 6 on `loc=no` pages), 1 a second number ("388 288", gold 388, which the monotone filter dropped, leaving 288). No miss is a map error: every predicted number that names a gold folio hit the right leaf (9/9; one more is excluded as not_found).
- *L false negatives (15 of 15 read):* range filter 5 (2282 folios 4 and 710 fall outside its map range 877-2491; 2538's 2358, 2496 and 2833 lie beyond the volume), pattern gaps 5 (four-dot connector in "3382. . . . 3384"; "447 „ „ 450" and "502 verso a„ 504" with `„` in the connector; "3081. - - - - - d:o ..." and "3198 - - - . ..." lead number followed by dashes and text), monotone filter 3 (773 in 2379, 45 in 7694, 388 in 2954), HTR 2.
- *Hits (9 of 9 read):* real: printed numbers with a range ("268 a 278", "773 a 796 verso", "408 verso a 415") and 3 "likely" targets (2 reached by extrapolation or interpolation).
- *Mono (10 locators dropped):* 3 gold (773, 388, 45) and 7 not (14, 90, 603, 5, 23, 2, and the year 1740). Without it SEL would be P 13/28, R 13/25 (post-hoc).
- *Conclusion:* L9's patterns work where a folio is written at a line end or in a range (all hits), but the sampled TOCs differ from the dev TOCs: most lines name no folio, many pages number their entries with item numbers, and some write folios in forms the dev text did not have. The map is not the limit here (E_map 0.857).

**Findings:** F17-23e.1 to F17-23e.7.

**Open issues.** (1) A page-level cue (the TOC page carries folios, `loc=yes`) would remove 6 of 8 E misses; it is a rule, not a tuning, and belongs to a later phase, with a four-dot connector and a lead-number-then-dashes arm. (2) With 25 gold folios the bars cannot be tested precisely; more fresh lines on `loc=yes` pages are needed for a decisive test. (3) `LocatorParams(variant="L9")` no longer raises; the existing unknown-variant test now uses `"L10"`.

## Review

Reviewed 2026-11-01 by Opus 5.5 ([REVIEW](../plan/phase-17-voc-rules-v1/REVIEW.md)). Verdict: **accept with
follow-ups**. 23a, 23b, 23d and 23e are final. The 23c test stage was reviewed in an addendum on 2026-11-02 (below).

**Re-run by the reviewer.**
- The suite: 1,432 passed, 11 skipped.
- The SG check on 3444: 0 differences. L8 is byte-equal on 7,715 lines.
- 23b `compare.py`: identical JSON. The 23b audit: 0 judged failures.
- The four gold hashes match.
- Bars D, L and E, recomputed from the gold: the same counts.
- Leakage:
  - 23d selected before the gold froze;
  - 23e's grid is dev-only;
  - the fresh volumes are disjoint from the 567;
  - no library code reads gold;
  - no VOC constant exists.

**Error analysis.**
- **TOC detector:** the reviewer stratified 23d's scan table by `loc=` and by the two long registers. The failure does
  not depend on how the gold defines a TOC page, and PH16 is better than SEL on registers that carry folios (F17-R.1).
- **`¬` joins:** the reviewer read all 80 drawn joins. One unclear row is wrong (F17-R.3), so 55/60. The bar fails
  either way.
- **Locators:** the 23e misses read as the step says. The step marks its post-hoc no-monotone number as post-hoc.
- **Link gold:** the bars could not be decided on 25 gold folios. That is a planning debt, not a step defect (F17-R.2).
- **Gold:** the reviewing session also ran the 23a readers and set the label rules after the reads. The rules are
  logged and definitional (F17-R.4).
- **Remaining `¬`:** most of those left in v6 sit at unit ends (F17-R.5).

**Process debts.**
- 23c rewrote this report with a script instead of Edit. Nothing was lost.
- Parallel steps pushed `STATUS.md` over its budget.
- Both are noted in `plan/CONTEXT.md` §5. No defects, so there is no review-fixes brief.

**Addendum: the 23c test stage (2026-11-02).** The user read the 240 crops (UA-061; 109 numbered, 131 none). The test ran
once, after `select.json`, on the hashed gold. The reviewer recomputed BASE 81/109 and SEL 79/109, the two discordant anchors
and McNemar p 0.5 from `test_eval.tsv`: the same. Protect diff empty; suite 1,432 passed, 11 skipped; no constant added.
- **Misses re-read:** 4 misses the step classed as HTR "blank at the gold scan" are 3 restarts and 1 offset (F17-23c.10,
  revised): map-side misses are 18 of 30, not 14.
- **`none` items:** 111 of the 124 single-page `none` items are versos of foliated leaves; the step's "capped" reading is
  withdrawn (F17-23c.11, revised). F17-23c.9 is reworded (not a paired comparison).
- **User-unsure golds:** 5 of BASE's 28 misses are on golds the user marked unsure; without them BASE would be 81/104 = 0.779.
  The bar fails either way.
- **Ties on image gold:** both discordant ties have the folio on the later copy, as GM's convention had it (F17-23c.3).
- **Process debt:** the `context` stage overwrote the dev `misses_read.tsv` and blanked the 23c reading's categories. The
  reviewer rebuilt it from the 23c session's own annotation script (rows identical; sha256 back to 0eedae4f…). The stage
  should not rewrite read files (CONTEXT §5).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F17-23c.10 | Folio map: answer from the numbering local to the query's neighbours (restarts, second numberings, distant ties) before a global chain; test on new image gold | `plan/CONTEXT.md` §4 |
| F17-23e.5, F17-23e.3 | Locators: a page-level cue (the TOC page carries folios) before a leading number is read; arms for four-dot connectors, `„` as a connector, and a lead number followed by dashes | `plan/CONTEXT.md` §4 |
| F17-R.2 | New link gold sized on usable yield: lines sampled on `loc=yes` pages, ≥ 60 gold folios | `plan/CONTEXT.md` §4, §5 |
| F17-23d.3, F17-R.1 | TOC detector: features for dated summary registers and enclosure lists, or a decision that `loc=no` lists are not index sides | `plan/CONTEXT.md` §4 |
| F17-23b.5, F17-R.3 | `¬` filler rule (lexicon or next-word check), tested on fresh join reads, as a new layer version (D17) | `plan/CONTEXT.md` §4 |
| F17-R.5, F16-22a.7 | VOC unit order (marginalia, catch-words; `¬` across unit ends) | `plan/CONTEXT.md` §4 |
| F17-23c.3 | Tied numbers: on the image gold both discordant ties put the folio on the later copy (2 cases); the image gold is too small to settle the rule | next folio-map phase |

## Reproduce

Planning (from the AS root; `<scratch>` is any writable scratch dir):

```
P=reports/data/phase-17-voc-rules-v1/planning
.venv/bin/python -I $P/draw_fresh.py
.venv/bin/python -I $P/wbc_probe.py
.venv/bin/python -I $P/cache_gm.py <scratch>/gm_inputs.pkl
.venv/bin/python -I $P/map_proto.py <scratch>/gm_inputs.pkl
.venv/bin/python -I $P/offsets.py <scratch>/gm_inputs.pkl
P17_CACHE_DIR=<scratch> .venv/bin/python $P/toc_proto.py
P17_CACHE_DIR=<scratch> .venv/bin/python $P/front_proto.py $P/toc_proto.py
.venv/bin/python -I $P/l9_proto.py
```
