# Phase 12 report: Hand and print by line-box height

> **Status:** final (phase review, 2026-10-25, steps 15u–15w; post-review addenda 15wr and 15x, both reviewed
> 2026-10-26).
> **Phase plan:** [`plan/phase-12-hand-print-box-height/PLAN.md`](../plan/phase-12-hand-print-box-height/PLAN.md).
> **Code state:** unreleased after AS v0.14.1 (15q–15x; v0.15.0 recommended now); no sibling code.
> **Last updated:** 2026-10-26, review of step 15x (Opus 5.5).

## Summary

The absolute line-box height of a volume tells printed from handwritten SG volumes. A cut of 87.75 px was fitted
on 76 labelled volumes and frozen. It then agreed with the EAD silver label on 60 of 60 fresh test volumes in the
five silver series (F12-15u.1). With it, the text layer v4 decides hand or print per volume from the volume's own
PageXML. On all 198 local volumes it reproduces the 168 reference values exactly (F12-15w.1), and 50 of the 51
volumes within 10 px of the cut have the right mode (F12-15w.4). The one error, and the risk for a full run, is
handwritten **index** volumes. Their small hand reaches 86.5 px (3463 is built as `print`), and the corpus has 251 of
them (F12-R.5). Choosing the right mode is not enough: the print rules carried over from 1.10.94 fit only the
printed resolutions. In the single-column print series (*nouvelles*, *plakkaten*) the merged-line splitter cuts
about a million ordinary lines in two (F12-R.4). In the printed resolutions the unit rules cut paragraphs after
their drop-cap lines (F12-15v.4, F12-R.3, F12-R.6). The REPUBLIC crosswalk cannot validate 1.01.02 (0.5% of lines
match, F12-15v.5). The run kit and a discovering audit are ready (F12-15w.3, F12-15w.8). The full run should wait for
the two fixes: step 15wr (split gate) and step 15x (drop-cap-aware unit rule). *Addendum (15wr review, 2026-10-26):*
the split gate is done and reviewed. It turns the splitter off in the 53 single-column print volumes and changes
nothing else (F12-15wr.1–3); 15x is next. *Addendum (15x review, 2026-10-26):* the drop-cap rule is done and
reviewed. In the printed resolutions it removes 22.9% of the paragraph starts; 98% of the removed cuts were lines beside
a dropped initial (59 of 60 sampled, 206 of 210 on the read scans), 19 of 20 pages read closer to the printed paragraphs,
none worse, and 99.3% of the decision and *Dat* starts survive (F12-15x.1–3, F12-R.12). In the single-column series the
rule also helps on readable text (*plakkaten* 27 of 28 right, *nouvelles* 30 of 36); its error there is merging letter
parts such as closing formulas and signatures in the *nouvelles* (F12-R.13, F12-R.14). Whether to keep it there is
UA-029. Nothing else blocks the full run (UA-024).

## Findings

One row per finding. The ID is `F12-<step>.<n>`. Planning findings are `F12-P.<n>`, the review's
are `F12-R.<n>`. IDs never change and are never reused. A finding that turns out to be wrong is
marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F12-P.1 | The *nouvelles* silver group is the EAD series *Gedrukte nouvelles van Nederlandse gezanten in den vreemde*: printed reports of Dutch envoys, so letter-like text ("Hoog Mogende Heeren, Ik heb …") is expected in printed volumes (supports F10-R.4) | data-property | sg, ead, hand-print, silver | confirmed | [Planning](#planning) |
| F12-15u.1 | The stop rule passed: with the frozen cut 87.75 the box-height mode agrees with silver on 60/60 test volumes (every stratum 10/10 or 20/20; Wilson 95% lower bounds 0.72 for n=10, 0.84 for n=20); weighted agreement 1.000 for `print` and 1.000 for `hand` against the 0.90 bar | result | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-15u.2 | The fit reproduces: 76/76 values equal the planning `h_w` (max diff 0.0), max print 86.0 (12135), min hand 89.5 (3492), cut 87.75. On the test set the print volumes lie at 65.0-86.5 and the hand volumes at 100.0-182.0, so the gap is wider on the hand side (cut-to-lowest-hand 12.25 px) than on the print side (cut-to-highest-print 1.25 px, volume 12122) | result | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-15u.3 | The nouvelles and plakkaten volumes are the close calls: 6 of the 20 print volumes in those strata have margin -5 to -1.25 px (12123, 12122, 12087, 12235, 12166, 12203); all 6 read as printed. Their scan-level values scatter widely (19 of 60 test volumes have `scan_minority_share` > 0.2, all nouvelles/plakkaten, 0.23-0.50), because printed letters and edicts mix several type sizes | data-property | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-15u.4 | The minority scans are printed, not handwritten: of 95 minority scans read from 19 volumes, 89 are printed text in a larger type or another typeface and 6 are OCR junk on decorative large type (h 140-180 px); none is handwritten. A scan-level box-height label is therefore not a hand/print label; only the volume-level weighted median is (consistent with F10-R.3) | negative-result | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-15u.5 | Page width varies: 16 of 30 test print volumes have a median `page_w` (3950-4614) below the fit print range (4621-4752; 16 fit volumes with a scan table: dev, 455, 456), all nine sampled plakkaten at 3950-4110 px; they still sit at 74.5-85.5 px. `page_w` mixes page size and scan DPI (the plakkaten are landscape sheets, `page_h` about 2,700), so this is not evidence of a lower DPI; the PageXML gives no DPI, and volumes from another scan campaign are untested | data-property | sg, hand-print, box-height, resolution | revised | [Step 15u](#step-15u-box-height-test), [Review of 15u](#review-of-step-15u) |
| F12-15u.6 | All 20 diagnostic (non-silver) volumes get `hand` (111.0-173.0 px, margins +23.25 to +85.25). No predicted-`print` volume exists to check; the 3 lowest were read (2 handwritten, 1 numeric table). The diagnostic therefore tests hand recall only, not print recall, outside the silver strata | result | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-15u.7 | Comparison only: the 15q `hrel` rule (1.529/1.534) disagrees with the box-height rule on 15 of 60 test volumes (13 silver-`hand` volumes get `print`, 2 nouvelles volumes get `hand`) and matches silver on 45/60, against 60/60 for the box-height rule | result | sg, hand-print, box-height | confirmed | [Step 15u](#step-15u-box-height-test) |
| F12-R.1 | The rule depends on a common scan scale, not on page size: divided by median page width, box height no longer separates the classes (test print 13.98-21.32 and hand 17.06-33.80 per 1,000 px of width), so normalising by `page_w` would break it and a scan campaign at another DPI could shift every value (exploratory, post hoc) | negative-result | sg, hand-print, box-height, resolution | confirmed | [Review of 15u](#review-of-step-15u) |
| F12-R.2 | Expected errors at full scale are few but not zero: a normal-tail estimate per stratum (fit + test pooled, n = 20 each; exploratory) puts about 5 of the 783 silver volumes on the wrong side, mostly early *eerste serie* volumes (mean 123.2, sd 19.1, P = 0.03, about 4 of 123) going to `print`, and about 0.5 each of *nouvelles* and *plakkaten* going to `hand`. Revised in the phase review: the error seen at scale is a different class, handwritten *index* volumes at 86.5–97 px (F12-R.5), which the per-stratum estimate does not model | data-property | sg, hand-print, box-height | revised | [Review of 15u](#review-of-step-15u), [Review](#review) |
| F12-R.3 | Neither unit rule set fits the printed 1.01.02 volumes: the print rules cut a paragraph after its drop-cap lines, but they separate the *WAER op gedelibereert* decisions and *Dat …* clauses that the hand rules lump with the next resolution (4 pages of 3771 and 3822 read). A drop-cap-aware print rule is the likely fix; one mode per volume still holds | open-issue | sg, print, units, print-rules | confirmed | [Review of 15v](#review-of-step-15v) |
| F12-15v.1 | The pre-pass reproduces 15u exactly: for all 16 pilot volumes `mode/<inv>.json` has the same `value`, `n_chars` and `n_regions` as `fit_volumes.tsv`; the six printed 1.01.02 volumes get `print` (66.0-69.0 px), the eight others `hand` (124.0-165.0 px), 455 = 75.0 and 456 = 82.5 (`fixed`/`print`, `box_height_mode` `print`) | result | sg, hand-print, box-height | confirmed | [Step 15v](#step-15v-volume-mode-build-v4) |
| F12-15v.2 | The v4 build changes nothing it should not: for the 8 hand-mode inventories and for 455/456 every per-inventory file equals the v3 build (`15m-a`) on decompressed bytes (131 + 48 files equal); only the `params.json` form and the corpus-level entries of the six printed volumes differ. The six printed volumes differ in 93 files and have 18 new ones (`split_model/`, `split_stats/`, `units/republic_lines/`), as intended | result | sg, hand-print, v4 | confirmed | [Step 15v](#step-15v-volume-mode-build-v4) |
| F12-15v.3 | The merged-line splitter has almost nothing to do in the printed 1.01.02 volumes: 0-218 candidates per volume (3771: 218; the other five: 0-3) against 11,515 (455) and 64,299 (456); all 218 in 3771 lie on scans 0300-0336 (the two-column tariff pages, 215 of them on 0329-0336) and all 20 sampled splits are right (col 1 then col 2, the right half starts at the second column). Revised in the phase review: this holds for the two-column *gedrukte resoluties* only; in the single-column *nouvelles* and *plakkaten* the splitter cuts about a million ordinary lines (F12-R.4) | data-property | sg, print, merged-lines | revised | [Step 15v](#step-15v-volume-mode-build-v4) |
| F12-15v.4 | The print unit rules over-segment the printed 1.01.02 volumes: units per page 24.9-25.9 (hand rules) become 43.9-45.5 in 3771-3773 and 43.6-45.1 become 65.6-70.1 in 3821-3823; the median `main_text` unit goes from 8-10 lines to 2-3. Of 10 pages of 3771 read side by side, 8 are worse, 2 mixed, 0 better: a paragraph with a two-line drop-cap indent is cut after line 1 or 2 (`print:region_start` + `print:indent`), e.g. "IS ter Vergaderinge gelesen de Requeste van Dorpmeesters ende Sesmannen van Hou. / pertingen, houdende ..." | negative-result | sg, hand-print, units, print-rules | revised | [Step 15v](#step-15v-volume-mode-build-v4), [Review of 15v](#review-of-step-15v) |
| F12-15v.5 | The REPUBLIC crosswalk is not usable for the printed 1.01.02 volumes: of 40,750 REPUBLIC line ids, 0 match exactly, 204 (0.5%) match near/split and 40,546 are unmatched (455: 27,033 exact of 29,148). The matches are right as lines (20 of 20 sampled sit in the same REPUBLIC paragraph, all `end` lines), but a unit equals the paragraph in only 2 of 20, and coverage is 0.5%: below the 18/20 bar on the strict reading, and unusable as an evaluation set whatever the reading | negative-result | sg, print, republic-crosswalk | confirmed | [Step 15v](#step-15v-volume-mode-build-v4) |
| F12-15v.6 | The REPUBLIC line boxes for 1.01.02 come from a different line detection than the AS PageXML: in 10 sampled unmatched ids the nearest AS line has the same text in 8 cases but a box 17-71 px away (sum of four coordinate differences, tolerance exceeded), in 2 cases a different line (176-184 px). Geometry matching therefore cannot work for 1.01.02, whereas it works for 455/456 | data-property | sg, print, republic-crosswalk | confirmed | [Step 15v](#step-15v-volume-mode-build-v4) |
| F12-15w.1 | The v4 build from scratch on all 198 local volumes reproduces the 168 reference box heights exactly (168/168 equal; no unequal, none missing) and gives 111 `hand` and 87 `print` (85 `box_height` in 1.01.02, 455/456 `fixed`); the 168 are 96 `hand`, 72 `print` (70 + 2) as planned; the 30 reserves split 15 `hand` (88.0 and 117.0-171.0 px) and 15 `print` (65.5-87.0) | result | sg, hand-print, box-height, v4 | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.2 | `15w-a` equals `15v-a` on the 16 shared inventories (258 + 55 files equal on decompressed bytes): the differences are the source path (14 `extract_stats`, 8 `scans.member`), corpus-level files that depend on the inventory set (`wbc/` 7 files, layout/types/units `stats.json`, `units/params.json` `republic_dir`) and, as intended for the six printed volumes built without a crosswalk, 6 absent `republic_lines/` files and the `republic_crosswalk*` keys of `units/stats.json`; no unit, segment or join table differs | result | sg, v4, reproducibility | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.3 | The new audit, which discovers its inventories, passes all 3,289 non-share invariants on `15w-a` (198 volumes, 1,107 v4 mode invariants among them) and reproduces 178 pass / 4 fail on `15m-a` with the same four lower-case merge shares (3351, 3823, 455, 456); the share is 0.5 to 1.0 (median 0.968) and below 0.95 in 75 of 198 volumes (49 of 87 `print`, 26 of 111 `hand`), reported only. The audit is structural: it passes on a layer with about a million wrong splits (F12-R.7) | result | sg, audit, v4 | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.4 | Of the 51 volumes within 10 px of the cut, 50 have the right mode and 1 is wrong: 3463 (86.5 px, silver `hand`) is a handwritten register read as `print` (5 of 5 scans handwritten); the other three handwritten volumes in the band (3478 at 88.0, 3492 at 89.5, 4617 at 97.0) are `hand`, 3478 by 0.25 px. All 47 other band volumes (78.0-87.0 px) are printed on every scan read (126 scans) | result | sg, hand-print, box-height | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.5 | Handwritten registers of resolutions (small, regular hand) sit at 86.5-97 px, inside the print range of the nouvelles and plakkaten (78-87 px): the cut separates them by margins of -1.25, +0.25, +1.75 and +9.25 px, so about one in four such volumes is expected on the wrong side. Only one `print` volume lies outside the printed series (3463, wrong) next to 9486 (no silver, printed *Processtukken*, right); no other volume of the 198 is `print` without silver `print`. Revised in the phase review: the four are *index* volumes, not registers of resolutions (EAD titles "1740 index", "1745 index", "1748 index 1e halfjaar", "Index"). Two of them (3463, 4617) were staged because they were contradictory (UA-017), so "one in four" is not a rate; F12-R.5 estimates the full-run count | negative-result | sg, hand-print, box-height | revised | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.6 | The one-mode-per-volume limit holds on the sample: of 160 minority scans read in 32 volumes with `scan_minority_share` > 0.2, 135 are printed pages in a larger type or another print run, 15 are the smaller-hand pages of the three handwritten volumes, 4 OCR junk, 6 unclear; no handwritten slip inside a `print` volume was found. The 6 unclear (12168, 12169, 12193, 12194 x3) are register-style titles at 100-160 px, probably a handwritten table of contents | data-property | sg, hand-print, minority | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.7 | The print split and unit rules work moderately on the plakkaten and nouvelles: of 20 units (4 volumes, seed 19) 10 are ok, 3 have a bad unit boundary (2 interleave the lines of two columns in one unit, 12088; 1 one-line unit ending in a comma, 12166), 7 are unclear (garbled or fragmentary text); none is a bad split. Revised in the phase review: the two 12088 "interleaved columns" units are half-lines left by wrong splits, and the splitter is wrong throughout the single-column print series (F12-R.4). The "garbled or fragmentary" units are probably the same | open-issue | sg, print-rules, units | revised | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.8 | Cost of the v4 build (`15w-a`, 2 workers): 1.01.02 25.3 GB XML, 2.75 GB output (0.1115 per XML byte), 7,419 worker-seconds, 5,340 s wall; 1.10.94 1.01 GB XML, 87 MB, 472 worker-seconds. Per XML byte `print` volumes cost 4.17e-7 worker-seconds and `hand` ones 1.40e-7 (3.0 x); the extract stage with the mode pre-pass is 80% of the work. A full 1.01.02 run of about 400 GB XML is predicted at about 42 GiB output and 67,000 worker-seconds (2.3-7 h with 8 workers). Revised in the phase review: the print rate includes the bigram splitter on about a million wrongly split lines (F12-R.4); 15wr re-measures it | result | sg, v4, cost | revised | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-15w.9 | The lower-case merge failures are not v4-specific: of 364 offending merges (5 per volume of the 75), 349 have an upper-case and 15 a punctuation character on the right (`&`, `(`, `:`); read examples are capitalised nouns after a broken word ("Esquader Fregatten onder com- / Capitein ...") and sentence starts, in hand and print volumes alike | data-property | sg, audit, word-breaks | confirmed | [Step 15w](#step-15w-v4-dev-build-audit-run-kit) |
| F12-R.4 | The v4 print rules carry the 1.10.94 merged-line splitter, which assumes two columns per page side. In the single-column print series of 1.01.02 it cuts ordinary lines in two: *nouvelles* 885,967 splits (median 70.9 per scan, about 43% of lines), *plakkaten* 169,527 (median 15.9 per scan), 9486 4,745 (47%), against 617 in the 31 *gedrukte resoluties*. 46 of 46 sampled splits in 8 volumes cut a continuous sentence, and the halves land in different layout columns, so units hold half-lines. The share of lines starting past 0.4 of their side's text span separates the layouts: 0.496–0.501 (*gedrukte resoluties*) against 0.005–0.225 (the 53 others) | open-issue | sg, print, merged-lines, v4 | confirmed | [Review](#review) |
| F12-R.5 | The handwritten volumes near the cut are *index* volumes: 3463 (86.5 px, `print`), 3478 (88.0), 3492 (89.5), 4617 (97.0) are all indexes (EAD), the 14 local index volumes lie at 86.5–137.0 px and no non-index hand volume below 100.0. The 1.01.02 EAD has 251 index-like titles (127 *tweede serie*, 115 *secrete*). A normal-tail estimate from the 11 index volumes not selected as contradictions (mean 116.7, sd 16.2) puts about 9 of them on the `print` side in a full run (exploratory) | data-property | sg, hand-print, box-height, ead | confirmed | [Review](#review) |
| F12-R.6 | Drop-cap cuts dominate the print cuts of the printed resolutions: in 3771, 3822, 3769, 3805 and 3864, cuts on the `indent` cue alone 1 or 2 main lines after the previous cut are 23,991 of 53,464 (44.9%; d=1 16,710, d=2 7,281, d=3 367), and only 53 of the 4,687 cuts at *WAER op* / *Dat* / *IS na* lines are of that kind (1.1%). So suppressing them should keep the separations of F12-R.3 (design measurement for 15x, today's cuts only) | data-property | sg, print, units, print-rules | confirmed | [Review](#review) |
| F12-R.7 | A structural audit is not evidence that text reads: the 15w audit passes all 3,289 non-share invariants on a layer in which about a million lines are cut wrongly (F12-R.4). When rules move to a new series, each stage's output needs reads per series | method | sg, audit, error-analysis | confirmed | [Review](#review) |
| F12-15wr.1 | The split gate separates the series with no overlap: all 31 *gedrukte resoluties* (0.496-0.501) and 3463 (0.415) have column-start share >= 0.35 and are split; all 25 *nouvelles*, 27 *plakkaten* and 9486 (<= 0.226) are not: 53 gated, 32 split, no volume in 0.23-0.41, shares within 0.008 of the review's table (largest gap 12162: 0.0234 against 0.031; the step said 0.001, revised in the 15wr review) | result | sg, print, split-gate | revised | [Step 15wr](#step-15wr-split-gate-for-single-column-print-volumes-review-fix) |
| F12-15wr.2 | The pre-registered read bar passed: 19 of 20 scans (>= 18 needed) have an in-order main unit in `15wr-a` (no half lines, no mixed columns, 0 backward jumps in y); the 20th (12226, a plakkaat) is OCR-garbled and counts as unclear. The 15wr review re-read all 20 units in full (the step had read the first 14-16 lines): 19 in order, and 12226 also reads in order through its word breaks ("han Ben- / 16 Detember 1757"), so the bar holds | result | sg, print, split-gate, error-analysis | confirmed | [Step 15wr](#step-15wr-split-gate-for-single-column-print-volumes-review-fix) |
| F12-15wr.3 | The gate removes 2,120,486 half-line rows (1,771,940 + 339,056 + 9,490 `split_from` rows, i.e. about 1.06 million splits; the step wrote 2,121,000) and changes nothing elsewhere: hand volumes, 455/456 and 1.10.94 are byte-identical to `15w-a`, the 31 resoluties and 3463 differ only by the `split_gate` key in `mode/<inv>.json` | result | sg, print, split-gate | revised | [Step 15wr](#step-15wr-split-gate-for-single-column-print-volumes-review-fix) |
| F12-15wr.4 | The print cost drops: 1.01.02 print rate 4.173e-7 to 2.402e-7 worker-s per XML byte (-42%), the 1.01.02 total 7,419 to 4,990 worker-s; the full-run estimate for 10% print falls from 1.67e-7 to 1.50e-7 (-11%), since the full run is mostly hand | result | sg, cost | confirmed | [Step 15wr](#step-15wr-split-gate-for-single-column-print-volumes-review-fix) |
| F12-R.8 | The reviewer's re-run reproduces 15wr: 53 gated / 32 split from `mode/<inv>.json`; hand (2,109 files), 1.10.94 and 455/456 byte-identical to `15w-a` on decompressed bytes; the 32 split volumes differ in `mode/<inv>.json` only, by the `split_gate` key; 0 `split_from` in 1,947,176 gated lines; corpus `wbc` rows equal outside the 53, `default_set` and `pinned` unchanged; 5 more lines that `15w-a` split (seed 20261030, split lines only) are whole and read right | result | sg, print, split-gate | confirmed | [Review](#review) |
| F12-R.9 | 3463 (handwritten index, built `print`) keeps the splitter: its index pages have two columns (share 0.415), and its 106 splits look like the old single-column ones: of 12 read, 8 cut one phrase ("Rug: Pissars, om Ocbroy tot ‖ het opregten"), none is clearly right, 4 unclear. The gate cannot catch a two-column hand volume; the P21 pin to `hand` (UA-028) removes these splits | open-issue | sg, print, merged-lines, hand-print | confirmed | [Review](#review) |
| F12-R.10 | The half lines had also distorted the derived word-break sets: in `15w-a` 31 of the 53 gated volumes' per-char WBC sets lacked `-` (22 had only `=`), in `15wr-a` all 53 have `-=„` (4 add `.`, as do 20 of the 31 *gedrukte resoluties*). The build joins with the pinned set `-=„`, so the join used the right set throughout; the derived sets are diagnostics | data-property | sg, print, word-breaks | confirmed | [Review](#review) |
| F12-15x.1 | The pre-registered bars all pass: A 59 of 60 dropped cuts continue the paragraph (0.983, bar 0.90), B 37,897 of 38,176 decision/*Dat* unit starts survive (0.9927, bar 0.97), C 19 of 20 pages better, 1 same, 0 worse (bar worse <= 2, better >= 12); the stop rule did not fire | result | sg, print, units, drop-cap | confirmed | [Step 15x](#step-15x-drop-cap-aware-print-unit-rule) |
| F12-15x.2 | The rule (`drop_cap_lines=2`, indent-only, print-mode 1.01.02) removes 179,412 `main_text` unit starts: 126,228 of 551,021 (22.9%) in the 31 *gedrukte resoluties* (units per scan 61.12 to 52.49, median lines per unit 3 to 5; 3771 46.05 to 35.21), 34,385 in the *nouvelles*, 18,572 in the *plakkaten*, 165 in 3463, 62 in 9486; hand volumes, 455/456 and the corpus files other than params/stats/wbc are byte-identical to `15wr-a` | behaviour-change | sg, print, units, drop-cap | confirmed | [Step 15x](#step-15x-drop-cap-aware-print-unit-rule) |
| F12-15x.3 | The 279 lost decision/*Dat* unit starts (0.7%: 136 *IS na/goet*, 77 *WAAR op*, 66 *Dat*) are clauses that follow a one- or two-line entry, a page-top tail or a junk line directly; the window cannot tell them from a drop-cap line, and the 20 page reads found the same pattern (3849 0257 "En hier op gedelibereert zynde", 3816 0355 "WAAR op" after "4080", 3795 0360 signature "Geteeckent, / C. Voet van Winsen.") Revised in the 15x review: the signature is one signature, so that merge is right; of 30 lost starts read (seed 20261032), 10 are the decision of a short pass entry merged with its 1–2-line request heading (one entry, one unit), 8 a decision glued to the 1–2-line tail of the previous entry at a page or column top, 7 *Dat* sub-clauses glued to the previous clause (1 a right merge), 5 a clause after a junk or number line ("4080", harmless) | open-issue | sg, print, units, drop-cap | revised | [Step 15x](#step-15x-drop-cap-aware-print-unit-rule) |
| F12-15x.4 | Outside the validated series the rule is unproven: of 10 dropped cuts per series 7 are right in the *nouvelles* (wrong: a dateline after a signature, a closing formula, an unreadable line) and 6 in the *plakkaten* (all 4 wrong are unreadable HTR or table junk); the brief's trigger (< 7) fired for the *plakkaten* (UA-029). Revised in the 15x review: the pre-registered result stands, but the trigger fired on unreadable input, not on wrong merges; 30 more reads per series (F12-R.13) give readable precision 27/28 (*plakkaten*) and 30/36 (*nouvelles*) | negative-result | sg, print, units, drop-cap, nouvelles, plakkaten | revised |
| F12-R.11 | The reviewer's re-run reproduces 15x: AS 1,240 passed, 11 skipped; LotA 381 passed, 1 xfailed; own decompressed-byte compare against `15wr-a` (hand, 455/456, 1.10.94 and all 594 `mode/` files equal; 85 × 7 print files differ; corpus `units/params.json` gains `drop_cap_lines` only for print); 179,412 suppressed cuts from my own code; bar A sample identical to `sampleA.tsv` under seed 20261029; bar B 37,897 / 38,176 on segment and whole-line text; diagnostic draws identical; audit 3,592 / 33 / 0 judged | result | sg, print, units, drop-cap | confirmed | [Review](#review) |
| F12-R.12 | Bar A holds for the whole population of the read scans: of all 210 suppressed cuts on the 20 scans, 206 are right (0.981): 3 wrong (3849 0257 "En hier op gedelibereert", 3816 0355 "WAAR op" after the junk line "4080", 3828 0195 "Signee," after a French dateline) and 1 unclear (3774 0281 "En prenant connoissance") | result | sg, print, units, drop-cap, error-analysis | confirmed | [Review](#review) |
| F12-R.13 | In the single-column print series the rule helps on readable text: 30 more suppressed cuts per series (seed 20261031) read *plakkaten* 21 right, 1 wrong, 8 unreadable input; *nouvelles* 23 right, 4 wrong, 3 unreadable. With the diagnostic, readable precision is 27/28 (*plakkaten*) and 30/36 = 0.83 (*nouvelles*). The *nouvelles* errors are letter parts beside an indent: closing formula ("Waar mede, &c."), signature after the dateline ("Getee- / kent"), "Art. 1.", a new paragraph after a broken line. Merging unreadable fragments is neutral | result | sg, print, units, drop-cap, nouvelles, plakkaten | confirmed | [Review](#review) |
| F12-R.14 | The extra reported audit failure (12118, lower-case merge share 0.9541 → 0.9496) comes from the rule: 377 new word-break merges at suppressed cuts, 0 lost; of the 60 not lower-case, about 33 are letter parts that the rule merged and the join then glued across a break mark ("Getee\|Waar meede, &c.", "ingekoomen\|Waar meede", "Rec. 31"), 12 capitalised nouns after a real break ("GuldenVlies"), 15 junk. A wrong merge in the *nouvelles* can therefore also glue words in the joined text | data-property | sg, print, units, drop-cap, word-breaks, nouvelles | confirmed | [Review](#review) |
| F12-R.15 | Counting `unclear` as `wrong` mixes input quality with rule error: all four *plakkaten* "failures" that fired the UA-029 trigger were unreadable HTR or table junk. Read bars on noisy series need a separate `unreadable input` verdict, reported apart and excluded from the precision denominator, fixed before reading | method | error-analysis, reads | confirmed | [Review](#review) | [Step 15x](#step-15x-drop-cap-aware-print-unit-rule) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Planned 2026-10-23 (Opus) from UA-022 (a) and the phase-10 review: the absolute line-box height separated all 76
labelled volumes seen in phase 10 (print 65.0–86.0 px, hand 89.5–173.0 px; F10-R.3), but post hoc. This phase fits
one cut on those 76 volumes (user decision, 2026-10-23: dev, 455/456 and the 60 spent phase-10 sample volumes) and
tests it once on a fresh random sample: 60 silver volumes in the five phase-10 strata and 20 non-silver volumes as a
diagnostic, drawn with seed 20261212
([`planning/test_sample.tsv`](data/phase-12-hand-print-box-height/planning/test_sample.tsv),
[`draw_sample.py`](data/phase-12-hand-print-box-height/planning/draw_sample.py)). Rule, cut procedure (planning value
87.75) and stop rule (stratum-weighted agreement ≥ 0.90 per silver class) are fixed in the phase `PLAN.md` (P1–P5)
before any test geometry exists. The v4 build steps (15v, 15w) are written only if the test passes.

After the pass (2026-10-25, Opus), 15v and 15w were planned from the unrun phase-10 briefs 15r/15s. The v4 build uses
the frozen cut 87.75 px on the volume's raw pre-pass rows, so its values must equal the 15u values exactly (16 in 15v,
168 in 15w). From those values, the 198 local volumes are expected to give 70 printed 1.01.02 volumes among the 168
with a reference value. 40 volumes lie within ±10 px of the cut and are read in 15w, together with every `print`
volume outside the silver print series (3463, 9486). Before any build, the REPUBLIC crosswalk for printed 1.01.02
volumes gets a fixed bar of 18/20 correct matches (plan decisions P9–P14).

### Step 15u: box-height test

**What changed.** Additive API in `archival_structures/analysis/hand_print.py` (`BoxHeightParams`, `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`, `BOX_HEIGHT_METHODS`) with 6 new tests; the harness [`validate.py`](data/phase-12-hand-print-box-height/step-15u/validate.py) (`fit`, `prepass`, `evaluate`) and [`analysis.py`](data/phase-12-hand-print-box-height/step-15u/analysis.py) (error-analysis reads). Brief: [`step-15u-box-height-test.md`](../plan/phase-12-hand-print-box-height/step-15u-box-height-test.md). Order of work as pre-registered: library, fit, freeze (`cut.json`, sha256 `ce58c3338549239f80fe891803091d08cfff2a3ff8f3e2ba4bcf5caf83ccb572`, written 16:48:30, before `data/hand_print_geom/15u-a/` existed), test set, diagnostic set. No rule, cut, feature or threshold changed after the freeze. No substitutions were needed (all 60 + 20 sample tars opened; 0 parse failures).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | new tests pass, suite green | 13 passed in `test_analysis_hand_print.py`; full suite 1,215 passed, 11 skipped (1,226 collected = 1,220 + 6) |
| 2. Fit reproduces | 76 values = `h_w`, max print 86.0 (12135), min hand 89.5 (3492), cut 87.75, 76/76 | all 76 equal (max abs diff 0.0); 86.0 (12135), 89.5 (3492), 87.75, 76/76; 455 = 75.0, 456 = 82.5 |
| 3. Freeze order | sha256 in PROGRESS before the first prepass; cut.json mtime precedes all cache files | sha256 `ce58c333...ccb572` recorded; cut.json mtime 1792943310 (16:48:30), first 15u-a file 1792943351 |
| 4. Test set | 60 volumes, substitutions listed, parse failures counted | 60 evaluated, 0 substitutions ([`substitutions_test.tsv`](data/phase-12-hand-print-box-height/step-15u/substitutions_test.tsv) is empty), 0 parse failures |
| 5. Stop rule | both weighted agreements >= 0.90 | print 1.000, hand 1.000: **pass** |
| 6. Diagnostic | 20 volumes table | below; all `hand` |
| 7. Protected data | unchanged | `diff` of `protect.py` outputs and of the 15q/phase-10 sha256 lists: identical |

Stop rule ([`stop_rule.json`](data/phase-12-hand-print-box-height/step-15u/stop_rule.json), [`test_volumes.tsv`](data/phase-12-hand-print-box-height/step-15u/test_volumes.tsv)):

| Stratum | silver | stratum size | n | agree | agreement | Wilson 95% | value range (px) |
|---|---|---|---|---|---|---|---|
| gedrukte_resoluties | print | 99 | 10 | 10 | 1.00 | 0.72-1.00 | 65.0-68.5 |
| nouvelles | print | 64 | 10 | 10 | 1.00 | 0.72-1.00 | 80.0-86.5 |
| plakkaten | print | 76 | 10 | 10 | 1.00 | 0.72-1.00 | 74.5-85.5 |
| eerste_serie | hand | 123 | 10 | 10 | 1.00 | 0.72-1.00 | 100.0-155.0 |
| tweede_serie | hand | 421 | 20 | 20 | 1.00 | 0.84-1.00 | 121.5-182.0 |

Weighted agreement, silver `print` (99, 64, 76): 1.000; silver `hand` (123, 421): 1.000. **Pass.**

Diagnostic set ([`diagnostic_volumes.tsv`](data/phase-12-hand-print-box-height/step-15u/diagnostic_volumes.tsv)); series = EAD `ancestor_3` (abbreviated):

| inv | series | mode | value | margin |
|---|---|---|---|---|
| 11220 | Registers van ingekomen ordinarisbrieven | hand | 111.0 | +23.25 |
| 2378 | Eerste/Geresumeerde Minuten | hand | 119.5 | +31.75 |
| 8287 | Staten van oorlog te land en ter zee | hand | 125.5 | +37.75 |
| 234 | Minuten van de notulen, 1576-1684 | hand | 128.0 | +40.25 |
| 10495 | Rekeningen ingeleverd bij de SG | hand | 135.0 | +47.25 |
| 4831 | Resoluties betreffende militaire zaken | hand | 138.0 | +50.25 |
| 350, 397, 751, 695 | Eerste/Geresumeerde Minuten | hand | 140.0, 141.0, 141.5, 143.0 | +52.25 to +55.25 |
| 11436, 11900 | Registers van ingekomen ordinarisbrieven | hand | 147.0, 158.0 | +59.25, +70.25 |
| 8939 | Binnenlandse verbalen | hand | 148.5 | +60.75 |
| 8696 | Buitenlandse verbalen | hand | 154.5 | +66.75 |
| 3991, 4445 | Geresumeerde minuten secrete resoluties | hand | 155.5, 157.5 | +67.75, +69.75 |
| 359 | Eerste/Geresumeerde Minuten | hand | 155.5 | +67.75 |
| 10306, 9821 | Rekeningen ingeleverd bij de SG | hand | 163.0, 165.0 | +75.25, +77.25 |
| 12444 | Kohieren en registers der paspoorten | hand | 173.0 | +85.25 |

**Error analysis.**

- *What was read:* (a) disagreements with silver: none (0 of 60). (b) |margin| <= 5 px: all 6 volumes (3 scans each, the sided scans nearest the volume value, seed 21 for ties; top 2 regions' `text_head` and `h_med`). (c) Diagnostic: no volume with mode `print`; the 3 `hand` volumes with the lowest values (11220, 2378, 8287), 3 scans each. (d) Minority scans: all 19 test volumes with `scan_minority_share` > 0.2 (no diagnostic volume qualifies), 5 randomly drawn minority scans each (seed 21), 95 scans. (e) Resolution and the `hrel` comparison from the caches. Verdicts per volume: [`verdicts.tsv`](data/phase-12-hand-print-box-height/step-15u/verdicts.tsv); read-outs come from `analysis.py margin|diag|minority|numbers`.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | margin <= 5, printed | 6 volumes (18 scans) | 12122 `Hoog Mogende Heeren, / Bieven van Besancon van den 18 deeser maaken gewas van` |
  | diagnostic lowest 3: handwritten | 2 volumes (6 scans) | 2378 `als vaaderlyke Voogd de Administra„ / tie vande goederen by des Supplts:` |
  | diagnostic lowest 3: table/numbers | 1 volume (3 scans) | 8287 `Tambour Major. 14. / Hoboist 1777 / Hobogst 1777` |
  | minority scans, printed (larger type or other typeface) | 89 of 95 | 12144 `Hong Mogende Heeren. / E. Cardinaal Doria is voorleede Maandag van hier vertrok-` (h=101 against a volume value of 80.0) |
  | minority scans, ocr_junk (decorative large type, h 140-180) | 6 of 95 (12179 x3, 12174 x2, 12166 x1) | 12179 `Secai cxs Leesexxeddiex dexeemuiestacte / Nederlanden, alsen den geett` |
  | minority scans, handwritten | 0 of 95 | |

- *Conclusion:* No disagreement exists to explain, so the failure modes are read off the near misses and the minority scans. The volumes closest to the cut (12122, 12123, 12087, 12235, 12166, 12203) are printed; the rule is right but its margin there is 1.25-4.75 px, close to the 1.0-1.5 px granularity of the medians. Large minority shares (up to 0.50) occur in nouvelles and plakkaten and come from printed letters and edicts in several type sizes, not from handwritten insertions, so a scan-level box-height label would mislabel printed material and only the volume-level weighted median is usable (F10-R.3 holds, as a rule on volumes). The test passes with margin on the hand side (lowest hand 100.0 against cut 87.75) and thin margin on the print side (highest print 86.5); a nouvelles or plakkaten volume above 87.75 would be called `hand`, and the 15u sample cannot say how common that is: the fit has max print 86.0 and the test 86.5, the pooled print range is 65.0-86.5 over 68 volumes (38 fit, 30 test) and only the nouvelles volume 12122 (86.5) lies above 86.0. The rule is untested on scans at a resolution below 3950 px and on non-silver print (no diagnostic volume was predicted `print`).
- *Resolution:* median `page_w` per class, fit set (16 volumes with a scan table: dev, 455, 456; the 60 phase-10 sample volumes have none in the caches): hand 5411-6723 (median 5691), print 4621-4752 (median 4674). Test set: silver `hand` 4979-6218 (median 5566), silver `print` 3950-5070 (median 4612). Test volumes outside the fit range 4621-6723: 16, all silver `print` (4 gedrukte resoluties 4541-4614, 3 nouvelles 4430-4610, 9 plakkaten 3950-4110: 12196, 12205, 12179, 12166, 12206, 12176, 12203, 12174, 12225). Plakkaten are scanned at about 4000 px and still give 74.5-85.5 px, so raw pixels were adequate for these volumes.
- *Comparison with `hrel` (no decision):* [`hrel_comparison.tsv`](data/phase-12-hand-print-box-height/step-15u/hrel_comparison.tsv). The rules disagree on 15 of 60 volumes; the `hrel` rule matches silver on 45, the box-height rule on 60. Of the 15, 13 are silver-`hand` volumes that `hrel` calls `print` (3127, 3107, 3190, 3623, 3611, 3308, 3315, 3587, 3307, 3523, 3520, 3547, 3539) and 2 are nouvelles volumes (12123, 12122) that `hrel` calls `hand`.

**Findings:** F12-15u.1 to F12-15u.7.

**Open issues.** none (the next action is the Opus session that writes briefs 15v and 15w, as the stop rule passed).

### Review of step 15u

By Opus 5.5, 2026-10-25. Script: [`review/review_15u.py`](data/phase-12-hand-print-box-height/review/review_15u.py).
**Verdict: accepted; the stop rule passed.**

- **Re-run.** I recomputed all 80 volume values from the 15u region caches with my own code (no library call): 0
  mismatches in value or mode, and the evaluated volumes are exactly the `sample` rows of `test_sample.tsv`. The
  stop-rule table and both weighted agreements (1.000 / 1.000) reproduce from the volume table. `fit` re-run:
  `fit_volumes.tsv` byte-identical, `cut.json` unchanged (sha256 `ce58c333…`). All 80 prepass sources are under
  `shared-data/`; 38,186 scans, 0 parse failures. Suite: 1,215 passed, 11 skipped.
- **Leakage.** `fit` reads only the 15q caches of the 76 fit volumes; `prepass` exits without `cut.json`; `cut.json`
  mtime (…310) precedes the creation of `data/hand_print_geom/15u-a/` (…350). Silver labels enter only `evaluate`. My
  own sha256 snapshot of the 15q caches, the phase-10 data and report, and `text_layer/`, taken before launch, is
  unchanged after the step.
- **Reads.** I checked three volumes independently. 12166 (minority share 0.50): its `hand`-side scans are
  plakkaten in larger type ("HEM, By der / gratien Godts Prince van / Orangien", h 103-168). 12122 (margin -1.25):
  its `hand`-side scans are typeset envoy letters ("Hoog Mogende Heeren, / DE tydingen uit Warschauw", h 82-127).
  3193 (the lowest test hand volume, 100.0): handwritten resolutions ("Ontfangen drie missiuen vanden Orateur Haga"),
  with single regions as low as 80-86 px. The agent's verdicts hold, and region- or scan-level box heights are not
  hand/print labels on either side (F12-15u.4).
- **Revised.** F12-15u.5: page width mixes page size and DPI, so the low plakkaten widths are not evidence of a lower
  resolution.
- **Exploratory (not a rule).** F12-R.1 (normalising by page width destroys the separation), F12-R.2 (about 5 of the
  783 silver volumes expected on the wrong side, mostly early *eerste serie* volumes).
- **For 15v/15w.** Keep `value` and `margin` in each volume's mode record, and have 15w list every volume within
  ±10 px of the cut for a read, together with every `print` volume outside the three printed series. F12-R.2 says the
  likely error is a handwritten volume called `print`, and the diagnostic set had no `print` volume to check.

### Step 15v: volume-mode build (v4)

**What changed.** New v4 presets (`sg_1.01.02_v4`, `sg_1.10.94_v4`: `LayerPreset.mode_presets`/`volume_mode`, `inventory_preset`, `MODE_RULE_FIELDS`), `line_split.prepass_rows`, the new module `text_layer/modes.py` (`decide_mode`, `write_mode`, `read_mode`, `extract_inventory_v2_modes`) and per-inventory presets in `build.py`; `mode/` tables per inventory; 7 new tests; docs (`docs/text_layer.md` § Text layer v4) and changelog entry. v1-v3 output is untouched. Brief: [`step-15v-volume-mode-build.md`](../plan/phase-12-hand-print-box-height/step-15v-volume-mode-build.md). Build: `data/text_layer_v2_dev/15v-a/{1.01.02,1.10.94}` (14 + 2 inventories, 0 failures). Scripts and outputs in [`data/phase-12-hand-print-box-height/step-15v/`](data/phase-12-hand-print-box-height/step-15v/): `compare.py`, `metrics.py`, `crosswalk_sample.py`, `reads_3771.py`, `reads_pages.py`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | new tests pass, suite green | 7 new tests; full suite 1,222 passed, 11 skipped (1,233 collected; before: 1,226 / 1,215 / 11); `test_text_layer_line_split.py` 14/14 |
| 2. Comparison with `15m-a` | zero unexplained differences | 0 unexplained (table below) |
| 3. `mode/<inv>.json` | `value`, `n_chars`, `n_regions` equal to `fit_volumes.tsv`; modes as listed | 16/16 equal; 3771-3773, 3821-3823 `print` (66.0, 66.0, 67.0, 67.0, 69.0, 69.0), the other 8 `hand` (124.0-165.0); 455 = 75.0, 456 = 82.5, both `fixed`/`print` with `box_height_mode` `print` |
| 4. Printed 1.01.02 next to 455/456 | within range, or explained | split candidates, units per page: outside, explained below; column share, layout merges per scan, `print:*` share, `page_number`/`other`: inside |
| 5. Crosswalk | next to 455 (exact 27,033, near 1,864, split_half 55, split_original 189, unmatched 7) | exact 0 in all six; near 5-62, split_half 0-5, split_original 0-2, unmatched 4,554-9,388 (99.4-99.9%) |
| 6. Protected data | unchanged | `data/hand_print_geom`, phase-10 data, step-15u data: identical (sha256). The `protect.py` list differs only inside `shared-data/` (`README.md` modified, new `NL-HaNA_1.01.02/republic_p32_items_v2/`, 5 files), written by another project's session while this step ran; nothing from this step writes there |

*Comparison with `15m-a`* ([`compare_15m-a.json`](data/phase-12-hand-print-box-height/step-15v/compare_15m-a.json); `mode/` ignored, 42 + 6 files):

| Part | Equal | Differ, explained | Differ, unexplained |
|---|---|---|---|
| hand-mode 3349-3354, 11182, 11281 (per-inventory files) | all | 0 | 0 |
| 455, 456 (`fixed`) | 48 files | 3 (`layout`, `types`, `units` `params.json`: per-mode form) | 0 |
| 3771-3773, 3821-3823 (print now) | 0 | 93 files: `extract_stats` 5, `lines` 6, `scans` 5, `regions_meta` 5, `layout` 18, `types` 12, `units` 18, `join` 18, `wbc/parts` 6 (3822 has 13 of 16: its `scans`, `regions_meta`, `extract_stats` are equal because no line was split), plus 18 new files (`split_model`, `split_stats`, `units/republic_lines`, 6 each) | 0 |
| corpus-level files (hand layer, 14 inventories) | 131 files equal in total | `layout/types/units` `params.json` (per-mode form) and `stats.json` (differ only in the six printed entries, checked per key), `wbc/*` (55 of 131 `wbc_stats.tsv` rows and 6 rows of each `wbc_per_inventory*.tsv`, all in the six printed inventories) | 0 |

*Printed 1.01.02 (print mode) next to 455/456* ([`print_vs_455_456.tsv`](data/phase-12-hand-print-box-height/step-15v/print_vs_455_456.tsv); "lines" = lines with text; the same volumes under the hand rules, from `15m-a`, in the last two columns):

| Inv | Scans | Split candidates (hyphen+bigram) | `column` share | Layout merges / scan | Units / page | Hand rules: units / page | Median `main_text` unit (lines): print / hand | `print:*` share | `page_number` / `other` types per scan |
|---|---|---|---|---|---|---|---|---|---|
| 3771 | 579 | 218 (3+215) | 1.000 | 0.166 | 45.5 | 25.9 | 2 / 10 | 1.0 | 2.09 / 0.99 |
| 3772 | 595 | 1 (0+1) | 1.000 | 0.171 | 43.9 | 24.9 | 2 / 10 | 1.0 | 1.96 / 0.94 |
| 3773 | 608 | 1 (0+1) | 1.000 | 0.153 | 44.4 | 24.9 | 3 / 9 | 1.0 | 1.92 / 0.89 |
| 3821 | 440 | 1 (0+1) | 1.000 | 0.216 | 69.1 | 45.1 | 3 / 8 | 1.0 | 1.75 / 0.95 |
| 3822 | 463 | 0 | 1.000 | 0.261 | 65.6 | 43.6 | 3 / 9 | 1.0 | 1.77 / 1.07 |
| 3823 | 442 | 3 (0+3) | 1.000 | 0.382 | 70.1 | 44.9 | 2 / 9 | 1.0 | 1.79 / 1.26 |
| 455 | 721 | 11,515 (2,401+9,103) | 0.992 | 2.467 | 38.8 | | 2 / | 1.0 | 1.82 / 0.98 |
| 456 | 730 | 64,299 (12,704+51,561) | 1.000 | 0.147 | 33.6 | | | 1.0 | 1.95 / 0.65 |

Split candidates and units per page lie outside the 455/456 range: candidates far below (0-218 against 11,515-64,299), units per page above it (43.6-70.1 against 33.6-38.8). The other measures lie inside. Explanations are in the reads below. Crosswalk (`units/stats.json` `republic_crosswalk`):

| Inv | exact | near | split_half | split_original | unmatched |
|---|---|---|---|---|---|
| 3771 | 0 | 62 | 5 | 2 | 9,388 |
| 3772 | 0 | 57 | 0 | 0 | 8,887 |
| 3773 | 0 | 59 | 0 | 0 | 8,468 |
| 3821 | 0 | 7 | 0 | 0 | 4,644 |
| 3822 | 0 | 7 | 0 | 0 | 4,605 |
| 3823 | 0 | 5 | 0 | 0 | 4,554 |
| 455 | 27,033 | 1,864 | 55 | 189 | 7 |

**Error analysis.**

- *What was read:* (a) crosswalk bar: 20 matched rows (seed 17, any class but `unmatched`; 204 exist) of the six volumes, unit text and REPUBLIC paragraph text, and 10 unmatched rows (seed 17), with the nearest AS line by box distance: [`crosswalk_sample.tsv`](data/phase-12-hand-print-box-height/step-15v/crosswalk_sample.tsv). (b) 3771: 20 split lines (seed 17 over the 218), 10 merged layout regions (seed 17 over 96), 15 units (seed 18 over 26,339) with their segments, 10 pages (seed 17) next to the `15m-a` hand build: `reads_3771.py`, `reads_pages.py`. All verdicts are mine, from the texts (the print is Dutch of 1716; unclear when the text alone cannot decide).
- *Crosswalk bar (fixed beforehand: usable iff at least 18 of 20 are the same paragraph).* Two readings give different numbers. Strict (the unit is the paragraph, start and end): same 2 (a unit that lacks only the drop-cap letter counts as same), wrong 18: in 14 the unit is the tail of the paragraph (it starts mid-sentence, or at "WAER op ...", where REPUBLIC keeps letter and decision as one paragraph), in 4 only the last line (a fragment). Lenient (the matched line lies in that paragraph and the unit's last words are the paragraph's last words): 20 of 20. **Verdict: 2/20 strict, not usable (below 18/20).** On the lenient reading the matches would be right but cover 204 of 40,750 ids (0.5%), so the crosswalk is useless for 1.01.02 either way. The rule was fixed on the strict reading ("same paragraph"); the lenient result is recorded because the user may want the other bar (see STATUS, open question).
- *Unmatched ids (10, seed 17):* 

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | AS line has the same text, box off by 17-71 px (sum of the four coordinate differences) | 8 | REPUBLIC "IS gehoort het rapport van de Heeren Ham, ..." (start), nearest AS box 326,340,882,96, d=71, text "IS gehoort het rapport van de Heeren" |
  | nearest AS line is a different line (176-184 px) | 2 | start of "ONtfangen een Missive van den Resident Rumpf ...", nearest "dresseert aen den Griffier Fagel, houdende ad-", d=184 |
  | no AS scan | 0 | |

  The REPUBLIC boxes come from another line detection, so the tolerance (`NEAR_PX`) is exceeded; it is not a matching bug.
- *3771, 20 split lines and 10 merged regions:* splits 20 of 20 right (col 1 then col 2; the right half starts at the second column's left edge, for example "van de waerde van" || "hondert gul-" and "Coffy Boonen, genietende voor tarra van de vaten" || "twaelf,"). They are all in the tariff pages (scans 0300-0336; 215 of 218 on 0329-0336). Merges 10 of 10 coherent (5 merge two source regions: index entries 0020 and 0010, a Missive 0439, a Requeste 0556, the attendance header 0408; the other 5 are single large regions): the lines read in order.
- *3771, 15 units (seed 18):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | boundary right | 9 | 10-line decision "WAAR op gedelibe- / reert zijnde, is goetgevonden ..." ; the 16-line index block ; the one-line running header "1716." |
  | wrong | 4 | "P het geproponeerde door de Heeren Ge-" alone (first line of a paragraph cut off); "vanden Bergh, haer Hoogh Mogende" alone; "dertienden deser loopende maendt, ende / daer nevens een Bylage" (a paragraph tail starting lower-case); "C1RANVIII." (a signature mark typed `main_text`) |
  | unclear | 2 | "Onder stondt," ; "Wichers." |

- *3771, 10 pages next to the hand build (what the print rules changed):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | worse | 8 (0535, 0425, 0311, 0375, 0297, 0285, 0113, 0028) | hand: one unit of 21 lines "IS ter Vergaderinge gelesen de Requeste van Dorpmeesters ende Sesmannen van Hou. pertingen, ..."; print: a 2-line unit ending "... van Hou." and a 19-line unit starting "pertingen, houdende, ..." |
  | mixed | 2 (0179, 0554) | 0554: the French article list is cut into 18+5+9+6 lines at "Et selon ce Reglement ..." (plausible paragraph starts), but "P het geproponeerde van de Heeren Lo-" / "stevonon ende Velters" is cut in two; 0179: "Onder stondt," becomes its own unit (plausible), "Accordeert met het voorschreve" / "Register." is cut in two |
  | better | 0 | |
  | same | 0 | |

  The pattern is the same on all 10 pages: the first one or two lines of a paragraph (the lines beside the dropped initial; the first letter sits in a separate region) are flagged `print:region_start` and the rest `print:indent`, so one paragraph becomes two or three units. Over the whole of 3771 the units per page rise from 25.9 to 45.5, the median `main_text` unit falls from 10 to 2 lines, and the share of `main_text` units starting with a lower-case letter rises from 0.254 to 0.392 (3821: 0.219 to 0.310). In the hand build a text region is already a paragraph for these volumes, which is why the region rule works there; the print rules were written for 455/456 where a region holds several paragraphs.
- *Conclusion:* the pre-pass, the mode decision and the per-inventory build work as specified and change nothing outside the six printed volumes. The volume-level mode is right for the layout stages (the splitter finds the few merged lines, the layout merges 0.15-0.38 regions per scan, column values on every line) but the print unit rules cut the printed 1.01.02 paragraphs worse than the hand rules (8 of 10 pages), so printed does not mean "use the 455/456 unit rules" for these volumes. The REPUBLIC crosswalk cannot validate 1.01.02 units (0.5% coverage; boxes from a different line detection). Limits: the reads are of one volume (3771) and of pages drawn from it; 3821-3823 (65-70 units per page) were only measured, not read.

**Findings:** F12-15v.1 to F12-15v.6.

**Open issues.** (1) The unit rules for printed 1.01.02 volumes over-segment (F12-15v.4); whether the v4 presets should use the hand unit rules for these volumes, or a drop-cap-aware print rule, is a decision for the phase review and 15w (not changed here: the brief fixes the print rules of the v3 preset). (2) The crosswalk bar was met on neither reading in coverage terms; 15w should not pass `--republic-dir` for 1.01.02 unless the user decides otherwise.

### Review of step 15v

By Opus 5.5, 2026-10-25. **Verdict: accepted.**

- **Re-run.** Suite 1,222 passed, 11 skipped (1,233 collected). `16/16` box values in `mode/` equal `fit_volumes.tsv`
  (per the step's check; 3771 `mode/3771.json`: 66.0, margin −21.75, 579 scans, 0 parse failures). Crosswalk stats in
  `units/stats.json` as reported (3771: near 62, split_half 5, split_original 2, unmatched 9,388; 3822: near 7, unmatched
  4,605). My own sha256 snapshot of `15m-a`, `15n-a`, `15o-pub`, `data/hand_print_geom/`, the 15u and phase-10 data
  and the untouched `text_layer` modules, taken before launch, is unchanged.
- **Code.** `build.py`: mode runs read `Run.preset_for(inv)` / `mode_params`; non-mode paths keep their old
  expressions; `republic_path` is `None` for hand inventories in mode runs. `inventory_preset` and the v4 presets
  follow the brief. No archive knowledge in `modes.py`.
- **Reads (F12-15v.4).** I read two random pages each of 3771 (0394, 0418) and 3822 (0080, 0168), print rules
  against hand rules. The drop-cap cut is real ("Ntfangen een Missive van den Resident / Mortaigne, geschreven te
  Regensburgh" as two units). But the print rules also split the decision paragraph *WAER op gedelibereert …* and the
  *Dat …* clauses from the request they answer, which the hand rules lump into one unit with the next resolution
  ("alles alhier ter Vergaderinge rapport te doen. En dat laatstelyk …"). So "0 better" overstates it: each rule set
  has its own defect (F12-R.3). F12-15v.4 is revised accordingly.
- **Crosswalk bar.** The bar meant "the matched line belongs to the right REPUBLIC paragraph" (the crosswalk is
  line-level). On that reading 20/20 of the matches are right, but they cover 0.5% of the REPUBLIC lines, so the
  crosswalk carries no information for 1.01.02 either way. Decided in review: 15w passes no `--republic-dir` for
  1.01.02. UA-027 is withdrawn as moot.
- **Process.** The step edited its own report rows with `sed` and appended the UAs with `cat >>`, against the
  Edit-only rule for shared files. No harm found (no concurrent writer); noted for the brief template.

### Step 15w: v4 dev build, audit, run kit

**What changed.** New `scripts/run_text_layer_v4_full.py` (the v3 hand-off script by archive: `--src ARCHIVE=DIR`,
`--republic-dir ARCHIVE=DIR`, `--inventories-file ARCHIVE=F`, `--only`; log `run_v4.log`; cost constants measured on
`15w-a`), a new audit `step-15w/audit.py` (discovers sub-layers, inventories and sources; adds the v4 `mode/`
invariants; `--self-test` builds the fixture as v2 and as v4), 13 new tests in `tests/test_run_text_layer_v4_full.py`,
the section *Running the full v4 build* and the known-issue paragraph in `docs/text_layer.md`, a changelog entry,
`.gitignore` (`!/scripts/run_text_layer_v4_full.py`), UA-024 and UA-025. The 182 staged tars are linked from
`data/text_layer_v4_staged/NL-HaNA_1.01.02/`. `15w-a` (196 + 2 volumes, `--stage all`, no `--republic-dir` for 1.01.02,
2 workers granted by coreshare) took 5,340 s for 1.01.02 and about 8 minutes for 1.10.94. No library change.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | new and ported pass; suite green | 13 new (8 ported + `test_two_archives_one_run`, `test_republic_per_archive`, `test_build_cmds_per_archive`, `test_bad_archive_argument`, `test_audit_self_test`); v3 tests 8/8 unchanged; full suite 1,235 passed, 11 skipped (1,246 collected; before: 1,222 / 11 / 1,233) |
| 2. `15w-a` = `15v-a` on the 16; 168/168 values | equal up to listed paths; modes as planned | 168/168 values equal (fit 76, test 60, diagnostic 20, UA-017 12; 0 unequal, 0 missing); 1.01.02 258 equal / 33 differ / 6 missing, 1.10.94 55 equal / 2 differ; all differences listed below. Modes of the 168: 1.01.02 `print` 70, `hand` 96; 455/456 `fixed`/`print` |
| 3. Audit | all v4 and phase-09 invariants pass except reported shares; `15m-a` 178/4 | `15w-a`: 3,412 pass, 75 fail, all 75 lower-case merge shares (0 judged failures); `15m-a`: 178 pass, 4 fail, the same four (3351 0.9415, 3823 0.9414, 455 0.9477, 456 0.932) |
| 4. Prediction and UAs | written | table below; UA-024 (full run), UA-025 (pilot publication) |
| 5. Protected data | unchanged | `data/hand_print_geom`, phase-10 data, step-15u data: identical (sha256, 484 files). The `protect.py` list differs only in `shared-data/` (other projects: `README.md` modified, new `NL-HaNA_1.01.02/republic_ner_2026-04-11/`, `.DS_Store` files); `15m-a`, `15n-a`, `15o-pub`, `15v-a`, LotA data: identical |

*Mode counts* ([`modes.tsv`](data/phase-12-hand-print-box-height/step-15w/modes.tsv), 198 volumes): 1.01.02 111 `hand` and
85 `print` (`box_height`), 1.10.94 2 `fixed`/`print`. By silver label (1.01.02): silver `print` 83 -> 83 `print`;
silver `hand` 78 -> 77 `hand`, 1 `print` (3463); no silver 35 -> 34 `hand`, 1 `print` (9486, printed). The 30 reserves
(no reference value): 15 `hand`, 15 `print`; the 11 in the band are 3478 (hand, 88.0) and 12207, 12226, 12230, 12132,
12193, 12105, 12106, 12088, 12118, 12168 (print, 78.0-87.0).

*Comparison with `15v-a`* ([`compare16.json`](data/phase-12-hand-print-box-height/step-15w/compare16.json),
[`json_keydiff.py`](data/phase-12-hand-print-box-height/step-15w/json_keydiff.py)). 1.01.02: 33 differing files =
14 `extract_stats/<inv>.json` (key `source` only: link dir instead of the pilot dir), 8 `scans/<inv>.scans.tsv.gz`
(column `member` only, for the 8 dir sources 11182, 11281, 3771-3773, 3821-3823; tars compare equal),
`layout/stats.json`, `types/stats.json`, `units/stats.json` (new inventories as keys; for the 14 shared inventories only
`seconds` differ, plus `republic_crosswalk` and `republic_crosswalk_roles` for 3771-3773 and 3821-3823),
`units/params.json` (key `republic_dir`) and 7 `wbc/` files (corpus files over 196 instead of 14 inventories: `n_inventories`
and the per-character shares; `default_set`, `derived_default_set`, `pinned`, `sanity_pass` identical).
6 files only in `15v-a`: `units/republic_lines/<inv>.tsv.gz` of the printed volumes. 1.10.94: only `extract_stats/455.json`
and `456.json` (`source`). No unit, segment, join or lines table differs.

*Audit.* [`audit_15w-a.json`](data/phase-12-hand-print-box-height/step-15w/audit_15w-a.json),
[`audit_15m-a.json`](data/phase-12-hand-print-box-height/step-15w/audit_15m-a.json). The v4 invariants (mode file and values,
`print` iff value <= cut for `box_height`, `mode/scans` rows = scans - parse failures, `mode/regions`, `split_model` iff
`print`, no `split_from` in hand volumes, unit methods match the mode, `params.json` keys) pass for all 198 volumes.
The 75 reported shares (0.50 to 0.9496) are in 49 `print` and 26 `hand` volumes.

*Size and time* (`15w-a`, from `status.tsv`; [`cost.py`](data/phase-12-hand-print-box-height/step-15w/cost.py)):

| | volumes | XML | output | worker-seconds | per XML byte |
|---|---|---|---|---|---|
| 1.01.02 | 196 (111 hand, 85 print) | 25,297,913,805 B | 2,753,304 KiB (0.1115) | 7,418.6 (extract 5,964 incl. pre-pass, layout 657, join 271, types 221, units 201, wbc 105) | 2.93e-7 |
| 1.01.02 `hand` | 111 | 10.5 GiB | 0.107 | 1,577 | 1.395e-7 (1.26-2.00e-7 per volume) |
| 1.01.02 `print` | 85 | 13.0 GiB | 0.115 | 5,842 | 4.173e-7 (2.67-7.13e-7) |
| 1.10.94 | 2 (`print`) | 1,009,177,478 B | 87,496 KiB (0.0888) | 471.9 (extract 428.6) | 4.68e-7 |

The 182 staged tars hold 4.31 XML bytes per archive byte (script constant 4.2). The full 1.01.02 run (9,122 volumes,
about 400 GB XML; mostly `hand`, so the script assumes 10% `print` by bytes: 1.67e-7) is predicted at output about 42 GiB
(0.1115 x 400 GB), 67,000 worker-seconds (range 1.4e-7 to 2.1e-7: 56,000 to 84,000), so with 8 workers the lower bound is
2.3 h (1.9-2.9 h) and 3 x that is 7 h (5.8-8.8 h). The dev build ran at 1.4 x its lower bound, so expect about 3.5 h. Caveats
as in the v3 docstring: another inventory mix (the dev set is 52% `print` by bytes, a large share of it the slow
nouvelles/plakkaten), a USB drive may be slower than local disk, other projects share the workers.

**Error analysis.** Reads from the text of the 3 largest regions of a scan (`read_scans.py`; no images), verdicts in
[`verdicts.tsv`](data/phase-12-hand-print-box-height/step-15w/verdicts.tsv). I judged printed vs handwritten from the text
(noisy HTR of handwriting versus OCR of type; page numbers `( 83 )`, colophons, line-end hyphenation) and not from the
scans, so a `printed` verdict is firm only where several features agree.

1. **Near the cut (51 volumes, 147 scans).** The band |margin| <= 10 holds 39 + 456 volumes known at planning plus 11
   reserves. Two scans each (the sided scans nearest the volume value, seed 21), five for 3463, 3492, 4617, 9486 and the 11
   reserves. Verdicts: printed 126, handwritten 20, OCR junk 1 (12225 scan 0016, 322 characters). Volume level: 47
   printed volumes (every scan printed), 4 handwritten volumes. 3463 (86.5 px, `print`): handwritten on 5 of 5, e.g.
   "15 November. gansinot versogt een pasport te besorgen voor 100 lasten rogge gaande na Venlo" and
   "5 Augusty. miss: Commandeur doys. over een beserteur te gulik gearresteert", with HTR errors typical of handwriting
   ("lartagena", "Cheurf: vande Paltz"): a wrong `print`. 3492 (89.5, `hand`), 3478 (88.0, `hand`) and 4617 (97.0, `hand`):
   handwritten on 15 of 15, e.g. "20. February. Pasport tot den vryen uitvoer van de begage van Hr. O. 7. V. Haren na Aken".
   9486 (79.0, `print`, no silver): printed on 5 of 5 ("( 83 ) Weg, nog de Capitaale Wal; en daar voor leggen- de Crachten in
   genoegsaamen staat"). The 11 reserves in the band: 3478 hand, the other 10 printed, all right. **50 of 51 volumes right**
   (the error is 3463).
2. **`print` outside the printed series.** In 1.01.02 two volumes are `print` without silver `print`: 3463 (wrong, above) and
   9486 (right). The list of the brief ("plus any reserve") finds no other: all 15 `print` reserves are silver `print`.
3. **Minority material (32 volumes, 160 scans, seed 19; the 19 volumes of 15u skipped).** Printed 135, handwritten 15,
   OCR junk 4, unclear 6. The handwritten 15 are the smaller-hand pages of 3463, 3478 and 3492 (minority = print side of a
   handwritten volume). In the 29 printed volumes the minority scans (value above the cut) are printed pages in a larger
   type or another run ("Hoog Mogende Heeren" letters at 100-125 px, French extracts at 105, colophons "By Isaac Scheltus,
   ordinaris Drukker"), as in 15u (F12-15u.4). The 4 OCR junk are large-type pages ("Maten exterael dee Receectieesde
   Gdedertlaende", 12162 scan 0318). The 6 unclear (12168 0014, 12169 0010, 12193 0006, 12194 0021, 0023, 0006) are lists of
   short titles ("Placaat Verbod van Guarrens of Hand tolens den 14 Dec: 1742", "Publicatie secretarissen van de Meyerye van
   s'Hertogenbosch") at 100-160 px: probably a handwritten table of contents bound into the printed volume. That would be the
   one real minority type, 6 of 160 scans (0.9% of the scans of 4 volumes), and the one-mode-per-volume limit costs little.
4. **Print rules outside the gedrukte resoluties (20 units; plakkaten 12225, 12166; nouvelles 12133, 12088; seed 19).** ok 10,
   bad unit boundary 3, unclear 7, bad split 0. Examples: ok: "De Ontfangers van het Middel op de Collateraate Succellien,
   betalt op de invorderins van de tiende Verhoogins van dien Impolt te vigileeren. 5 Octob. Num." (`print:region`, 4 lines);
   bad unit boundary: 12088 unit 0064 ("... doit estre tellement piqué de l'afsaire de Neufavoir declaré dans son Conseil, que
   si les Suisles ne Neufchatel ...", the lines of the French and the Dutch column alternate in one 12-line unit) and 0630;
   12166 unit 0586, one line ending "ende oft pemaint wiste ende gedoochde var eenige Kig-houden," (continues in the next
   unit). The unclear ones are garbled or fragmentary text (12166 0455 "Sumilior oBnoglov an Sanilaad", 12133 0027).
   The nouvelles column interleaving is a layout/column issue, not a split-rule defect, and is documented in
   `docs/text_layer.md`.
5. **Audit failures (75 volumes, 364 offending merges, 5 each, seed 19;**
   [`audit_failure_rows.tsv`](data/phase-12-hand-print-box-height/step-15w/audit_failure_rows.tsv)). Right side upper-case 349,
   punctuation 15 (`&`, `(`, `:`), digit 0. I read 22 of them: capitalised nouns after a broken word ("onder com- | Capitein
   Mewvill", "Intendent de la Ville de Liere") and sentence starts, `&` before an abbreviation; none points at the v4 build.
   The share is lower in `print` volumes (49 of 87 below 0.95) than in `hand` (26 of 111): typeset 17th/18th-century Dutch
   capitalises nouns.

**Conclusion.** The v4 build works at the scale of the local volumes: the mode decision reproduces the 168 reference values
exactly, the audit is clean, the output is the 15v reference on the 16 shared volumes, and 50 of the 51 volumes nearest the
cut are right. The one error is the failure mode the review expected (F12-R.2): a handwritten register whose small hand falls
below the cut. Four such registers lie in the band, so the full run should produce a few more (a handwritten volume called
`print` gets the print splitter and unit rules). The decision for the user is UA-024 (the full run, with this evidence and
the prediction) and UA-025 (publish the pilot, 3463 included).

**Findings:** F12-15w.1 to F12-15w.9.

**Deviations from the brief.** None in the deliverables. (1) The audit keeps the lower-case merge share as an invariant
with `reported: true` (pass/fail computed as in 15m) instead of removing it, so that `15m-a` reproduces 178/4; the summary
line gives `n_fail_judged` (0). (2) `read_scans.py` samples "the sided scans nearest the volume value" with ties broken by
`random.Random(21)`, one reading of the brief.

**Brief defects.** (1) UA IDs: the brief says "next free IDs UA-024, UA-025 (check both UA files)"; the highest in use is
UA-027 but 024/025 were never used (reserved by the plan), so the two UAs got them. (2) The brief said the print
inventories of the band were "39 + 456"; with the 11 reserves it is 51, as the brief's own rule gives. (3) UA-026 was
resolved with (a) while the step ran (documented in `docs/text_layer.md` as a known issue; the brief only said "not to fix
here"). (4) `compare.py` of 15v compares whole trees, so with 198 against 16 volumes it lists 3,600 missing
files; a wrapper (`compare16.py`) restricts it to the 16 shared inventories and the corpus-level files, and the brief's
`--explained-invs` flag was not needed. (5) The brief asks for the lower-case merge share to be "reported, never
pass/fail" and for `15m-a` to reproduce 4 failures; both hold only with the `reported` flag (deviation 1).

### Step 15wr: split gate for single-column print volumes (review fix)

*Post-review addendum (2026-10-26): review fix for F12-R.4.*

**What changed.** `modes.column_start_share` measures the share of lines (at least 20 characters, with a box) that start in
the right 60% of the text span of their page side. `presets.SPLIT_GATE_V4` (min share 0.35, fixed in advance, P16) is added
to `sg_1.01.02_v4`: in `extract_inventory_v2_modes` a print-mode volume is split only if its share is at least 0.35 (or
unmeasurable); `mode/<inv>.json` records `split_gate`. The audit invariants follow the gate. The layer was rebuilt from
scratch as `data/text_layer_v2_dev/15wr-a` (196 + 2 volumes, 0 failed rows). Tests: 1,248 collected, 1,237 passed, 11 skipped
(before: 1,246 / 1,235 / 11; two new tests).

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 1,237 passed, 11 skipped; modes file 9 passed; audit `--self-test` ok |
| 2 | gate decisions | 31 resoluties 0.496-0.501 and 3463 0.4152: split (32); 25 nouvelles, 27 plakkaten, 9486: not split (53; max share 0.2258). Nothing in 0.23-0.41. Largest gap to the review's `pooled_mid_start`: 0.0002 |
| 3 | compare with `15w-a` | 111 hand + 455/456 + all of 1.10.94 (57 files): equal. 32 split volumes: only `mode/<inv>.json` differs, and only by the added `split_gate` key (key diff, 32/32). 53 gated volumes: differ (explained), `split_model/` and `split_stats/` (106 files) only in `15w-a`. Corpus files: `layout|types|units/stats.json` differ in `seconds` for all keys and in other values for the 53 only; `wbc/*` differ in the 53 inventories (`n_lines`, `char_status`; `wbc_set` and `differs_from_default` for 32 / 29 of them); `default_set` and `pinned` identical |
| 4 | no `split_from` in gated volumes; audit | 0 of 1,947,176 lines have `split_from`; audit 3,593 pass, 32 fail (lower-case merge share, reported only), 0 judged |
| 5 | read bar; protected data | 19 of 20 in order: passed. `protect2` (hand_print_geom, phase-10, 15u, `15w-a`) identical; `protect` differs in one line, `language-of-the-archive/gold/06-terms-1.01.02.json` (size 3382 to 4766), written by another project, not by this step |

**Gate table summary** (`step-15wr/gate.tsv`): 31 resoluties split, 3463 split, 25 + 27 + 1 gated. Splits (rows with
`split_from`, two per split) before and after: nouvelles 1,771,940 to 0 (885,970 splits), plakkaten 339,056 to 0, 9486
9,490 to 0, resoluties 1,234 and 3463 212 unchanged. Units per scan before and after: nouvelles 36.40 to 25.87, plakkaten
25.03 to 22.21, 9486 17.04 to 11.86, resoluties 61.12 and 3463 27.25 unchanged.

**Error analysis.** Reads (`verdicts.tsv`, `reads_out.txt`): for each of the 20 pre-registered scans the largest
`main_text` unit of `15wr-a` was read (first 14-16 lines, all 20 checked for y order and `split_from`). 19 in order, 0 half
lines, 0 other order errors, 1 unclear. Examples in order: nouvelles 12088 "Soo aenstondts artiveren in de- / se Rivier
's Lands Oorlog schip / Monnickendam, gecommandeert ..." (hyphenated, whole lines); 9486 54 lines of a testimony
("derhalf of twee uuren van de Stadt lagen, en waar / omtrent men niet wilt, ..."). The unclear one, 12226: "800 18 P. dat
Abu na het / erempel en in nahniging han syet / zeen bi Onse Bublicatie han Ben-": OCR too garbled to judge. Random lines
of the gated volumes (seed 20261030, lines over 30 characters): of 5, 4 were split in `15w-a`, e.g. 12109 "seedert vier
daagen van | Berlin herwaarts op marsch is, en nog" (15w-a, two units' halves) against the whole line in `15wr-a`; 1 (12187)
was not split in `15w-a` either (the sampling drew from all lines, not only split ones: a deviation). 3771 (tariff
pages): 3 of 3 sampled split lines identical in id and text to `15w-a` ("onder de precautien van het selve", "tarra van").

**Cost.** `15wr-a` status.tsv, same method as the run-script comment: 1.01.02 print rate 2.402e-7 worker-s per XML byte
(85 volumes, 13.0 GiB; was 4.173e-7), hand 1.44e-7 (was 1.395e-7, noise), total 4,990 worker-s (was 7,419); output 2,647,572 KiB
(0.1072 per XML byte, was 0.1115), 1.10.94 86,036 KiB. Constants in `scripts/run_text_layer_v4_full.py` updated (print rate
and output per byte for 1.01.02). The full-run estimate (10% print) falls 11%: 1.50e-7 against 1.67e-7.

**Conclusion.** The gate fixes the F12-R.4 defect with no side effect on hand volumes, 1.10.94 or the two-column
series; the measure separates the series with a wide margin (0.226 vs 0.415), so the fixed threshold needed no tuning.
Remaining open: the drop-cap unit rule (15x) and the 0.415 vs 0.35 margin for 3463, a handwritten index volume that stays
split (UA-028). **Brief defects:** (1) the brief's `test_extract_split_gate` needs `min_chars` lowered (fixture lines are
shorter than 20 characters, so the share is unmeasurable and the splitter would stay on); the test sets `min_chars: 1`.
(2) The brief says 885,967 splits for the nouvelles; the `split_from` row count is twice the number of splits (1,771,940).
(3) The brief's compare check "corpus `*/stats.json` per key" must ignore `seconds`. (4) The 20 reads show only the first
14-16 lines in the text file; the rest was checked by geometry.

### Step 15x: drop-cap-aware print unit rule

*Post-review addendum (2026-10-26): UA-026 (b), F12-R.6, P18, P19.* Brief:
[`step-15x-drop-cap-units.md`](../plan/phase-12-hand-print-box-height/step-15x-drop-cap-units.md).

**What changed.** `UnitParams.drop_cap_lines` (default `None`, omitted from `to_dict`): in the print branch of
`split_region`, a cut whose only cue is `indent` is dropped when it falls within `drop_cap_lines` main lines of the last
kept cut (0 = the region's first main line). `LayerPreset.mode_overrides` (per mode, `MODE_RULE_FIELDS` keys only,
`ValueError` otherwise) is applied by `inventory_preset`; `sg_1.01.02_v4` sets `{"print": {"unit_params":
{**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}}}`. The layer was rebuilt from scratch as `data/text_layer_v2_dev/15x-a`
(196 + 2 volumes, 0 failed rows, 4 workers granted by coreshare). Tests: 1,251 collected, 1,240 passed, 11 skipped (before
1,248 / 1,237 / 11; three new tests: `test_print_drop_cap_lines`, `test_unit_params_to_dict_omits_none_drop_cap`,
`test_inventory_preset_overrides`; `test_v1_v3_presets_unchanged` and `test_inventory_preset` updated). UA-025 is
unanswered, so no published pilot predates 15wr/15x.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | tests green | 1,240 passed, 11 skipped; units + modes files 27 passed |
| 2 | compare with `15wr-a` (`compare_15wr-a.json`) | 3,200 files equal, 6 differ. Hand volumes, 455/456, `mode/<inv>.json` of all 198 and all 57 files of 1.10.94: equal. In the 85 print-mode 1.01.02 volumes exactly 7 files each differ (`units/units`, `units/segments`, `units/doc_id_map`, `wbc/parts`, `join/{segments,stats,units}`). Corpus: `units/params.json` (print entry gains `"drop_cap_lines": 2`; hand and the 1.10.94 file unchanged), `units/stats.json`, four `wbc/*` files; `pinned`, `default_set` equal |
| 3 | audit | 3,592 pass, 33 fail (lower-case merge share, reported only), 0 judged (`15wr-a`: 3,593 / 32 / 0; one more reported share, no judged change) |
| 4 | bars | A, B, C pass (below); stop rule not fired |
| - | protected data | both `protect` and `protect2` before/after identical (no difference, LotA gold included) |

**Counts per series** (`counts_per_series.txt`, `suppressed.tsv`: 179,412 suppressed cuts; main_text unit starts are lines that start
a `main_text` unit; units per scan count all unit types as in 15wr):

| Series | Volumes | Suppressed | of unit starts | Units per scan | Median lines per main unit |
|---|---|---|---|---|---|
| *gedrukte resoluties* | 31 | 126,228 | 551,021 (22.9%) | 61.12 to 52.49 | 3 to 5 |
| 3463 (index, built `print`) | 1 | 165 | 3,658 | 27.25 to 26.29 | 2 to 3 |
| *nouvelles* | 25 | 34,385 | 234,551 | 25.87 to 23.23 | 2 to 3 |
| *plakkaten* | 27 | 18,572 | 125,079 | 22.21 to 20.22 | 2 to 3 |
| 9486 | 1 | 62 | 337 | 11.86 to 10.62 | 2 to 5 |

3771: 46.05 to 35.21 units per scan (brief: 45.5 to about 35). The 20 read pages: 751 to 541 `main_text` units.
The brief's "roughly 45% fewer print cuts" counted cuts in five volumes; the share of unit starts here is 22.9% (unit starts
include each region's first piece); not recomputed.

**Bar A, precision of the dropped cuts.** 210 suppressed cuts on the 20 scans; sample of 60
(`random.Random(20261029).sample` of the sorted list, `sampleA.tsv`). Read file `reads_AC2.txt`: for each sampled cut the two
lines before and after with their `x` offset in line heights, and, per page, every unit that differs between the builds in
full text (units identical in both builds are marked `=`; the unabridged file `reads_AC.txt` is kept). Result: **59 right,
1 wrong, 0 unclear = 0.983 (bar 0.90): pass.** `verdicts.tsv`.

| Category | Count | Example (verbatim) |
|---|---|---|
| right: line beside the initial, sentence runs on | 59 | "Ntfangen een Missive van den Raedt van / **State, geschreven alhier in den Hage** / den veertienden deser loopende maendt," (3769 0600) |
| wrong: decision clause | 1 | "**En hier op gedelibereert zynde, heb-** / ven naar Edele 105. goecgevonden," merged into a two-line page-top tail (3849 0257) |

**Bar B, decisions and clauses kept** (automatic, 31 *gedrukte resoluties*): 38,176 lines matching
`^(W[AE]+R op|Dat |DAt |IS (na|goet)|Is (na|goet))` start a unit in `15wr-a`; **37,897 still do = 0.9927 (bar 0.97): pass**
(design expectation about 0.99). The 279 lost (`barB_lost.tsv`): 136 *IS/Is na|goet*, 77 *WAAR/WAER op*, 66 *Dat*;
they sit mostly in volumes 3816 (28), 3862 (19), 3822 and 3823 (17 each).

**Bar C, pages** (20 scans, units of both builds side by side, `pages.tsv`): **19 better, 1 same, 0 mixed, 0 worse (bar:
worse <= 2, better >= 12): pass.** Examples: 3769 0416 21 to 11 units, every unit now an entry or request from its
"Ntfangen" / "IS ter Vergaderinge" / "Y resumptie" opening to its decision; 3772 0344 20 to 15; 3816 0460 59 to 35; 3828
0195 49 to 37. The one `same` is 3773 0200 (no suppressed cut on the page). Defects seen on better pages: a page number
glued to the last unit (3769 0600 "sten / 12 7"), a signature merged ("Geteeckent, / C. Voet van Winsen.", 3795 0360), a
"WAAR op gedelibereert" clause glued to the junk line "4080" (3816 0355), a French memorial where "En prenant connoissance
de cette cause" may open a new paragraph (3774 0281), and a three-line chunk beside an initial that stays cut because
the window is two lines (3849 0257 "coure wyze en vry van alle latten, voor / 29n reekenins ... / tanger van de Exploicten").

**Diagnostic, no pass/fail: *nouvelles* and *plakkaten*** (10 per series, `random.Random(20261028)` over the sorted
suppressed cuts, `diag_nouv_plak.txt`). *Nouvelles*: 7 right, 3 wrong (12087 unreadable line; 12103 "Dat. 29 Juny" dateline
after the signature "Getee- / kent, J. L. H. van Wassenaer."; 12106 "Ik ben met veel hoogagtinge," closing formula). Right e.g.
12088 "& on y fera demain louverture de la Diete generale de la / **maniere accoutumée, les affaires domestiques y occuperont ap-**". *Plakkaten*: 6 right, 4 wrong, all four
unreadable (12166 "orlaet", 12176 garbage line, 12232 "susvens", 12235 German table junk; unclear counts as wrong). Right e.g.
12226 "... om / **te verkrygen haare voorschreeve Officien: en dat voor**". The *plakkaten* fall below 7 of 10: trigger
fired, UA-029 proposes restricting the rule to the two-column volumes. The gate was not changed in this step. 3463:
165 suppressed cuts, counted only.

**Conclusion.** The rule does what the design measurement promised in the printed resolutions: lines beside a dropped
initial rejoin their paragraph (59 of 60), pages move toward the printed paragraphs (19 of 20, none worse) and 99.3% of the
decision/*Dat* openings survive. Its cost is a small, systematic loss of decision clauses that directly follow a one- or two-line
entry (0.7%, F12-15x.3), which the cue set cannot separate from a drop-cap line. In the single-column series its precision
is unproven (7/10 and 6/10, the *plakkaten* failures being unreadable text rather than shown paragraph starts); that is
for the user to decide (UA-029). No parameter or cue was changed after seeing output.

**Brief defects.** (1) `suppressed.py` is specified as a script but gives no definition of "units per page"; I used all
unit types per scan as in `step-15wr/counts.py` for the table and `main_text` units for the 20-page counts. (2) The compare
brief's "explained" files are 7 per inventory (the brief lists `join/*` as three tables); consistent. (3) The brief gives the audit baseline as 32 reported
failures; `15x-a` has 33: volume 12118 (a *nouvelles* volume) moves from 9,122 merges at 0.9541 to 9,499 merges at
0.9496 lower-case right sides (target 0.95, reported only, not judged), presumably because the join stage works
within units and there are fewer unit boundaries (not traced).
(4) The "45% fewer cuts" expectation and the "units per page" figures are on different bases from the brief's table; the
3771 figure matches (46.05 against 45.5).

**Findings:** F12-15x.1, F12-15x.2, F12-15x.3, F12-15x.4. **Open issues.** UA-029.

## Review

By Opus 5.5, 2026-10-25 (phase review; 15u and 15v were reviewed above, 15w here). Code review:
[`plan/phase-12-hand-print-box-height/REVIEW.md`](../plan/phase-12-hand-print-box-height/REVIEW.md). Scripts:
[`review/`](data/phase-12-hand-print-box-height/review/) (`review_15w.py`, `column_share.py`, `dropcap_design.py`,
`draw_reads.py`). **Verdict: accept with follow-ups**; one defect goes to step 15wr.

- **Re-run.** 168/168 reference values equal (own join), modes 111 / 85 / 2, `15w-a` = `15v-a` on the 16 shared
  volumes (258 / 33 / 6 and 55 / 2 / 0 files), audit 3,412 / 75 (all reported) on `15w-a` and 178 / 4 on `15m-a`,
  self-test ok; AS suite 1,235 passed, 11 skipped; LotA 381 passed, 1 xfailed. `cut.json` unchanged.
- **The splitter in the single-column print series (F12-R.4).** 15w read units of the *nouvelles* and *plakkaten* but
  no split lines. `split_stats` shows 885,967 splits in the 25 *nouvelles* (about 73 per scan) and 169,527 in the 27
  *plakkaten*, against 617 in the 31 *gedrukte resoluties*. I read 46 random split pairs in 8 volumes (12088, 12166,
  12109, 12122, 12207, 12225, 12184, 9486). In all 46 the halves are one sentence: "Le Collonel Rendall, qui ‖ avoit
  éte Gouverneur des Barbades &" (12088), "de respective Collegien ter Admiraliteyt ‖ van haer ressont" (12166),
  "de onvoorziene ‖ overgave van Breda" (9486). The model takes the short lines of a single-column side as its column
  width (12088: 764 px of an 1,800 px span), so every full line becomes "merged". The halves then go to two layout
  columns, and a unit holds the left halves of a page ("wesen vereyscht, men op gisteren den / Minitter ten rade /
  Men wil hier voor …", 12088_0428). F12-15w.7's "interleaved columns" are this. Pooled over both page sides, the share
  of lines that start past 0.4 of their side's text span separates the layouts: 0.496–0.501 for the two-column
  *gedrukte resoluties*, 0.005–0.225 for every *nouvelles*, *plakkaten* and *Processtukken* volume
  ([`column_share_pooled.tsv`](data/phase-12-hand-print-box-height/review/column_share_pooled.tsv)). 456 (0.164)
  is two-column with most lines merged, so a gate on this share belongs to 1.01.02 only. Step 15wr adds it, with a
  read bar fixed beforehand.
- **Index volumes (F12-R.5).** The EAD titles show that the four handwritten volumes near the cut are indexes: 3463
  "1740 index" (86.5, wrong `print`), 3478 "1745 index" (88.0), 3492 "1748 index 1e halfjaar" (89.5), 4617 "Index"
  (97.0). The resolution registers of the same years lie at 146–151 px (3459, 3471, 3487, 3493). No non-index hand
  volume of the 198 is below 100.0. For a full run this means the errors concentrate in a known kind of volume: 251
  index-like titles, about 9 of them expected on the `print` side (normal tail, exploratory). They are easy to list
  afterwards (`print` volumes outside the printed series). Whether to pin them is UA-028.
- **Drop-cap cuts (F12-R.6).** Page 3771_0535 read line by line with the cue values: a paragraph's next one or two
  lines sit 1.1–1.7 line heights right of the column edge beside the initial, and their `indent` cue cuts the
  paragraph. In five volumes these cuts (indent alone, d ≤ 2) are 45% of all print cuts, while cuts at decision and
  *Dat* lines are almost never of that kind (53 of 4,687). This is the design of step 15x. Its bar and read list are
  fixed in its brief before the rule exists (`review/reads_15x.tsv`, drawn now).
- **The 15w deviation** (the lower-case merge share kept with `reported: true`) is accepted. The brief asked for two
  things that need it, and `n_fail_judged` = 0 is the number to read.
- **Statuses.** Confirmed: F12-P.1, F12-15u.1–4, 6, 7, F12-R.1, R.3, F12-15v.1, 2, 5, 6, F12-15w.1–4, 6, 9, F12-R.4–R.7.
  Revised: F12-15u.5 (15u review), F12-R.2, F12-15v.3, F12-15v.4 (15v review), F12-15w.5, 7, 8. None withdrawn.
- **Candidates for `docs/findings.md`** (for someone reusing AS): F12-15u.1 with F12-R.1 (raw box height separates
  print from hand in one scan campaign; normalising by page width breaks it); F12-15u.4 (scan-level box height is not
  a hand/print label); F12-R.4 (REPUBLIC's merged-line splitter assumes two columns per page side; gate it on the
  column layout); F12-R.5 (index volumes are the hand volumes with print-sized boxes); F12-15v.5/15v.6 (geometry
  crosswalks need the same line detection); F12-R.7 (structural audits do not show that text reads).

**Review of step 15wr** (Opus 5.5, 2026-10-26). Verdict: **accept**. Code review in
[`REVIEW.md`](../plan/phase-12-hand-print-box-height/REVIEW.md#step-15wr-review-2026-10-26). The gate does what P16 says:
`column_start_share` follows the brief exactly, the gate runs only when the resolved preset splits and the preset has a
`split_gate` (so hand volumes and 1.10.94 write the same mode file as before), and an unmeasurable share keeps the
splitter, so the gate cannot turn the splitter off for a volume it cannot measure. The re-run reproduces the step
(F12-R.8): AS 1,237 passed, 11 skipped; LotA 381 passed, 1 xfailed; 53 gated / 32 split; my own decompressed-byte
comparison against `15w-a` finds no difference outside the 53 gated volumes except the 32 `split_gate` keys and the
expected corpus entries. I re-read all 20 bar units in full, not only their first lines. All 20 read in order. That
includes 12226, whose OCR is garbled but whose word breaks join ("han Ben- / 16 Detember 1757", "syest- / sien"). The
pre-registered verdict stays 19/20, and the bar holds either way (F12-15wr.2). Two numbers in the step are revised:
the largest share gap to the review table is 0.0076 (12162), not 0.0002 or 0.001 (F12-15wr.1, still inside ±0.03), and
the half-line rows total 2,120,486 (F12-15wr.3). 3463 stays split and its splits are wrong (F12-R.9). That is a mode
error, not a gate error, and the P21 pin handles it. The gated volumes' derived WBC sets recovered `-` (F12-R.10). The
four brief defects the step listed change no conclusion. The 15x brief was checked against `15wr-a` and corrected in
place (anchors, the build command, the baseline counts, full-unit reads); its bars and read lists are unchanged.

**Review of step 15x** (Opus 5.5, 2026-10-26). Verdict: **accept.** Code review in
[`REVIEW.md`](../plan/phase-12-hand-print-box-height/REVIEW.md#step-15x-review-2026-10-26); script
[`review_15x.py`](data/phase-12-hand-print-box-height/review/review_15x.py), written apart from the step's `bars.py`.
The rule is the one P18 fixed: only a cut whose sole cue is `indent` is dropped, within two main lines of the last kept
cut; with `drop_cap_lines=None` the old loop runs, and `to_dict` leaves the key out, so v1–v3 and 1.10.94 `params.json`
do not change. The re-run reproduces the step (F12-R.11): the suites, the byte comparison against `15wr-a` (only the 7
explained files in each of the 85 print volumes and the expected corpus entries differ), 179,412 suppressed cuts, the bar-A
sample and the diagnostic draws under their pre-registered seeds, bar B on segment and on whole-line text, and the audit.
*My reads.* 21 bar-A cuts and 5 pages in full (3769 0416, 3795 0360, 3816 0355, 3816 0460, 3849 0257): 20 right, 1 wrong
(3849 0257, the step's), 5 pages `better`. Then all 210 cuts on the 20 scans, not just the sample: 206 right (F12-R.12).
On the bar-B side, 30 of the 279 lost starts: a third are short pass entries whose request heading and decision become one
unit; the rest are decisions glued to a 1–2-line tail at a page or column top, *Dat* sub-clauses, and clauses after a junk
line, which do no harm (F12-15x.3 revised). *The single-column series* (UA-029): the pre-registered diagnostic fired on
four unreadable fragments, so I read 30 more suppressed cuts per series with a new seed (20261031, fixed before
drawing). On readable text the rule is right in 27 of 28 *plakkaten* and 30 of 36 *nouvelles* cuts; the *nouvelles*
errors are letter parts beside an indent (closing formula, signature, article number). Those merges also cause the
extra reported audit failure: in 12118 the join glues "Getee|Waar meede, &c." across a break mark (F12-R.13, R.14). The
rule removes about five wrong cuts for each wrong merge there, so it helps on net; UA-029 now recommends keeping it (a)
and gives the costs of each option. The verdict scheme was part of the problem: `unclear` counted as `wrong` let junk input
fire the trigger (F12-R.15, a planning note in `plan/CONTEXT.md`). Statuses: F12-15x.1, .2 confirmed; .3, .4 revised.

*Candidates for `docs/findings.md`* (15wr and 15x): F12-R.4 with F12-15wr.1 (gate a two-column splitter on the column-start
share of the PageXML); F12-R.6 with F12-15x.1 (an indent-only cut within two lines of the previous cut is a line beside a
dropped initial: 0.98 precision, 99.3% of decision starts kept); F12-R.15 (an `unreadable input` verdict in read bars).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F12-R.4, F12-15w.7 | Split gate for single-column print volumes of 1.01.02; rebuild `15wr-a`; re-measure the print cost; correct the *Known issue* in `docs/text_layer.md` | done in step 15wr (reviewed 2026-10-26, accept) |
| F12-R.9 | 3463's wrong splits (two-column handwritten index built `print`) | the P21 pin step after the full run (UA-028) |
| F12-15v.4, F12-R.3, F12-R.6 | Drop-cap-aware print unit rule (UA-026 (b)) | done in step 15x (reviewed 2026-10-26, accept) |
| F12-15x.4, F12-R.13, F12-R.14 | Keep the drop-cap rule in the *nouvelles*/*plakkaten*, restrict it, or add a letter-part exception later | UA-029 |
| F12-15x.3 | Decisions glued to a 1–2-line tail or request heading (0.7% of decision starts) | known issue in `docs/text_layer.md`; no step planned |
| F12-R.15 | `unreadable input` verdict in read bars | `plan/CONTEXT.md` §5 (planning note) |
| F12-R.5, F12-15w.5 | Handwritten index volumes on the `print` side in a full run: pin, or list and re-build afterwards | UA-028 |
| F12-R.2, F12-15u.6 | 15w audit read every volume within ±10 px and every `print` volume outside the printed series | done in 15w (F12-15w.4, .5) |
| F12-R.1 | A volume from another scan campaign (different DPI) could shift its value; untested | full-run audit: list `print` volumes outside the printed series (UA-028) |
| UA-024, UA-025 | Full run and pilot publication: no longer wait for any step (15wr, 15x reviewed) | review notes in both UAs (2026-10-26) |

## Reproduce

```
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/planning/draw_sample.py
# phase review (reads only; run from the AS root)
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15w.py
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/column_share.py \
    data/text_layer_v2_dev/15w-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4 \
    data/text_layer_v2_dev/15w-a/1.10.94/NL-HaNA_1.10.94_text_layer_v4 \
    reports/data/phase-12-hand-print-box-height/step-15w/modes.tsv reports/data/phase-12-hand-print-box-height/review/column_share.tsv
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/dropcap_design.py \
    data/text_layer_v2_dev/15w-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4 3771 3822 3769 3805 3864
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/draw_reads.py
# review of 15x (reads only)
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15x.py compare
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15x.py bars
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15x.py extra 20261031 30
.venv/bin/python -I reports/data/phase-12-hand-print-box-height/review/review_15x.py lost 30 20261032
```
