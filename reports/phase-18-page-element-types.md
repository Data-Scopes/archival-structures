# Phase 18 report: Page element types v1

> **Status:** final (reviewed 2026-11-17, [REVIEW](../plan/phase-18-page-element-types/REVIEW.md); review fix 24r pending).
> **Phase plan:** [`plan/phase-18-page-element-types/PLAN.md`](../plan/phase-18-page-element-types/PLAN.md).
> **Code state:** AS v0.23.0 (24a and 24r).
> **Last updated:** 2026-11-17, by the phase review (Opus 5.5).

## Summary

Phase 18 gave AS one vocabulary for page elements (`placement:` as its own axis, ten new generic types), ported
`listlike` from shared-tools verbatim into `analysis/listlike.py`, added `analysis/page_elements.py` and seven archive
presets, and built whole-page line gold for 100 pages in 10 strata of 9 archives (40 dev pages agent-read, 60 test pages
agent pre-labelled and corrected by the user). Additive for the siblings: no sibling import changes. **Both bars fail.**

| Bar | Test result (60 pages) | Verdict |
|---|---|---|
| L: `listlike` (selected C0R1) separates list/table/form from running text, P ≥ 0.90, R ≥ 0.80 | P 0.950, R 0.575 (bootstrap 0.50–0.66); as handed over (C0R0) R 0.060 | fails on recall |
| M: the layer's `marginalia` call finds margin lines, P ≥ 0.90, R ≥ 0.80 | P 0.259 (bootstrap 0.09–0.50), R 0.872 | fails on precision |

- `listlike` works on a line window, so it misses table and list lines that wrap or sit in register cells (619 misses;
  F18-24d.12), plus digit-initial ledger rows and amounts followed by `—`/`„` (F18-R.5). Its precision pass depends on
  how prose cells of item tables are labelled (F18-R.1).
- The margin call is the layout role `extra` plus a page-margin rule. Both treat second table columns as margin, even in
  mid-page on registers (F18-24d.10, F18-R.2).
- The layer never says `list`, and calls 881 table lines `running_text` (F18-24d.13). Phase 19 needs element-level
  grouping and a table-column/margin split, tested on fresh gold, because this test gold is spent (F18-R.13).

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F18-P.1 | The SG v5 rules, given only archive plumbing (name, archive id, scan-id prefix, source pattern), build one inventory of each new archive with 0 failures in 8–25 s on one worker (2.10.50 85, 2.10.01 1090, 1.05.06 8 and 45, AsdSAA 5023 4 and 89 3.1, AsnDA 0114.11 1); every one got mode `hand` by method `default` because the box-height pre-pass saw `n_chars 0`. **Revised at review:** only 2.10.50 got `default` in the 24b builds (no region reaches `min_region_chars` 100, `n_regions 0`); the other archives got `box_height`, and 1.05.06 49/1167/1171 and AsdSAA 5023 4 `print` (F18-24b.2, F18-R.11) | method | text-layer, presets, new-archives | revised | [Planning](#planning) |
| F18-P.2 | Unchanged, the types stage calls many lines `marginalia` outside SG: 30.5% of segments in 2.10.50 85 (5,384/17,628), 28.9% in VOC 1120, 18.0% in 2.10.01 1090, 14.2% in AsnDA 1, 10.2% in AsdSAA 5023 4; the SG `index` rule fires too (2.10.01 1090: 2,465 segments, 9.7%) | data-property | types, margin, new-archives | confirmed | [Planning](#planning) |
| F18-P.3 | `listlike` as handed over (C0R0) flags 1.0–8.5% of lines in five new archives and 22.7% in AsdSAA 89; dropping the bare line-final digit as an amount (C1) cuts the flags by half or more (2.10.01 0.085 → 0.034, 2.10.50 0.024 → 0.001); adding `closed_share ≤ 0.4` with min 2 (R1) raises them to 7–46%; a case-sensitive numbering pattern changed at most 152 lines per volume and was dropped from the grid | method | listlike, grid | confirmed | [Planning](#planning) |
| F18-P.4 | Line ids are not unique across scans in AsdSAA (`tr_1_tl_1`; 10,147 distinct ids for 22,407 lines in 5023 4): a join on `line_id` alone multiplied the lines to 328,515, so every table keys on (scan_id, line_id) | data-property | asdsaa, keys | confirmed | [Planning](#planning) |
| F18-P.5 | None of the new archives' PageXML carries region structure types (AsdSAA, AsnDA, 1.05.06: none; 2.10.01: every region `Text`); only VOC has provider layout tags (1120 regions: paragraph 3,510, marginalia 1,271, header 889, catch-word 852, page-number 734, untagged 16,940), so a provider baseline exists for VOC only | data-property | pagexml, region-tags | confirmed | [Planning](#planning) |
| F18-P.6 | The NA viewer gives no IIIF file list for 1.05.06 (inventory 44), while it does for 1.01.02, 1.04.02, 2.10.50 and 2.10.01; two of the nine 1.05.06 inventories with ≥ 40 pages (10, 44) have no local thumbs either, so the draw kept only inventories with thumbs. Memorix IIIF serves AsdSAA 89 (HTTP 200; `info.json` 2427×3028 equals the PageXML) | data-property | images, iiif, 1.05.06 | confirmed | [Planning](#planning) |
| F18-24a.1 | The vocabulary gains a fifth namespace `placement:` (main, margin, top, bottom, interlinear) and six generic types (`salutation`, `endorsement`, `reference`, `note`, `addition`, `noise`) plus 10 new subtypes on `heading`, `closing`, `running_title`, `numbering`; all tags known before stay known, and `generic:marginalia` maps to `placement:margin` through `legacy_placement` | result | vocabulary, placement | confirmed | [Step 24a](#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets) |
| F18-24a.2 | `listlike` ported verbatim: `window_features`, `list_score` and `list_flag` equal the shared-tools copy (NaN-aware) on 5 documents (4 self-test, 1 fixture of 13 lines in 2 groups) under all six grid cells | result | listlike, port | confirmed | [Step 24a](#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets) |
| F18-24a.3 | C1 drops "Anno 1650", "den 3 Maart 1700" and "Fait le 4 mai 1699" as amount ends (bare year digits), keeps "gl. 120", "120 livres", "fol. 12", dot leaders; but on the 13-line fixture the line flags are identical for C0R0 and C1R0 (10 of 13) because the window shares stay above threshold, so the effect is a corpus-level one (F18-P.3), not visible on a toy | negative | listlike, c1 | confirmed | [Step 24a](#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets) |
| F18-24a.4 | `read_layer_lines` runs on the real SG v5 inventory 12212 (5,276 non-empty lines, 0 errors; 4,137 main_text, 584 index, 341 marginalia; 1 line with more than one segment; SG PageXML region tag is `Text` for every line) | data-property | page-elements, reader | confirmed | [Step 24a](#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets) |
| F18-24a.5 | Nothing outside the new modules, the vocabulary, the presets and docs needed a change; no existing vocabulary test changed its expectation; the seven archive presets differ from `voc_1.04.02_v5sg` only in name, archive id, prefix, pattern and description | result | presets, scope | confirmed | [Step 24a](#step-24a-vocabulary-v2-listlike-port-page_elements-reader-archive-presets) |
| F18-24b.1 | The seven archive layers build with the 24a presets in 18 inventories (1.05.06: 5, 2.10.50: 4, 2.10.01: 5, AsdSAA 5023/5075/89: 1 each, AsnDA: 1), 0 failures, staged file counts equal `n_files`; each archive took 15 s to 2 min (2.10.50: about 2 min for four inventories) | result | text-layer, presets, new-archives | confirmed | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24b.2 | Mode detection differs from the planning probe (F18-P.1): inventories 49, 1167, 1171 of 1.05.06 and AsdSAA 5023 4 get mode `print` by method `box_height`; 2.10.50 stays `hand` by `default`, all other volumes `hand` by `box_height`. **Revised at review:** AsdSAA 5023 4 is a handwritten volume (image of P039), so its `print` is a mode error of the SG cut outside SG (F18-R.11) | data-property | text-layer, mode, new-archives | revised | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24b.3 | Planning defect, fixed: the 150-line upper cap dropped the typical page of the two-column print volumes (3818 and 3799: median 245 and 244 lines, 479 and 474 scans over 150, so 2 and 7 eligible of 486 and 485); the cap is now 300 for `sg_print` (150 elsewhere) and the eligible counts are 3818 401, 3799 479, 12212 79, 12129 748, 12117 480, so the sg_print draw is random again (8 of 10 sg_print pages changed; sampling v1 kept as `sample_pages_v1_cap150.tsv`) | bug-fixed | sg, sampling | confirmed | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24b.4 | The 100 pages (60 test, 40 dev; 6 and 4 per stratum) all have an image with a consistent aspect: 50 NA IIIF, 10 Memorix IIIF 3 (size equals the PageXML), 40 thumbs (1.05.06 and AsdSAA 600 px, AsnDA 300 px); the 300 px AsnDA thumbs are too small to read | result | images, new-archives | confirmed | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24b.5 | In the ten pages read, page numbers, running heads and item numbers (P001, P007, P019), printed form text (P031) and margin notes (P037, P049) are visible on the image but have no PageXML line, so a dump-only pre-label cannot produce them | data-property | pagexml, gold, margin | confirmed | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24b.6 | Line order in the dump is not reading order for the letter P031 (body lines in near-reverse order) and the letterhead of P049 (reversed), while printed and notarial spreads follow the page at region level; short lines (< 3 characters) are 1.5 to 9.4% per stratum, highest in AsdSAA 5023 (97 of 1,030) and 89 (16 of 221 lines; 7.2%) | data-property | order, htr-noise | confirmed | [Step 24b](#step-24b-stage-build-and-sample) |
| F18-24c.1 | The 100 agent reads cover 7,501 lines (3,167 dev, 4,334 test) with 325 unclear (4.3%) and 43 `custom` lines; `check_reads.py` was clean on first submission, but 10 of 22 batches had labelled without opening the image and needed a follow-up that changed 6 to 245 rows each (about 640 in all) | measurement | method | confirmed | [Step 24c](#step-24c-element-gold) |
| F18-24c.2 | Self-check of 30 random lines against dump and image: 29 agree, 1 type wrong (P044 k68, `list` vs a note-like sentence interleaved with a margin block, judged on a thumbnail); the reads are good enough to freeze the dev gold, with the account-line `table`/`list` split and the `table`/`form` boundary as the known soft spots. **Revised at review:** the 30 lines are 14 test and 16 dev (the section said 19 and 11); the user kept P044 k68 as `list`, and changed 2 of the 14 test lines (list to table, the UA-070 convention), which the self-check had called right (F18-R.12) | measurement | method | revised | [Step 24c](#step-24c-element-gold) |
| F18-24c.3 | Type candidates from the notes: printed letterhead (`custom:letterhead`, 38 lines on 6 letter pages) and the "Copie/Copia" label (`custom:copymark`, 5 lines); no other `custom` use | observation | vocabulary | confirmed | [Step 24c](#step-24c-element-gold) |
| F18-24c.4 | The dev gold is frozen as agent labels (3,167 lines, 40 pages, sha256 e0f2a4de…c904) before any measurement; the test gold waits for the user's 60 corrected pages (UA-063), and `summarise_elements.py` round-trips a synthetic export (changed flag, element runs, refusal to overwrite) | decision | method | confirmed | [Step 24c](#step-24c-element-gold) |
| F18-24c.5 | After UA-063 the user changed 648 of 4,334 test lines (0.150): type on 618, placement on 2, start on 61; `table` 556 of 1,124 (0.495) and `note` 57 of 155 (0.368) carry 95% of them, 2.10.50 0.453, AsdSAA 5075 0.401, AsnDA 0.178, three strata 0.000 | result | gold, agent-reads | confirmed | [Step 24c](#step-24c-element-gold) |
| F18-24c.6 | The agent pre-labels failed at the table/list/form boundary (list to table 184, form:value to table 169, running_text to table 104, form:label to table 57) and at note against `heading:summary`/`addition` (43), not on text types (running_text 0.009 changed); `data/annotations/` is byte-identical before and after; 9 page notes, 175 unclear lines (4.0%) | observation | gold, table-vs-list | confirmed | [Step 24c](#step-24c-element-gold) |
| F18-24d.1 | Dev (agent gold, 40 pages, 3,167 lines; 949 positives, 1,505 negatives), `listlike` bar L: C0R0 P 0.781 R 0.060 F1 0.112; C0R2 0.793/0.048/0.091; C0R1 0.862/0.407/0.553; C1R0 0.758/0.026/0.051; C1R2 0.828/0.025/0.049; C1R1 0.882/0.410/0.560. C1R1 is 0.007 above C0R1, a tie by the 0.01 rule, so C0R1 is selected; no cell is near R >= 0.80 on dev | result | listlike, grid, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.2 | Dev bar M (layer's margin call): P 0.227 (Wilson 0.181 to 0.280; bootstrap 0.077 to 0.533), R 1.000 (61/61); 208 false positives, 149 by `rule:extra_role` (layout role `extra`) and 59 by `rule:page_margin`, 111 of them gold `form` and 59 `table`; 161 sit on one archive (2.10.50) | result | margin, types, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.3 | Dev false negatives of C0R1 (563 of 949): of 30 read, 15 are list items or continuation lines of long-item lists (inventory entries, SG/VOC lists, AsdSAA obligations), 10 form lines (labels and values in a split caption), 5 account item lines without an amount; recall is 0.94 in AsdSAA 5023 but 0.14 in sg_hand, 0.06 in sg_print and 0.14 in VOC | result | listlike, recall, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.4 | Dev false positives of C0R1 (62): of 30 read, 13 are ragged prose lines (short lines in a narrow column), 10 are appointment/name lines of 2.10.50 P084 that gold calls `running_text` but read as a list (possible gold error), 7 are dated or number-ending lines on the P079 ledger page; precision is 0.86 pooled but 0.40 in 2.10.01 and 0.33 in sg_print | result | listlike, precision, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.5 | The layer's line types map poorly onto the gold on dev: the layer never says `list` (the `index` type does not occur in these 40 pages), so 284 list and 255 table lines are `running_text`, 172 form lines `margin`; `running_text` calls have P 0.581 and R 0.991, `numbering` 0.870/0.746, `running_title` 0.320/0.571, `quire_marks` 0.590/0.650, and the 72 `custom` calls (type `other`) hit none of the 22 gold `custom` lines | result | layer-types, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.6 | C0R1 flags other families at high rates on dev: salutation 0.71, running_title 0.64, table 0.60, endorsement 0.55, numbering 0.49, closing 0.35, against running_text 0.043; C0R0 flags 0.1 or less of every family except custom (0.50) | observation | listlike, false-positives, dev | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.7 | Bar L FAILS on recall: selected C0R1 on 60 test pages P 0.950 (Wilson 0.934 to 0.963, bootstrap 0.898 to 0.977; bar 0.90 passes) and R 0.575 (Wilson 0.549 to 0.600, bootstrap 0.498 to 0.655; bar 0.80 fails); TP 836, FP 44, FN 619. **Revised at review:** precision passes only under the UA-070 (a) ruling; with prose cells as `running_text` (UA-070 b) C0R1 has P 0.753, R 0.573 (F18-R.1). The verdict (fail) does not change | result | bar-L, listlike, test | revised | [Step 24d](#step-24d-measure) |
| F18-24d.8 | `listlike` as handed over (C0R0) on test: P 0.967, R 0.060 (87 of 1,455; Wilson 0.049 to 0.073); recall of C0R1 per stratum 0.74 (5023), 0.64 (5075), 0.65 (VOC), 0.63 (AsnDA), 0.50 (2.10.50), 0.44 (2.10.01), 0.04 (sg_hand, 1 of 28). **Revised at review:** the sg_hand figure is one page: P001 holds 27 of the 28 sg_hand positives (F18-R.6) | result | listlike, test | revised | [Step 24d](#step-24d-measure) |
| F18-24d.9 | Bar M FAILS on precision: the layer margin call has P 0.259 (Wilson 0.214 to 0.311, bootstrap 0.087 to 0.497; bar 0.90) and R 0.872 (Wilson 0.790 to 0.925, bootstrap 0.636 to 1.000; bar 0.80 passes by the point estimate); TP 82, FP 234, FN 12 over 94 margin and 3,875 main lines | result | bar-M, margin, test | confirmed | [Step 24d](#step-24d-measure) |
| F18-24d.10 | Of the 234 margin false positives 166 are `rule:extra_role` and 68 `rule:page_margin`; 160 are gold `table` (by stratum 2.10.50 157, 2.10.01 33): second table columns and right strips read as margin; the 12 misses are an indented left strip of P044 (11) and one P004 remark. **Revised at review:** 157 and 33 are all false positives of those strata; the gold `table` ones are 128 (2.10.50) and 27 (2.10.01). 55 of the 160 `table` false positives were agent `form` the user relabelled: the type split depends on that relabel, the count does not (placement unchanged) (F18-R.2, F18-R.3) | error-analysis | margin, extra-role | revised | [Step 24d](#step-24d-measure) |
| F18-24d.11 | All 44 bar-L false positives are gold `running_text`: ragged lines of narrow columns and wrapped single-sentence entries (31, P041, P034, P039, P011), 10 paragraph-final or sum-spelling lines of letters and deeds (P049, P052, P055, P058), 3 merged number-and-text lines; no gold error found. **Revised at review:** 30 of the 44 have a fragment line (under 4 characters, or gold noise/numbering/quire mark) within three lines, which drives the flag more than column width (F18-R.4); `prose` on P049–P058 is the same sentence property as on P045–P047 (F18-R.8) | error-analysis | listlike, false-positives | revised | [Step 24d](#step-24d-measure) |
| F18-24d.12 | Bar-L false negatives (619): 474 `table`, 108 `list`, 37 `form`, 295 on six pages (P030, P036, P045, P028, P046, P059); in a sample of 30, 17 are wrapped table or inventory lines, 7 items of name or item lists written as prose lines, 5 amount or ledger fragments on a line of their own (new category), 1 form value; 125 carry the note `prose`, and column-head lines are found 8 of 12. **Revised at review:** the six pages hold 298 false negatives (64 + 53 + 50 + 44 + 44 + 43), not 295 | error-analysis | listlike, false-negatives | revised | [Step 24d](#step-24d-measure) |
| F18-24d.13 | Layer types on test: the layer never calls `list` (0 of 278) and calls table lines `running_text` (881) or `margin` (190), list lines `running_text` (273), closings `running_text` (116) and notes `margin` (112); running_text calls have P 0.586, R 0.998; VOC provider region tags are `none` for 210 of 278 list lines and call no list or table. **Revised at review:** `tables/region_tags_voc.tsv` pools all ten strata (code defect, F18-R.7); on VOC alone the provider tags 67 of 69 list and 153 of 154 table lines `paragraph` and has no list or table tag, so the conclusion holds and the numbers do not | result | layer-types, region-tags | revised | [Step 24d](#step-24d-measure) |
| F18-24d.14 | Agent pre-labels against the user gold (4,334 lines): family agreement 0.858, placement 0.9995, start 0.986; `table` 0.527, `note` 0.632, all other families 0.95 to 1.00; by stratum 0.547 (2.10.50) to 1.000 (three strata) | result | pre-labels, agreement | confirmed | [Step 24d](#step-24d-measure) |
| F18-R.1 | Bar L's precision verdict depends on the UA-070 labelling ruling, its recall verdict does not: as labelled (prose cells of item tables are `table`) C0R1 has P 0.950, R 0.575; with those 298 lines as `running_text` (UA-070 b) P 0.753, R 0.573; without them P 0.938, R 0.573. The ruling followed the frozen dev gold and predates the test stage, but was set by a session that knew the dev numbers | result | bar-L, gold, rulings, review | confirmed | [Review](#review) |
| F18-R.2 | Bar M fails robustly: without 2.10.50 (no gold margin line, 157 false calls) P is 82/159 = 0.516, without 2.10.50 and 2.10.01 0.651. The 2.10.50 false calls are register cells in mid-page (15 read: x at 0.14 to 0.76 of the page width; `rule:page_margin` fires mid-page), and they do not depend on the user's form-to-table relabel: placement was changed on 2 of 4,334 lines | result | bar-M, margin, registers, review | confirmed | [Review](#review) |
| F18-R.3 | Form against table is not a stable gold boundary on registers: on 2.10.50 inventory 85, P028 keeps 71 `form:value` lines while P029 and P030 (same column heads) are all `table`, and the dev pages P077 to P080 are mostly `form`. Bar L is unaffected (both positive); a form-vs-table detector or per-type table needs a ruling and relabel first | data-property | gold, form, table, 2.10.50, review | confirmed | [Review](#review) |
| F18-R.4 | `listlike` false positives on running text come from fragment lines in the window: 30 of the 44 have a line under 4 characters or a gold noise/numbering/quire-mark line within three lines in page order, and running-text lines with such a neighbour are flagged 30/361 = 0.083 against 14/1,792 = 0.008 without | result | listlike, false-positives, htr-noise, review | confirmed | [Review](#review) |
| F18-R.5 | Two structural `listlike` misses: numeric ledger rows (P036, 53 false negatives) score 1 of 4 under R1 because a digit-initial line is not 'capitalised', full-width lines are not short and rows end in a full stop; and 76 positive lines (0 negatives; 66 on VOC) end in an amount followed by a dash or `„` (`285:—`, `ƒ1550„`) that the C0 amount pattern misses. Digit-initial lines are 351 positives and 35 negatives. Candidates for phase 19, on fresh gold (the test gold is spent for `listlike`) | result | listlike, false-negatives, features, review | confirmed | [Review](#review) |
| F18-R.6 | The sg_hand recall of 1 of 28 is one page: P001 holds 27 of the 28 positives, prose lines of an account the user relabelled `running_text` to `table` with the page note 'perhaps table is not the type of a line'. The stratum figure says nothing about SG lists | observation | sg, listlike, gold, review | confirmed | [Review](#review) |
| F18-R.7 | `eval_elements.py` selects 'VOC' lines by a non-empty `region_tag`, but every line carries one (`Text` in SG and 2.10.01, `untagged` elsewhere), so both region-tag tables pool all ten strata. On VOC alone the provider tags 67 of 69 list lines and 153 of 154 table lines `paragraph`; it has no list or table tag. To fix in 24r (tables only; no stage re-run) | open-issue | region-tags, voc, review | confirmed | [Review](#review) |
| F18-R.8 | The user's `prose` note is one line property (written as sentences, any type) collected on the 16 pages reviewed after the UA-070 ruling (P045–P060: running_text 318 of 334, table 298, closing 21, heading 4) and on none of P001–P044. On the four prose-table pages C0R1 flags 173 of 298 prose cells (0.58) and 71 of 93 other table lines (0.76); descriptive only (UA-072 a) | data-property | gold, prose, table, review | confirmed | [Review](#review) |
| F18-R.9 | The rulings' note tokens `colhead` and `cont` were never written (the 4 'cont' matches are the word 'continues'), and on P029 12 of the 14 table lines after a note have `start = 0`. Elements built as runs of `start` in dump order are therefore not element boundaries on register and interleaved pages: the annotation format needs element ids or a continues flag | method | gold, elements, annotation-format, review | confirmed | [Review](#review) |
| F18-R.10 | `datasets/bulk_tagging.py` keeps its own namespace-to-types map without `placement`, while its namespace dropdown lists `NAMESPACES` (now five): the user's bulk-tagging widget offers `placement` with no type suggestions. To fix in 24r | open-issue | vocabulary, bulk-tagging, review | confirmed | [Review](#review) |
| F18-R.11 | Mode detection is not validated outside SG: AsdSAA 5023 inventory 4, a handwritten 17th-century volume (image of P039), gets `print` by box height (margin −4.75), and 2.10.50 gets `default` because its register cells form no region of 100 characters (`n_regions 0`) | data-property | text-layer, mode, new-archives, review | confirmed | [Review](#review) |
| F18-R.12 | An agent self-check does not see convention errors: of the 14 test lines in the 30-line self-check, the user changed 2 (list to table, the UA-070 convention) that the self-check had called right, and kept as `list` the one line it called wrong (P044 k68). 86% of the user's changes (556 of 648) land on gold `table`, mostly such conventions, so the self-check (29/30) and the user's changed share (0.150) measure different things | method | gold, agents, self-check, review | confirmed | [Review](#review) |
| F18-R.13 | Leakage checks pass: dev selection before the test gold existed and one test run (hashes chained); readers barred from layer tables and the check page carries no layer call; SG test volumes asserted outside all dev layer runs; one-volume archives split by scan range; no library code reads gold. Reproduced byte-identically by the reviewer from the layers | method | leakage, review | confirmed | [Review](#review) |

## Steps

### Planning

Probes run by the planner on 2026-11-02 (scratch builds, not kept; scripts and the draw in
[`data/phase-18-page-element-types/planning/`](data/phase-18-page-element-types/planning/)).

- **Builds** (F18-P.1, F18-P.2): one inventory per new archive, with the preset `VOC_1_04_02_V5SG` plus archive
  plumbing registered at run time; segment counts per `line_type` read from `types/segments/`.
- **`listlike`** (F18-P.3): the shared-tools copy on the probe builds and on VOC 1056/1120, groups (scan, layout region),
  12 cells (cfg C0/C1/C2/C12 × rules R0/R1/R2); C2 and C12 dropped.
- **Keys and tables** (F18-P.4): (scan_id, line_id) is unique in `lines/` of SG 12212 and 10115 and VOC 2361; every line
  has a `layout/lines` row; empty lines have no `types/segments` row (383, 88, 512).
- **Images** (F18-P.6): `handprint.na_iiif_bases` on one scan each of 1.01.02, 1.04.02, 1.05.06, 2.10.50, 2.10.01; a
  Memorix `full/1200,` request for AsdSAA 89 scan 377.
- **Draw:** `volumes.tsv` (31 rows; sha256 5b4d1cc5…c5ab): SG volumes outside all 233 dev-layer inventories (hand pool
  8,729, print pool 160).

### Step 24a: Vocabulary v2, listlike port, page_elements reader, archive presets

**What changed.** `datasets/vocabulary.py`: namespace `placement` with `PLACEMENT_TYPES`; generic types `salutation`,
`endorsement`, `reference`, `note`, `addition`, `noise`; subtypes `heading` {title, date, summary}, `closing` {formula,
dating, signature}, `running_title` {date}, `numbering` {page, folio, item, other}; `LEGACY_PLACEMENT_TYPES`,
`legacy_placement`. New `analysis/listlike.py` (verbatim port, then `LISTLIKE_CFG_C1`, `RULE_R1`, `RULE_R2`,
`LISTLIKE_CELLS`, `group_shares`) and `analysis/page_elements.py` (`read_layer_lines`, `layer_call`,
`listlike_records`, `type_family`, mapping tables). Seven `*_v5sg` archive presets in `text_layer/presets.py`. Docs:
`vocabulary.md`, new `page_elements.md`, `index.md`, changelog entry under *Unreleased*. As the planner found, the
presets' box-height pre-pass reports `n_chars 0`, so every inventory gets mode `hand` by method `default`; not fixed.

**Results.** Targeted tests: 66 passed (107 subtests). Full suite: 1,457 passed, 11 skipped (1,443 at planning).
Acceptance 4: `sorted(k for k in PRESETS if k.endswith('_v5sg'))` lists the seven keys and `voc_1.04.02_v5sg`.
Equivalence with shared-tools `listlike` holds on all fixtures and all six cells.

**Error analysis.** No vocabulary test changed its expectation (a frozen copy of the old generic table, plus the carrier
and position types, is still known). `listlike` on the 13-line two-group fixture (Dutch/French amounts, roman
numbering, `Item`, a dot leader, years): C0R0 flags 10 of 13, C1R0 flags 10 of 13, C0R1 and C1R1 13 of 13, i.e. no line
changes flag on this toy. The amount-end test itself differs on three lines, which C1 stops counting as amount ends:
`'Anno 1650'`, `'den 3 Maart 1700'`, `'Fait le 4 mai 1699'` (bare year digits). Window shares stay above threshold
because the neighbours still end in amounts, so C1's effect needs real volumes (F18-P.3, measured in 24d). The reader
was also run read-only on SG v5 inventory 12212 on the drive: 5,276 non-empty lines, no key or join errors, line types
main_text 4,137, index 584, marginalia 341, catch_word 71, page_number 54, other 33, date_header 21, running_header 20,
signature_mark 15; one line with two segments; region tag `Text` for all (SG has no Laypa tags, so `region_type` is None
there).

**Conclusion.** The library pieces are in place; nothing existing changed.

### Step 24b: Stage, build and sample

**What changed.** No library code. Scripts, logs and tables are in
[`reports/data/phase-18-page-element-types/step-24b/`](data/phase-18-page-element-types/step-24b/): `stage_inputs.py` (18 volumes staged under
`data/page_elements_inputs/`), `build.sh` and `build_log.txt` (seven `as-build-text-layer --stage all` runs into
`data/page_elements_layers/24b-a/`), `draw_pages.py` (`sample_pages.tsv`, `SHA256SUMS`; cap 300 lines for `sg_print` after the F18-24b.3 redraw, v1 in `sample_pages_v1_cap150.tsv`), `fetch_images.py`
(`image_checks.tsv`, images in `data/page_elements_images/`) and `dump.py` (`dumps/P001..P100`, `dump_stats.tsv`). Brief:
[`step-24b-sample-pages.md`](../plan/phase-18-page-element-types/step-24b-sample-pages.md). All seven builds ran
through `coreshare.py run` (`--want 4 --min 1`); the first one queued for under a minute, the rest started at once, so the
fallback of `--workers 1` without coreshare was not needed.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Layers, failures | seven layers, every staged inventory in `status.tsv`, 0 failures | 7 layers, 18 inventories (1.05.06: 5, 2.10.50: 4, 2.10.01: 5, 5023: 1, 5075: 1, 89: 1, AsnDA: 1), all stages `done`, `end stage=all failures=0` in all seven logs |
| 1. Staged file counts | equal `n_files` | all 18 equal (tarballs: files on disk = members = `n_files`; 2.10.01: five symlinks, scans-table rows 456, 356, 643, 570, 102 = `n_files`) |
| 2. Draw | 60 test, 40 dev | 60 test, 40 dev, 6/4 per stratum, 0 shortfalls; `sample_pages.tsv` sha256 `57fbd6fd5d8ebdf1c559c54f2ef4a6b8856705e0896a9c8c2ada16d3b22b3000` in `SHA256SUMS`; SG volumes not in any `text_layer_v2_dev` run; no scan drawn twice |
| 3. Images | one per page, aspect checks | 100 images, 0 retries, 0 aspect mismatches (`image_checks.tsv`): 50 `na_iiif` (SG 20, VOC 10, 2.10.50 10, 2.10.01 10), 10 `iiif3` (AsdSAA 89, info size equals the PageXML on all 10), 40 `thumb` (1.05.06, 5023, 5075, AsnDA; aspect within 2%) |
| 3. Dumps | one pair per page, line counts equal | 100 `.txt` + 100 `.lines.tsv`; each dump line count equals the page's non-empty line count (asserted, equals `n_lines_text`) |
| 4. Protect | differ only in new paths | VOC pair identical (0 diff lines). The main pair differs in 9,393 lines, all under `language-of-the-archive/output/text_terms/` (a running LotA job: `association/progress.log` grew and `bigrams_raw/inv=*/part-0.parquet` were rewritten); none in AS, none a path created here |
| 4. Full suite | green | 1,457 passed, 11 skipped, 142 subtests passed (76 s) |

Pages and lines per stratum (from `dump_stats.tsv`; short = text shorter than 3 characters after strip):

| Stratum | Test / dev pages | Lines | Short lines | Image source |
|---|---|---|---|---|
| sg_hand | 6 / 4 | 478 | 7 (1.5%) | NA IIIF |
| sg_print | 6 / 4 | 1,143 | 14 (1.2%) | NA IIIF |
| voc | 6 / 4 | 520 | 22 (4.2%) | NA IIIF |
| na_1.05.06 | 6 / 4 | 849 | 15 (1.8%) | thumb, 600 px (TIFF to PNG) |
| na_2.10.50 | 6 / 4 | 989 | 25 (2.5%) | NA IIIF |
| na_2.10.01 | 6 / 4 | 560 | 17 (3.0%) | NA IIIF |
| asdsaa_5023 | 6 / 4 | 1,030 | 97 (9.4%) | thumb, 600 px |
| asdsaa_5075 | 6 / 4 | 933 | 21 (2.3%) | thumb, 600 px |
| asdsaa_89 | 6 / 4 | 221 | 16 (7.2%) | Memorix IIIF 3 |
| asnda_0114.11 | 6 / 4 | 778 | 57 (7.3%) | thumb, 300 px |

Scans-table `status` counts and `mode/<inv>.json` per inventory: 1.05.06 49 `ok` 121 / `no_regions` 5, mode print;
1167 565 / 18, print; 808 301 / 7, hand; 1171 314 / 18 / `partial_shot_dropped` 6, print; 1191 241 / 5, hand;
2.10.50 1: 331 / 5, 619: 316 / 4 / `no_words` 1, 85: 201 / 2, 86: 114 / 20 / `no_words` 1 (all hand, method `default`);
2.10.01 250: 415 / 41, 2830: 352 / 4, 1649: 599 / 43 / `no_words` 1, 1616: 514 / 54 / `no_words` 2, 2542: 99 / 3 (all hand);
AsdSAA 5023 4: 230 / 2, print; 5075 2409: 460 / 12 / `no_words` 1, hand; 89 3.1: 1,021 / 14, hand; AsnDA 0114.11 1:
604 / 26, hand. Method is `box_height` for everything outside 2.10.50.

**Error analysis.**

- *What was read:* the dump and the image of the first page of each of the ten strata (P001, P007, P013, P019, P025,
  P031, P037, P043, P049, P055), whole dump for the order check and the picture for lines missing from the PageXML. The
  300 px AsnDA thumb (P055) and the 600 px thumbs are too small to count missing lines reliably.
- *Categories* (facts for the 24c rubric, not labels):

  | Page | Order of the dump | Missing from PageXML (visible on image) | HTR noise |
  |---|---|---|---|
  | P001 sg_hand (spread, 32 lines) | follows the page: left leaf column, then the centre `Memorie.` (r4), then right leaf; three right-margin amounts last | page number `31)` top right; right-edge pencil marks | `ver„` hyphen marks; "Avj=" for the year |
  | P007 sg_print (spread, 67 lines; redrawn page, v1 was scan 12212_0053) | follows the page, left leaf then right leaf, one region per paragraph | item numbers `2.`, `3.`, `4.`, `5.`; the signature `A 3` is read as `23` | blackletter misreads (`Reticul 1.` for `Articul 1.`, `(5)` read as `1`, `Rontsman`, `Aaaa-`); catchword `len`/`in` kept as lines; page number `(4)` present |
  | P013 voc (cover with paper insert, 54 lines) | partly: the title `3:e Batavia` first, then reading blocks by region; folio numbers (`F:o 1102`, `„ 1103 tot 1104`) are separate regions in between | stamp at the right edge; none else obvious | `„` for the dash, `1 1 03` pieces |
  | P019 na_1.05.06 (printed spread, 73 lines) | follows the page: region 1 (left leaf, 55 lines), region 2 (right leaf, 18) | page number `(2)` top of the leaf | long-s misreads ("Fiablissementen", "Kiablissementen") |
  | P025 na_2.10.50 (register spread, 92 lines) | one region per cell; roughly row by row but a row's cells are interleaved (`van Esselt`, `Genrit Anthonij`, `6. met Pappoort afgegaan`, then the next row) | the large initial `E`, the cell borders; the struck-through `Bat.` note in the right margin | `45. als voren` is correct, `,overleden.` stray comma |
  | P031 na_2.10.01 (letter, 36 lines) | **not reading order**: header block, then the address at the bottom, then the body lines in near-reverse order (regions 9 to 18 run from the bottom up) | printed `AFDEELING. BUREAU.`, the handwritten `1e`, the `4 M?` pencil mark | printed form text read as `Arpert100.`, `111`, `602240.` |
  | P037 asdsaa_5023 (notarial deed spread, 108 lines) | follows the page, the long main block first (r1, 40 lines) and the margin notes later; a few lines are out of order within the block (L29 before L30) | the left-margin notes next to the deed (about 4 short note lines), the page number, the signatures | `.` and `1` as one-character fragments at the gutter (L31, L33, 0.1 to 0.2 % wide) |
  | P043 asdsaa_5075 (inventory list spread, 72 lines) | follows the page, left leaf then right leaf, one region each | the page number `117` partly; no missing text lines seen | line wraps are separate lines (`Een koper braetspitt` / `met sijn` / `toebehoren,`) |
  | P049 asdsaa_89 (typed letterhead with handwriting, 42 lines) | top to bottom, but the letterhead is **reversed** (`13-1913.` before `„DE YROUW` before `TENTOONSTELLING`); one region for the header and the first body lines | the typed `Goedgekeurd bij Koninklijk Besluit` line start, `Dinsdags en Donderdags` partly, the red margin note `Dag Boskuil`, the red `476` (read as `716`) | cut words (`DAMRAK 28-`, `SPREEKUREN DAGELIJKSCH BESTU`), `Wutchen` for `wenschen` |
  | P055 asnda_0114.11 (deed spread, 63 lines) | follows the page: region 1 (left leaf, 38 lines), region 2 (right, 25) | not judged: 300 px | spelling noise (`gerkeuen`, `wrjgenden`) |

  Counts of what was read: ten pages, 10 of 10 dumps follow the page at region level; in 2 (P031, P049) the line or region
  order inside a block is reversed or scrambled; at least 6 of 10 images show text with no line in the PageXML (page
  numbers and running heads in P001, P019 and P007's right leaf; printed form text in P031; margin notes in P037 and P049); P043 none.
  Short fragments (shorter than 3 characters) are 1.5 to 9.4 % of the lines in a stratum, highest in AsdSAA 5023 (97 of
  1,030) where the gutter produces single-character lines.
- *Conclusion:* the line order in the dump is trustworthy for the printed and notarial volumes and for tabular spreads at
  region level, but not for letters with a letterhead or a form (P031, P049): the 24c rubric must tell the agents that
  the order of lines is not reading order there and to use the coordinates. Page numbers, running heads, item numbers and
  margin notes are often absent from the PageXML, so a gold built from the dump alone misses them; the check page has to
  show the image and let the user add them (F18-24b.5).

**Findings:** F18-24b.1 to F18-24b.6.

**Open issues.** none. The sg_print eligibility defect (F18-24b.3) was fixed by redrawing with a 300-line cap; P012 (3818 scan 0484, 7 lines) stayed because the seeded draw picked it again from the larger eligible set.

### Step 24c: Element gold

**Part A (tooling, done).** Files in
[`reports/data/phase-18-page-element-types/step-24c/`](data/phase-18-page-element-types/step-24c/):
`reader_prompt.md` (template with `{PAGE_IDS}`; type list with allowed subtypes from `GENERIC_TYPES`, placements, rubric
points 1-9 verbatim, the 24b facts, the list of files a reader may and may not open), `make_batches.py` and
`reader_batches.tsv` (22 batches, one stratum each, 2 per stratum, 2 to 6 pages, 110 to 516 lines; the batches with a
page of 238 to 242 lines (P011, P067, P068) hold 2 pages), `check_reads.py` (`--pages`, default all 100) and
`merge_reads.py` (writes `prelabels.tsv`, refuses to overwrite without `--force`). Both scripts were tested on synthetic reads
for P012 and P052 in a scratch directory (a good read: OK and 16 merged rows; a bad read: 8 errors of 6 kinds, merge
writes nothing); no synthetic read is left in `reads/`.

Design choices: the reader writes `line_id` as `-` and `merge_reads.py` fills the real id from the dump (a reader
copying 240 UUIDs would make avoidable errors); `check_reads.py` also checks the subtype against `GENERIC_TYPES`
(`is_known_tag` only checks the type), rejects the legacy `marginalia` and requires a note for `unclear = 1` and for
`custom:`. The reader's `k` is the number after `L` in the dump.

**Part B (agent reads, orchestrator, Opus 5.5).** 22 Sonnet readers (batches B01-B22 of `reader_batches.tsv`), `reader_prompt.md` unchanged;
all 100 reads written, `check_reads.py` OK on first submission (0 reruns for check errors). Prompt defect found by the orchestrator: the prompt said
a reader *may* open the image, not *must*; readers of B06, B10, B11, B12, B14, B15, B16, B17, B18 and B21 labelled some or all pages from the dump
only, several by a default in a script. Every batch got the same follow-up (open every image; re-read any label set by rule or block; rewrite only
where a label changes). Rows changed by the follow-up: B01-B05, B07-B10, B13, B18-B20 0; B06 6; B14 8; B15 6; B17 41 (blocks main to margin); B21 about 41;
B22 6; B16 103 (unclear flags only); B12 179 (P030, P077 `table` to `form:value`/`form:label`); B11 about 245 (P028, P029, P078 `table` to form/note; P080
on a second follow-up: 2 rows). The rubric was not changed. Caveat: many readers wrote their files through scratch scripts (labels by hand-chosen line
ranges), and several scratch scripts in shared `/private/tmp/claude-504/` directories were overwritten by other readers (B12 and B19 re-checked their
files); the self-check below is the independent check. The `table`/`form` boundary differs between readers (B12 kept P079 as `table`, B11 P080 as `table`
plus a `form:label` caption band); both count as bar-L positives.

**Part C (Sonnet 5.5, 2026-11-02).** `merge_reads.py` wrote `prelabels.tsv`: 7,501 rows (3,167 dev, 4,334 test), 100 pages, (scan_id, line_id) unique,
no `-` line id left. `annotations_list_before.txt` (3,138 files under `data/annotations`, with sizes) was written before part C. Files added in
[`step-24c/`](data/phase-18-page-element-types/step-24c/): `prelabels.tsv`, `selfcheck.tsv`, `gold/dev_labels.tsv`, `gold/SHA256SUMS`, `gold/dev_gold.done`,
`build_element_page.py` (with `element_page.css`, `element_page.js`), `element_page.html`, `summarise_elements.py` and `test_summarise_elements.py`;
UA-063 is in `docs/USER_ACTIONS.md`.

*Label counts per family and stratum (all 100 pages, 7,501 lines; `.` = 0).* Types with subtypes in full: running_text 3,782, table 953, list 757,
form:value 360, form:label 277, noise 193, closing:signature 180, note 175, heading:title 117, closing:formula 83, heading:summary 78, numbering:page 77,
addition 64, salutation 56, heading:date 54, endorsement 49, closing:dating 43, quire_marks:catchwords 39, custom:letterhead 38, numbering:other 36,
numbering:item 17, running_title:date 17, running_title 17, reference 10, quire_marks:signature 9, stamp:curatorial 8, numbering:folio 6, custom:copymark 5,
drop_capital:floriate 1. Placements: main 6,771 (dev 2,823, test 3,948), top 380, margin 248, bottom 68, interlinear 34. Unclear 325 (dev 151, test 174; 4.3%).

| stratum | addition | closing | custom | drop_capital | endorsement | form | heading | list | noise | note | numbering | quire_marks | reference | running_text | running_title | salutation | stamp | table | lines |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| asdsaa_5023 | 24 | 79 | 5 | . | . | . | 64 | 74 | 96 | 29 | 6 | . | . | 573 | . | 14 | . | 66 | 1030 |
| asdsaa_5075 | 10 | 30 | . | . | . | . | 33 | 472 | 3 | 33 | 18 | . | 1 | 11 | . | . | . | 322 | 933 |
| asdsaa_89 | 1 | 24 | 38 | . | . | 7 | 14 | . | 8 | 3 | 17 | . | . | 92 | . | 17 | . | . | 221 |
| asnda_0114.11 | 8 | 75 | . | . | 41 | 1 | 9 | . | 15 | 7 | 16 | . | . | 565 | . | . | . | 41 | 778 |
| na_1.05.06 | . | 27 | . | . | . | . | 27 | . | 35 | . | 20 | 19 | . | 716 | . | 5 | . | . | 849 |
| na_2.10.01 | 3 | 25 | . | . | 1 | 61 | 16 | . | 7 | 2 | 8 | 4 | . | 313 | 4 | 5 | 8 | 103 | 560 |
| na_2.10.50 | . | . | . | . | . | 568 | 16 | . | 6 | 72 | 14 | . | 1 | 42 | 12 | . | . | 258 | 989 |
| sg_hand | 15 | 12 | . | . | . | . | 10 | 72 | 4 | 13 | 9 | 1 | . | 330 | 1 | 2 | . | 9 | 478 |
| sg_print | . | 27 | . | 1 | 7 | . | 31 | 18 | 14 | 1 | 19 | 17 | . | 983 | 12 | 13 | . | . | 1143 |
| voc | 3 | 7 | . | . | . | . | 29 | 121 | 5 | 15 | 9 | 7 | 8 | 157 | 5 | . | . | 154 | 520 |
| all | 64 | 306 | 43 | 1 | 49 | 637 | 249 | 757 | 193 | 175 | 136 | 48 | 10 | 3782 | 34 | 56 | 8 | 953 | 7501 |

*Custom lines (all 43, verbatim notes).* `custom:letterhead` 38 lines (P049, P051, P093-P096: printed letterhead on letter pages) and `custom:copymark`
5 lines ("Copie/Copia" labels, P039, P040, P086, P088).

| page | k | type | placement | note (verbatim) |
|---|---|---|---|---|
| P039 | 1 | custom:copymark | top | Copie/Copia label |
| P040 | 1 | custom:copymark | top | Copie/Copia label |
| P049 | 3 | custom:letterhead | top | printed letterhead |
| P049 | 4 | custom:letterhead | top | printed letterhead |
| P049 | 5 | custom:letterhead | top | printed letterhead |
| P049 | 6 | custom:letterhead | top | printed letterhead |
| P049 | 7 | custom:letterhead | top | printed letterhead |
| P049 | 10 | custom:letterhead | top | printed letterhead |
| P049 | 11 | custom:letterhead | top | printed letterhead |
| P049 | 12 | custom:letterhead | top | printed letterhead |
| P049 | 14 | custom:letterhead | top | printed letterhead |
| P049 | 15 | custom:letterhead | top | printed letterhead |
| P051 | 1 | custom:letterhead | top | printed letterhead |
| P051 | 2 | custom:letterhead | top | printed letterhead |
| P051 | 3 | custom:letterhead | top | printed letterhead |
| P051 | 4 | custom:letterhead | top | printed letterhead |
| P051 | 7 | custom:letterhead | top | printed letterhead |
| P051 | 8 | custom:letterhead | top | printed letterhead |
| P051 | 9 | custom:letterhead | top | printed letterhead |
| P051 | 11 | custom:letterhead | top | printed letterhead |
| P051 | 13 | custom:letterhead | top | printed letterhead |
| P086 | 41 | custom:copymark | top | Copie/Copia label |
| P088 | 4 | custom:copymark | top | Copie/Copia label |
| P088 | 63 | custom:copymark | top | opye = Copye fragment |
| P093 | 2 | custom:letterhead | top | printed ministry letterhead |
| P094 | 3 | custom:letterhead | top | printed letterhead |
| P095 | 3 | custom:letterhead | top | printed letterhead |
| P095 | 5 | custom:letterhead | top | printed letterhead |
| P095 | 7 | custom:letterhead | top | printed letterhead |
| P095 | 8 | custom:letterhead | top | printed letterhead |
| P095 | 9 | custom:letterhead | top | printed letterhead |
| P095 | 10 | custom:letterhead | top | printed letterhead |
| P095 | 12 | custom:letterhead | top | printed letterhead |
| P095 | 13 | custom:letterhead | top | printed letterhead |
| P096 | 3 | custom:letterhead | top | printed letterhead |
| P096 | 4 | custom:letterhead | top | fragment of TENTOONSTELLING in letterhead |
| P096 | 5 | custom:letterhead | top | printed letterhead |
| P096 | 8 | custom:letterhead | top | printed letterhead |
| P096 | 9 | custom:letterhead | top | printed letterhead |
| P096 | 10 | custom:letterhead | top | printed letterhead |
| P096 | 11 | custom:letterhead | top | printed letterhead |
| P096 | 13 | custom:letterhead | top | printed letterhead |
| P096 | 14 | custom:letterhead | top | printed letterhead |

*Unclear lines (40 of 325, drawn with `random.Random(20261102).sample`, notes verbatim).*

| page | k | split | type | placement | note (verbatim) |
|---|---|---|---|---|---|
| P001 | 15 | test | note | main | Memorie, flourished remark under entry |
| P005 | 45 | test | heading:date | margin | marginal date and archive reference of the missive |
| P017 | 2 | test | heading:summary | margin | marginal remark about the list |
| P021 | 1 | test | numbering:page | top | page number 148 split over two lines (L1 '1', L2 'f 48') |
| P024 | 63 | test | noise | main | stray '17' inside text, in left margin area of body |
| P028 | 33 | test | form:value | main | fragment next to 'den' in birth entry, unreadable |
| P028 | 67 | test | form:value | main | crossed-out text in appointment column |
| P029 | 73 | test | note | main | pencil circled remark, badly read |
| P032 | 41 | test | numbering:item | margin | small margin mark 2e. |
| P037 | 85 | test | closing:signature | main | Coll. Corver collation note |
| P037 | 87 | test | note | margin | marginal note on lieutenant Alexander Hinojosa |
| P046 | 54 | test | note | margin | narrow inserted text between the two columns; type/placement unsure |
| P046 | 68 | test | note | margin | narrow inserted text between the two columns; type/placement unsure |
| P047 | 91 | test | addition | margin | inserted item left of column, unclear placement |
| P048 | 25 | test | heading:title | main | opening of inventory, long preamble sentence |
| P048 | 26 | test | heading:title | main | opening of inventory, long preamble sentence |
| P059 | 33 | test | heading:title | main | unclear |
| P061 | 15 | dev | addition | interlinear | small inserted word |
| P061 | 87 | dev | running_text | main | faint text, bleed-through or later insertion |
| P061 | 101 | dev | noise | main | number-like garbage near a paraph |
| P061 | 102 | dev | heading:date | bottom | date at page foot, probably opening next sitting |
| P062 | 37 | dev | noise | margin | fragments at right edge |
| P069 | 61 | dev | numbering:item | main | item number at foot of left leaf |
| P074 | 113 | dev | numbering:other | top | '28' at far right top edge, outside text, probably archive numbering |
| P078 | 37 | dev | form:value | main | stray number (56) in disposal column |
| P079 | 43 | dev | form:label | main | garbled printed caption |
| P081 | 6 | dev | stamp:curatorial | main | printed receipt label with handwritten entries |
| P082 | 28 | dev | form:label | main | column headers 3-4, order mixed |
| P083 | 10 | dev | note | main | 'Coll.' collation mark |
| P088 | 106 | dev | noise | main | Hooft, stray word above text |
| P089 | 63 | dev | table | main | small word before item |
| P091 | 43 | dev | heading:title | main | subheading in inventory (room/category) |
| P091 | 102 | dev | heading:title | main | subheading in inventory (room/category) |
| P093 | 1 | dev | numbering:other | top | struck-through number 140 plus archivist number; unsure folio vs other |
| P096 | 28 | dev | closing:signature | main | signature, transcription differs |
| P097 | 9 | dev | noise | margin | scribbled figures in margin |
| P097 | 10 | dev | noise | margin | scribbled figures in margin |
| P099 | 63 | dev | closing:signature | main | may be part of endorsement |
| P100 | 69 | dev | endorsement | main | registration note, guess |
| P100 | 71 | dev | endorsement | main | registration note, guess |

**Self-check (30 lines).** 30 lines drawn with `random.Random(20261102).sample(range(7501), 30)` (19 test, 11 dev; 22 pages), each judged against the dump
(four lines either side with their coordinates) and the page image opened with the Read tool. **Agreement: 29 of 30; 1 disagreement.** For P021, P023, P024,
P059 and P099 only 300-600 px thumbnails exist (single words cannot be read); there the judgement rests on the dump neighbours and the visible layout.
Verdicts are in `selfcheck.tsv` (one row per line, comment column); the 7 lines with a remark:

| page | k | type | placement | verdict | comment |
|---|---|---|---|---|---|
| P017 | 8 | table | main | agree | account line with amounts, table |
| P044 | 50 | list | main | agree | inventory item, list |
| P044 | 68 | list | main | type wrong | list->note or running_text: the main-column lines 66,68,71 ('plaetsen met mijn Notariael signet / over beijde deuren op een touwetje / versegelt') read as one sentence with the margin block lines 64-72 (labelled note/margin); the two halves must carry one type. Judged on a 600 px thumb, so low confidence |
| P047 | 54 | list | main | agree | list for the Item text, amounts in a separate table band; the list/table split of account lines is debatable but within the table/list family the user will correct |
| P051 | 8 | custom:letterhead | top | agree | custom:letterhead, printed top-left block of the letterhead |
| P068 | 199 | heading:summary | main | agree | heading family is right; the italic 'Extract uyt het Reces' line is subtype title rather than summary (not counted: family and placement correct) |
| P087 | 58 | table | main | agree | table for account-entry text plus amount column; text-only lines of a ledger entry are debatable (list vs table) |

Disagreement: **P044 k68** (type wrong): the main-column lines 66, 68 and 71 ("plaetsen met mijn Notariael signet / over beijde deuren op een
touwetje / versegelt") read as one sentence together with the margin block lines 64-72, which the reader labelled `note`/margin; the two halves must
carry one type, and `list` is the odd one. Judged on a thumbnail, so low confidence. Observations that are not disagreements: account lines of
the P017, P047 and P087 kind are split into `table` or `list` text plus a `table` amount band, a call a user would plausibly change; P068 k199
("Extract uyt het Reces", italic) is labelled `heading:summary` where `heading:title` is as defensible. With 1 of 30 wrong (limit 6), the dev gold was frozen.

**Dev gold.** `gold/dev_labels.tsv`: the 3,167 dev rows of `prelabels.tsv` (40 pages), sha256
`e0f2a4de7c24f23ca596941e39b0b56eabc8ed603f0aa51b123498568d4dc904` (`gold/SHA256SUMS`, `gold/dev_gold.done`, written 2026-11-02 21:56 UTC after the
self-check).

**Check page.** `element_page.html` (60 test pages in `page_id` order, pre-labels embedded, about 1 MB): images by a path relative to the HTML file
(`../../../../data/page_elements_images/<page_id>.<jpg|png>`), so it works when opened from `step-24c/`; the zoom swaps to
`{base}/full/2400,/0/default.jpg` beyond 1.5x for the 30 `na_iiif` and 6 `iiif3` test pages (the 24 thumbnails have no higher resolution). The rubric points
1-9, the type definitions and the key legend are in a collapsible block at the top; hovering a row highlights its box and the reverse; boxes carry the dump
position k; keyboard handling is off while any input, select or textarea has focus; placement keys use `event.code` (Shift+Digit1..5). The page shows no
layer type, layout role, `listlike` output or PageXML region tag (grepped). *Checked with:* `node --check` on the extracted inline script (OK), and Google
Chrome headless (`--dump-dom`, `--screenshot`) on the page and on a copy with an injected test: first page rendered (32 boxes and rows, image 1200 px
wide), row click, Shift+click range, `h` then `2` gave `heading:summary`, Shift+Digit2 gave margin, `u` toggled, a key typed in the page note changed
nothing, Cmd+Z undid, Enter marked reviewed and moved on, the export has 60 items with merged lines; no script error. The mouse wheel, drag and the
remote zoom swap were not exercised (no gesture driver); the viewer logic is adapted from the 23a page.

**Part D script.** `summarise_elements.py` (options `--verdicts`, `--gold-dir`, `--prelabels`, `--dev-labels`) and `test_summarise_elements.py`: on a synthetic
export (page A reviewed with one changed line, B reviewed unchanged, C unreviewed with an edit) it wrote 4,334 `test_labels.tsv` rows, `changed` = 1 on A,
0 on B, 1 on C (reviewed 0), 59 annotation files (the 40 dev pages, the 2 reviewed test pages and the per-inventory `elements-*.json` files), the unreviewed
page left out of the annotations, the element runs of page A equal to its `start` count (11) and covering all its lines, and a second run refused to overwrite
`test_gold.done` (exit 2). The scratch output is not kept in the repository.

**Part D, after UA-063 (Sonnet 5.5, 2026-11-17).** `gold/test_gold.done` 2026-11-17T10:57:25Z; `test_labels.tsv` sha256 286b3ad0...fb41 (matches `gold/SHA256SUMS`);
`summarise_elements.py` was not rerun. `annotations_list_after.txt` (written 2026-11-17 with the same `find ... | sort` and sizes) is byte-equal to `_before` (3,138
lines, `diff` empty): `data/annotations/` was not touched. Analysis script `step-24c/analyse_after.py` writes `step-24c/after/` (`overall`, `by_stratum`,
`by_gold_family`, `by_pre_family`, `top_changes`, `top_change_examples`, `pages` as TSV). All 60 test pages reviewed; 4,334 lines; 175 unclear (4.0%).

Changed share (any of type, placement, start differs from the pre-label): **648 of 4,334 lines, 0.150**. Of those, type changed on 618 (family on 616), placement on 2,
start on 61. So the user's corrections were almost only type corrections; the agents' placements were accepted (placement agreement 0.9995).

| Stratum | lines | changed | share |
|---|---|---|---|
| na_2.10.50 | 525 | 238 | 0.453 |
| asdsaa_5075 | 558 | 224 | 0.401 |
| asnda_0114.11 | 461 | 82 | 0.178 |
| sg_hand | 255 | 37 | 0.145 |
| asdsaa_5023 | 629 | 45 | 0.072 |
| na_2.10.01 | 395 | 18 | 0.046 |
| voc | 325 | 4 | 0.012 |
| asdsaa_89, na_1.05.06, sg_print | 117, 571, 498 | 0 | 0.000 |

Per gold type: `table` 556 of 1,124 (0.495), `note` 57 of 155 (0.368); every other family 0.000 to 0.100 (`running_text` 19 of 2,175 = 0.009, `list` 4 of 278,
`heading` 4 of 84, `form` 2 of 78, `closing` 2 of 134, `custom` 2 of 23, `reference` 1 of 10, `quire_marks` 1 of 29; `addition`, `endorsement`, `noise`, `numbering`,
`running_title`, `salutation` 0). 87% of all changes (556 of 648) land on gold `table`.

Twenty most frequent pre to gold changes (type only, `tables/prelabel_changes.tsv` in 24d; examples are page and line index `k`, text from the dumps):
list to table 184 (P045 k3 "obligatie van twee duijsent gul. houdende", the prose-description inventory ruling of UA-070); form:value to table 169 (P029
k17 "bevoren te Utrecht", k18 "26 Nor: 1815 uit Texel."); running_text to table 104 (P001 k2 "schenen 12en. April Avj=", k3 "vijffentachtentigh per"); form:label to
table 57 (P027 k2 "Doopnaam", k3 "Daam"); heading:summary to note 27 (P038 k2 "op 't Veer", k3 "van"); addition to note 16 (P038 k13 "seker bequaem", k14 "persoon
tot Collecteur"); heading:title to running_text 16 (P048 k25 "van Hosias Rengersz op den achtsten februarij @o xvjc."); running_title to table 12 (P025 k3 "Obsenvatien",
note "column heads of ledger"); form:label to note 8 (P031 k8 "reau, de Dasteekenins"); heading:title to table 5; table to note 4 (P002); salutation to
heading:summary 3; running_text to note 2; and eight single changes (list to reference, closing:formula to quire_marks:catchwords, table to heading:title, ...). The
margin summaries and additions the agents called `heading:summary`/`addition` became `note` (43 lines), following the ruling that a marginal remark about the text is a
`note`, while the placement stayed `margin`. The agents' main failure was therefore the table/list/form boundary on ledger and register pages (2.10.50 P029, P030; 5075 P045 to P048), not the
text types.

Page notes (9 of 60): P001 (accounts: "perhaps table is not the type of a line", every line is running text in a cell), P013 (line 40 merges a left-margin reference and a list line; a smaller inserted folio),
P017 (line 8 merges `heading:summary` with `table`), P038 (low-resolution image, unsure about `addition` lines), P042 (index page, all `list` lines are index entries),
P043 (probate inventory), P045 (list items that are paragraphs of running text), P047 (what separates table from list, and `running_text` against `list`; whether every row is a start),
P048 (the preamble sentence is running text, unsure whether it is a heading). Tokens in `note`: the user wrote no `colhead` token; table column heads carry free text ("column heads of ledger",
"column heads of both pages", 12 lines on P025, P026); `cont` is present on 4 lines (P006, P017, P035, P037) but on non-table types; `prose` is on 641 lines of 16 pages (P045 to P060),
318 of them `running_text` and 298 `table`, not only on P045 to P048 (see Brief defects).

**Findings:** F18-24c.1 to F18-24c.6.

*Status:* test gold frozen; part D analysis done.

### Step 24d: Measure

> **Dev stage 2026-11-02; test stage 2026-11-17 (Sonnet 5.5).** The test results are in the subsection *Test stage* at the end of this section. Everything below is measured on the **dev gold, which is agent-read** (40 pages, 3,167 lines, sha256 e0f2a4de...c904), so it is for selection and reading, not for the bars. `eval_elements.py --stage test` refuses now (`refuse: gold/test_gold.done missing (UA-063 open)`); `test_labels.tsv` and the test rows of `prelabels.tsv` were not opened.

**What changed.** `reports/data/phase-18-page-element-types/step-24d/eval_elements.py` (both stages, `--selftest`: hand-computed P/R, Wilson, deterministic bootstrap, the tie rule, the date-header rule, the three refusal rules, the 1:1 join assertion). Outputs: `dev_lines.tsv`, `dev_grid.tsv`, `select.json`, `tables/dev_*.tsv`, `cases_dev_*.tsv`. Join: the 40 dev pages match their layer lines 1:1 on (scan_id, line_id), 3,167 of 3,167. `prelabel_agreement.tsv` needs the user's gold and is test-only; not produced.

**Dev grid, bar L** (lines with `unclear == 0`; gold `running_text` negative, `list`/`table`/`form` positive; 949 positives, 1,505 negatives):

| Cell | P | R | F1 | TP | FP | FN |
|---|---|---|---|---|---|---|
| C0R0 | 0.781 | 0.060 | 0.112 | 57 | 16 | 892 |
| C0R2 | 0.793 | 0.048 | 0.091 | 46 | 12 | 903 |
| C0R1 | 0.862 | 0.407 | 0.553 | 386 | 62 | 563 |
| C1R0 | 0.758 | 0.026 | 0.051 | 25 | 8 | 924 |
| C1R2 | 0.828 | 0.025 | 0.049 | 24 | 5 | 925 |
| C1R1 | 0.882 | 0.410 | 0.560 | 389 | 52 | 560 |

Selected: **C0R1** (max F1 is C1R1 0.560; C0R1 is within 0.01 and earlier in the tie order). Its dev intervals: P 0.862 (Wilson 0.827 to 0.891; bootstrap 0.704 to 0.951), R 0.407 (Wilson 0.376 to 0.438; bootstrap 0.260 to 0.558). The bar (P >= 0.90, R >= 0.80) would fail on dev; this says nothing yet about the test verdict.

**Bar M on dev** (gold margin 61 positives, gold main 2,732 negatives; `top`, `bottom`, `interlinear` lines are outside the bar): P 0.227 (Wilson 0.181 to 0.280; bootstrap 0.077 to 0.533), R 1.000 (61/61). Per stratum: voc 26 TP, 1 FP; AsdSAA 5023 26 TP, 10 FP; AsnDA 7 TP; 1.05.06 2 TP; 2.10.50 0 TP, 161 FP; 2.10.01 19 FP; sg_hand 10 FP; sg_print 3 FP; AsdSAA 89 4 FP. Gold has no margin lines in the five strata with FPs only.

**Error analysis (dev, agent gold; test reads come after UA-063).** Read with the two lines above and below (`cases_dev_*.tsv`).
- *L false positives* (62; 30 sampled with `random.Random(20261102)`): 13 ragged prose lines in a narrow column (P061 "ten van Seulent an"; P072 "generaal beneevens d' Edele"; P087, P088; AsdSAA 5075), 10 appointment/name lines of P084 (2.10.50: "F. 13. Veltman,", "A. IiJske en 8. Canela,") gold `running_text` but read as an item list (possible gold error), 7 dated or number-ending lines of the P079 ledger page ("Res 3 Sertemter 1838 N16"), where gold has the surrounding lines as `table`.
- *L false negatives* (563; 30 sampled): 15 list items or continuation lines of long-item lists (P063 dated entries "14 meij."; P067 name lists "Bergsma van Isma."; P069 "Retour scheepen."; P092 obligation entries "laste van Joan paulsen, ten behoeve"), 10 form lines (P077 split captions "de Kolonie vertrokken of in het"; P079/P080 caption fragments "en", "parour."; P093 "29 November jl."), 5 account item lines without an amount (P084 "een Konponaal."; P089 "porceleijne kopjens, tien stuijv. ƒ „-10"; P090 "ƒ 9„"). No category outside the fixed list occurred, except that none was clearly a gold error.
- *M false positives* (208; 20 shown): 12 `rule:extra_role` (layout role `extra`: P077 "kinderen", "op Juxa toegekend"; P078 "Barxalling"; P079 "van het", "Overshock"), 8 `rule:page_margin` (role `main`: P078 "Gehuwd te"; P079 "waarop het"; P082 "2."; P085 ","). All 20 are gold `form`, `table`, `noise` or `heading:title` in the main column. Over the whole pool: 149 `extra_role`, 59 `page_margin`; gold family form 111, table 59, noise 11, list 9, note 5. No false negatives (61/61): of the 85 gold margin lines, 77 carry `rule:extra_role`, 6 `rule:page_margin`.
- *Layer types* (five largest off-diagonals, 10 lines each in `cases_dev_layer_types.tsv`): list to running_text 284 (every `main_text` by `rule:default`: dated entries, name lists, wrapped items), table to running_text 255 (account items and amounts split over lines), form to margin 172 (`extra_role` or `page_margin` on form captions of P078 to P082), closing to running_text 161 (signatures and "Was geteekent,"), form to running_text 148. The layer has no list, table or form type to be right with.
- *Pre-labels*: test only; not read.

**Conclusion (dev, provisional).** For lists and tables the thresholds, not the features, lose on dev: recall is 0.41 at best and C1 (no bare final digit) makes no real difference once the third condition is loosened (R1). Recall collapses on hand-written and print SG pages (0.06 to 0.14) where a list is a column of wrapped items. For margin the rule that makes second columns and form fields `extra`/`page_margin` explains 208 false positives; phase 19 should split margin from table/form columns before anything else. Gold soft spots (table/form boundary; account lines) touch both bars on 2.10.50 and cannot be separated from method error until the user's gold exists.

**Findings:** F18-24d.1 to F18-24d.6 (dev), F18-24d.7 to F18-24d.14 (test).

#### Test stage (2026-11-17)

Before the run: `select.json` (cell C0R1, 2026-11-02T23:07+0100) predates `test_gold.done`; test gold sha256 286b3ad0...fb41 unchanged; `annotations_list_after.txt` equals `_before`; no
`test_done.json`. `eval_elements.py --selftest` ok. The run (once, from the AS root: the layer roots in `sample_pages.tsv` are relative; a first start from the step directory failed on
a missing VOC path before reading any gold row and wrote nothing; see Brief defects): 60 of 60 test pages reviewed, 0 excluded, 1:1 join on every page. `test_done.json` 2026-11-17T11:04:42Z, hashes of `test_labels.tsv` and `select.json` (c8c5fb32...8f0).
Selection was not changed: no retuning.

| Bar | cell/call | TP | FP | FN | P | R | P Wilson | R Wilson | P bootstrap | R bootstrap | Verdict (P >= 0.90, R >= 0.80) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| L | C0R1 (selected) | 836 | 44 | 619 | 0.950 | 0.575 | 0.934 to 0.963 | 0.549 to 0.600 | 0.898 to 0.977 | 0.498 to 0.655 | **fail** (R) |
| L | C0R0 (as handed over) | 87 | 3 | 1,368 | 0.967 | 0.060 | 0.907 to 0.989 | 0.049 to 0.073 | 0.800 to 1.000 | 0.018 to 0.124 | fail (R) |
| M | layer `marginalia` | 82 | 234 | 12 | 0.259 | 0.872 | 0.214 to 0.311 | 0.790 to 0.925 | 0.087 to 0.497 | 0.636 to 1.000 | **fail** (P) |

L: 1,455 positives (`list`/`table`/`form`: 278, 1,124, 78 gold lines less the unclear) and 2,153 `running_text` negatives. M: 94 margin positives, 3,875 main negatives.
Per stratum (`tables/bars_by_stratum.tsv`): bar L recall of C0R1 is 0.74 (AsdSAA 5023), 0.64 (5075), 0.63 (AsnDA), 0.65 (VOC), 0.50 (2.10.50), 0.44 (2.10.01) and 0.04 (sg_hand, 1 of 28); precision is 1.00 on 5075, 2.10.50 and
0.99 on VOC, but 0.81 on 5023 and 0.81 on 2.10.01. Strata without positives give only false positives (sg_print 6, 1.05.06 3, AsdSAA 89 4). Bar M precision is 0.87 and 0.92 on AsdSAA 5023 and 5075, 0.89 on sg_hand, 0.00 on
2.10.50 (157 FP, 0 positives) and 2.10.01 (33 FP).

**Error analysis, bar L (selected cell C0R1).**
- *False positives* (44; all read, 30 shown in `cases_test_L_fp.tsv`): all 44 have gold `running_text`. 31 are ragged lines of a narrow text column or a line that wraps a sentence in the middle, with the neighbours being
  running text too (P041 7 and P034 6 lines of single-sentence entries: "Aan Aen ellinister van Staat, Kommissaris Generaal"; P039 "Indien de Supplte. geen 5 pr. cto", "Instructie strijdig is als voorss. is."; P011
  "Ridderschap en Steeden het voorschreeve", "Rapport geaxgreeert."); 10 carry the note `prose` (P049, P052, P055, P058; AsdSAA 89 letters and AsnDA deeds, not the inventory tables of the UA-070 ruling):
  paragraph-final short lines ("Mei met alles klaar te komen.", "nde onderteekend.") and deed lines that spell out sums (P058 "tien gulden zes en tarhtig cent");
  on these pages the user's `prose` marks running text, so the token is not one flag across the test pages (orchestrator correction, 2026-11-17); 3 merged number and text lines (P034 "Is na deliberatie goedgevonden en Verstaan", notes
  "merged item number 7116 + text", P021 after "ƒ 48"). The split into the first two groups follows the `note` column and my reading of the case rows, not a count of every line. None is a gold error that I could identify on the text. Brief categories: "short prose lines" and "numbering or references" apply, "headings", "noise" and "gold error" had 0.
  The wrong-flag cause is the same on all of them: the second condition of the list rule (several short lines in a row) is met by narrow columns. Page spread: P041 7, P034 6, P039 4, P058 4, P011 3, P049 3, 11 pages with 1 to 2.
- *False negatives* (619 of 1,455; 30 sampled with `random.Random(20261102)`, `cases_test_L_fn.tsv`; gold `table` 474, `list` 108, `form` 37; 239 in 2.10.50, 159 in 5075, 78 in VOC, 54 in 2.10.01, 43 in AsnDA,
  27 in sg_hand, 19 in 5023; six pages hold 295 of them: P030 64, P036 53, P045 50, P028 44, P046 44, P059 43). In the sample of 30: 17 are table cells or wrapped entries written as prose lines that
  continue a row (P001 "klocke geluijt te hebben", P029 "plaatst by de Marne", P030 "ontslagen", P045 "Een gesegelde Rentebrieff van achthondert gul...", P046 "van verlegen quinttijner floers", P059 "gedeelte van nomen
  goevermeld in koarting 4 n..."); 7 are items of name or item lists written as prose lines (P044 "Een wit schoorsteenvalletje", "ditto valletjens"; P018 "Een slavin diana van timor"; P025 "van Emmerik"; P026 "acques"); 5 are
  fragments with an amount or ledger digits on a line of their own (P046 "ƒ 120", "ƒ 64„"; P014 "12: 25 p=s rode path: zijde pan 10 astas de t"; P036 "21. 1,,1,, 29, 6,, 50. 1, 1,, 12 2."), a category the brief
  did not list (verbatim examples given: add as "amount fragments"); 1 is a form value (P028 "ent Overleden, op de leinglei naar Nederland"). 0 gold errors found. All 30 are
  `main_text` by `rule:default` except three the layer called margin or date line. Token read: of the 619 FN, **125 carry `prose`**, 18 `amount column` (4 of them with `prose`), 43 the note "ledger rows of packages" and 12 lines with "column head": the proxy
  recall of the selected cell on column-head lines (note contains "column head", gold not `running_text`, 12 lines) is 8 of 12 (0.667); no line carries the literal token `colhead`. No recall on `prose` is reported (UA-072 (a)): 125 of the 298 `table` lines with that
  token are missed (0.58 flagged), but the token covers P045 to P060 inconsistently (see Brief defects).
- *What else C0R1 flags* (`tables/listlike_by_type.tsv`): running_text 0.021; running_title 0.875, custom 0.826, list 0.612, reference 0.600, table 0.569, endorsement 0.563, salutation 0.531, numbering 0.493, form 0.487,
  closing 0.470, addition 0.407, heading 0.393, noise 0.379; C0R0 flags 0.1 or less of everything but custom (0.565), running_title (0.375), addition (0.222), salutation (0.219), reference (0.200), heading (0.179), closing (0.172).

**Error analysis, bar M.** 234 FP (20 read in `cases_test_M_fp.tsv`) and 12 FN (all). FP: 166 `rule:extra_role` (layout role `extra`), 68 `rule:page_margin` (role `main`). By gold family 160 `table`, 18 `form`, 15 `noise`, 12
`heading`, 8 `reference`, 8 `closing`, 7 `note`, 4 `salutation`; by stratum 2.10.50 157 (P025 44, P029 33, P036 29, P030 27, P027 21), 2.10.01 33, VOC 18, sg_print 11, AsdSAA 5023 9. Of the 20 read: 15 are cells of a table column that the layer
puts into a second column or right-hand strip ("Dick", "Axie.", "Johan", "Thomas", "Christiaan", "109.", "overleden"), 3 table or header cells in the page's right strip (`rule:page_margin`: "Joh: Josephus", "Hendrik Hieronimus",
"28 „"), 2 top or closing lines ("Post Datum.", "Namens het Dagelyksch Bestuur:"). No gold errors found. These rows are consistent with the 24d dev finding: the margin call is the layout role `extra`, which treats a second
column as margin. FN (12): 11 on P044, a left strip (x median 2,529 against 2,915 for main) of a probate inventory, lines gold calls `note` (5, "notarial remark about the sealed cabinet") and `closing` (6, the actum line and witnesses: "Jacob Adriaensz als getuijgen", "J:de.Winter N.P"), which
the layer keeps in the main column (`main_text`, `rule:default`); 1 on P004 ("hij vdoght hier van copne", "marginal remark left of main text"). Gold margin on P044 is a judgement (the strip is
indented by 400 px, 19 lines); they could also be read as an indented main block, so I do not call it a gold error.

**Layer types** (`tables/layer_types.tsv`, pooled; five largest off-diagonal cells, 10 lines each in `cases_test_layer_types.tsv`): table to running_text 881 (all `rule:default`; ledger cells, wrapped inventory lines and
amount fragments: P025 "80. Overbobie in Indie gepasponteerd.", P028 "Namen en Soenamen.", P047 "ƒ 400", P059 "het geheel groot tien vestoenn twee en twintig..."), list to running_text 273 (P018 "Een slavin diana van timor", P042
"Nimmeeghse schepen zyn Zalaris 139", P043 "Twee preeckstoelen met neusdoecken,"), table to margin 190 (P025 "Genrit Anthonij", "168.", P036 "5716 54": `rule:extra_role` 8, `rule:page_margin` 2 of the 10), closing to running_text 116 (P037 "Cornelis
van Vloonwyck, heere van Vlooswyck", P038 "Amstelredamme den 27 aprill 1657. Was", P040 "Wigbolt Slicher": signatures and dating lines), note to margin 112 (P038 "seker bequaem", "Burgermrn.", P046 "lhier breder na luijt": the margin call is right as a place, wrong as a type,
since gold type is `note` or `addition`). The layer never calls `list` (0 of 278) and its `custom` calls (95) are 46 noise and 42 table lines. P and R of the layer calls: running_text P 0.586 R 0.998, numbering P 0.824 R 0.575,
quire_marks P 0.667 R 0.828, running_title P 0.150 R 0.375, list 0 of 278, custom P 0.000. Region tags (VOC, `tables/region_tags_voc.tsv`): the provider's Laypa tags give `none` for most lines of every family (list 210 of 278, table, form, closing 128 of 134); they agree with
gold only on 2 of 4 numbering and 1 heading line; no line carries a list or table tag.

**Pre-label agreement** (`tables/prelabel_agreement.tsv`): overall type-family agreement 0.858, placement 0.9995, start 0.986, changed 0.150. By gold family: `table` 0.527 (changed 0.495), `note` 0.632 (0.368), all others 0.95 to 1.00. By stratum: 2.10.50 0.547, 5075 0.642,
AsnDA 0.822, sg_hand 0.863, 5023 0.929, 2.10.01 0.962, VOC 0.997, and 1.000 for AsdSAA 89, 1.05.06 and sg_print. The top changes are in the Step 24c section.

**Brief defects (24c part D, 24d test).** (1) The 24d brief names no working directory, but `sample_pages.tsv` holds relative `layer_root` paths (`data/text_layer_voc_23b/...`), so `eval_elements.py` must run from the AS root; started elsewhere it fails before reading any gold row (it wrote nothing; `test_done.json` is written last). Add "run from the AS root" to the brief. (2) The brief's note token `colhead` does not occur in the test gold: the user wrote free text ("column heads of ledger", "column head(s) of both pages", 12 lines on P025, P026); I matched "column head" instead. (3) The token `prose` is on 641 lines of 16 pages (P045 to P060, 318 `running_text`, 298 `table`, 10 closing, 4 heading), not only on P045 to P048 as UA-072 (a) states, so a `prose` recall would be inconsistent between pages; none is reported. (4) `cont` occurs on 4 lines of non-table types (P006, P017, P035, P037) and not on table lines. (5) The FN category list lacks lines holding only an amount or ledger digits (5 of 30 read); added with verbatim examples as the brief allows. (6) Step 24c part D asks for "20 most frequent pre to gold changes": counted on type (24d `prelabel_changes.tsv`) and, in `step-24c/after/top_changes.tsv`, on type plus placement plus start; both agree on the ranking of the first four.

**Conclusions.** *Bar L fails on recall* (R 0.575, interval 0.50 to 0.66, far below 0.80); precision 0.950 passes (Wilson lower limit 0.934), and C0R0 as handed over has P 0.967 and R 0.060 (not selected, no bar).
The dev result (R 0.407) was pessimistic: the test gold calls more of the ledger and inventory pages `table` and the cell finds those, but 619 positives remain, 17 of 30 read are wrapped lines of rows or items that a line-window feature cannot link to a row. Phase 19 should
build a per-element reader that groups lines into rows (column or indent alignment and the row start) before it decides on list/table, and train or tune it on elements, not lines. *Bar M fails on precision* (P 0.259, interval 0.09 to 0.50; R 0.872 passes by point estimate, lower Wilson 0.79):
the margin call is the layout role `extra` plus `page_margin`, and 160 of its 234 false calls are table columns on 2.10.50, 2.10.01 and VOC. Phase 19 should first separate a second table column from a margin block (width, vertical extent, line count against the main column) and use the
text layer's role only as a candidate. Neither bar is retuned or retested.

### Step 24r: Review fixes

> 2026-11-17, Sonnet 5.5. Three small fixes from the phase review; no gold read for measurement, no bar re-run.

**What changed.** (1) `datasets/bulk_tagging.py`: `PLACEMENT`/`PLACEMENT_TYPES` added to `_NAMESPACE_TYPES`, so `type_options_for_namespace('placement')` returns `['bottom', 'interlinear', 'main', 'margin', 'top']` (was `[]`; F18-R.10). Three tests in `tests/test_datasets_bulk_tagging.py` (placement options, no placement subtypes, every non-`doctype` namespace has options). (2) `reports/data/phase-18-page-element-types/step-24r/region_tags_voc.py` writes `region_tags_voc_test.tsv` and `region_tags_voc_dev.tsv` from the 24d line tables with `stratum == "voc"` only (F18-R.7); the 24d files and script are untouched. (3) `analysis/listlike.py`: module docstring made a literal block (`follows::`, lines 9 to 39 indented); no code line changed. Changelog entry `### Step 24r` added.

**Results.** Bulk-tagging and listlike tests 35 passed; full suite 1,460 passed, 11 skipped (with the git safe.directory override). `listlike --selftest` ends `listlike selftest ok`. Sphinx `listlike.py:docstring` warnings: 0 (was 5). VOC acceptance numbers reproduce exactly: test 325 VOC lines; raw tag `list` 69 (`paragraph` 67, `catch-word` 1, `untagged` 1), `table` 154 (`paragraph` 153, `untagged` 1), `running_text` 67 (`paragraph` 64, `untagged` 3); dev 195 VOC lines, `list` 51 and `running_text` 90, all `paragraph`.

**Error analysis.** Read both VOC tables in full. On VOC test, the provider tags the 69 `list` lines and 154 `table` lines almost only `paragraph` (67 and 153), so the provider tag carries no list/table signal there; the layer call is `generic:running_text` for them (67 and 153). Margin: provider `marginalia` occurs on 5 VOC test lines (4 gold `heading`, 1 gold `reference`) and the layer calls all five `margin`; gold margin placement is 4 lines, of which 3 are `unclear` and 1 is not, and one `main` line is called `margin`. With one countable margin line outside `unclear`, the `marginalia` tag cannot be judged on this sample. On VOC dev, `marginalia` covers 23 lines (11 gold `heading`, 12 `note`) and 23 gold margin lines are called `margin`, with 2 gold `margin` called `main`. Conclusion: the pooled tables had hidden that VOC raw tags are `paragraph` for list and table lines; the original pooled counts are superseded for VOC by these files.

**Findings.** No new findings.

## Review

Reviewed 2026-11-17 by Opus 5.5 ([REVIEW](../plan/phase-18-page-element-types/REVIEW.md)). Verdict: **accept with
follow-ups**; review fix 24r (`plan/phase-18-page-element-types/step-24r-review-fixes.md`): the `bulk_tagging`
namespace map, VOC-only region-tag tables, the `listlike` docstring in the docs.

**Re-run by the reviewer.**
- AS suite: 1,457 passed, 11 skipped (one provenance test needs git's `safe.directory` in this session's environment).
  LotA 609 passed, 1 skipped, 1 xfailed; NLP 66 passed.
- Both 24d stages, re-run into a scratch directory from the layers: every output file byte-identical (`select.json` up to
  its time stamp). Gold hashes match `SHA256SUMS`; `select.json` (2026-11-02) precedes `test_gold.done` and
  `test_done.json` (2026-11-17), whose `select_sha256` matches.
- `data/annotations/`: a fresh listing equals the before-list. `listlike` diff against shared-tools: header and appended
  constants only.

**Steps' findings.** 28 confirmed, 9 revised, 0 withdrawn. The revisions are a planning claim the builds contradicted
(F18-P.1), counts (F18-24c.2, F18-24d.10, F18-24d.12), one pooled table (F18-24d.13, F18-R.7), and qualifications of the
bar-L and per-stratum results (F18-24d.7, F18-24d.8, F18-24d.11). No verdict changes. In the 24c text, "87% of all
changes (556 of 648)" is 86%.

**Error analysis by the reviewer** (individual lines from `test_lines.tsv`, the gold and the dumps; one page image).
- *Bar L false positives, all 44 read.* The step's reading holds: all are running text and none is a gold error. The
  driver is fragment lines in the window, not only narrow columns: 30 of 44 have a short, noise or number line within
  three lines (F18-R.4).
- *Bar L false negatives, 25 fresh (seed 20261117).* 12 register cells of 2.10.50, 6 prose cells or amounts (P045,
  P046, P059), 3 numeric ledger rows of P036, 2 account lines of P001, 1 VOC ledger row, 1 wrapped list item. No gold
  error. Features computed on P036, P001 and P045 show the two structural gaps of F18-R.5. The 0.04 recall of sg_hand is
  P001 alone (F18-R.6).
- *Bar M, all 12 misses and 15 fresh false calls on 2.10.50.* The step's reading of the misses holds (P044's indented
  strip). The false calls are register cells in mid-page, so the gold is right. The bar fails without the registers too
  (F18-R.2).
- *Rulings during UA-063* (column heads to `table`; notes inside tables; UA-070 (a) item tables; UA-071 (a)
  insertions). They are definitional, logged and predate the test stage. Recall fails under every convention; precision
  passes only under the one taken (F18-R.1). Column heads are 12 lines (8 flagged). The tokens `colhead` and `cont` were
  never written, and `start` runs do not make elements on register pages (F18-R.9).
- *`prose`.* It is one line property, applied from P045 on, not two meanings. On P049–P060 it marks running text, on
  P045–P047 and P059 table cells: the same "written as sentences" judgement on lines of different types. It is absent on
  P001–P044 (F18-R.8).
- *Gold `table` on 2.10.50 behind the bar-M false calls.* The count of false calls does not depend on it, because bar M
  reads placement, which the user changed on 2 lines. Their type split does depend on it: 55 of the 160 'table' false
  calls were agent `form`. P028 keeps `form` where P029 and P030 (same register) are `table` (F18-R.3).
- *Gold quality.* The agent self-check agreed with itself 29/30, but missed the conventions behind most of the
  user's changes (F18-R.12).
- *Mode on new archives.* A handwritten AsdSAA volume is built as `print` (F18-R.11).
- *Leakage.* All checks pass (F18-R.13).

**Brief defects (24c part D, 24d test).** (1) Valid: the scripts now need "run from the AS root" or must resolve against
it (CONTEXT §5). (2) and (4) Valid in effect, but the cause is that the rulings were given in chat and not on the check
page. The four `cont` hits are the word "continues". (3) Revised: one property, partial coverage (F18-R.8). (5) and (6)
Handled correctly by the step.

**Candidates for `docs/findings.md`** (for someone reusing AS; not edited here): F18-24a.1 (the vocabulary's placement
axis), F18-P.4 (key lines on (scan_id, line_id) for AsdSAA), F18-24d.7 with F18-R.1 (`listlike` scores lines and
misses wrapped rows), F18-24d.9 with F18-R.2 (the layer's `marginalia` call is not a margin detector on tables or
registers), F18-R.11 (mode detection is SG-calibrated), F18-R.12 (agent self-checks miss convention errors).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F18-R.10, F18-R.7, REVIEW #3 | `bulk_tagging` namespace map with `placement`; VOC-only region-tag tables; `listlike` docstring as a literal block | `plan/phase-18-page-element-types/step-24r-review-fixes.md` |
| F18-24d.10, F18-R.2 | Separate second table columns and register cells from margin blocks before calling margin (bar M) | `plan/CONTEXT.md` §4 (phase-19 candidates); UA-073 |
| F18-24d.12, F18-R.5, F18-R.4 | Element-level list/table reader (row grouping by column and indent), digit-initial rows, amount + `—`/`„`, fragment lines out of the window; fresh gold | `plan/CONTEXT.md` §4; UA-073 |
| F18-R.3, F18-R.8, F18-R.9, F18-24c.3 | Annotation format v2: element ids or a continues flag, per-element "cells are prose", a form-vs-table ruling, `letterhead`/`copymark` types | `plan/CONTEXT.md` §4 |
| F18-R.11, F18-24b.2 | Mode detection outside SG (5023 4 handwritten built as `print`; 2.10.50 `default`) | `plan/CONTEXT.md` §4 |
| F18-24d.13 | The layer has no `list`/`table` type; the types stage on new archives | `plan/CONTEXT.md` §4 |

## Reproduce

From the AS root, with `.venv/bin/python` (T7_Data mounted for the SG and VOC layers):

- Tests: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_datasets_vocabulary.py tests/test_datasets_bulk_tagging.py tests/test_analysis_listlike.py tests/test_analysis_page_elements.py tests/test_text_layer_presets_archives.py`.
- Layers (24b): `reports/data/phase-18-page-element-types/step-24b/build.sh` (seven `as-build-text-layer --preset <key>_v5sg` runs through `coreshare.py`).
- Gold: `shasum -a 256 -c reports/data/phase-18-page-element-types/step-24c/gold/SHA256SUMS`.
- Measurement: `.venv/bin/python -I reports/data/phase-18-page-element-types/step-24d/eval_elements.py --selftest`;
  `--stage dev` rewrites the dev outputs; `--stage test` refuses once `test_done.json` exists (the reviewer re-ran both
  stages into a scratch directory by loading the module and setting `HERE`).
