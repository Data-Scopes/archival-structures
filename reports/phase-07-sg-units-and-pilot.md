# Phase 07 report: SG units, types and the pilot layer

> **Status:** draft (step sessions add sections).
> **Phase plan:** [`plan/phase-07-sg-units-and-pilot/PLAN.md`](../plan/phase-07-sg-units-and-pilot/PLAN.md).
> **Code state:** AS unreleased after v0.9.0 (phase 06 intended for v0.10.0); siblings untouched.
> **Last updated:** 2026-10-19, by step 15fp (Opus).

## Summary

<Written by the phase review.>

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F07-P.1 | Hand 1.01.02 (3351, 3354, 3771, 11182 checked) and print 456 have only `structure {type:Text}` on every TextRegion, so there are no layout labels; print 455 has Laypa types | data-property | sg, layout, region-types | draft | [Planning](#planning) |
| F07-P.2 | In 455, most typed regions are empty: only 6 of 204 `date`, 111 of 1,149 `header` and 122 of 934 `page-number` regions carry a TextLine | data-property | sg, print, region-types | draft | [Planning](#planning) |
| F07-P.3 | Sibling regions with lines overlap (bbox ≥ 0.5 of the smaller) on 78/219 scans of 3351, 124/579 of 3771, 320/730 of 456 and 704/721 of 455, where `index`×`resolution` pairs dominate (4,350) | data-property | sg, layout, overlap | draft | [Planning](#planning) |
| F07-P.4 | republic's `merge_overlapping_text_regions` runs unchanged under pagexml-tools 0.9.0 and keeps every line; regions go 4,146 → 4,017 (3351, 199 scans) and 1,604 → 645 (455, 120 scans) | result | sg, layout, republic-port | draft | [Planning](#planning) |
| F07-P.5 | AS's `classify_inventory_structure` labels every SG scan `odd_shaped` (inventory `mixed`), because 2 covers in 219 scans fall below `min_share=0.15` | open-issue | sg, opening-detection | draft | [Planning](#planning) |
| F07-P.6 | On landscape hand scans, AS's `get_separation_point` and the widest central zero-coverage gap differ by > 50 px on 127/199 (3351) and 243/298 (11182) scans, against 25/297 in print 456, so the `page_w/2` page side is suspect in hand | open-issue | sg, page-split | draft | [Planning](#planning) |
| F07-P.7 | pagexml-tools 0.9.0: `PageXMLPage` ignores its own `text_regions` (and `.stats` raises KeyError on them), `set_parentage` skips `extra`, `merge_columns` sets `doc_id` not `id`, `add_to_pagexml` writes no columns, `copy_column` drops tables | open-issue | pagexml-tools | draft | [Planning](#planning) |
| F07-P.8 | republic's date-header detection is not portable to SG: it needs a neural `line_class`, `date`/`attendance` region types and REPUBLIC inventory metadata, none of which SG has | method | sg, date-headers, republic-port | draft | [Planning](#planning) |
| F07-15fp.1 | The layout stage keeps every input line exactly once, gives unique and (for unchanged regions) byte-identical region ids, and leaves 0 overlapping pairs on all 16 inventories; the planning overlap counts reproduce exactly (78, 124, 320, 704 scans) | result | sg, layout | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.2 | Hand-read merge precision is below the 0.90 bar: hand 23/28 = 0.82 when page furniture absorbed at a column edge counts as right (9/28 when not), print 4/30 = 0.13 | negative-result | sg, layout, merge | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.3 | Laypa's print regions are page-level blocks whose XML line order already interleaves the two text columns: 1,403 of 2,044 unchanged 456 regions span both columns, and only 30 of 2,169 print merges joined two columns themselves, so print units must split on the line `column` field | data-property | sg, print, layout, units | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.4 | In hand 3349–3353 the recto's margin column abuts the verso text, so the gutter often has no gap (823/887 openings of 3350 use `min_coverage`) and the split cuts the recto's margin notes off to the verso in 11 of 30 scans where `page` ≠ `page_side` | negative-result | sg, hand, page-split | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.5 | `min_coverage` can miss the gutter by a whole page (sep ≈ 0.34·`page_w` in 3350_0851/0856/0859/0886), which puts the verso's text on the recto; 1 of 30 sampled `min_coverage` scans, 3 of 4 checked of 19 flagged in 3350 | open-issue | sg, hand, page-split | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.6 | The fitted gutter of hand SG is off the page centre (`gutter_rel` 0.436 in 3351 to 0.557 in 11182), while 3771–3823 and print sit at 0.498–0.502 with a gap on every opening | data-property | sg, page-split | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.7 | Of 60 hand-read `extra` regions, 56 are margin notes, headings, page numbers, catch words or noise and 4 are main text (323 of 1,047 extra characters); `main` holds 94.6–99.8% of the characters per inventory | result | sg, layout, roles | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.8 | 3821–3823 are largely printed registers in narrow sub-columns (keyword, summary, date, page); the role rule puts keyword, date and page in `extra`, giving 11 extra regions per page and 163 pages with ≥ 10 in 3821 | data-property | sg, registers, roles | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.9 | 455's reduction from 9,343 to 2,953 regions comes from two overlapping Laypa label layers: 1,251 of 1,784 merges join an `index`- and a `resolution`-tagged region, and 1,058 of 1,426 pages end with one main region | data-property | sg, print, overlap | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.10 | 455's index (register) part is scans 0003–0005 only; Laypa's `index` tag sits on resolution text throughout the volume, so the `index`×`resolution` overlap (F07-P.3) is not an index-part effect | data-property | sg, print, region-types | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |
| F07-15fp.11 | pagexml-tools 0.9.0 `PageXMLPage(coords=None)` with no children raises `IndexError` in `parse_derived_coords`; the layout passes the page box | open-issue | pagexml-tools | draft | [Step 15fp](#step-15fp-page-layout-page-split-region-merge-columns-roles) |

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

## Review

<Written by the phase review.>

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|

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
