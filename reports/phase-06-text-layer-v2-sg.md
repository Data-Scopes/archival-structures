# Phase 06 report: Text layer v2 — SG line geometry and layout repair

> **Status:** final (reviewed 2026-10-18; step 15fr adds an addendum).
> **Phase plan:** [`plan/phase-06-text-layer-v2-sg/PLAN.md`](../plan/phase-06-text-layer-v2-sg/PLAN.md).
> **Code review:** [`plan/phase-06-text-layer-v2-sg/REVIEW.md`](../plan/phase-06-text-layer-v2-sg/REVIEW.md).
> **Code state:** AS unreleased after v0.9.0 (intended for v0.10.0); siblings untouched.
> **Last updated:** 2026-10-18, by the phase review (Opus).

## Summary

AS now reads the States General PageXML into a **v2 line layer**: one row per scan (scans
without text included), per region and per text line, with box, baseline, confidence and page
side. The presets are `sg_1.01.02_v2` (hand) and `sg_1.10.94_v2` (print). The layer keeps the
legacy region `doc_id`, and its region text reproduces the legacy SG paragraph TSVs exactly:
151,246 of 151,246 rows in 14 inventories (F06-15d.1). The VOC v1 layer is unchanged
(`TOTAL 157 / 157`).

For print, a verbatim port of REPUBLIC's merged-line splitter, with an AS adapter, splits
11,462 merged lines in 455 and 64,063 in 456. The port matches the original on 2,856 of 2,856
REPUBLIC pages (F06-15e.1). Hyphen splits are reliable: 30 of 30, and 12 of 12 in the review's
sample. Bigram splits are right in about 70%, and the rest put one to three boundary words in
the wrong column (F06-15e.3, F06-R.2). The user chose a lower threshold factor, 1.19 instead of
1.22 (UA-002), which step 15fr applies.

The glued-word segmenter is a **negative result**. Its precision on synthetic glued words is
0.98, but on real repairs it is 0.62 (print) and 0.16 (hand), because it breaks genuine
inflected and compound words. It ships switched off, and the user decided to leave it off
(UA-003; F06-15f.2, F06-R.5).

Data properties phase 07 needs:
- print 455's region polygons are not line unions, and 93% of its regions are empty specks
  (F06-15d.2, .3);
- single-page scans are common in some hand volumes (F06-15d.5);
- the candidate rate can't drive an `auto` splitting rule for hand volumes (F06-15e.4);
- a split half's `text_raw` is a piece of an HTR line, so mappings must go through
  `split_from` (F06-R.4).

The review re-ran every acceptance check, and all rebuilt tables are byte-identical to the
steps' outputs (F06-R.1).

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F06-P.1 | The staged SG PageXML (pagexml-tools 0.9.0, recursive regions, old `-`/`„` join) reproduces the TSV `doc_id` and text for 100% of rows in 3349, 3351, 3353 and 3771 (12,093, 4,144, 10,600 and 11,696 rows) | result | sg, text-layer, doc-id, lota | confirmed | [Planning](#planning) |
| F06-P.2 | PageXML `Word` elements equal the line text split on spaces in 100% of lines (455: 33,659 of 33,660; 3349: 14,324 of 14,324), so they cannot restore lost spaces | data-property | sg, print, text-layer | confirmed | [Planning](#planning) |
| F06-P.3 | In print 455, 3.9% of lines are merge candidates (≥1,100 px and ≥45 characters); no hand line in 3349 crosses the scan midline. *Revised by the review:* 3.9% holds for the 40 planning scans only; over the whole inventory the rate is 7.1% of all lines (F06-P.5) and 7.8% of participating lines (F06-15e.4) | data-property | sg, print, layout | revised | [Planning](#planning) |
| F06-P.4 | 1,504 of 1,603 REPUBLIC print line ids for 455 (94%) match a PageXML line box exactly, and 22 more within 5 px, so a REPUBLIC crosswalk is possible | data-property | sg, print, republic, lota | confirmed | [Planning](#planning) |
| F06-P.5 | Merged lines dominate print 456: 64,289 of 111,209 lines (58%) are merge candidates, and the median line width (1,810 px) is the merged width. 455 has 11,468 candidates (7.1%), and Laypa tags 81% of its lines `index` across scans 4–717, so a tag filter on `resolution` would miss 85% of its candidates | data-property | sg, print, layout | confirmed | [Planning](#planning) |
| F06-15d.1 | The v2 extract reproduces the legacy SG TSVs exactly: `doc_id` sets equal and `legacy_region_text` equal for 151,246 of 151,246 rows in 14 inventories (100%) | result | sg, text-layer, doc-id, lota | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15d.2 | Empty structure: 3-27 `no_regions` scans per hand inventory (3351: 18/219, 3354: 27/217); in print 455, 130,737 of 140,094 regions (93.3%, median 26 x 26 px) have no lines, in 456 none | data-property | sg, print, layout | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15d.3 | Line boxes lie within their region box (5 px) in 14 hand inventories and print 456 (region = line union for 100%), but 67.3% of print 455 lines (109,071) exceed their region polygon, mostly by < 30 px horizontally | data-property | sg, print, geometry | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15d.4 | v1 is unchanged: `TOTAL 157 / 157` on the 22 NLP inventories after the change | result | text-layer, nlp | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15d.5 | Single-page scans are common: 58 of 201 scans in 3351 and 61 of 190 in 3354 have lines on one side only, so 35/65 left/right; elsewhere 50/50 | data-property | sg, hand, layout | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15d.6 | The `skip_resource_forks` flag changes nothing today: `iter_pagexml` already drops dot-files and `__MACOSX` for every source; v2 extraction of 6 tar.gz inventories (4,243 scans) needed no extra filter | method | text-layer | confirmed | [Step 15d](#step-15d-v2-extract) |
| F06-15e.1 | The AS port of republic's splitter gives the same output as the original on 2,856 of 2,856 REPUBLIC resolution pages (455: 1,421; 456: 1,435), none raising, once the original's pagexml-tools 0.7 behaviour is restored | result | sg, print, republic, text-layer | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.2 | The v2 print layer splits 11,462 merged lines in 455 (2,398 hyphen, 9,064 bigram) and 64,063 in 456 (12,670, 51,393); 8 and 36 candidates stay whole, of which about 7 and 6 are real merged lines (R2's numbers) | result | sg, print, layout, lota | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.3 | Split precision read on 60 splits: hyphen 30 of 30 correct, bigram 22 of 30 (73%), 6 off by 1-2 boundary words, 2 unclear, 0 non-merged lines | result | sg, print, layout | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.4 | Merge-candidate rate under the derived rule: printed 1.01.02 volumes 3771-3823 0.00-0.14%, 455 7.8%, 456 66.7%, but 5 of 8 hand/letter inventories show 40-87% because the rule presumes two columns, so a rate alone cannot drive the `auto` rule | data-property | sg, print, hand, layout | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.5 | Derived parameters for 456 (924 px, 45 chars, 1127 px) are close to the notebook's (900, 45, 1100): 63,896 of 64,063 splits identical (99.74%); the 1.22 factor leaves 192 real merged lines (1100-1126 px, short right part) unsplit, which the notebook threshold splits (19 of 20 read). *Revised by the review:* the user chose factor 1.19 (UA-002; 1,100 px on 456), so the default changes in step 15fr, and the 1.22 counts in F06-15e.2 and .4 become the pre-15fr baseline | result | sg, print, layout | revised | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.6 | The ported `page_split_columns` raises `IndexError` on every page under pagexml-tools >= 0.8.0 (`get_all_text_regions` no longer looks into columns); fixed in the port. HTR text with a double space where columns meet (115 lines in 455 + 456) raised `ValueError` and aborted the bigram split of 78 page sides before the text was normalised | bug-fixed | text-layer, republic | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.7 | 26 of 38 unsplit candidates read are single-column lines with a box 20-50% wider than a column, not merged lines: the candidate rule (>= 45 characters and wide) over-flags; the real failures are indented lines, page-bottom lines, degenerate splits and one hyphen without space | negative-result | sg, print, layout | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15e.8 | Column labels are right for 20 of 20 pages read (3 and 9 lines wrong-side in the whole inventories) but page numbers and date headings in 456 (everything tagged `Text`) get an arbitrary `1`/`2`; `x` never occurs | data-property | sg, print, layout | confirmed | [Step 15e](#step-15e-merged-line-splitter) |
| F06-15f.1 | Unigram glued-word segmenter (`min_token_len` 8, `min_part_count` 20, `split_penalty` 0, chosen on dev): on synthetic glued pairs/triples (test, 5,000) exact 0.310 (print) and 0.376 (hand), boundary precision 0.984 / 0.9995, recall only 0.273 / 0.288, no split in 67% / 62%; text conservation 100% | result | sg, print, hand, text-layer | confirmed | [Step 15f](#step-15f-glued-word-repair) |
| F06-15f.2 | Real precision is far below the synthetic precision: of 50 real repairs read, 31 correct in print (0.62) and 8 correct in hand (0.16); both under the 0.90 bar fixed in advance, so `word_split` stays `off` for `sg_1.10.94_v2` and `sg_1.01.02_v2` | negative-result | sg, print, hand | confirmed | [Step 15f](#step-15f-glued-word-repair) |
| F06-15f.3 | Glued tokens remain: print still has 5,961 tokens with >= 18 letters after repair (5,793 + 495 before the repair in 455 + 456, i.e. 6,288 before); of the remaining ones 2,910 have a covering by attested parts that scores below the unsplit score `log(0.5/total)`, 607 need more than 4 parts, 980 contain a part below 20 occurrences; the unsplit score makes 3-4 part splits nearly impossible in a 1.8M-token vocabulary | negative-result | sg, print, text-layer | confirmed | [Step 15f](#step-15f-glued-word-repair) |
| F06-15f.4 | The real errors are genuine words broken at a frequent prefix or suffix: print 17 of 50 (`voorquam`, `overlast`, `inladinge`, `verlatende` -> `verlaten de`), hand 41 of 50 (`-en`, `-de`, `op`, `voor`, `aen` + stem); a training-vocabulary unigram model has no way to tell `opstellen` from `op stellen` | negative-result | sg, print, hand | confirmed | [Step 15f](#step-15f-glued-word-repair) |
| F06-R.1 | The phase reproduces byte for byte. Rebuilt into fresh directories, the hand tables (56 of 56 files) and the split print tables (12 of 12, `split_model`/`split_stats` included) equal the steps' outputs. The legacy check, the A/B (2,856 of 2,856 pages), v1 `TOTAL 157 / 157` and 15f's test evaluation reproduce exactly | result | sg, text-layer, reproduction | confirmed | [Review](#review) |
| F06-R.2 | In a fresh sample, hyphen splits are correct in 12 of 12. Bigram splits are correct in 11 of 16, and 5 put 1–3 boundary words in the wrong column (one inside a glued hyphen `en redt-rende`, one off by 3, where `daer toe af-.` went to the right column). This confirms F06-15e.3. Glued hyphens without a space are a failure that neither splitting step can fix | result | sg, print, layout | confirmed | [Review](#review) |
| F06-R.3 | Lowering the factor from 1.22 to 1.19 adds 45 candidates in 455 (thresholds 1,099/1,097 → 1,072/1,070 px). About 23 of the 25 read are merged lines with a short right part (`het welck haer Hoogh Mogende na dese Provincie te doen.`), so 1.19 adds few false candidates in 455 as well as in 456 | result | sg, print, layout | confirmed | [Review](#review) |
| F06-R.4 | A split half's `text_raw` is a piece of an HTR line, not a PageXML `TextEquiv`, and the merged original's text is not stored. `" ".join(L, R)` restores it except in 87 split lines (1 in 455, 86 in 456) whose run of spaces at the column join was collapsed to one. Any `doc_id`/offset mapping must go through `split_from` | open-issue | sg, print, text-layer, provenance | confirmed | [Review](#review) |
| F06-R.5 | Re-reading 15f's 50 hand repairs gives 6–8 correct, as logged (8). The most frequent production repairs (`vandenselven`, `vanderselver`, `deswegens`, `naerdien`, 18–19 times each) split spellings that the period writes as one word, so even "correct" overstates the method. The negative result stands | negative-result | sg, hand, text-layer | confirmed | [Review](#review) |

## Planning

Planning checks, run 2026-10-17 in the planner's scratchpad (`p06/`, scripts `c1_docids.py`,
`c2_words.py`, `c4_lineids.py`, `c5_layout.py`), read only on all inputs.

**C1, legacy reproduction.** Every TSV row was matched, on both `doc_id` and text:

| Inventory | Source | Scans | TSV rows | `doc_id` matched | Text equal |
|---|---|---|---|---|---|
| 3349 | shared-data tar.gz | 963 | 12,093 | 12,093 | 12,093 |
| 3351 | shared-data tar.gz | 219 | 4,144 | 4,144 | 4,144 |
| 3353 | shared-data tar.gz | 922 | 10,600 | 10,600 | 10,600 |
| 3771 | AS local dir | 579 | 11,696 | 11,696 | 11,696 |

The 1.01.02 tar files contain binary AppleDouble `._*.xml` members. These must be skipped.

**C2, words and glued tokens (first 150 scans).** In print (455), 1,251 tokens have ≥18 word
characters, for example `Vranckrijcktoeleggen`, `wordendoorwietedoen,` and
`HondertstenpenninghtoekomendeInge-`. In hand (3349) there are 16, a mix of glued words
(`Nassaugeimpetreerd`) and genuine long words (`onordentelijcheijt`).

**C3, layout.**
- Print scans are about 4,681 px wide. Line-left peaks sit at about 250, 1,250, 2,250 and
  3,000 px, i.e. two pages of two columns.
- Most of the 8,686 print regions in 40 scans have no lines (8,070).
- Hand scans are 5,000–6,000 px wide. Their lines never cross the midline.

**C4, REPUBLIC.** REPUBLIC paragraphs also break words across paragraph boundaries
(`… zijn gel-` | `sen en geresumeert …`, scan 455_0052).

## Steps

### Step 15d: v2 extract

**What changed.** New `text_layer/extract_v2.py` (scans, regions and lines tables with geometry,
legacy `doc_id`, `scan_hook`), `iter_pagexml` reads `.tar.gz`, `discover_inventories_v2`, two SG
presets, and a v2 branch in the build CLI (extract only). v1 code paths are untouched. Brief:
[`step-15d-extract-v2.md`](../plan/phase-06-text-layer-v2-sg/step-15d-extract-v2.md); schemas
in `docs/text_layer.md` (*v2 (SG presets)*); changelog entry under *Unreleased*.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1 Legacy regression, 14 inventories | `doc_id` set equal, text equal 100% | set equal in 14/14; text equal 151,246 / 151,246 rows (100%); no duplicate `doc_id` inside any TSV |
| 2 Uniqueness | `region_id` and `line_id` unique per inventory | 0 duplicates in all 16 inventories |
| 3 Coverage | scan rows = non-junk XML members; = `scan_stats` minus dropped | equal in 16/16; scan_stats-1.01.02 scan set identical for the 14 hand inventories (0 differences); it does not cover 1.10.94 (its 455/456 are other 1.01.02 volumes) |
| 4 v1 unchanged | `TOTAL 157 / 157` | `TOTAL 157 / 157` (22 NLP inventories, `data/text_layer_v2_dev/15d-v1check/`) |
| 5 Pilot extract, `--workers 8` | 16 inventories exit 0 | 16/16, 0 failures; wall 21.2 s (hand_tar, 6 inv) + 28.0 s (hand_local, 8) + 41.4 s (print, 2) = 90.6 s |
| 6 Tests | green | text-layer + scan-id modules 124 passed; full suite 988 passed, 0 collection errors (baseline 799 passed with 6 collection errors; the matplotlib/ipywidgets modules imported in this environment) |

Per inventory (scans equal XML members everywhere; no `partial_shot_dropped` and no
`parse_error` scan occurs in the pilot set, those two statuses are only exercised by the tests):

| Inventory | TSV rows | `doc_id` matched | Text equal | Scans (= XML members) | `no_regions` / `no_words` |
|---|---|---|---|---|---|
| 3349 | 12,093 | 12,093 | 12,093 | 963 | 6 / 0 |
| 3350 | 9,710 | 9,710 | 9,710 | 894 | 7 / 0 |
| 3351 | 4,144 | 4,144 | 4,144 | 219 | 18 / 1 |
| 3352 | 11,152 | 11,152 | 11,152 | 1,028 | 5 / 0 |
| 3353 | 10,600 | 10,600 | 10,600 | 922 | 8 / 0 |
| 3354 | 3,775 | 3,775 | 3,775 | 217 | 27 / 1 |
| 3771 | 11,696 | 11,696 | 11,696 | 579 | 6 / 1 |
| 3772 | 11,435 | 11,435 | 11,435 | 595 | 8 / 0 |
| 3773 | 11,649 | 11,649 | 11,649 | 608 | 5 / 0 |
| 3821 | 17,447 | 17,447 | 17,447 | 440 | 5 / 0 |
| 3822 | 17,383 | 17,383 | 17,383 | 463 | 3 / 0 |
| 3823 | 16,775 | 16,775 | 16,775 | 442 | 6 / 0 |
| 11182 | 5,473 | 5,473 | 5,473 | 755 | 6 / 0 |
| 11281 | 7,914 | 7,914 | 7,914 | 1,139 | 20 / 0 |
| 455 (print) | - | - | - | 721 | 0 / 6 |
| 456 (print) | - | - | - | 730 | 3 / 1 |

**Error analysis.**

- *What was read:* the full `legacy_check` output (14 inventories, all rows compared); the
  containment tables for all 16 inventories; 10 random exceeding lines and 10 random
  out-of-box baselines (seed 15, from `lines/` and `regions_meta/`); 10 random zero-line print
  regions of 455 (seed 15; 456 has none); 10 random one-sided scans (seed 15) with their first
  lines; 8 random hand regions with lines but no text (3823). Scripts: `reports/data/phase-06-text-layer-v2-sg/step-15d/{legacy_check,ea}.py`;
  raw output in `data/text_layer_v2_dev/15d-a/{legacy_check,ea}.txt` (gitignored).
- *Containment* (tolerance 5 px):
  - hand (14 inventories) and print 456: 0 lines outside their region box in 1.8 M lines; the
    region box equals the union of its line boxes (+-5 px) for 100% of regions (checked on 3349, 3771, 456),
    i.e. in these the region box is derived from the lines.
  - print 455: 109,071 of 162,011 lines (67.3%) exceed the region box, in 7,814 of 9,357 regions with lines;
    only 13.6% of 455 regions equal their line union. Classes (heuristic: baseline angle > 2 deg,
    else overhang size): small overhang <= 30 px 66,051 (60.6%), 30-100 px 23,129 (21.2%),
    rotated or sloped baseline 11,968 (11.0%), overhang > 100 px 7,923 (7.3%). Almost all overhang
    is horizontal (left/right; top/bottom 4,246 / 2,970 lines). Of the 10 read (none fell in the
    sloped-baseline or > 100 px class, which is 18% of the exceeding lines, so those are unread): all 10 are a
    5-55 px left or right overhang of a region polygon that is narrower than its text line
    (5 in the <= 30 px class, 5 in 30-100 px), with baseline angles of at most 1.9 deg. Class "region box derived from
    lines" does not apply to 455; no sample looks like a rotated scan. The sloped-baseline class
    (11,968 lines) is a heuristic only, not confirmed to be rotation.
  - Baselines outside the line box by > 5 px: hand 0; print 10 (455) and 20 (456) of 273,220 lines (0.01%). Of the 10 read:
    8 are headings or paragraph openers (`DE Resolutien ...`) whose baseline kinks by 50-70 px
    (angle -2.9 to -5.5 deg), 1 a drop-cap `C)` (-27 deg), 1 a line with a 6 px excursion at the baseline end (-0.9 deg).
    Cause: the line polygon bounding box does not cover a kinked baseline. Not rotation of the scan.
- *Empty structure:*
  - `no_regions` scans (0 regions): 3-27 per hand inventory (3351: 18 of 219, 3354: 27 of 217, others < 3%); 0 in print 455, 3 in 456.
  - `no_words`: 1 each in 3351, 3354, 3771, 456; 6 in 455 (scans 0001, 0002, 0006, 0719, 0720, 0721, with 7-41 regions and no lines: covers and blank pages).
  - Zero-line regions occur only in print 455: 130,737 of 140,094 regions (93.3%); 456 has none (3,099 regions, all with lines)
    and the hand inventories none. Tags: resolution 87,023, index 34,724, separator 4,674, catch-word 1,375, header 1,038, signature-mark 893, page-number 812, date 198.
    Median box 26 x 26 px (the page is about 4,600 px wide). The 10 read: 10 of 10 have boxes of 8-200 px
    (6 of them <= 30 px), no reading-order index, ids `region_<uuid>_<n>` and sit on scans with 122-386 regions, 112-375 of
    them empty: they are Laypa detections of specks, punctuation and ink marks, not text regions. They are
    harmless to the legacy text but would swamp any region-level statistic in 455.
  - Regions with lines but no line text: 14-90 in the early hand and 456 inventories, but 198, 135, 124, 585, 734, 850 in
    3771-3773, 3821-3823 (3823: 850 of 17,625 regions, all tag `Text`, median box 60 x 53 px, 806 with one line). They are small (45-100 px) text
    boxes on which HTR returned nothing (marks, stains).
- *Page side:* share of lines is 49.4-50.7% per side in 12 of 14 hand inventories and both print ones;
  3351 and 3354 are 35/65 (left/right) because 58 and 61 of their ~200 scans are one-sided (single pages, 29% and 32%).
  One-sided scans in other inventories: 1-8 (11182: 32, 11281: 45, of ~750 and ~1,120 scans). Of the 10 read, 8 are single
  pages with running text (e.g. 3351_0198 left, 45 lines, `326.`, `Utrecht`; 3350_0036 right, 46 lines, `Dominica den 1en. Julij 1703.`),
  1 is a page with 2 lines (`82. Grll.`, `167`), 1 a page with one line (`25.`, a page number). None looks like a cover.
  So single-page scans are normal in the 33xx inventories and in the 112xx letter volumes; a `page_side`
  column does not split such scans in two pages.
- *Conclusion:* the v2 tables are consistent with the legacy layer to the character. The print 455 region polygons are not line
  unions, so region boxes from print must not be used as bounding boxes of the lines they hold; line boxes are the
  reliable geometry. 93% of 455 regions carry no text, which any region-level count must exclude.

**Findings.** F06-15d.1 to .6 in the table at the top.

**Reproduce.** `.venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15d/legacy_check.py data/text_layer_v2_dev/15d-a`
and `... step-15d/ea.py data/text_layer_v2_dev/15d-a all`, after the three pilot builds (`PROGRESS.md`
block for the commands); v1 check as in the brief, criterion 4.

### Step 15e: merged-line splitter

**What changed.** New module `archival_structures/text_layer/line_split.py`: a verbatim port of
republic-project's `long_line_splitter.py` (commit `47dfc010`, MIT) with one crash fix, and below
`# --- AS adapter ---` the parameter derivation, the in-corpus PMI scorer, `SplitHook` (the
`scan_hook`) and `extract_inventory_v2_split`. `LayerPreset.split_region_tags`; `build.py` calls the
split extract for presets with `merged_line_split="on"`; `nltk` is a core dependency. Brief:
[`step-15e-merged-line-split.md`](../plan/phase-06-text-layer-v2-sg/step-15e-merged-line-split.md).
Runs: `data/text_layer_v2_dev/15e-a/print/` (derived parameters, the CLI), `15e-b/print/` (456, the
notebook's parameters), `15e-ab/` (A/B), `15e-rates/`, `15e-v1check/`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1 A/B vs the original | 100% identical | 455: 1,421 of 1,421 resolution pages identical; 456: 1,435 of 1,435; 0 raise on either side. The original splits 173 (455) and 530 (456) lines on these pages (138 and 376 pages changed). Unmodified under pagexml-tools 0.9.0 the original raises `IndexError` on 50 of 50 pages (see F06-15e.6), so the A/B patches `PageXMLPage.get_all_text_regions` back to its 0.7.x behaviour for both sides; the port has the same fix built in |
| 2 pilot split run (455 / 456) | counts | candidates before 11,470 / 64,096; split by hyphen 2,398 / 12,670; by bigram 9,064 / 51,393; unsplit after 8 / 36; page errors 0 / 0. Wall 4.0 min, `--workers 8`, both inventories (fit pass included) |
| 3 text conservation | 100% | 11,462 of 11,462 (455) and 64,063 of 64,063 (456); 0 exceptions (runs of spaces counted as one). 7 and 108 lines had a double space and were normalised before splitting |
| 4 geometry | halves within original box +-2 px; L.x < R.x; column in {1,2,x} | L.x < R.x for 100% (0 exceptions). Within box: 455 11,462 of 11,462; 456 64,059 of 64,063 (4 exceptions, 0.006%). Column: all 171,925 (455) and 174,219 (456) non-empty participating lines have `1` or `2` (100%, also on the sides with a split); the 117 and 1,050 participating lines without column have no text (the ported column split skips them). `x` never occurs |
| 5 parameters, 456 | table | below; derived and notebook runs split 63,896 of 64,063 originals identically |
| 6 rates for phase 07 | table | below |
| 7 v1 regression, suite | `TOTAL 157 / 157`, green | `TOTAL 157 / 157` (`15e-v1check`); 1,002 passed, 0 collection errors (988 before, +14); protected inputs unchanged (sha256 and mtime) |

*Parameters, inventory 456* (derived per page side; the notebook values are per page, left side):

| | min_merged_width | default_line_width | chars | text_left | text_right | candidates | split hyphen / bigram | unsplit after |
|---|---|---|---|---|---|---|---|---|
| derived left / right | 1127 / 1124 | 924 / 921 | 45 / 45 | 384 / 2348 | 2329 / 4364 | 64,096 | 12,670 / 51,393 | 36 |
| notebook (both sides) | 1100 | 900 | 45 | 450 | 2350 | 64,289 | 12,702 / 51,553 | 37 |

455 derived: 1099 / 1097, 901 / 899, 46 chars, x 385-2269 and 2343-4316. The scorer is fitted on the
non-candidate lines only: 947,114 tokens in 455 (9,481 docs) but 245,781 in 456 (12,056 docs),
because 58% of 456's lines are candidates. Fitting uses no labels (word counts of the inventory's
own text), so there is no label leakage; the model is fitted on the same lines it then scores
(transductive, as in the notebook).

*Candidate rates, 15d lines of the 16 inventories* (`rates.py`; participating tags = `index`,
`resolution`, `untagged`, `Text`; rate = candidates / participating lines with >= 20 characters):

| Inventory | derived width / chars / min merged (left) | candidates | rate | by tag |
|---|---|---|---|---|
| 3349 / 3350 / 3351 | 1379 / 45 / 1682; 1555 / 45 / 1897; 1099 / 46 / 1341 | 31,856 / 362 / 192 | 39.6% / 0.5% / 1.9% | all `Text` |
| 3352 / 3353 / 3354 | 1051 / 27 / 1282; 1073 / 27 / 1309; 1023 / 45 / 1248 | 54,872 / 50,658 / 155 | 64.0% / 67.4% / 1.7% | all `Text` |
| 11182 / 11281 | 1096 / 34 / 1337; 1079 / 25 / 1316 | 45,859 / 55,400 | 86.5% / 78.6% | all `Text` |
| 3771 / 3772 / 3773 | 895 / 42; 893 / 41; 895 / 41 (min 1092, 1089, 1092) | 174 / 1 / 1 | 0.14% / 0.00% / 0.00% | `Text` |
| 3821 / 3822 / 3823 | 872 / 39; 876 / 39; 873 / 39 (min 1064, 1069, 1065) | 1 / 0 / 3 | 0.00% | `Text` |
| 455 | 901 / 46 / 1099 | 11,470 | 7.83% | `index` 9,739, `resolution` 1,664, `untagged` 67; 0 in catch-word, page-number, header, signature-mark, date |
| 456 | 924 / 45 / 1127 | 64,096 | 66.71% | `Text` |

Full table (both sides, all counts): `data/text_layer_v2_dev/15e-rates/rates.tsv`.

**Error analysis.**

- *What was read, and how:* from `15e-a` (derived parameters), seed 16 (`ea.py`).
  - 15 hyphen and 15 bigram splits per inventory (60). Each was shown as original, L, R, the line above
    R in column 2 and the line below L in column 1 (the lines the splitter uses as evidence). A split is
    "correct" if L ends where the line below L continues it and R starts where the line above R
    continues. Judged by reading Dutch/French text; no labels.
  - All 8 unsplit candidates of 455, and 30 of the 36 of 456.
  - 20 pages with a split (10 per inventory), 10 to 14 lines around the first split.
  - 12 of the 167 originals on which derived and notebook run split differently, and 20 of the 192
    that only the notebook run splits.
- *Split precision by kind:*

  | Kind | Read | Correct | Off by 1-2 words | Unclear | Not a merged line |
  |---|---|---|---|---|---|
  | hyphen | 30 (15 + 15) | 30 | 0 | 0 | 0 |
  | bigram | 30 (15 + 15) | 22 | 6 | 2 | 0 |

  Off-by-k examples (L / R as split; `|` = the true boundary): `ende Pasport, om nevens sijne byhebbende | Baga.` (the line below L continues `gie`: k=1, "Baga." belongs to L);
  `de Provinciendoorgaens, immers verre de meeste, | ren van` (above R ends `furne-`: k=2);
  `Sa Majesté estant per | sencore ...` (k=1; below L starts `suadée`).
  `... vroome Christe | kleet;` (k=2; below L starts `nen`). The wrong splits give 1-2 boundary
  words to the wrong column, in the cases where a column's last word is a word-break without hyphen.
- *Unsplit candidates (38 read):*

  | Category | Count | Example |
  |---|---|---|
  | single-column line with a wide box (not merged) | 26 | `het Sauvegardereght tot wederzijdts gerustheydt,` (1,301 px, 48 chars) |
  | merged, split failed: indented line (short left part) | 3 | `Ampt Rhijnberck aen de renden Officier bevolen, ten getracteert sul.` (x 2787) |
  | merged, no usable context at the page bottom | 3 | `eersten Maert lestleden, ende op den voet van het tier uytgesondert, ...` (below: `1`) |
  | merged, degenerate split (the chosen word is the first word; line kept whole) | 3 | `te zijn, volgens haer Ho. Mog. Resolutie van den Jliteyt. dat in West-Vrieslant ...` |
  | merged, hyphen glued to the next word (`wedertekee-insereer.`, no space) | 1 | `Honden, gekomen uyt Engelandt, wedertekee-insereer.` |
  | merged, neighbouring lines are fragments | 1 | `Troupes overgenomen zijn, niet bevoeght zijn te Dat de Provincien ...` |
  | merged, reason unknown | 1 | `Is ter Vergaderinge gelesen de Requeste van do, is naer voorgaende ...` |

  So about 7 merged lines remain unsplit in 455 (7 of its 8) and about 6 in 456 (5 of 30 read;
  36 x 5/30). The other candidates are single lines with a box 20-50% wider than a column.
- *Merged lines below the threshold:* the 192 lines that the notebook's 1100 px treats as candidates
  and the derived 1127 px does not are merged lines with a short right part (19 of 20 read:
  `de te houden, tot conservatie van haer Hoogh || doen.`, `... Zee- || des Francois.`). Of the 167
  originals split by both runs but differently (all bigram), 12 read: derived better 4, notebook
  better 6, unclear 2. The 30 sampled splits of the main sample were identical in both runs.
- *Column assignment (20 pages read):* left and right sequences are correct on all 20; 3 (455) and 9
  (456) lines in the whole inventories lie wholly on the wrong side of the page-side midpoint. Page
  numbers and date headings get a column by their x position: in 456, where Laypa tags every region
  `Text`, `( 1299 )` got `2`, `( 17 )` got `1`, so `column` of such lines means nothing.
- *Conclusion:* hyphen splits are reliable (30 of 30); bigram splits are right in 73% and off by one
  or two boundary words in 20%. The splitter does what REPUBLIC's does, with all the weaknesses of its
  local evidence (the last word of a column). Its failures are rare in absolute numbers (about 13
  of 75,500 merged lines stay whole), but the 1.22 threshold factor loses 192 merged lines in
  456 (0.3% of its merged lines). That is recorded as UA-002; the default stays derived as specified.

**Findings:** F06-15e.1 to .8 in the table at the top.

**Open issues.** UA-002 (threshold factor). The 8 + 12 failure kinds above are not fixed (the brief
forbids changing the ported functions).

**Reproduce.** After `15d-a` exists: the CLI
`.venv/bin/python -m archival_structures.text_layer.build --pagexml-dir <shared-data>/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v2_dev/15e-a/print --preset sg_1.10.94_v2 --workers 8`;
`.venv/bin/python reports/data/phase-06-text-layer-v2-sg/step-15e/ab_republic.py 455|456 out.json`
(without `-I`: it puts republic-project on `sys.path`; `--limit N`, `--no-env-patch`);
`.../run_notebook_params.py data/text_layer_v2_dev/15e-b/print`;
`.venv/bin/python -I .../rates.py data/text_layer_v2_dev/15d-a OUT.tsv`;
`.venv/bin/python -I .../ea.py splits|unsplit|columns|check|compare RUN_DIR OUT [...]`.

### Step 15f: glued-word repair

**What changed.** New `text_layer/word_split.py` (`WordSplitParams`, `token_core`, `build_vocabulary`,
`segment`, `repair_line`, `apply_word_split`), a v2 stage `wordfix` in the build CLI
(`--word-vocab`, `--word-params`; `--pagexml-dir` is optional for this stage), and two preset
fields `word_split` / `word_split_params`. The stage rewrites `lines/<inv>.lines.tsv.gz` (`text`
from `text_raw`, flag `wordsplit:<n>`), and writes `word_split/{vocab,params,stats,edits}`. Both SG
presets carry the chosen parameters and stay **off** (criterion 4). Brief:
[`step-15f-glued-word-repair.md`](../plan/phase-06-text-layer-v2-sg/step-15f-glued-word-repair.md).

**Results.** Splits by `sha1(scan_id) % 10` (0 test, 1 dev, 2-9 train). Print: 276,448 train /
30,758 dev / 40,369 test lines, vocabulary 100,585 types, 1.78M tokens. Hand: 307,803 / 34,618 /
39,031 lines, 73,347 types, 1.84M tokens. 5,000 synthetic glued pairs/triples per split (seed 17).
Grid of 48 settings on dev; the best dev exact rate was 0.3224 (print) and 0.3764 (hand); among the
settings within 0.02, the most conservative was `min_token_len` 8, `min_part_count` 20, `split_penalty` 0
for both.

| Archive | Split | Exact | Boundary P | Boundary R | No split | Real tokens split |
|---|---|---|---|---|---|---|
| print | dev | 0.322 | 0.989 | 0.286 | 0.656 | 1,617 / 12,407 (13.0%) |
| print | test (once) | 0.310 | 0.984 | 0.273 | 0.666 | 2,184 / 16,900 (12.9%) |
| hand | dev | 0.375 | 0.996 | 0.289 | 0.617 | 454 / 9,842 (4.6%) |
| hand | test (once) | 0.376 | 0.9995 | 0.288 | 0.616 | 482 / 11,660 (4.1%) |

| Criterion | Measured |
|---|---|
| 1 Evaluation table | above |
| 2 Production run (CLI `--stage wordfix`, vocabulary over all scans of the archive) | print 455: 14,246 lines changed, 15,384 tokens split, tokens >= 18 letters 5,793 -> 5,476; 456: 3,088 lines, 3,118 tokens, 495 -> 485. Hand 3349: 1,037 lines, 1,045 tokens, 101 -> 101; 3350: 1,050, 1,063, 101 -> 101; 3351: 187, 188, 2 -> 2; 3352: 1,194, 1,205, 102 -> 102; 3353: 944, 952, 59 -> 59; 3354: 151, 154, 0 -> 0 |
| 3 Text conservation | 0 violations in 17,334 (print) and 4,563 (hand) changed lines; inserted spaces equal the `edits` count in all of them |
| 4 Hand-read precision (50 real repairs each, seed 17) | print 31 correct, 2 wrong boundary, 17 should not split: **0.62**. Hand 8 correct, 1 wrong boundary, 41 should not split: **0.16**. Both below 0.90, so `word_split="off"` for both presets |
| 5 v1 and suite | `TOTAL 157 / 157` (`data/text_layer_v2_dev/15f-v1check/`); suite 1010 passed, 0 collection errors (8 new tests) |

**Error analysis.**

- *What was read:* 50 real repairs per archive (seed 17 over all 18,502 print and 4,607 hand
  repairs), with the line as context; the 30 long tokens that remain in print (seed 17); the reason
  for each of the 5,961 remaining print tokens by a covering search. Output:
  `data/text_layer_v2_dev/15f-a/eval/ea_{print,hand}.txt`.
- *Categories (real repairs):*

  | Category | Print | Hand | Example |
  |---|---|---|---|
  | correct (glued, boundary right) | 31 | 8 | `deGulick;` -> `de Gulick`; `Bergenop` -> `Bergen op` |
  | wrong boundary / fragment | 2 | 1 | `gecontrade` -> `gecontra de`; `delijcke` -> `de lijcke` |
  | genuine word broken: stem + `-de`/`-en` | 6 | 20 | `vermeynende` -> `vermeynen de`; `qualiteijten,` -> `qualiteijt en` |
  | genuine word broken: prefix + stem or compound | 11 | 21 | `inladinge` -> `in ladinge`; `voorgestelt` -> `voor gestelt` |
  | abbreviation, Latin or French | 0 | 0 | `endroits` counted as a genuine word |

  Cases where the line is ambiguous (`voorgemelte`, `vijfhondert`, `swaerende`) were counted as
  genuine words, which is the conservative reading.
- *Why the synthetic numbers mislead:* a synthetic glued token has parts that exist in the corpus by
  construction, and the real candidates (OOV tokens of >= 8 letters) are mostly rare inflections and
  spelling variants whose stem and `de`/`en`/`op`/`voor` are all frequent. Boundary precision on
  synthetic tokens (0.98-1.0) is therefore not the precision of a repair. Candidates that the method
  splits are 13% (print) and 4% (hand) of all long OOV tokens.
- *Remaining long tokens (print, 30 read):* 27 are glued but missed (`waeropgedelibereert` 114x,
  `Vergaderingerapport` 96x, `HeerenGedeputeerdender-`), 3 are genuine (`Lieutenant-Collonel`,
  `noodtsaeckelijckheyt`, `Vcrijts-Vergaderinge`). Over all 5,961: covering exists but the score
  does not beat the unsplit score 2,910; a part below 20 occurrences 980; internal punctuation 830;
  attested word 634; more than 4 parts 607. With a 1.78M-token vocabulary a part has p ~ 1e-3
  (log -7), so three parts (-21) lose against `log(0.5/total)` = -15: the method can only split
  into two very frequent parts. This is the same reason recall is 0.27-0.29.
- *Conclusion:* with the brief's parameters the segmenter is useless as a default repair: it breaks
  genuine words more often than it repairs glued ones, and misses most glued ones. The text-layer
  `text` stays equal to `text_raw` for both SG presets. A usable repair would need a lexicon of
  genuine word forms (so `opstellen` is attested) and a different scoring of the unsplit token; that
  is outside this step.

**Findings:** F06-15f.1, F06-15f.2, F06-15f.3, F06-15f.4.

**Open issues.** Whether to try a lexicon-based variant (UA-003).

**Reproduce.** Copy the layers to `data/text_layer_v2_dev/15f-a/{print,hand}/<preset>/lines`;
`for a in print hand; do .venv/bin/python -I reports/data/phase-06-text-layer-v2-sg/step-15f/evaluate.py $a dev LINES EVAL; ... $a test LINES EVAL; done`;
`.venv/bin/as-build-text-layer --out-dir data/text_layer_v2_dev/15f-a/print --preset sg_1.10.94_v2 --stage wordfix`
(same for hand with `sg_1.01.02_v2`); `.venv/bin/python -I .../step-15f/ea.py LAYER_ROOT OUT.txt`.

### Addendum: step 15fr (review fixes)

**What changed.** `derive_split_params` has a keyword `merged_width_factor`, default **1.19**
(was 1.22; UA-002 b). The republic-project attribution names the copyright holders in
`line_split.py` and its test file. The unused `KEEP_TRAIL` in `word_split.py` is gone. The changelog
lists the 15e public names and the `format_version` manifest key. Docs: `docs/text_layer.md`.
Nothing else in behaviour changed.

**Re-run** (`data/text_layer_v2_dev/15fr-a/print/`, exit 0, 0 failures), against 15e-a:

| | 455 15e-a | 455 15fr-a | 456 15e-a | 456 15fr-a |
|---|---|---|---|---|
| `min_merged_width` left / right | 1099 / 1097 | 1072 / 1070 | 1127 / 1124 | 1100 / 1096 |
| candidates before | 11,470 | 11,515 (+45) | 64,096 | 64,299 (+203) |
| split by hyphen | 2,398 | 2,401 | 12,670 | 12,704 |
| split by bigram | 9,064 | 9,103 | 51,393 | 51,561 |
| unsplit after | 8 | 11 | 36 | 37 |
| page errors | 0 | 0 | 0 | 0 |

Splits rise by 42 in 455 and by 202 in 456 (expected about 45 and 190). The 456 candidate count
(64,299) sits close to the notebook-parameter run (64,289). `ea.py check` (text in
`data/text_layer_v2_dev/15fr-ea.txt`): text conservation 0 exceptions of 11,504 (455) and 64,265 (456);
halves within the original box: 0 exceptions in 455, 4 in 456 (the same count as 15e);
`L.x >= R.x` for 0 lines. Column coverage: 99.93% (455), 99.40% (456).

**Rates** (`rates.py`, 16 rows; `data/text_layer_v2_dev/15fr-rates/rates.tsv`), candidates (rate of
participating lines), 15e -> 15fr:

| group | inv | candidates | rate % |
|---|---|---|---|
| hand_tar | 3349 | 31,856 -> 37,331 | 39.64 -> 46.45 |
| hand_tar | 3350 | 362 -> 467 | 0.48 -> 0.63 |
| hand_tar | 3351 | 192 -> 205 | 1.92 -> 2.05 |
| hand_tar | 3352 | 54,872 -> 62,245 | 63.98 -> 72.57 |
| hand_tar | 3353 | 50,658 -> 55,567 | 67.42 -> 73.95 |
| hand_tar | 3354 | 155 -> 187 | 1.74 -> 2.09 |
| hand_local | 11182 | 45,859 -> 46,006 | 86.53 -> 86.81 |
| hand_local | 11281 | 55,400 -> 58,480 | 78.64 -> 83.01 |
| hand_local | 3771 | 174 -> 218 | 0.14 -> 0.18 |
| hand_local | 3772 | 1 -> 1 | 0.00 |
| hand_local | 3773 | 1 -> 1 | 0.00 |
| hand_local | 3821 | 1 -> 1 | 0.00 |
| hand_local | 3822 | 0 -> 0 | 0.00 |
| hand_local | 3823 | 3 -> 3 | 0.00 |
| print | 455 | 11,470 -> 11,515 | 7.83 -> 7.86 |
| print | 456 | 64,096 -> 64,299 | 66.71 -> 66.93 |

The hand rows only change in the rates table: the hand presets do not run the splitter.
Because the hand layers' own `derive_split_params` call picks up 1.19 too, any future hand
splitting would use it (not run here).

**Error analysis** (seed 18; `reports/data/phase-06-text-layer-v2-sg/step-15fr/ea15fr.py`, output
`data/text_layer_v2_dev/15fr-ea15fr.txt`). Judged from the text and the neighbours only, no images.

- *New splits*: 42 originals in 455, 202 in 456. I read 20 per inventory.
  - 456: 18 correct (R is a paragraph-final fragment such as `te doen.`, `werden.`, `gevallen.`,
    matching the line above R in column 2); 1 other (#26: R `Reso-riserende.` holds two fragments,
    an OCR artefact); 1 not verifiable (#33: widths 266 / 839, the original is 1,105 px wide and may be one
    wide line). Off by k words: 0.
  - 455: 15 correct (same pattern); 1 off by about 3 words (#9: R `ces griefs pent gevallen.`, where only
    `gevallen.` belongs in column 2); 1 other (#10: R `puis-lsaire`, OCR noise); 3 not verifiable (#3,
    #16, #18: balanced halves of 538/538, 852/215 and 547/518 px on narrow-column French inserts).
  - Not a merged line: 0 confirmed.
  - The 455 sample is not independent: 11 of 20 come from scan 0222, and 18 of the 42 new splits
    are in that one scan (a French insert in narrow sub-columns). There the L half is itself an
    interleaving of two sub-column lines, e.g. `revenus du Peage & au- a cette affaire, &`; the splitter
    cuts at the page column, not at the sub-column.
- *New candidates still unsplit*: 3 in 455, 1 in 456 (all of them read).
  - 455 #21 and #22 (scan 0222 and 0223) are merged lines with interleaved sub-column text; no
    hyphen or bigram signal, so they fail.
  - 455 #23 (`Potentaten (quasp in de voorschreve Graef-komen.`, w=1,082) is a merged line that failed:
    the hyphen continuation `Graef-` / `komen.` is in the same line, so no neighbour match.
  - 456 #44 (`den Heere Baron van Schmetteau, Plenipotentia-`, w=1,109) is a single wide line.

**Conclusion.** In the 40 splits read, 1.19 produced no confirmed false split: 33 are correct, 1 is off by
about 3 words, 2 are OCR-noise cases and 4 cannot be judged from text. It caught 202 more merged lines in 456
and 42 in 455, and the 4 new unsplit candidates are 3 merged lines that fail and 1 single wide line.

**Note for the user (not a finding row).** Scans 0222 and 0223 of inventory 455 hold a French insert
in narrow sub-columns. Page-column splitting does not handle them and they dominate 455's new splits. Phase 07
may want to exclude or flag these scans. Also, the factor affects the hand rates table
(3349, 3352, 3353, 11281: +4 to +9 points) if hand splitting is ever enabled.

**Checks.** Tests: `tests/test_text_layer_line_split.py` all pass; full suite 1010 passed, 0 collection errors.
Protected inputs (the two print zips): sha256 and mtime equal before and after
(`data/text_layer_v2_dev/15fr-protected-{before,after}.txt`).

## Review

Phase review (Opus, 2026-10-18). The code review is in
[`REVIEW.md`](../plan/phase-06-text-layer-v2-sg/REVIEW.md). Verdict: accept with follow-ups.
Step 15fr runs before the release.

**Acceptance, re-run by the reviewer.** One script rebuilt everything into fresh directories
(`data/text_layer_v2_dev/06R-a/`, `06R-v1check/`, `06R-ab/`, `06R-15f/`). The sha256 and mtime
of the 10 protected input files were recorded before and after; they were unchanged.
- **15d.** hand_tar 21 s and hand_local 28 s, exit 0. All 56 table and stats files are
  byte-identical to `15d-a`. `legacy_check.py` gives the same output as the step:
  `LEGACY_OK True`, 151,246 of 151,246 rows, 0 duplicate region or line ids in 16
  inventories, coverage 16 of 16. v1: `TOTAL 157 / 157`.
- **15e.** The print build with the splitter took 249 s, exit 0. All 12 files (tables,
  `split_model`, `split_stats`) are byte-identical to `15e-a`. `ea.py check`: text
  conservation 0 of 11,462 and 0 of 64,063 exceptions; 4 halves outside the box (in each, both
  halves reach 27–86 px below the original box). `ab_republic.py`: 455 1,421 of 1,421 and 456
  1,435 of 1,435 identical, 0 raising, JSON equal to the step's apart from the timing.
- **15f.** `evaluate.py {print,hand} test`, with the logged dev choice, gives dev and test
  blocks equal to `15f-a/eval/*_test.json`.
- **Suites.** AS 1010 passed, 0 collection errors. NLP `tests/` 56 passed. LotA 318 passed and 9
  failed. The 9 failures come from a LotA path-root configuration for `shared-data/`, not from
  AS (REVIEW finding 10).

**Findings.** All 23 planning and step findings were checked against the code, a re-run or the
reviewer's reading:
- 21 are confirmed;
- 2 are revised. F06-P.3's 3.9% held for the 40 planning scans only. F06-15e.5's default
  changes to factor 1.19 by the user's decision (UA-002).

The review adds F06-R.1 to F06-R.5.

**Leakage.**
- 15f splits by scan (`sha1(scan_id) % 10`). It builds the vocabulary from the train scans only.
  It chose its parameters on dev with a rule fixed in the brief, and evaluated test once. The
  re-run reproduces the test numbers.
- The real-repair precision is read on the production run, whose vocabulary includes the line
  being repaired. That can only favour the method, so the negative result is not an artefact.
- 15e fits its parameters and its PMI scorer on each inventory's own non-candidate lines.
  This is transductive, as in the REPUBLIC notebook, and uses no labels. Its checks are an
  exact A/B and read precision, so there is no split to leak into.

**The reviewer's own error analysis.** Scratchpad scripts read the existing layers with seeds
606–609.
- *Bigram splits* (8 per inventory): 11 of 16 correct. Off by one: `... ten eynde lons, |
  que sadie Altesse` and `... 's Landts | gerechtige que`. Off by two: `... uytsprake | van
  sijne Philip`. Off by three: `... sy sullen | daer toe af-. Ten sevenden`. One error sits
  inside a glued hyphen: `tegens alle recht | en redt-rende het Regiment` (`commande-` above
  R). So the errors are at boundary words, as 15e found (F06-R.2).
- *Hyphen splits* (6 per inventory): 12 of 12 correct (`aenge- | sien`, `Artisle- | rye`).
- *Unsplit candidates* (6 per inventory): 455's are merged lines that failed (glued hyphen
  `wedertekee-insereer.`, page bottoms, `Jliteyt. dat in West-Vrieslant`). 456's are single
  lines whose box is wider than a column (`sonder betalinge van 's Landts gerechtigheydt,`, x
  2,924, w 1,425). This matches 15e's categories.
- *The UA-002 margin* (455, 25 of 45 read): about 23 are merged lines with a short right part.
  Many are French resolutions on 455_0222/0223 (`le Conseiller de Hoppe à tes Puissances
  reglées a`). About 2 may be single wide lines (F06-R.3).
- *Glued words:* all 50 hand repairs re-read; the verdicts agree with the step's within ±2
  (F06-R.5).
- *Column labels:* in 456, 174,219 lines with text and 3 whitespace-only lines have a column,
  and the 1,050 without one are all empty, as 15e reports.
- *Split text* (F06-R.4): the halves' `text_raw` comes from the splitter's pieces. The original
  is recoverable only through `split_from` and the join.

**Commands** (AS root; `<S>` is the review scratchpad; the scripts are not kept):
`as-build-text-layer` as in 15d criterion 5 and 15e criterion 2, into `06R-a/{hand_tar,hand_local,print}`;
`legacy_check.py data/text_layer_v2_dev/06R-a`; `ea.py check 06R-a/print/<layer> OUT 15d-a/print/<layer>`;
`ab_republic.py 455|456 06R-ab/ab_<inv>.json`; `evaluate.py print|hand test <lines> 06R-15f`
(with `15f-a/eval/*_dev.json` copied in).

**Candidates for `docs/findings.md`** (not added; the user decides):
- F06-15d.1: the legacy SG `doc_id` and region text are reproducible from PageXML;
- F06-15d.3: in print 455, region polygons don't bound their lines, so use line boxes;
- F06-15e.6: pagexml-tools ≥ 0.8 breaks REPUBLIC's splitter, and AS's port carries the fix;
- F06-15f.2 with F06-15f.4: synthetic glued-word precision says little about real repairs in
  historical Dutch;
- F06-P.2: PageXML `Word` elements don't restore lost spaces.

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| UA-002, F06-15e.5, F06-R.3 | Factor 1.19 as the default (`merged_width_factor`); re-run print, read the new splits | step 15fr (before the release) |
| REVIEW #2, #3, #6 | MIT copyright notice in the port; changelog gaps (`SplitModel` etc., manifest `format_version`); drop `KEEP_TRAIL` | step 15fr |
| F06-R.4 | Map old → new ids and char offsets for split halves through `split_from`; decide whether the layer stores the merged original's text | phase 07 (15h) |
| F06-15e.4 | The `auto` splitting rule for printed 1.01.02 volumes (the candidate rate alone is unusable for hand) | phase 07 (open question) |
| F06-15e.7, F06-R.2 | The splitter's remaining failure kinds (indented lines, page bottoms, glued hyphens): not fixed, because the port stays verbatim | phase 07 audit; no step yet |
| UA-003, F06-15f.2–4, F06-R.5 | Glued-word repair stays off (user, 2026-10-18). Phase 07's open question on a redesign is closed | — |
| REVIEW #5 | If word repair is ever turned on: guard `wordfix` against subset runs (as F05-R.5 for `wbc`) | phase 07, only if repair returns |
| Phase-05 hand-off | The preset carries the wbc rule and anchors, a subset-run guard, a manifest history, and a `„`-robust word-break statistic (F05-R.4) | phase 07 (15h). F05-R.4 is added to its row |
| F06-15e.6 | republic-project's own splitter fails under pagexml-tools ≥ 0.8 | user (republic-project) |
| REVIEW #10 | LotA's `PathOutsideRootsError` for `shared-data/` EAD (9 tests) | LotA |

## Reproduce

- F06-P.1–4: the planner's scratchpad scripts. They are not kept in the repo; step 15d
  re-measures F06-P.1 on all 14 inventories (`reports/data/phase-06-text-layer-v2-sg/step-15d/legacy_check.py`).
