# Phase 11 report: Text extent options and NLP ports (optional)

> **Status:** final (phase review, 2026-10-27).
> **Phase plan:** [`plan/phase-11-text-extent-and-nlp-ports/PLAN.md`](../plan/phase-11-text-extent-and-nlp-ports/PLAN.md).
> **Code state:** AS unreleased after v0.15.1; NLP read only (2026-10-27).
> **Last updated:** 2026-10-27, by the phase review (Opus).

## Summary

Phase 11 delivered the optional items of inventory step 18 for NLP, without editing NLP. `text_extent` gained a
region-type filter and continuous margin gaps (UA-014 (a)), and NLP's report/84 numbers reproduce exactly through
them (F11-18a.1). NLP's DP and TextTiling segmenters (C10) and its separator-sheet rule (C12) are in AS, with 51 new
tests and exact equality on NLP's own 22 VOC inventories (F11-18b.1, F11-18c.1). The C18 docs exist: a
leakage-and-splits page and the granularity rule stated as a hypothesis. What a reader should know:
- the segmenters return segment *start* indices, unlike the module's gap indices; converters exist (F11-18b.2);
- DP over-segments (2555: 510 starts for 128 labelled) and its weight is fragile (F11-18b.3, .4); TextTiling puts
  spurious starts at both ends of 21 of 22 inventories (F11-R.2);
- the margin gap is not monotone in END probability (F11-18a.3), and a catch-word at the foot of a page is an anti-END
  cue rather than noise to filter out (F11-R.1).
NLP switches its imports after UA-032.

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`. Planning findings use `F11-P.<n>`, and the review's own findings
use `F11-R.<n>`. IDs never change and are never reused. A finding that turns out to be wrong is marked `withdrawn`,
not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F11-18a.1 | AS `region_types` and `text_extent_table` reproduce NLP report/84 exactly on 20,912 VOC pages: extents within 1.1e-16, early_end to END all-lines n_flag 3,112 P 0.1398 R 0.3154, body 2,762 P 0.1705 R 0.3416, 8 of 8 gap bins. | result | voc, nlp, text-extent, port | confirmed | [Step 18a](#step-18a-text_extent-region-types-and-margin-gaps) |
| F11-18a.2 | The catch-word region sets the lowest line on 3,885 of the 5,221 pages (74%) whose class differs between the all-lines and body variants (most changes are late_start/full_text; page-numbers set the top on 1,267); among the 1,897 changed early_end flags it is 1,135 (60%). | data-property | voc, text-extent | revised | [Step 18a](#step-18a-text_extent-region-types-and-margin-gaps) |
| F11-18a.3 | `bot_gap` is not monotone in END probability: bins 0.15-0.25 and 0.25-0.40 have END rate 0.389 and 0.368, bin above 0.40 only 0.169 (844 pages, largest gaps are single-line page-number pages), so a threshold on the gap cannot replace a band. | method | voc, text-extent | confirmed | [Step 18a](#step-18a-text_extent-region-types-and-margin-gaps) |
| F11-18a.4 | NLP's `page_extents*.csv` keep the 1,076 pages without lines as `n_lines = 0` rows (the brief called them absent: a brief defect); AS returns `None` for such pages by design, so a consumer joining on scan name treats a missing row as empty. | data-property | voc, nlp, text-extent | revised | [Step 18a](#step-18a-text_extent-region-types-and-margin-gaps) |
| F11-18b.1 | AS `segment_dp`, `textiling_depth_scores`, `calibrate_length_penalty_weight` and `predicted_start_rate` equal NLP on its 22 VOC sequences: weight 0.0293559853524414, 0 mismatches, 41,810 / 41,810 change-point rows reproduced. | result | voc, nlp, port, segmentation | confirmed | [Step 18b](#step-18b-c10-dp-and-texttiling-segmentation) |
| F11-18b.2 | The segmenters return segment start indices while `detect_boundaries_*` return gap indices; passing one to `boundaries_to_segments` unconverted shifts every cut by one, so `starts_to_boundaries` and `boundaries_to_starts` are added. | method | segmentation, api | confirmed | [Step 18b](#step-18b-c10-dp-and-texttiling-segmentation) |
| F11-18b.3 | The pooled mean DP segment length is 3.85 at the calibrated weight but jumps from 28 to 98 pages between weights 0.29 and 0.43, and the pooled median stays 1 page below 0.29: the weight is a fragile knob. | data-property | voc, segmentation | confirmed | [Step 18b](#step-18b-c10-dp-and-texttiling-segmentation) |
| F11-18b.4 | On inventory 2555 DP predicts 510 starts for 128 labelled (median segment 1 page vs 6); of 10 sampled starts 4 are separator sheets, 3 dated entry heads, 2 empty pages and 1 a signature fragment, so these are section-level breaks, not document starts. | negative-result | voc, segmentation | confirmed | [Step 18b](#step-18b-c10-dp-and-texttiling-segmentation) |
| F11-18c.1 | AS `separator_sheets` equals NLP on all 20,912 VOC pages: 169 sheets, 0 mismatches over `detect_sheets`, `apply_rule` (12 convention x threshold runs) and next scan id. | result | voc, separator-sheets, port | confirmed | [Step 18c](#step-18c-c12-separator-sheet-rule) |
| F11-18c.2 | The VOC sheet pattern has no visible false positive in 169 hits, and misses about 5 of 174 sheet-like pages (3%) because of OCR residue after the number (judged from text only). | data-property | voc, separator-sheets | confirmed | [Step 18c](#step-18c-c12-separator-sheet-rule) |
| F11-18c.3 | `apply_rule` now raises `ValueError` on an unknown convention (NLP treated it as "next"), and ids that do not parse as scan ids get no next page (unit skipped) instead of an error or a wrong id. | behaviour-change | separator-sheets, api | confirmed | [Step 18c](#step-18c-c12-separator-sheet-rule) |
| F11-18d.1 | `docs/cross-project-review.md` L93-95 says AS makes Framing A the default and the inventory's C10 row cites the granularity rule as settled; review: the inventory got a dated *Done* row, the cross-project review stays as a dated record overridden by its own §0 decisions 9-10. | method | docs, granularity | revised | [Step 18d](#step-18d-c18-docs-leakage-and-splits-granularity-hypothesis) |
| F11-R.1 | Among the 1,897 early_end flags the body variant changes, pages whose all-lines bottom line is a catch-word are rarely document ends: END rate 0.025 of 550 dropped and 0.113 of 585 gained flags (447 of them INSIDE), against 0.188 and 0.278 for the other changes; a catch-word is an anti-END cue, not just margin noise (changed pages only, not a corpus rate). | data-property | voc, text-extent, catch-words | confirmed | [Review](#review) |
| F11-R.2 | `textiling_depth_scores` zero-pads its smoothing, so 21 of NLP's 22 VOC inventories get spurious starts at page 1 and at the last page (42 of 1,981 starts; none labelled); kept for parity with NLP, documented. | open-issue | voc, segmentation, nlp | confirmed | [Review](#review) |
| F11-R.3 | `apply_rule` iterated the sheet set, so two consecutive sheets gave a result that depended on `PYTHONHASHSEED` (also in NLP); now sorted, with a test; the 22 inventories have 0 consecutive sheets, so the AS = NLP check was unaffected. | bug-fixed | separator-sheets, nlp | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`, `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Step 18a: `text_extent` region types and margin gaps

**What changed.** `archival_structures/analysis/text_extent.py` gets two additive options (NLP report/84, UA-014):
`region_types` on `compute_page_text_extent` / `compute_corpus_text_extents` (lines kept only if
`line_region_type(line)`, the direct parent's type, is in the set; default `None` = today's behaviour), and the
continuous margin gaps through `margin_gaps` and `text_extent_table` (`text_extent`, `top_gap`, `bot_gap`; the class
keeps its own comparison). `BODY_REGION_TYPES_VOC` is a named example, not a default. One bug fix: the empty-corpus
frame now has all eight columns. 9 new tests (31 in the file). Brief:
[`step-18a`](../plan/phase-11-text-extent-and-nlp-ports/step-18a-text-extent-options.md). Script and outputs:
`reports/data/phase-11-text-extent-and-nlp-ports/step-18a/` (`recheck_report84.py`, `recheck.tsv`, `bins.tsv`,
`extents_*.tsv.gz`, `run.log`; error analysis `error_analysis.py`, `ea_*.tsv`). Run with NLP's interpreter
(pagexml-tools 0.9.0) and AS on `PYTHONPATH`, through coreshare (1 worker granted).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| Extents matched, both modes | 20,912, 0 scannames one-sided | 20,912 / 20,912, 0 / 0 |
| max abs diff of top, bot, left, right | <= 1e-12 | <= 1.11e-16 in both modes |
| `n_lines` (all) / `n_body_lines` (body) equal | 20,912 | 20,912 / 20,912 |
| all-lines class equals `page_table.text_extent` | all pages with lines | 19,836 / 19,836 |
| early_end to END, all lines | n_flag 3,112, P 0.1397814910025707, R 0.3154459753444525 | identical (1e-12) |
| early_end to END, body | n_flag 2,762, P 0.17052860246198406, R 0.34155184916606235 | identical (1e-12) |
| 8 gap bins, `n` and `END_rate` | equal to `supp_end_rate_by_bot_bin.csv` | 8 / 8 (e.g. (0.15, 0.25] n 208, 0.389) |
| New tests, full suite | green | 31 passed in the file; 1300 passed, 11 skipped |

All numbers reproduce exactly; nothing needed adjusting. Brief defect: NLP's `page_extents*.csv` contain the 1,076
pages without lines (`n_lines == 0`, NaN margins); the brief said they were absent. The script emits `n_lines = 0` rows
for them so that the 20,912 comparison holds (AS returns `None` for such pages, unchanged).

**Error analysis.**

- *What was read:* all 5,221 pages whose class differs between the all-lines and body variants (19,519 pages have body
  lines), to count which region type sets `bot` (the lowest line); 10 of them in a seeded sample (`random_state=18`,
  5 per direction, `ea_samples.tsv`); the 10 largest `bot_gap` among END and among non-END pages (`ea_gap_pages.tsv`).
- *Categories (type of the lowest line on class-changing pages):*

  | Region type setting `bot` | Pages | Example (verbatim) |
  |---|---|---|
  | catch-word | 3,885 (74%) | "Maar", "kreeg", "Zang" |
  | untyped `text_region` | 635 | "'tschip", "Maas" |
  | paragraph | 604 | "meerder afval aan des vijants zijde. —" |
  | signature-mark | 65 | |
  | marginalia, header, page-number | 22, 7, 3 | |

  Of the 10 sampled changes, the all-lines `bot` was set by a catch-word in 4 (one-word foot lines "Maar", "kreeg",
  "Zang", "e"), by an untyped `text_region` fragment in 3 ("'tschip", "Maas", "eede"; the last on an END page), and by
  a `paragraph` line in 3. Those 3 early_end to full_text changes did not come from the page's own bottom line: the
  page-number at the top ("159", "267", "305") and the shift of the inventory's stable margins (re-estimated over body
  lines only) moved the class. So the class can change through the stable-margin estimate as well as through the
  page's own margin. The top margin is set by `header` (2,075), `page-number` (1,267),
  `paragraph` (1,005) and `text_region` (858) lines.
  The class change by direction: 597 early_end to full_text (7.5% END), 624 full_text to early_end (15.4% END).
- *Largest `bot_gap`:* the 10 largest among END pages (0.70-0.79) are short letters or closing pages of 1-9 lines
  ending in a place-and-date line ("Jembij Int E' Compthoir Adij 14:en Januarij 1668"); the 10 largest among non-END
  pages (0.91-0.95) are all single-line pages holding only a number ("188", "55"), typed `page-number` or untyped.
  A threshold on `bot_gap` catches both short closing pages and number-only pages; it does not separate them, and
  bins above 0.40 hold 844 pages with an END rate of only 0.169, below the 0.15-0.40 bins (0.389, 0.368).
- *Conclusion:* the catch-word at the foot of a page sets the bottom margin on most pages whose class changes, flipping
  pages in both directions (review: "largely a catch-word effect" overstated it; 550 of the 1,039 dropped flags with body
  lines have a catch-word bottom, and 1,904 of 3,112 flags survive, F11-18a.2 revised); restricting to body types raises P from 0.140 to 0.171 and R
  from 0.315 to 0.342 while flagging 350 fewer pages. The gap is not monotone in END probability, so a consumer
  should use it as a feature with a band, not a threshold.

**Findings:** F11-18a.1, F11-18a.2, F11-18a.3, F11-18a.4.

**Open issues.** none

### Step 18b: C10, DP and TextTiling segmentation

**What changed.** `segment_dp`, `calibrate_length_penalty_weight`, `textiling_depth_scores`, `predicted_start_rate`,
`MAX_SEGMENT_LENGTH` and the private `_geometric_length_penalty` are ported verbatim from NLP
`unsupervised_segmentation.py` into `archival_structures/stream_analysis/overview/subsequence_detection.py`
(new section at the end of the module). Two new converters, `starts_to_boundaries` and `boundaries_to_starts`, map
between the segmenters' start indices and the gap indices of `detect_boundaries_*`. All seven public names are lazily
exported from `archival_structures.stream_analysis`. Only type hints changed (`List` to `list`). Brief:
[`step-18b`](../plan/phase-11-text-extent-and-nlp-ports/step-18b-unsupervised-segmentation.md). Script and outputs:
`reports/data/phase-11-text-extent-and-nlp-ports/step-18b/` (`compare_nlp.py`, `compare.tsv`, `run.log`,
`weight_sensitivity.tsv`, `segment_lengths.tsv`, `dp_starts_2555.tsv`).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| Calibrated weight (AS and NLP) | 0.0293559853524414 | 0.0293559853524414 both |
| `segment_dp` start lists, 22 inventories x weights {calibrated, 1.0} | 0 mismatches | 0 of 44 |
| `textiling_depth_scores` depth arrays and starts, window 3 | 0 mismatches | 0 of 22 |
| `unsupervised_changepoints.csv` `is_change_point` | 41,810 / 41,810 | 41,810 / 41,810 (0 mismatches) |
| `predicted_start_rate` | equal | 0 of 22 differ |
| New tests | pass | 23 passed (`tests/test_subsequence_detection_unsupervised.py`) |
| Full AS suite | green | 1300 passed, 11 skipped (baseline 1241 passed; the rest is the other steps' tests) |
| Existing functions in `subsequence_detection.py` | unchanged | diff is additions only (docstring paragraph, `from math import log`, new section) |

**Error analysis.** The port is verbatim, so this reads the method (labels are read for description only).

- *What was read:* DP and TextTiling segment-length quantiles against labelled starts for the largest (2448, 2,636
  pages), smallest (8023, 155 pages) and 2555 (1,090 pages) inventories; 10 DP starts of 2555 (seeded
  `RandomState(18)`) with their page text (`dp_starts_2555.tsv`); mean segment length at the 25 weight candidates.
- *Segment lengths (pages), median / mean:*

  | Inventory | DP (starts) | TextTiling (starts) | Labelled (starts) |
  |---|---|---|---|
  | 2448 | 2 / 4.9 (533) | 6 / 11.8 (223) | 4 / 17.0 (155) |
  | 8023 | 1 / 3.9 (40) | 2.5 / 9.7 (16) | 23 / 22.1 (7) |
  | 2555 | 1 / 2.1 (510) | 4 / 6.5 (168) | 6 / 8.5 (128) |

  DP over-segments strongly (2555: 510 predicted starts against 128 labelled; median segment of one page), most in the
  inventories with long documents. TextTiling is closer in start count but still above the labels.
- *Weight sensitivity (pooled, all 22 inventories):* mean segment length is 3.24 at weight 0.02, 3.85 at the
  calibrated 0.0294 (nearest candidate to the target 4.0; the next candidate gives 4.21), 13.2 at 0.20, 28.3 at 0.29,
  97.7 at 0.43 and about 180 from 0.77 on (the 200 cap). The pooled median is 1 page up to weight 0.29. The mean
  therefore rises smoothly up to about 0.25 and then collapses into near-document-less segmentations within one
  factor of two.
- *10 DP starts in 2555 (a reading, not a metric):*

  | Category | Count | Example (verbatim) |
  |---|---|---|
  | Separator sheet, labelled as a start | 3 | "No: 28", "No. 40.", "No: 19." |
  | Separator sheet, not labelled | 1 | "No. 24." |
  | Head of a dated entry (plausibly a new item, labelled inside) | 3 | "Maandag Den 26:' November 1742. Present Den Edelen heere President..." |
  | Page without text | 2 | `nan` |
  | Marginal/signature fragment inside a document | 1 | "den ae 172 vSlingelandt" |

  The method finds the visual break at separator sheets (they differ from their neighbours in text and embedding) and
  at dated entry heads, but it also cuts at empty pages and signature pages, which is where most of the surplus
  starts come from. With a median segment of one page the starts are not document starts in the labelled sense.
- *Conclusion:* the segmenters detect similarity breaks, not document starts. They are section-level tools, which
  agrees with NLP's finding that both score below the majority baseline. The port is exact, so the behaviour is
  NLP's, including the quirks: `np.convolve(mode="same")` zero-pads, so constant non-zero input still yields starts
  at the edges (`[0, 1, 10]` for ten 0.5 sims); a window-3 TextTiling valley spans three gaps and yields three starts
  (`[0, 10, 11, 12]` for one dip).

**Findings:** F11-18b.1 to F11-18b.4.

**Open issues.** The `return [0] if n_pages == 1 else []` in `segment_dp` has an unreachable `[]` branch, and
`textiling_depth_scores` on empty input emits a numpy `RuntimeWarning`; both kept for the NLP shim (decision P2).

### Step 18c: C12, separator-sheet rule

**What changed.** New `archival_structures/analysis/separator_sheets.py` (C12, ported from NLP
`src/startpage/separator_sheets.py`): `is_separator_sheet`, `detect_sheets`, `next_scan_id`, `apply_rule`, with the VOC
regex and the 12-character cap as named defaults (`VOC_SHEET_PATTERN`, `VOC_MAX_SHEET_CHARS`), column names and the
next-page function as parameters. The next page id comes from `ScanId` (C13). Brief:
[`step-18c-separator-sheets.md`](../plan/phase-11-text-extent-and-nlp-ports/step-18c-separator-sheets.md). NLP is not edited.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| `is_separator_sheet` AS = NLP on 20,912 page texts | 0 mismatches | 0 |
| `detect_sheets` sets equal | 169 sheets, 0 diff | 169, 0 diff |
| `apply_rule` AS = NLP (none/sheet/next/dict x thresholds 0.3/0.5/0.7, shuffled rows, random probs) | 0 mismatches | 0 in all 12 runs |
| `next_scan_id` = NLP's next id on all page ids | 0 mismatches | 0 / 20,912 |
| New tests | pass | 27 passed (`tests/test_analysis_separator_sheets.py`) |

Details: [`compare.tsv`](data/phase-11-text-extent-and-nlp-ports/step-18c/compare.tsv),
[`sheets.tsv`](data/phase-11-text-extent-and-nlp-ports/step-18c/sheets.tsv). Hits per inventory were not re-counted here
beyond the total of 169.

**Error analysis.**

- *What was read:* all 169 detected texts (`sheets.tsv`, grouped by shape with digits masked), and all 19 near misses
  (page text of at most 20 characters containing `No`/`Nr`/`N°`/`Nº` followed by a digit, not detected), from
  `page_text.csv.gz` of the 22 VOC 1.04.02 inventories. Page images were not consulted, so labels below are judged from the
  text alone. This is a reading of the VOC pattern on these 22 inventories, not a corpus rate.
- *Categories (detected, 169):*

  | Category | Count | Example |
  |---|---|---|
  | `No`/`N:o` + number, clear sheet number | 169 | `No: 7.`, `No. 63`, `N:o: 51.` |
  | number with a letter suffix | 0 | none found |
  | other (page number, folio) | 0 apparent | the only odd shapes: `No\n9.` (line break), `N=o 6.` (OCR), 3-digit `No. 172`, `No. 492` |

- *Categories (near misses, 19):*

  | Category | Count | Example |
  |---|---|---|
  | likely missed sheet (sheet number plus a stray OCR character or digit) | 5 | `No: 2\n\nƒ`, `No. 2\n„`, `No: 45½`, `No. 5\n5`, `No\n-\n33` |
  | ambiguous | 1 | `No 2 88` (1274) |
  | running header + number, probably a written page | 4 | `Patria\nNo. 14`, `No. 7\nPatria` (all 1274) |
  | correctly rejected (several numbers or text) | 9 | `No. 70\nNo. 72\nNo 72`, `No. 65\nin\nCarel`, `No1\n14\n4` |

- *Conclusion:* the pattern is precise here (no false positive seen in 169) and misses about 5 of 174 sheet-like pages
  (about 3%), all through OCR residue after the number. The cap of 12 characters and the end anchor are what reject them;
  loosening either would also admit the 9 correctly rejected cases, so no change is proposed. Whether the missed pages are
  really sheets needs the images. The 4 `Patria` pages in 1274 show that a header next to a number must stay excluded.
  Dominant shapes: `No: 99.` (32), `No. 99` (31), `No: 99` (26), 2555 has 140 of the 169.

**Findings:** F11-18c.1, F11-18c.2, F11-18c.3.

**Open issues.** none

### Step 18d: C18 docs, leakage and splits, granularity hypothesis

**What changed.** Docs only, no code, tests or changelog. New page `docs/challenges/leakage_and_splits.md` (NLP's four
leakage rules generalised, a fifth line on post hoc thresholds from step 18a, the split protocol with AS function names,
a pointer on granularity). `docs/challenges/index.md` gets the catalog row (*Methodology*) and the toctree entry.
`docs/challenges/document_segmentation.md` gets `## The granularity hypothesis` (status: hypothesis, user decision
2026-10-13) and the heading `### Framing A is the default assumption elsewhere in this doc` is now `### Framing A's
vocabulary is used for convenience below`, stating that Framing B is the default (decision 10). `docs/stream_analysis.md`
gets bullets for `segment_dp`, `calibrate_length_penalty_weight`, `textiling_depth_scores`, `predicted_start_rate`,
`starts_to_boundaries`, `boundaries_to_starts`, the start-index convention and the volume-section caveat. Brief:
[`step-18d`](../plan/phase-11-text-extent-and-nlp-ports/step-18d-c18-docs.md).

**Results.**

| Criterion | Measured |
|---|---|
| New page with four sections, index row and toctree entry | yes (`grep -n leakage_and_splits docs/challenges/index.md`: L24, L38) |
| Hypothesis section states hypothesis status; Framing A wording agrees with decision 10 | yes |
| Every AS name imports | one `.venv/bin/python -c "from ... import ..."` over all 28 names (splits, aggregate, baselines, segmentation.default_k, the eight subsequence_detection names, text_extent) exited 0 |
| `make reports` | clean, no warnings |

**Error analysis.** Not metrics. I read the three pages once and grepped `docs/` for "granularity rule" and "Framing A".
Other mentions: `page_order_problems.md` and `simulation_harness.md` use Framing A as one of two named framings, which
is consistent with Framing B being the default. `docs/cross-project-review.md` L93-95 says AS's doc "makes Framing A the
default"; that described the pre-fix state and is now outdated (F11-18d.1). `docs/shared_library/upstream_inventory.md`
L333 treats the granularity rule as settled ("consistent with the granularity rule"), which sits oddly beside the
hypothesis status; not mine to edit.

**Open issues.** F11-18d.1.

## Review

Code review: [`REVIEW.md`](../plan/phase-11-text-extent-and-nlp-ports/REVIEW.md) (verdict: accept with follow-ups).

**Re-run.** Every acceptance number was re-run by the reviewer: 18a's `recheck_report84.py` from scratch on the 22 zips
gives a `recheck.tsv` and `bins.tsv` identical to the step's; 18b's and 18c's compare scripts give 0 mismatches,
weight 0.0293559853524414, 41,810/41,810 rows and 169 sheets. AS suite 1301 passed, 11 skipped; NLP 66 passed, no NLP
file changed.

**Step findings.** All confirmed except: F11-18a.2 revised (the denominator mixes all class changes; the step's
conclusion "largely a catch-word effect" overstated it); F11-18a.4 revised (a brief defect, not an AS issue);
F11-18d.1 revised (the inventory got a dated note; the cross-project review stays a dated record).

**Reviewer's error analysis.**

- *Catch-words and END (F11-R.1).* The early_end flags that the body variant changes, split by whether a catch-word
  was the page's lowest line (from `class_*.tsv.gz`, `ea_changed_types.tsv`, labels from NLP `page_table.csv`):

  | Flag change | Catch-word bottom | Pages | END rate |
  |---|---|---|---|
  | dropped (all-lines only) | yes | 550 | 0.025 |
  | dropped | no | 489 | 0.188 |
  | gained (body only) | yes | 585 | 0.113 |
  | gained | no | 273 | 0.278 |

  169 more dropped flags are pages with no body line. Of the 585 gained catch-word pages, 447 are `INSIDE`. A
  catch-word promises the next page's first word, so the text continues: its presence is evidence *against* an END,
  and filtering it out of the margin turns continuing pages into early_end. For NLP: a catch-word flag is a better
  feature than a body-only margin. This reads the changed pages only, not a corpus rate.
- *TextTiling edges (F11-R.2).* On all 22 sequences, TextTiling puts a start at page 1 in 21 inventories and at the
  last page in 21 (42 of 1,981 starts); none of these positions is a labelled start. The cause is the zero padding of
  `np.convolve(mode="same")`. The port keeps it (P2); `docs/stream_analysis.md` says so.
- *Separator sheets (F11-R.3).* No detected sheet is followed by another (0 of 169), which is why the set-order
  dependence did not show in the AS = NLP check. Fixed with `sorted(sheets)` and a test.
- *Docs.* The hypothesis section's "mostly separator sheets and dated entry heads" generalised a sample of 10; it now
  gives the counts. The leakage page linked outside the Sphinx tree; now a plain path.

**Candidates for `docs/findings.md`** (not edited; the user decides): F11-R.1 (catch-word as an anti-END cue),
F11-18a.3 (the margin gap needs a band, not a threshold), F11-18b.2/F11-R.2 (segmenter conventions and edge starts),
F11-18c.2 (the VOC sheet pattern: no false positive in 169, about 3% missed through OCR residue).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| UA-032 | NLP installs AS editable, then a small step turns NLP `unsupervised_segmentation.py` (the five ported names) and `separator_sheets.py` into re-exports, and its `early_end_extract_body.py` can use `region_types` | `docs/USER_ACTIONS.md` UA-032; `plan/CONTEXT.md` §4 |
| F11-R.1 | NLP: try a catch-word flag (presence of a `catch-word` region) as an END feature instead of the body-only margin | `docs/from-nlp-start-page-detection.md` §1 AS status; `plan/CONTEXT.md` §4 |
| F11-R.2 | Edge padding for TextTiling (a behaviour change), only together with the NLP shim and an NLP re-run of its floors | `plan/CONTEXT.md` §4 |
| F11-18c.2 | Whether the 5 near-miss pages are sheets needs the images | `plan/CONTEXT.md` §4 (low priority) |
| F08-R.2, F08-17b.2 | Left out of phase 11 (decision P8) | `plan/CONTEXT.md` §4 |
| UA-033 | `make docs` broken (no Sphinx in `.venv`) | `docs/USER_ACTIONS.md` UA-033 |

## Reproduce

Run in the NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` with NLP's interpreter and
the AS working tree on the path (`AS=/Users/marijnkoolen/Code/Huygens/archival-structures`,
`D=$AS/reports/data/phase-11-text-extent-and-nlp-ports`), writing to a new directory `OUT`:

```
PYTHONNOUSERSITE=1 PYTHONPATH=$AS venv/bin/python $D/step-18a/recheck_report84.py --nlp-root . --out-dir OUT --workers 1   # about 8 min
PYTHONNOUSERSITE=1 PYTHONPATH=$AS venv/bin/python $D/step-18b/compare_nlp.py --out-dir OUT   # about 1 min
PYTHONNOUSERSITE=1 PYTHONPATH=$AS venv/bin/python $D/step-18c/compare_nlp.py --out-dir OUT
```

The 18a error-analysis tables: `$D/step-18a/error_analysis.py`. Tests, in the AS root:
`.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_text_extent.py
tests/test_subsequence_detection_unsupervised.py tests/test_analysis_separator_sheets.py`.
