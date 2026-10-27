# From the sibling `nlp-start-page-detection` (NLP)

*NLP writes its messages for AS to `nlp-start-page-detection/docs/to-archival-structures/`. This file
records each one, the verdict and what AS does with it, so AS sessions don't need to read NLP's
reports. `NLP/` = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/`.*

## 1. `early_end` tested against NLP's END labels (2026-10-21)

Source: `NLP/docs/to-archival-structures/2026-10-21-early-end-verdict.md` (NLP Step 6.4,
`report/84-early-end-vs-end-labels.md`; Step 6.3, `report/83-generale-missiven-recall.md` §9).
Answers AS `docs/cross-project-review.md` §7.2 and the `early_end` claim in `docs/findings.md`.

**Verdict: `early_end` pages are enriched for document ends, but they are not boundary
markers.** Measured on 22 VOC inventories (NL-HaNA 1.04.02, 20,905 scans with START/END labels),
with AS's `text_extent` at its defaults (per-inventory 10th-percentile stable margins, tolerance
0.05):

| Measure | Value |
|---|---|
| Pages flagged `early_end` | 14.9% |
| Precision for END (base rate 6.6%) | 14.0%; lift 2.1, bootstrap CI over inventories [1.6, 2.7]; enriched in 19 of 22 inventories |
| Recall of END pages | 31.5% (macro 30%); F1 0.19; worst inventory 0 |
| Closing-formula regex, for comparison | recall 71%, F1 0.33 |
| `late_start` as a START marker | lift 0.97 (chance) |
| Generale Missiven, 21 other inventories (recall only) | 4 of 28 last scans (14.3%) against 8.9% of interior scans; lift 1.6, overlapping CIs. `short` covers 10 of 28 last scans against 5.6% of interior ones |

Why it fails:
- **Granularity.** About half of VOC starts are mid-page. `early_end` is rarer on `START_mid`
  pages (5.9%) than on ordinary pages (13.4%), because the next document fills the page.
- **No white space.** Of 36 `full_text` END pages read, 17 have a closing block that reaches the
  foot of a full page, and 7 are tables.
- **Threshold artefacts.** 29 of 36 false positives read are full pages within 0.034 of the
  threshold. Many inventories have a stable margin of only 0.04–0.05.
- **Non-body lines.** Untyped edge fragments and catch-words pin the bottom margin on 10 of 36
  END pages that look early on the image.
- **The one real signal is rare.** A large bottom gap (0.15–0.40 of the page above the stable
  margin) is a genuine END signal (about 55% precision), but it covers only about 13% of END
  pages.

NLP's recommendation: not a stand-alone or primary boundary feature for VOC letter bundles. At
most a weak auxiliary signal, computed over body-text regions, with a margin-gap score in place
of the binary class.

**Requests** (NLP user decision UA-010 (b), NLP's numbering), both on
`archival_structures.analysis.text_extent`:
1. **Body-text extents:** an option to compute `top`/`bot` over body-text regions only. In NLP's
   test those were the PageXML region types `paragraph`, `signature-mark` and `header`. END
   precision went from 0.140 to 0.171 and recall from 0.315 to 0.342 (lift 2.6). NLP notes the
   gain is modest and the variant was defined post hoc, after reading the errors.
2. **A margin-gap score:** expose `bot - stable_bot` (and the top equivalent) as a continuous
   column next to the class, so consumers set their own threshold.

NLP imports `compute_page_text_extent`, `estimate_stable_margins` and `classify_text_extent`
read-only, in `NLP/scripts/early_end_*.py`. Supporting data is in NLP `output/early_end/` and the
dossier `report/error_analysis/6.4-early-end/index.html`.

**AS status (2026-10-21):**
- the claim in `docs/findings.md` is revised;
- whether and where to make the two changes was **UA-014**, answered (a) on 2026-10-21: both
  options, additive (region types as a parameter, default all regions), as one step in phase 10
  with an NLP re-check of report/84;
- nothing in `text_extent` has changed yet.

**AS status (2026-10-27, phase 11, step 18a; reviewed):** both requests are in
`archival_structures.analysis.text_extent` (unreleased after v0.15.1; v0.16.0 recommended):
`compute_page_text_extent(page, equal_tol=0.02, region_types=None)` and
`compute_corpus_text_extents(pages, region_types=None, equal_tol=0.02)` filter lines by the parent region's type
(`line_region_type`, NLP's `_rtype` rule; `BODY_REGION_TYPES_VOC` = NLP's three types, a named example);
`margin_gaps(row, stable)` and `text_extent_table(df, stable=None, top_tol, bot_tol)` give `top_gap`, `bot_gap` next
to the class. Through them, report/84 reproduces exactly (3,112 / 0.1398 / 0.3154 and 2,762 / 0.1705 / 0.3415; 8 of 8
gap bins). Two notes for NLP from the AS review: the gap is not monotone in END probability (above 0.40 the END rate
falls to 0.169), so use a band (F11-18a.3); and a page whose lowest line is a catch-word is rarely an END (END rate
0.025–0.113 among the flags the body variant changes), so a catch-word flag may be the better feature (F11-R.1).
Phase 11 also ported `segment_dp`/`textiling_depth_scores` (start-index convention kept; TextTiling's edge starts
documented, F11-R.2) and `separator_sheets`. NLP's import switch waits for UA-032 (NLP's venv has a non-editable AS
0.13.0).
