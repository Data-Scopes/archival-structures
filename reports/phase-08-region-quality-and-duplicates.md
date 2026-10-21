# Phase 08 report: Region quality and duplicates

> **Status:** final (reviewed 2026-10-21).
> **Phase plan:** [`plan/phase-08-region-quality-and-duplicates/PLAN.md`](../plan/phase-08-region-quality-and-duplicates/PLAN.md).
> **Code state:** AS unreleased after v0.12.0 (recommended: v0.13.0); LotA, 2026-10-21.
> **Last updated:** 2026-10-21, by the phase review (Opus).

## Summary

AS now owns LotA's two corpus-integrity tools, and LotA calls them:

- `analysis.region_quality`: the usable / fragment / noise rule cascade, on text with an injected
  lexicon;
- `analysis.duplicate_pages`: the MinHash/LSH re-shoot detector over `(page_id, text, position)`.

Both ports are exact. The 600 gold rows and 428,544 regions reproduce, and the full SG duplicate
run is byte-identical (48,473 rows, F08-16.1, F08-17.1, F08-R.3). The corruption harness gets a
fifth case, `page_duplication`, and a copy of a start page is now a continuation (F08-17b.1–.2).

What the error analysis says about the tools themselves:

- **Region quality** is reliable on prose. It is not reliable on 1–3-token regions. There, numbers
  drive most errors: the numeric exemption is undone by `long_nonalpha_run`, and numerals in the
  lexicon count as words (F08-16.3, F08-R.1). Gold labels in that band are themselves
  inconsistent.
- **Duplicate detection** is precise where pages are adjacent: 0.95 of the adjacent rows are
  re-shoots. Among far-apart pages only 0.13 are, because formulaic SG decisions dominate there.
  The Jaccard ranges of the two groups overlap, so adjacency, not the threshold, is what separates
  them (F08-17.3–.5, F08-R.2).

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`, e.g. `F02-05.2`. Planning findings use
`F<phase>-P.<n>` (in a `### Planning` section before the steps), and the review's own findings
use `F<phase>-R.<n>`. IDs never change and are never reused. A finding that turns out
to be wrong is marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F08-P.1 | LotA's current filter reproduces its stored gold scorings exactly. That is 300/300 rows against the `-after` files, and 300/300 against `-before` once the numeric-exemption threshold is set to `inf`. `1.01.02-gold-scored.tsv` and `-supplement.tsv` are stale copies of `-before` | data-property | region-quality, lota, sg | confirmed | [Planning](#planning) |
| F08-P.2 | The report numbers recompute from the stored scorings. Sheet 02 held-out keep P/R is 0.973/0.973 before the retune (report/05: 0.97/0.97) and 0.973/0.986 after. On the combined held-out 150, keep/drop accuracy goes from 0.807 to 0.893 and 3-way accuracy from 0.727 to 0.820 (report/09) | result | region-quality, lota | confirmed | [Planning](#planning) |
| F08-P.3 | No LotA code computes the gold metrics: they were computed ad hoc from the scored TSVs. AS step 16 adds the script | method | region-quality, lota | confirmed | [Planning](#planning) |
| F08-P.4 | The stored SG candidate table has 48,473 rows over 4,228 inventories, 14,994 of them adjacent. Current code reproduces it on 7 of 7 inventories. All 13 documented re-shoots are in it as 23 rows, all adjacent | data-property | duplicates, sg, lota | confirmed | [Planning](#planning) |
| F08-P.5 | All 23 re-shoot rows are same-side pairs (page-1/page-1 or page-2/page-2) across scans N and N+1. Adjacency by sequence order, as the inventory's C17 API proposed, would not force any of them; adjacency must be by scan number | method | duplicates, sg | confirmed | [Planning](#planning) |
| F08-P.6 | LotA's `resolve_path` refuses output dirs outside its allowed roots, the AS scratchpad included, so LotA CLI runs must write under LotA `output/` | method | lota | confirmed | [Planning](#planning) |
| F08-P.7 | LotA's quality features include four bbox values that no rule reads. Four of its 15 quality tests are LotA-bound (lexicon cache, gold sampling, gold split, `inventory_quality`), so the inventory's "port 14 of 15" is out of date | data-property | region-quality, lota | confirmed | [Planning](#planning) |
| F08-P.8 | AS has no Dutch lexicon. `resources/wordlists/wordlist-lang_nl-period_early_modern.tsv` holds 1,124 function words and numbers, so the region-quality lexicon must be injected (LotA: 233,538 fastText terms) | data-property | region-quality | confirmed | [Planning](#planning) |
| F08-16.1 | The AS port `analysis.region_quality` reproduces 600/600 gold rows (300 after-retune, 300 before with the exemption threshold at `inf`; class and rules equal, \|score diff\| <= 1e-9), the LotA wrappers reproduce 4 x 150/150, and `inventory_quality` over 200 inventories (428,544 regions) has 0 mismatches | result | region-quality, lota, sg | confirmed | [Step 16](#step-16-region-quality-filter-c16) |
| F08-16.2 | The report numbers recompute from AS predictions: sheet 02 held-out keep P/R 0.973/0.973 before and 0.973/0.986 after; combined held-out keep/drop accuracy 0.807 to 0.893 and 3-way 0.727 to 0.820 | result | region-quality, lota | confirmed | [Step 16](#step-16-region-quality-filter-c16) |
| F08-16.3 | All 16 combined-held-out keep/drop errors: 10 are digit/punctuation-only regions (numbers, tallies, times) that the gold keeps or drops inconsistently, 3 are short abbreviation or letter items with debatable gold labels, 2 are HTR garble kept as `fragment`, 1 is an abbreviation+number dropped. Of the 11 usable/fragment confusions (review: all 11 read), 9 gold-usable regions sit at score 0.4 (8 through `short_region`, 1 long garbled line through `no_lexicon_hits_despite_length`), and 2 gold-fragment regions are lifted to usable by `strong_lexicon_hits` on a 2-token region (review: one of them, "xxiiij £", is a roman numeral, F08-R.1) | data-property | region-quality, sg | revised | [Step 16](#step-16-region-quality-filter-c16) |
| F08-16.4 | In the held-out 1-3-token band (76 regions, 10 gold noise) the retune removed the noise over-prediction by count (25 predicted noise before, 10 after). It only moved regions out of noise (15, of which 14 are gold-keep; none moved in), so noise precision in the band rose from 0.20 (5/25) to 0.40 (4/10) and noise recall fell from 5/10 to 4/10; 6 of the 10 remaining predicted noise are numeric regions the gold keeps (revised at review: the step said the retune moved regions "into noise elsewhere") | result | region-quality, sg | revised | [Step 16](#step-16-region-quality-filter-c16) |
| F08-17.1 | The AS port of the duplicate detector is byte-identical to LotA's: `dup_check` 17/17, and the full SG run (9,122 inventories, 4,105.6 s, 4.70 GB) gives the same candidate table (48,473 rows) and manifest | result | duplicates, sg, lota | confirmed | [Step 17](#step-17-duplicate-page-detector-c17) |
| F08-17.2 | Adjacency by position (page at p with every page at p+1) finds all 13 documented re-shoots as 23 rows, all adjacent, all same-side pairs across scans N and N+1; the cross pairs are compared and rejected | method | duplicates, sg | confirmed | [Step 17](#step-17-duplicate-page-detector-c17) |
| F08-17.3 | Non-adjacent rows (sample A, 30 of 24,976 outside inv 638-643) are 4 re-shoots, 19 repeated formulas or headings (passport grants, session heading plus missive, requeste openers), 2 form pages and 5 parallel copies of longer texts: 0.13 re-shoots, 0.30 with the parallel copies. Review: read in full, 12020 scan 149 page-1/page-2 is two letters of 2 April 1733 with the same wording to two envoys (Vienna, London), a formula rather than a copy, so 4 parallel copies and 0.27 | data-property | duplicates, sg | revised | [Step 17](#step-17-duplicate-page-detector-c17) |
| F08-17.4 | Adjacent rows outside the 23 documented (sample B, 20 of 14,971) are 19 re-shoots and 1 formula page (0.95; Wilson interval about 0.76-0.99), at J = 0.63-0.97. Review: 13 of the 19 re-shoots differ in their first 60 characters (region order, marginalia), not 4; the 5 of these read in full (1560, 8579, 8744, 11705, 12141) have the same body and the same ending, so the verdicts hold | result | duplicates, sg | revised | [Step 17](#step-17-duplicate-page-detector-c17) |
| F08-17.5 | The 9 rows without `scan_number_b` are all re-shoots between a scan and its `deelopname` partial-shot scan; their names carry no scan number, so they are never adjacent and only the LSH finds them. A filter on `adjacent` alone would drop them | data-property | duplicates, sg, lota | confirmed | [Step 17](#step-17-duplicate-page-detector-c17) |
| F08-17b.1 | `page_duplication` (Case 5, per-page rate, one draw per page) gives copy fractions of 0.096-0.115 / 0.285-0.290 / 0.483-0.504 at rates 0.1 / 0.3 / 0.5 on 3771 (575 units), all within +-0.06 of the rate; on gm 1363 (348 units) 0.098-0.115 / 0.273-0.325 / 0.486-0.509 | result | simulation, sg | confirmed | [Step 17b](#step-17b-page_duplication-operator) |
| F08-17b.2 | The first-occurrence rule changes 0-3 start labels per run on 3771 (3 sections) and 0-6 on gm 1363 (11 documents); the old rule's start F1 falls to 0.667 (3771) and 0.786 (gm 1363) at rate 0.5, WindowDiff up to 0.24 / 0.22 | result | simulation, sg | confirmed | [Step 17b](#step-17b-page_duplication-operator) |
| F08-17b.3 | A copy changes the Framing-B pair share by under 0.01 (3771: 0.9965 canonical, 0.9977 at rate 0.5; gm 1363: 0.9712, 0.9809), because copies are same-document pairs. Duplication is a Framing-A problem only | data-property | simulation | confirmed | [Step 17b](#step-17b-page_duplication-operator) |
| F08-R.1 | LotA's injected lexicon (fastText vocabulary, 233,538 terms) holds 9,298 all-digit terms (145 of the 2,837-term fixture subset), so numbers of 4 or more digits count as lexicon hits: `strong_lexicon_hits` lifts the held-out tally "3761 -10 3" (gold noise) to usable at 0.5, fires on "15 8 1011 1011 8 S 0 0 S 01", and lifts the roman numeral "xxiiij £" (gold fragment) to usable. The AS docstring now says so; removing numerals from LotA's lexicon would change LotA's outputs and is LotA's call | open-issue | region-quality, lexicon, sg, lota | confirmed | [Review](#review) |
| F08-R.2 | Jaccard alone does not separate re-shoots from formulaic pages: the 23 documented re-shoot rows start at J = 0.615, just above the 0.6 threshold, and the 59 sampled rows overlap (re-shoots 0.628-0.968, median 0.81; others 0.602-0.763, median 0.66). Adjacency separates them far better (0.95 vs 0.13 re-shoots). Re-shoots with poorer HTR may fall below 0.6; that recall is unmeasured | data-property | duplicates, sg | confirmed | [Review](#review) |
| F08-R.3 | The review re-run reproduces every acceptance number: `lota_check` 4 × 0 of 150; `iq_check` 0 mismatches over 428,544 regions (37 s, unchanged from planning, so building `QualityThresholds` per region costs nothing measurable); `metrics.py` 0.973/0.973 → 0.973/0.986, 0.807 → 0.893, 0.727 → 0.820; `dup_check` 17/17; `cmp` of both full-run TSVs (written 18:31, after the 17:20 edit of `integrity.py`); `reshoots.py` 13/13, 23 rows; `run17b.py` identical on both streams; AS 1,166 passed, LotA 368 + 1 xfailed, lint clean | result | review, rerun | confirmed | [Review](#review) |
| F08-R.4 | The ports dropped two LotA rationales: the calibration of the 100-shingle floor (the 755 smoke-set candidates) and decision P2-12 behind `DEFAULT_KEEP_CLASSES`. LotA's new comment pointed to an AS docstring that did not have the notes, LotA's `__all__` listed only the five re-exports, and the changelog missed two behaviour changes (default `simulate_inventory` cases; `ValueError`s in `duplicate_candidates`). All fixed in the review, docs and comments only | bug-fixed | review, docs, lota | confirmed | [Review](#review) |

- **Kind:** `result` (an acceptance number), `behaviour-change` (output differs, as intended),
  `bug-fixed`, `data-property` (a fact about the archives or the corpus), `negative-result`,
  `open-issue` (found, not fixed), `method` (a lesson about how to do the work).
- **Tags:** lower-case keywords, comma-separated: the archive (`sg`, `voc`, …), the component
  and the sibling (`lota`, `nlp`).
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Measured on 2026-10-21 with the scripts in
[`data/phase-08-region-quality-and-duplicates/planning/`](data/phase-08-region-quality-and-duplicates/planning/README.md),
which read LotA's code and stored outputs and write nothing.

| Check | Result |
|---|---|
| Gold re-scoring, current LotA code (`gold_check.py`) | 0 mismatches in each of 4 × 150 rows |
| `inventory_quality`, 200 seeded inventories (`iq_check.py 20261021 200`) | 0 mismatches; 428,544 regions; 37 s |
| `duplicate_candidates`, 7 inventories (`dup_check.py`) | 7/7 identical; 638 has 2,681 rows |
| Documented re-shoots in the stored table (`reshoots.py`) | 13/13; 23 rows; all adjacent and same-side |

User decisions: the phase-07 text-layer follow-ups go to a new phase 09 (P1);
`page_duplication` only, with the copy right after the original (P2); a start page is labelled a
start at its first occurrence only (P3).

**Findings:** F08-P.1–F08-P.8.

### Step 16: Region-quality filter (C16)

**What changed.** New `archival_structures/analysis/region_quality.py` (pure port of LotA's rule cascade on
region text; injected lexicon; `QualityThresholds` dataclass; `quality_frame` for a DataFrame), 19 tests in
`tests/test_analysis_region_quality.py`, fixture `tests/fixtures/region_quality/` (300 gold rows with LotA's stored
before/after verdicts, 2,837-term lexicon subset, README). LotA `corpus/quality.py` now calls AS (wrappers keep
their signatures; `THRESHOLDS` stays the commented dict and is read at call time), plus
`test_thresholds_match_as_defaults` and a section in LotA `docs/from-archival-structures.md`. Scripts in
[`data/phase-08-region-quality-and-duplicates/step-16/`](data/phase-08-region-quality-and-duplicates/step-16/)
(`build_fixture.py`, `metrics.py`, `lota_check.py`, `protect.py`, `predictions.tsv`).

**Results against the acceptance criteria.**

| Criterion | Result |
|---|---|
| Fixture: 300 rows, texts equal the scored files row by row, lexicon subset 2,837 terms | met (assertions pass; 2,837) |
| Sheet 02 held-out 75, keep P / R, before | 0.973 / 0.973 |
| Sheet 02 held-out 75, keep P / R, after | 0.973 / 0.986 |
| Combined held-out 150, keep/drop accuracy, before / after | 0.807 / 0.893 |
| Combined held-out 150, 3-way accuracy, before / after | 0.727 / 0.820 |
| AS predictions equal LotA's stored class, rules and score (1e-9) | 300/300 after, 300/300 before |
| `lota_check.py` through the LotA wrappers | `mismatches 0 of 150` four times |
| `iq_check.py 20261021 200` | `mismatches []`, 200 inventories, 428,544 regions |
| AS tests | step 16 module 19 passed; full suite 1,153 passed (1,123 + 19 + 11 from step 17b) |
| LotA | `make test` 368 passed + 1 xfailed (369 collected), `make lint` clean |
| Protected outputs (`protect.py`, 39 lines incl. 3 sha256) | unchanged |

Combined held-out keep P / R for reference: 0.951 / 0.835 before, 0.949 / 0.935 after. The retune bought
recall (+0.10) at no cost in precision.

**Error analysis.** Predictions are those of the defaults on the combined held-out 150 (75 per sheet; sheet 02c
oversamples short regions, so the error mix is short-region heavy by construction). I read all 16 keep/drop
errors (9 gold-keep predicted noise, 7 gold-noise predicted keep) and 10 of the 11 usable/fragment confusions
(seed 20261021; one left out by the sample).

| Category | n | Cases (sheet row: gold -> predicted, rules, text) |
|---|---|---|
| numeric tally or digit-punctuation run (other) | 7 | 02c 32 fragment -> noise `long_nonalpha_run` "441 440."; 02c 84 usable -> noise "33:06:08"; 02c 120 usable -> noise "125, 125, 125."; 02c 126 usable -> noise "Westeinonak284 271 244, 225, 211, 229"; 02c 134 usable -> noise "2. 10. 4. 4."; 02c 150 usable -> noise "15 8 1011 1011 8 S 0 0 S 01"; 02c 58 noise -> usable "3761 -10 3" |
| bare number / folio | 3 | 02 46 usable -> noise "1" (`mostly_singleton_tokens`, `short_region`); 02c 104 fragment -> noise "7"; 02c 22 noise -> fragment "10 29" |
| abbreviation or proper-name list | 1 | 02c 46 usable -> noise "Cat: 8" (single-char token, 2 tokens) |
| HTR garble | 2 | 02 110 noise -> fragment "Aerasse ooo ad 2s angenoehe 4" (`no_lexicon_hits_despite_length`, score 0.40); 02c 8 noise -> fragment "Ine" (`short_region`) |
| short complete phrase | 0 | none among the errors |
| gold label debatable | 3 | 02c 34 noise -> fragment "fol. 89."; 02c 48 noise -> fragment "Na. 5." (both look like "Cat: 8", which gold calls usable); 02 30 noise -> fragment "c." |

Usable/fragment confusions (10 read): 8 gold-usable predicted fragment, 2 gold-fragment predicted usable. All 8
are `short_region` (or a number with the numeric exemption) at score 0.40 with no lexicon hit, for example 02 88
"de Bie", 02c 74 "Resolde Cos", 02c 28 and 30 "De Statenbr.", 02c 14 "21", 02c 122 "30", 02 124 (a garbled
long line, `no_lexicon_hits_despite_length`). The two upward cases carry `strong_lexicon_hits` on a 2-token
region (02 54 "er Ripperda", 02c 4 "xxiiij £").

**Rule pairs behind the remaining errors.** Ten of 16 involve the numeric exemption: the exemption removes the
`low_alpha_content` penalty, but `long_nonalpha_run` (-0.30) and `mostly_singleton_tokens` (-0.20), plus
`short_region` (-0.10), still push a numeric region to noise (0.1-0.3), while the gold keeps most tallies and
times. The pair `numeric_content_exempt` + `long_nonalpha_run` is the largest single cause (7 errors). The other
cause is the `short_region` rule giving 0.40 (fragment) to every 1-2-token region that has no lexicon hit, so
2-token garble ("Ine") and 2-token usable names ("de Bie") land in the same class.

**LotA Rule 8 (1-3-token band over-predicts noise about 3x).** In the held-out half the band has 76 regions,
10 gold noise. Before the retune 25 are predicted noise (2.5x), after 10 (1.0x); over all 300 rows 54 -> 25
against 22 gold (2.5x -> 1.1x). By count the over-prediction is fixed. By identity it is not: only 4 of the 10
predicted noise are gold noise after (5 of 25 before), so the retune moved the wrong regions out of noise and
into noise elsewhere (numeric tallies). No threshold was changed here.

**Conclusion.** The port is exact, so the filter's validity is LotA's: good on prose (107 of 123 gold-usable
held-out are predicted usable), unreliable on the 1-3-token band, where gold labels for numbers and
abbreviations are themselves inconsistent (at least 3 cases). A consumer that needs the short band should
treat `fragment` and numeric-exempt regions as "keep" and not rely on `noise` there.

**Findings:** F08-16.1-F08-16.4.

### Step 17: Duplicate-page detector (C17)

**What changed.** AS gets `analysis/duplicate_pages.py` (MinHash/LSH over character 5-gram shingles, exact-Jaccard
confirmation, forced candidates for pages at adjacent positions) over plain `(page_id, text, position)` triples, with
13 tests. LotA's `duplicate_candidates` keeps its signature, page splitter, manifest and CLI, builds the page text and
calls AS, then renames `position_*` to `scan_number_*` and restores the 11 columns and the two `attrs`. LotA lost about
120 lines (constants, `_shingles`, MinHash, LSH, and the `random`, `zlib`, `numpy` imports).

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | AS tests green | 13 new tests pass; AS full suite 1,166 passed |
| 2 | `dup_check.py` on 17 inventories | `same True` 17/17 (the 7 planning inventories and the 10 re-shoot ones; row counts equal to the stored table) |
| 3 | Full SG run, `cmp` | candidate table (48,473 rows) and manifest both byte-identical; 4,105.6 s, peak RSS 4.70 GB, 8 workers |
| 4 | `reshoots.py` on the new table | `found 13/13, rows 23`, table rows 48,473, adjacent 14,994 |
| 5 | LotA `make test` / `make lint`; protected outputs | 368 passed + 1 xfailed (no drop); lint clean; `protect_before.txt` equals `protect_after.txt` (119 files) |

The adjacency rule is pinned by tests: the four-page test (positions 10, 10, 11, 11) finds both same-side pairs and
rejects the cross pairs, and a band-free pair (Jaccard in [0.6, 0.7), no shared LSH band, found by trial) is reported
with `adjacency=True` and dropped with `adjacency=False`, while the `adjacent` column still reports position adjacency.

**Error analysis.** Sampled with `pandas.sample(random_state=20261021)` from the new table; first 150 characters of both
page texts read for all 59 rows (`step-17/ea_sample.py`, texts in `ea_sample_texts.tsv`, verdicts in `ea_verdicts.tsv`).
Verdicts come from those 150 characters only, so a `re-shoot` means "same text", which may be a second shot or a
duplicated image.

| Sample | n (pool) | re-shoot | repeated formula or heading | form / template | other: parallel copy | blank-ish / index |
|---|---|---|---|---|---|---|
| A: non-adjacent, outside 638-643 | 30 (24,976) | 4 | 19 | 2 | 5 | 0 |
| B: adjacent, not documented | 20 (14,971) | 19 | 1 | 0 | 0 | 0 |
| C: no `scan_number_b` | 9 (9) | 9 | 0 | 0 | 0 | 0 |

- Sample A is mostly formulaic text. Passport requests ("Op de Requeste van ... is na voorgaende deliberatie
  goetgevonden ende verstaen, dat ten behoeve van den Suppliant een Pasport ...", inv 690, 700, 615, 664, 725),
  session headings plus an envoy's missive ("Mercurii den 18. January 1758.Ontfangen een missive van den Heere van
  Marteville ...", inv 4444, 4445, 4501, 928), and "Is ter vergaderinge geleesen de Requeste van ... koopman te
  Rotterdam" (inv 1559) share most 5-grams while the persons differ. These pages have 250-750 shingles, so the
  100-shingle floor does not exclude them. The 4 re-shoots are genuine copies far apart: inv 12136 scans 3 and 87
  ("( 5 )Rapport van een Committe van het Congres", 1,861/1,845 shingles), 12136 scans 50 and 56, 8805 scans 197 and
  199, and 12236 scans 312 and 322 (both start "( 8 ) teyt verschillen", J = 0.954). The 5 "parallel copy" rows are
  longer legal or treaty texts that recur with different page numbers (inv 12335, "201." and "382." before the same
  "adsigneren renten erffelijck ofte ten lijve"; inv 12491, a French treaty article on scans 317 and 319); they may be
  drafts and register copies.
- Sample B: 19 of 20 are the same text on scans N and N+1, with ATR variants ("Hoofd schout" / "Hoofel schout",
  "WestminstenDaaghs" / "WoestminsteDaaghs"), at J = 0.63-0.97. Four are cases where the second page starts at another
  region (inv 2923 scans 139/140: B starts with the tail of the previous page, then the same "Is ter Vergaderinge
  geleesen de Reques,te va[n] Thyme Lubberts"; inv 8730 scans 27/28: different first 150 characters, J = 0.883), so the
  text agrees but the region order does not. The one non-copy is inv 3622 scans 1153/1154 (J = 0.647), two different
  passport grants for the Griffier Fagel. The adjacent precision is therefore 19/20 = 0.95 (Wilson 95% interval about
  0.76-0.99); sample A's non-adjacent rows are 4/30 = 0.13 re-shoots (9/30 = 0.30 with the parallel copies).
- Sample C: all 9 pairs are a scan and its `deelopname` ("partial shot") scan (`..._0237_deelopname2`, `..._0065_deelopname1`;
  inv 11411, 11423, 11432, 11518, 12168 and 5751 with a `5751B_P001` scan). These file names carry no scan number, so
  the pair is never adjacent and only the LSH finds it (J = 0.73-0.92). All 9 are the same text (a map legend, a
  letter, a testament, a regulation).
- LotA's open question, whether to filter on `adjacent`: in these samples `adjacent=True` is 0.95 re-shoots, and
  `adjacent=False` is 0.13 re-shoots, but the 9 rows without a scan number are all re-shoots and all non-adjacent, so
  a filter on `adjacent` alone would drop them. Not decided here.
- Conclusion: the detector's precision is high where adjacency applies and low among far-apart pairs, where the
  formulaic decisions of the Staten Generaal (passports, session headings) dominate. The port is exact, so these
  numbers also describe LotA's stored table.

**Draft findings.** F08-17.1-F08-17.5 (see the Findings table).

**Files.** `step-17/`: `coeff_pin.txt`, `protect.py`, `protect_before.txt`, `protect_after.txt`, `ea_sample.py`,
`ea_sample_texts.tsv`, `ea_verdicts.tsv`. The new LotA output is `output/integrity/as-step17/` (candidate table and
manifest; identical to the stored ones).

**Findings:** F08-17.1-F08-17.5.

### Step 17b: page_duplication operator

**What changed.** Brief: [`step-17b`](../plan/phase-08-region-quality-and-duplicates/step-17b-page-duplication-operator.md).
New operator `page_duplication(stream, provenance, rate, rng)` in
`archival_structures/stream_analysis/corruption/operators.py`: after each page, with probability
`rate` (per page), the same unit is appended again with its provenance tagged
`page_duplication`; one `rng.random()` draw per input page. `observed_start_labels` now labels
only the first occurrence of a canonical index as a start (identical output for the four
existing operators). `CASE_NAMES` gets `'page_duplication'` last. Docs: Case 5 in
`page_order_problems.md`, stage 2 and 3 in `simulation_harness.md`. Changelog: no sibling
imports affected. Scripts: `data/phase-08-region-quality-and-duplicates/step-17b/`
(`run17b.py`, `cases.py`; outputs written to the session scratchpad, not kept).

**Results against the acceptance criteria.**

1. Corruption tests: 61 existing pass unchanged (one rename), 11 new (8 `TestPageDuplication`,
   2 metrics, 1 simulate): 72 passed.
2. AS full suite: 1,153 passed (includes whatever the concurrent step 16 had added at that
   moment).
3. Real-data run, 3771 `section` layer (575 units, 3 documents), `n_copies / 575`:

| rate | seed 0 | seed 1 | seed 2 |
|---|---|---|---|
| 0.1 | 0.115 (66) | 0.097 (56) | 0.096 (55) |
| 0.3 | 0.285 (164) | 0.290 (167) | 0.290 (167) |
| 0.5 | 0.499 (287) | 0.483 (278) | 0.504 (290) |

   All nine within +-0.06 of the rate (largest deviation 0.017).

**Error analysis.** Old rule = `canonical_index in starts` at every position; true = new
`observed_start_labels`. `copies_of_starts` = copies whose original is a start page. The old
rule's extra True count equals `copies_of_starts` in every run.

3771 (575 units, 3 sections; canonical pair share 0.9965):

| rate | seed | n_copies | copies_of_starts | F1 (true vs old) | WindowDiff | pair share |
|---|---|---|---|---|---|---|
| 0.1 | 0 / 1 / 2 | 66 / 56 / 55 | 0 / 1 / 0 | 1.0 / 0.857 / 1.0 | 0 / 0.044 / 0 | 0.9969 / 0.9968 / 0.9968 |
| 0.3 | 0 / 1 / 2 | 164 / 167 / 167 | 0 / 3 / 1 | 1.0 / 0.667 / 0.857 | 0 / 0.243 / 0.037 | 0.9973 (all) |
| 0.5 | 0 / 1 / 2 | 287 / 278 / 290 | 1 / 3 / 2 | 0.857 / 0.667 / 0.75 | 0.201 / 0.242 / 0.237 | 0.9977 (all) |

   With 3 starts, at most 3 copies of starts are possible; rate 0.3 seed 1 and rate 0.5 seed 1
   copied all three.

gm 1363 (`document` layer, 348 units, 11 documents; canonical pair share 0.9712):

| rate | seed | n_copies | copies_of_starts | F1 | WindowDiff | pair share |
|---|---|---|---|---|---|---|
| 0.1 | 0 / 1 / 2 | 34 / 40 / 38 | 0 / 1 / 0 | 1.0 / 0.957 / 1.0 | 0 / 0.049 / 0 | 0.9738 / 0.9742 / 0.9740 |
| 0.3 | 0 / 1 / 2 | 95 / 113 / 103 | 2 / 4 / 2 | 0.917 / 0.846 / 0.917 | 0.095 / 0.146 / 0.093 | 0.9774 / 0.9783 / 0.9778 |
| 0.5 | 0 / 1 / 2 | 172 / 177 / 169 | 5 / 5 / 6 | 0.815 / 0.815 / 0.786 | 0.222 / 0.194 / 0.223 | 0.9807 / 0.9809 / 0.9806 |

   Copy fractions on gm 1363 also track the rate (0.098-0.115, 0.273-0.325, 0.486-0.509).

*Cases read (seed 0, rate 0.3).* On 3771 there are no copies of start pages, so the 10 read are
all middle-page copies: observed positions 4, 14, 18, 29, 37, 41, 47, 51, 55, 58 (canonical 3,
12, 15, 25, 32, 35, 40, 43, 46, 48; sec_001 and sec_002). Each is directly preceded by its
original (same canonical index), none is a start, and the new and old labels are both False.
On gm 1363 the two copies of start pages are observed position 237 (canonical 187, start of
`gm_0248`) and 351 (canonical 276, start of `gm_0251`): the original at the previous position is
labelled True, the copy False under the new rule, and the old rule labelled the copy True. The 8
middle-page copies read (positions 4, 14, 18, 29, 37, 41, 47, 51, all of `gm_0246`) are False
under both rules. All 10 labels per stream were confirmed by hand against the canonical stream.

**Conclusion.** The operator behaves as specified: the copy rate matches the rate on both
streams, copies are always adjacent to their original, and the first-occurrence rule is what
separates a repeated start page from a new document. The effect on Framing A is visible only
when a start page is copied; with few documents (3 on 3771) a run at low rate often has none, so
F1 jumps between 1.0 and 0.67 depending on the seed. Framing B is essentially unaffected (pair
share moves by under 0.01). The harness has no negative result here, but it also does not show
how a real classifier copes with re-shoots: that needs a classifier run and is not part of this
step.

**Findings:** F08-17b.1-F08-17b.3.

## Review

Read with the code and the diff, not the log. Details in
[`plan/phase-08-region-quality-and-duplicates/REVIEW.md`](../plan/phase-08-region-quality-and-duplicates/REVIEW.md).

**Re-run.** Every acceptance check was re-run except the 68-min full SG run. Its two TSVs were
`cmp`-ed against the stored files, and their mtimes (18:31) postdate the edit of LotA
`integrity.py` (17:20). All numbers reproduce (F08-R.3).

**Step 16 error analysis, checked.** I recomputed the held-out errors from `predictions.tsv`.

- The 16 keep/drop errors and their categories hold. 10 involve `numeric_content_exempt`, and 7
  pair it with `long_nonalpha_run`.
- Three corrections:
  - there are 11 usable/fragment confusions, and 9 of them are gold-usable regions at 0.4, not 8;
  - not all 11 fire `short_region`;
  - the retune cannot move a region *into* noise. An exemption only raises scores: it moved 15
    regions out of noise, 14 of them gold-keep (F08-16.3, F08-16.4 revised).
- New: why "3761 -10 3" (gold noise) is predicted usable. "3761" is in the fastText vocabulary,
  so `strong_lexicon_hits` fires. The vocabulary holds 9,298 all-digit terms (F08-R.1).
- No leakage: the metrics use the even (held-out) rows, the thresholds come from the odd rows, and
  the fixture's lexicon subset is built from gold *text*, not labels. The 300-row band counts in
  the step's Rule 8 paragraph include the tune half and are descriptive only.

**Step 17 error analysis, checked.** I read the opening text of all 30 sample-A rows. For 6 A
rows, 5 B rows and 2 C rows I also read the full page texts, with `difflib` matching blocks
(script in the session scratchpad, read-only).

- **Sample A:**
  - 12460 24/120 is a resolution of 18 June 1680 and its "Extract uijt het Register": a real
    parallel copy;
  - 12020 149 p1/p2 is two letters of 2 April 1733 with the same wording to the envoys at Vienna
    and London: a formula, not a copy;
  - 12290, 3616, 2922 and 3622 are commissions, requests and passports for different persons or
    ships, so the formula verdicts hold.
- **Sample B:** all 5 low-J re-shoots read in full have the same body and ending. 1560 297/298 is
  one request at J = 0.628, so HTR variation alone brings a true re-shoot close to the threshold.
- **Sample C:** 5751A 0568 and 5751B P001 are two shots of the 1626 letter on the *Wapen van
  Amsterdam*. 12168 0620 and its `deelopname1` carry the same *Ordre ende Reglement*.
- **Conclusion:** the verdicts and the main conclusion hold (F08-17.3, F08-17.4 revised in detail).
  Opening characters alone could not decide 13 of the 20 B rows. The full texts settle the ones
  read.

**Step 17b, checked.** `run17b.py` reproduces both tables exactly. The diff of the 61 existing
tests removes one line (the rename). `observed_start_labels` is unchanged for streams without
repeats.

**Fixed in the review** (docs and comments only, F08-R.4):

- the AS `duplicate_pages` docstring now carries the SG calibration of the 100-shingle floor and
  the threshold (the LotA comment pointed there);
- the `region_features` docstring now notes the numeral hits;
- LotA `quality.py` gets back the P2-12 rationale and a complete `__all__`;
- the changelog lists two behaviour changes the steps left out.

AS tests 32/32 on the two modules after the edits; LotA lint clean and its quality/integrity tests
32/32.

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F08-17.4, F08-17.5, F08-R.2 | LotA's open question, whether to filter the candidate table on `adjacent`. Adjacent rows are 0.95 re-shoots, but the 9 `deelopname` rows are re-shoots and never adjacent | LotA `docs/from-archival-structures.md` §7; AS `plan/CONTEXT.md` §4 |
| F08-R.1 | Numerals in LotA's region-quality lexicon. Dropping them changes LotA's stored outputs, so it is LotA's decision (its Rule 8 work) | AS `plan/CONTEXT.md` §4 |
| F08-R.2 | Re-shoot recall below J = 0.6 is unmeasured. A read of adjacent same-side pairs at J 0.4–0.6 would measure it | AS `plan/CONTEXT.md` §4 (phase 10 candidate) |
| F08-17b.2 | How a real start-page classifier copes with re-shoots needs a classifier run under `page_duplication` | AS `plan/CONTEXT.md` §4 (NLP or phase 10 candidate) |

## Reproduce

Planning checks: see the planning `README.md` linked above. The steps add their commands here.

Step 16 (AS root unless stated; the `-I` flag is not used because the scripts are ours):

```
# from the LotA root: fixture (writes only tests/fixtures/region_quality/ in AS) and LotA wrapper check
venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/step-16/build_fixture.py
venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/step-16/lota_check.py
venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/planning/iq_check.py 20261021 200
# from the AS root
.venv/bin/python reports/data/phase-08-region-quality-and-duplicates/step-16/protect.py out.txt   # diff vs protect_before.txt
PYTHONPATH=. .venv/bin/python reports/data/phase-08-region-quality-and-duplicates/step-16/metrics.py
.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_region_quality.py
```
