# Phase 03 report: Evaluation for NLP

> **Status:** final (phase review, 2026-10-14).
> **Phase plan:** [`plan/phase-03-evaluation-for-nlp/PLAN.md`](../plan/phase-03-evaluation-for-nlp/PLAN.md).
> **Code state:** AS unreleased after v0.6.0 (to be released as v0.7.0); NLP 2026-10-14 (after step 09, uncommitted).
> **Last updated:** 2026-10-14, by the phase review (Opus).

## Summary

AS now has one segmentation-evaluation package, `stream_analysis.evaluation`. It holds the
metrics (`segmentation`, `aggregate`), the document-atomic and inventory-held-out splits with
public leakage validators (`splits`), and the trivial baselines, with the oracle labelled
(`baselines`). Labels are bool, and NLP's window size is an explicit `k_convention="pages"`.
NLP's evaluation, chunk-split and baseline code now delegate to AS through shims, and its stored
numbers reproduce exactly: the CRF metrics bit for bit (F03-07.1, F03-07b.1), all 7 cached split
CSVs byte for byte (F03-08.1), and every deterministic trivial-baseline cell (F03-09.1, F03-R.2).

What a reader should know:
- Pk/WindowDiff floors are not monotone in "knowledge": on long-chunk dev, majority (0.303)
  beats the own-rate oracle (0.515) on 16 of 22 inventories (F03-09.2).
- NLP's long-chunk and held-out splits are not nested, so a component fitted under one and
  scored under the other leaks (F03-08.5).
- A pre-existing provenance bug in the corruption harness was found and fixed (F03-09.5,
  F03-R.1).
- Step 09 overwrote one stored, untracked NLP output; nothing deterministic was lost (F03-09.7,
  F03-R.2).

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`, e.g. `F03-07.2`. Planning findings use
`F03-P.<n>`, and the review's own findings use `F03-R.<n>`. IDs never change and are never
reused. A finding that turns out to be wrong is marked `withdrawn`, not deleted.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F03-P.1 | NLP's `trivial_baseline_metrics.csv` cannot be fully reproduced: the every-k and oracle rows were bootstrapped with `seed=None`, so 16 of 360 cells (4 rows × 4 CI columns) are random | open-issue | nlp, baselines, reproducibility | confirmed | [Planning](#planning) |
| F03-P.2 | NLP's split cache is keyed by `(kind, seed)` only: `long_chunk_seed1` holds context window W=1, not a second seed, and a `force=True` rebuild with other params silently overwrites a cached split | open-issue | nlp, splits | confirmed | [Planning](#planning) |
| F03-P.3 | `output/long_sequence_subsequences.csv` (2026-09-30) predates the 1539 duplicate-row fix and has one row more than `long_chunk_seed0` (`1539/NL-HaNA_1.04.02_1539_1048`), yet NLP `CLAUDE.md` trap 2 names it as the split sequence models must consume | open-issue | nlp, splits, voc | confirmed | [Planning](#planning) |
| F03-P.4 | NLP's `tests/`, `scripts/run_trivial_baselines.py`, `output/splits/` and the stored metric CSVs are untracked in git, so the only backup of the evaluation tests and baseline script is a copy. *Revised by the review: the user committed `tests/`, `scripts/` and `src/` before step 07; `output/` stays untracked, which is how F03-09.7 lost a file* | open-issue | nlp | revised | [Planning](#planning) |
| F03-P.5 | NLP's VOC end-page rule (start scan number − 1) and physical adjacency give the same 1,135 document ends in all 22 annotated inventories | data-property | voc, nlp, splits | confirmed | [Planning](#planning) |
| F03-07.1 | AS `evaluate_all(k_convention="pages")` reproduces NLP's `evaluate_all` exactly on the CRF scorefix decode: 12 of 12 result sets deep-equal (bit-exact frames, scalars, CIs) | result | nlp, evaluation, crf | confirmed | [Step 07](#step-07-c6-evaluation-suite-in-as-corruptionmetrics-becomes-a-wrapper) |
| F03-07.2 | No Pk/WD window is clipped on the CRF decodes: 0 of 22 inventories (long-chunk) and 0 of 5 (held-out) have a run shorter than k+1, min run length 12 and 642 | negative-result | nlp, evaluation, windows | confirmed | [Step 07](#step-07-c6-evaluation-suite-in-as-corruptionmetrics-becomes-a-wrapper) |
| F03-07.3 | The "gaps" and "pages" default k differ in 1 of 22 inventories (2448: n=2636, S=154, k 8 vs 9) | data-property | nlp, evaluation, voc | confirmed | [Step 07](#step-07-c6-evaluation-suite-in-as-corruptionmetrics-becomes-a-wrapper) |
| F03-07.4 | Switching the CRF decode to the AS "gaps" k lowers macro Pk by 0.0017-0.0039 and WD by 0.0017-0.0038 on long-chunk seeds 0-2 and changes nothing on the held-out splits | result | nlp, evaluation | confirmed | [Step 07](#step-07-c6-evaluation-suite-in-as-corruptionmetrics-becomes-a-wrapper) |
| F03-07.5 | Non-bool labels now raise `TypeError` in `corruption.metrics` (`score_start_classification`, `score_same_document_pairs`, `window_diff`) | behaviour-change | corruption, evaluation | confirmed | [Step 07](#step-07-c6-evaluation-suite-in-as-corruptionmetrics-becomes-a-wrapper) |
| F03-08.1 | The moved split code rebuilds all 7 cached NLP splits byte-identically (CSV and manifest), before and after the NLP edit, with the cache untouched; the VOC end-id rule is replaced by adjacency | result | nlp, splits | confirmed | [Step 08](#step-08-c7-generic-splits-held-out-builder-public-validators-check_nested_split) |
| F03-08.2 | `long_chunk` (K=12) gives 238 chunks; 49 are oversized single-document chunks, and the 5 largest hold 710, 614, 364, 261 and 247 core pages (median chunk 70, min 3) | data-property | nlp, splits, voc | confirmed | [Step 08](#step-08-c7-generic-splits-held-out-builder-public-validators-check_nested_split) |
| F03-08.3 | Only 8023 lacks a partition (no test pages): it has 5 chunks, so striding yields train x4 + dev and the rebalance never reaches it; 7923 also has 5 chunks but gets a test chunk (44 pages) from the rebalance | data-property | nlp, splits, p2 | confirmed | [Step 08](#step-08-c7-generic-splits-held-out-builder-public-validators-check_nested_split) |
| F03-08.4 | The greedy rebalance makes single huge documents the dev/test content of some inventories: 1529 has test 614 > train 220 pages, dev 364 (two single-document chunks of 614 and 364 pages) | data-property | nlp, splits, p2 | confirmed | [Step 08](#step-08-c7-generic-splits-held-out-builder-public-validators-check_nested_split) |
| F03-08.5 | `long_chunk_seed0` and `inventory_held_out_seed0` are not nested: 2,445 long-chunk train pages lie in held-out inventories, and 5,066 held-out train pages lie in long-chunk non-train splits, so a component trained on one split and scored on the other leaks (rule 1) | result | nlp, splits, leakage | confirmed | [Step 08](#step-08-c7-generic-splits-held-out-builder-public-validators-check_nested_split) |
| F03-07b.1 | Through the shim, NLP's stored CRF metrics reproduce exactly (12 rows x 19 columns = 228 cells) and so do the trivial baselines (344 of 360 cells, 16 excluded); the A/B against the original module is deep-equal for 4 of 4 methods | result | nlp, evaluation, crf, baselines | confirmed | [Step 07b](#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced) |
| F03-07b.2 | `pd.read_csv` with the default float parser mis-reads the stored CRF metrics by up to 96 ulp (44 of 228 cells differ); exact comparison needs `float_precision="round_trip"` | method | nlp, reproducibility | confirmed | [Step 07b](#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced) |
| F03-07b.3 | `full_sequences` has exactly two label values, `not_start_page` (18,538) and `start_page` (2,367), so the shim's "every other string is negative" rule has no effect on this data | data-property | nlp, voc | confirmed | [Step 07b](#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced) |
| F03-07b.4 | The CRF long-chunk seed-0 run raises 0 `WindowClippedWarning` through the shim, so the D3 `pages` override protects only the 2448 k difference | negative-result | nlp, evaluation, windows | confirmed | [Step 07b](#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced) |
| F03-07b.5 | Switching the shim to the AS "gaps" k would move CRF long-chunk seed-0 macro Pk from 0.21981 to 0.21809 and WD from 0.25026 to 0.24854, and the held-out seed-42 numbers not at all | result | nlp, evaluation | confirmed | [Step 07b](#step-07b-nlp-evaluationpy-becomes-a-shim-over-as-stored-numbers-reproduced) |
| F03-09.1 | NLP's `run_trivial_baselines.py` on AS baselines reproduces the stored CSV in all 344 comparable cells (0 differ, 16 excluded), and AS predictions equal the original script's page for page for all 4 methods | result | nlp, baselines, reproducibility | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.2 | On long-chunk dev, majority has the lowest macro Pk (0.3033) of the four floors, below the own-rate oracle (0.5153) and every-k at the train rate (0.6437); the oracle is worse than majority on 16 of 22 inventories (better on 3, tied on 3), so Pk/WindowDiff must not be read without F1 | result | evaluation, baselines, metrics | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.3 | With no predicted boundary, Pk and WindowDiff are both the fraction of windows holding at least one reference boundary, so they are equal by construction (0.30328 both) | method | evaluation, metrics | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.4 | Five dev inventories (1529, 1388, 7923, 8023, 8237) have a single start page, so majority gets Pk 0.0 there and macro Pk of majority is flattered; the hardest for majority are the short-segment ones (3142 Pk 0.521, mean segment 3.0 pages) | data-property | evaluation, baselines, voc | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.5 | `intra_document_swap` swaps the units but not their provenance entries, so `observed_start_labels` does not follow a moved start page (on the 60-unit demo, 12 units moved and 12 provenance entries disagree with their unit); Framing-A floors and labels are unchanged by this operator. *Revised by the review: kind was bug-found. The bug is fixed (provenance now moves with its unit, also fixing `swap_and_insertion`), and the "floors unchanged" result was an artefact of it; see F03-R.1* | bug-fixed | corruption, metrics | revised | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.6 | Under `foreign_document_insertion` (rate 0.5, 60 units) the every-k floors fall from F1 1.00 to 0.60 and majority Pk falls from 0.474 to 0.421 while it stays at F1 0 | result | corruption, baselines | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-09.7 | Running `verify_as_evaluation_shim.py` against the first version of the edited script overwrote NLP's `output/trivial_baseline_metrics.csv` (default `--out` bound at definition time, the verify script reassigns `RESULTS_CSV_PATH`); the 344 reproducible cells are unchanged, the 16 random CI cells now hold a different draw and the original is lost | open-issue | nlp, baselines, reproducibility | confirmed | [Step 09](#step-09-c8-trivial-baselines-oracle-clearly-labelled-the-corruption-harness-gets-floors) |
| F03-R.1 | With the `intra_document_swap` fix, Framing-A labels follow a moved start page (step 09's 60-unit demo: true starts at 31 and 59 instead of 30 and 54), and the floors move: every-k and the oracle fall from F1 1.0 to 0.8, and majority Pk goes from 0.4737 to 0.4386; 11 new test cases fail on the old operator and pass on the fixed one | bug-fixed | corruption, metrics, baselines | confirmed | [Review](#review) |
| F03-R.2 | Despite the overwrite (F03-09.7), the rerun is identical to the **pre-phase** file in every deterministic cell of the 6 columns printed in full at planning (k, n_eval, f1_pooled, pk_macro, window_diff_macro, and f1_macro_ci_lo for the seeded rows); the old random `f1_macro_ci_lo` of rows 1, 2, 7 and 8 survives in that print (0.06115810296212190, 0.07788260236345521, 0.10702516061832880, 0.08870662295136990) | result | nlp, baselines, reproducibility | confirmed | [Review](#review) |
| F03-R.3 | 21 of 50 NLP tests (`test_voc_text_layer.py`) fail on import: pagexml-tools' `text_helper.py` imports `fuzzy_search.similarity`, which the installed fuzzy-search lacks; this is unrelated to this phase's modules | open-issue | nlp, environment, pagexml | confirmed | [Review](#review) |
| F03-R.4 | A verify script that patches a module constant (`RESULTS_CSV_PATH`) and a script whose default was bound at definition time together wrote to a protected, untracked output; the step template now requires hashing protected outputs before and after, and explicit scratch paths | method | process, reproducibility | confirmed | [Review](#review) |
| F03-R.5 | Clipping margins per inventory: the tightest long-chunk dev run leaves 8 gaps above k (3891: k=3, 12-page run), and held-out test leaves over 600, so `WindowClippedWarning` would need much shorter chunks to fire on VOC | data-property | nlp, evaluation, windows | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`,
  `open-issue`, `method`.
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Planning session (Opus, 2026-10-14); no code changed. The prototypes ran in the scratchpad
only, with NLP's interpreter, and wrote nothing to NLP.

- **F03-P.1.** In `run_trivial_baselines.py`, the scoring calls for `majority`,
  `every_kth_corpus_rate` and `every_kth_own_rate_ORACLE` pass `seed=None` to `evaluate_all`.
  `bootstrap_ci` and `bootstrap_pooled_f1` use `np.random.RandomState(None)`. Majority's CIs
  are 0 by construction; the other two methods × two split cells give 16 random cells. With the
  user's agreement, acceptance excludes them and an A/B with a fixed seed covers them instead.
- **F03-P.2.** The cache file name is `<kind>_seed<seed>`, and the manifest check only compares
  the params a caller passes. `train_bilstm_sequence.py` stores W=1 under seed 1
  (`SPLIT_SEED_FOR_CONTEXT_WINDOW = {1: 1, 3: 0}`).
- **F03-P.3.** Reported by the planning subagent: the file has 22,202 data rows, against
  22,201 in `long_chunk_seed0.csv`, and its column order differs. It is written by
  `scripts/evaluate_long_subsequence_chunk_split.py`. Not verified further; for NLP to decide.
- **F03-P.4.** `git status` in NLP, 2026-10-14. Step 07b copies the originals to
  `output/as-phase-03-originals/` before editing.
- **F03-P.5.** For each inventory, the end-id set was computed both ways and compared (scratch
  script, 2026-10-14): 1,135 = 1,135, and 0 inventories differ. No inventory lacks a
  top-of-page start. This lets step 08 drop the VOC scan-name grammar from the split path
  (decision P7).

Also checked, with no finding: all 7 cached split CSVs rebuild byte-identically with the
current NLP code. The CRF scorefix metrics (12 rows) reproduce exactly from the decoded CSV.
The 7-case WindowDiff fixture gives identical values in the AS and NLP originals.

### Step 07: C6 evaluation suite in AS; `corruption/metrics` becomes a wrapper

**What changed.** New package `archival_structures/stream_analysis/evaluation/`
(`segmentation.py`, `aggregate.py`), a port of NLP's `startpage/evaluation.py` with bool
labels (`check_labels`), `default_k(labels, convention)`, `WindowClippedWarning`, length-mismatch
`ValueError`s, a generic `build_eval_frame(..., sequences, sep)` with a duplicate-scan-id
(context) guard, and `evaluate_all(..., k=None, k_convention="gaps")`. `corruption/metrics.py`
delegates `_score_binary` and `window_diff` (strict contract kept). Brief:
[step-07](../plan/phase-03-evaluation-for-nlp/step-07-evaluation-suite.md). NLP was not edited.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 16 ported NLP tests | pass | 16 pass |
| Corruption metrics tests, file unchanged | 17 pass | pass; `git diff` of the file empty |
| 7 shared WindowDiff cases, both wrappers | table values | 14/14 equal |
| Packaging test with `evaluation` | pass | pass |
| Equivalence with NLP (CRF scorefix, `k_convention="pages"`) | 12 equal | 12 of 12 deep-equal |
| Full AS suite | 618 + new | 666 passed (48 new), 96 s |

**Error analysis.**

- *What was read:* all 6 `(split_kind, seed)` groups of `crf_sequence_decoded_scorefix.csv` (3
  held-out, 3 long-chunk), CRF decode; every run of every inventory was checked for clipping.
  The brief asked to read 10 clipped runs; there are none.
- *Categories:*

  | Category | Count | Example |
  |---|---|---|
  | Inventories with a clipped window (long-chunk, 22 inv.) | 0 | min run length 12 pages, 35 runs; max k = 182 (pages convention) in a long run |
  | Inventories with a clipped window (held-out, 5 inv.) | 0 | min run length 642, max k = 5 |
  | Runs shorter than 2 pages | 0 | none in any group |
  | Inventories where default k differs (22) | 1 | 2448: n=2636, S=154, gaps 8, pages 9 |

- *Sensitivity (CRF decode, macro, "pages" to "gaps"):*

  | Group | Pk | WD |
  |---|---|---|
  | held-out 42, 43, 44 | unchanged (0.3085, 0.2987, 0.2801) | unchanged (0.3525, 0.3434, 0.3381) |
  | long-chunk 0 | 0.2198 to 0.2181 | 0.2503 to 0.2485 |
  | long-chunk 1 | 0.1784 to 0.1767 | 0.2080 to 0.2063 |
  | long-chunk 2 | 0.2135 to 0.2096 | 0.2458 to 0.2420 |

- *Conclusion:* clipping never alters NLP's stored Pk/WD, so the warning is a guard for future
  data (shorter chunks), not a correction. The k convention only matters for inventory 2448
  (k 9 vs 8) and moves macro Pk/WD by under 0.004, well below the CRF-vs-baseline gaps. The
  held-out splits do not contain 2448. The shim in 07b must still pass `k_convention="pages"`
  to keep stored numbers.

**Findings:** F03-07.1 to F03-07.5.

**Open issues.** none

### Step 07b: NLP `evaluation.py` becomes a shim over AS; stored numbers reproduced

**What changed.** NLP `src/startpage/evaluation.py` (514 lines) is replaced by a shim of about 100
lines over `archival_structures.stream_analysis.evaluation` ([brief](../plan/phase-03-evaluation-for-nlp/step-07b-nlp-evaluation-shim.md)).
It converts string labels to bool, extracts page ids from the tuple-shaped `full_sequences`,
renames `page_id` to `scanname`, and passes `k_convention="pages"` explicitly (D3). New:
`scripts/verify_as_evaluation_shim.py` and the originals in `output/as-phase-03-originals/`
(`evaluation.py`, `test_evaluation.py`, `run_trivial_baselines.py`). The 21 importers and
`tests/test_evaluation.py` are unchanged (`cmp` against the copy: identical). No AS code changed.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| Baseline: verify with the original module only, before editing | pass | pass (CRF 228/228 cells, baselines 344 cells, splits unchanged) |
| CRF, original and shim | 12 rows x all metric columns equal | 12 x 19 = 228 cells, 0 differ, for both |
| Trivial baselines, original and shim | 12 x 30 minus 16 cells equal | 344 compared, 16 excluded, 0 differ, for both |
| A/B, long_chunk/dev, seed 0, n_bootstrap 200 | 4 methods deep-equal | 4 of 4 (covers the 16 excluded cells) |
| Split cache | unchanged | 14 files, sha256 identical before and after, for both runs |
| NLP `tests/test_evaluation.py` through the shim | 17 passed, file unchanged | 17 passed in 5.8 s; `cmp` identical |
| Public names | all present | `grep` shows `START` and 13 `def`s; 6 names are re-exported from AS |

**Error analysis.**

- *What was read:* the label values of all 20,905 pages in `full_sequences`; the warnings of one
  CRF long-chunk seed-0 `evaluate_all` run (3,090 rows) with `WindowClippedWarning` always
  shown; the "gaps" versus "pages" macro Pk/WD for CRF long-chunk seed 0 and held-out seed 42.
  Scratch script: `ea.py` in the session scratchpad.
- *Categories:*

  | Category | Count | Example |
  |---|---|---|
  | Label values other than `start_page` / `not_start_page` | 0 | Counter: `not_start_page` 18,538, `start_page` 2,367 |
  | `WindowClippedWarning` in the CRF long-chunk seed-0 run | 0 | the brief expected some short runs; none occur (as in step 07) |
  | k convention effect, long-chunk seed 0 | Pk 0.21981 vs 0.21809; WD 0.25026 vs 0.24854 | "pages" is NLP's, "gaps" the AS default |
  | k convention effect, held-out seed 42 | none | both 0.30849 (Pk) and 0.35255 (WD) |
  | First attempt compared the stored CRF CSV with the default parser | 44 of 228 cells differ, up to 96 ulp | stored `pk_macro` 0.2198120288312173 against computed 0.21981202883121737 (2 ulp after parsing exactly) |

- *Conclusion:* the shim is numerically identical to the original, bit for bit. The only
  failure seen was in the verification itself: pandas' fast float parser is not exact, so the
  stored CSVs must be read with `float_precision="round_trip"`. The D3 override matters only for
  inventory 2448 and changes the long-chunk macro Pk/WD by about 0.002.

**Findings:** F03-07b.1 to F03-07b.5.

**Open issues.** none

### Step 08: C7 generic splits, held-out builder, public validators, `check_nested_split`

**What changed.** New `archival_structures/stream_analysis/evaluation/splits.py` (re-exported from
`evaluation`): NLP's chunk/stride/rebalance code ported verbatim (prints to `logging`, P2 edge
behaviour documented), plus `pages_from_starts`, `build_long_sequence_split`,
`build_inventory_held_out`, `core_rows`, `eval_scan_ids`, `SplitValidationError` and the public
validators `check_no_duplicate_page_ids`, `check_no_document_crosses_partitions`,
`check_context_not_scored`, `check_split_coverage`, `check_nested_split`. In NLP,
`long_subsequence_chunk_split.py` is a shim and `splits.py` private helpers delegate to AS; the
byte check is `scripts/verify_as_splits.py`. Brief:
[step-08](../plan/phase-03-evaluation-for-nlp/step-08-splits.md).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| `verify_as_splits.py` before the NLP edit | 7/7 identical, cache untouched | 7/7 identical, untouched |
| `verify_as_splits.py` after the NLP edit | 7/7 identical, cache untouched | 7/7 identical, untouched |
| New AS split tests | pass | 19 passed |
| Full AS suite | 666 + new | 685 passed (666 + 19), 75 s |
| grep for VOC constants in `splits.py` | nothing | nothing |
| NLP `load_or_build_split('long_chunk')` | shape (22201, 7), from cache | (22201, 7), no rebuild |

**Error analysis.** Rebuilt `long_chunk_seed0/1` read per inventory (core pages); chunk sizes
from a K=12 rebuild; nesting on the real split CSVs.

- *Coverage (22 inventories):* only 8023 lacks a partition (dev 23, train 132, test 0), in both
  seeds. It has 5 chunks (14, 79, 26, 13, 23 pages); striding gives train x4 + dev (the
  test slot is index 9), and the greedy swap, which only retypes a chunk that precedes a split
  change, never makes any of its chunks test. 7923 also has 5 chunks
  (15, 81, 12, 44, 23 pages) but the rebalance turns chunk 3 (44 pages) into test.
  Inventory 1529 (6 chunks: 90, 98, 6, 614, 364, 26) ends with test 614, dev 364, train 220
  pages: the two single-document chunks become test and dev.
- *Chunk sizes (K=12):* 238 chunks, 66 single-document chunks of which 49 are oversized
  (document >= total/12). Core pages per chunk: min 3, median 70, max 710. Largest five:
  2448_chunk_002 (1 doc, 710, train), 1529_chunk_003 (1 doc, 614, test), 1529_chunk_004 (1 doc,
  364, dev), 2448_chunk_007 (10 docs, 261, train), 1388_chunk_009 (1 doc, 247, test). Totals
  train/dev/test 14,670 / 3,090 / 3,145 pages.
- *Nesting (real splits, core rows):*

  | component to downstream | component-train keys | violations |
  |---|---|---|
  | long_chunk_seed0 train to held-out_seed0 train | 2,445 not in held-out train | violation |
  | long_chunk_seed0 train+dev to held-out_seed0 train | 2,957 | violation |
  | held-out_seed0 train to long_chunk_seed0 train | 5,066 in long-chunk dev/test | violation |

  As expected the splits are not nested. A component (continuation classifier, encoder) fitted
  on long_chunk train and then scored on held-out test sees 2,445 pages of held-out inventories
  during training; fitted on held-out train and scored on long_chunk dev, it has seen 5,066 of
  those pages. Either way rule 1 requires a component to be trained on the split it is
  evaluated under.
- *VOC end-id rule:* dropped. At planning time both rules gave 1,135 end pages in all 22
  inventories; the byte-identical rebuilds confirm it end to end.
- *Other:* the duplicate-scanname NOTE (1539) and the 8023 gap NOTE are now `logging` warnings
  (stderr) in NLP instead of stdout prints.

**Findings:** F03-08.1 to F03-08.5.

**Open issues.** none

### Step 09: C8 trivial baselines (oracle clearly labelled); the corruption harness gets floors

**What changed.** New `evaluation/baselines.py` (`majority`, `every_k`, `length_prior`,
`oracle_every_k_own_rate`, `train_start_rate`, `BaselineRun`, `baseline_suite`, `baseline_floors`),
`aggregate.flatten_metrics`, and `corruption.start_classification_floors`. NLP's
`run_trivial_baselines.py` now calls AS for predictions, train rate, scan ids and flattening, and
has `--out`. `verify_as_evaluation_shim.py` takes the removed helpers from the saved original
script. Brief: [step-09](../plan/phase-03-evaluation-for-nlp/step-09-trivial-baselines.md).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| NLP rerun (`--out` scratch) vs stored CSV, `round_trip` | 344 cells equal | 344 compared, 0 differ; shape (12, 30) |
| Anchor values | exact | majority f1 0.0 / Pk 0.30327993644426438; oracle f1 0.20297699594046009; HSMM s42 f1 0.12250332889480690 (all equal) |
| `verify_as_evaluation_shim.py` | exit 0 | `SUMMARY: 16/16 checks passed -> PASS` (11 old checks plus 5 for AS-vs-original predictions) |
| `verify_as_splits.py` | OK | OK |
| `output/splits/` sha256 | unchanged | unchanged |
| Stored CSV mtime | unchanged | **changed** (see F03-09.7) |
| New AS tests | pass | 11 passed |
| Full AS suite | 685 + new | 696 passed (685 + 11), 101 s |
| `grep -n "ORACLE\|oracle" baselines.py` | name and docstring warning | present |

**Error analysis.** Per-inventory Pk on long-chunk dev (22 inventories, seed 0, `k_convention="pages"`)
for majority, the own-rate oracle and every-k at the train rate, read for the five worst
inventories under majority and for the five with Pk 0.

| inventory | dev pages | starts | mean segment | Pk majority | Pk oracle | Pk every-k |
|---|---|---|---|---|---|---|
| 3142 | 121 | 40 | 3.0 | 0.521 | 0.487 | 0.453 |
| 2548 | 339 | 94 | 3.6 | 0.496 | 0.501 | 0.499 |
| 2555 | 94 | 7 | 13.4 | 0.483 | 0.494 | 0.494 |
| 3891 | 82 | 13 | 6.3 | 0.434 | 0.579 | 0.579 |
| 2775 | 110 | 17 | 6.5 | 0.433 | 0.288 | 0.452 |
| 1267 | 268 | 8 | 33.5 | 0.338 | 0.662 | 0.662 |
| 2448 | 277 | 7 | 39.6 | 0.198 | 0.802 | 0.802 |
| 1529, 1388, 7923, 8023, 8237 | 364 / 116 / 28 / 23 / 48 | 1 each | = pages | 0.000 | 1.0 / 1.0 / 0.0 / 0.0 / 1.0 | 1.0 (all five) |

- *What makes "no boundaries" hard.* Pk's window is half the mean segment length, so majority errs
  on roughly the fraction of windows that contain a boundary, about 0.5 when segments are regular
  (3142, 2548, 8834 0.420 with mean segment 2.3). 2555 (mean segment 13.4, Pk 0.483) shows that the
  mean is not the whole story (this is a pattern in the numbers, not checked page by page): I did not look at where its 7 starts fall, whereas 2448 (mean 39.6, Pk 0.198) has
  a few long documents and large stretches with no boundary.
- *What the oracle buys.* The oracle's knowledge of the rate helps on the short-segment inventories
  only a little (3142 -0.034, 2775 -0.145, 8276 -0.174) and hurts on 16 of 22 (better on 3, tied on 3; macro 0.515 against
  0.303). On inventories with few starts it places a boundary every 30-40 pages, which the
  reference mostly lacks (1267 0.338 to 0.662, 2448 0.198 to 0.802), and the five single-start
  inventories get 1.0 or 0.0 depending on whether `round(1/rate)` falls inside the dev part
  (the oracle's rate is over the whole inventory). F1 is the metric the oracle helps on: pooled F1
  0.203 against 0.142 for the corpus rate and 0 for majority.
- *Majority Pk equals WindowDiff.* With no predicted boundary, a window disagrees exactly when it
  holds at least one reference boundary, for both metrics, so they coincide (0.30328).
- *Floors on a corrupted stream.* Synthetic stream: 10 documents of 6 units (`s0`..`s59`, `d0`..`d9`),
  `intra_document_swap` rate 0.5, `random.Random(0)`: 12 units moved. The floors table is identical
  with and without the corruption (majority F1 0.000, Pk 0.4737; every-k and oracle F1 1.000, Pk 0;
  length prior seed 42/43/44 F1 0.118/0.182/0.381). The reason is not that the labels follow the
  canonical start: `intra_document_swap` swaps `stream[i]` and `stream[j]` but only tags
  `provenance[i]` and `provenance[j]`, so each provenance entry stays at its position and
  `observed_start_labels` returns the canonical start positions (0, 6, ..., 54) although doc 5's
  start unit `s30` sits at position 31 and doc 9's `s54` at position 59 (F03-09.5). I did not change
  the operator. As a check that the floors can move, the same stream under
  `foreign_document_insertion` (rate 0.5, `Random(0)`; provenance consistent with units there) gives
  start positions 0, 6, 11, 18, 20, 30, 33, 42, 48, 49: every-k and the oracle drop to F1 0.600
  (Pk 0.298, WD 0.351), majority stays at F1 0 (Pk 0.421), length prior seed 42/43/44 F1
  0.118/0.182/0.286.

**Findings:** F03-09.1 to F03-09.7.

**Open issues.** F03-09.5 (operator bug, not fixed here) and F03-09.7 (stored CSV overwritten).

## Review

Phase review (Opus, 2026-10-14). The code review is in
[`REVIEW.md`](../plan/phase-03-evaluation-for-nlp/REVIEW.md).

**Re-run.**
- AS suite: 717 passed. NLP `test_evaluation` + `test_semi_markov`: 21 passed.
- `verify_as_evaluation_shim.py`: 16/16. `verify_as_splits.py`: 7/7 identical, cache untouched.
- The NLP baseline rerun went to scratch, and the stored CSV hash was unchanged across all
  reviewer runs.
- Every logged acceptance number reproduced.

**Status changes.** 30 findings are confirmed. Two are revised:
- F03-P.4: the user committed `tests/`, `scripts/` and `src/`.
- F03-09.5: the bug is now fixed, and its "floors unchanged" result was an artefact.

**Reviewer's error analysis.**
- *Pre-phase CSV.* The 12 × 6 table printed at planning time (before any step ran) was diffed
  as text against the rerun, formatted the same way. The only differing lines are three
  `f1_macro_ci_lo` cells of seed-less rows (F03-R.2). A direct float comparison first showed 15
  "differences" in the 17th significant digit; they are an artefact of pandas'
  `display.precision`, not of the data.
- *Corruption harness after the fix.* Step 09's demo was re-run: the true starts follow `s30`
  and `s54` to positions 31 and 59, and the floors move (F03-R.1). Under the old operator,
  11 new test cases fail.
- *Oracle against majority.* Recomputed per inventory with AS code: 16/3/3. Read for 2448
  (0.198 → 0.802) and 1267 (0.338 → 0.662). The step's explanation (a fixed period on
  few-start inventories) holds.
- *Clipping.* Checked per inventory and per run, not pooled: 0 clipped runs. The margins are in
  F03-R.5.
- *Split facts* (F03-08.2–.5): 8023, 1529, the chunk-size distribution and the three nesting
  counts were re-measured from the cached CSVs. All identical.
- *Stale subsequence CSV* (F03-P.3): verified (22,202 vs 22,201 rows; `1539_1048` twice).

**Leakage.** `train_start_rate` uses train core rows only (tested). The oracle is flagged by
name, by field and in its docstring. `check_nested_split` exists and is exercised on real
splits. No feature here is derived from distance to a ground-truth start.

**Candidates for `docs/findings.md`** (not edited): F03-09.2 (floors are not monotone in rate
knowledge; read Pk with F1), F03-09.3 (majority Pk = WD by construction), F03-08.5 (check
nesting before reusing a component across splits), F03-07b.2 (read stored floats with
`round_trip`), F03-R.4 (protect stored outputs in reproduction steps).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F03-09.7 | Restore NLP `output/trivial_baseline_metrics.csv` from the user's backup if the old random CI draw matters | user |
| F03-P.1 | Seed the bootstrap of the every-k/oracle rows in `run_trivial_baselines.py` (user chose to leave it this phase) | NLP |
| F03-P.2 | Key NLP's split cache by its parameters (W=1 is stored as "seed 1") | NLP |
| F03-P.3 | Retire or regenerate `output/long_sequence_subsequences.csv`, and update NLP `CLAUDE.md` trap 2, which points to it | NLP |
| F03-R.1 | Regenerate any corrupted-stream YAMLs written with `intra_document_swap` / `swap_and_insertion` before the fix | AS (simulation harness users) |
| F03-R.3 | Fix the `fuzzy_search` version under pagexml-tools in NLP's environment | NLP / phase 05 (text layer) |
| P4 | Trim NLP `tests/test_evaluation.py` to the kNN test, now that AS holds the bool ports | NLP, optional |
| P2 | Opt-in corrected split strategy (rebalance overshoot, F03-08.3/.4) | AS, later phase, optional |
| — | NLP: three `output/embeddings/*.npz` show as modified because the LFS filter was added after they were committed as plain binaries (the files are intact) | NLP |

## Reproduce

- Review: `.venv/bin/python -m pytest -q -p no:cacheprovider` (717 passed) and `tests/test_stream_analysis_corruption.py` for the operator fix; NLP `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_evaluation.py tests/test_semi_markov.py`, `scripts/verify_as_evaluation_shim.py --scratch DIR`, `scripts/verify_as_splits.py --out DIR`, `scripts/run_trivial_baselines.py --out DIR/tb.csv`. The pre-phase comparison (F03-R.2), the fixed-operator demo (F03-R.1) and the per-inventory clipping check (F03-R.5) used scratchpad scripts.
- Step 09: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_evaluation_baselines.py`; NLP `venv/bin/python scripts/run_trivial_baselines.py --out DIR/tb.csv`, then `venv/bin/python scripts/verify_as_evaluation_shim.py --scratch DIR`. Per-inventory Pk and the corruption demos: scratchpad scripts (not kept).
- Step 07: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_evaluation_segmentation.py tests/test_stream_analysis_evaluation_aggregate.py tests/test_stream_analysis_corruption_metrics.py tests/test_packaging_core_imports.py`.
  Equivalence, clipping, k and sensitivity numbers: a scratchpad script (not kept in the repo) run with
  NLP's `venv/bin/python`, loading NLP's original `evaluation.py` by file path and AS's
  `evaluation` from the live tree, over `output/crf_sequence_decoded_scorefix.csv`.
