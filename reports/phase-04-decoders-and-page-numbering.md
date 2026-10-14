# Phase 04 report: Decoders and page numbering

> **Status:** final (phase review, 2026-10-14).
> **Phase plan:** [`plan/phase-04-decoders-and-page-numbering/PLAN.md`](../plan/phase-04-decoders-and-page-numbering/PLAN.md).
> **Code state:** AS unreleased after v0.7.0; NLP and LotA 2026-10-14 (after steps 10b, 11 and 12b, uncommitted).
> **Last updated:** 2026-10-14, by the phase review (Opus).

## Summary

AS gained three new modules:
- the sequence decoders (`stream_analysis.decoding`: semi-Markov, linear-chain CRF with its
  training loop, BiLSTM);
- a Nationaal Archief scan-id parser (`model.scan_id`);
- the folio → scan alignment core with a GLOBALISE index loader (`analysis.page_numbering`,
  `datasets.globalise_index`).

NLP's decoder and alignment modules and the CRF helpers in three scripts are now shims, and
LotA's adapters parse scan ids through AS. Every stored NLP output reproduces byte for byte
through the shims (F04-10b.1, F04-12b.1), as it did from the original code before the phase
(F04-P.1, F04-P.2).

What a reader should know:
- On the held-out split the semi-Markov decoder halves F1. The cause is its input: emission-only
  start log-odds are negative at most true starts there, while the CRF recovers starts through
  transitions (F04-P.3, F04-10b.3, F04-R.1).
- Page-number anchors read from the shared text layer's region table equal the mined ones
  (466/467 inventories, one extra pair), so anchors are available for all 6,914 inventories
  (F04-12.2, F04-12.3).
- The scan-id grammar holds without exception on 21,623 ground-truth rows and 162,468 sampled
  text-layer ids (F04-11.1, F04-11.2).

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`. Planning findings use `F04-P.<n>`, the
review's own findings `F04-R.<n>`. IDs never change and are never reused; a wrong finding is
marked `withdrawn`.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F04-P.1 | Before any change, the current NLP code reproduces its stored decoder outputs: `train_semi_markov_duration.py` 4/4 CSVs byte-identical (75.6 s, 8 threads) and multitask `start_only` 6/6 metric rows equal (79.4 s) | result | decoders, crf, nlp | confirmed | [Planning](#planning) |
| F04-P.2 | Before any change, `build_globalise_alignment.py` reproduces 9/9 stored CSVs byte-identically; its manifest step needs the NLP repo as cwd (`git rev-parse`) | result | page-numbering, globalise, nlp | confirmed | [Planning](#planning) |
| F04-P.3 | On inventory-held-out test the semi-Markov prior halves pooled F1 relative to the plain CRF (0.168 vs 0.352, seed means), while on long-chunk dev the three decodes are within 0.003 (0.780–0.783) | result | decoders, semi-markov, voc, nlp | confirmed | [Planning](#planning) |
| F04-P.4 | `scipy.special.ndtr` equals `scipy.stats.norm.cdf` bit for bit; an `erfc` replacement shifts the duration log-pmf by up to 6e-13 (18 of 500 values), so scipy stays and becomes a declared core dependency (sklearn already requires it) | method | decoders, packaging | confirmed | [Planning](#planning) |
| F04-P.5 | `InventoryNumber` sorts all 6,914 VOC text-layer inventories exactly like NLP's regex sort keys; 54 inventories carry a suffix (`A`–`E`, `I`, `II`) | data-property | scan-id, voc | confirmed | [Planning](#planning) |
| F04-P.6 | NLP's two CRF training loops (`train_crf_sequence`, `train_multitask_crf`) are equivalent for the 3-state model: the stored CRF rows agree to full precision across 3 CSVs for all 6 seeds | result | crf, nlp | confirmed | [Planning](#planning) |
| F04-P.7 | The alignment build's hard assertion leaves 0.001 of headroom on `exact` (stored 0.6190 against 0.615 ± 0.005) | method | page-numbering, nlp | confirmed | [Planning](#planning) |
| F04-10.1 | AS ports of NLP's decoders are identical to the originals: `decode_semi_markov` 200/200, `log_duration_pmf` 2/2, CRF `state_dict` 10/10 and `viterbi`/`nll`/`_partition` 200/200 each, `train_crf` 3/3 (state dict and best F1) | result | decoders, crf, semi-markov, nlp | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10.2 | `decode_semi_markov` now returns `list[bool]` instead of 0/1 ints; NLP's shim must convert back (`int(x)`) to stay byte-identical | behaviour-change | decoders, semi-markov, nlp | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10.3 | Decoder inputs are validated: `decode_semi_markov` raises `ValueError` for `max_duration < 1` or a `log_pmf` shorter than `max_duration`, and `log_duration_pmf` raises `ValueError` instead of `assert` when there is no mass | behaviour-change | decoders, semi-markov | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10.4 | `scipy>=1.10` is now a declared core dependency (scikit-learn already requires it); `semi_markov` uses `scipy.special.ndtr`, equal to `norm.cdf` on both priors (`np.array_equal`, 2/2) | method | decoders, packaging | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10.5 | With a flat duration prior, ties resolve to the longest segment ending at each position, so forced starts land late: T=10, `max_duration`=3 gives segment lengths [1, 3, 3, 3] | method | decoders, semi-markov | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10.6 | The synthetic task of the brief is too easy to separate decoders: best dev F1 is 1.0 for all 3 seeds, so criterion 3's `train_crf` check proves identical optimisation paths (weights equal) but says little about F1 | method | crf | confirmed | [Step 10](#step-10-sequence-decoders-in-as) |
| F04-10b.1 | NLP's decoders as shims over AS reproduce its stored numbers on real data: 4/4 semi-Markov CSVs byte-identical (decoded CSV 20,112 rows), `start_only` 6 x 23 rows equal, A/B against the original code 5/5 byte-identical, BiLSTM 5/5 seeds equal, 24 protected files unchanged (325 s, 8 threads) | result | decoders, crf, semi-markov, nlp | confirmed | [Step 10b](#step-10b-nlp-decoder-shims) |
| F04-10b.2 | On long-chunk dev the semi-Markov decode equals the plain CRF (F1 0.7829 against 0.7814, Pk 0.2054 against 0.2039; seed sd 0.006 to 0.017), so C9's "decoder matters little" still holds there; 114 of 9,270 pages differ (CRF-only starts 29/70 correct, semi-Markov-only 21/44) | negative-result | decoders, semi-markov, voc, nlp | confirmed | [Step 10b](#step-10b-nlp-decoder-shims) |
| F04-10b.3 | The held-out semi-Markov collapse (F1 0.168 against 0.352) is a start-rate collapse: 1.0 to 4.5% predicted starts against 11.7% true (CRF 7.4 to 7.9%, no-transition 7.7 to 16.1%), with predicted segments up to the 250-page cap (inventory 8834, seed 42: 3 starts in 642 pages, 100 gold) | result | decoders, semi-markov, voc, nlp | confirmed | [Step 10b](#step-10b-nlp-decoder-shims) |
| F04-10b.4 | The fitted held-out prior (median 3) matches the gold segment medians (2 to 3), so the prior's shape is not what is off; the DP's summed negated emission scores against a few-nat prior term is the likely cause, not isolated by any experiment. *Revised by the review: the cause is isolated in F04-R.1 (start log-odds from emissions alone are negative at most held-out gold starts). The mechanism stated here is also off: a start at page i only flips that page's term (−s[i] to +s[i]), so a start costs −2·s[i] plus one extra prior term, not a run of negated scores* | result | decoders, semi-markov, nlp | revised | [Step 10b](#step-10b-nlp-decoder-shims) |
| F04-10b.5 | NLP's `startpage` package is not installed in its venv, so the brief's pytest command fails at collection (`ModuleNotFoundError: startpage`) until `PYTHONPATH=src` is set; with it 21 passed before and after the edit. *Revised by the review: the failure depends on collection order. `tests/test_evaluation.py` puts `src/` on `sys.path` itself and `tests/test_semi_markov.py` (unchanged since before the phase) doesn't, so `pytest tests/test_evaluation.py tests/test_semi_markov.py` gives 21 passed without `PYTHONPATH`; the brief listed them the other way round (F04-R.3)* | method | nlp, packaging | revised | [Step 10b](#step-10b-nlp-decoder-shims) |
| F04-11.1 | `ScanId.parse(prefix="NL-HaNA_1.04.02")` parses 21,623/21,623 rows (20,905 distinct names) of the 22 GT segmentation CSVs, each with the inventory of its file; 0 failures | result | scan-id, voc, globalise | confirmed | [Step 11](#step-11-scan-id-helpers) |
| F04-11.2 | All 162,468 distinct scan ids of the 54 suffixed plus 200 sampled numeric text-layer inventories (254 files) parse with the inventory equal to the file name; `scan_number` equals NLP's `rsplit` rule for all 162,468; 0 non-conforming ids | data-property | scan-id, voc, nlp | confirmed | [Step 11](#step-11-scan-id-helpers) |
| F04-11.3 | Suffixed inventories' scans carry the suffixed number (54/54 first and last ids checked); no sign of the unsuffixed-number risk. Scans often start at 0002, 0003 or 0007 (e.g. `1604B` at 0007, `2352A`-`E` at 0003) | data-property | scan-id, voc | confirmed | [Step 11](#step-11-scan-id-helpers) |
| F04-11.4 | LotA adapters keep their behaviour through the delegation: an id with `.jpg` still gives `None` (checked by the adapter filter, since `ScanId` accepts extensions); LotA 11/11 adapter tests and 316 suite pass unchanged | method | scan-id, lota | confirmed | [Step 11](#step-11-scan-id-helpers) |
| F04-12.1 | AS `align_all` equals NLP's `align_all` (minus `n_sections`) over the whole 227,526-document GLOBALISE index for 6/6 configs (baseline, mode, runs, extrap, dup_first, chosen); `load_globalise_index` equals `load_index` (all and the 22 annotated inventories); `anchors_array` equals `load_anchors` for 467/467 inventories | result | page-numbering, globalise, voc, nlp | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12.2 | Anchors from the text-layer region table (last non-empty `page-number` region per scan, digits only) agree with the mined CSV: 466/467 inventories identical, 64,505 pairs in both, 0 only in mined, 1 only in regions (10209, scan 0969, which is not in NLP's scan spine). Inventory 2555: 374/374 identical | data-property | page-numbering, text-layer, voc | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12.3 | Region anchors reproduce the mined-anchor accuracy exactly on the 22 annotated inventories (arrays identical in 22/22): baseline n 1496, exact .6190, ±1 .8743, ±2 .9298; chosen (mode, extrap, dup_first, with the verso alternative) exact .7794, ±1 .9104, ±2 .9479. No drop | result | page-numbering, voc, text-layer | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12.4 | Document order is as good as reading order for this rule: 3,946 of 97,014 scans with a non-empty `page-number` region (4.1%) have several such regions, yet the last-wins rule in document order matches the mined (reading-order) value in all of them. And 33.5% of those scans (32,508) end in a non-digit text and yield no anchor, so only 64,506 anchors exist | data-property | page-numbering, text-layer, voc | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12.5 | Verbatim quirks are kept and documented: strict LIS with later duplicates winning, banker's rounding (`int(round(12.5)) == 12`), `max_scan or s` (0/None means no upper clamp), the `+extrapolated` suffix when only end clamping applied; hard-coded numbers (`edge=6`, `run_window=3`, bands, slope thresholds) are keyword defaults | method | page-numbering | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12.6 | With the chosen config 42 of 1,281 `high`-confidence documents (3.3%) are off by more than 2 scans (all `chain` method; 25 foliation, 17 pagination); inside `high`, 96.7% are within ±2. Causes seen in 5 read cases: misread digits in the anchors (`3393`, `1005`), non-linear numbering near the document, noisy anchors in the chain | result | page-numbering, voc, error-analysis | confirmed | [Step 12](#step-12-page-numbering-core) |
| F04-12b.1 | NLP `globalise_alignment` as a shim over AS reproduces the alignment outputs: 9/9 CSVs byte-identical to the stored ones, manifest equal without `git_head` (baseline_all22 n 1496, exact 0.6189839572192514), 10/10 identical to a run of the original module, 13 protected files unchanged; the stricter `scan_no` rejects none of 64,505 anchor, 269,383 spine and 21,623 annotation rows | result | page-numbering, globalise, voc, nlp | confirmed | [Step 12b](#step-12b-nlp-alignment-shim) |
| F04-12b.2 | The build script asserts baseline exact within 0.005 of .615 while the stored value is .6190, so only 0.0040 headroom and a gain of about +0.001 trips it. Suggestion for the user, not a change: compare against the manifest value instead. *Revised by the review: the stored value is 0.0040 above the target, so the headroom before the 0.005 tolerance trips is 0.0010 (as F04-P.7 says); the suggestion stands* | method | page-numbering, voc | revised | [Step 12b](#step-12b-nlp-alignment-shim) |
| F04-12b.3 | In the stored `document_alignment.csv` (chosen config, `extrap` on) 1,769 of 14,827 rows are `chain+extrapolated`, all `low`; 1,749 are real linear extrapolation (dist 2 to 45 scans in the 10 sampled), 20 are clamp-only on inventories with fewer than 3 chain anchors (`numbering_mode` unknown, dist 0) | data-property | page-numbering, globalise, voc | confirmed | [Step 12b](#step-12b-nlp-alignment-shim) |
| F04-R.1 | The held-out semi-Markov collapse comes from its input. The emission-only start log-odds (`start_score_from_emissions`) are negative at most held-out gold starts: median −0.65, 22% > 0 (long-chunk dev: +4.63, 84%). The same holds on the held-out *training* inventories (−0.64, 23%), so it isn't a generalisation gap. The CRF still finds starts through its transitions (Viterbi 267 starts against 424 gold on seed-42 test), which the emission score drops (semi-Markov 36) | result | decoders, semi-markov, crf, voc, nlp | confirmed | [Review](#review) |
| F04-R.2 | LotA's whole `archival_language/ead/` directory, including the `adapters.py` that step 11 edited, is untracked in LotA's git, so the edit has no diff and the original survives only as the excerpt quoted in brief 11 | open-issue | lota, scan-id, process | confirmed | [Review](#review) |
| F04-R.3 | NLP's `test_semi_markov.py` imports `startpage` without putting `src/` on `sys.path`, unlike `test_evaluation.py`, so the pair passes only in that order or with `PYTHONPATH=src`; this predates the phase | open-issue | nlp, environment | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`,
  `open-issue`, `method` (see `plan/templates/REPORT.md`).
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Planning session (Opus, 2026-10-14); no code changed. The prototypes ran in the scratchpad
with the siblings' own interpreters and wrote nothing to the siblings. Hashes of the
protected NLP outputs were taken before and checked after.

- **F04-P.1.** Reruns of the original code with every output redirected to the scratchpad.
  The semi-Markov script: 4/4 CSVs byte-identical, 20,112 decoded rows. `run_config("start_only")`:
  6 rows × 23 columns `.equals()` the stored rows (`float_precision="round_trip"`). So steps
  10/10b can demand exact equality.
- **F04-P.2.** The 9 CSVs of `output/globalise_alignment/` (14,827 `document_alignment`
  rows) are byte-identical. The run died at L230 (`git rev-parse HEAD` outside the repo)
  after writing them. Step 12b runs with `cwd=<NLP>`.
- **F04-P.3.** Seed means of `f1_pooled` / `pk_macro` from the rerun:

  | Split kind | crf | no_transition | semi_markov |
  |---|---|---|---|
  | long_chunk (dev) | 0.7814 / 0.2039 | 0.7801 / 0.2071 | 0.7829 / 0.2054 |
  | inventory_held_out (test) | 0.3523 / 0.2958 | 0.3694 / 0.3217 | 0.1681 / 0.3472 |

  Step 10b reads held-out inventories to see whether the emission scores or the prior cause
  this. No tuning.
- **F04-P.4.** Checked on 200,001 points in [−40, 40] and on NLP's two fitted priors
  (`max_duration` 250).
- **F04-P.5.** Checked on the file names of `voc_text_layer_v1/regions/`; 0 names fail
  `InventoryNumber`.
- **F04-P.6.** From the Explore subagent's comparison of `crf_sequence_metrics_scorefix.csv`,
  `multitask_crf_metrics.csv` (`start_end`) and `semi_markov_duration_metrics_scorefix.csv`
  (`crf` rows); e.g. long-chunk seed 0 `f1_pooled` 0.7858017135862914 in all three. The code
  differs only in an unused loss accumulator and an unused list.
- **F04-P.7.** `manifest.json` `baseline_all22`: n 1496, exact 0.6189839572192514, ±1
  0.8743315508021391, ±2 0.9298. The script asserts `|x − target| < .005`.

Left out of scope by the user's C13 decision (2026-10-14), for the review to carry into
*Follow-ups*:
- LotA's ad hoc scan-id sites: `cli/document_units.py` L572/574,
  `cli/page_split_cases.py` L71, `cli/page_split_image_sample.py` L50–51,
  `corpus/tsv_source.parse_doc_id`, `corpus/derive_voc._normalise_scan_id`;
- AS `analysis/sequence_patterns._scan_sequence_number`;
- NLP `voc_text_layer.inventory_sort_key` (phase 05), `separator_sheets._scan_no` (phase 07),
  `layout_features._inventory_number_sort_key`.

### Step 10: sequence decoders in AS

**What changed.** New package `archival_structures.stream_analysis.decoding`
(`semi_markov.py`, `crf.py`, `bilstm.py`, lazy `__init__.py`), ported from NLP's decoder modules
and generic training loop; `scipy>=1.10` added to the core dependencies; two names added to the
packaging test. Brief: [`step-10-decoders.md`](../plan/phase-04-decoders-and-page-numbering/step-10-decoders.md).
Changes against the originals: bool outputs, input validation, `ndtr`, state ids instead of
NLP's constants and strings (F04-10.2–4). NLP is untouched.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. New tests pass, 4 ported tests unchanged | all | 122 new tests collected, 126 passed with the packaging module |
| 2. pmf equals the `norm.cdf` reference | `np.array_equal` | 2/2 priors, `max_duration` 250 |
| 3a. `decode_semi_markov` vs NLP | 200/200 | 200/200 identical (after `bool()`) |
| 3b. `log_duration_pmf` | 2/2 | 2/2 `np.array_equal` |
| 3c. `LinearChainCRF` init and ops | all | `state_dict` 10/10 (seeds 0–4 x `n_states` 2, 3); `viterbi` 200/200, `nll` 200/200, `_partition` 200/200 on random sequences |
| 3d. `train_crf` vs NLP | 3/3 | 3/3 identical `state_dict` and best F1 (1.0 each) |
| 4. Import with torch blocked | succeeds | packaging tests pass with the two new names |
| 5. Full suite | 717 + new | 867 passed (717 + 122 new + 28 from step 11 running in parallel) |

**Error analysis.**

- *What was read:* the 4 original semi-Markov tests next to the ports; one forced-start decode
  per prior/score setting (T=10, `max_duration`=3); the equivalence script
  (scratchpad `step10/equiv.py`, run with NLP's `venv`) had 0 mismatches, so there was no failing
  case to classify.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Assertion changed in a ported test | 1 kind | `== [1, 0, 0, 0, 1, 0, 0, 0]` became `== [True, False, False, False, True, False, False, False]` (bool output) |
  | Brief test spec that did not hold | 1 | `test_zero_mass_raises` with (mu=20, `max_duration`=5): sigma is not given; with sigma 0.5 the cdf difference (about 1e-300) is still positive, so nothing raises. Used sigma 0.1 (underflows to 0) |
  | Forced starts, `max_duration`=3, T=10, NLP prior 1 | 3 settings | scores -50: `S.S.S..S..` lengths [2, 2, 3, 3]; scores 0: `S.S..S.S..` [2, 3, 2, 3]; flat prior: `SS..S..S..` [1, 3, 3, 3] |

- *Forced starts.* With strongly negative scores every non-start earns +50 and every start
  costs 50, so the minimum number of starts (4 = ceil(10/3)) wins. Which segment lengths
  appear then is decided by the prior: this prior is nearly flat at d=1 and d=2 (-1.64, -1.66)
  and lower at d=3 (-1.93), hence [2, 2, 3, 3]. With an exactly flat pmf all candidate
  segmentations tie. `np.argmax` takes the first maximum and `s_range` ascends, so at each end
  position t the earliest start `s`, that is the longest allowed segment, wins; the backtrack
  therefore gives the long segments (3) at the end and the leftover short segment (1) at the
  front: [1, 3, 3, 3].
- *Conclusion:* the port is exact on every tested path. The only behavioural differences are
  the intended ones. The synthetic training task of the brief is separable (best F1 1.0 for all
  seeds), so matching F1 is weak evidence; the identical `state_dict`s (every parameter after
  up to 40 epochs of Adam) are the strong evidence. The real-data check is step 10b.

**Findings:** F04-10.1, F04-10.2, F04-10.3, F04-10.4, F04-10.5, F04-10.6.

**Open issues.** none

### Step 10b: NLP decoder shims

**What changed.** In NLP, `src/startpage/{semi_markov,crf_sequence,bilstm_sequence}.py` are
re-exports over `archival_structures.stream_analysis.decoding` (`decode_semi_markov` wraps AS
to give 0/1 ints; `START, INSIDE, END` and `STATE_NAMES` stay in NLP). `scripts/train_crf_sequence.py`
(`dev_f1`, `train_crf`), `scripts/train_multitask_crf.py` (`collapsed_f1`, `dev_f1`, `train_crf`,
`decode_sequences`) and `scripts/train_semi_markov_duration.py` (`start_score_from_emissions`)
keep their names and signatures and delegate to AS; neither CRF script has an optimiser loop
any more. New: `scripts/verify_as_decoders.py` and the copies in
`output/as-phase-04-originals/`. AS code is unchanged. Brief:
[`step-10b-nlp-decoder-shims.md`](../plan/phase-04-decoders-and-page-numbering/step-10b-nlp-decoder-shims.md).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1a. Stored semi-Markov CSVs vs `output/` | 4/4 byte-identical | 4/4 (current), 4/4 (originals); decoded CSV 20,112 rows |
| 1b. `start_only` rows vs stored | 6 rows x 23 cols equal | 6 x 23 equal (both modes) |
| 1c. A/B current vs originals | 5/5 byte-identical | 5/5 |
| 1d. `BiLSTMTagger(6)` AS vs original | 5/5 seeds | 5/5 (`state_dict` and forward `torch.equal`) |
| 1e. Protected outputs | unchanged | 24 files, sha256 and mtime unchanged after both modes |
| Verify script runtime | | exit 0, 325 s for both modes, 8 threads (about 2.7 min per mode) |
| 2. NLP tests | 21 passed | 21 passed (`PYTHONPATH=src`, see F04-10b.5); AS suite 890 passed |
| 3. grep | re-exports present; no `optim.Adam` in the two scripts | 3 re-export lines; 0 `optim.Adam` hits |

**Error analysis.** Scratch files: scratchpad `step10b/` (decoded CSV read with pandas, seeds
as stored; nothing tuned).

*Semi-Markov against the plain CRF* (`f1_pooled` / `pk_macro`, seed means from the 18 rows, with
the floors from `output/trivial_baseline_metrics.csv`):

| Split (eval) | CRF | no transition | semi-Markov | majority | every-k (corpus rate) | length prior (3-seed mean) |
|---|---|---|---|---|---|---|
| long-chunk (dev) | 0.7814 / 0.2039 | 0.7801 / 0.2071 | 0.7829 / 0.2054 | 0.0000 / 0.3033 | 0.1421 / 0.6437 | 0.1195 / 0.5935 |
| held-out (test) | 0.3523 / 0.2958 | 0.3694 / 0.3217 | 0.1681 / 0.3472 | 0.0000 / 0.3849 | 0.1182 / 0.4879 | 0.1198 / 0.4757 |

On long-chunk dev the three decodes differ by 0.003 F1 (seed sd 0.006 to 0.017) and 0.003 Pk
(sd 0.022); per-seed bootstrap CIs over inventories are about 0.70 to 0.85, a width of 0.12 to 0.15. C9's
statement "the choice of decoder matters little" therefore still holds on the primary split.
On held-out data it does not: semi-Markov F1 is 0.168 against 0.352 (per seed 0.070, 0.294,
0.141 against 0.342, 0.363, 0.352) and its Pk (0.347) is above the CRF's (0.296), though still below
the majority floor (0.385). All decoders beat the floors on F1 by a wide margin on long-chunk dev;
on held-out only the CRF and no-transition do so clearly (semi-Markov F1 0.168 is
only 0.05 above the 0.118 to 0.120 floors).

*Where the CRF and the semi-Markov decode disagree* (long-chunk dev, seed 0; inventories
picked by `random.Random(10).sample` of the 22: 8276, 1267, 3891).

| Inventory | Pages | Gold starts | CRF starts | SM starts | Disagreeing pages |
|---|---|---|---|---|---|
| 8276 | 26 | 5 | 5 | 5 | 0 |
| 1267 | 268 | 8 | 7 | 7 | 0 |
| 3891 | 82 | 13 | 19 | 19 | 2 |

The two disagreements (3891): page `0333` gold not-start, CRF start, semi-Markov not-start (gold
segment of 4 pages: the prior merged correctly); page `0579` gold start, CRF not-start,
semi-Markov start (gold segment 4: the prior split correctly). Pooled over all 3 seeds of long-chunk dev there are 114 disagreeing
pages of 9,270 (1.2%): the CRF alone starts 70 (29 correct, 41%), semi-Markov alone starts 44
(21 correct, 48%). So the prior mostly merges what the CRF splits (61% of the disagreements) but
is no more accurate in either direction; the net is a wash, as the F1 numbers say. Held-out
data is different: 596 disagreements over 3 seeds, 575 of them CRF-only starts (236 correct,
41%) against 21 semi-Markov-only starts (2 correct).

*The held-out collapse* (3 seeds, 5 test inventories, 3,614 test pages per seed; per-inventory rates in
`heldout_per_inventory.csv` in the scratchpad). Predicted start rate against the true rate of
0.117: CRF 0.074 to 0.079, no-transition 0.077 to 0.161, semi-Markov 0.010, 0.045, 0.024 (seed
42, 43, 44). Worst inventory, seed 42: 8834 (642 pages, 100 gold starts, 3 predicted,
F1 0.000). Gold segments there: median 2, mean 6.3, max 88, 16% of length 1; the CRF predicts 25
starts (median segment 14); semi-Markov predicts 3 starts with segments of 246, 248 and 148 pages,
that is it hits the cap `MAX_DURATION` = 250 (the DP decodes a whole inventory as one
sequence). Median inventory by semi-Markov F1, seed 42: 1547 (688 pages, 71 gold starts, F1
0.098): gold median segment 3, mean 9.5; CRF 48 starts, median 8; semi-Markov 11 starts,
median 12 but mean 62.6 (max 236). The fitted held-out prior (mu 1.2256, sigma 0.9915, median 3,
mean 7.2) matches the gold medians (2 to 3), so the prior's shape is not the miscalibrated part.
The no-transition decode, which uses the same emission scores without any prior or
transitions, predicts about the right number of starts (0.077 to 0.161 against 0.117), but the
semi-Markov DP sums the negated score over every non-start page, so a start must beat
a long run of mildly negative scores plus a prior term of a few nats. This points at the
emission scores (held-out uses kNN scores, 342 pages without cached score, logit 0, are
recorded) and how the DP scales them, not at the duration prior. I did not isolate it: no score
was recomputed, nothing was tuned.

*Conclusion.* The shims are exact (byte-level) on real data. The semi-Markov decoder is
equal to the CRF on the primary split and much worse on held-out; the cause is most likely the
emission-score scale in the DP, a hypothesis that needs a score-level experiment.

**Findings:** F04-10b.1, F04-10b.2, F04-10b.3, F04-10b.4, F04-10b.5.

**Open issues.** The held-out semi-Markov collapse is explained only by reading predicted
rates and segment lengths; no experiment separates emission scale from the prior.

### Step 11: scan-id helpers

**What changed.** New `archival_structures/model/scan_id.py` (`ScanId`, `InvalidScanIdError`,
`scan_number`, `parse_doc_id`, `NA_INSTITUTION`, `DEFAULT_DIGITS`) and `tests/test_model_scan_id.py`
(33 tests). LotA `archival_language/ead/adapters.py`: `_BaseAdapter.scan_id` / `parse_scan_id`
delegate to `ScanId`; the unused-looking `InvalidInventoryNumberError` import stays (five other
uses). Other LotA call sites are not switched (follow-ups below). Brief:
[`step-11-scan-id.md`](../plan/phase-04-decoders-and-page-numbering/step-11-scan-id.md).

**Results.**

| Criterion | Expected | Result |
|---|---|---|
| 1. New AS tests; full AS suite | green | 33 new; targeted modules 44 passed; full suite 867 passed (includes step 10's tests) |
| 2. LotA `test_adapters.py` / suite | 11 / 316 | 11 passed (file unchanged); 316 passed with `--ignore=tests/test_derive_voc.py` (that file can't be collected here, F03-R.3, so its `parse_scan_id` monkeypatching is untested) |
| 3. GT CSVs | 21,623 rows, 20,905 distinct, 0 fail | 22 files, 21,623 rows, 20,905 distinct names, 0 failures, inventory equal in all rows |
| 4. Text layer | all parse, inventory = file name, `scan_number` = NLP rule | 54 suffixed + 200 sampled (seed 11) = 254 files, 162,468/162,468 distinct ids parse, 0 failures, 0 `scan_number` mismatches |

**Error analysis.**

- *What was read:* the failure lists of criteria 3 and 4 (both empty, so there was no failure
  to classify and no seeded sample of failures to draw); the first and last scan id of each of
  the 54 suffixed inventories.
- *Categories:* partial shot / `deelopname` suffix 0, wrong inventory in the id 0, 5-digit
  number 0, unexpected extension 0, other 0. Every sampled text-layer id ends in `.jpg`.
- *Suffixed inventories:* all 54 carry the suffixed number in every checked id, e.g.
  `NL-HaNA_1.04.02_10119A_0002.jpg` ... `NL-HaNA_1.04.02_10119A_0820.jpg`,
  `NL-HaNA_1.04.02_9524II_0001.jpg` ... `NL-HaNA_1.04.02_9524II_0512.jpg`. The unsuffixed
  number never appears in a suffixed inventory's scans. Several start after 0001
  (`1604B`: 0007, `2352A`: 0003, `9014B`: 0003), so scan 1 is not guaranteed present.
- *Behaviour kept:* LotA's `parse_scan_id` rejected extensions; `ScanId` accepts them, so the
  adapter checks `parsed.ext`. Number 10000 formats as 5 digits and is rejected at
  `digits=4`, as in LotA.
- *Conclusion:* the grammar is exact for both corpora in the sampled data; the 6,606 numeric
  inventories not sampled were not parsed.

**Follow-ups (not switched, per brief).** LotA `cli/document_units.py`, `cli/page_split_*`,
`corpus/tsv_source.parse_doc_id`, `corpus/derive_voc._normalise_scan_id`; NLP
`voc_text_layer`, `separator_sheets`, `layout_features` (phases 05, 07) and
`globalise_alignment.scan_no` (step 12b).

**Findings:** F04-11.1, F04-11.2, F04-11.3, F04-11.4.

**Open issues.** none

### Step 12: page numbering core

**What changed.** New `archival_structures/analysis/page_numbering.py` (numpy and pandas only:
`AlignmentConfig`, `CONFIDENCE_BANDS`, `longest_increasing_chain`, `main_chain`,
`numbering_restarts`, `numbering_mode`, `interpolate_folio_to_scan`, `confidence_band`,
`align_documents`, `align_all`, `anchors_array`, `page_number_anchors`) and
`archival_structures/datasets/globalise_index.py` (`GLOBALISE_INDEX_COLUMNS`,
`load_globalise_index`). Ported verbatim from NLP's `globalise_alignment`, with the hard-coded
numbers as keyword defaults (`edge=6`, `run_window=3`, `foliation=1.5`, `pagination=0.75`,
bands). NLP is untouched. 23 synthetic tests in `tests/test_analysis_page_numbering.py` and
`tests/test_datasets_globalise_index.py`; both modules added to the packaging import test.
Brief: [`step-12-page-numbering.md`](../plan/phase-04-decoders-and-page-numbering/step-12-page-numbering.md).

**Results.**

| Criterion | Expected | Result |
|---|---|---|
| 1. New tests; full suite | green | 23 new; packaging test with the 2 new modules passes; full suite see PROGRESS.md |
| 2a. `load_globalise_index` vs `ga.load_index` | equal | equal, 227,526 x 10, also with `inventories=` the 22 annotated |
| 2b. `align_all` vs NLP `align_all` (minus `n_sections`), whole index | 6/6 | 6/6 (baseline, mode, runs, extrap, dup_first, mode+extrap+dup_first), `DataFrame.equals` |
| 2c. `anchors_array` vs `ga.load_anchors()` | 467/467 | 467/467 |
| 3. Region-table anchors vs mined CSV | measurement; 2555 identical (374) | 467/467 inventories have a regions file; 466 identical; pairs: 64,505 in both, 0 only in mined, 1 only in regions; 2555: 374/374 identical |

Region-anchor accuracy (22 annotated inventories, 1,496 documents, `score_start_accuracy`;
chosen = `mode, extrap, dup_first` scored with `use_alt=True`, i.e. the verso alternative counts;
without `use_alt` the chosen config gives exact .6344, ±1 .8964, ±2 .9392):

| Anchors | Config | n | exact | ±1 | ±2 |
|---|---|---|---|---|---|
| mined | baseline | 1496 | .6190 | .8743 | .9298 |
| regions | baseline | 1496 | .6190 | .8743 | .9298 |
| mined | chosen | 1496 | .7794 | .9104 | .9479 |
| regions | chosen | 1496 | .7794 | .9104 | .9479 |

The anchor arrays of the 22 annotated inventories are identical between the two sources, so
equal accuracy is expected, not a separate result. Tables:
`reports/data/phase-04-decoders-and-page-numbering/step12-*.tsv` (per-inventory agreement,
accuracy, the 92 high-confidence errors of both configs, the 3,946 multi-region scans).

**Error analysis.**

- *Criterion 3 differences.* Only one pair differs, so all of it was read; the seeded sample of
  30 (`random.Random(12)`) was drawn instead from the 3,946 scans with several `page-number`
  regions, where document order and reading order could diverge. Differing pair: inventory
  10209, `NL-HaNA_1.04.02_10209_0969.jpg`, page-number region text `12` (box 121 x 159), no row
  in the mined CSV and no row in NLP's scan spine either (the spine has 1,594 scans for 10209,
  the regions file 1,565 scan ids, 0969 among them). Category: region present in one source
  only (different scan set/PageXML version); counts: reading-vs-document order 0, nested or
  zero-line 0, word-break join 0, different source 1, other 0.
- *Sample of 30 multi-region scans.* Each has two or three non-empty page-number regions, e.g.
  `NL-HaNA_1.04.02_4356_0782.jpg` (`1302.`, `1303.`: two page numbers of a double page),
  `..._7600_0369.jpg` (`329`, `25 33 7`), `..._10233_0181.jpg` (`debit`, `Credit`, `rest`:
  mis-tagged column headings), `..._8307_0199.jpg` (`d: 15 No br 1789`, `167`). The mined
  value (reading order, last wins) equals the regions value (document order, last wins) in all
  of them, and in all 3,946, since the totals agree. Often the last region is not the right
  number of the pair (`1302.` then `1303.`; `104.` then `141`), but both sources pick the same
  one, so this is a property of the shared rule.
- *Non-digit last regions.* 32,508 of 97,014 scans (33.5%) end on a non-digit text (`17.`,
  `273 9`, `519 ƒ`, `1628:`, `Eisch voor het Boek Jaar`) and are dropped without fallback. A
  tolerant pattern (trailing punctuation) would add many anchors; not tried (measurement only).
- *High-confidence errors (chosen).* 1,281 documents are `high`; 42 (3.3%) have error > 2
  (inventories 2548: 9, 1274: 6, 2775: 6, 3142: 6, 1120: 4, others fewer), all method `chain`.
  Five read: (1) 2775 TANAP 72165, folio 1526 -> predicted 333, gold 271: anchors near the
  gold read 1511, 1517, 1518, 1525 at scans 291 to 331, so the numbering is not linear around
  the document, the document starts 60 scans before the chain; (2) 2775 TANAP 72164, the same
  stretch, folio 1524 -> 329 vs 267; (3) 8276 TANAP 140744, folio 16 -> 54 vs 22: pagination
  mode, anchor (51, 13) in the chain while (21, 15) was dropped, noisy anchors; (4) 2548 TANAP
  62395, folio 388 -> 445 vs 440: neighbouring anchors read `3393`, `385`, `379`, misread
  digits; (5) 2548 TANAP 62602, folio 1605 -> 1670 vs 1673: the true anchor (1673) was read as
  `1005` and `1907` for 1607, so it fell out of the chain. Misread numbers dominate; a
  foliation/pagination confusion shows up in none of the five.
- *Conclusion.* The core is a faithful port, the region table is a sufficient anchor source
  (one extra pair in 64,506), and the band `high` is right 96.7% of the time within ±2.

**Findings:** F04-12.1, F04-12.2, F04-12.3, F04-12.4, F04-12.5, F04-12.6.

**Open issues.** none

### Step 12b: NLP alignment shim

**What changed.** NLP `src/startpage/globalise_alignment.py` is now a shim: `Config`,
`INDEX_COLS`, `BANDS`, `lis_indices`, `main_chain`, `extra_runs`, `numbering_mode`, `_interp`,
`_band`, `_locate`, `align_inventory`, `scan_no`, `load_index` alias or delegate to AS
(steps 11-12); `align_all` calls AS and adds back `n_sections = NaN`. Path constants, the anchor,
spine and gold loaders and the scorers stay in NLP. The original is kept in
`output/as-phase-04-originals/src/`. New `scripts/verify_as_globalise_alignment.py` reruns the
unchanged build script in subprocesses (`cwd=<NLP>`, module `OUT` redirected to a scratch
directory) for the shim and for the original module.

**Results.**

| Criterion | Result |
|---|---|
| 1. stored CSVs byte-identical | 9/9; manifest equal without `git_head` (baseline_all22 n 1496, exact 0.6189839572192514) |
| 1. A/B current vs originals | 10/10 identical (9 CSVs + manifest) |
| 1. build-script assertion, both modes | passed |
| 1. protected outputs (13 files, sha256 + mtime) | unchanged |
| 2. `grep archival_structures` | imports present; no `lis_indices`/`_interp`/`align_inventory` bodies |
| 3. NLP `test_semi_markov` + `test_evaluation` | 21 passed (needs `PYTHONPATH=src`) |
| 3. AS suite | 890 passed |

Verify runtime: about 12 s for both modes (the brief expected ~1 minute per mode).

**Error analysis.** `scan_no` is stricter, so I counted what it parses: 64,505 anchor rows
(non-empty `page_number`), 269,383 spine rows, 21,623 annotation rows (1,498 START rows after
the merge); none raised. From `document_alignment.csv` (14,827 rows), `method` x `confidence`:
`chain` 12,784 (high 10,305, medium 1,754, low 725); `chain+extrapolated` 1,769 (all low);
`no_folio` 245 and `no_anchors` 29 (confidence none). The 10 sampled `+extrapolated` rows
(seed 0) have `nearest_anchor_dist` 2 to 45 scans, e.g. inventory 8084 folio 4 -> scan 12
(dist 45), 8364 folio 0 -> scan 96 (dist 12.9). The brief's premise is wrong for the stored
output: it was built with the chosen config, `extrap` on (`chosen_refinements`: mode, extrap,
dup_first). So the suffix marks real linear extrapolation in 1,749 rows; only 20 rows have
dist 0, all `numbering_mode` unknown (fewer than 3 chain anchors, so no extrapolation is
possible and np.interp only clamps). Step 12 covers the clamp-only case for the baseline
(F04-12.5). Conclusion: the shim changes nothing observable; the `+extrapolated` rows are
all `low` confidence by construction.

**Findings:** F04-12b.1, F04-12b.2, F04-12b.3.

**Open issues.** none

## Review

Phase review (Opus, 2026-10-14). The code review is in
[`REVIEW.md`](../plan/phase-04-decoders-and-page-numbering/REVIEW.md); verdict: accept with
follow-ups.

**Acceptance, re-run by the reviewer:**
- AS suite 890 passed (717 + 173 new);
- LotA `test_adapters.py` 11 passed and the suite 316 passed (excluding `test_derive_voc.py`,
  which can't be collected: F03-R.3);
- NLP `test_evaluation.py` + `test_semi_markov.py` 21 passed;
- `verify_as_decoders.py`: exit 0, all 7 checks PASS;
- `verify_as_globalise_alignment.py`: exit 0, all 6 checks PASS;
- the step 10 equivalence script: decode 200/200, pmf 2/2, CRF 10/10 and 200/200,
  `train_crf` 3/3;
- the step 12 equivalence script: index equal, 6/6 configs, 467/467 anchor arrays;
- the planning hashes of NLP's decoder and alignment outputs (`output/splits/` included) still
  match.

**Findings.** All 31 planning and step findings were checked against the code or a re-run: 28
are confirmed and 3 revised. F04-10b.4 now has its cause (F04-R.1). F04-10b.5's cause is
collection order (F04-R.3). F04-12b.2's headroom is 0.001, not 0.004.

**The reviewer's own error analysis: the held-out semi-Markov collapse (F04-R.1).** Step 10b
measured the collapse but couldn't separate the emission scores from the prior. The reviewer
retrained the CRF exactly as the script does (long-chunk seed 0; held-out seed 42, 8 threads,
read-only) and read `start_score_from_emissions` at each scored page:

| Split | Pages | Gold starts | Viterbi starts | Semi-Markov starts | Score at gold starts (median, share > 0) | Score at non-starts (median, share > 0) |
|---|---|---|---|---|---|---|
| long-chunk dev | 3,090 | 401 | 416 | 416 | +4.63, 0.838 | −14.48, 0.040 |
| held-out test | 3,614 | 424 | 267 | 36 | −0.65, 0.219 | −4.30, 0.029 |
| held-out train (fit + val) | 17,291 | 1,943 | 773 | 128 | −0.64, 0.229 | −5.02, 0.012 |

In the DP a start at page i turns that page's term from −s[i] into +s[i], and it adds one more
log-pmf term (−1.7 to −2.6 nats for lengths 1–5). So a start pays off only when roughly
s[i] > 1. On long-chunk data most gold starts clear that bar. On held-out data fewer than a
quarter have even s[i] > 0, and this is so on the training inventories as well. The 3-state
CRF trained on kNN scores puts its start evidence in the transitions (END → START) rather than
in the START emission, and the emission-only log-odds drop the transitions. The prior is not
the problem, which agrees with F04-10b.4. A natural next input would be the CRF's posterior
start marginals. That is an NLP research question and was not tried here (no tuning in a
review).

**Process.** Steps 10 ∥ 11 and then 10b ∥ 12 ran in parallel. The shared files (changelog,
this report, `STATUS.md`) kept every step's entries. Two sessions used a Python
read-modify-write where the rules ask for Edit, and nothing was lost. The reviewer removed two
stale lines from the changelog. A transient failure of `test_analysis_token_analysis` during
step 12's parallel run didn't reproduce: 5 reruns and 60 `PYTHONHASHSEED` values all passed.
The test still depends on `list(set)[0]` and a path relative to the working directory.

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F04-R.1 | Feed the semi-Markov decoder the CRF's posterior start marginals (forward-backward) instead of emission log-odds, and re-measure on held-out; a research step for NLP | NLP |
| F04-R.2 | Commit LotA's `archival_language/ead/` (untracked; step 11 edited `adapters.py`) | user |
| F04-R.3 | Add `sys.path` setup (a `tests/conftest.py`, or install `startpage` via NLP's planned `pyproject.toml`) so NLP tests don't depend on order | NLP |
| F04-12b.2 | Compare the alignment build's baseline assertion against the manifest value | NLP (user's decision) |
| F04-12.4 | A tolerant page-number pattern (trailing `.`/`:`) could recover some of the 32,508 scans whose last page-number region isn't all digits; measure before adopting | later AS step (C11 follow-up) |
| P5 | Switch the remaining scan-id parsers to `ScanId`: LotA `cli/document_units.py` L572/574, `cli/page_split_cases.py` L71, `cli/page_split_image_sample.py` L50–51, `corpus/tsv_source.parse_doc_id`, `corpus/derive_voc._normalise_scan_id`; AS `analysis/sequence_patterns._scan_sequence_number` | phase 07 (optional) or a LotA session |
| P5 | NLP `voc_text_layer.inventory_sort_key` → `InventoryNumber`; `separator_sheets._scan_no` → `ScanId` | phases 05 and 07 |
| packaging | `poetry lock` after adding `scipy>=1.10` to core (phase-01 precondition, still open) | user |

## Reproduce

All commands run from the repo root named; `<NLP>` is
`/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`, `<LotA>`
`/Users/Shared/claude_projects/data_scopes/language-of-the-archive`.

- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider` → 890 passed.
- NLP decoders: `cd <NLP>; venv/bin/python scripts/verify_as_decoders.py --out-dir <scratch>` →
  exit 0 (about 5.5 min, 8 threads).
- NLP alignment: `cd <NLP>; venv/bin/python scripts/verify_as_globalise_alignment.py --out-dir <scratch>`
  → exit 0 (about 12 s).
- NLP tests: `cd <NLP>; venv/bin/python -m pytest -q -p no:cacheprovider tests/test_evaluation.py tests/test_semi_markov.py`
  → 21 passed.
- LotA: `cd <LotA>; venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py`
  → 316 passed.
- Held-out score analysis (F04-R.1): `cd <NLP>; venv/bin/python <AS>/reports/data/phase-04-decoders-and-page-numbering/review_heldout_scores.py`
  (read-only, about 3 min).
- The equivalence scripts of steps 10 and 12 were scratch scripts (session scratchpad
  `step10/equiv.py`, `step12/equiv.py`) and are not kept. Their checks are described in the
  briefs (step 10 criterion 3, step 12 criterion 2).
