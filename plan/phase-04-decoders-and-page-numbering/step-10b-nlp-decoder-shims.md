# Step 10b: NLP decoders become shims over AS; stored numbers reproduced

> **Phase:** 04, Decoders and page numbering. **Depends on:** 10.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 10 built `archival_structures.stream_analysis.decoding` (semi-Markov, CRF, BiLSTM,
`train_crf`). In this step NLP's three decoder modules become thin re-exports. The CRF
training helpers in two NLP scripts become wrappers around AS's `train_crf` (user decision
2026-10-14: both copies delegate). A verify script then proves that NLP's stored numbers
reproduce byte for byte. Planning showed that the **current** NLP code reproduces them, so
any difference is caused by this step.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`).
- **Read `<NLP>/CLAUDE.md` first**; its rules apply to files there. One of them: ask how many
  cores torch may use. The user answered **8 threads** (2026-10-14): call
  `torch.set_num_threads(8)` in the verify script, before any model is built.
- Interpreter: `<NLP>/venv/bin/python` (torch 2.12.1, scipy 1.17.1, numpy 2.4.6). It sees AS
  through an editable install. `startpage` is not installed: scripts put `<NLP>/src` and
  `<NLP>/scripts` on `sys.path` themselves.
- Precondition: `<NLP>/venv/bin/python -m pytest -q -p no:cacheprovider tests/test_semi_markov.py tests/test_evaluation.py`
  gives **21 passed** before you edit. If it doesn't, stop and ask.
- AS API from step 10 (check `STATUS.md` *Facts later steps need* for deviations):

```python
from archival_structures.stream_analysis.decoding.semi_markov import (
    EPS, fit_lognormal_duration, log_duration_pmf, decode_semi_markov)  # decode -> list[bool]
from archival_structures.stream_analysis.decoding.crf import (
    NEG_INF, LinearChainCRF, start_score_from_emissions, collapsed_start_f1,
    viterbi_start_f1, train_crf, decode_core)
# start_score_from_emissions(model, features, start_states=(0,)) -> np.ndarray
# collapsed_start_f1(y_true_states, y_pred_states, start_states) -> float
# viterbi_start_f1(model, sequences, start_states) -> float
# train_crf(train_sequences, dev_sequences, n_features, n_states=3, start_states=(0,), seed=0,
#           *, lr=0.05, max_epochs=40, patience=6) -> (model, best_f1)
# decode_core(model, sequences, start_states, sep="/") -> (y_true: list[bool], y_pred: list[bool],
#           scan_ids: list[str], gold_states: list[int], pred_states: list[int])
from archival_structures.stream_analysis.decoding.bilstm import BiLSTMTagger  # predict -> list[bool]
```

**NLP files to change** (all tracked and clean in git on 2026-10-14):

| File | Anchors | Change |
|---|---|---|
| `src/startpage/semi_markov.py` (102 lines) | `decode_semi_markov` L58 | shim. Re-export `EPS`, `fit_lognormal_duration`, `log_duration_pmf`. `decode_semi_markov(start_scores, log_pmf, max_duration)` wraps AS and returns `[int(x) for x in ...]` (NLP returned 0/1 ints) |
| `src/startpage/crf_sequence.py` (106 lines) | `START, INSIDE, END = 0, 1, 2` L27, `STATE_NAMES` L28, `NEG_INF` L29 | shim. Keep `START, INSIDE, END`, `STATE_NAMES` (NLP's layout) defined here; re-export `NEG_INF`, `LinearChainCRF` from AS |
| `src/startpage/bilstm_sequence.py` (32 lines) | `BiLSTMTagger` L18 | shim: re-export `BiLSTMTagger` |
| `scripts/train_crf_sequence.py` (439 lines; imported by others as `step31`) | `dev_f1(model, sequences)` L225, `train_crf(train_sequences, dev_sequences, n_features, seed)` L241, module constants `MAX_EPOCHS`, `PATIENCE` | keep both names and signatures; bodies become `viterbi_start_f1(model, sequences, (START,))` and `train_crf(..., n_states=3, start_states=(START,), seed=seed, max_epochs=MAX_EPOCHS, patience=PATIENCE)` |
| `scripts/train_multitask_crf.py` (434 lines) | `collapsed_f1` L173, `dev_f1` L179, `train_crf(train_sequences, dev_sequences, n_features, n_states, start_states, seed)` L191, `decode_sequences(model, sequences, start_states)` L225 | keep names and signatures; delegate to AS. `decode_sequences` maps AS bools back to `"start_page"`/`"not_start_page"` and returns the same 5-tuple |
| `scripts/train_semi_markov_duration.py` (304 lines) | `start_score_from_emissions(model, features)` L98 | body becomes `start_score_from_emissions_as(model, features, start_states=(START,))` (import under an alias) |

Each docstring says "Thin re-export / wrapper over AS `stream_analysis.decoding` (AS phase 04
step 10)". Remove imports that become unused (`f1_score` etc.) only where nothing else in the
file uses them.

Unchanged and out of scope: `scripts/train_bilstm_sequence.py` (its own `train_bilstm`
loop stays), `scripts/error_analysis_3.4_end_misses.py` (it calls `mt.train_crf`,
`mt.decode_sequences`, which keep working), every feature builder and `CONFIGS`.

**Copy the originals first**, before any edit, into a new directory
`<NLP>/output/as-phase-04-originals/` (`chmod g+w` the directory and the files):
`src/semi_markov.py`, `src/crf_sequence.py`, `src/bilstm_sequence.py`,
`scripts/train_crf_sequence.py`, `scripts/train_multitask_crf.py`,
`scripts/train_semi_markov_duration.py`, `tests/test_semi_markov.py` (keep those two subdirs).

## Protected outputs

Hash these (sha256 + mtime) before the first run and check after every run. Nothing may
change them:
`<NLP>/output/semi_markov_duration_{metrics,decoded,fit}_scorefix.csv`,
`<NLP>/output/semi_markov_gain_vs_start_rate_scorefix.csv`, `<NLP>/output/multitask_crf_*.csv`,
`<NLP>/output/crf_sequence_*_scorefix.csv`, every file in `<NLP>/output/splits/`.

How the scripts write (verified 2026-10-14):
- `train_semi_markov_duration.main()` reads four module globals at call time:
  `METRICS_OUT_PATH`, `DECODED_OUT_PATH`, `DURATION_FIT_OUT_PATH`,
  `GAIN_VS_START_RATE_OUT_PATH` (L53–56). **Patch all four** on the imported module before
  `main()`. (`METRICS_PATH` doesn't exist; patching it would do nothing.)
- `train_multitask_crf.run_config(config_name, full_sequences, feature_df, position_lookup)`
  writes nothing and returns `(metric_rows, end_rows, position_rows)`. Build its arguments the
  way `main()` does (L380–382): `step31.build_full_page_sequences()`,
  `step31.load_scalar_features()`, `load_position_lookup()`. **Never call `mt.main()`**; it
  writes `output/multitask_crf_*.csv`.
- `load_or_build_split` writes `output/splits/` only if a cache is missing; both caches it
  uses exist. The hash check proves it.

## Deliverables

| File | Change |
|---|---|
| the 6 NLP files above | shims / wrappers |
| `<NLP>/output/as-phase-04-originals/` | the copies |
| `<NLP>/scripts/verify_as_decoders.py` | new (below) |
| `docs/shared_library/changelog.md` (AS) | under the existing `### Step 10` entry, add with Edit an "NLP switched (step 10b)" bullet listing the 6 NLP files |
| `reports/phase-04-decoders-and-page-numbering.md` (AS) | `### Step 10b` section, findings `F04-10b.n` |

## `scripts/verify_as_decoders.py`

argparse: `--out-dir` (required; a scratch directory, never under `<NLP>/output/`),
`--mode {current,originals,both}` (default `both`), `--threads` (default 8).
For each mode it runs, **in a subprocess** (so the two modes never share imported modules):
1. `train_semi_markov_duration` with the 4 paths patched into `<out-dir>/<mode>/`;
2. `train_multitask_crf.run_config("start_only", ...)`, with the metric rows written to
   `<out-dir>/<mode>/multitask_start_only_metrics.csv` (`index=False`).

Mode `originals` puts `<NLP>/output/as-phase-04-originals/scripts` first on `sys.path`. It
imports the `startpage` package from `<NLP>/src`, then loads the three original src copies
with `importlib.util.spec_from_file_location` under the names `startpage.semi_markov`,
`startpage.crf_sequence` and `startpage.bilstm_sequence`, and registers them in `sys.modules`
before importing any script. Mode `current` uses the edited files. The subprocess cwd is
`<NLP>`.

Checks (print each with PASS/FAIL; exit 0 only if all pass):
- **stored:** for the current mode, the 4 semi-Markov CSVs are **byte-identical** to the stored
  `<NLP>/output/...` files. The start_only rows read with `float_precision="round_trip"`
  `.equals()` the `config == "start_only"` rows of `<NLP>/output/multitask_crf_metrics.csv`
  (after `reset_index(drop=True)`; 6 rows × 23 columns);
- **A/B:** with `both`, every CSV in `current/` is byte-identical to its `originals/` twin;
- **unit equivalence:** for seeds 0..4, `BiLSTMTagger(6)` from AS and from the original copy,
  after the same `torch.manual_seed`, have `torch.equal` state_dicts and forward outputs on a
  random `(12, 6)` input;
- **protected hashes** unchanged.

## Constraints

- Never `git commit`; `chmod g+w` everything new. Don't bump AS's version.
- Don't edit AS code. If AS needs a change, stop and ask (it belongs to step 10).
- Don't change `tests/test_semi_markov.py`; it must pass through the shim unchanged.
- Don't touch `src/startpage/evaluation.py`, `splits.py`, `long_subsequence_chunk_split.py`
  (phase 03 shims, uncommitted), or `globalise_alignment.py` (step 12b).

## Tests

- NLP: `<NLP>/venv/bin/python -m pytest -q -p no:cacheprovider tests/test_semi_markov.py tests/test_evaluation.py`
  → **21 passed**, as before (4 + 17).
- AS full suite unchanged: `.venv/bin/python -m pytest -q -p no:cacheprovider` (count from
  step 10's `STATUS.md`).
- `<NLP>/venv/bin/python scripts/verify_as_decoders.py --out-dir <scratchpad>/verify10b`
  → exit 0.

## Acceptance criteria

Planning reran the **original** code on 2026-10-14 (8 threads) and got:
- `train_semi_markov_duration`: 4/4 CSVs byte-identical to the stored files (75.6 s); the
  decoded CSV has 20,112 rows;
- `run_config("start_only")`: 6/6 rows `.equals()` the stored rows (79.4 s).

So:
1. Verify script exit 0: **stored 4/4 byte-identical; start_only 6/6 rows equal; A/B 5/5
   files byte-identical; BiLSTM 5/5 seeds equal; protected hashes unchanged.**
2. NLP tests 21 passed; AS suite green.
3. `grep -n "archival_structures" <NLP>/src/startpage/{semi_markov,crf_sequence,bilstm_sequence}.py`
   shows the re-exports; the two scripts no longer contain an optimiser loop
   (`grep -n "optim.Adam" <NLP>/scripts/train_{crf_sequence,multitask_crf}.py` gives no hits).

If criterion 1 fails, stop and ask; don't loosen a comparison.

## Error analysis

- Read the semi-Markov metrics rows (18 rows: 2 split kinds × 3 seeds × 3 decodes) from
  `current/` and state for each split kind whether semi-Markov beats the plain CRF on
  `f1_pooled`. Set this beside the C9 note that "the choice of decoder matters little
  (0.78–0.80 F1 inside a ±0.14 CI)": does it still hold? Quote the numbers.
- From `current/semi_markov_duration_decoded_scorefix.csv`, pick 3 inventories (seeded
  `random.Random(10)`) and, for each, list the pages where `y_pred_semi_markov` and
  `y_pred_crf` disagree (up to 10 per inventory). Say whether the disagreements are long
  documents split by the CRF and merged by the prior, or the reverse. Use pandas with
  `usecols`; don't print the whole file.
- Report the floors next to the decoder numbers (phase-03 rule: a floor next to every decoder
  number). Take them from the stored `<NLP>/output/trivial_baseline_metrics.csv` (read-only,
  `float_precision="round_trip"`; 12 rows). Use the rows for the same split kind and eval split:
  majority, every-k at the train rate, and the length prior. Majority Pk on long-chunk dev is
  0.30327993644426438. Put F1 and Pk side by side.
- Seed means from the planning rerun (`f1_pooled` / `pk_macro`): long-chunk dev crf
  0.7814/0.2039, no_transition 0.7801/0.2071, semi_markov 0.7829/0.2054. Held-out test crf
  0.3523/0.2958, no_transition 0.3694/0.3217, **semi_markov 0.1681/0.3472**. On held-out data the
  prior halves F1 (F04-P.3). Read 2 held-out inventories (the worst and one median by
  semi-Markov F1). Compare their predicted start rate with the true start rate, and the
  held-out fitted prior (mu 1.2256, sigma 0.9915, median length 3) with their true segment
  lengths. Say whether the collapse is miscalibrated emission scores (held-out uses kNN
  scores) or the prior. This is reading, not tuning: don't change the decoder.
- Expected findings: the reproduction (result), the semi-Markov vs CRF reading (result or
  negative-result), and any shim surprise (method).

## Do not touch

- AS code (step 10 owns `decoding/`; steps 11/12 own `model/`, `analysis/`, `datasets/`).
- NLP outputs listed under *Protected outputs*.

## When done

1. Add your `### Step 10b: NLP decoder shims` section to
   `reports/phase-04-decoders-and-page-numbering.md` (AS), per `plan/templates/REPORT.md`;
   findings `F04-10b.n`, status `draft`. Run `make reports` from the AS root and fix any
   warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`. Add to *Blockers and open questions for the user*: "NLP
   working tree: 6 decoder files changed, `scripts/verify_as_decoders.py` and
   `output/as-phase-04-originals/` new; commit in NLP".

```markdown
### Step 10b: NLP decoder shims
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none in AS; NLP names unchanged
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers and runtimes>
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 10b](../../reports/phase-04-decoders-and-page-numbering.md#step-10b-nlp-decoder-shims);
  findings F04-10b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
