# Step 09: C8 trivial baselines (oracle clearly labelled); the corruption harness gets floors

> **Phase:** 03, Evaluation for NLP. **Depends on:** 07, 07b (verify script and originals),
> 08 (`core_rows`, `eval_scan_ids`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP's trivial baselines (majority, every-k at the train start rate, a per-inventory own-rate
**oracle**, and a geometric length prior) live inside one script. They are the floors that
every segmentation number should be read against. This step moves them into
`archival_structures.stream_analysis.evaluation.baselines` with bool predictions. The oracle
gets an unmistakable name (D9: it uses each sequence's own true labels, so it is not a
legitimate predictor). The AS corruption harness gets a one-call way to report the same floors.
NLP's script keeps its CLI, its split choice and its output columns, and reproduces its stored
CSV.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`). **Read
`<NLP>/CLAUDE.md` first.** Interpreter `<NLP>/venv/bin/python`.

Source: `<NLP>/scripts/run_trivial_baselines.py` (203 lines, **untracked in git**; the original
copy from step 07b is at `<NLP>/output/as-phase-03-originals/run_trivial_baselines.py`). Read it
in full. Anchors:
- L40 `RESULTS_CSV_PATH`, L41 `HSMM_SEEDS = [42, 43, 44]`, L43 `SPLIT_CELLS` (long_chunk/dev;
  inventory_held_out seed 0/test);
- L49 `core_rows`, L57 `train_start_rate(split_df, labels)` (core rows with `split ==
  "train"`; mean of `label == START`), L64 `eval_scan_ids`;
- L70 `predictions_from_period_fn` (`k = max(1, period_fn(inv))`; start iff `pos % k == 0`),
  L81 `predictions_majority`, L86 `predictions_length_prior_hsmm` (below), L103
  `predict_for_scan_ids`, L111 `flatten_metrics` (23 keys), L130 `score`;
- L137 `main()`:
  - `k_corpus = round(1 / rate_train)` and `mean_segment_length = 1 / rate_train`;
  - the oracle period is `round(1 / own_rate[inv]) if own_rate[inv] > 0 else len(seq)`, where
    own_rate is over the inventory's **whole** sequence;
  - the first three methods are scored with `seed=None` and the HSMM with `seed=seed`;
  - `k_or_param` is `k_corpus` for all three non-HSMM rows (including the oracle) and
    `round(mean_segment_length, 2)` for the HSMM;
  - row keys: `method, split_kind, eval_split, held_out_seed, seed, k_or_param, n_eval,
    **metrics`.

Length prior, verbatim (one `rng` shared across inventories, in `full_sequences` order):

```python
rng = np.random.default_rng(seed); p = 1.0 / mean_segment_length
for inv, sequence in full_sequences.items():
    n_pages = len(sequence); starts = set(); pos = 0
    while pos < n_pages:
        starts.add(pos); length = max(1, int(rng.geometric(p))); pos += length
```

Stored target `<NLP>/output/trivial_baseline_metrics.csv` (12 rows × 30 columns; never
overwrite it). Anchor values (exact floats):

| row | method | split | f1_pooled | pk_macro | window_diff_macro |
|---|---|---|---|---|---|
| 0 | majority | long_chunk | 0.0 | 0.30327993644426438 | 0.30327993644426438 |
| 1 | every_kth_corpus_rate | long_chunk | 0.14209115281501339 | 0.64368919382495504 | 0.67454487316348311 |
| 2 | every_kth_own_rate_ORACLE | long_chunk | 0.20297699594046009 | 0.51529562765471926 | 0.54049017105710950 |
| 3 | length_prior_hsmm s42 | long_chunk | 0.12250332889480690 | 0.57573261593056602 | 0.61398313086630063 |
| 6 | majority | inventory_held_out | 0.0 | 0.38493556686104008 | 0.38493556686104008 |

`k_or_param` is 9 / 8.64 (long_chunk) and 9 / 8.9 (held-out); `n_eval` is 3090 / 3614. **16
cells can't be reproduced** (user decision 2026-10-14: exclude them and don't change NLP's
seeding). They are the four CI columns `f1_macro_ci_lo/hi` and `f1_pooled_ci_lo/hi` in the
four rows `every_kth_corpus_rate` and `every_kth_own_rate_ORACLE`, because those were
bootstrapped with `seed=None`.

AS names you call (from 07 and 08; check `STATUS.md` *Facts later steps need* for
deviations):

```python
from archival_structures.stream_analysis.evaluation import (
    evaluate_all,            # (y_true, y_pred, scan_ids, sequences, tolerance=1, n_bootstrap=1000, seed=None, *, k=None, k_convention="gaps", sep="/")
    core_rows, eval_scan_ids)  # splits.py (08)
from archival_structures.stream_analysis.corruption.metrics import observed_start_labels  # (observed, provenance, canonical) -> list[bool]
from archival_structures.stream_analysis.corruption.operators import initial_provenance    # (stream) -> list[ProvenanceEntry]
from archival_structures.stream_analysis.corruption.canonical_stream import StreamUnit     # StreamUnit(scan_id: str, document_id: str, ...)
```

## Deliverables

| File | Change |
|---|---|
| `archival_structures/stream_analysis/evaluation/baselines.py` | new (API below) |
| `archival_structures/stream_analysis/evaluation/aggregate.py` | add `flatten_metrics(results) -> dict`: NLP L111's 23 keys and expressions, verbatim |
| `archival_structures/stream_analysis/evaluation/__init__.py` | re-export the new names |
| `archival_structures/stream_analysis/corruption/metrics.py` | add `start_classification_floors` (below); export it from `corruption/__init__.py` |
| `docs/challenges/simulation_harness.md` | §5 *Evaluation metrics*: one bullet on floors and `start_classification_floors` |
| `tests/test_stream_analysis_evaluation_baselines.py` | new |
| `<NLP>/scripts/run_trivial_baselines.py` | uses AS for predictions, the train rate, scan ids and `flatten_metrics`; adds `--out PATH` (default `RESULTS_CSV_PATH`); everything else unchanged |
| `docs/shared_library/changelog.md` | `### Step 9: trivial baselines (C8)` under *Unreleased*, with Edit |
| `reports/phase-03-evaluation-for-nlp.md` | `### Step 09` section, draft findings `F03-09.n` |

## Behaviour to implement

```python
def majority(n: int) -> list[bool]                                  # all False
def every_k(n: int, k: int, offset: int = 0) -> list[bool]          # [(i - offset) % k == 0]; k < 1 -> ValueError
def length_prior(n: int, mean_length: float, rng: np.random.Generator) -> list[bool]
    # NLP loop above for one sequence; mean_length < 1 -> ValueError. The caller shares rng
    # across sequences, so seed-for-seed equality holds when sequences are visited in order.
def oracle_every_k_own_rate(true_labels: Sequence[bool]) -> list[bool]
    # ORACLE: rate = mean(true_labels); period = round(1/rate) if rate > 0 else n;
    # every_k(n, max(1, period)). The docstring's first line says it uses the sequence's own
    # true labels and is not a legitimate predictor (D9).
def train_start_rate(split_df, is_start: Mapping[tuple[str, str], bool], split: str = "train") -> float
    # core_rows(split_df) with split == split; mean of is_start[(inventory_number, page_id)].
    # No rows -> ValueError. The docstring states the leakage rule: fit on train only.

@dataclass(frozen=True)
class BaselineRun:
    method: str            # "majority" | "every_k_train_rate" | "every_k_own_rate_ORACLE" | "length_prior"
    seed: int | None
    param: float           # k for every_k, mean_length for length_prior, nan otherwise
    is_oracle: bool
    predictions: dict[str, list[bool]]   # sequence id -> predictions, same order/length as input

def baseline_suite(sequences: Mapping[str, Sequence[bool]], train_start_rate: float,
                   seeds: Sequence[int] = (42, 43, 44), include_oracle: bool = False) -> list[BaselineRun]
    # order: majority; every_k_train_rate with k = round(1/train_start_rate) (then max(1, k));
    # [oracle]; one length_prior per seed with rng = np.random.default_rng(seed) shared over
    # `sequences` in mapping order and mean_length = 1/train_start_rate.
    # Validates labels with check_labels. train_start_rate must be in (0, 1].

def baseline_floors(sequences: Mapping[str, Sequence[bool]], *, train_start_rate: float | None = None,
                    seeds=(42, 43, 44), include_oracle=True, tolerance=1, k=None, k_convention="gaps",
                    n_bootstrap=200, bootstrap_seed=0) -> pd.DataFrame
    # If train_start_rate is None, it is computed from `sequences` and column
    # rate_from_eval_labels=True (then every_k/length_prior are rate-oracles too; say so).
    # Page ids f"{i:06d}", scan ids f"{seq_id}/{page_id}"; one evaluate_all per run.
    # Columns: method, seed, param, is_oracle, rate_from_eval_labels, then flatten_metrics keys.
```

In `corruption/metrics.py`:

```python
def start_classification_floors(observed, provenance, canonical, **kwargs) -> pd.DataFrame:
    """Framing-A floors for one corrupted stream: baseline_floors({"stream": observed_start_labels(...)}, **kwargs)."""
```

Import `baselines` inside the function, so that `corruption` stays as light as it is.

**NLP script.**
- Build `bool_sequences = {inv: [l == START for _, l, _ in seq]}` and
  `is_start = {(inv, s): l == START ...}`.
- `rate_train = train_start_rate(split_df, is_start)`, and `scan_ids = eval_scan_ids(split_df,
  eval_split)`.
- Run `baseline_suite(bool_sequences, rate_train, seeds=HSMM_SEEDS, include_oracle=True)`.
  Turn each run into NLP's `preds` dict `{(inv, scanname): START | "not_start_page"}`, then
  score it exactly as before. That means `score(...)` goes through the 07b shim's
  `evaluate_all`, with `seed=None` for the three non-HSMM runs.
- Map the method names `every_k_train_rate → every_kth_corpus_rate`,
  `every_k_own_rate_ORACLE → every_kth_own_rate_ORACLE` and `length_prior →
  length_prior_hsmm`.
- Keep the row order, `k_or_param` (k_corpus for the oracle row too), the prints, and the
  chmod.
- Delete the local `core_rows`, `label_lookup` (if unused), `train_start_rate`,
  `eval_scan_ids`, `predictions_*` and `flatten_metrics`.
- Add a module-docstring line that the oracle row is an upper bound, not a baseline.
- Keep `from startpage.evaluation import START, evaluate_all` at module level, and keep
  `score()` calling the module-level `evaluate_all`. 07b's verify script swaps that name to
  compare the original implementation with the shim.

## Constraints

- Seed-for-seed equality with `np.random.default_rng`: same `p`, same `int(rng.geometric(p))`,
  same visiting order. Don't vectorise.
- Banker's rounding (`round`) as in NLP, for `k_corpus` and for the oracle period.
- No VOC constants in AS. Don't touch splits (08) or NLP `evaluation.py` (07b).
- Never overwrite `<NLP>/output/trivial_baseline_metrics.csv` or `output/splits/`.
- Never `git commit`; `chmod g+w` new files.

## Tests

`tests/test_stream_analysis_evaluation_baselines.py`:
- `majority(4) == [F,F,F,F]`; `every_k(7, 3) == [T,F,F,T,F,F,T]`; `every_k(5, 2, offset=1) ==
  [F,T,F,T,F]`; `every_k(3, 0)` raises.
- `length_prior` equals an inline copy of NLP's loop (above) over three sequences of lengths 30,
  1, 17 with one shared `default_rng(42)`, for seeds 42, 43 and 44. `length_prior(5, 0.5, rng)`
  raises.
- Oracle:
  - `oracle_every_k_own_rate([T,F,F,F,T,F,F,F])` gives period 4: `[T,F,F,F,T,F,F,F]`;
  - all-False of length 5 gives period 5: `[T,F,F,F,F]`;
  - `"ORACLE" in run.method` and `run.is_oracle` hold for that run in `baseline_suite`, and
    `"oracle"` is in the function's docstring (lower-cased).
- Leakage: `train_start_rate` on a frame where every start page is in `dev` or is a `context`
  row gives `0.0`. A frame with no train rows raises.
- `baseline_suite` order and count: 1 + 1 + (1) + len(seeds); each prediction list has its
  input's length.
- `baseline_floors` on two toy sequences has one row per run and includes `pk_macro` and
  `rate_from_eval_labels`. Majority recall is 0.
- `start_classification_floors` on a 3-document, 9-unit canonical stream with
  `initial_provenance` (observed = canonical) gives a frame in which the oracle row's
  `f1_pooled` is ≥ the majority row's.
- `flatten_metrics` returns exactly NLP's 23 keys in NLP's order.

Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_evaluation_baselines.py`,
then the full suite. NLP: `cd <NLP> && venv/bin/python scripts/run_trivial_baselines.py --out <scratch>/tb.csv`.

## Acceptance criteria

1. The NLP rerun matches the stored CSV in every cell except the 16 excluded ones. Read both
   with `dtype={"f1_worst_inventory": str, "tol1_f1_worst_inventory": str,
   "pk_worst_inventory": str}` **and `float_precision="round_trip"`** (step 07b, F03-07b.2:
   pandas' default parser is off by up to 96 ulp), and compare with `==` (NaN equals NaN). That is 12 × 30 − 16 =
   344 cells. The anchor values in the table above match exactly.
2. Also run `venv/bin/python scripts/verify_as_evaluation_shim.py` (from 07b). It still exits 0
   after the script change; its trivial-baseline check imports the new script.
3. `output/splits/` sha256 is unchanged, and the stored CSV's mtime is unchanged.
4. The new AS tests pass and the full AS suite is green. The total is the post-08 count plus
   your tests.
5. `grep -n "ORACLE\|oracle" archival_structures/stream_analysis/evaluation/baselines.py` shows
   the name and the docstring warning.

## Error analysis

- Per inventory (long_chunk/dev), from `results["pk"]["per_inventory"]` for majority and the
  oracle: list the 22 rows (Pk, true start rate, mean segment length). Read the 5 worst for
  majority, including 3142 (Pk 0.521). Say what makes an inventory hard for "no boundaries":
  short segments? Then say how much the oracle's knowledge of the rate buys there (finding).
- Majority Pk equals majority WindowDiff (0.30328 in both). Explain why this holds by
  construction when the prediction has no boundaries (one sentence; method finding).
- Floors on a corrupted stream: use the harness on a synthetic 60-unit stream (10 documents of
  6 units) with `intra_document_swap` at rate 0.5 (`random.Random(0)`). Report the floors table
  next to the uncorrupted one. Say whether any floor moves, and why (Framing A labels follow the
  canonical start).

## Do not touch

- NLP `evaluation.py`, `splits.py`, `long_subsequence_chunk_split.py`, the cached outputs; AS
  `segmentation.py`, `splits.py`, `parsers/`, LotA.

## When done

1. Fill in `### Step 09: …` in `reports/phase-03-evaluation-for-nlp.md`, with findings
   `F03-09.n` (draft). Run `make reports`; no warnings.
2. Append the `plan/templates/STEP.md` *When done* block to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: all steps done; next is the phase review (prompt 3 in
   `plan/PROMPTS.md`).
