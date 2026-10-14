# Step 07: C6 evaluation suite in AS; `corruption/metrics` becomes a wrapper

> **Phase:** 03, Evaluation for NLP. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP has a full segmentation-evaluation suite (string labels, per-inventory aggregation,
bootstrap, run handling). AS has a smaller one (`corruption/metrics.py`, bool labels). This step
ports NLP's suite into a new AS package `archival_structures/stream_analysis/evaluation/`. The
port uses AS conventions: bool labels, AS default k, and D3b edge cases. AS's
`corruption.metrics.window_diff` and `score_start_classification` become thin wrappers over it.
**AS only.** NLP is not edited in this step; step 07b turns NLP's module into a shim and proves
reproduction. NLP's numbers must stay reproducible, so everything not named below is ported
**verbatim**.

## Inputs

NLP root: `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (read-only in
this step). Source to port: `src/startpage/evaluation.py` (514 lines, checked 2026-10-14).
Read it in full; it is the only NLP file you need. Anchors (line numbers as of 2026-10-14):

| L | Name | Port as |
|---|---|---|
| 20 | `START = "start_page"` | **not ported** (string labels stay in NLP) |
| 35 | `build_eval_frame(y_true, y_pred, scan_ids, full_sequences)` | `aggregate.build_eval_frame`, changed (below) |
| 73, 80, 87 | `page_level_prf`, `_page_level_counts`, `page_level_prf_with_counts` | `segmentation`; `_page_level_counts` → public `page_level_counts` |
| 107 | `iter_contiguous_runs(true_labels, pred_labels, positions, is_last_page=None)` | `segmentation`, verbatim, no label validation |
| 140–174 | `_tolerance_match_counts`, `_prf_from_counts`, `_tolerance_counts_runs`, `_tolerance_prf_runs` | `segmentation`; `_tolerance_prf_runs` → public `tolerance_prf_runs`, keep a private alias `_tolerance_prf_runs = tolerance_prf_runs` |
| 181 | `tolerance_prf(y_true, y_pred, sequence, tolerance=1, positions=None)` | `segmentation` |
| 219–270 | `_boundary_gaps`, `default_k`, `_window_errors`, `_pk_compare`, `_wd_compare`, `_windowed_metric` | `segmentation`, changed (below) |
| 273, 279 | `pk`, `window_diff` (both `(true_labels, pred_labels, k=None, positions=None, is_last_page=None)`) | `segmentation`, plus keyword-only `k_convention="gaps"` |
| 297, 319 | `_segment_matches_in_run`, `segment_exact_match` | `segmentation` |
| 343–385 | `PerInventoryResult`, `per_inventory` | `aggregate`, verbatim |
| 385 | `start_rate_calibration(eval_frame)` | `aggregate` |
| 406, 421 | `bootstrap_ci`, `bootstrap_pooled_f1` | `aggregate`, verbatim. **Keep `np.random.RandomState(seed)`**: NLP's CIs depend on it |
| 444 | `evaluate_all(y_true, y_pred, scan_ids, full_sequences, tolerance=1, n_bootstrap=1000, seed=None)` | `aggregate`, changed (below) |
| 490 | `format_report(results)` | `aggregate`, verbatim |

NLP's tests to port: `tests/test_evaluation.py` (268 lines, 17 tests, pytest, string labels,
helper `make_frame` at L19). Port the 16 tests from L37 to L229. **Don't port**
`test_page_level_prf_reproduces_existing_knn_numbers` (L250); it needs a 60 MB `.npz` and
NLP's torch training code.

AS file to wrap: `archival_structures/stream_analysis/corruption/metrics.py` (209 lines).
Functions: `_prf1(tp, fp, fn) -> PRF1` (L43), `_score_binary` (L50), `_to_gaps` (L105), and
`window_diff(true_labels, predicted_labels, k=None)` (L112–150). `window_diff` raises
ValueError on a length mismatch, on n<2, and when not `1 <= k <= n_gaps`; its default is
`max(1, round(n_gaps / (2 * n_segments)))`. It has 17 tests in
`tests/test_stream_analysis_corruption_metrics.py` (`TestWindowDiff` at L79–107). The packaging
test `tests/test_packaging_core_imports.py` (L39) imports a list `names` with the extras blocked.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/stream_analysis/evaluation/__init__.py` | new; re-exports every public name of `segmentation` and `aggregate`, with `__all__` |
| `archival_structures/stream_analysis/evaluation/segmentation.py` | new; per-sequence metrics |
| `archival_structures/stream_analysis/evaluation/aggregate.py` | new; frame, per-inventory, bootstrap, `evaluate_all`, `format_report` |
| `archival_structures/stream_analysis/corruption/metrics.py` | `_score_binary` and `window_diff` delegate (below); nothing else changes |
| `tests/test_stream_analysis_evaluation_segmentation.py` | new |
| `tests/test_stream_analysis_evaluation_aggregate.py` | new |
| `tests/test_stream_analysis_corruption_metrics.py` | **unchanged**; all 17 must pass as they are |
| `tests/test_packaging_core_imports.py` | add `"archival_structures.stream_analysis.evaluation"` to `names` |
| `docs/shared_library/changelog.md` | `### Step 7: segmentation evaluation suite (C6)` under *Unreleased*, added with Edit (create the `## Unreleased` heading above `## v0.6.0` if it is missing) |
| `reports/phase-03-evaluation-for-nlp.md` | fill in `### Step 07` and add draft findings `F03-07.n` |

Don't add evaluation names to `stream_analysis/__init__.py`'s `_LAZY_EXPORTS`, because
`window_diff` would clash with the corruption export. Users import from
`archival_structures.stream_analysis.evaluation`.

## Behaviour to implement

**Labels (P1).** Every metric function that reads labels validates them first with a public
helper:

```python
def check_labels(labels, name: str = "labels") -> list[bool]:
    """Return labels as list[bool]. Raise TypeError if any element is not bool or numpy.bool_
    (strings, ints, None, floats all raise; the message names `name` and the first bad value)."""
```

Positive means `True` ("is start"). Every `== START` in NLP's code becomes truthiness of a
validated bool. Apply `check_labels` in `page_level_counts`, `page_level_prf(_with_counts)`,
`tolerance_prf`, `tolerance_prf_runs`, `default_k`, `pk`, `window_diff`, `segment_exact_match`,
`build_eval_frame` and `start_rate_calibration` (on `y_true`/`y_pred`). `iter_contiguous_runs`
and `per_inventory` don't validate; they are generic.

**Length mismatch (D3b).** `page_level_counts`, `page_level_prf*`, `tolerance_prf*`, `pk`,
`window_diff` and `segment_exact_match` raise `ValueError` when `len(true) != len(pred)`, or
when `positions` is given and has a different length.

**`page_level_prf`.** Keep sklearn: `precision_recall_fscore_support(y_true, y_pred,
labels=[True], average=None, zero_division=0)`, and return floats as NLP does. This keeps NLP's
numbers bit-identical.

**Default k (D3).** One function and two conventions:

```python
def default_k(true_labels, convention: str = "gaps") -> int:
    # S = sum(_boundary_gaps(labels)) + 1
    # "gaps"  (AS default):  max(1, round((len(labels) - 1) / (2 * S)))
    # "pages" (NLP):         max(1, round((len(labels) / S) / 2))   # keep this exact float expression
    # any other value -> ValueError
```

Use Python's `round` (banker's rounding) in both, as both originals do. Checked values: n=6, S=1
→ gaps 2, pages 3. n=4, S=2 → 1, 1. n=10, S=3 → 2, 2. n=7 with labels `[F,F,T,F,F,F,F]`,
S=2 → 2, 2.

`pk`, `window_diff`, `evaluate_all` and `per_inventory` callers take
`k: int | None = None, *, k_convention: str = "gaps"`. If `k` is None, k is
`default_k(true_labels, k_convention)`, computed **once per call on the full `true_labels`**
(for `evaluate_all`: per inventory, pooled over all its runs), exactly as NLP's
`_windowed_metric` does. Don't compute it per run.

**Short input and clipping (D3b).** `pk`/`window_diff` return `float("nan")` when
`len(true_labels) < 2`, and also when no run has a window (NLP behaviour). Runs shorter than 2
pages are skipped. When `k` exceeds a run's (or the whole sequence's) number of gaps, k is
clipped to that number, as NLP does (`rk = min(k, n)`). The new part: emit **one**
`warnings.warn(..., WindowClippedWarning, stacklevel=2)` per `pk`/`window_diff` call in which
any clipping happened. Define it as `class WindowClippedWarning(UserWarning)` in `segmentation`
so callers can filter it. Clipping must not change any value compared with NLP.

**`build_eval_frame` (generic).**

```python
def build_eval_frame(y_true, y_pred, scan_ids, sequences, sep: str = "/") -> pd.DataFrame:
```

- `sequences: Mapping[str, Sequence[str]]` maps inventory → page ids in physical order (NLP's
  tuple-shaped `full_sequences` is adapted in NLP's shim).
- `scan_ids` are `f"{inventory}{sep}{page_id}"`, split on the first `sep`.
- Output columns: `inventory, page_id, position, is_last_page, y_true, y_pred`. This renames
  NLP's `scanname` to `page_id`; otherwise it is NLP's logic (sort, `is_last_page`, KeyError
  for an unknown page).
- **Context guard (hazard 3):** if `scan_ids` contains a duplicate, raise `ValueError`. The
  message must say that a page is scored twice and ask whether `role == "context"` rows were
  dropped. Overlapping long-chunk sub-sequences put the same page in several rows, and this is
  how that mistake shows up. Step 08 adds the split-side helpers.

**`evaluate_all`.**

```python
def evaluate_all(y_true, y_pred, scan_ids, sequences, tolerance=1, n_bootstrap=1000, seed=None,
                 *, k=None, k_convention="gaps", sep="/") -> dict:
```

NLP's body is kept verbatim; only `k`/`k_convention` are passed through to the per-inventory
`pk` and `window_diff` calls. The result keys and their shapes are unchanged:
`page_level`, `tolerance`, `pk`, `window_diff`, `segment_exact_match`,
`start_rate_calibration`.

**`corruption/metrics.py`.**

- `_score_binary(t, p)` becomes `tp, fp, fn = page_level_counts(t, p)` then `return _prf1(tp,
  fp, fn)`. Keep `_prf1`, so the floats are bit-identical to before. `score_start_classification`
  and `score_same_document_pairs` keep calling `_score_binary`.
- `window_diff(true_labels, predicted_labels, k=None)` **keeps its strict contract** (user
  decision, 2026-10-14). It raises ValueError on a length mismatch, on n<2, and when not
  `1 <= k <= n_gaps`; when `k is None` it uses `default_k(true_labels, "gaps")`; then it returns
  `segmentation.window_diff(true_labels, predicted_labels, k=k)`. Update its docstring to say
  that it is a strict wrapper and to point to `evaluation.segmentation.window_diff` for NaN and
  clipping semantics.
- Remove `_to_gaps` if nothing uses it any more.
- Behaviour change, for the changelog: non-bool labels now raise `TypeError` in these functions.

## Constraints

- Port verbatim: run splitting, greedy one-to-one tolerance matching (tie-break `(abs(t - p),
  t)`), Pk/WD window comparisons, segment-exact-match truncation, `per_inventory` macro/worst
  (NaN-skipping), and both bootstrap functions with `RandomState`. Don't "fix" anything.
- Replace `print` with `logging`, if any appears. No module-level side effects.
- Imports: numpy, pandas, sklearn only (all core). No torch.
- Don't touch `parsers/` or anything outside the files listed above.
- Never `git commit`; never bump the version; `chmod g+w` new files and the new directory.

## Tests

`tests/test_stream_analysis_evaluation_segmentation.py` and `..._aggregate.py`:

- **The 16 ported NLP tests**, with labels as bools (`"start_page"` → `True`). Keep the names
  and the expected numbers. `test_build_eval_frame_position_and_sort_order` checks `page_id`
  instead of `scanname`. Pass `full_sequences` as `{inv: [page_id, ...]}`.
- **Shared WindowDiff fixture** (parametrised), checked through both
  `corruption.metrics.window_diff` and `evaluation.segmentation.window_diff` with explicit k.
  Both original implementations gave exactly these values on 2026-10-14:

  | case | true | pred | k | WD |
  |---|---|---|---|---|
  | one_off | TFTF | TTFF | 1 | 2/3 |
  | one_off_k2 | TFTF | TTFF | 2 | 0.5 |
  | identical | TFFTFF | TFFTFF | 2 | 0.0 |
  | all_missed | TFFTFFTF | TFFFFFFF | 2 | 2/3 |
  | extra | TFFFFFFF | TFFFTFFF | 3 | 0.6 |
  | mixed | TFTFFFTFFT | TTFFFTFFFT | 2 | 0.375 |
  | mixed_k3 | TFTFFFTFFT | TTFFFTFFFT | 3 | 3/7 |

  Also: `pk(mixed, k=2) == 0.375`.
- **D3:** `default_k` for both conventions on the four checked cases above, and `ValueError`
  for an unknown convention. `window_diff([T,F,T,F], [T,T,F,F])` with no k gives 2/3.
- **D3b:**
  - `pk` and `window_diff` of a 1-element list are NaN.
  - A length mismatch raises ValueError in each function listed above.
  - `window_diff([T,F,T], [T,F,F], k=5)` returns 1.0 with exactly one `WindowClippedWarning`;
    `k=2` returns 1.0 with no warning.
- **P1:** string labels raise TypeError in `pk`, `window_diff`, `page_level_prf`,
  `tolerance_prf`, `segment_exact_match` and `evaluate_all`, and in the two corruption
  wrappers. `[0, 1]` ints raise too; a numpy bool array is accepted.
- **Context guard:** a duplicated scan id in `build_eval_frame` raises ValueError matching
  `"context"`.
- **`evaluate_all` passes `k_convention` through.** On a two-inventory toy example where k
  differs (n=6, S=1), `results["pk"]["per_inventory"]` differs between `"gaps"` and `"pages"`,
  and matches a per-inventory call to `pk`.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_evaluation_segmentation.py tests/test_stream_analysis_evaluation_aggregate.py tests/test_stream_analysis_corruption_metrics.py tests/test_packaging_core_imports.py`,
then the full AS suite: `.venv/bin/python -m pytest -q -p no:cacheprovider`. Expected counts
come from `--collect-only -q`.

## Acceptance criteria

1. 16 ported NLP tests pass with their original expected numbers.
2. All 17 tests in `tests/test_stream_analysis_corruption_metrics.py` pass **unchanged**
   (`git diff --stat tests/test_stream_analysis_corruption_metrics.py` is empty).
3. All 7 shared-fixture cases equal the table values through both wrappers.
4. The packaging test passes with `evaluation` added.
5. **Equivalence with NLP on real data, read-only.** Write a scratchpad script and run it with
   `<NLP>/venv/bin/python`, which sees the live AS tree.
   - Put `<NLP>/src` on `sys.path`. Load NLP's original module with
     `importlib.util.spec_from_file_location("ev_orig", "<NLP>/src/startpage/evaluation.py")`.
   - Read `<NLP>/output/crf_sequence_decoded_scorefix.csv` (20,112 rows; columns
     `split_kind,seed,page_id,y_true,y_pred_crf,y_pred_no_transition`).
   - Take sequences from `startpage.sequence_features.build_full_page_sequences()`, and map
     each page_id to its inventory.
   - For each `(split_kind, seed)` group and both decodes, compare `ev_orig.evaluate_all(strings,
     seed=int(seed))` with AS `evaluate_all(bools, seed=int(seed), k_convention="pages")`.
   - Require deep equality: scalars `==` (NaN equals NaN), DataFrames with
     `pd.testing.assert_frame_equal(check_exact=True)`.
   - Expect 12 equal result sets. Write nothing under `<NLP>/`.
6. Full AS suite green. The baseline is in `STATUS.md` (618 collected); new total = 618 + your
   new tests.

The user decides when the step is done.

## Error analysis

Write these into the phase report:

- **Clipping.** On the criterion-5 data, count the inventories per call in which clipping
  happens. Read 10 clipped runs (inventory, run length, k) and say whether NLP's Pk/WD on short
  dev runs are in effect computed with a smaller window (finding).
- **k conventions.** Compare `default_k` under "gaps" and "pages" for the 22 inventories (true
  labels of the full sequence): in how many do they differ, and by how much (finding)?
- **Sensitivity.** Rerun one CRF group with `k_convention="gaps"`, and report the change in
  `pk.macro` and `window_diff.macro`.

## Do not touch

- NLP (all of it): step 07b.
- `parsers/`, LotA, `ground_truth/`.
- `stream_analysis/__init__.py`.

## When done

1. Fill in `### Step 07: …` in `reports/phase-03-evaluation-for-nlp.md` (it exists, from
   planning; follow `plan/templates/REPORT.md`). Add one row per finding to its *Findings*
   table (ID `F03-07.n`, status `draft`). Don't edit other steps' sections or the
   *Summary*/*Review* sections. Run `make reports` from the AS root and fix any warning.
2. Append the block from `plan/templates/STEP.md` (*When done*) to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: step 07 done; next is 07b (and 08, which may run in
   parallel). Under *Facts later steps need*, record the final public names and signatures of
   `evaluation`, if any differ from this brief.
