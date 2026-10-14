# Step 07b: NLP `evaluation.py` becomes a shim over AS; stored numbers reproduced

> **Phase:** 03, Evaluation for NLP. **Depends on:** 07.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 07 built `archival_structures.stream_analysis.evaluation`, which takes bool labels and
defaults to AS's k. NLP has 21 importers of `startpage.evaluation`, and they all pass string
labels (`"start_page"` / `"not_start_page"`) and expect NLP's k. In this step NLP's module
becomes a shim. It converts labels, passes `k_convention="pages"` (decision D3: NLP overrides
AS's default explicitly), and keeps every public name. The importers don't change. A
verification script then proves that the stored numbers are reproduced exactly through the
shim.

## Inputs

NLP root: `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`). **Read
`<NLP>/CLAUDE.md` first**; its rules apply to files there. Interpreter: `<NLP>/venv/bin/python`.
It uses system site-packages, and `archival_structures` resolves to the live AS tree (an
editable install). `startpage` is **not** installed: scripts and tests put `<NLP>/src` on
`sys.path` themselves.

**Precondition:** NLP pytest must import. Before 2026-10-14 it failed with
`ModuleNotFoundError: No module named 'pygments'` for the `claude-agent` account. If
`<NLP>/venv/bin/python -m pytest --version` still fails, stop and ask the user (STATUS.md has
the fix).

Git state (2026-10-14):
- `src/startpage/evaluation.py` is tracked and clean (last commit 863b3a5).
- **`tests/`, `scripts/run_trivial_baselines.py`, `output/splits/` and the output CSVs are
  untracked.**

Before editing anything, copy these into a new directory `<NLP>/output/as-phase-03-originals/`
(`chmod g+w` it):
- `src/startpage/evaluation.py` → `evaluation.py`
- `tests/test_evaluation.py`
- `scripts/run_trivial_baselines.py`

Step 09 reuses these copies.

The current module (`src/startpage/evaluation.py`, 514 lines) exports these public names:
`START`, `build_eval_frame`, `page_level_prf`, `page_level_prf_with_counts`,
`iter_contiguous_runs`, `tolerance_prf`, `default_k`, `pk`, `window_diff`,
`segment_exact_match`, `PerInventoryResult`, `per_inventory`, `start_rate_calibration`,
`bootstrap_ci`, `bootstrap_pooled_f1`, `evaluate_all`, `format_report`. It also has the private
`_tolerance_prf_runs`, which `evaluate_all` uses. Its importers (scripts) only use
`evaluate_all`, `format_report`, `bootstrap_ci`, `page_level_prf` and `START`. The tests use
`ev.<name>` for most of the names above.

`full_sequences` (from `startpage.sequence_features.build_full_page_sequences()`) is
`{inventory: [(scanname, label, page_type), ...]}` in physical order, built from the 22
`annotations/*.csv` (20,905 pages).

AS API from step 07 (check `STATUS.md` *Facts later steps need* for any deviation):

```python
from archival_structures.stream_analysis import evaluation as asev
asev.check_labels(labels, name="labels") -> list[bool]          # TypeError on non-bool
asev.build_eval_frame(y_true, y_pred, scan_ids, sequences, sep="/")   # sequences: {inv: [page_id]}; column page_id
asev.page_level_prf(y_true, y_pred); asev.page_level_prf_with_counts(y_true, y_pred)
asev.iter_contiguous_runs(true_labels, pred_labels, positions, is_last_page=None)
asev.tolerance_prf(y_true, y_pred, sequence, tolerance=1, positions=None)
asev.tolerance_prf_runs(true_labels, pred_labels, positions=None, is_last_page=None, tolerance=1)
asev.default_k(true_labels, convention="gaps")
asev.pk(true_labels, pred_labels, k=None, positions=None, is_last_page=None, *, k_convention="gaps")
asev.window_diff(...same...)
asev.segment_exact_match(true_labels, pred_labels, positions=None, is_last_page=None)
asev.PerInventoryResult; asev.per_inventory(metric_fn, eval_frame, worst_column=None,
    higher_is_better=True, pass_position_info=False, **kwargs)
asev.start_rate_calibration(eval_frame); asev.bootstrap_ci(...); asev.bootstrap_pooled_f1(...)
asev.evaluate_all(y_true, y_pred, scan_ids, sequences, tolerance=1, n_bootstrap=1000, seed=None,
    *, k=None, k_convention="gaps", sep="/")
asev.format_report(results)
```

Reproduction targets (all local, checked 2026-10-14):
- **CRF:** `<NLP>/output/crf_sequence_decoded_scorefix.csv`
  - 20,112 rows; columns `split_kind,seed,page_id,y_true,y_pred_crf,y_pred_no_transition`.
  - Groups: `long_chunk` seeds 0, 1, 2 (3,090 rows each) and `inventory_held_out` seeds 42, 43,
    44 (3,614 rows each).
  - It is scored against `<NLP>/output/crf_sequence_metrics_scorefix.csv`: 12 rows, key
    `(split_kind, eval_split, decode, seed)` with `decode ∈ {crf, no_transition}`; `eval_split`
    is `dev` for long_chunk and `test` for inventory_held_out.
  - Its metric columns are produced by `flatten_metrics` in `<NLP>/scripts/train_crf_sequence.py`
    L296–312: copy that function into your verification script (it is pure). The script itself
    imports torch; don't import it.
  - The producer called `evaluate_all(y_true, y_pred, scan_ids, full_sequences, seed=seed)`,
    with `scan_id = f"{inventory}/{page_id}"`, in CSV row order.
  - A read-only check on 2026-10-14 with NLP's original code reproduced all 12 rows exactly,
    including the CIs.
- **Trivial baselines:** `<NLP>/output/trivial_baseline_metrics.csv`
  - 12 rows, 30 columns, from `scripts/run_trivial_baselines.py`. Its `main()` writes to the
    module constant `RESULTS_CSV_PATH`; set it to a scratchpad path before calling `main()`.
  - **The bootstrap of the `every_kth_corpus_rate` and `every_kth_own_rate_ORACLE` rows ran with
    `seed=None`**, so their four columns `f1_macro_ci_lo, f1_macro_ci_hi, f1_pooled_ci_lo,
    f1_pooled_ci_hi` are not reproducible: 4 rows × 4 columns = 16 cells (user decision:
    exclude them and leave NLP's script as is). Every other cell is deterministic. The majority
    long_chunk/dev row has `pk_macro = window_diff_macro = 0.3032799364442644`.
  - `main()` loads cached splits via `load_or_build_split`. It must not rebuild them: record
    the sha256 of every file in `<NLP>/output/splits/` before and after, and assert that nothing
    changed.

## Deliverables

| File | Change |
|---|---|
| `<NLP>/src/startpage/evaluation.py` | replaced by the shim (below) |
| `<NLP>/scripts/verify_as_evaluation_shim.py` | new; the reproduction check (below) |
| `<NLP>/output/as-phase-03-originals/` | new; the three original copies |
| `<NLP>/tests/test_evaluation.py` | **unchanged** (user decision: all 17 tests run against the shim) |
| `docs/shared_library/changelog.md` (AS) | append to the `### Step 7` entry, with Edit, one paragraph: NLP's `startpage.evaluation` is a shim, with the 17 names listed above |
| `reports/phase-03-evaluation-for-nlp.md` (AS) | `### Step 07b` section and draft findings `F03-07b.n` |

## Behaviour to implement

The shim imports from `archival_structures.stream_analysis.evaluation` and defines:

- `START = "start_page"`.
- `_to_bool(labels)`: per element, a `bool`/`numpy.bool_` stays as it is, and a `str` becomes
  `l == START`. Anything else raises `TypeError`. It must accept bools, because NLP's own
  `per_inventory` tests pass frames that the shim already converted.
- `_page_ids(full_sequences)` gives `{inv: [t[0] if isinstance(t, (tuple, list)) else t for t
  in seq]}`.
- A wrapper per label-taking function that calls `_to_bool` on the label arguments and
  forwards the rest:
  - `page_level_prf`, `page_level_prf_with_counts`, `tolerance_prf`, `_tolerance_prf_runs`
    (→ `tolerance_prf_runs`) and `segment_exact_match`;
  - `pk`, `window_diff` and `default_k`, which also default to `k_convention="pages"` /
    `convention="pages"`;
  - `evaluate_all(y_true, y_pred, scan_ids, full_sequences, tolerance=1, n_bootstrap=1000,
    seed=None)`, which forwards `sequences=_page_ids(full_sequences)` and
    `k_convention="pages"`.
- `build_eval_frame(y_true, y_pred, scan_ids, full_sequences)` converts and forwards, then
  renames `page_id` → `scanname` (NLP's column name).
- `start_rate_calibration(eval_frame)` converts `y_true`/`y_pred` on a copy, then forwards.
- These are re-exported unchanged: `iter_contiguous_runs`, `PerInventoryResult`,
  `per_inventory`, `bootstrap_ci`, `bootstrap_pooled_f1`, `format_report`.
- A module docstring that says the logic lives in AS, that labels are adapted, and that NLP's
  k convention is passed explicitly (D3).

`scripts/verify_as_evaluation_shim.py` (argparse, `--scratch DIR` for temporary output, which
defaults to a new temp dir; exit code 0 only if all checks pass):

1. Load the original module from `output/as-phase-03-originals/evaluation.py` as `ev_orig`
   (with `importlib`) and the shim as `startpage.evaluation`.
2. **CRF check**, for both implementations: recompute the 12 rows and compare them with the
   stored metrics CSV. Floats must match exactly (`==`, or both NaN); the
   `f1_worst_inventory` column is compared as `str`.
3. **Trivial-baseline check**, for both implementations: import
   `scripts/run_trivial_baselines.py` with `importlib`, then set its `RESULTS_CSV_PATH` to the
   scratch dir. For the original, also set the module's `evaluate_all`/`START` to `ev_orig`'s.
   Run `main()`, read the result and the stored CSV with
   `pd.read_csv(..., dtype={"f1_worst_inventory": str, "tol1_f1_worst_inventory": str,
   "pk_worst_inventory": str})`, and compare every cell except the 16 excluded ones. Assert
   that `output/splits/` is unchanged (sha256).
4. **A/B with seeds:** for the long_chunk/dev cell, take the predictions of all 4 baseline
   methods (from the module's `predictions_*` functions) and call `ev_orig.evaluate_all` and
   the shim's `evaluate_all` with `seed=0, n_bootstrap=200`. Assert deep equality of the result
   dicts (DataFrames with `check_exact=True`). This covers the 16 excluded cells.
5. Print one line per check (`OK`/`FAIL`, with counts of compared cells) and a final summary.

## Constraints

- Before editing, run checks 2 and 3 with the **original** module only (the script supports
  `--original-only`), and record that both pass. If they don't, the stored targets are stale:
  stop and ask the user.
- Don't edit any of the 21 importers or any other NLP module.
- Don't write under `<NLP>/output/` except the originals directory. Never overwrite
  `trivial_baseline_metrics.csv` or any split file.
- Never `git commit`; `chmod g+w` new files and directories.

## Tests

- NLP: `cd <NLP> && venv/bin/python -m pytest -q -p no:cacheprovider tests/test_evaluation.py`
  → 17 passed. This includes `test_page_level_prf_reproduces_existing_knn_numbers`, which
  trains a kNN on `output/embeddings/embeddings_paragraph_only.npz` (60 MB, local) and may take
  a few minutes. If it fails with the original module too, record that and continue.
- AS: the step-07 test files still pass (`.venv/bin/python -m pytest -q -p no:cacheprovider
  tests/test_stream_analysis_evaluation_segmentation.py tests/test_stream_analysis_evaluation_aggregate.py`).

## Acceptance criteria

1. `verify_as_evaluation_shim.py` exits 0, and its output shows:
   - CRF: 12 rows × all metric columns equal, for both implementations;
   - trivial baselines: 12 rows × 30 columns minus 16 cells equal, for both;
   - A/B: 4 methods with deep equality;
   - split cache unchanged.
2. NLP `tests/test_evaluation.py`: 17 passed through the shim, with the file unchanged
   (compare with the original copy using `cmp`).
3. `grep -n "def \|^START" src/startpage/evaluation.py` shows every public name in the list
   above.
4. AS suite green.

## Error analysis

- `import warnings; warnings.simplefilter("always", WindowClippedWarning)` in one CRF
  long_chunk run. Count the warnings, read 5, and confirm that they are expected clipping on
  short runs, not a k bug.
- Find the label values in `full_sequences` (`collections.Counter`). The shim assumes
  `{"start_page", "not_start_page"}`, and every other string counts as negative. Report any
  other value and the number of pages that have it (finding).
- Diff the CRF stored metrics against a rerun with `k_convention="gaps"` for 2 rows. This
  shows what the D3 override protects (one sentence, if step 07 didn't already report it).

## Do not touch

- AS code (step 07 owns it; report any bug you find in it under *Brief defects* and stop if it
  blocks you).
- NLP split modules (step 08) and `run_trivial_baselines.py` (step 09).

## When done

1. Fill in `### Step 07b: …` in `reports/phase-03-evaluation-for-nlp.md` (follow
   `plan/templates/REPORT.md`), with draft findings `F03-07b.n`. Run `make reports` from the AS
   root and fix any warning.
2. Append the `plan/templates/STEP.md` *When done* block to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: 07b done; record under *Facts later steps need* the
   originals path and the verify-script command.
