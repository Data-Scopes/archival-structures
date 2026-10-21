# Step 17b: `page_duplication` corruption operator

> **Phase:** 08, Region quality and duplicates. **Depends on:** nothing (it doesn't need 17).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to
> explore anyway, record it under *Brief defects*.

## Context

AS's simulation harness corrupts ground-truth page streams at controlled rates. It does this
with four operators: intra-document swap, foreign-document insertion, both combined, and
document-level swap. Real SG inventories also contain **re-shoots**: the same page photographed
twice, the copy right after the original. All 13 documented SG re-shoots are adjacent (phase 08,
step 17). This step adds a fifth operator, `page_duplication`, and the one metric change it needs.
It is new work, not a port; no sibling uses `stream_analysis.corruption` (checked 2026-10-21).

## Inputs

All in AS, checked 2026-10-21:
- `archival_structures/stream_analysis/corruption/operators.py` (229 lines):
  - the module docstring names "the four page-order-problem cases";
  - `ProvenanceEntry` L25–37, `initial_provenance` L40, `_tag` L46;
  - `intra_document_swap` L50–73, `foreign_document_insertion` L76–171, `swap_and_insertion`
    L174–186, `document_level_swap` L189–229.
- `canonical_stream.py`: `StreamUnit` (frozen: `scan_id`, `document_id`, `page_side=None`,
  `bbox=None`); `document_blocks(stream) -> list[tuple[doc_id, start, end]]`.
- `metrics.py`: `true_start_indices` L64, `observed_start_labels` L71–87 (current body below).
- `simulate.py`: `CASE_NAMES` L36–41, `_apply_case` L44–63 (an if-chain that ends in
  `raise ValueError(f'unknown case {case!r}, expected one of {CASE_NAMES}')`).
  `scripts/simulate_corruption.py` L64 uses `choices=CASE_NAMES`, so it needs no edit.
- `__init__.py`: re-exports the four operators and lists them in `__all__`.
- Tests (61 collected):
  - `tests/test_stream_analysis_corruption.py` (38): helper `_make_stream(*lengths)` (doc `d{k}`,
    scan `d{k}_p{i}.jpg`) and `_apply(operator, stream, provenance, rng)`, which calls
    `fn(stream, provenance, 1.0, rng)`. `test_provenance_points_at_the_unit_in_its_position` is
    parametrised over the four operators and asserts `len(provenance) == len(observed) ==
    len(canonical)`;
  - `tests/test_stream_analysis_corruption_metrics.py`: `TestTrueStartIndicesAndObservedStartLabels`
    (L40) has a Case-1 worked example;
  - `tests/test_stream_analysis_corruption_simulate.py`: `test_default_cases_are_all_four`
    (about L99) asserts `{r.case for r in results} == set(CASE_NAMES)`.
- Docs:
  - `docs/challenges/page_order_problems.md`: Cases 1–4 (L30–163), the *Summary* table (L164–176);
  - `docs/challenges/simulation_harness.md`: §2 *Corruption operators* (L89–122) lists the four
    signatures, and §6 *Code location* (L188).

```python
# metrics.py, current
def observed_start_labels(observed, provenance, canonical) -> list[bool]:
    starts = true_start_indices(canonical)
    return [p.canonical_index in starts for p in provenance]
# operators: existing style (no rate validation; one rng.random() draw per decision)
def intra_document_swap(stream, provenance, rate, rng):
    stream = list(stream); provenance = list(provenance)
    for _doc_id, start, end in document_blocks(stream):
        if end - start < 2 or rng.random() >= rate: continue
        ...
```

## Deliverables

| File | Change |
|---|---|
| `corruption/operators.py` | `page_duplication(stream, provenance, rate, rng)`; docstring "Case 5"; the module docstring says five cases |
| `corruption/metrics.py` | `observed_start_labels`: first occurrence only (P3) |
| `corruption/simulate.py` | `'page_duplication'` appended to `CASE_NAMES` (last); a branch in `_apply_case` |
| `corruption/__init__.py` | export `page_duplication` |
| `tests/test_stream_analysis_corruption.py` | `TestPageDuplication` |
| `tests/test_stream_analysis_corruption_metrics.py` | 2 tests |
| `tests/test_stream_analysis_corruption_simulate.py` | rename `test_default_cases_are_all_four` → `test_default_cases_are_all_case_names` |
| `docs/challenges/page_order_problems.md` | `## Case 5 -- page duplication (re-shoot)` before *Summary*; a Summary row |
| `docs/challenges/simulation_harness.md` | §2: the fifth signature and the rate sense; the length change |
| `docs/shared_library/changelog.md` | `### Step 17b: page_duplication operator` under `## Unreleased` (add the heading above `## v0.12.0` if missing), with Edit; affected sibling imports: none |
| `reports/phase-08-region-quality-and-duplicates.md` | `### Step 17b` section, findings `F08-17b.n` |

## Behaviour to implement

```python
def page_duplication(
    stream: list[StreamUnit], provenance: list[ProvenanceEntry], rate: float, rng: random.Random,
) -> tuple[list[StreamUnit], list[ProvenanceEntry]]:
```

- Walk the input in order. For each position `k`:
  1. append `stream[k]` and `provenance[k]` unchanged;
  2. draw `rng.random()` **once**;
  3. if the draw is `< rate`, append the **same** `stream[k]` object again, with
     `_tag(provenance[k], 'page_duplication')`.

  The draw count is always `len(stream)`, so a seed reproduces the run whatever the rate.
- The rate is **per page**, not per document (decision P2): the expected number of copies is
  `rate · n`. Say so in the docstring and in `simulation_harness.md` §3. Other operators use
  per-document rates.
- Only the copy is tagged, so `n_touched` in `simulate.py` counts copies.
- The output is longer than the input: `len(observed) == len(canonical) + n_copies`. Invariant:
  `[canonical[p.canonical_index] for p in provenance] == observed`.
- `rate=0` is a no-op (equal lists). `rate=1` doubles every page. An empty stream returns
  `([], [])`. No rate validation, as in the other four operators.
- It composes with the others (it takes and returns provenance). `document_blocks` on a
  duplicated stream still gives one run per document, because the copies are adjacent.

`observed_start_labels` (P3): label a position True when its `canonical_index` is a true start
**and** that index has not appeared at an earlier observed position. Use a `seen` set. The output
is identical for every existing operator, because none repeats a canonical index. Update the
docstring: a re-shoot copy of a start page is a continuation, not a new document.

## Constraints

- No `page_drop` (deferred, decision P2). No block or offset duplication.
- Don't change the four existing operators or the other metrics.
- Never `git commit`; never bump the version; `chmod g+w` new files. No protected outputs: the
  harness writes only to `tmp_path` in tests. Don't run `scripts/simulate_corruption.py` with its
  default output dir.

## Tests

`TestPageDuplication` in `tests/test_stream_analysis_corruption.py`:
1. `test_rate_zero_is_a_no_op`;
2. `test_rate_one_doubles_every_page`: the order is `p0, p0, p1, p1, …`, and only the copies
   carry `('page_duplication',)`;
3. `test_copy_follows_its_original`: every tagged position `k` has
   `observed[k] is observed[k-1]` and the same `canonical_index`;
4. `test_provenance_invariant` (seeds 0–4, rate 0.3, `_make_stream(4, 3, 5, 2, 6)`): the
   invariant holds, and `len(observed) == len(canonical) + number of tagged entries`;
5. `test_same_seed_same_output`, and `test_draw_count_is_independent_of_rate`: after the
   operator, the next `rng.random()` value is the same for rates 0.1 and 0.9;
6. `test_composes_with_intra_document_swap` (the invariant holds after swap → duplication);
7. `test_document_blocks_stay_one_run_per_document`.

Metrics (`tests/test_stream_analysis_corruption_metrics.py`):
8. `test_observed_start_labels_duplicate_start_is_continuation`: canonical d1 = (c1, c2),
   d2 = (c3); observed = (c1, c1, c2, c3), with provenance indices (0, 0, 1, 2), gives
   `[True, False, False, True]`;
9. `test_observed_start_labels_unchanged_without_repeats`: the Case-1 worked example still gives
   `[False, False, True, False, True, False]`, and so does the existing test.

Simulate: the renamed test passes with five cases. Add `test_page_duplication_case_writes_longer_stream`
(read the YAML back with `read_corrupted_stream`; `len(observed) > n_units` at rate 0.5).

Do **not** add `page_duplication` to the parametrised `test_provenance_points_at_the_unit_in_its_position`.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_stream_analysis_corruption*.py`
(61 before), then the full AS suite (1,123 before phase 08; more if 16 or 17 ran).

## Acceptance criteria

1. The 61 existing corruption tests pass, unchanged except the one rename. The new tests pass.
2. Full AS suite green.
3. Real-data run: `simulate_inventory(Path("data/ground_truth/NL-HaNA_1.01.02/3771.yaml"),
   <new scratchpad dir>, cases=("page_duplication",), rates=(0.1, 0.3, 0.5), seeds=(0, 1, 2))`.
   The file is the one the existing tests use; its `section` stream has 575 units and 3 documents
   (measured 2026-10-21). For each run, `n_copies / 575` lies within ±0.06 of the rate (3 SD).
   Report the 9 values.
4. `make reports` clean after your report section.

The user decides when the step is done.

## Error analysis

For the 9 runs of criterion 3, compare two labellings with `metrics.py`:
- the true labels, from the new `observed_start_labels`;
- the **old rule**, `[p.canonical_index in starts for p in provenance]`, which labels every
  repeat of a start as a start.

Per run, report:
- `n_copies` and `n_copies_of_starts`;
- `score_start_classification(true, old)` (F1);
- `window_diff(true, old)`;
- the share of True values in `true_same_document_pairs`, against the canonical stream.

3771 has only 3 sections, and no local SG file has more than 7 (measured 2026-10-21), so copies
of a start page are rare. Also do the 9 runs on
`ground_truth/NL-HaNA_1.04.02/generale_missiven_overview/converted/NL-HaNA_1.04.02_1363.yaml`
with `layer="document"`: 348 units, 11 documents, the most found at planning.

Read 10 duplicated positions (seed 0, rate 0.3): every copy of a start page, up to 5, and then
middle-page copies up to 10 in all. Confirm each label by hand against the canonical stream.

Findings to expect:
- F08-17b.1: the operator, and the expected copy rate;
- F08-17b.2: how many start labels the first-occurrence rule changes at each rate.

## Do not touch

LotA, NLP, `analysis/`, `text_layer/`, and the four existing operators.

## When done

Follow `plan/templates/STEP.md` *When done*: the report section `### Step 17b: page_duplication
operator` and its findings, then `make reports`; a `PROGRESS.md` block; overwrite `STATUS.md`.
