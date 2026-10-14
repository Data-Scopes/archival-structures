# Phase 03: Evaluation for NLP

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> not read this file. Seeded 2026-10-14 from `docs/shared_library/upstream_inventory.md`; the
> planning session has not run yet.

## Goal

AS has one segmentation-evaluation package, `stream_analysis/evaluation/`, built from NLP's
`evaluation.py` (C6) and merged with AS's `corruption/metrics.py`, plus generic chunk splits
(C7) and trivial baselines (C8). NLP imports these through shims, and **reproduces its stored
numbers exactly**.

## Owns

- **AS:** `stream_analysis/evaluation/` (new: `segmentation.py`, `aggregate.py`, `splits.py`,
  `baselines.py`), `stream_analysis/corruption/metrics.py`, and their tests.
- **NLP:** `src/startpage/evaluation.py`, `long_subsequence_chunk_split.py`, `splits.py`,
  `scripts/run_trivial_baselines.py`, and their tests.

Phase 02 owns `parsers/` and LotA; there is no overlap.

## Sources

The planning session reads these sections, and no others.

- `upstream_inventory.md`:
  - glossary at L183–203;
  - C6 at L270–283;
  - C7 at L285–296;
  - C8 at L298–309;
  - §4 steps 7–9 at L451–453;
  - §6 D3 and D9 at L505–508 and L532–533.
- NLP `CLAUDE.md`, for the leakage and split rules and its test conventions (tests run as
  scripts; there is no pytest config).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 07 | C6 evaluation suite; `corruption/metrics` window-diff and start scoring become wrappers | — | 16 portable NLP tests ported, AS's 16 kept, shared WindowDiff fixture; NLP `output/trivial_baseline_metrics.csv` and one stored `evaluate_all` output identical through the shim | to write |
| 08 | C7 generic splits, held-out builder, public validators, `check_nested_split` | 07 | new tests (none exist today); NLP's cached split CSVs in `output/splits/` rebuilt **byte-identical** | to write |
| 09 | C8 trivial baselines, including the oracle, clearly labelled (D9); the corruption harness reports the floors | 07 (08 if the baselines need splits) | new unit tests; majority Pk/WindowDiff 0.303 and the whole `trivial_baseline_metrics.csv` reproduced | to write |

The planning session may write briefs for 07 only and plan 08–09 after 07 lands, if 07's
as-built API is likely to move. Record that choice in `STATUS.md`.

## Why this order

07 fixes the label and default-k conventions that 08 and 09 use. 08 comes before 09 because
NLP's baseline script takes its splits from `load_or_build_split`.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D3 | AS default `k = round((n−1)/(2S))`; NLP passes its own `round((n/S)/2)` explicitly | AS functions take `k=None` → AS default; NLP's shim or callers pass NLP's k. AS's expected values (the "2/3 at default k=1" case) stay | NLP is the more specific project and overrides AS's generic default; NLP's published numbers stay reproducible |
| D3b | Edge cases: NaN for n<2, raise on length mismatch, clip k to the number of gaps with a warning | Lock these in tests before touching callers | Adopted with D3 |
| P1 | Canonical labels are `list[bool]` "is start"; NLP adapts strings | Validate the input type in AS: a string label must raise, not be truthy | Today bool labels give NLP all-zero gaps, and strings are all truthy in AS |
| D9 | The oracle baseline (each inventory's own start rate) moves into AS | Name it with `oracle` in the identifier and the docstring | It is not a legitimate predictor |
| P2 | Split code stays reproducible | Keep the 10-slot striding and greedy rebalance as they are; offer any correction only as an opt-in | Every NLP number depends on the cached splits |

## Hazards

1. **Reproduction needs NLP's stored outputs and inputs locally.** The planning session checks
   that `output/trivial_baseline_metrics.csv`, `output/splits/`, `output/start_page_position.csv`
   and the inputs of one `evaluate_all` run exist on local disk. If not, it asks the user to
   stage them and names the files.
2. `test_page_level_prf_reproduces_existing_knn_numbers` needs a 60 MB `.npz` and stays in NLP.
3. Callers must drop `role=context` rows before evaluating. The AS API should make that hard to
   forget, e.g. by raising when an `eval_mask` column is present but unapplied. Decide in
   planning.
4. C7: `load_top_of_page_scannames()` is called inside library functions, so it must become a
   parameter. `chunk_split` shares one `random.Random` across inventories, which makes results
   depend on dict order; to stay byte-identical, keep the iteration order explicit. Library
   functions `print`; use logging instead.
5. C8: the length prior needs seed-for-seed equality with `np.random.default_rng`, seeds 42–44.
6. **Leakage** (`CLAUDE.md`): `check_nested_split` encodes the rule that a component model's
   training split must nest inside the downstream split. The review checks that the baselines'
   `train_start_rate` is fitted on train only.

## Open questions for planning

- Which stored `evaluate_all` output is the reproduction target for 07? Pick one that can be
  recomputed locally in minutes.
- Is NLP's `pyproject.toml` (step 1) installed, so that `startpage` imports without `sys.path`
  inserts? Shims depend on it.
- Do the line numbers in C6–C8 still hold (dated 2026-10-13)?
- How do the NLP tests run (script or pytest), and what command does the shim acceptance use?

## Review checklist

- [ ] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, re-run by the reviewer.
- [ ] AS suite and NLP tests green.
- [ ] NLP reproductions are byte-identical or exactly equal, checked by the reviewer with a diff.
- [ ] D3: AS default unchanged in AS tests; NLP's k passed explicitly everywhere a published
      number is produced.
- [ ] String labels raise in AS (P1).
- [ ] No VOC constants in AS core (held-out set, coverage gaps, scan-name grammar).
- [ ] Leakage: `train_start_rate` on train only; `check_nested_split` tested.
- [ ] `changelog.md` has steps 07–09, with NLP imports listed.
- [ ] `reports/phase-03-evaluation-for-nlp.md` final (one section per step with its error
      analysis; findings confirmed); findings appended to `reports/index.md`; `make reports`
      clean. The planning session creates the report skeleton (prompt 1, task 5).
- [ ] `Brief defects` entries answered.

## Hand-off

<Filled in at review. Phase 04 (decoders) builds on the conventions settled here.>
