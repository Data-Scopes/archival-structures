# Step 12b: NLP `globalise_alignment.py` becomes a shim; alignment outputs reproduced

> **Phase:** 04, Decoders and page numbering. **Depends on:** 11, 12.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 12 ported NLP's alignment core to AS `analysis/page_numbering.py` and the index loader to
`datasets/globalise_index.py`; step 11 added `scan_number`. Here NLP's module keeps every
public name but delegates the generic parts to AS. The gold loaders and scorers stay in NLP.
A verify script reruns `scripts/build_globalise_alignment.py` into a scratch directory and
proves its outputs are byte-identical. Planning (2026-10-14) reran the **current** script
into a scratch directory: **all 9 CSVs were byte-identical** to the stored ones. So any
difference is caused by this step.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`).
**Read `<NLP>/CLAUDE.md` first.** Interpreter `<NLP>/venv/bin/python`.

`<NLP>/src/startpage/globalise_alignment.py` (242 lines; tracked, clean, last commit 93a139a).
Its importers: `scripts/build_globalise_alignment.py` (as `ga`; it uses `Config, load_anchors,
load_spine, load_index, load_gold_starts, align_all, score_start_accuracy, accuracy_summary,
main_chain, extra_runs, numbering_mode, scan_no`, and `INDEX_PATH/ANCHORS_PATH/SPINE_PATH` in
the manifest) and `scripts/error_analysis_4_1b_candidates.py` (`load_gold_starts,
load_anchors, score_start_accuracy`). Nothing in `src/` or `tests/` imports it.

AS API (from steps 11–12; check `STATUS.md` *Facts later steps need* for final names):

```python
from archival_structures.model.scan_id import scan_number            # (scan_id, *, digits=None) -> int
from archival_structures.datasets.globalise_index import GLOBALISE_INDEX_COLUMNS, load_globalise_index
from archival_structures.analysis.page_numbering import (
    CONFIDENCE_BANDS, AlignmentConfig, longest_increasing_chain, main_chain, numbering_restarts,
    numbering_mode, interpolate_folio_to_scan, confidence_band, _locate, align_documents, align_all)
# align_all(index_df, anchors_by_inv, cfg=AlignmentConfig(), max_scans=None, *, inventory_column=...)
#   has no "n_sections" column; NLP's version added out["n_sections"] = np.nan.
```

## The shim (`src/startpage/globalise_alignment.py`)

Keep, unchanged and defined in NLP: `PROJECT_ROOT`, `INDEX_PATH`, `ANCHORS_PATH`, `SPINE_PATH`
(L24–28), `load_anchors`, `load_spine`, `load_gold_starts`, `score_start_accuracy`,
`accuracy_summary`.

Replace with AS aliases (same names, so both scripts keep working):
- `INDEX_COLS = GLOBALISE_INDEX_COLUMNS`, `BANDS = CONFIDENCE_BANDS`, `Config = AlignmentConfig`;
- `scan_no(scan_id)` → `return scan_number(str(scan_id))`;
- `load_index(inventories=None)` → `return load_globalise_index(INDEX_PATH, inventories)`;
- `lis_indices = longest_increasing_chain`, `main_chain`, `extra_runs = numbering_restarts`,
  `numbering_mode`, `_interp = interpolate_folio_to_scan`, `_band = confidence_band`,
  `_locate`, `align_inventory = align_documents`;
- `align_all(index_df, anchors_by_inv, cfg=Config(), max_scans=None)`: call AS, then
  `out["n_sections"] = np.nan`, then return.

Module docstring: "Thin shim over AS `analysis.page_numbering` / `datasets.globalise_index` /
`model.scan_id` (AS phase 04 steps 11–12b). Gold loaders and scorers stay here."

`scan_no` gets stricter: AS rejects ids that don't follow `NL-HaNA_<archive>_<inv>_<NNNN>`,
where the old function accepted anything with an `_`. The verify run proves this on the real
inputs. If it raises, stop and report the offending ids; don't loosen the parse.

Before editing, copy the original into `<NLP>/output/as-phase-04-originals/src/globalise_alignment.py`.
The directory may already exist from step 10b; `chmod g+w` anything new.

## Protected outputs

Hash (sha256 + mtime) before the first run and check after every run:
`<NLP>/output/globalise_alignment/*` (11 files, including `manifest.json` and
`error_analysis_candidates.json`), `<NLP>/output/pagenumber_sequence_features.csv`,
`<NLP>/output/vertical_gap_features.csv`.

How the build script writes (verified 2026-10-14):
- every output goes to the module global `OUT` (L34), read at call time in `main()`. Set
  `m.OUT = <scratch dir>` on the imported module before calling `m.main()`;
- `main()` parses `sys.argv` (`--dev-only`); set `sys.argv = [<name>]`;
- the manifest calls `git rev-parse HEAD` **in the current directory**. Run with `cwd=<NLP>`.
  The planning run from the scratchpad died with `CalledProcessError` at L230, after writing
  the 9 CSVs;
- **don't run or import** `scripts/error_analysis_4_1b_candidates.py`. It is a top-level
  script: importing it executes it, and it writes into `output/globalise_alignment/`. It
  uses only `load_gold_starts`, `load_anchors` and `score_start_accuracy`, which stay in NLP
  unchanged.

## Deliverables

| File | Change |
|---|---|
| `<NLP>/src/startpage/globalise_alignment.py` | shim as above |
| `<NLP>/output/as-phase-04-originals/src/globalise_alignment.py` | copy of the original |
| `<NLP>/scripts/verify_as_globalise_alignment.py` | new (below) |
| AS `docs/shared_library/changelog.md` | add an "NLP switched (step 12b)" bullet under the `### Step 12` entry, with Edit. Also add a bullet under `### Step 11` saying NLP `scan_no` now delegates |
| AS `reports/phase-04-decoders-and-page-numbering.md` | `### Step 12b` section, findings `F04-12b.n` |

## `scripts/verify_as_globalise_alignment.py`

argparse `--out-dir` (required; refuse a path inside `<NLP>/output/`) and
`--mode {current,originals,both}` (default `both`). Each mode runs in a **subprocess** with
`cwd=<NLP>`. Mode `originals` imports the `startpage` package from `<NLP>/src`, then registers
the original copy under `startpage.globalise_alignment` with
`importlib.util.spec_from_file_location` before importing the build script. The build
script is unchanged, so both modes import it from `<NLP>/scripts`. Each mode writes to
`<out-dir>/<mode>/`.

Checks (PASS/FAIL each; exit 0 only if all pass):
- **stored:** the 9 CSVs (`accuracy_by_band_22, coverage_by_band, coverage_local_504,
  document_alignment, inventory_summary, refinement_ablation_dev,
  silver_as_predictor_metrics, silver_starts_unlabelled, validation_22`) in `current/` are
  byte-identical to `<NLP>/output/globalise_alignment/`. `manifest.json` parses equal to the
  stored one after dropping `git_head`;
- **A/B:** with `both`, the 10 files are identical between `current/` and `originals/`
  (manifest likewise without `git_head`);
- the build script's own assertion (L107–108: baseline exact .615, ±1 .873, ±2 .928 within
  0.005) passed. A failure shows as an exception in the subprocess;
- **protected hashes** unchanged.

## Constraints

- Never `git commit`; `chmod g+w` everything new. Don't edit AS code; if AS needs a change,
  stop and ask.
- Don't edit `build_globalise_alignment.py`, the error-analysis scripts, or
  `mine_catchword_pagenum_linespacing.py`. The anchors keep coming from the mined CSV;
  switching to region-table anchors would change the numbers, and step 12 only measured it.
- Don't touch NLP's phase-03 shims or the step-10b files.

## Tests

- `<NLP>/venv/bin/python scripts/verify_as_globalise_alignment.py --out-dir <scratchpad>/verify12b`
  → exit 0 (each mode takes ~1 minute).
- NLP pytest: `<NLP>/venv/bin/python -m pytest -q -p no:cacheprovider tests/test_semi_markov.py tests/test_evaluation.py`
  → 21 passed (nothing imports this module; this guards against collateral damage).
- AS full suite green (count from `STATUS.md`).

## Acceptance criteria

1. Verify script exit 0: **stored 9/9 CSVs byte-identical, manifest equal without `git_head`
   (stored baseline_all22: n 1496, exact 0.6189839572192514), A/B 10/10 identical, protected
   hashes unchanged.**
2. `grep -n "archival_structures" <NLP>/src/startpage/globalise_alignment.py` shows the
   imports; the module no longer defines `lis_indices`/`_interp`/`align_inventory` bodies.
3. NLP 21 passed; AS suite green.

If criterion 1 fails, stop and ask; don't loosen a comparison.

## Error analysis

- The headroom: the build script's tolerance is 0.005, and stored exact 0.6190 sits 0.0040
  above the asserted 0.615. A future change of +0.001 would trip it. Record this as a method
  finding, and say whether the assertion should compare against the manifest value instead
  (a suggestion for the user, not a change).
- From `current/document_alignment.csv` (pandas, `usecols`), tabulate `method` ×
  `confidence` counts (14,827 rows). Read 10 rows whose method ends in `+extrapolated` but
  whose config has `extrap` off: is the suffix only `np.interp`'s end clamping? Keep this
  separate from the step-12 analysis; cite step 12's finding if it covers the same quirk.
- Expected findings: the reproduction (result), the assertion headroom (method), and any id
  that the stricter `scan_no` rejected (data-property; if there are none, say so, with the
  counts of anchor, spine and gold rows parsed).

## Do not touch

- AS code. NLP outputs under *Protected outputs*.

## When done

1. Add `### Step 12b: NLP alignment shim` to `reports/phase-04-decoders-and-page-numbering.md`
   (findings `F04-12b.n`, status `draft`); run `make reports` and fix any warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md` (phase ready for review; NLP working-tree changes listed for
   the user).

```markdown
### Step 12b: NLP alignment shim
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none in AS; NLP names unchanged
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers and runtimes>
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 12b](../../reports/phase-04-decoders-and-page-numbering.md#step-12b-nlp-alignment-shim);
  findings F04-12b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
