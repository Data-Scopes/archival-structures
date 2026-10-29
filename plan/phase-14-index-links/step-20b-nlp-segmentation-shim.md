# Step 20b: NLP switches to the phase-11 ports

> **Phase:** 14, Index → resolution links. **Depends on:** 20a (part 6 done: both sibling venvs on pagexml-tools 0.9.1).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

AS phase 11 ported two NLP modules with parity checks: C10 (DP and TextTiling segmentation) into
`archival_structures/stream_analysis/overview/subsequence_detection.py` and C12 (separator sheets) into
`archival_structures/analysis/separator_sheets.py`. NLP still runs its own copies. UA-032 (resolved 2026-10-27) installed
AS editable into NLP's venv, so NLP can now import the ports. This step turns the two NLP modules into thin re-exports,
keeps every original name, saves the originals, and proves with an A/B script that NLP's results do not change. AS code
does not change.

## Inputs

- **Read first:** `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/CLAUDE.md`. Its rules apply in
  NLP: never commit, `chmod g+w`, outputs under `output/`, never fill a UA Response. Below, `NLP` = that root.
- NLP test command (from `NLP`): `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests` (66 passed
  on 2026-10-27). NLP's venv imports the live AS tree (editable; `direct_url.json` points at the AS root).
- `NLP/src/startpage/unsupervised_segmentation.py` (327 lines). Imports: `dataclass`, `math.log`, `typing` (`Dict, List,
  Optional, Sequence, Tuple`), `numpy as np`, `pandas as pd`. Names:
  - **ported** (take them from AS): `MAX_SEGMENT_LENGTH` (L46, 200), `_geometric_length_penalty` (L86), `segment_dp`
    (L92), `calibrate_length_penalty_weight` (L167), `textiling_depth_scores` (L201), `predicted_start_rate` (L284);
  - **stay in NLP, verbatim**: `build_similarity_sequences` (L49), `ChangePointResult` (L252), `boundaries_to_page_results`
    (L260), `run_unsupervised_segmentation` (L288; calls `segment_dp`, `textiling_depth_scores`, `predicted_start_rate`).
  - The module docstring (L1–38) stays, plus one paragraph saying which names now come from AS.
- AS counterparts (signatures identical to NLP's; only type hints differ): `subsequence_detection.py` `MAX_SEGMENT_LENGTH`
  (L479, 200), `_geometric_length_penalty` (L482), `segment_dp` (L488), `calibrate_length_penalty_weight` (L567),
  `textiling_depth_scores` (L601), `predicted_start_rate` (L656). Parity (AS F11-18b.1): equal on NLP's 22 VOC sequences,
  weight 0.0293559853524414, 41,810 / 41,810 change-point rows. Quirks kept on purpose (zero-pad edge starts, F11-R.2).
- `NLP/src/startpage/separator_sheets.py` (64 lines): `_SHEET_RX` (L25), `MAX_SHEET_CHARS = 12` (L26),
  `is_separator_sheet(text) -> bool` (L29), `detect_sheets(page_texts) -> set` (L36), `_scan_no(page_id) -> int` (L41),
  `apply_rule(df, sheets, convention, threshold=0.5) -> np.ndarray` (L45). Docstring L1–19 stays.
- AS `analysis/separator_sheets.py`: `VOC_SHEET_PATTERN` (compiled, the same regex as `_SHEET_RX`, `re.I`),
  `VOC_MAX_SHEET_CHARS = 12`, `is_separator_sheet(text, *, pattern=VOC_SHEET_PATTERN, max_chars=VOC_MAX_SHEET_CHARS)`,
  `detect_sheets(page_texts, *, pattern, max_chars, inventory_col="inventory_number", page_col="page_id",
  text_col="text") -> set[tuple[str, str]]`, `next_scan_id(page_id) -> Optional[str]`, `apply_rule(df, sheets,
  convention, threshold=0.5, *, next_page_id=next_scan_id, inventory_col="inventory_number", page_col="page_id",
  prob_col="pred_prob")`. Parity (F11-18c.1): 20,912 VOC pages, 169 sheets, 0 mismatches. Deliberate differences: an
  unknown convention raises `ValueError` (NLP treated it as "next"; NLP's scripts only pass none/sheet/next or a dict of
  sheet/next, F11-18c.3); `sorted(sheets)` order (F11-R.3; 0 consecutive sheets in the 22 inventories).
- NLP importers (no test imports either module): `scripts/evaluate_unsupervised_segmentation.py:43`,
  `scripts/evaluate_generale_missiven.py:45`, `scripts/evaluate_cross_encoder.py:52` (via the first),
  `scripts/count_separator_sheets.py:14`, `scripts/evaluate_separator_sheet_rule.py:22`,
  `scripts/evaluate_separator_convention_inference.py:24`.
- Parity scripts from phase 11 (AS root, read only):
  `reports/data/phase-11-text-extent-and-nlp-ports/step-18b/compare_nlp.py` (146 lines; `import
  startpage.unsupervised_segmentation as N` vs AS; reads NLP `embeddings_paragraph_only.npz`,
  `output/unsupervised_changepoints.csv`, `output/early_end/page_table.csv`, `output/early_end/page_text.csv.gz`; about
  1 min plus two 25 s calibrations) and `step-18c/compare_nlp.py` (64 lines; `from startpage import separator_sheets as
  NL`, `--page-text` default `NLP/output/early_end/page_text.csv.gz`). Usage: `cd NLP && PYTHONNOUSERSITE=1
  PYTHONPATH=<AS root> venv/bin/python <script> --out-dir <dir>`.
- Shim precedent: `NLP/src/startpage/semi_markov.py` (a docstring naming the AS module and the originals path, then
  `from archival_structures… import (  # noqa: F401 …)`); A/B precedent `NLP/scripts/verify_as_decoders.py` (modes
  `current` / `originals` / `both` in separate subprocesses; originals loaded from `output/as-phase-04-originals/` under the
  `startpage.*` names). Read both.

## Deliverables

| File | Change |
|---|---|
| `NLP/output/as-phase-14-originals/src/{unsupervised_segmentation,separator_sheets}.py` | byte copies before any edit (`cp -p`; `chmod -R g+w`) |
| `NLP/src/startpage/unsupervised_segmentation.py` | re-exports the six ported names from AS; the four NLP-only names verbatim |
| `NLP/src/startpage/separator_sheets.py` | re-exports `is_separator_sheet`, `detect_sheets`, `apply_rule` from AS; `_SHEET_RX = VOC_SHEET_PATTERN`, `MAX_SHEET_CHARS = VOC_MAX_SHEET_CHARS`, `_scan_no` verbatim |
| `NLP/scripts/verify_as_segmentation_shim.py` | A/B (below) |
| `reports/data/phase-14-index-links/step-20b/` | run logs and the verify output summary |
| `docs/shared_library/changelog.md` | `### Step 20b: NLP shims for C10/C12` under *Unreleased* (Edit): AS API unchanged; NLP imports now from AS |
| `reports/phase-14-index-links.md` | `### Step 20b: …` section, draft findings `F14-20b.n` |

## Behaviour to implement

1. **Before editing**, run both phase-11 parity scripts once (out dirs `reports/data/phase-14-index-links/step-20b/
   pre18b/`, `pre18c/`). They must reproduce F11-18b.1 and F11-18c.1 (0 mismatches; weight 0.0293559853524414). If not,
   stop and report.
2. Copy the originals, then write the two shims. In `unsupervised_segmentation.py` the NLP-only functions keep their
   bodies and resolve `segment_dp` etc. to the AS names imported at module top. Keep the module's other imports only if
   still used.
3. `verify_as_segmentation_shim.py --out-dir D [--mode current|originals|both]` (default `both`), modelled on
   `verify_as_decoders.py`: each mode in a subprocess; `originals` loads the two copies from
   `output/as-phase-14-originals/src/` under the names `startpage.unsupervised_segmentation` /
   `startpage.separator_sheets` (`importlib.util.spec_from_file_location` + `sys.modules`). Each mode computes, on the
   inputs of the phase-11 scripts: (a) `calibrate_length_penalty_weight` on the 22 VOC similarity sequences; (b)
   `run_unsupervised_segmentation` with that weight (the change-point DataFrame); (c) `detect_sheets` on the page texts
   and `apply_rule` for conventions `none`, `sheet`, `next` and the per-inventory dict used by
   `scripts/evaluate_separator_convention_inference.py`, thresholds 0.3/0.5/0.7, on a fixed probability column (copy
   how `step-18c/compare_nlp.py` builds it). Writes `D/<mode>/weight.txt`, `changepoints.csv`, `sheets.tsv`,
   `rule_<conv>_<thr>.npy`; with `both`, compares and prints PASS/FAIL per check; exit 0 only if all pass. Nothing is
   written under `output/` except by the user. Run it with `--out-dir reports/data/phase-14-index-links/step-20b/verify`.
4. Run NLP's suite. Do not run NLP's evaluation scripts with their default outputs.

## Constraints

- No AS code change. If a shim needs a behaviour AS lacks, stop and report.
- NLP files: only the three named plus the originals dir. No deprecation warnings (precedent).
- Never `git commit`; `chmod g+w` every new file and dir; zsh; no `rm -rf`, `nohup`, `&`; Edit for shared files (20c
  runs in parallel). Single process (the verify script is small; no coreshare needed).

## Tests

- NLP: `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests` → 66 passed (no tests added).
- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_subsequence_detection_unsupervised.py
  tests/test_analysis_separator_sheets.py` → 25 passed.

## Acceptance criteria

1. Pre-edit parity reproduces F11-18b.1 and F11-18c.1.
2. Verify `both`: weight equal (same float repr), change-point frames equal (`DataFrame.equals` after the same sort;
   41,810 rows), sheet sets equal (169), every `apply_rule` array equal; exit 0.
3. Every original public and private name still importable from the NLP modules (`python -c` import of each).
4. NLP suite 66 passed; the two AS test files 25 passed.

The user decides when the step is done.

## Error analysis

If any check fails, read every differing row (up to 30) and classify (order, dtype, real difference). If all pass,
state it and list the names that now come from AS. Draft finding: the switch with its counts.

## Do not touch

AS `archival_structures/`, every NLP file not named above, NLP `output/` except the originals dir, the external drive.

## When done

1. Report: `### Step 20b: NLP switches to the phase-11 ports` in `reports/phase-14-index-links.md` (draft findings
   `F14-20b.n`); `make reports` clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 20b: NLP switches to the phase-11 ports
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none in AS; NLP modules re-export AS names
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-14-index-links.md` § Step 20b](../../reports/phase-14-index-links.md#step-20b-nlp-switches-to-the-phase-11-ports); findings F14-20b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```

3. Overwrite the phase `STATUS.md`.
