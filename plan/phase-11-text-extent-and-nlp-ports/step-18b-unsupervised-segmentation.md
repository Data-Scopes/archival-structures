# Step 18b: C10, DP and TextTiling segmentation

> **Phase:** 11, Text extent options and NLP ports. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP's label-free segmenters (a dynamic-programming segmenter under a geometric length prior, and a Hearst-style
TextTiling depth score) are generic sequence tools. They move into AS next to the existing similarity-based boundary
detectors (inventory item C10). NLP found both score **below** the majority baseline for VOC document starts, so AS
documents them as section-level tools (step 18d writes that up). This step ports the code verbatim, adds the tests NLP
never had, and shows AS = NLP on NLP's own 22 similarity sequences. NLP is not edited (its AS install is a
non-editable 0.13.0 copy; the shim comes later).

**Parallel run:** steps 18a and 18c run at the same time in the same working tree. Edit shared files only with Edit,
only at your own placeholder (below), and in `STATUS.md` only your own row of the *Steps* table. Don't overwrite
`STATUS.md`.

## Inputs

- NLP root `NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (read only; never write there,
  never `pip install` into its venv). Source `$NLP/src/startpage/unsupervised_segmentation.py` (327 lines, checked
  2026-10-27). **Read exactly these ranges** and port them:
  - L1–38 module docstring (the rationale, incl. the "label-free except for two corpus-level constants" caveat L8–18);
  - L39–46 imports and `MAX_SEGMENT_LENGTH = 200`;
  - L86–89 `_geometric_length_penalty(length: int, prior_mean_length: float) -> float`;
  - L92–164 `segment_dp(similarities: np.ndarray, prior_mean_length: float = 4.0, length_penalty_weight: float = 1.0,
    max_segment_length: int = MAX_SEGMENT_LENGTH) -> List[int]`;
  - L167–198 `calibrate_length_penalty_weight(similarities_by_inventory: Dict[str, np.ndarray], prior_mean_length:
    float = 4.0, target_mean_segment_length: float = 4.0, candidates: Sequence[float] = tuple(np.geomspace(0.02, 2.0,
    25))) -> float`;
  - L201–248 `textiling_depth_scores(similarities: np.ndarray, window: int = 3, depth_threshold: Optional[float] =
    None) -> Tuple[np.ndarray, List[int]]`;
  - L284–285 `predicted_start_rate(boundaries: List[int], n_pages: int) -> float`.

  Not ported (NLP-coupled): `build_similarity_sequences` L49–83, `ChangePointResult` L251–257,
  `boundaries_to_page_results` L260–281, `run_unsupervised_segmentation` L288–327.
- Convention: `similarities[i]` is the similarity between pages i and i+1 (length n_pages − 1, NaN allowed). Both
  `segment_dp` and `textiling_depth_scores` return **sorted segment start indices that always include 0**. Quirks to
  keep (decision P2, verbatim port): `segment_dp`'s `return [0] if n_pages == 1 else []` (the `[]` branch is
  unreachable); `textiling_depth_scores` on an empty array returns `(np.array([]), [0])` by operator precedence and emits
  a numpy RuntimeWarning from `np.nanmean`; `np.convolve(mode="same")` zero-pads the edges; `calibrate…` ties go to the
  first candidate (strict `<`).
- AS destination `archival_structures/stream_analysis/overview/subsequence_detection.py` (448 lines). Its imports
  (L37–46): `from __future__ import annotations`, `logging`, `Optional, Sequence`, `numpy as np`, `pandas as pd`,
  `scipy.ndimage.gaussian_filter1d`, `logger`. Its own convention is **gap indices**: `detect_boundaries_threshold`
  (L106) and `detect_boundaries_changepoint` (L150) return "boundary at index i = split between scan i and i+1", and
  `boundaries_to_segments(n_scans: int, boundaries: list[int]) -> list[tuple[int, int]]` (L196) turns them into
  `(start, end)` pairs: `starts = [0] + [b + 1 for b in boundaries]`. Section banners are `# ----…` comment blocks
  (e.g. L102–104 `# Boundary detection`).
- `archival_structures/stream_analysis/__init__.py` has a `_LAZY_EXPORTS` dict (L11 on); its
  `# overview.subsequence_detection` block (L46–54) maps each public name to
  `"archival_structures.stream_analysis.overview.subsequence_detection"`.
- Data for the equivalence check (measured 2026-10-27 at planning, with NLP's interpreter): the 22 similarity sequences
  come from
  ```python
  from startpage.config import EMBEDDINGS_DIR
  from startpage.sequence_features import build_full_page_sequences
  from startpage.unsupervised_segmentation import build_similarity_sequences
  z = np.load(EMBEDDINGS_DIR / "embeddings_paragraph_only.npz", allow_pickle=True)
  sims = build_similarity_sequences(z["text_embeddings"], z["scan_ids"].tolist(), build_full_page_sequences())
  ```
  22 inventories, 20,905 pages, 0.1 s. NLP's `calibrate_length_penalty_weight(sims, target_mean_segment_length=4.0)`
  returns `0.0293559853524414` in 23.5 s. With that weight, `segment_dp(s, 4.0, w)` and `textiling_depth_scores(s, 3)`
  reproduce the stored `$NLP/output/unsupervised_changepoints.csv` (columns `inventory_number, page_id, position,
  method ∈ {dp, textiling}, is_change_point, …`; a page is a change point iff its `position` is in the start list):
  **41,810 of 41,810 rows agree**.
- Command pattern (checked; imports the AS working tree):
  `cd $NLP && PYTHONNOUSERSITE=1 PYTHONPATH=/Users/marijnkoolen/Code/Huygens/archival-structures venv/bin/python <script>`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/stream_analysis/overview/subsequence_detection.py` | new section `# Label-free segmentation (DP, TextTiling; ported from NLP)` with the ported functions and two converters; module docstring gains one paragraph |
| `archival_structures/stream_analysis/__init__.py` | `_LAZY_EXPORTS` entries for the new public names (append to the subsequence block) |
| `tests/test_subsequence_detection_unsupervised.py` | new tests (below) |
| `reports/data/phase-11-text-extent-and-nlp-ports/step-18b/compare_nlp.py` | the equivalence script; its output `compare.tsv` and `run.log` in the same dir |
| `docs/shared_library/changelog.md` | replace the line `<!-- 18b -->` under `## Unreleased` with `### Step 18b: …` |
| `reports/phase-11-text-extent-and-nlp-ports.md` | fill `### Step 18b: …`; findings `F11-18b.n` |

## Behaviour to implement

- Public names: `MAX_SEGMENT_LENGTH`, `segment_dp`, `calibrate_length_penalty_weight`, `textiling_depth_scores`,
  `predicted_start_rate`, `starts_to_boundaries`, `boundaries_to_starts`; private `_geometric_length_penalty`.
- Function bodies verbatim from NLP. Docstrings: keep NLP's content (it is the rationale; README rule "ports carry
  their rationale"), but replace NLP-internal references ("CLAUDE.md", "report/46", "Step 1.6", "every supervised
  baseline here") with a neutral wording plus one line "Ported from NLP `src/startpage/unsupervised_segmentation.py`
  (AS phase 11, step 18b)". Each of `segment_dp` and `textiling_depth_scores` states in its first docstring line:
  "Returns segment **start** indices (always including 0), not the gap indices of `detect_boundaries_*`; convert with
  `starts_to_boundaries`." Add a sentence: NLP found both below the majority-class baseline for VOC document starts;
  treat them as section-level tools.
- Converters (new):
  ```python
  def starts_to_boundaries(starts: Sequence[int]) -> list[int]:
      """Segment start indices (incl. 0) -> gap indices: [s - 1 for s in sorted(set(starts)) if s > 0]."""
  def boundaries_to_starts(boundaries: Sequence[int]) -> list[int]:
      """Gap indices -> segment start indices: [0] + [b + 1 for b in sorted(set(boundaries))]."""
  ```
  So `boundaries_to_segments(n, starts_to_boundaries(segment_dp(sims)))` gives the DP's segments.
- Type hints: the module uses `list[int]` with `from __future__ import annotations`; keep NLP's `List`/`Dict`/`Tuple`
  hints in ported signatures only if you import them, otherwise switch to builtins (no behaviour change).
- Add `from math import log` (or use `math.log`); no new dependency.

## Constraints

- Verbatim behaviour: no bug fixes, no vectorisation, no changed defaults (the NLP shim must give identical outputs).
- Don't change existing functions in `subsequence_detection.py`.
- Never write under `$NLP`; never `git commit`; never bump the version; `chmod g+w` every new file and directory.
- Agent shell: no `rm -rf`, `nohup` or `&`; zsh. The comparison is single-process (no coreshare needed).
- Don't touch `text_extent.py`, `separator_sheets.py`, `docs/`, `text_layer/`, `analysis/hand_print.py`.

## Tests (`tests/test_subsequence_detection_unsupervised.py`)

- `_geometric_length_penalty`: equals `-log((1-p)**(L-1) * p)` for p = 1/mean, several L.
- `segment_dp` optimality: for 12 seeded random similarity arrays with n_pages 1–8, compare with a brute force over all
  2^(n−1) segmentations using the same centred score (median of finite sims, NaN → baseline) and penalty; the DP's
  score equals the brute-force max (`math.isclose`, abs 1e-9) and the DP's starts are one argmax. Use
  `max_segment_length ≥ n`; one extra case with `max_segment_length=2` against a brute force restricted to lengths ≤ 2.
- `segment_dp` on a synthetic 3-document sequence (within 0.9 ± noise, two dips 0.1) returns the two true starts plus 0
  for a moderate weight; always starts with 0, sorted, unique; empty input → `[0]`; all-NaN input → `[0]` or a valid
  start list (assert validity, not a value).
- `textiling_depth_scores`: depth array length n_gaps; a single deep valley is the only boundary at the default
  threshold; constant input → only `[0]` (std 0 → threshold max + 1); explicit `depth_threshold` honoured; empty input
  → `(empty array, [0])`.
- `calibrate_length_penalty_weight`: on two synthetic inventories, the returned weight is the candidate whose mean
  segment length is closest to the target (recompute in the test); first candidate wins ties; empty dict → first
  candidate.
- `predicted_start_rate([0, 3], 6) == 1/3`; `n_pages=0` → NaN.
- Converters: round trip `boundaries_to_starts(starts_to_boundaries(s)) == sorted(set(s) | {0})`; `boundaries_to_segments`
  on converted DP output covers [0, n) without gaps.
- Lazy exports: `from archival_structures.stream_analysis import segment_dp, textiling_depth_scores,
  starts_to_boundaries` works.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_subsequence_detection_unsupervised.py`, then
  the full suite `.venv/bin/python -m pytest -q -p no:cacheprovider` (baseline 2026-10-27: 1241 passed, 11 skipped).
  Counts from `pytest --collect-only -q`.

## Equivalence script `compare_nlp.py`

argparse `--out-dir` (required). Run with NLP's interpreter as above. Build `sims` as in *Inputs*; then, importing the
NLP originals under aliases and the AS ports:
1. both calibrations return the same weight, `== 0.0293559853524414`;
2. per inventory, `segment_dp` start lists identical (AS vs NLP) at that weight and at weight 1.0;
3. per inventory, `textiling_depth_scores` depth arrays `np.array_equal` and start lists identical (window 3);
4. AS start lists reproduce `unsupervised_changepoints.csv`'s `is_change_point` on all 41,810 rows;
5. `predicted_start_rate` equal per inventory.
Write `compare.tsv` (check, inventories or rows compared, mismatches) and log to `run.log`.

## Acceptance criteria

1. All five checks: 0 mismatches; weight 0.0293559853524414; 41,810 / 41,810 rows.
2. New tests pass; full AS suite green; `git diff` shows no change to existing functions in `subsequence_detection.py`.

If a number does not reproduce, stop and record it under *Open questions* in your `PROGRESS.md` block.

## Error analysis

The port is verbatim, so the error analysis reads the method, not the port:
- For 3 inventories (the largest, the smallest, and 2555), give DP segment-length quantiles next to the labelled ones.
  Positions map to page ids through `unsupervised_changepoints.csv` (`inventory_number, page_id, position`); labelled
  starts are `$NLP/output/early_end/page_table.csv` (`usecols=['inventory_number','scanname','start']`, `start` bool;
  `page_id` and `scanname` may differ by a file extension: strip `.jpg`/`.tif`/`.xml`). Labels are read for this
  description only, never fed to the method.
- Read 10 DP starts in 2555 (seeded, `random_state=18`) with their page text from
  `$NLP/output/early_end/page_text.csv.gz` (`inventory_number, scanname, text`) and the labelled `start`: is the
  predicted start a document start, a separator sheet ("No. 21"), or inside a document? This is a reading of the
  method's behaviour, not a metric; say so.
- Record the convention mismatch (start vs gap indices) as a finding (method), and the weight's sensitivity: the mean
  segment length at the 25 candidates (a table) as data.

## Do not touch

- Other steps' sections, placeholders and `STATUS.md` rows; NLP and LotA files.

## When done

1. Fill your `### Step 18b: …` section in `reports/phase-11-text-extent-and-nlp-ports.md` (template
   `plan/templates/REPORT.md`) and add rows `F11-18b.n` (status `draft`) to its *Findings* table with Edit. Run
   `make reports` from the AS root and fix any warning.
2. Replace the line `<!-- 18b block -->` in the phase `PROGRESS.md` with the block below.
3. Edit only your row (`| 18b |`) of the *Steps* table in the phase `STATUS.md`.

```markdown
### Step 18b: C10, DP and TextTiling segmentation
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18b](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18b-c10-dp-and-texttiling-segmentation); findings F11-18b.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
