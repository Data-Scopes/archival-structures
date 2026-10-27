# Step 18a: `text_extent` region types and margin gaps

> **Phase:** 11, Text extent options and NLP ports. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP tested AS's `early_end` class against its END labels (NLP report/84) and asked for two additive options in
`archival_structures/analysis/text_extent.py` (user decision UA-014 (a)): (1) `top`/`bot` computed over lines of chosen
region types only, and (2) the continuous margin gaps `bot − stable_bot` and `top − stable_top` next to the class. Both
must leave every current output unchanged by default. The step then reproduces NLP's report/84 numbers through the new
AS options, reading NLP's files without editing anything in NLP.

**Parallel run:** steps 18b and 18c run at the same time in the same working tree. Edit shared files only with Edit,
only at your own placeholder (below), and in `STATUS.md` only your own row of the *Steps* table. Don't overwrite
`STATUS.md`.

## Inputs

- AS `archival_structures/analysis/text_extent.py` (223 lines, checked 2026-10-27). Current public API, unchanged:
  ```python
  def _equal_frac(lines: list, page_width: int, tol_frac: float = 0.02) -> float
  def compute_page_text_extent(page: pdm.PageXMLScan, equal_tol: float = 0.02) -> Optional[Dict]   # L65
  def compute_corpus_text_extents(pages: List[pdm.PageXMLScan]) -> pd.DataFrame                    # L111
  def estimate_stable_margins(df: pd.DataFrame, percentile: float = 10.0) -> Dict[str, float]      # L126
  def classify_text_extent(row, stable, top_tol: float = 0.05, bot_tol: float = 0.05) -> str      # L147
  def classify_corpus_text_extents(df, stable=None, top_tol=0.05, bot_tol=0.05) -> pd.Series      # L181, name 'text_extent'
  def full_text_page_fraction(df, stable=None, top_tol=0.05, bot_tol=0.05) -> float               # L203
  ```
  `compute_page_text_extent` does `lines = [ln for ln in page.get_lines() if ln.coords]`, returns `None` when no line
  is left or the page height/width is 0, else a dict `page_id, n_lines, top, bot, left, right, mean_width,
  equal_frac`. `compute_corpus_text_extents` drops `None` rows, indexes by `page_id`; its empty-frame branch lists only
  `page_id, n_lines, top, bot, left, right`.
- Existing tests: `tests/test_analysis_text_extent.py`, 22 tests; its helper `_make_scan(doc_id, page_h, page_w,
  line_specs)` builds one untyped region. Typed regions in pagexml-tools 0.9.0 (checked):
  `pdm.PageXMLTextRegion(doc_id=..., coords=..., lines=[...], doc_type='header')` gives `region.type ==
  ['text_region', 'header']`; `region.add_type('paragraph')` gives `['text_region', 'paragraph']`; an untyped region
  has `region.type == 'text_region'` (a string); `line.parent` is the region.
- NLP root `NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (read only; never write there,
  never `pip install` into its venv). NLP's body variant (`scripts/early_end_extract_body.py` L29–55), verbatim:
  ```python
  BODY = {"paragraph", "signature-mark", "header"}
  def _rtype(line):
      t = line.parent.type if line.parent else None
      if isinstance(t, list):
          return t[-1] if len(t) > 1 else "text_region"
      return t
  def _sn(pid):
      return os.path.splitext(pid)[0] if pid.lower().endswith((".jpg", ".tif", ".xml")) else pid
  # per page: lines = [ln for ln in page.get_lines() if ln.coords and _rtype(ln) in BODY]
  #   top=min(top)/ph, bot=(ph-max(bottom))/ph, left=min(left)/pw, right=(pw-max(right))/pw
  ```
  Pages come from `pagexml.parser.parse_pagexml_files_from_archive(str(NLP/"pagexml"/f"{inv}.zip"),
  ignore_errors=True, silent_mode=True)`, for the 22 inventories 1120, 1267, 1274, 1388, 1529, 1539, 1547, 1557, 2448,
  2548, 2555, 2775, 3142, 3891, 7923, 8023, 8121, 8237, 8276, 8284, 8697, 8834 (local zips, 3.8–84 MB).
- NLP reference files in `$NLP/output/early_end/` (measured 2026-10-27):
  - `page_extents.csv`: 20,912 rows; columns `page_id, n_lines, top, bot, left, right, mean_width, equal_frac,
    inventory_number, scanname, page_h, page_w` (AS defaults, all lines; pages without lines absent);
  - `page_extents_body.csv`: 20,912 rows; `inventory_number, scanname, n_body_lines, top, bot, left, right`
    (NaN margins where `n_body_lines == 0`; 19,519 pages have ≥ 1 body line);
  - `page_table.csv`: 20,905 labelled rows; use `usecols=['inventory_number','scanname','end_last','n_lines','top','bot',
    'text_extent']`. The END target is the boolean `end_last` (1,379 pages; equals NLP's `label_class=='END' |
    (end_last & start)`, checked). 7 pages of `page_extents*` have no label row: join on `scanname`, inner;
  - `pr_main.csv` row 1: early_end → END pooled P 0.1397814910025707, R 0.3154459753444525 (n_flag 3,112);
    `supp_body_pr.csv` row 1: P 0.17052860246198406, R 0.34155184916606235 (n_flag 2,762);
  - `supp_end_rate_by_bot_bin.csv`: `bin, n, END_rate, …`, 8 bins (planning re-computed all 8 exactly from
    `page_table.csv`, e.g. (0.15, 0.25] n 208 END 0.389).
- NLP's classification protocol (`early_end_analysis.py` L78–88): per inventory, over pages with `n_lines > 0`,
  `stable = estimate_stable_margins(g[['top','bot','left','right']])`, then `classify_text_extent(row, stable, 0.05,
  0.05)`. In the body variant the same, with body extents and `n_lines := n_body_lines` (stable margins from body
  extents of pages with ≥ 1 body line; pages with none are never flagged). Pooled P = |flag ∧ END| / |flag|, R =
  |flag ∧ END| / |END| over the 20,905 labelled pages; flag = class `early_end` only (not `short`).
- Running AS code under NLP's interpreter (checked 2026-10-27; it imports the AS working tree, pagexml-tools 0.9.0):
  `cd $NLP && PYTHONNOUSERSITE=1 PYTHONPATH=/Users/marijnkoolen/Code/Huygens/archival-structures venv/bin/python …`.
  AS's own `.venv/bin/python` also has pagexml-tools 0.9.0 and may be used instead; say which you used.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/text_extent.py` | `line_region_type`, `region_types` kwarg on two functions, `margin_gaps`, `text_extent_table`, `BODY_REGION_TYPES_VOC`; empty-frame columns fixed |
| `tests/test_analysis_text_extent.py` | new tests (below), helper for typed regions; existing 22 unchanged |
| `reports/data/phase-11-text-extent-and-nlp-ports/step-18a/recheck_report84.py` | the re-check script (below) |
| `reports/data/phase-11-text-extent-and-nlp-ports/step-18a/` | `extents_all.tsv.gz`, `extents_body.tsv.gz`, `recheck.tsv`, `bins.tsv`, `run.log` |
| `docs/shared_library/changelog.md` | replace the line `<!-- 18a -->` under `## Unreleased` with `### Step 18a: …` |
| `reports/phase-11-text-extent-and-nlp-ports.md` | fill `### Step 18a: …`; findings `F11-18a.n` |

## Behaviour to implement

```python
BODY_REGION_TYPES_VOC: FrozenSet[str] = frozenset({"paragraph", "signature-mark", "header"})
# NLP report/84's body-text types for VOC 1.04.02 (Laypa labels). A named example, NOT a default.

def line_region_type(line) -> Optional[str]:
    """Type of the line's direct parent region (NLP `_rtype`, report/84, verbatim rule):
    list with >1 element -> last element; one-element list -> "text_region"; str -> as is; no parent -> None."""

def compute_page_text_extent(page, equal_tol=0.02, region_types: Optional[Collection[str]] = None) -> Optional[Dict]
def compute_corpus_text_extents(pages, region_types=None, equal_tol=0.02) -> pd.DataFrame
def margin_gaps(row, stable) -> Dict[str, float]          # {'top_gap': row['top']-stable['top'], 'bot_gap': row['bot']-stable['bot']}
def text_extent_table(df, stable=None, top_tol=0.05, bot_tol=0.05) -> pd.DataFrame
```

- `region_types=None` (default): exactly today's behaviour. Otherwise keep a line only if it has coords **and**
  `line_region_type(line) in region_types`; all returned values (`n_lines`, margins, `mean_width`, `equal_frac`) are
  computed over the kept lines; no kept line → `None`. Nested regions get no special handling (the direct parent counts).
- `compute_corpus_text_extents` passes `region_types` and `equal_tol` through (new kwargs, defaults as today). Its
  empty frame gets all eight columns (`page_id` index plus `n_lines, top, bot, left, right, mean_width, equal_frac`);
  this is the only change to existing output, a bug fix: say so in the changelog.
- `margin_gaps`: raw differences, no tolerance subtracted (decision P5). Positive = more white space than stable.
- `text_extent_table`: `stable` estimated from `df` if `None` (as `classify_corpus_text_extents` does); returns a frame
  indexed like `df` with columns `text_extent` (identical to `classify_corpus_text_extents(df, stable, top_tol,
  bot_tol)`), `top_gap`, `bot_gap`. Never derive the class from the gaps: the class keeps its own comparison
  `bot > stable['bot'] + bot_tol` (float rounding at the boundary could differ). Empty `df` → empty frame with the
  three columns.
- Docstrings: say that `region_types` matches the parent region's type as returned by pagexml-tools (PageXML/Laypa
  `type`), that untyped regions have type `"text_region"`, and that stable margins for a filtered run must be estimated
  from a filtered frame (don't mix). Module docstring: one paragraph on the two options with "NLP report/84, UA-014".
- Keep the module's style (4-space, type hints, `Optional`, short docstrings with *Args*).

## Re-check script `recheck_report84.py`

argparse: `--nlp-root` (required), `--out-dir` (required), `--workers` (int, default 1), `--limit N` (first N
inventories, smoke test). Docstring states purpose and the command. Per inventory (a `multiprocessing.Pool` over the 22
inventories, largest zip first), parse the zip as above and write one row per page with ≥ 1 line for each mode:
`inventory_number, scanname (NLP _sn rule), n_lines, top, bot, left, right` (all-lines, `region_types=None`) and the
same with `region_types=BODY_REGION_TYPES_VOC` (pages with no body line: a row with `n_lines=0`, margins empty). Then:

1. **Extents equality.** Join on `scanname` against `page_extents.csv` (all) and `page_extents_body.csv` (body; compare
   `n_lines` to `n_body_lines`). Report row counts, the number of scannames on each side only, and max |Δ| per margin.
2. **Classification.** Per inventory, stable margins from the mode's own extents (pages with `n_lines > 0`), then
   `text_extent_table`. Inner-join with `page_table.csv` labels (20,905 rows); pages missing an extent row are
   unflagged. Pooled n_flag, P, R for `early_end` → `end_last` in both modes. Also assert the all-lines classes equal
   `page_table.csv`'s `text_extent` on every page with `n_lines > 0`.
3. **Gap bins.** All-lines mode, pages with `n_lines > 0`: `pd.cut(bot_gap, [-1, 0.02, 0.05, 0.07, 0.10, 0.15, 0.25,
   0.40, 1])`, n and `end_last` rate per bin, rounded to 3 decimals as NLP did, against `supp_end_rate_by_bot_bin.csv`.

Write `recheck.tsv` (criterion, expected, measured, pass) and `bins.tsv`; log to `run.log`. Run it through coreshare
(zsh; write the prefix out):
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status
cd $NLP && python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project AS --label 18a --want 4 --min 1 --wait -- env PYTHONNOUSERSITE=1 PYTHONPATH=/Users/marijnkoolen/Code/Huygens/archival-structures venv/bin/python /Users/marijnkoolen/Code/Huygens/archival-structures/reports/data/phase-11-text-extent-and-nlp-ports/step-18a/recheck_report84.py --nlp-root $NLP --out-dir /Users/marijnkoolen/Code/Huygens/archival-structures/reports/data/phase-11-text-extent-and-nlp-ports/step-18a --workers {workers}
```
Smoke-test first with `--limit 2 --out-dir <scratchpad dir>`. Runtime is unmeasured (NLP's two extract runs took a few
minutes each with 2 workers).

## Constraints

- Additive only: no existing signature, default or output changes except the empty-frame columns.
- No VOC type list as a default anywhere: `region_types` defaults to `None`.
- Never write under `$NLP`; never `git commit`; never bump the version; `chmod g+w` every new file and directory.
- Agent shell: no `rm -rf`, `nohup` or `&`; zsh.
- Don't touch `subsequence_detection.py`, `separator_sheets.py`, `docs/`, `text_layer/`, `analysis/hand_print.py`.

## Tests

New tests in `tests/test_analysis_text_extent.py` (add a helper that builds a scan from several regions, each with a
type or none):
- `line_region_type`: typed list, one-element list, string, no parent → `None`.
- `region_types=None` gives the same dict as calling without the kwarg on a mixed-type page (P6).
- Filtered margins: page with a `header` region at the top, `paragraph` body, `page-number` at the bottom;
  `region_types={'paragraph'}` moves `top`/`bot` to the paragraph lines and `n_lines` counts only them.
- No line of the chosen types → `None`; `compute_corpus_text_extents` drops it.
- `compute_corpus_text_extents` empty input → frame with the eight columns.
- `margin_gaps` values; `text_extent_table`: class column equals `classify_corpus_text_extents`, gaps equal
  `df.top − stable.top`, `df.bot − stable.bot`; estimated `stable` when `None`; empty frame.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_text_extent.py`, then the full
  suite `.venv/bin/python -m pytest -q -p no:cacheprovider` (baseline 2026-10-27: 1241 passed, 11 skipped). Counts
  from `pytest --collect-only -q`.

## Acceptance criteria

1. Extents: all 20,912 NLP pages matched in both modes (0 scannames on either side only); max |Δ| of `top, bot, left,
   right` ≤ 1e-12 in both modes; body `n_lines == n_body_lines` on all 20,912; all-lines `n_lines` equal.
2. All-lines classes equal `page_table.csv`'s `text_extent` on every labelled page with lines.
3. early_end → END: all-lines n_flag 3,112, P 0.1397814910025707, R 0.3154459753444525; body n_flag 2,762,
   P 0.17052860246198406, R 0.34155184916606235 (equal to 1e-12).
4. The 8 gap bins equal `supp_end_rate_by_bot_bin.csv` in `n` and `END_rate`.
5. New tests pass; full AS suite green.

If any number does not reproduce, stop and record it under *Open questions* in your `PROGRESS.md` block.

## Error analysis

- Read 10 pages where the body variant changes the class (all-lines `early_end` → body `full_text`, and the reverse;
  seeded sample, `random_state=18`, 5 each, or all if fewer): which non-body region set the all-lines margin (look at
  that region's type and text in the PageXML, e.g. catch-word, page-number, marginalia, untyped). Count the region types
  that set `bot` on all pages whose class changes (a table, by type).
- Read the 10 pages with the largest `bot_gap` among END pages and the 10 largest among non-END pages (by
  `bot_gap`, from `extents_all`): what the text looks like (first/last body line text). Note what a consumer
  threshold on `bot_gap` would and would not catch.
- Expected findings: the reproduction (result), which region types move the margin (data-property), what the gap
  column offers over the class (method). A negative result is a finding.

## Do not touch

- Other steps' sections, placeholders and `STATUS.md` rows; NLP and LotA files.

## When done

1. Fill your `### Step 18a: …` section in `reports/phase-11-text-extent-and-nlp-ports.md` (template
   `plan/templates/REPORT.md`) and add rows `F11-18a.n` (status `draft`) to its *Findings* table with Edit. Run
   `make reports` from the AS root and fix any warning.
2. Replace the line `<!-- 18a block -->` in the phase `PROGRESS.md` with the block below.
3. Edit only your row (`| 18a |`) of the *Steps* table in the phase `STATUS.md`.

```markdown
### Step 18a: text_extent region types and margin gaps
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-11-text-extent-and-nlp-ports.md` § Step 18a](../../reports/phase-11-text-extent-and-nlp-ports.md#step-18a-text_extent-region-types-and-margin-gaps); findings F11-18a.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
