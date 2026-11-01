# Step 22c: The index-side detector on VOC tables of contents (bar D on GLOBALISE gold)

> **Phase:** 16, A second archive (1.04.02 VOC): what generalises. **Depends on:** 22a (the layer).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 13 built an index-side detector for SG hand volumes (`analysis/index_pages.py`). It flags a page side whose
lines form two columns (`col_share`) and end in a number or start with a date. It passed bar D1 on SG (lines P 0.971,
R 0.949). The detector runs in the layout stage as `index_sides` in the v5 preset, so 22a's VOC layer already has
its flags in `layout/pages`.

VOC volumes of the *Overgekomen brieven* open with a *Register der brieven/papieren*: a table of contents. The
GLOBALISE document-segmentation gold labels these scans "Table of contents": 186 scans in 21 of the 22 inventories.
This step:
1. measures the detector **unchanged** against that gold;
2. tries a fixed grid of parameter values on dev;
3. tests the selected cell once.

No rule is added: the detector's knobs are parameters already (`INDEX_SIDES_DEFAULTS`).

## Inputs

Paths are relative to the AS root, checked 2026-10-31.

- **Layer (from 22a):** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/`; take the exact root from
  `STATUS.md`. Tables:
  - `scans/<inv>.scans.tsv.gz`;
  - `lines/<inv>.lines.tsv.gz` (`scan_id … line_id x y w h … text …`);
  - `layout/lines/<inv>.tsv.gz` (`scan_id line_id region_id page layout_region_id layout_line_idx layout_column
    layout_role`);
  - `layout/pages/<inv>.tsv.gz` (`scan_id page n_lines n_text8 n_text20 col_share end_num date_start rule
    index_side`), written for `hand` volumes only.
- **Gold (frozen at planning):** `reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv` (sha256 starts
  `095e7f3198ac3542`). Columns `inv row scan_id toc start split`, one row per scan of the 22 inventories (21,623
  rows). `toc = 1` when *Type of non-document page* starts with "Table" ("Table of contents", "Tables of contents").
  - dev (11): 1267 1274 1388 1547 1557 2548 2555 3142 7923 8121 8697, with 117 TOC scans of 12,324;
  - test (11): 1120 1529 1539 2448 2775 3891 8023 8237 8276 8284 8834, with 69 TOC scans of 9,299.
  - **2775 is planner-seen:** the planner read its TOC text on 2026-10-31. It is left out of the test bar and
    reported on its own, so the test bar covers **10 volumes, 61 TOC scans**.
  - 2555 also has mid-volume TOC runs (scans 621–625, 745–749).
- **Code (current, unchanged by this step except for the constants at the end):**

```python
# archival_structures/analysis/index_pages.py
INDEX_SIDES_DEFAULTS = {"col_min_chars": 20, "col_min_lines": 15, "mid_frac": 0.4, "text_min_chars": 8,
                        "col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": None, "shape_only_lines": 5,
                        "gap": 0, "min_run": 1, "end_pattern": r"\d+\s*[.,:;]?\s*$", "date_pattern": None}
def side_features(lines, cfg) -> dict   # t = stripped texts >= text_min_chars; s = lines >= col_min_chars;
    # col_share = share of s starting right of lo + mid_frac*(hi - lo) when len(s) >= col_min_lines, else None;
    # end_num = share of t matching re.search(end_pattern); date_start = share of t with re.match(date_pattern, I)
def side_rule(feat, cfg) -> bool        # shape = max(end_num, date_start); True if col_share >= col_share_min and
    # shape >= shape_min; else True if shape_only_min is set, n_text8 >= shape_only_lines and shape >= shape_only_min
def fill_runs(flags, gap, min_run) -> list
# archival_structures/text_layer/layout.py (private; call it, don't copy it)
def _write_pages_table(path, line_rows, line_map, cfg, scan_order) -> int
    # line_rows: rows of lines/<inv>.lines.tsv.gz; line_map[(scan_id, line_id)] = {"page": ...} (from layout/lines);
    # cfg: the index_sides dict; scan_order: {scan_id: position in the scans table}. Writes layout/pages columns.
# archival_structures/text_layer/presets.py
SG_INDEX_DATE_START = r"\W{0,2}\d{1,2}\s*[.,:]?\s*" + _TYPE_MONTHS
SG_INDEX_SIDES_V5 = {"col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": None, "gap": 8, "min_run": 1,
                     "date_pattern": SG_INDEX_DATE_START}
# archival_structures/analysis/index_links.py
def front_index_run(flags, *, min_flagged=10, max_first=10, gap=20, min_after=300, max_share=0.6) -> Optional[tuple]
```

- **What the planner saw.**
  - 22a's planning run of 2775: its TOC (scans 7–14) had `col_share` 0.0–0.12 and `end_num` 0.10–0.33, so 0 of 8
    were flagged, and 3 other sides were.
  - Dev TOC text (1267, 1274, 1388, 1557, 2548, 8121, 8697) shows one text column. The folio numbers sit either in
    short lines or regions of their own ("„ 1492:", "33", "158."), or at the start of an entry as a range ("288 a 291.
    Copie missive…", "1999 en 2000 Originele…", "fo. 17 verso a 78", "1960 tot 1962"). Entries often begin with a
    receipt date ("den 25:' Maart P„r 't schip…").
  - Short number lines fall under `text_min_chars` 8 and under `col_min_chars` 20.

## Deliverables

| File | Change |
|---|---|
| `reports/data/phase-16-voc-generalisation/step-22c/` | `eval_toc.py`, `replica_check.tsv`, `dev_grid.tsv`, `select.json`, `test_done.json`, `scan_eval.tsv`, `front_runs.tsv`, `cases_read.tsv` |
| `archival_structures/text_layer/presets.py` | **only if the selected cell passes bar D:** `VOC_TOC_CUE` and `VOC_INDEX_SIDES_V1` (the selected values), commented, used by no preset |
| `docs/shared_library/changelog.md` | `### Step 22c: …` only if the constants were added (Edit) |
| `reports/phase-16-voc-generalisation.md` | `### Step 22c: …` section, draft findings `F16-22c.n` |

## Behaviour to implement

**1. Replica.** For each of the 22 inventories, call `layout._write_pages_table` into a scratch path:
- `line_rows` from `lines/`;
- `line_map` from `layout/lines`;
- `scan_order` from `scans`;
- `cfg = SG_INDEX_SIDES_V5`.

The `rule` and `index_side` columns must equal the layer's `layout/pages` row for row → `replica_check.tsv`. Any
difference: stop and report. A volume without `layout/pages` is a `print` volume: it has no flags, so its TOC scans
count as missed.

**2. Scan-level scoring.** A scan is predicted *index* if any of its sides is flagged; gold is `toc`. Scans without
lines have no side and count as predicted 0. Report TP/FP/FN, P, R and F1 per volume and pooled.

**3. Grid (fixed at planning; 32 cells),** each `cfg = {**SG_INDEX_SIDES_V5, **cell}`:
- `text_min_chars` ∈ {8 (SG), 3};
- `shape_only_min` ∈ {None (SG), 0.15, 0.25, 0.35} (`shape_only_lines` stays 5);
- `date_pattern` ∈ {`SG_INDEX_DATE_START` (SG), `VOC_TOC_CUE`};
- `gap` ∈ {8 (SG), 2}.

```python
from archival_structures.text_layer.presets import _TYPE_MONTHS
VOC_TOC_CUE = (r"\W{0,2}((den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + _TYPE_MONTHS +
               r"|(fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}\s*(verso\s*)?(a|tot|en|—|–|-)\s*\d{1,4})")
```

`VOC_TOC_CUE` is a line-start cue (it goes in `date_pattern`, which `side_features` applies with `re.match`,
IGNORECASE). It covers receipt dates with or without "den", and folio ranges with "a", "tot", "en" or a dash. It was
written from dev text only.

**4. Dev, selection, test.**
1. Run all 32 cells on the 11 dev volumes → `dev_grid.tsv` (cell, TP, FP, FN, P, R, F1).
2. **Selection:** max dev F1. Ties within 0.005 go to the fewest values changed from SG, then the lower
   `shape_only_min` (None lowest), then `gap` 8.
3. Write `select.json` before any test number.
4. **Test once:** the selected cell and the unchanged cell (all SG values) on the 10 test volumes; 2775 separately →
   `test_done.json`, `scan_eval.tsv` (`inv scan_id split toc pred_unchanged pred_selected`).
5. **Bar D (VOC):** test scan-level P ≥ 0.95 **and** R ≥ 0.90. Give numerator/denominator and Wilson intervals.
6. **Verdict:** `unchanged` if the SG cell passes, `reparameterised` if only the selected one passes, `fails on VOC`
   otherwise.

**5. Front runs, reported only.** `front_index_run` with its SG defaults on three flag sequences per volume, in
`layout/pages` side order:
- (a) the unchanged flags;
- (b) the selected cell's flags;
- (c) oracle flags (the sides of gold TOC scans).

→ `front_runs.tsv` (`inv variant first last end` or `None`). Say which condition rejects each oracle `None`
(`min_flagged`, `max_first`, `min_after`, `max_share`, or no 20-side gap). No parameter of `front_index_run` is
fitted. Step 22d uses oracle TOC sides.

**6. Constants.** Only if the selected cell passes bar D, add to `presets.py` (next to `SG_INDEX_SIDES_V5`):
- `VOC_TOC_CUE` (if the selected cell uses it);
- `VOC_INDEX_SIDES_V1 = {**SG_INDEX_SIDES_V5, …selected values…}`;
- a comment naming this step and the bar.

No preset uses them yet; a fitted VOC layer is a later step.

## Constraints

- No edit to `index_pages.py`, `layout.py`, the layer, or any preset except the two constants in §6.
- The test split is read once, after `select.json`. A failed bar is a negative result: no further cells.
- No gold in library code. Do not open `planning/toc_peek_dev.tsv` or `planning/gm_proto_dev.tsv`, and do not open
  2775's TOC text before the test run.
- Never `git commit`; `chmod g+w`; zsh; no `rm -rf`, `nohup`, `&`, `sed -i`; Edit for shared files. One process is
  enough (22 volumes × 32 cells of in-memory features).
- Step 22b may run at the same time: edit only your own report section and your own `STATUS.md` lines.

## Tests

No library code changes unless §6 applies; then add `tests/test_text_layer_voc_preset.py::test_voc_index_sides`
(22a creates the file; append the test). Check:
- `VOC_INDEX_SIDES_V1` keys ⊆ `INDEX_SIDES_DEFAULTS` keys;
- `side_rule(side_features(lines, cfg), cfg)` is True on a synthetic one-column side of 6 lines, each starting
  "den 4 Junij …";
- it is False on 6 lines of prose.

Full suite green.

## Acceptance criteria

1. The replica equals `layout/pages` on all 22 volumes (or the step stops).
2. `dev_grid.tsv` has 32 rows; `select.json` is time-stamped before `test_done.json`.
3. Bar D (VOC) reported for the unchanged and the selected cell on 10 test volumes, with counts and Wilson
   intervals, plus 2775 separately; and the verdict.
4. `front_runs.tsv` for the three variants, with the rejection reasons.

The user decides when the step is done.

## Error analysis

Read, for the **selected** cell:
- every dev FN and FP, up to 30 each, seed 20261031 if more;
- every test FN and FP, up to 30 each;
- the unchanged cell's dev FNs, up to 15.

For each scan, give its features, 5 lines of text and the gold label. Classify:
- FN: one-column TOC; numbers in their own short lines; a very short TOC page (≤ 5 lines); garbled HTR; TOC title
  page only; layout lost the side;
- FP: a register-like list that is not a TOC (accounts, rosters, cargo lists); date-led letters (*dagregister*);
  a TOC page the gold missed; other.

Write `cases_read.tsv` (`inv scan_id split cell gold pred category note`).

Draft findings are expected for:
- bar D unchanged and selected;
- why the SG features miss VOC TOCs, by category with counts;
- the front-run conditions;
- 2775 on its own.

## Do not touch

The layer, `index_pages.py`, `layout.py`, `folio_map.py` and `index_links.py` (22b, 22d), the ground-truth files,
NLP and LotA.

## When done

1. Add `### Step 22c: The index-side detector on VOC tables of contents` to `reports/phase-16-voc-generalisation.md`
   (template `plan/templates/REPORT.md`), with findings `F16-22c.n` (status `draft`). Run `make reports` and fix any
   warning.
2. Append the `PROGRESS.md` block.
3. Update `STATUS.md` with Edit: the bar result, the selected cell, and whether the constants were added.
