# Step 19c: index-side detector

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** 19a (side verdicts), 19b (code, clones).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Index pages of the States General (1.01.02) hold two columns of entries per page side, each pointing to a resolution by
date or page number under lemma headings. They fill the index volumes and a 40–70-scan section at the front of every
*tweede serie* half-year register (3471: scans 0003–0056). The user decided (2026-10-28) that the index entry rule
(19d) applies **per page side**, detected from the PageXML alone. This step adds that detector to the layout stage: per
layout page side, three features, a rule, and a contiguity fill (index sections are runs of pages). The variant is
selected on the GT **fit** volumes and tested once on the GT **test** volumes (bar D1) and on 60 read sides (bar D2).
All rules, grids and bars below were fixed at planning (2026-10-28), before any test number was computed.

## Inputs

- Code after 19b: `archival_structures/analysis/page_structure.py` (`LayoutParams` with `hand_column_split`,
  omitted from `to_dict` when `None`; follow the same pattern), `archival_structures/text_layer/layout.py`:
  `layout_inventory(inv, layer_root, params)` L211–289 loads `scans`, `regions_meta`, `lines` rows, calls `layout_scan`
  per scan, builds `line_map[(scan_id, line_id)] = {"page": …, …}` and writes `layout/{scans,regions,lines}/<inv>.tsv.gz`
  with `write_tsv_gz(path, columns, rows)`; `PAGE_ORDER = {"verso": 0, "recto": 1, "single": 0}` (L49).
  `archival_structures/text_layer/presets.py`: `_TYPE_MONTHS` (L93, a regex group of Dutch month abbreviations).
- Clones from 19b (see `STATUS.md`): `data/text_layer_v2_dev/19b-a`, `19b-z`; the `SPLIT` JSON of 19b.
- Planning (`reports/data/phase-13-register-index-volumes/planning/`): `side_features.tsv` (115,274 rows: `inv, layer,
  scan_id, page, n_lines, n_text8, n_text20, col_share, end_num, date_start, gt_label`), `gt_split.tsv` (`inv, split,
  has_index_section`: 22 fit (10 with an index section), 20 test (9)), `detector_grid.py` (the grid on `fit`; output
  below), `hand_15y-b.txt`, `hand_15z-b.txt`, `reads_sides.tsv`.
- 19a: `reports/data/phase-13-register-index-volumes/step-19a/side_verdicts.tsv` (`read_id, verdict, kind, note`) and
  `show_side.py` (copy its listing logic for your own read tool).
- GT: `ground_truth/NL-HaNA_1.01.02/inventory_metadata-NL-HaNA_1.01.02.json`: a list of 674 dicts with
  `inventory_num` (int) and `sections` (list of `{"start", "end", "page_type"}`, page numbers inclusive;
  `page_type` ∈ `index_page`, `resolution_page`, `respect_page`, …). **Page numbers:** verso = 2·scan − 2, recto =
  2·scan − 1 (scan = the int after the last `_` of the scan id); `single` pages have no GT number.

Planning output of `detector_grid.py fit` (reproduce it exactly):
```
V01 c3=None gap=0 min_run=1: lines P=0.9841 R=0.8383 | sides tp=395 fp=9 fn=287
V02 c3=None gap=0 min_run=3: lines P=0.9954 R=0.7349 | sides tp=339 fp=3 fn=343
V03 c3=None gap=4 min_run=1: lines P=0.9798 R=0.8835 | sides tp=461 fp=13 fn=221
V04 c3=None gap=4 min_run=3: lines P=0.9842 R=0.8336 | sides tp=442 fp=11 fn=240
V05 c3=None gap=8 min_run=1: lines P=0.9808 R=0.9297 | sides tp=551 fp=13 fn=131
V06 c3=None gap=8 min_run=3: lines P=0.9857 R=0.9197 | sides tp=547 fp=11 fn=135
V07 c3=0.3 gap=0 min_run=1: lines P=0.9302 R=0.9408 | sides tp=521 fp=49 fn=161
V08 c3=0.3 gap=0 min_run=3: lines P=0.9603 R=0.8547 | sides tp=457 fp=28 fn=225
V09 c3=0.3 gap=4 min_run=1: lines P=0.9225 R=0.9663 | sides tp=619 fp=58 fn=63
V10 c3=0.3 gap=4 min_run=3: lines P=0.9444 R=0.9472 | sides tp=614 fp=44 fn=68
V11 c3=0.3 gap=8 min_run=1: lines P=0.9177 R=0.9860 | sides tp=652 fp=63 fn=30
V12 c3=0.3 gap=8 min_run=3: lines P=0.9374 R=0.9860 | sides tp=652 fp=51 fn=30
```

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_pages.py` (new) | `INDEX_SIDES_DEFAULTS`, `side_features`, `side_rule`, `fill_runs` |
| `archival_structures/analysis/page_structure.py` | `LayoutParams.index_sides` |
| `archival_structures/text_layer/layout.py` | `PAGE_COLUMNS`; `layout/pages/<inv>.tsv.gz` when `params.index_sides is not None` |
| `archival_structures/text_layer/presets.py` | `SG_INDEX_DATE_START`, `SG_INDEX_SIDES_V5` (constants only; no preset uses them yet) |
| `tests/test_analysis_index_pages.py` (new), `tests/test_text_layer_layout.py` | tests below |
| `reports/data/phase-13-register-index-volumes/step-19c/` | `features_check.py`, `select.py`, `mapping_reads.txt`, `mapping_verdicts.tsv`, `show_any.py`, `errors_reads.txt`, `error_verdicts.tsv`, `selection.json`, `protect_*.txt` |
| `docs/shared_library/changelog.md` | `### Step 19c: index page sides` under *Unreleased* (Edit) |
| `reports/phase-13-register-index-volumes.md` | `### Step 19c: …` section, findings `F13-19c.n` |

## Behaviour to implement

**`index_pages.py`** (generic; archive knowledge only as parameters):
```python
INDEX_SIDES_DEFAULTS = {"col_min_chars": 20, "col_min_lines": 15, "mid_frac": 0.4, "text_min_chars": 8,
                        "col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": None, "shape_only_lines": 5,
                        "gap": 0, "min_run": 1, "end_pattern": r"\d+\s*[.,:;]?\s*$", "date_pattern": None}
def side_features(lines, cfg) -> dict      # lines: dicts with string "x", "w", "text"
def side_rule(feat, cfg) -> bool
def fill_runs(flags, gap, min_run) -> list
```
`side_features` (exactly the planning definitions; `c = {**INDEX_SIDES_DEFAULTS, **cfg}`):
- `n_lines` = lines whose stripped text is non-empty; `t` = stripped texts with `len >= text_min_chars`, `n_text8 =
  len(t)`; `s` = lines with `len(text) >= col_min_chars` (**unstripped** text), `n_text20 = len(s)`.
- `col_share`: if `len(s) >= col_min_lines`: `xs` = sorted int `x` of `s`, `lo = xs[len(xs) // 20]`, `hi = max(x + w)`
  over `s`, share of `s` with `x >= lo + mid_frac * (hi - lo)`; else `None`.
- `end_num` = share of `t` where `re.search(end_pattern, text)`; `date_start` = share of `t` where
  `re.match(date_pattern, text, re.IGNORECASE)` (0.0 when `date_pattern` is `None`); both 0.0 when `t` is empty.
`side_rule`: `shape = max(end_num, date_start)`; true iff (`col_share` is not `None` and `col_share >= col_share_min`
and `shape >= shape_min`) or (`shape_only_min` is not `None` and `n_text8 >= shape_only_lines` and `shape >=
shape_only_min`). `fill_runs`: every run of at most `gap` false flags between two true flags becomes true; then every
true run shorter than `min_run` becomes false (copy `fill` from `planning/detector_grid.py`).

**`LayoutParams.index_sides`**: `Optional[dict] = dataclasses.field(default=None, hash=False)`, keys checked against
`INDEX_SIDES_DEFAULTS` in `__post_init__`, **omitted from `to_dict` when `None`**, test `is not None`.

**Layout.** In `layout_inventory`, when `params.index_sides is not None`: group the inventory's `lines` rows by
`(scan_id, line_map[(scan_id, line_id)]["page"])` (every line row, also blank ones and those of empty regions); compute
`side_features` per side; order the sides by `(int(scan_id.rsplit("_", 1)[1]), PAGE_ORDER[page], page)`; `rule` per
side, then `index_side = fill_runs(rules, gap, min_run)`. Write `layout/pages/<inv>.tsv.gz` with
`PAGE_COLUMNS = ["scan_id", "page", "n_lines", "n_text8", "n_text20", "col_share", "end_num", "date_start", "rule",
"index_side"]`: floats as `str(round(v, 4))`, `col_share` `""` when `None`, `rule`/`index_side` as `0`/`1`. Add
`n_index_sides` to the inventory stats only when the field is set. No table is written when it is `None` (v1–v4
unchanged).

**Presets (constants).** `SG_INDEX_DATE_START = r"\W{0,2}\d{1,2}\s*[.,:]?\s*" + _TYPE_MONTHS` (comment: phase 13 P8);
`SG_INDEX_SIDES_V5 = {**<selected variant>, "date_pattern": SG_INDEX_DATE_START}` with a comment giving the selection
and both bar results. Grid variants map to keys as: `c1 → col_share_min` (0.25), `c2 → shape_min` (0.15), `c3 →
shape_only_min`, `g → gap`, `m → min_run`.

## Procedure (from the AS root)

1. Protect `_before` (`protect.py … --skip 19b 19c`, and the `find … shasum` list of `15y-b`, `15z-b`, `19b-a`, `19b-z`,
   `data/text_layer_v4_full`). Code and tests; suite green.
2. Clones: `cp -cR data/text_layer_v2_dev/19b-a data/text_layer_v2_dev/19c-a`, the same for `19b-z` → `19c-z`;
   `chmod -R g+w`. `SIDES` = the V05 dict: `{"col_share_min": 0.25, "shape_min": 0.15, "shape_only_min": null, "gap":
   8, "min_run": 1, "date_pattern": <SG_INDEX_DATE_START as a JSON string>}`. `--layout-params` replaces the whole
   override, so pass **both** fields: `{"hand_column_split": <19b's dict>, "index_sides": SIDES}`. Run `--stage
   layout`, `--stage types`, `--stage units` on each clone with its hand list (as 19b; coreshare for > 1 worker).
3. `features_check.py`: the six feature columns of every `layout/pages` row against `side_features.tsv` (`inv, scan_id,
   page`): all 115,274 rows present and equal. If not, stop (the definitions differ).
4. **Mapping check (P10)**, before any GT number: for the first 5 `fit` volumes with `has_index_section = 1` (by
   inventory number), list the sides of GT pages `end − 1 … end + 2` of their `index_page` section (`show_any.py INV
   SCAN PAGE`, listing as 19a's `show_side.py`) and read them: does the index end at GT page `end`? Write
   `mapping_verdicts.tsv` (`inv, gt_end, verdict ∈ {consistent, off_by_k, unclear}, note`). Proceed iff ≥ 4 of 5 are
   `consistent`; else stop and report (no fix by guessing).
5. `select.py fit`: the 12 variants from the `layout/pages` features and the GT labels, exactly as
   `detector_grid.py` (positive = `index_page` or `respect_page`; sides without a GT page label left out; line weight
   = `n_lines`). Its output must equal the block above. **Selection:** among variants with lines P ≥ 0.95 the highest
   R (ties: lower V); none → the highest P. Expected: V05. If the selection differs from the run config, rerun step 2
   with it. Write `selection.json`.
6. `select.py test` **once**, for the selected variant only: lines P and R, sides tp/fp/fn on the 20 test volumes
   (bar D1).
7. **D2:** for the 60 `reads_sides.tsv` sides, the clones' `index_side` against 19a's verdicts (`index` = 1, `not
   index` = 0; `unclear`, `unreadable input` left out). Accuracy over the judged sides; false positives in strata
   `S3h` + `S3a`.
8. Error analysis reads (below); `make reports`; protect `_after`; `diff`.

## Tests

`tests/test_analysis_index_pages.py`: `side_features` on hand-made lines (two columns → `col_share` near 0.5; 14
long lines → `None`; `end_num`/`date_start` counts; unstripped length rule for `col_min_chars`); `side_rule` both arms;
`fill_runs` (gap fill, `min_run` drop, gap 0 and min_run 1 identity, empty list); `LayoutParams` round trip with
`index_sides`, key omitted when `None`, unknown key raises. `tests/test_text_layer_layout.py`:
`test_index_sides_pages_table` (a small inventory with the field set writes `layout/pages/<inv>.tsv.gz` with
`PAGE_COLUMNS`, one row per side; without it no `pages/` directory). Full suite green (counts from `--collect-only`).

## Acceptance criteria

1. Tests green; full suite green.
2. Step 3: 115,274 rows equal. Step 4: ≥ 4 of 5 `consistent`. Step 5: the 12 lines equal the planning block; selection
   reported.
3. **D1** (GT test volumes, once): lines P ≥ 0.95 **and** lines R ≥ 0.90.
4. **D2** (reads): accuracy ≥ 0.90 over judged sides **and** ≤ 2 false positives in `S3h` + `S3a`; more than 10 sides
   left out → not judgeable, stop.
5. A failed D1 or D2 is a negative result: report it with the numbers, no tuning, no second test; add a Decide UA (next
   free ID): v5 ships the column split only (recommended), or the detector anyway. Step 19d still runs (its test does
   not use the detector).
6. v1–v4 unchanged: `LayoutParams().to_dict()` has no `index_sides`; protected lists unchanged apart from the clones
   and other projects' `shared-data/` changes (name them).

The user decides when the step is done.

## Error analysis

Read with `show_any.py` and classify (write `error_verdicts.tsv`): every false-positive side on the test volumes (all,
if ≤ 20; else 20 with `random.Random(20261119)`), 20 false-negative sides drawn with probability proportional to
`n_lines` (`random.Random(20261120)`), and every D2 disagreement. Expected categories: FN sparse index page (`col_share`
unmeasured), single-column index book (4617, 4623, 4792 style), index page without dates or numbers, GT boundary or
label error; FP resolution page with a list or two columns, title/register page, gap fill across a non-index page. Also
report the detected index sides of all 125 hand volumes: per volume group (tweede serie index volumes, half-year
registers ≥ 3349, GT volumes, minuten, other) the sides and lines flagged, and for the half-year registers the first and
last flagged scan (the front index section). Draft findings: D1, D2, the selected variant, the main FN/FP causes.

## Do not touch

`units.py`, `types.py`, `build.py`, `split_regions_into_columns` (19b), the published layers,
`data/text_layer_v2_dev/{15y-b,15z-b,19b-a,19b-z}` and older dev layers, the planning and 19a files, `shared-data/`, the
siblings. Never `git commit`; `chmod g+w`; `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`; shared files only with
Edit. Never touch the external drive.

## When done

1. Report: `### Step 19c: index-side detector` (what changed, the grid table, selection, D1, D2, mapping check, error
   analysis, flagged sections; findings `F13-19c.n` as `draft`); `make reports` clean.
2. Changelog (Edit): `### Step 19c: index page sides`: new module `analysis.index_pages` (`INDEX_SIDES_DEFAULTS`,
   `side_features`, `side_rule`, `fill_runs`), `LayoutParams.index_sides` (omitted when `None`), the `layout/pages/`
   table (`PAGE_COLUMNS`), `presets.SG_INDEX_DATE_START`, `SG_INDEX_SIDES_V5`. "Affected sibling imports: none."
3. `PROGRESS.md` block (template of `plan/templates/STEP.md`); overwrite `STATUS.md`: 19c done, D1/D2 results, facts
   for 19d/19e (clone paths `19c-a`, `19c-z`; the full `--layout-params` JSON; `SG_INDEX_SIDES_V5`).
