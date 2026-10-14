# Step 12: page-numbering core, region-table anchors, GLOBALISE index loader

> **Phase:** 04, Decoders and page numbering. **Depends on:** 11 (`scan_number`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

NLP aligns the GLOBALISE index of VOC documents (folio ranges) to scans. It uses the
page numbers that HTR read in `page-number` regions as anchors: it finds a longest increasing
chain, detects foliation against pagination, and interpolates folio → scan, with a confidence
band. This step moves the generic core into AS `analysis/page_numbering.py`, adds an anchor
extractor that works on the shared text layer's region table (so anchors exist for all 6,914
inventories without re-parsing PageXML), and a GLOBALISE index loader in
`datasets/globalise_index.py`. The index path is always passed in; the data stays where it is.
**NLP is not edited**; step 12b makes it a shim and reproduces its outputs.

## Inputs

NLP root `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (`<NLP>`),
read-only. Port from `<NLP>/src/startpage/globalise_alignment.py` (242 lines; read it whole):

| NLP name (line, 2026-10-14) | AS name |
|---|---|
| `BANDS = (3, 10)` L36 | `CONFIDENCE_BANDS = (3, 10)` |
| `INDEX_COLS` L29–35 (10 GLOBALISE column → short-name pairs) | `datasets.globalise_index.GLOBALISE_INDEX_COLUMNS` |
| `Config` L39–44 (`frozen`; `mode, runs, extrap, dup_first`, all `False`) | `AlignmentConfig`, **the same 4 fields only** (`build_globalise_alignment.py` builds `Config(mode=True)` etc. and step 12b aliases it) |
| `load_index(inventories=None)` L52–59 | `load_globalise_index(path, inventories=None, columns=GLOBALISE_INDEX_COLUMNS)` |
| `lis_indices(values)` L79 | `longest_increasing_chain(values) -> list[int]` |
| `main_chain(anchors)` L95 | `main_chain(anchors) -> tuple[np.ndarray, np.ndarray]` |
| `extra_runs(anchors, keep, min_len=3, max_rounds=4)` L102 | `numbering_restarts(anchors, keep, min_len=3, max_rounds=4)` |
| `numbering_mode(chain)` L117 | `numbering_mode(chain, *, foliation=1.5, pagination=0.75) -> tuple[str, float]` |
| `_interp(f, run, extrap=False, max_scan=None)` L130 | `interpolate_folio_to_scan(folio, run, extrap=False, max_scan=None, *, edge=6) -> tuple[float, float, bool]` |
| `_band(dist, inside)` L144 | `confidence_band(dist, inside, bands=CONFIDENCE_BANDS) -> str` |
| `_locate(f, main, runs, prev_scan, cfg, max_scan=None)` L150 | `_locate(..., run_window=3)` (stays private) |
| `align_inventory(docs, anchors, cfg=Config(), max_scan=None)` L170 | `align_documents(docs, anchors, cfg=AlignmentConfig(), max_scan=None) -> pd.DataFrame` |
| `align_all(index_df, anchors_by_inv, cfg=Config(), max_scans=None)` L203 | `align_all(index_df, anchors_by_inv, cfg=AlignmentConfig(), max_scans=None, *, inventory_column="inventory_number")`, **without** the `n_sections` NaN column (NLP's shim adds it back) |
| `load_anchors()` L62–68 (array building only) | `anchors_array(anchors_df) -> np.ndarray` |

Not ported (NLP-specific; they stay in NLP): `PROJECT_ROOT`, `INDEX_PATH`, `ANCHORS_PATH`,
`SPINE_PATH`, `scan_no` (AS has `scan_number` from step 11), `load_anchors`, `load_spine`,
`load_gold_starts`, `score_start_accuracy`, `accuracy_summary`.

Step 11 API (check `STATUS.md`): `from archival_structures.model.scan_id import scan_number`
(`scan_number(scan_id: str, *, digits: int | None = None) -> int`; raises
`InvalidScanIdError` on a malformed id).

Data (read-only; never load a whole large file into context):
- GLOBALISE index CSV: `<NLP>/metadata/digitised_indexes/GLOBALISE - Digitized Indexes of the Dutch East India Company OBP (1602-1799).csv`
  (117 MB, 314,583 lines, UTF-8 BOM before `ID`). A byte-identical copy is in AS
  `ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/source/`. Pass a path; don't copy
  it into `tests/`.
- Mined anchors: `<NLP>/output/pagenumber_sequence_features.csv` (16 MB, 64,506 lines,
  467 inventories; columns `inventory_number, document_series_full, scan_id, page_number,
  prev_page_number, irregularity`).
- Text-layer regions: `<NLP>/data/voc_text_layer_v1/regions/<inv>.tsv.gz` (6,914 files,
  3.9 GB). TSV, `quoting=csv.QUOTE_NONE`; header `scan_id region_id region_box region_tag
  region_text`. Rows are in **document order** (not reading order) and include nested and
  zero-line regions. `region_text` is word-break-joined. Read with
  `usecols=["scan_id", "region_tag", "region_text"]`, one inventory at a time. Never follow
  paths recorded in the layer's `manifest.json`.
- How NLP mined anchors (`<NLP>/scripts/mine_catchword_pagenum_linespacing.py`): it went
  through PageXML regions **in reading order, only regions with ≥1 line**, and the last
  non-empty `page-number` region text on a page wins. The text is the line texts joined
  with `" "` and stripped. It keeps the page only if `re.match(r"^\d+$", text)` matches; it
  doesn't fall back to an earlier region. Planning compared inventory 2555: the regions-file
  rule gives 374 pairs, all 374 identical to the mined CSV.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/page_numbering.py` | new: the core above plus `page_number_anchors`, `anchors_array`. numpy + pandas only. `analysis/` has no `__init__.py`; don't add one |
| `archival_structures/datasets/globalise_index.py` | new: `GLOBALISE_INDEX_COLUMNS`, `load_globalise_index` (`datasets/__init__.py` is lazy; don't edit it) |
| `tests/test_analysis_page_numbering.py`, `tests/test_datasets_globalise_index.py` | new, synthetic |
| `tests/test_packaging_core_imports.py` | add `"archival_structures.analysis.page_numbering"` and `"archival_structures.datasets.globalise_index"` to `names` |
| `docs/shared_library/changelog.md` | `### Step 12: page numbering and GLOBALISE loader (C11)` under `## Unreleased`, with Edit |
| `reports/phase-04-decoders-and-page-numbering.md` | `### Step 12` section, findings `F04-12.n` |

## Behaviour to implement

**Port the core verbatim.** Step 12b demands byte-identical NLP outputs. Keep, and document
in the docstrings:
- LIS: `np.searchsorted(..., side="left")`, so the chain is strictly increasing, and an equal
  page number replaces the tail (a repeated number keeps the **later** scan);
- `int(round(s))`, Python's banker's rounding (x.5 goes to the even number);
- `max(1.0, min(s, max_scan or s))`, so a `max_scan` of 0 or None means "no upper clamp";
- the `"+extrapolated"` method suffix whenever the folio is outside the anchor range, even
  when only `np.interp`'s end clamping applied (`extrap=False`);
- `_locate` calls `_interp(f, r)` without `extrap`/`max_scan` for restart runs;
- `numbering_mode` with fewer than 3 anchors gives `("unknown", nan)`;
- the output columns and their order: `start_scan, start_scan_alt, end_scan, numbering_mode,
  method, nearest_anchor_dist, confidence`.

Replace each hard-coded number with its keyword parameter, defaulting to the old value:
`edge=6` (the `k = 6` slope window), `run_window=3` (`r[0,1]-3 <= f <= r[-1,1]+3`), the
bands, and the slope thresholds. `align_documents` and `align_all` use the defaults (no new
`AlignmentConfig` fields).

`align_all`: `max_scans = max_scans or {}`; group by `inventory_column` with `sort=False`; `concat`;
`index_df.join(res)`. Raise `ValueError` if `index_df.index` is not unique (NLP relies on
`reset_index`; a silent join on duplicates is the hazard).

`load_globalise_index(path, inventories=None, columns=GLOBALISE_INDEX_COLUMNS)`: NLP's
`load_index` statement for statement: `read_csv(path, usecols=list(columns), dtype=str)`,
rename, optional `isin(inventories)`, `pd.to_numeric(errors="coerce")` on whichever of
`folio_start, folio_end, year_earliest` are present, `tanap_id` with `"."` removed
(`regex=False`) if present, `reset_index(drop=True)`. The `dtype=str` and the numeric coercion
are VOC-index behaviour; they belong in this loader, not in the core.

`page_number_anchors(regions, *, tag="page-number", pattern=r"^\d+$", tag_column="region_tag",
text_column="region_text", scan_column="scan_id", scan_number=scan_number) -> pd.DataFrame`:
1. Keep rows with `tag_column == tag` whose text, `str(...).strip()`, is non-empty (NaN counts
   as empty).
2. Per scan, the **last** such row in frame order wins.
3. Keep it if `re.match(pattern, text)`; `page_number = int(text)`.
4. Return columns `scan_id, scan_no, page_number` (`scan_no = scan_number(scan_id)`), sorted by
   `scan_no` with a stable sort, index reset.

`anchors_array(df) -> np.ndarray` float `(n, 2)` `[scan_no, page_number]`, sorted by `scan_no`
(stable), as in NLP's `load_anchors`.

## Constraints

- No VOC constants in `page_numbering.py` (no paths, no column names of the GLOBALISE CSV, no
  archive codes). The GLOBALISE column map lives in `datasets/globalise_index.py`.
- Never `git commit`; `chmod g+w` new files. NLP, `ground_truth/` and the text layer are
  read-only. Write scratch output only to the session scratchpad.
- Don't port the catchword chain or the transition classifier (ok/skip/reset); that is a
  possible later step.

## Tests (synthetic)

`tests/test_analysis_page_numbering.py`:
- `test_lis_strict_and_later_duplicate_wins`, `test_lis_with_noise` (a clean 1..50 chain with
  8 injected outliers is recovered), `test_lis_empty`;
- `test_numbering_restarts_finds_second_run` (two chains 1..20 then 1..15) and
  `..._ignores_short_runs`;
- `test_numbering_mode_foliation_pagination_spread_unknown` (slopes 2, 1, 0.5; 2 anchors);
- `test_interpolate_inside_clamped_and_extrapolated` (inside; outside without `extrap` =
  end-clamped and `inside=False`; with `extrap` it uses the edge slope; `max_scan` clamp; lower
  clamp at 1.0);
- `test_confidence_band_thresholds` (3 → high, 4 → medium, 10 → medium, 11 → low, outside → low);
- `test_align_documents_foliation` (anchors every other scan, folio = (scan+1)//2 pattern; with
  `AlignmentConfig(mode=True)`: exact `start_scan`, `start_scan_alt = start+1`, `end_scan`);
- `test_align_documents_no_anchors_and_no_folio`, `test_dup_first_keeps_first_scan`,
  `test_bankers_rounding_is_kept` (an interpolated 12.5 gives 12);
- `test_align_all_requires_unique_index`;
- `test_page_number_anchors_last_region_wins`, `..._non_digit_last_region_drops_page` (no
  fallback to an earlier region), `..._empty_and_nan_text_skipped`, `..._custom_tag_and_pattern`.

`tests/test_datasets_globalise_index.py`: a 5-row CSV in `tmp_path`, written with a UTF-8 BOM
and all 10 source columns plus one extra, covering a non-numeric folio (gives NaN), a dotted
TANAP id, an inventory filter and the column map override.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_numbering.py tests/test_datasets_globalise_index.py tests/test_packaging_core_imports.py`,
then the full suite.

## Acceptance criteria

1. New tests pass; the full AS suite is green (count in `STATUS.md`, plus yours).
2. **Equivalence on the real index (scratch script, `<NLP>/venv/bin/python`, read-only).**
   With `<NLP>/src` on `sys.path`, `import startpage.globalise_alignment as ga`:
   - `load_globalise_index(ga.INDEX_PATH).equals(ga.load_index())`, and likewise with
     `inventories=` set to the 22 annotated inventories (the `<inv>` prefixes of the
     `<inv> - Document Segmentation.csv` files in `<NLP>/annotations/`);
   - with `anchors = ga.load_anchors()` and `max_scans` built as in
     `<NLP>/scripts/build_globalise_alignment.py` L93 (`spine.groupby('inventory_number').scan_no.max().to_dict()`
     with `spine = ga.load_spine()`), and for each of the 5 configs
     (`Config()`, `mode=True`, `runs=True`, `extrap=True`, `dup_first=True`) plus the chosen
     combination (`mode=True, extrap=True, dup_first=True`): AS `align_all(ix, anchors, cfg, max_scans)`
     `.equals(ga.align_all(...).drop(columns="n_sections"))` over the whole index (`ix` =
     `ga.load_index()`). Expect **6/6 configs equal**.
   - `anchors_array` on each inventory's mined rows equals `ga.load_anchors()[inv]`
     (`np.array_equal`) for **467/467** inventories.
3. **Anchors from the region table.** For every inventory in the mined CSV that has a
   regions file, compare `page_number_anchors(regions)` with the mined `(scan_id, page_number)`
   pairs (rows with non-null `page_number`). Report how many inventories are identical, and the
   totals of pairs only in mined, only in regions, and in both. This is a measurement, not a
   gate. Inventory 2555 must be identical (374 pairs), or stop and ask.

The user decides when the step is done.

## Error analysis

- For criterion 3, read every differing pair up to 30, then a seeded sample of 30
  (`random.Random(12)`). Classify each into: reading order vs document order (several
  page-number regions on one page), nested or zero-line region, word-break join changed the
  text, different PageXML version (a region present in one source only), other. Give counts
  and verbatim examples (scan id, both texts).
- **Accuracy with region anchors** (measurement, no tuning): rerun the baseline
  `AlignmentConfig()` and the chosen config on the 22 annotated inventories with anchors from
  the regions table. Use NLP's `ga.load_gold_starts()`, `ga.score_start_accuracy(al, gold)` and
  `ga.accuracy_summary` (read-only). Report n / exact / ±1 / ±2 beside the mined-anchor numbers
  (baseline all 22: n 1496, exact .6190, ±1 .8743, ±2 .9298). A drop is a finding; don't
  adjust the extractor to close it.
- Read 5 documents where the confidence band is `high` but the error is >2 (if any exist), and
  say what went wrong (restart, misread number, foliation mistaken for pagination).
- Expected findings: equivalence 6/6 (result), the region-anchor agreement (data-property),
  region-anchor accuracy (result or negative-result), and the quirks kept verbatim (banker's
  rounding, `max_scan or s`, the extrapolated suffix) (method).

## Do not touch

- NLP code. `stream_analysis/` (step 10). `model/` (step 11). `datasets/__init__.py`.

## When done

1. Add `### Step 12: page numbering core` to `reports/phase-04-decoders-and-page-numbering.md`
   (findings `F04-12.n`, status `draft`; long tables go to
   `reports/data/phase-04-decoders-and-page-numbering/*.tsv`); run `make reports` and fix any
   warning it prints.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`. If a name or signature differs from this brief, put the
   final names under *Facts later steps need* (step 12b aliases them).

```markdown
### Step 12: page numbering core
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added>
- **Tests:** <commands> → <results>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-04-decoders-and-page-numbering.md` § Step 12](../../reports/phase-04-decoders-and-page-numbering.md#step-12-page-numbering-core);
  findings F04-12.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```
