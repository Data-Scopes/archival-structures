# Step 25e: Measure on test (bars L2 and M2, items, baselines)

> **Phase:** 19, Page elements v2. **Depends on:** 25b (the user ran `summarise_v2.py`: `step-25b/gold/test_gold.done`),
> 25c (`step-25c/select_margin.json`), 25d (`step-25d/select_elements.json`). **Model:** Sonnet. **Read:**
> `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this file. **Read nothing else unless
> this brief names it.** If you have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Phase 19 built a margin split (25c) and an element reader (25d), selected one cell of each on dev, and froze fresh
test gold on 60 pages (25b, user-corrected). This step first closes 25b (annotations check, an analysis of the user's
corrections), then runs the test stage **once**: bar L2 for the selected element cell, bar M2 for the selected margin
cell, the phase-18 systems (E0 = `listlike` C0R1, M0 = the layer's call) on the same pages, item starts, and an error
analysis. A failed bar is a negative result: no retuning, no second run.

## Inputs

- `reports/data/phase-19-page-elements-v2/step-25b/gold/`: `test_v2.tsv` (columns `GOLD_V2_COLS` + `pre_type
  pre_placement pre_element pre_item_start pre_prose changed_type changed_placement changed_element changed_item
  changed_prose reviewed page_note`; one row per line of a reviewed page), `test_elements.tsv`, `SHA256SUMS`,
  `test_gold.done` (UTC time + sha256 of `test_v2.tsv`). Check the hash first; stop if it fails. Read with
  `element_gold.read_gold`.
- `step-25b/sample_pages.tsv` (`page_id stratum archive inv split scan_id layer_root …`), `step-25b/rulings.md`,
  `step-25b/annotations_list_before.txt`, `step-25b/element_verdicts_v2.json` (page notes).
- `step-25c/select_margin.json`, `step-25d/select_elements.json` (cell names, dev numbers, `time`).
- Code: `analysis/margin_split.margin_calls(lines, cell)`, `analysis/element_reader.element_calls(lines, flags, cell,
  page_w)` and `item_starts(lines, calls, rule, page_w)`, `analysis/page_elements.read_layer_lines`,
  `read_scan_sizes`, `listlike_records`, `listlike.window_features`, `list_flag`, `RULE_R1`;
  `reports/data/phase-19-page-elements-v2/common/p19eval.py` (`prf`, `wilson`, `bootstrap`, `select_cell`, loaders).
- 25d's `eval_elements_v2.py` and 25c's `eval_margin.py`: copy their per-page line building (join on (scan_id, line_id),
  1:1 asserted).

`D = reports/data/phase-19-page-elements-v2/step-25e/`.

## Deliverables

| File | Change |
|---|---|
| `D/close_25b.py` | `annotations_list_after.txt` (same command as before) and its diff (must be empty); `after/` tables |
| `D/eval_test.py` | `--selftest`; `--stage test` (once) → `test_lines.tsv`, `test_eval.tsv`, `test_done.json`, `tables/`, `cases_test_*.tsv` |
| `reports/phase-19-page-elements-v2.md` | `### Step 25b part D: the user's corrections` and `### Step 25e: Measure` sections; findings `F19-25b.n` (continue numbering), `F19-25e.n` |

## Behaviour to implement

**close_25b.py.** `find data/annotations -type f -exec stat -f '%z %N' {} + | sort -k2 > step-25b/annotations_list_after.txt`,
then compare with `_before` (byte-equal, else stop). Tables in `D/after/`: changed share per field (type, placement,
element, item, prose) overall, per stratum and per gold family; the 20 most frequent (pre_type → type) changes with 3
examples each; reviewed pages; every page note; the rulings with the pages they apply from.

**eval_test.py --stage test.** Refuse unless both select files exist, both `time` values precede the time in
`test_gold.done`, and `D/test_done.json` does not exist. Rows of reviewed pages only. Per page: layer lines, C0R1 flags,
`margin_calls` for the selected margin cell **and** M0, `element_calls` for the selected element cell **and** E0,
`item_starts` I1 and I2 on the selected element cell's calls.
- **Bar L2:** rows with `unclear == "0"` and gold family in {running_text, list, table, form}; positive = family ≠
  running_text; flag = the selected element cell. **Bar M2:** rows with `unclear == "0"` and placement in {main,
  margin}; positive = margin; flag = the selected margin cell. Pass if P ≥ 0.90 and R ≥ 0.80. Report TP FP FN P R,
  Wilson intervals and page-cluster bootstrap intervals (2,000 resamples, seed 20261117), verdict.
- **Reported, no bar:** E0 and M0 on the same rows (phase-18 systems on fresh pages); both bars per stratum; both bars on
  the six fresh-volume strata alone (sg_hand, sg_print, voc, na_2.10.01, na_2.10.50, na_1.05.06; the four single-volume
  strata share a volume with dev); L2 recall on lines of `prose = 1` elements; item starts I1/I2 P/R/F1 per family on
  gold list/table/form lines with `unclear == "0"` (positive = gold `item_start == "1"`); dev-A numbers of the same
  cells beside the test numbers.
- `test_lines.tsv`: one row per gold line: ids, text, x y w h, page, layout_role, line_type, gold fields, the flags
  above. `test_eval.tsv`: one row per bar and baseline. `test_done.json`: time, hashes of the gold and both select
  files, cells, pages reviewed and excluded.
- `--selftest`: the refusals (missing file, select time after gold time, existing `test_done.json`), the statistics.

## Constraints

- One test run. If the run fails before writing `test_done.json`, fix the cause and rerun; say so in the report. After
  `test_done.json` exists, nothing is rerun on test.
- No change to any module, cell or constant. A failed bar is reported as a negative result.
- Never `git commit`; `chmod g+w`; no `rm -rf`, `nohup`, `&`, `$TMPDIR`; zsh; BSD `sed -i ''`; scripts resolve paths
  against the AS root. Git ownership: see 25a. Multi-worker jobs through coreshare (none expected).

## Acceptance criteria

1. Annotations before == after; gold hash OK; select times < gold time < `test_done.json` time.
2. 60 reviewed pages (or the number the user reviewed), every gold line joined 1:1.
3. `test_eval.tsv` with L2, M2, E0, M0 rows and verdicts; `make reports` clean.

## Error analysis

Read lines with two above and two below and the image (`data/page_elements_images_v2/<pid>.*`):
- **L2:** all false positives and false negatives up to 30 each (`random.Random(20261117)` sample if more).
  Categories, fixed now: FP — running text smoothed into a group, running text beside an aligned column, short prose on
  a tabular side, a gold error; FN — single-column list/table without flags, prose items of an amount table, form
  values, register cells not paired, a gold error. Add a category only with a verbatim example.
- **M2:** all false positives and 20 false negatives (all if fewer). Categories: FN — note on a tabular side, note
  inside the body band, never called by the layer, a gold error; FP — table column on a non-tabular side, heading or
  salutation in an extra region, a gold error.
- Where test and dev-A differ by more than 0.05 in P or R, the strata that carry the difference, with examples.
- Items: 20 missed and 20 false starts of I1 with categories.
Conclude per bar: did the new system move the phase-18 failure, and what should the next phase build.

## Do not touch

All modules; `step-25b/gold/`, `step-25c/`, `step-25d/` outputs (read only); `data/annotations/`; phase-18 files.

## When done

1. Write the two report sections and findings (draft); run `make reports`.
2. Append a PROGRESS block (template fields; report link `../../reports/phase-19-page-elements-v2.md#step-25e-measure`).
3. Overwrite STATUS: 25e done; next the phase-19 review (Opus).
