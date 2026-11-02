# Step 23e: Locator L9 and the monotone filter; VOC links with the 23c map (bars L and E on fresh gold)

> **Phase:** 17, VOC rules v1. **Depends on:** 23c (its `select.json`), 23d (done: both edit `index_links.py`),
> 23a (`gold/agent_gold.done`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 16's VOC locator L8 reads a folio number that stands alone on a TOC line or leads it as a range. With gold TOC
sides it reached E 0.644 against bar 0.90 (F16-22d.1). The read failures were locator errors as often as map errors
(F16-22d.5). L8's fixable defects (F16-22d.6):
- the range connectors `à`, ` / ` and `. . .`;
- trailing ranges at the line end, such as "… in dato 27 Dito - - - „ 656 a 668 verso", which L8 lacks (all 5 "verso"
  misses are of this kind);
- years ("A„o 1738") and day numbers ("dato 14", "tot 14") taken as folios.

The locators of one TOC also increase along it. This step adds L9 and a monotone filter, selects one cell on the 22
GLOBALISE volumes (all dev now, with their document starts as targets, **scoring only**), and tests it **once** on
23a's fresh locator and target gold.

## Inputs

Paths relative to the AS root, checked 2026-11-01. Line numbers predate 23d; use the anchors.
- `archival_structures/analysis/index_links.py`:
  - `LocatorParams` (frozen; anchor `class LocatorParams`): `variant; separators=r"[\s.,:;]+"; ordinal_after="";
    month_after=""; quantity_after=""; amount_before=""; range_slack=6; sub_entry_punct=False; left_boundary=False;
    lead_pattern=""; standalone_pattern=""`.
  - L8 (anchor `VOC_LOCATOR_PARAMS = {"L8"`): `_VOC_PREFIX`, `_VOC_RANGE`, `_STANDALONE_STRIP = " \t„\"'.:,;-—–"`.
  - `extract_locators(text, params, *, folio_range=None) -> list[Locator(number, char_start, char_end)]` dispatches
    `if params.variant == "L8": return _extract_l8(...)`. `_extract_l8` (anchor `def _extract_l8(`):
    1. a standalone fullmatch on the stripped core;
    2. else a lead match;
    3. else `extract_locators(text, dataclasses.replace(params, variant="L7"), folio_range=None)`;
    4. then the range filter `folio_range[0]-range_slack <= n <= folio_range[1]+range_slack`.
  - `longest_increasing_chain` is in `archival_structures/analysis/page_numbering.py` (strict LIS; returns indices).
- **Harness to copy:** `reports/data/phase-16-voc-generalisation/step-22d/eval_links.py`:
  - `load_gold` (l.72; the max over a scan's rows);
  - `Vol` (l.82–161): gold TOC sides, the front/later split at a gap > 20 sides, exclude = front sides plus all TOC
    sides, TOC lines in `lines/` file order, `fmap(mparams, tags)`, `links(cell)` (leaf = `scan_ids[p:p+2]`),
    `coverage`;
  - `score` (l.164: E = hits/links, C = coverage, F1 = 2EC/(E+C));
  - `silver_rows` (l.220);
  - `wilson`.
  - Its cell **C3** = (L8, lines, map `FolioMapParams()`, tags `("page-number",)`).
- **Planning prototype:** `reports/data/phase-17-voc-rules-v1/planning/l9_proto.py` (the patterns below, verbatim).
  Its L7 fallback is `dataclasses.replace(L8, variant="L7")`, as specified below, so these are the expected dev
  numbers on the 22 GLOBALISE volumes with the C3 map:

| Variant | hits/links (E) | coverage |
|---|---|---|
| L8 | 1009/1851 = 0.545 | 790/1526 = 0.518 |
| L9 | 1239/2045 = 0.606 | 971/1526 = 0.636 |
| L9 + mono | 898/1112 = 0.808 | 866/1526 = 0.567 |
| L9g | 1199/1870 = 0.641 | 969/1526 = 0.635 |
| L9g + mono | 901/1104 = 0.816 | 867/1526 = 0.568 |

- **Dev layer** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5`. **Dev gold**
  `reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv` (up to 4 rows per scan: max). Volumes: 1120 1267
  1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834.
- **23c and 23d results** (`STATUS.md`, *Facts later steps need*, and the files):
  - `step-23c/select.json` (SEL23C map fields; tags `("page-number",)`);
  - `step-23d/select.json` (the TOC cfg) and VOC_RUN = `dict(min_flagged=1, max_first=<window>, gap=20,
    min_after_frac=0.5)`;
  - the `toc_sides` signature: `toc_sides(side_texts, side_scan_index, cfg) -> list[bool]` in
    `archival_structures/analysis/index_pages.py`.
- **Test layer** `data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5`. **Test gold** (open only in
  `test`), in `reports/data/phase-17-voc-rules-v1/step-23a/gold/`:
  - `toc_labels.tsv` (`inv scan_id scan_num label list_type note reader`; one row per scan);
  - `link_lines.tsv` (`inv scan_id page line_id line_idx text gold_folios status note`; **one row per sampled
    line**; `gold_folios` is `;`-separated, empty = none);
  - `link_targets.tsv` (`inv line_id folio target_scan status confidence evidence reader`; **one row per (line,
    folio)**; `status` is `found` or `not_found`).
- Silver: the OBP CSV, read via `archival_structures.datasets.globalise_index.load_globalise_index(path,
  inventories=[...])` as in 22d.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_links.py` | `LocatorParams` fields `trail_pattern=""`, `trail_num_pattern=""`, `guard_before=""`, `guard_after=""`; dispatch `variant in ("L8", "L9", "L9g")` to `_extract_l8`; its new arms; `VOC_LOCATOR_PARAMS["L9"]`, `["L9g"]`; `monotone_mask(numbers) -> list[bool]` |
| `tests/test_analysis_index_links.py` | `test_l9_connectors`, `test_l9_trailing_range_verso`, `test_l9g_guards`, `test_l8_unchanged`, `test_monotone_mask` |
| `reports/data/phase-17-voc-rules-v1/step-23e/` | `eval_links17.py` (stages `l8check`, `dev`, `test`, `errors`), `l8check.json`, `dev_grid.tsv`, `select.json`, `test_done.json`, `test_links.tsv`, `test_lines.tsv`, `end_to_end.tsv`, `silver.tsv`, `cases_read.tsv` |
| `docs/index_links.md` | paragraph *L9 and the monotone filter (phase 17)* |
| `docs/shared_library/changelog.md` | `### Step 23e: locator L9, monotone_mask` under `## Unreleased`; "no sibling imports affected" |
| `reports/phase-17-voc-rules-v1.md` | `### Step 23e: …`, findings `F17-23e.n` |

## Behaviour to implement

**Patterns** (module constants next to the L8 ones; all matched with `re.I`):
```python
_VOC9_PRE = r"(?:(?:fo|fol|folio|pag|pag:s|p)\.?\s*)?"
_VOC9_CON = r"(?:a|à|tot|en|—|–|-|/|(?:\.\s*){2,3})"
_VOC9_NUM = _VOC9_PRE + r"(\d{1,4})(?:\s*(?:verso|v)\.?)?"
_VOC9_RANGE = _VOC9_NUM + r"\s*" + _VOC9_CON + r"\s*" + _VOC9_PRE + r"\d{1,4}(?:\s*(?:verso|v)\.?)?"
_VOC9_MONTHS = r"(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)"
```
- `VOC_LOCATOR_PARAMS["L9"]` = `dataclasses.replace(VOC_LOCATOR_PARAMS["L8"], variant="L9", ...)` with:
  - `standalone_pattern=r"(?:" + _VOC9_RANGE + r"|" + _VOC9_NUM + r")"`;
  - `lead_pattern=r"\W{0,3}" + _VOC9_RANGE + r"\b"`;
  - `trail_pattern=r"(?:^|[\s„\-.])" + _VOC9_RANGE + r"\W*$"`;
  - `trail_num_pattern=r"(?:[-.]\s*){3,}\W{0,3}" + _VOC9_NUM + r"\W*$"`.
- `VOC_LOCATOR_PARAMS["L9g"]` = L9 with `variant="L9g"` plus:
  - `guard_before=r"(?:dato|den|tot|primo|ulto?)\s*$|(?:a[„o\.]*o|anno|ao)\W*$|" + _VOC9_MONTHS + r"[a-z]*\.?\s*$"`
    (the last arm, a year after a month, as in "dato 14 Nov. 1670.", was added at planning from dev text);
  - `guard_after=r"^\s*[.,:'\"]*\s*" + _VOC9_MONTHS`.

**`_extract_l8`** for variants L8, L9 and L9g; L8's output must stay byte-equal:
1. Standalone, as now. The locator is the **first non-None group** (L8's patterns have one group).
2. Else lead (`re.match`), the first non-None group.
3. Else, if `trail_pattern` is set, `re.search` it, then `trail_num_pattern`, the first non-None group each.
4. Else the L7 fallback as now. Then, if `guard_before` or `guard_after` is set, drop a locator when
   `re.search(guard_before, text[:char_start], re.I)` or `re.match(guard_after, text[char_end:], re.I)`.
5. The range filter as now.

**`monotone_mask(numbers: Sequence[int]) -> list[bool]`:** True for the members of a longest non-decreasing
subsequence: `longest_increasing_chain([n + i * 1e-6 for i, n in enumerate(numbers)])`.
- It is applied to a volume's locator numbers in TOC reading order: all TOC sides' lines in `lines/` file order, a
  line's locators left to right, as in the prototype.
- Masked locators are dropped before scoring.

**Grid (fixed):** variant {L8, L9, L9g} × mono {off, on} × map {SEL22B = `FolioMapParams()`, SEL23C from
`step-23c/select.json`} = 12 cells, all on lines with tags `("page-number",)`.
- If SEL23C equals SEL22B, there are 6 cells; say so.
- **C3** = (L8, off, SEL22B).

**Stages of `eval_links17.py`** (from the AS root, `-I`; writes only into `step-23e/`):
- `l8check`: L8 locators (number, char_start, char_end) on every line of the gold TOC sides of the 22 dev volumes,
  from the current module and from a `git show HEAD:archival_structures/analysis/index_links.py` copy loaded with
  importlib from the scratchpad. All must be equal; write `l8check.json`. Any difference stops the step.
- `dev`:
  - all 22 volumes, the 22d scoring (E, coverage, F1; leaf `scan_ids[p:p+2]`);
  - first assert the six SEL22B rows of the prototype table (hits/links and coverage, e.g. (L8, off) 1009/1851 and
    790/1526). A mismatch stops the step: record it and ask;
  - `select.json`: max dev F1; within 0.005 of the max, fewer differences from C3; UTC time.
- `test`:
  - **once.** Assert `select.json` exists, `test_done.json` does not, and `step-23a/gold/agent_gold.done` exists;
  - volumes = those in `link_lines.tsv`. TOC sides (oracle) = sides of scans with `label == toc`, split and
    excluded as in `Vol`;
  - for SEL and C3, extract the locators on all TOC lines (folio range = the cell map's chain range), apply mono if
    the cell has it, then score only the sampled lines with `status == ok`:
    - **L:** per line, the predicted multiset P_l against the gold multiset G_l (`gold_folios`).
      TP = Σ|P_l ∩ G_l|, P = TP/Σ|P_l|, R = TP/Σ|G_l|;
    - **E:** per predicted (line, N):
      - hit if N ∈ G_l, its target is `found`, and the target scan ∈ leaf(locate(N));
      - excluded if N ∈ G_l and its target is `not_found`;
      - otherwise a miss (also when `locate` returns None);
      - E = hits / (predicted − excluded);
    - **E_map** (diagnostic): over gold (line, N) with a `found` target, the share where the target lies in
      leaf(locate(N));
  - `test_links.tsv`, `test_lines.tsv`, `test_done.json`.
  - **End to end (reported only):** on each test volume, run 23d's `toc_sides` with its SEL cfg, take
    `front_index_run(flags, **VOC_RUN)`, then SEL locators on the predicted run's lines. Report E on the sampled
    lines that fall on predicted sides, and how many sampled lines do.
  - **Silver (reported only):** precision and recall of SEL and C3 locators against the OBP `folio_start` set per
    volume, as `silver_rows` does.
- `errors`: dumps for the error analysis.

## Constraints

- Additive only. L1–L8 and their params stay unchanged (`l8check`). `volume_links`, `volume_links_v2`, the builder
  and `front_index_run` (23d's) are not touched.
- L9's patterns are fixed above, written from phase-16 dev text. Do not tune them on fresh text.
- **Blindness:** never open `step-23a/dumps/` or `step-23a/reads/`. Open `step-23a/gold/` only in `test`.
- Starts and targets are **scoring only**: no rule reads them (NLP owns start detection).
- Protected: run `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py` before `dev` and after `test`.
- Never `git commit`; never bump the version; `chmod g+w`. Agent shell: no `rm -rf`, `nohup`, `&` or `$TMPDIR`;
  BSD `sed -i ''`; never `git stash`/`checkout`/`reset`.

## Tests

Run at planning with the prototype's `l9` (`folio_range=None`):
- `test_l9_connectors`: L9 on `"763 à 773."` gives [763]; on `"5038. . . 5107."` gives [5038]; on `"122 a 125. /
  Copie"` gives [122]. L8 gives [773], [] and [122].
- `test_l9_trailing_range_verso`: `"Dito missive als vooren in dato 27 Dito - - - - „ 656 a 668 verso"` gives [656]
  with L9.
- `test_l9g_guards`: with L9g, `"dato 14 Nov. 1670."`, `"A„o 1738"` and `"tot 14"` give [];
  `"den 25 Maart 1741 fo 288"` gives [288]. L9 (no guards) gives [1670], [1738], [14] and [288].
- `test_l8_unchanged`: L8 on the 9 strings of this section gives the values the current code gives (pin them).
- `test_monotone_mask`: `[5, 3, 7, 7, 1992, 9]` gives `[False, True, True, True, False, True]` (patience keeps the
  later of two equal-length chains; checked at planning).
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_index_links.py`, then the full suite.

## Acceptance criteria

1. The new tests and the full suite are green; `l8check.json` shows 0 differences.
2. The C3 reproduction holds. `dev_grid.tsv` has 12 (or 6) rows. `select.json` is time-stamped before
   `test_done.json`.
3. **Bars (VOC, fresh):** SEL's L P ≥ 0.95 and R ≥ 0.90, and E ≥ 0.90, each with a Wilson interval. Report C3 beside
   them, plus E_map, the end-to-end E, silver and the exclusion count. A miss is a negative result; no retuning.

## Error analysis

- **Test:** every SEL E miss, up to 30 (seeded 20261101 if more). Categories: not a folio (year, day, amount); range
  end taken; HTR misread; right folio, start on the leaf's neighbour (±1); map error (≥ 2 scans off); subdocument;
  gold doubt; unclear.
- Every L false negative (a gold folio not predicted), up to 20: which arm should have caught it.
- 10 hits, to check they are real.
- Every line where mono dropped a gold folio.
- Expected findings: L and E on fresh gold; how much of E's gap is map (E_map) and how much locator (L); whether
  mono's dev gain holds on fresh TOCs; the end-to-end loss through the detector.

## Do not touch

`folio_map.py`, `index_pages.py`, `presets.py`, the text layer, and the 23a–23d step dirs (except reading
`step-23a/gold/` in `test` and the 23c/23d `select.json`).

## When done

1. Add `### Step 23e: Locator L9 and VOC links` to `reports/phase-17-voc-rules-v1.md` (results, error analysis,
   findings `F17-23e.n`, status `draft`), then run `make reports`.
2. Append the `PROGRESS.md` block (template `plan/templates/STEP.md`).
3. Overwrite `STATUS.md`: 23e done; next action is the phase review.
