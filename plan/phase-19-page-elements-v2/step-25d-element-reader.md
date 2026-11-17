# Step 25d: `element_reader`, dev grid for bar L2, item starts

> **Phase:** 19, Page elements v2. **Depends on:** 25c (`page_grid`, `common/p19eval.py`); the item-start report also
> needs 25b part C (`step-25b/dev_items/`). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session*
> only), the phase `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to
> explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

`listlike` scores each line from a three-line window. In phase 18 it found list/table/form lines with P 0.950, R 0.575
(bar L failed): it misses wrapped table rows, register cells and long list items, because it never sees the element a
line belongs to. This step adds an **element reader**: per page side it groups lines into blocks (runs of one layout
region) and groups (row-paired blocks), calls groups table or list, and smooths `listlike` flags within groups. A fixed
grid of six cells is scored on dev-A and `select_elements.json` is written. The reader also proposes item/row starts,
reported without a bar. It reads all lines of a side, margin calls included, and never uses `margin_split` (the two
bars are independent). This step never opens test gold.

## Inputs

- `archival_structures/analysis/page_grid.py` (25c): `sides(lines)`, `y_overlap(a, b)`, `x_overlap(a0, a1, b0, b1)`,
  `blocks(side)`, `is_tabular_side(side, max_median_chars=25, min_columns=3)`. Lines are `read_layer_lines` dicts with
  int x, y, w, h (keys as 25c lists).
- `archival_structures/analysis/listlike.py`: `window_features(lines, window=3, cfg=None)`, `list_flag(features,
  rule=None)`, `LISTLIKE_CELLS` (`"C0R1": (None, RULE_R1)`); `page_elements.listlike_records(lines)` builds its input
  (`text`, `x0`, `x1`, `group = (scan_id, layout_region_id)`). The C0R1 flag per line is
  `list_flag(window_features(listlike_records(lines), cfg=None), rule=RULE_R1)` over a page's lines in layer order,
  exactly as `reports/data/phase-18-page-element-types/step-24d/eval_elements.py` (`page_lines`) computes it.
- `reports/data/phase-19-page-elements-v2/common/p19eval.py` (25c): statistics and `dev_lines(which)`.
- Dev sets: `step-25a/dev/devA_v2.tsv`, `devB_v2.tsv`; item starts (when 25b part C is done):
  `step-25b/dev_items/devA_v2.tsv`, `devB_v2.tsv` (same columns, `item_start` filled on list/table/form lines; check
  `dev_items/SHA256SUMS`). Page sizes: `page_elements.read_scan_sizes`.
- Planning probe (read it): `reports/data/phase-19-page-elements-v2/planning/probe_elements.py`. Its cells `E0`,
  `Eshort`, `E2g`, `E4s3`, `E5cap` are this step's E0, E1, E2, E4, E5 and give the expected numbers below; E3 was not
  probed.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/element_reader.py` (new, stdlib + AS imports) | `ELEMENT_CELLS`, `ELEMENT_ORDER`, `ITEM_MARKER`, `read_side`, `element_calls`, `item_starts` |
| `tests/test_analysis_element_reader.py` (new) | synthetic sides |
| `reports/data/phase-19-page-elements-v2/step-25d/eval_elements_v2.py` | `--selftest`, `--stage dev` → `dev_grid.tsv`, `devB_grid.tsv`, `select_elements.json`, `dev_items.tsv`, `cases_dev_*.tsv` |
| `docs/page_elements.md` | a `## Element reader` section |
| `docs/shared_library/changelog.md` | `### Step 25d: element_reader` under `## Unreleased` (Edit only); new module, no sibling imports affected |
| `reports/phase-19-page-elements-v2.md` | `### Step 25d: …` section, findings `F19-25d.n` |

## Behaviour to implement

```python
ELEMENT_CELLS = {"E0": {}, "E1": {"table": True}, "E2": {"table": True, "smooth": 0.5},
                 "E3": {"table": True, "smooth": 0.5, "lists": True}, "E4": {"table": True, "smooth": 0.3},
                 "E5": {"table": True, "smooth": 0.5, "tabular_side": 40}}
ELEMENT_ORDER = ["E0", "E1", "E2", "E3", "E4", "E5"]
ITEM_MARKER = re.compile(r"\s*(item|idem|een|twee|drie|vier|[0-9]+[.)°]|[a-z][.)]|[-–•*])(\s|$)", re.I)
def read_side(side) -> list[dict]          # groups of one side: [{"blocks": [[line…]…], "lines": [...], "rows": [[line…]…]}]
def element_calls(lines, flags, cell="E0", page_w=None) -> list[bool]   # per input line; flags = C0R1 per line
def item_starts(lines, calls, rule="I1", page_w=None) -> list[bool]     # per input line
```
**Grouping** (`read_side`, per side from `page_grid.sides`, lines in input order):
1. `bl = page_grid.blocks(side)`; hull of block b = `(min x, max x + w)`.
2. For every pair i < j: skip if `x_overlap(hull_i, hull_j) >= 0.1`; else let `a` be the shorter block (`a = bl[i]` on
   equal length) and `b` the other; pair them if the share of lines of `a` that have some line of `b` with
   `y_overlap >= 0.5` is ≥ 0.5. Groups = connected components of the pairs (union-find); an unpaired block is a group.
3. Rows of a group: its lines sorted by `y + h / 2`; a line joins the last row if `y_overlap(line, last_row[0]) >= 0.5`,
   else it opens a row.

**Calls** (`element_calls`), per group with lines `gl`: `flag(l)` = the C0R1 flag. `table` = ≥ 2 blocks and ≥ 3 rows
and the share of rows with ≥ 2 lines ≥ 0.5 **and** `median(len(text) for gl) < 30`. `smooth(s)` = `len(gl) >= 4` and
`sum(flags of gl) / len(gl) >= s`. `lists` = `len(gl) >= 4` and (indent share ≥ 0.3 or marker share ≥ 0.4), where a
line is indented if `x >= min(x of its block) + 0.02 * page_w` and a marker line if `ITEM_MARKER.match(text)`.
`tab_side` = `is_tabular_side(side)`. A line is positive if: E0 `flag`; E1 `flag or table`; E2 E1 `or smooth(0.5)`;
E3 E2 `or lists`; E4 E1 `or smooth(0.3)`; E5 E2 `or (tab_side and len(text) < 40)`. E3 needs `page_w` (ValueError if
None). Unknown cell → ValueError.

**Item starts** (`item_starts`, reported only): within each group, consider only lines with `calls` true, in row order.
I1: a line starts an item if it is the first such line of its group; or, in a `table` group, it is the first line of a
row that follows a row containing a numeric line (`page_grid.numeric_share([line]) == 1`) of a block other than the
group's longest block; or, in other groups, it matches `ITEM_MARKER` or its x is ≥ 0.015 × page_w left of the previous
line's x. I2: I1, or the previous line of the same block ends in an amount (`re.search(r"\d[\d.,:„—-]*\s*[„—-]?$",
text)`).

**`eval_elements_v2.py --stage dev`:** for each dev page, layer lines and C0R1 flags as in 24d; calls per cell on all
page lines; bar-L rows = gold lines with `unclear == "0"` and family in {running_text, list, table, form}; positive =
family ≠ running_text. Per cell TP FP FN P R F1 → `dev_grid.tsv` (dev-A), `devB_grid.tsv`. `select_cell` (tie band
0.01, `ELEMENT_ORDER`, dev-A F1) → `select_elements.json` {cell, dev_A, intervals (Wilson, page bootstrap 2,000, seed
20261117), dev_B, time, devA_sha256}; refuse to overwrite (exit 2). Items: if `step-25b/dev_items/` exists, for the
selected cell's calls, I1 and I2 against gold `item_start` on gold list/table/form lines with `unclear == "0"`: TP FP
FN P R F1 per family → `dev_items.tsv`; otherwise write `dev_items_pending.txt` and say so in STATUS. Never opens
`step-25b/gold/`. `--selftest` as 25c's.

## Constraints

- Additive; no change to `listlike.py` (verbatim port) or `page_elements.py`; `element_reader` does not import
  `margin_split`.
- Never open test gold. Never `git commit`; `chmod g+w`; no `rm -rf`, `nohup`, `&`, `$TMPDIR`; zsh; BSD `sed -i ''`;
  scripts resolve paths against the AS root. Git ownership: see 25a.

## Tests

`read_side` pairs a description column with an amount column (rows aligned) and does not pair two columns that overlap
in x; rows; E1 calls a short register table and not a running-text column with a margin note beside it (median ≥ 30);
E2 smoothing; E3 indent and markers (with `page_w`), ValueError without it; E4 at 0.3; E5 on a tabular side; I1/I2 on a
two-column inventory (item after an amount) and on a marker list. Then the full suite.

## Acceptance criteria

1. dev-A (1455 positives, 2153 negatives, as phase 18's bar L; measured at planning by the probe with the same rules): E0 TP 836
   FP 44 FN 619 (= phase 18's C0R1 bar L on these pages); E1 900/87/555 (P 0.912, R 0.619); E2 1,097/105/358 (0.913,
   0.754); E4 1,186/166/269 (0.877, 0.815); E5 1,376/436/79 (0.759, 0.946); E3 not probed (report it). If a probed
   number differs, stop and report the difference and its cause before selecting.
2. `select_elements.json` written once. By the planning numbers E4 (F1 0.845) and E5 (0.842) tie within 0.01, so E4
   is selected unless E3 scores F1 ≥ 0.835 (then E3 is in the tie band and, being earlier in the order, wins).
3. Suite green; `make reports` clean.

## Error analysis

- Selected cell on dev-A: 30 false positives and 30 false negatives (all if fewer; `random.Random(20261117)`), read
  with context and the image. Categories, fixed now: FP — running text smoothed into a list group, running text beside
  an aligned column, short prose lines on a tabular side, a gold error; FN — single-column list or table without
  flags, prose items of an amount table (`prose = 1` elements), form values, a gold error.
- Recall per gold family and per stratum; recall on dev-A `prose = 1` elements (16 pages carry it).
- What E1–E5 each add over E0 (lines gained and lost per stratum).
- Items (if available): 20 missed and 20 false item starts for I1, with categories (item continues after an amount,
  header row, wrapped description, marker in running text).

## Do not touch

`data/annotations/`, `shared-tools/`, text-layer modules, phase-18 files, `step-25b/`, `margin_split.py`.

## When done

1. Add `### Step 25d: element_reader` to `reports/phase-19-page-elements-v2.md` (findings `F19-25d.n`, draft); run
   `make reports`.
2. Append a PROGRESS block (template fields; report link `../../reports/phase-19-page-elements-v2.md#step-25d-element_reader`).
3. Overwrite STATUS: 25d done, the selected cell, whether the item report is pending, next 25e (waits on
   `step-25b/gold/test_gold.done`).
