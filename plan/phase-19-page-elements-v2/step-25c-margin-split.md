# Step 25c: `page_grid` and `margin_split`, dev grid for bar M2

> **Phase:** 19, Page elements v2. **Depends on:** 25a (`read_scan_sizes`, the dev v2 files). Can run in parallel
> with 25b. **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`,
> then this file. **Read nothing else unless this brief names it.** If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The text layer calls a line `marginalia` by layout role `extra` or by a page-margin rule. In phase 18 that call found
margin lines with P 0.259, R 0.872 on 60 pages: second table columns and register cells count as margin, mostly in
2.10.50 and 2.10.01 registers. This step adds a per-page-side geometry module (`page_grid`, reused by 25d) and a margin
split (`margin_split`) that **removes** candidates from the layer's call by fixed rules, scores a fixed grid of six
cells on dev-A, and writes `select_margin.json`. The test (bar M2) is step 25e; this step never opens test gold.

## Inputs

- `archival_structures/analysis/page_elements.py`: `read_layer_lines(layer_root, inv, scan_ids=None) -> list` (one dict
  per non-empty line in lines-table order; keys `scan_id line_id region_id page_side x y w h text layout_region_id
  layout_column layout_role page line_type type_method n_segments region_tag`; values are strings, convert x y w h to
  int); `layer_call(line) -> dict` (`placement` is `margin` iff `line_type == "marginalia"`); `read_scan_sizes(layer_root,
  inv) -> {scan_id: (page_w, page_h)}` (25a). `page` is the layout's page side: `verso`, `recto` or `single`.
- Dev sets (25a; check `reports/data/phase-19-page-elements-v2/step-25a/dev/SHA256SUMS`): `dev/devA_v2.tsv` (60 pages
  P001–P060, 4,334 lines) and `dev/devB_v2.tsv` (40 pages P061–P100), read with
  `archival_structures.datasets.element_gold.read_gold`. Columns include `page_id stratum scan_id line_id k type
  placement unclear note`.
- Page → layer: `reports/data/phase-18-page-element-types/step-24b/sample_pages.tsv` (`page_id stratum archive inv
  split scan_id layer_root …`); join gold to `read_layer_lines(layer_root, inv, {scan_id})` on **(scan_id, line_id)**,
  asserting 1:1 per page (phase 18 did this for all 100 pages).
- Statistics to copy verbatim from `reports/data/phase-18-page-element-types/step-24d/eval_elements.py`: `prf`,
  `wilson`, `pct`, `counts`, `bootstrap`, `select_cell` (read those functions there) into a shared helper
  `reports/data/phase-19-page-elements-v2/common/p19eval.py`, plus a loader `dev_lines(which) -> list[dict]` (gold row +
  layer line merged, ints for x y w h, plus `page_w`, `page_h`) and `bar_counts(rows, flags, pos_fn, keep_fn)`. 25d and
  25e import it (`sys.path.insert(0, <AS root>/reports/data/phase-19-page-elements-v2/common)`). Use `SEED = 20261117`.
- Planning probe (read it): `reports/data/phase-19-page-elements-v2/planning/probe_margin.py`. Its cells
  `M0`, `M1 inside`, `M9 tabular`, `M8 inside+tabular`, `P_M4`, `P_M5` are this step's M0–M5 and give the expected
  numbers below.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/page_grid.py` (new, stdlib) | geometry per page side (below) |
| `archival_structures/analysis/margin_split.py` (new, stdlib) | `MARGIN_CELLS`, `MARGIN_ORDER`, `margin_calls` |
| `tests/test_analysis_page_grid.py`, `tests/test_analysis_margin_split.py` (new) | synthetic pages |
| `reports/data/phase-19-page-elements-v2/common/p19eval.py` | shared statistics and dev loader |
| `reports/data/phase-19-page-elements-v2/step-25c/eval_margin.py` | `--selftest`, `--stage dev` → `dev_grid.tsv`, `select_margin.json`, `devB_grid.tsv`, `cases_dev_*.tsv` |
| `docs/page_elements.md` | a `## Margin split` section |
| `docs/shared_library/changelog.md` | `### Step 25c: page_grid and margin_split` under `## Unreleased` (Edit only); new modules, no sibling imports affected |
| `reports/phase-19-page-elements-v2.md` | `### Step 25c: …` section, findings `F19-25c.n` |

## Behaviour to implement

`page_grid.py` (lines are dicts as above with int x, y, w, h):
```python
NUMERIC_RE = re.compile(r"[\W\d_ƒfl£xX]*\d[\W\d_]*|[ƒf£]\s*[\d.,:„—-]+.*")
def sides(lines) -> dict              # {(scan_id, page): [line, …]} in input order
def y_overlap(a, b) -> float          # (min(a.y+a.h, b.y+b.h) - max(a.y, b.y)) / max(1, min(a.h, b.h)); may be negative
def x_overlap(a0, a1, b0, b1) -> float  # max(0, min(a1, b1) - max(a0, b0)) / max(1, min(a1 - a0, b1 - b0))
def body_band(side, min_chars=20) -> tuple | None
def is_tabular_side(side, max_median_chars=25, min_columns=3) -> bool
def numeric_share(lines) -> float     # share with NUMERIC_RE.fullmatch(text.strip()); 0.0 for []
def blocks(side) -> list              # maximal runs of consecutive lines with the same layout_region_id
```
- `body_band`: `m` = side lines with `layout_role == "main"`, `line_type != "marginalia"` and `len(text) >= min_chars`;
  if `m` is empty, all side lines. `xs = sorted(x)`, `xe = sorted(x + w)` over `m`; return `(xs[len(xs) // 10],
  xe[len(xe) * 9 // 10])`; None for an empty side.
- `is_tabular_side`: `nm` = side lines with `line_type != "marginalia"`; true if `nm` and `median(len(text) of nm) <
  max_median_chars`, or if the number of distinct `layout_column` values over **all** side lines is ≥ `min_columns`.

`margin_split.py`:
```python
MARGIN_CELLS = {"M0": {}, "M1": {"inside": True}, "M2": {"tabular": True}, "M3": {"inside": True, "tabular": True},
                "M4": {"tabular_and": True}, "M5": {"inside": True, "tabular": True, "max_median_chars": 20}}
MARGIN_ORDER = ["M0", "M1", "M2", "M3", "M4", "M5"]
def margin_calls(lines, cell="M0") -> list[bool]   # one bool per input line, True = margin
```
A **candidate** is a line with `line_type == "marginalia"`; a non-candidate is never margin. Per side: `band =
body_band(side)`, `tab = is_tabular_side(side, max_median_chars=cfg.get("max_median_chars", 25))`. Candidates are
grouped by `layout_region_id`; for each candidate region: `inside` = band is not None and `band[0] < median(x + w/2 of
its candidate lines) < band[1]`; `numeric` = at least 3 candidate lines and `numeric_share ≥ 0.5`. A candidate region is
**dropped** (its lines not margin) when: M1 `inside`; M2 `tab`; M3 `inside or tab`; M4 `tab and (inside or numeric)`;
M5 as M3 with `max_median_chars = 20`. M0 drops nothing. Unknown cell → ValueError.

`eval_margin.py --stage dev`: load dev-A and dev-B; keep lines with `unclear == "0"` and `placement in {"main",
"margin"}`; positive = gold `margin`; flag = `margin_calls(page lines, cell)` (computed per page on all its layer lines,
then looked up per gold line). Per cell: TP FP FN P R F1 → `dev_grid.tsv` (dev-A), `devB_grid.tsv`. `select_cell`
with tie band 0.01 in `MARGIN_ORDER` on dev-A F1 → `select_margin.json` {cell, dev_A: {P, R, F1, TP, FP, FN}, intervals
(Wilson and page bootstrap, 2,000, seed 20261117), dev_B, time, devA_sha256}. Refuse to overwrite `select_margin.json`
(exit 2). It never opens `reports/data/phase-19-page-elements-v2/step-25b/gold/`. `--selftest`: asserts for the copied
statistics (as in 24d's selftest) and the select rule.

## Constraints

- Additive: no change to `page_elements.py`'s existing functions, no layer rebuild, no text-layer change.
- Never open test gold (`step-25b/gold/`), the phase-18 `test_lines.tsv` is allowed (it is dev-A's layer join).
- Never `git commit`; `chmod g+w`; no `rm -rf`, `nohup`, `&`, `$TMPDIR`; zsh; BSD `sed -i ''`; scripts resolve
  paths against the AS root (docstring says "run from the AS root"). Git ownership: see 25a (`GIT_CONFIG_*` override).

## Tests

- `page_grid`: `sides` keys; `y_overlap` of identical, disjoint and half-overlapping boxes; `body_band` with and without
  qualifying main lines; `is_tabular_side` by median and by column count; `numeric_share` on "ƒ 24„", "115„16",
  "Memorie" (not numeric); `blocks`.
- `margin_split`: a synthetic opening (verso and recto sides) where the recto margin note sits mid-scan but outside the
  recto band (kept by M1); a register side with a short-line main column and a second column (dropped by M2); a numeric
  margin column on a tabular side (dropped by M4); M0 = layer call; unknown cell raises.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_page_grid.py tests/test_analysis_margin_split.py`, then the full suite.

## Acceptance criteria

1. dev-A, 94 positives (measured at planning by the probe with the same rules): M0 TP 82 FP 234 FN 12 (P 0.259; this is
   phase 18's bar M on these pages); M1 71/130/23; M2 54/9/40; M3 47/0/47; M4 78/132/16; M5 47/2/47. If a number
   differs, stop and report the difference and its cause before selecting.
2. `select_margin.json` written once; by the planning numbers the selected cell is M2 (F1 0.688; M3 0.667, M5 0.657
   are outside the 0.01 band). dev-B numbers reported.
3. Suite green (count = 25a's + your new tests); `make reports` clean.

## Error analysis

- For the selected cell on dev-A: every false positive (expected 9) and 30 of the false negatives (all if fewer;
  `random.Random(20261117)`), read with two lines above and below and the page image
  (`data/page_elements_images/<page_id>.*`). Categories, fixed now: FN — margin note on an account or register side
  (tabular side), note inside the body band, margin line the layer never called (the 12 of M0), a gold error; FP — table
  column outside the band on a non-tabular side, heading or salutation in an extra region, a gold error.
- Per stratum: the cell's TP/FP/FN, and which strata carry the dropped true margins (planning: mostly sg_hand account
  pages, e.g. P002–P004 "Memorie", "x £").
- dev-B: the same table, read 10 lines; note where its agent conventions differ.
Conclude: what a margin detector would need beyond removal (it cannot add a line, so R ≤ 0.872 on dev-A).

## Do not touch

- `data/annotations/`, `shared-tools/`, text-layer modules and presets, phase-18 files, `step-25b/`.

## When done

1. Add `### Step 25c: page_grid and margin_split` to `reports/phase-19-page-elements-v2.md` with findings `F19-25c.n`
   (draft); run `make reports`.
2. Append a PROGRESS block (template fields: session, status, files, public API, tests, acceptance, report link
   `../../reports/phase-19-page-elements-v2.md#step-25c-page_grid-and-margin_split`, error analysis, deviations, brief
   defects, open questions).
3. Overwrite STATUS: 25c done, the selected cell, next 25d.
