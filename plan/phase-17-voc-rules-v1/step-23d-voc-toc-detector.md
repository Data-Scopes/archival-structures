# Step 23d: One-column TOC detector with a front window and a ledger guard; front run scaled to volume length

> **Phase:** 17, VOC rules v1. **Depends on:** dev stage: nothing; test stage: 23a's `gold/agent_gold.done`. May
> start in parallel with 23a, 23b and 23c. 23e runs after this step (both edit `index_links.py`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The SG index-side detector (phase 13) looks for two-column printed indexes with numbers at line ends. On the VOC
tables of contents (TOCs, *Register der brieven*), which are one-column handwritten lists, it reached at best P 0.067
and R 0.393 against bar D (P ≥ 0.95, R ≥ 0.90) (F16-22c.1). Its false positives were ledger pages (F16-22c.4), and
`front_index_run`'s `min_flagged` 10 and `min_after` 300 found a run in 0 of 22 volumes (F16-22c.6).

This step adds a detector built on what the VOC TOCs share: a front window, lines that are or carry folio numbers or
ranges, dated entries, a register title, and a guard against amounts. It also adds a length-scaled `min_after` to
`front_index_run`, selects one cell on the 22 GLOBALISE volumes (all dev now), and tests it **once** on the fresh
TOC labels from 23a.

## Inputs

Paths relative to the AS root, checked 2026-11-01.
- `archival_structures/analysis/index_pages.py` (70 lines): `INDEX_SIDES_DEFAULTS`, `side_features`, `side_rule`,
  and `fill_runs(flags, gap, min_run) -> list` (l.50–70: fills runs of ≤ `gap` false flags between true flags,
  then drops true runs shorter than `min_run`).
  - **Do not import `text_layer.presets` here** (`analysis/page_structure.py` imports this module). Copy the month
    alternation verbatim from `presets.py` l.93:
    `(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)`.
- `archival_structures/analysis/index_links.py` l.17–34 (anchor `def front_index_run(`):
```python
def front_index_run(flags: Sequence[bool], *, min_flagged: int = 10, max_first: int = 10, gap: int = 20,
                    min_after: int = 300, max_share: float = 0.6) -> Optional[tuple]:
    # None if flagged share > max_share; end = start of the first run of `gap` unflagged sides (None if no such run);
    # idx = flagged sides before end; None if len(idx) < min_flagged or idx[0] >= max_first or n - end < min_after;
    # else (idx[0], idx[-1], end)
```
- **Harness to copy:** `reports/data/phase-16-voc-generalisation/step-22c/eval_toc.py`. Copy, don't import:
  - `Vol` (l.58–91): loads `scans/`, `lines/` (x, w, text) and `layout/lines` pages; its side keys are sorted by
    (scan order, `layout.PAGE_ORDER[page]`, page);
  - `load_gold`; `score`; `prf`; `wilson`;
  - the phase-16 cell machinery: `SG`, `VOC_TOC_CUE` (l.22), and `flags(cfg)` = `side_rule` + `fill_runs` over
    `{**SG_INDEX_SIDES_V5, **SG, **cfg}`.
  - 22c's selected cell **PH16** = `dict(text_min_chars=8, shape_only_min=0.25, date_pattern=VOC_TOC_CUE, gap=2)`.
- **Planning prototype:** `reports/data/phase-17-voc-rules-v1/planning/toc_proto.py` (the features and rule below,
  verbatim) and `front_proto.py`. Results on all 22 GLOBALISE volumes (scan level, gold collapsed with max per scan):
  - the cell window 60, toc_min 0.2, gap 4, title on: TP 106, FP 48, FN 80 (P 0.688, R 0.570, F1 0.624), the same
    for amount guard off, 0.15 and 0.3;
  - without a window: F1 0.32. Phase 16's best dev F1 was 0.300;
  - FN concentrate in 2548 (35), 2448 (19) and 2555 (17): ship lists with folio columns apart, and mid-volume TOCs;
  - with these flags, `front_index_run` finds a run in 7 of 22 volumes under the SG defaults, and in 21 of 22 with
    `min_flagged=1, max_first=60, gap=20, min_after=min(300, ceil(0.5·n))`. The 22nd, 8023, has no TOC.
- **Dev layer:** `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5` (protected).
- **Dev gold:** `reports/data/phase-16-voc-generalisation/planning/toc_gold.tsv` (`inv row scan_id toc start
  split`). **Row grain: up to 4 rows per scan; collapse with max per scan.** It covers 22 volumes, all dev now:
  1120 1267 1274 1388 1529 1539 1547 1557 2448 2548 2555 2775 3142 3891 7923 8023 8121 8237 8276 8284 8697 8834.
- **Test layer:** `data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5` (built by 23a).
- **Test gold** (open only in the test stage): `reports/data/phase-17-voc-rules-v1/step-23a/gold/toc_labels.tsv`.
  - Columns: `inv scan_id scan_num label list_type note reader`. **Row grain: one row per scan.**
  - It covers scans 1..max(60, last toc + 10) of each volume read.
  - Positives: `label == toc` (any `list_type`). Negatives: `no`. `no_text` and `unclear` are left out of P and R.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_pages.py` | `TOC_SIDES_DEFAULTS`, `toc_side_features`, `toc_side_rule`, `toc_sides` (below) |
| `archival_structures/analysis/index_links.py` | `front_index_run(..., min_after_frac: Optional[float] = None)` (below); nothing else |
| `tests/test_analysis_index_pages.py` | `test_toc_side_features`, `test_toc_side_rule_window_and_guard`, `test_toc_sides_fill` |
| `tests/test_analysis_index_links.py` | `test_front_index_run_min_after_frac` (default unchanged; frac 0.5 finds the run in a 200-side volume) |
| `reports/data/phase-17-voc-rules-v1/step-23d/` | `eval_toc17.py` (stages `dev`, `test`, `cases`), `dev_grid.tsv`, `select.json`, `test_done.json`, `scan_eval.tsv`, `front_runs.tsv`, `cases_read.tsv` |
| `docs/index_links.md` | paragraph *VOC TOC detector (phase 17)* |
| `docs/shared_library/changelog.md` | `## Unreleased` above `## v0.21.0` if absent; `### Step 23d: TOC detector, front_index_run min_after_frac`; "no sibling imports affected" |
| `reports/phase-17-voc-rules-v1.md` | `### Step 23d: …`, findings `F17-23d.n` |

## Behaviour to implement

In `index_pages.py`:
```python
_TOC_MONTHS = r"(jan|feb|...|dec)"   # verbatim copy, see Inputs
_TOC_NUM = r"(?:fo\.?|fol\.?|f\.|pag\.?)?\s*\d{1,4}"
_TOC_RANGE = _TOC_NUM + r"\s*(?:verso\s*)?(?:a|à|tot|en|—|–|-|/|\.\s*\.\s*\.?)\s*\d{1,4}"
TOC_SIDES_DEFAULTS = {
    "window": 60, "min_lines": 3, "text_min_chars": 3, "toc_min": 0.2, "amount_max": None, "title": True, "gap": 4,
    "folio_line_pattern": r"^\W{0,3}(?:" + _TOC_RANGE + r"|" + _TOC_NUM + r")\W{0,3}(?:verso\W*)?$",
    "lead_pattern": r"^\W{0,3}" + _TOC_RANGE + r"\b",
    "tail_pattern": _TOC_RANGE + r"\W{0,3}(?:verso\W*)?$",
    "date_pattern": r"^\W{0,2}(?:den\s+)?\d{1,2}\s*[.,:'\"]*\s*" + _TOC_MONTHS,
    "amount_pattern": r"ƒ|\d+\s*[:.]\s*\d{1,2}\s*[:.]\s*\d{1,2}\b|transport|per\s+saldo|debet|credit",
    "title_pattern": r"regist|regyst|inhoud|indice|index"}
```
All patterns are compiled with `re.I`.
- `toc_side_features(texts: Sequence[str], cfg=TOC_SIDES_DEFAULTS) -> dict`:
  - `ls` = the stripped texts with length ≥ `text_min_chars`; `n = len(ls)`;
  - `fol` = the share of `ls` where `folio_line_pattern` matches (`re.match`), or `lead_pattern` matches, or
    `tail_pattern` is found (`re.search`);
  - `date` = the share matching `date_pattern` (`re.match`);
  - `amt` = the share where `amount_pattern` is found (`re.search`);
  - `title` = `title_pattern` is found in any of `ls[:5]`;
  - for n = 0: `dict(n=0, fol=0.0, date=0.0, amt=0.0, title=False)`.
- `toc_side_rule(feat, scan_index: int, cfg=TOC_SIDES_DEFAULTS) -> bool`: `scan_index < window` and
  (`amount_max is None or amt < amount_max`) and ((`n >= min_lines` and `fol + date >= toc_min`) or (`cfg["title"]`
  and `feat["title"]`)). `scan_index` is the 0-based position of the side's scan in the `scans/` table.
- `toc_sides(side_texts, side_scan_index, cfg=TOC_SIDES_DEFAULTS) -> list[bool]`: the rule per side, then
  `fill_runs(rules, cfg["gap"], 1)`.
- Merge the cfg as `{**TOC_SIDES_DEFAULTS, **cfg}`, and raise `ValueError` on an unknown key.

`front_index_run`: add `min_after_frac: Optional[float] = None`. The tail test becomes
`n - end < need`, with `need = min_after if min_after_frac is None else min(min_after, math.ceil(min_after_frac * n))`.
With the default, the function must behave as today.

**Grid (fixed):** `window` {45, 60} × `toc_min` {0.15, 0.2, 0.3, 0.4} × `amount_max` {None, 0.3} × `gap` {0, 2, 4} ×
`title` {False, True} = 96 cells. The rest is `TOC_SIDES_DEFAULTS`.

**Stages of `eval_toc17.py`** (from the AS root, `-I`; writes only into `step-23d/`):
- `dev`:
  - side texts from `Vol`, `scan_index` from the `scans/` order;
  - a scan is predicted if any of its sides is flagged, and scored over the collapsed gold rows;
  - first assert that the cell (60, 0.2, None, 4, True) gives TP 106 / FP 48 / FN 80 (the planning prototype).
    A mismatch stops the step: record it and ask;
  - score the 96 cells and PH16;
  - `select.json`: max dev F1; within 0.005 of the max, fewer differences from `TOC_SIDES_DEFAULTS`, then the
    smaller `window`; with a UTC time;
  - `front_runs.tsv`: per volume, `front_index_run` on the SEL side flags with the SG defaults and with
    **VOC_RUN** = `dict(min_flagged=1, max_first=<SEL window>, gap=20, min_after_frac=0.5)`.
- `test`:
  - **once.** Assert that `select.json` exists, `test_done.json` does not, and `step-23a/gold/agent_gold.done`
    exists;
  - SEL and PH16 on the fresh volumes: P, R, F1, Wilson, per volume, recall per `list_type`;
  - an exact two-sided McNemar test of SEL against PH16 on per-scan correctness (pred == gold) over the labelled
    scans;
  - front runs with VOC_RUN and the SG defaults;
  - `scan_eval.tsv` and `test_done.json`.
- `cases`: dumps for the error analysis (first 5 lines and the features of each case).

If SEL passes bar D, add `VOC_TOC_SIDES_V1 = {<SEL cfg>}` to `index_pages.py` and
`VOC_FRONT_RUN_V1 = dict(min_flagged=1, max_first=<SEL window>, gap=20, min_after_frac=0.5)` to `index_links.py`,
each pinned by a test. Otherwise add neither.

## Constraints

- Additive only. `INDEX_SIDES_DEFAULTS`, `side_features`, `side_rule` and `fill_runs` stay unchanged, and so does
  the text layer (the detector is not a layer stage).
- **Blindness:** never open `step-23a/dumps/` or `step-23a/reads/`. Open `step-23a/gold/` only in `test`. Run
  nothing on the 23a layer before `test`.
- No pattern may be written from fresh text. The patterns above come from the phase-16 dev TOCs.
- Protected: run `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py` before `dev` and after `test`.
- Never `git commit`; never bump the version; `chmod g+w`. Agent shell: no `rm -rf`, `nohup`, `&` or `$TMPDIR`;
  BSD `sed -i ''`; never `git stash`/`checkout`/`reset`. Single process.
- Parallel sessions: edit only your own `STATUS.md` row; shared files with Edit only. 23c edits
  `folio_map.py` at the same time; do not touch it.

## Tests

- `test_toc_side_features`: the lines `["Register der brieven", "„ 288 a 291.", "den 25 Maart 1741", "Copie missive
  aan", "„ 656 a 668 verso"]` give `n=5`, `fol=0.4`, `date=0.2`, `amt=0.0`, `title=True`.
- `test_toc_side_rule_window_and_guard`: the same features flag at `scan_index` 10 and not at 60. With
  `amount_max=0.3`, a side with `amt=0.5` is not flagged.
- `test_toc_sides_fill`: rules `[T, F, F, T]` with `gap` 2 give four flags; with `gap` 1 they stay as they were.
- `test_front_index_run_min_after_frac`: 200 flags, 0–5 true. The default returns None; `min_flagged=1,
  min_after_frac=0.5` returns `(0, 5, 6)`.
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_index_pages.py tests/test_analysis_index_links.py`,
  then the full suite.

## Acceptance criteria

1. The new tests and the full suite are green. `front_index_run` with default arguments gives the same output as
   before on the 22 GLOBALISE flag lists from the layer's `layout/pages` (compare against a `git show HEAD:` copy).
2. The planning cell reproduces (106/48/80). `dev_grid.tsv` has 96 rows plus PH16. `select.json` is time-stamped
   before `test_done.json`.
3. **Bar D (VOC):** SEL scan-level P ≥ 0.95 and R ≥ 0.90 on the fresh labels (Wilson reported). Also report PH16
   and the McNemar b, c and p. A miss is a negative result; no retuning.
4. Front runs reported (no bar): the number of dev and fresh volumes with a run under VOC_RUN and under the SG
   defaults.

## Error analysis

- **Test:** every SEL FN and FP, up to 30 each (seeded 20261101 if more), with the first 5 lines and the features.
  - FN categories: register with folio numbers apart from the entries; alphabetical index; title page; very short
    page; TOC beyond the window; other.
  - FP categories: account or ledger page; letter or prose; front matter (cover, volume title); a list that does not
    point into the volume; label doubt.
- **Dev:** 15 FN and 15 FP of SEL (seeded), with the same categories, so the dev/test difference can be read.
- Is the ledger guard inert inside the window on the fresh volumes too? Count the sides it blocks.
- Expected findings: bar D on fresh gold; whether the window makes the guard redundant; the front-run counts; the
  FN class that dominates.

## Do not touch

`folio_map.py`, `presets.py`, the text layer, the L1–L8 locator code, and the 23a–23c, 23e step dirs (except reading
`step-23a/gold/` in `test`).

## When done

1. Add `### Step 23d: One-column TOC detector` to `reports/phase-17-voc-rules-v1.md` (results, error analysis,
   findings `F17-23d.n`, status `draft`), then run `make reports`.
2. Append the `PROGRESS.md` block (template `plan/templates/STEP.md`).
3. Overwrite only the 23d row of `STATUS.md`. Under *Facts later steps need*, add the SEL cfg and VOC_RUN (23e uses
   them for its end-to-end diagnostic).
