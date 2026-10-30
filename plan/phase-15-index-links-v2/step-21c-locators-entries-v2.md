# Step 21c: locators v2 and entry assembly from lines

> **Phase:** 15, Index links v2. **Depends on:** 21a dev gold (for the selection), 21a test gold, and 21b
> (`FOLIO_MAP_V2`) for the test run.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

In v1 (phase 14) an index **entry** is one v5 `main_text` unit on a front-index side, and its **locators** are the folio
numbers found by rule L2. Three problems follow:
- **Two-column indexes:** the hand layout keeps the XML order, so the lines of a two-column index alternate between
  the columns. The units are then cut at almost every line, and the words are paired with the wrong number. In 13 of
  the 29 volumes of this phase's pool, at least 25% of the units on front-index sides have no locator.
- **Right columns typed `marginalia`:** these are in no `main_text` unit, so their entries are lost.
- **L2 accepts a bare space before a capital** (`14 Aug`, `10 Mey`), and its amount rule has no left word boundary
  (`Denhof 354.` loses 354).

This step adds line-level entry assembly (with a column reorder taken over from phase 13) and three new locator
variants. It chooses one assembly and one locator variant on the **dev** gold, then tests them **once** on the test
gold. You do not change the builder (that is 21d).

## Inputs

- `archival_structures/analysis/index_links.py` (184 lines, as checked 2026-10-30; read it whole).
  - The current API (verbatim names):
    - `front_index_run(flags, *, min_flagged=10, max_first=10, gap=20, min_after=300, max_share=0.6)` returns
      `(first, last, end)` or None;
    - `VolumeInputs(inv, scan_ids, sides, flags, numbers)` and `read_volume_inputs(layer_root, inv)`;
    - `SG_MONTHS`;
    - `DEFAULT_LOCATOR_VARIANT = "L2"`;
    - `Locator(number, char_start, char_end)`;
    - `LocatorParams(variant, separators=r"[\s.,:;]+", ordinal_after="", month_after="", quantity_after="",
      amount_before="", range_slack=6)`;
    - `SG_LOCATOR_PARAMS` (keys `"L1"…"L4"`, values `LocatorParams`, all with
      `ordinal_after=r"(e|en|ste|de)\b"`, `month_after=SG_MONTHS`, `quantity_after=r"(maenden|…|stuivers)\b"` and
      `amount_before=r"(f|ƒ|fl|gl|£)\s*\.?\s*$"`);
    - `extract_locators(text, params, *, folio_range=None) -> list[Locator]`;
    - `LINK_COLUMNS`;
    - `volume_links(inputs, units, texts, params, map_params=FolioMapParams())`.
  - How `extract_locators` works:
    - **Runs** are consecutive `(?<!\d)\d{1,4}(?!\d)` matches whose gaps fullmatch `separators`.
    - `rest` is the text after a run with one leading separator match stripped. L1 keeps a run only if `rest == ""`.
      L2–L4 keep it if `rest == "" or rest[0].isupper()`.
    - L3/L4 then drop a single match that has `ordinal_after` right after it, a month or quantity after the
      separators, or `amount_before` in `text[m.start()-4:m.start()]`.
    - L4 keeps `lo-6 ≤ n ≤ hi+6` given `folio_range`.
- `archival_structures/analysis/folio_map.py` after 21b: `build_folio_map(...)`, `FOLIO_MAP_V2`. The map's exclude
  set is `{s for i, s in enumerate(v.sides) if i <= last or v.flags[i]}`.
- Column reorder (phase 13, step 19f): `archival_structures/analysis/page_structure.py`, `_reorder_lines(region, c,
  row_tol_frac)` at about L485 (grep `def _reorder_lines`). Read L463–520 (`_column_geometry`, `_reorder_lines`).
  - It needs `region.lines`, and each line needs `.id .text .metadata .baseline .coords.x/.y/.w/.h`. It returns the
    lines reordered left column then right column, or None when a guard keeps the order.
  - The config is `SG_HAND_COLUMN_ORDER_V5` in `archival_structures/text_layer/presets.py` (grep it).
  - Planner check: it runs on duck-typed `SimpleNamespace` lines with `baseline=None` and `row_tol_frac=None`. On spent
    phase-14 volumes it reordered 48/85 front sides of 3590, 72/114 of 3611, 22/48 of 3254 and 64/85 of 3456. Phase 13
    measured it on hand regions (bar B2, 35/39 = 0.897; on index sides 18 right, 2 wrong); here it runs once per
    *side*.
- `SG_INDEX_NO_CUT = r"[\d\s.,:;]+"` in `presets.py` (phase 13: a line of only numbers and punctuation never starts an
  entry). The word-break characters per inventory are in `presets.SG_HAND_V2.wbc_pinned`
  (`{"default": "-=„", "11281": "-:=„", "3349": "-„", …}`; use `wbc_pinned.get(inv, wbc_pinned["default"])`).
- Layer: `L = data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5`, with `read_tsv_gz` from
  `archival_structures.text_layer.io`. The tables: `lines/<inv>.lines.tsv.gz` (`scan_id … line_id x y w h … text`),
  `units/units/<inv>.tsv.gz` (`scan_id page unit_id unit_order unit_type …`), `units/segments/<inv>.tsv.gz` (`scan_id
  unit_id seg_order line_id seg_idx`), `join/units/<inv>.tsv.gz` (`scan_id unit_id … text`) and
  `join/segments/<inv>.tsv.gz` (`scan_id unit_id seg_order line_id seg_idx char_start char_end …`; offsets into the
  `join/units` text).
- Gold from 21a (`reports/data/phase-15-index-links-v2/step-21a/`):
  - `line_gold.tsv`: `read_id inv scan_id page k line_id col label entry locators note`. Labels are S/C/H/F/X; `entry`
    is an id per side; `locators` is `;`-separated, and `N@A` puts N under entry A;
  - `side_verdicts.tsv`: `read_id split inv … side_verdict used_for`;
  - `targets.tsv`: `target_id read_id split inv entry first_line_id target_locator`;
  - `target_gold.tsv`: `target_id … target_scan_id folio_evidence … content content_sides …`.
  - The split is in `side_verdicts.tsv`. **Dev rows only until the selection is written.**
- Tests: `tests/test_analysis_index_links.py` has 10 tests, with the helpers `_flags`, `TABLE` (a locator case table),
  `_synthetic_inputs`, `_write_layer` (a mini v5 layer in `tmp_path`) and `_load_builder`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_links.py` | new locator variants L5–L7, `LineBox`, `Entry`, `read_side_lines`, `side_reading_order`, `assemble_entries`, `entry_locators`; v1 names unchanged |
| `tests/test_analysis_index_links.py` | the new tests below; the 10 old tests unchanged and green |
| `reports/data/phase-15-index-links-v2/step-21c/eval_links.py` | grid on dev, selection, one test run (below) |
| `…/step-21c/` outputs | `dev_grid.tsv`, `select.json`, `test_done.json`, `test_sides.tsv`, `test_errors.tsv`, `test_targets.tsv`, `eval_links.txt` |
| `docs/shared_library/changelog.md` | `### Step 21c: locators v2, entry assembly` under *Unreleased* (Edit only) |
| `reports/phase-15-index-links-v2.md` | the `### Step 21c: …` section, with draft findings `F15-21c.n` |

## Behaviour to implement

**Locator variants.** `LocatorParams` gets two fields **after** the existing ones, with v1 defaults:
`sub_entry_punct: bool = False` and `left_boundary: bool = False`. Add a module constant
`SG_AMOUNT_BEFORE_V2 = r"(?<![^\W\d_])(f|ƒ|fl|gl|£)\s*\.?\s*$"` and three entries in `SG_LOCATOR_PARAMS`:
- **L5** = L2 with `sub_entry_punct=True`. A run that is not at the end of the text is kept only if `rest[0]` is
  upper-case **and** the separator stripped before `rest` contains one of `.,:;`. This is P9 of phase 14 as written:
  `N[.,:;]` then a capital. A bare space no longer counts.
- **L6** = L5 + the L3 exclusions, with `left_boundary=True`. A candidate whose first digit directly follows a letter
  is dropped (`U1`, `Oorlog1789`, `Thesing7.`, `Cunes94`). `amount_before` = `SG_AMOUNT_BEFORE_V2`, searched in the
  **whole prefix** `text[:m.start()]` when `left_boundary` is set. The 4-character window stays for L3/L4.
- **L7** = L6 + the L4 range filter (`folio_range`).

The exclusions apply for variants in {L3, L4, L6, L7}, and the range for {L4, L7}. L1–L4 behave exactly as before.

**Lines of a side.**
- `@dataclass(frozen=True) class LineBox`: `line_id: str, text: str, x: int, y: int, w: int, h: int, unit_id: str,
  unit_type: str, unit_order: int, seg_order: int`.
- `read_side_lines(layer_root, inv, sides, *, unit_types=("main_text", "marginalia")) -> dict` maps each side in
  `sides` to its `LineBox` list, in **base order** `(unit_order, seg_order)`. Only lines of units whose `unit_type` is
  in `unit_types` are included. A line that is in two units appears once, at its first position. Furniture units
  (page_number, running_header, catch_word, date_header, other) are left out.

**`side_reading_order(lines, cfg) -> tuple[list[LineBox], bool]`.** Build one `SimpleNamespace(id, text, metadata={},
baseline=None, coords=SimpleNamespace(x, y, w, h))` per line. Then call
`page_structure._reorder_lines(SimpleNamespace(lines=objs, id="side", metadata={}), cfg, None)`. Return the reordered
LineBoxes and True, or the base order and False when it returns None. The docstring says this is the phase-13 19f
geometry, used per side and without the F13-R.10 guards.

**`assemble_entries(lines, method, *, end_pattern=SG_ENTRY_END, no_cut=SG_INDEX_NO_CUT, heading_max_words=4,
wbc="-=„", column_cfg=None) -> list[Entry]`.**
- `@dataclass(frozen=True) class Entry`: `kind: str` (`entry`/`heading`), `line_ids: tuple`, `text: str` and `spans:
  tuple` of `(line_id, start, end)` offsets into `text`.
- `SG_ENTRY_END = r"(?<!\w)\d{1,4}[\s.,:;]*$"`.
- The methods:
  - **A1:** the line rule on the base order;
  - **A2:** `side_reading_order(lines, column_cfg)`, then the line rule;
  - **A3:** A2 plus the heading rule.
- **Line rule.** Walk the lines and skip empty stripped texts.
  - A line whose stripped text fullmatches `no_cut` goes to the previous entry, if the current entry is empty and a
    previous entry exists.
  - Otherwise the line joins the current entry. If its stripped text matches `end_pattern` (search), the entry closes.
  - At the end, a non-empty current entry closes.
- **Heading rule (A3).** When the current entry is empty, a line whose stripped text has no digit and at most
  `heading_max_words` whitespace-separated words becomes an `Entry(kind="heading")` of its own.
- **Text.** Join stripped line texts with `" "`. If a line ends in a character of `wbc` and is not the last line, drop
  that character and join without a space. `spans` record each line's part of the text.

**`entry_locators(entry, params, *, folio_range=None) -> list[(Locator, line_id)]`.** Run `extract_locators` on
`entry.text`. Each locator's line is the span that contains its `char_start`. Headings give none.

**`eval_links.py`** (run from the AS root; it writes into `step-21c/` only).
- **Gold per side.**
  - Gold locators are the multiset of `(line_id, N)` over `line_gold` rows; `N@A` counts on that line.
  - A gold link is `(line_id, N, first_line_id of its entry)`. The entry is the row's own `entry`, or A for `N@A`, and
    an entry's first line is its `S` line.
  - Only `index` sides count, together with the reserves used.
  - **Embedded entries** (`step-21a/embedded_entries.tsv`: `read_id inv k line_id text locators note`; added
    2026-10-30): entries of the other column that lie wholly inside a merged line. Their `(line_id, N)` **count as
    gold locators** (P/R). For links and attribution they are **neutral**: a predicted link whose `(line_id, N)`
    matches an embedded locator (multiset) is dropped from link P/R/F1 and from A, as is the gold side of it. Report
    the number of neutral pairs per side in `dev_grid.tsv` (`n_neutral`) and `test_sides.tsv`.
- **Predictions per side and method.**
  - **A0** = v1: the side's `main_text` units, text from `join/units`, locators by `extract_locators` on the unit text.
    A locator's line is the `join/segments` row with `char_start ≤ loc.char_start < char_end`. The entry's first line
    is the unit's lowest `seg_order` line.
  - **A1–A3** use `read_side_lines` and `assemble_entries` with `column_cfg=SG_HAND_COLUMN_ORDER_V5` and the
    inventory's `wbc`, then `entry_locators`. Each predicted link is `(line_id, N, entry.line_ids[0])`.
  - L7's `folio_range` is the `FOLIO_MAP_V2` chain's (first, last) numbers.
- **Metrics.**
  - Locator P/R: multiset match on `(line_id, N)`.
  - Link P/R/F1: multiset match on `(line_id, N, first_line_id)`.
  - **Attribution A** = matched links / matched locators.
- **Dev grid** (`dev_grid.tsv`): all 16 combinations of {A0, A1, A2, A3} × {L2, L5, L6, L7}, with columns `method
  variant loc_tp loc_fp loc_fn loc_P loc_R link_tp link_fp link_fn link_F1 attribution`.
- **Selection rule (fixed):** the maximum dev link F1. On a tie, the lower method (A0 < A1 < A2 < A3), then the lower
  variant (L2 < L5 < L6 < L7). Write `select.json` (`{"method", "variant", "dev_link_F1", "run_utc"}`) **before** any
  test row is read.
- **Test, once.** Write `test_done.json` first, and refuse to run the test if it exists. Score the selected
  combination on the test sides, together with the v1 baseline (A0 + L2), which is reported only.
  - `test_sides.tsv`: per side, the counts above.
  - `test_errors.tsv`: every locator FP/FN and every attribution error of the selection, with `read_id k line text`,
    gold and predicted.
  - `test_targets.tsv`: per filtered test target (`target_scan_id` ≠ `none`, `folio_evidence` ≠ `unclear`), whether
    a predicted link has `N` = `target_locator`, sits in an entry whose first line = `first_line_id`, and has its
    `FOLIO_MAP_V2` front scan = `target_scan_id`. Its columns are `target_id found entry_ok map_ok e2e_hit`.
  - Print Wilson 95% intervals for P, R, A and E.

## Constraints

- Keep the v1 names, signatures and outputs: `volume_links` and L1–L4 are unchanged, and the 10 old tests pass as
  they are. `page_structure.py` and `presets.py` are imported, not edited.
- Patterns and thresholds are module constants or parameters with named defaults.
- **Leakage:** the selection reads dev rows only. Test is scored once, after `select.json`. No rule changes after the
  test run; a failed bar is a negative result. No gold is read by library code.
- **Changelog:** add your entry under `## Unreleased`. 21b creates that section. If it is missing, add it above the
  top version as 21b's brief says (`make version` first).
- **Protected outputs:** list them with `.venv/bin/python -I reports/data/phase-15-index-links-v2/planning/protect15.py
  reports/data/phase-15-index-links-v2/step-21c/protect_before.txt --skip reports/data/phase-15-index-links-v2/step-21c`
  before your first run, and again after your last (into `protect_after.txt`). Then `diff` the two lists.
- The agent shell: no `rm -rf`, `nohup` or `&`; zsh; no `sed -i`; `chmod g+w` new files; never `git commit`,
  `git stash`, `checkout` or `reset`.

## Tests

Add to `tests/test_analysis_index_links.py`:
- `test_l5_needs_punct_before_capital`: `"Verlof 14 Aug"` gives no locator under L5 but `14` under L2. `"Hof 252.
  Misse 300."` gives `252, 300` under both.
- `test_l6_left_boundary`: `"Denhof 354."` gives `354` under L6, and none under L3. `"Thesing7."` gives none under
  L6. `"X f 150. Y 77."` gives `150, 77` under L2 and `77` under L6 (an amount with a free-standing `f`).
- `test_l7_is_l6_with_range`.
- `test_read_side_lines_base_order_and_types`: uses the mini layer of `_write_layer`; extend that helper with
  `units/segments` and `lines` x/y/w/h, keeping the old tests green. Marginalia lines are included, page_number lines
  are not, and a line in two units appears once.
- `test_assemble_a1_line_rule`: the entries close at end numbers, a numbers-only line joins the previous entry, and
  a trailing open entry is kept.
- `test_assemble_a2_reorders_two_columns`: interleaved lines from two x clusters (left x≈100, right x≈1300, ≥ 3
  lines each, overlapping in y) come out left then right.
- `test_assemble_a3_heading`: `"Commissien."` at an entry start becomes a heading. `"Voor"` followed by a line
  ending in `37.` also becomes a heading under A3. Document this in the test name or a comment; it is a known cost of
  the rule.
- `test_wbc_join_and_spans`: `"ver„"` + `"nietigen 580."` gives `"vernietigen 580."`, and the locator maps to the
  second line.
- `test_entry_locators_line_of_locator`.

Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_index_links.py`, then the full suite.
Take the expected count from `pytest --collect-only -q`.

## Acceptance criteria

1. The new tests pass, the old ones are unchanged, and the full AS suite is green.
2. `dev_grid.tsv` has 16 rows. The selection follows the rule, and `select.json`'s mtime is earlier than
   `test_done.json`'s.
3. Test bars (once, selected combination):
   - **L:** locator micro P ≥ **0.95** and R ≥ **0.90**;
   - **A:** attribution ≥ **0.90**;
   - **E:** end to end ≥ **0.90** of the filtered test targets.

   Report each with numerator/denominator and its Wilson interval, with the v1 baseline beside it. A fail is a
   negative result.
4. Protected trees unchanged.

The user decides when the step is done.

## Error analysis

- **Dev grid.** Per method, the reorder rate (sides reordered out of judged sides) and the counts of `heading`
  entries.
- **Test errors of the selection.** Read every locator FP and FN, up to 40 each (all of them if there are fewer).
  Classify them as amount, count, ordinal, date, year, glued, garbled, a sub-entry bare-space case, mid-line, a
  number missed at a line end, or gold error.
- **Attribution errors.** Read up to 40. Classify them as:
  - column order: reorder missed;
  - column order: reorder wrong;
  - heading attached / heading taken;
  - first-line fragment;
  - mid-line entry end;
  - merged-columns line;
  - entry continues after a number;
  - gold error.
- **Spent volumes.** Read 10 seeded (`random.Random(2103)`) predicted entries on the spent phase-14 volumes 3590 and
  3611 under the selection, as a check outside the gold: is each one whole?
- **E misses.** Say whether the locator, the entry or the map failed.

## Do not touch

`folio_map.py` (21b's), `page_structure.py`, `presets.py`, `scripts/build_index_links.py` (21d), every layer and
`data/index_links_*`, 21a's and 21b's files (read only), `planning/` (except running `protect15.py`), `shared-data/`,
the siblings, the external drive.

## When done

1. In the report, write `### Step 21c: locators v2 and entry assembly` in `reports/phase-15-index-links-v2.md`, with
   draft findings `F15-21c.n`. Then run `make reports` and check it is clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 21c: locators v2 and entry assembly
- **Session:** <model>, <date>. **Status:** done / partial / blocked
- **Files changed:** <paths>
- **Public API:** <names added>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-15-index-links-v2.md` § Step 21c](../../reports/phase-15-index-links-v2.md#step-21c-locators-v2-and-entry-assembly); findings F15-21c.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <or "none">
```

3. Overwrite the phase `STATUS.md` with:
   - the selected `method` and `variant` (21d builds with them);
   - the bars;
   - any change to a constant name.
