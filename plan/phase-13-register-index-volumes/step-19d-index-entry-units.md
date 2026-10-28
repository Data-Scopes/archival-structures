# Step 19d: index entry unit rule

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** 19a (entry gold), 19c (pages table,
> clones). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

A hand unit is one layout region minus furniture, cut only at date/running headers. On index pages one PageXML region
holds many entries, so the hand units under-segment: in phase 12 the hand-pinned index volumes lost 26–50% of their
entry starts against their print builds (F12-15z.3, F12-R.21), whose `short_prev` cuts fell on entry starts. Index
entries start with a hanging outdent (the first line about one line height left of its continuation), often with a
date (`17. April. Ry. voor den Consul …`), and in register indexes the line before ends in a page reference
(`… 42.`). This step adds entry cues to the hand unit rule, **only on page sides flagged `index_side` by 19c**,
chooses one of five pre-registered variants on 10 dev gold sides, and tests it once on 20 test gold sides. The test
applies the rule to the gold sides directly, so it does not depend on the detector's bars.

## Inputs

- `archival_structures/text_layer/units.py` (checked 2026-10-28):
  - `UnitParams` L45–67 (frozen: `indent_frac`, `short_frac`, `gap_factor`, `drop_cap_lines`; `to_dict` drops
    `drop_cap_lines` when `None`; `from_dict` rejects unknown keys). `HEADER_TYPES = ("date_header",
    "running_header")` L31; `PAGE_ORDER` L32.
  - `paragraph_features(mains, header_between)` L76–97: per main `i ≥ 1`, `med_h` = median `h` of the mains,
    `col_left`/`col_right` = median `x` / median `x + w`, `col_w = col_right - col_left`; `indent = (x_i - col_left) /
    med_h`, `short = (col_right - (x_{i-1} + w_{i-1})) / col_w`, `gap`, `header`. `cues_fired(feat, params)` L100.
  - `split_region(segs, params, print_mode)` L115–175: non-main runs → `<prefix>:run` units; mains; hand (L153–156)
    cuts only where `header_between[i]`; methods `hand:region` (one piece) / `hand:region_cut_at_header`.
  - `build_units_page(scan_id, page, regions, params, print_mode, taken, claimable=None)` L190–242 calls
    `split_region(segs, params, print_mode)` per region and sorts by `_key = (PAGE_ORDER, layout_column, bbox y,
    region_order, first _pos)`.
  - `load_segments(root, inv)` L403 (segments from `types/segments`, int fields, `layout_line_idx`; segment columns
    `scan_id, line_id, seg_idx, char_start, char_end, x, y, w, h, geom_method, page, layout_region_id, line_type,
    type_method, column_override` — **no text**); `unit_inventory(inv, layer_root, params, *, print_mode=False,
    republic_path=None)` L420: loads `lines` rows (`text` per `(scan_id, line_id)`), builds per page the list of
    `(region_dict, segs)` (L455–461) and calls `build_units_page`.
- 19c: `layout/pages/<inv>.tsv.gz` (`scan_id, page, …, rule, index_side`); `presets.SG_INDEX_DATE_START`; clones
  `data/text_layer_v2_dev/19c-a`, `19c-z` and the `--layout-params` JSON (see `STATUS.md`).
- 19a gold (`reports/data/phase-13-register-index-volumes/step-19a/`): `entry_sides.tsv` (`read_id, split, group,
  role, side_verdict, n_S, n_C, n_H, n_unclear, used_for`; use the sides with `side_verdict == "index"`, `split` dev /
  test) and `entry_gold.tsv` (`read_id, scan_id, page, line_id, k, label ∈ S/C/H/?, note`); the side's inventory and
  layer are in `planning/reads_entries.tsv` (`read_id, …, inv, layer, scan_id, page`).
- Baselines (read only): v4 hand units `data/text_layer_v2_dev/{15y-b,15z-b}/1.01.02/NL-HaNA_1.01.02_text_layer_v4/units/
  {units,segments}/<inv>.tsv.gz`; v4 print builds of the pinned index volumes: `15z-a` (3463: `15x-a`), same paths.
- Tests: `tests/test_text_layer_units.py` (17 tests; helper `seg(i, ltype, x=300, y=None, w=500, h=30, chars=20,
  idx=0)` L13 and `region(rid="R", role="main", col=0)` L19).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/units.py` | `UnitParams.index_entry`; `INDEX_ENTRY_DEFAULTS`; `entry_cues`; `split_region(..., index_side=False)`; `build_units_page(..., index_side=False)`; segment `text` in `unit_inventory`; index flags from `layout/pages` |
| `archival_structures/text_layer/presets.py` | `SG_INDEX_ENTRY_V5` (constant only) |
| `tests/test_text_layer_units.py` | tests below |
| `reports/data/phase-13-register-index-volumes/step-19d/` | `entry_eval.py`, `eval_dev.json`, `eval_test.json`, `units_stats.py`, `error_reads.txt`, `error_verdicts.tsv`, `protect_*.txt` |
| `docs/shared_library/changelog.md` | `### Step 19d: index entry units` under *Unreleased* (Edit) |
| `reports/phase-13-register-index-volumes.md` | `### Step 19d: …` section, findings `F13-19d.n` |

## Behaviour to implement

```python
INDEX_ENTRY_DEFAULTS = {"cues": ["outdent", "date_start", "end_ref_prev"], "outdent_frac": 0.5,
                        "indent_frac": 0.5, "short_frac": 0.1, "end_pattern": r"\d+\s*[.,:;]?\s*$",
                        "date_pattern": None}
INDEX_CUES = ("outdent", "indent", "short_prev", "date_start", "end_ref_prev")
```
`UnitParams.index_entry: Optional[dict] = dataclasses.field(default=None, hash=False)`: keys checked against
`INDEX_ENTRY_DEFAULTS`, every cue in `INDEX_CUES`, **omitted from `to_dict` when `None`**, tested with `is not None`.

`entry_cues(mains, header_between, cfg) -> list[list[str]]`: for each main `i ≥ 1` (with `med_h`, `col_left`,
`col_right`, `col_w` as in `paragraph_features`; `t_i` = the segment's `text`, stripped):
- `outdent`: `(x_{i-1} - x_i) / med_h >= outdent_frac`;
- `indent`: `(x_i - col_left) / med_h >= indent_frac`;
- `short_prev`: `(col_right - (x_{i-1} + w_{i-1})) / col_w > short_frac`;
- `date_start`: `re.match(date_pattern, t_i, re.IGNORECASE)` (never when `date_pattern` is `None`);
- `end_ref_prev`: `re.search(end_pattern, t_{i-1})`;
- `header`: `header_between[i]` (always on).
Only the cues listed in `cfg["cues"]` (plus `header`) fire; order in the output = `INDEX_CUES` order, `header` last.

`split_region(segs, params, print_mode, index_side=False)`: unchanged unless `not print_mode and index_side and
params.index_entry is not None`. Then the mains are cut where `entry_cues` fired; methods: one piece → `hand:region`;
else the first piece `hand:index_start`, every other `"hand:index:" + "+".join(cues)` (e.g.
`hand:index:outdent+date_start`). Non-main runs as before. `build_units_page(..., index_side=False)` passes it on.

`unit_inventory`: give every segment a `text` key (`lines` row text `[char_start:char_end]`); this changes no output.
When `params.index_entry is not None` and not `print_mode`: read `layout/pages/<inv>.tsv.gz` (missing → raise
`FileNotFoundError` naming the file and "run layout with index_sides") and call `build_units_page(...,
index_side=(sid, page) in flagged)` with `flagged` = the `(scan_id, page)` rows with `index_side == "1"`.

Comment the code with the rationale (phase 13 P11/P12; F12-R.21, F12-15z.3).

**Variants** (fixed at planning; `date_pattern` = `SG_INDEX_DATE_START` in all):
| Variant | `cues` | params |
|---|---|---|
| E1 | `outdent` | `outdent_frac` 0.5 |
| E2 | `outdent`, `date_start` | as E1 |
| E3 | `outdent`, `date_start`, `end_ref_prev` | as E1 |
| E4 | E3 + `short_prev` | `short_frac` 0.1 |
| E5 | `indent`, `short_prev` (the print rule, `SG_PRINT_UNIT_PARAMS`) | `indent_frac` 0.5, `short_frac` 0.1 |

## Evaluation (`entry_eval.py`)

For each gold side (`side_verdict == "index"`): take the side's `(region_dict, segs)` list from the 19c clone exactly
as `unit_inventory` builds it (layout regions of that page from `layout/regions/<inv>.tsv.gz`, segments from
`load_segments` plus `text`), and call `build_units_page(sid, page, regions, UnitParams(index_entry=cfg), False,
taken=set(), claimable=None, index_side=True)`. **Predicted starts:** units in `_key` order, types `main_text` and
`date_header` only; the start line is the unit's first segment's `line_id`; a `main_text` unit directly preceded
(among those units) by a `date_header` unit in the same `layout_column` is not a start (the pair is one entry, started
by the date line). **Scoring** against the gold labels: TP = start on an `S` line; FP = start on a `C` line; FN = an `S`
line that is no start (also when its line is in no `main_text`/`date_header` unit; count that case separately); starts
on `H`/`?` lines are ignored. Micro P, R, F1 over the sides of a split.
Baselines, scored the same way on the same sides: **v4 hand** (the units of `15y-b`/`15z-b` for that page), **split
only** (the 19c clone's own units, no entry rule), and **v4 print** for the sides of the pinned index volumes (3433,
3436, 3439, 3442, 3445, 3466, 3469, 3472, 3475, 3489 in `15z-a`; 3463 in `15x-a`).
Selection: E1–E5 on the **dev** sides; the highest F1 (ties: lower E). Then the chosen variant **once** on the
**test** sides, with the baselines.

## Procedure (from the AS root)

1. Protect `_before` (`protect.py … --skip 19b 19c 19d`; the `find … shasum` list of `15y-b`, `15z-b`, `15z-a`,
   `15x-a`, `19c-a`, `19c-z`, `data/text_layer_v4_full`). Code, tests, suite green.
2. `entry_eval.py dev` → `eval_dev.json` (all five variants and the baselines); choose. `entry_eval.py test --variant
   E<k>` → `eval_test.json`. Set `SG_INDEX_ENTRY_V5` to the chosen variant (comment: dev and test numbers).
3. Dev run: `cp -cR` `19c-a` → `19d-a`, `19c-z` → `19d-z`, `chmod -R g+w`; `--stage units --unit-params
   '{"index_entry": <chosen cfg as JSON>}'` on each clone with its hand list (`--preset sg_1.01.02_v4 --out-dir
   data/text_layer_v2_dev/<clone>/1.01.02 --inventories-file <list>`; coreshare for > 1 worker). In a mode run the
   unit params are merged into the print params too; the print volumes are not rebuilt, and print ignores
   `index_entry`.
4. `units_stats.py`: on flagged index sides and elsewhere, main_text units in `19c-*` vs `19d-*`; the
   `unit_method` counts; units elsewhere must be identical (byte-compare the rows of unflagged pages).
5. Error reads; `make reports`; protect `_after`; `diff`.

## Tests

`tests/test_text_layer_units.py`: `entry_cues` per cue (outdent fires at 0.5·h left of the line above, not at 0.4;
`date_start` on `"17. April. Miss: …"` with the SG pattern; `end_ref_prev` after `"… 42."`; `short_prev`; `indent`);
`split_region` on an index side: a region of three entries (outdented first lines) gives three main units with methods
`hand:index_start`, `hand:index:outdent`, …; the same segments with `index_side=False` or `index_entry=None` give
today's output; print mode ignores `index_entry`; `UnitParams` round trip, key omitted when `None`, unknown cue raises;
`unit_inventory` without `layout/pages` and with `index_entry` set raises `FileNotFoundError`. Full suite green.

## Acceptance criteria

1. Tests green; full suite green (counts from `--collect-only`).
2. Dev numbers of E1–E5 and the baselines reported; the choice follows the rule.
3. **Entry bar** (test sides, once; pre-registered 2026-10-28): micro P ≥ 0.90 **and** R ≥ 0.85 **and** F1 > the v4
   hand F1 on the same sides. Fail → negative result with the numbers, no tuning, no second test; Decide UA (next free
   ID): v5 without the entry rule (recommended) or with it.
4. Step 4: units outside flagged sides unchanged; protected lists unchanged apart from the clones and other projects'
   `shared-data/` changes (name them).

The user decides when the step is done.

## Error analysis

Read every test FP and FN (all, if ≤ 40; else 40 with `random.Random(20261121)`), each with the 3 lines around it, and
write `error_verdicts.tsv` with a category: hanging indent below 0.5·h; entry with no outdent, date or reference; cut at
a continuation (number or year at a line end, date inside the text); column split error (19b); gutter-merged line; the
start line typed `marginalia`/`other` (no main unit); gold ambiguity. Compare per group (G1–G4): which index styles the
rule fits, and where E5 (print) differs from the chosen variant. Draft findings: the bar result, the chosen variant, the
main error causes, the change in main_text units on flagged sides.

## Do not touch

`layout.py`, `page_structure.py`, `index_pages.py`, `types.py`, `build.py`, the published layers,
`data/text_layer_v2_dev/{15x-a,15y-b,15z-a,15z-b,19b-*,19c-*}` and older dev layers, the planning and 19a files,
`shared-data/`, the siblings. Never `git commit`; `chmod g+w`; `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`;
shared files only with Edit. Never touch the external drive.

## When done

1. Report: `### Step 19d: index entry unit rule` (what changed, the dev table, the choice, the test bar with baselines,
   the unit counts, the error analysis; findings `F13-19d.n` as `draft`); `make reports` clean.
2. Changelog (Edit): `### Step 19d: index entry units`: `UnitParams.index_entry` (omitted when `None`),
   `INDEX_ENTRY_DEFAULTS`, `INDEX_CUES`, `units.entry_cues`, the `index_side` arguments of `split_region` and
   `build_units_page`, the new `unit_method` values, the `layout/pages` requirement, `presets.SG_INDEX_ENTRY_V5`.
   "Affected sibling imports: none."
3. `PROGRESS.md` block (template of `plan/templates/STEP.md`); overwrite `STATUS.md`: 19d done, the bar result, facts
   for 19e (the chosen cfg; clone paths `19d-a`, `19d-z`).
