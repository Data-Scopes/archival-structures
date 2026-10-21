# Step 15ir: phase 07 review fixes

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15i2 and 15i Part B (done);
> the user has committed the phase-07 tree first (`REVIEW.md`, *Release*).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** The review
> findings it fixes are in `plan/phase-07-sg-units-and-pilot/REVIEW.md` (read only the *Findings*
> table if you need the evidence).

## Context

The phase-07 review found five code defects behind the published pilot `text_layer_v2`, and a set
of test and documentation gaps. This step fixes them in code, rebuilds a **dev** layer `15ir-a`
from the same sources as `15i-b`, and measures each fix. It does **not** touch the published
copies in `shared-data/`: UA-013 was answered (a), so `text_layer_v2` stays as published, gets the
*Known issues* notes below, and the fixes go into the full run `text_layer_v3`. Every fix is gated so that
v1 (`VOC_V1`) and `LEGACY_15FP` output stay byte-identical.

## Inputs

- Sources: `data/text_layer_v2_dev/15i-src/` (symlinks; never write through them). Baseline:
  `data/text_layer_v2_dev/15i-b/{hand,print}/` (= the published layer).
- Build commands (the `argv` recorded in the `15i-b` manifests; print reads the shared-data
  zips, read only):
  ```
  O=data/text_layer_v2_dev/15ir-a; mkdir -p $O/hand $O/print
  .venv/bin/as-build-text-layer --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir $O/hand --preset sg_1.01.02_v2 --stage all --workers 8
  .venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir $O/print --preset sg_1.10.94_v2 --stage all --workers 8
  ```
- Step scripts to reuse (copy into `reports/data/phase-07-sg-units-and-pilot/step-15ir/`, then
  edit the copies): `step-15i/audit.py`, `step-15i/rows116.py`, `step-15i2/repro.py`,
  `step-15i2/protect.py` (change its skip list to `15ir-*` only).
- Code anchors (checked 2026-10-21):
  - `archival_structures/analysis/page_structure.py`: `split_regions_on_line_field` (L303),
    `_line_sort_key` (L117), `merge_overlapping_text_regions` (L271, the `while True` loop),
    `get_overlapping_region_sets` (L156; fills `blocked` / `column_blocked`), `LayoutParams` (L52),
    `LEGACY_15FP` (L92).
  - `archival_structures/text_layer/layout.py` L128–150: `n_blocked += len(blocked)`,
    `n_col_blocked += len(col_blocked)`, `blocked_out`.
  - `archival_structures/text_layer/types.py`: in-line rule 1 (`pats["date_prefix"].match`, ~L202),
    the `whole = any(...)` line (~L212), the pass-2 `if key in offchain:` block (~L371),
    `_pn_candidate` (L154), `TypeParams` (L45).
  - `archival_structures/text_layer/units.py`: `build_units_page` (L181; the `for u in pending:`
    loop L213–222), `unit_inventory` (L383; `reserved` L392–397; crosswalk L449–458).
  - `archival_structures/text_layer/join_v2.py`: `join_unit` (L28; `lead[i + 1] = 1` L57),
    `join_inventory_v2` (L124).
  - `archival_structures/text_layer/build.py`: `stage_join`, the `sets[inv] = default_set` line
    (~L650); `stage_units` (L427); `decide_v2` / `_apply_pinned` (L590).
  - `archival_structures/text_layer/presets.py`: `LayerPreset` fields L16–34, `SG_HAND_V2` L94.
- Tests to extend: `tests/test_analysis_page_structure.py`, `tests/test_text_layer_layout.py`,
  `tests/test_text_layer_types.py`, `tests/test_text_layer_units.py`,
  `tests/test_text_layer_join_v2.py`, `tests/test_text_layer_word_breaks.py`.

## Deliverables and behaviour

**Fix 1: print reading order in column-split regions (F07-R.1).** `split_regions_on_line_field`
keeps the source (XML) line order, and Laypa's page-level print regions interleave columns, so a
unit can read y 2952 → 3102 → 1371 → … → 1322 (`455_0010`). Add
`LayoutParams.sort_split_lines: bool = True`; `LEGACY_15FP` sets it `False`. When true,
`split_regions_on_line_field` gains a keyword `sort_lines=False` (pass the param) and sorts each
group's lines with `_line_sort_key` **only when the region is actually split** (a region with one
value is still returned as the same object, unchanged). Do not reorder `unchanged` regions (they
carry the legacy id and text; R5). Measured on `15i-b`: 455 has 546 layout regions whose non-blank
lines jump back > 50 px in `layout_line_idx` order (416 `split_at_column`, 127 `unchanged`,
2 `merged+split_at_column`, 1 `merged`); units with such a jump: 455 335 of 28,079, 456 160 of
25,030; 3 of the 116 rows (153, 185, 275) sit in such units.

**Fix 2: blocked-pair counts (F07-R.5).** `blocked` / `column_blocked` collect pairs in every merge
pass, so a relation is counted again under each new merged id (455: `n_guard_blocked` 1,322,
`n_column_blocked` 62; 456: 673, 113). In `merge_overlapping_text_regions` give each pass fresh
dicts and, when the loop ends, put only the **last pass's** pairs into the caller's `blocked` /
`column_blocked`. These are pairs of final regions that overlap but stay apart; no merged-away id
can appear. Layout tables other than the counts must not change.

**Fix 3: types (F07-R.4).** (a) Compute `whole` before rule 1 and skip rule 1 (`date_prefix`) when
`whole` is true, so "28. Junij 1650." stays one `date_header` segment (19 lines on `15i-b`: 11281 10,
3772 5, 3771 2, 3353 1, 455 1). (b) Replace the pass-2 relabel: keep a dict `cand_by_key` from
`cands`; every segment with `type_method == "rule:pn_line"` becomes `rule:pn_line_offchain` unless
`key in chain_set` and the page's candidate is `pn_line` and the segment's `line_id` is the
candidate line's id. `line_type` does not change. Expected on `15i-b` counts (`rule:pn_line`
10,327, `rule:pn_line_offchain` 1,792): about 1,414 band lines that were never chain candidates,
plus about 87 date-prefix remainders, move to `_offchain`. (c) Validate in `TypeParams.__post_init__`:
`margin_cut` in `{"month_only", "all", "off"}`, `top_n >= 1`, `bottom_n >= 1`; raise `ValueError`.
Same in `LayoutParams.__post_init__` for `gutter_rule` in `{"gap", "main_edge", "auto"}`.

**Fix 4: unit ids of legacy twins (F07-R.3).** All legacy ids of a scan are reserved, so a unit whose
box equals a legacy region's box gets `w+1` even when it holds every line of that region (455: 45 of
58 collisions, 456: 32 of 32; their map rows are `changed`). In `build_units_page` add a keyword
`claimable=None`: a dict `{legacy_id: frozenset(line_id)}` of legacy ids that are **not** a layout
region id of the scan. In the `pending` loop, before the collision loop: if the unit's own-box id is
in `claimable`, no unit holds it yet, and `claimable[id]` ⊆ the unit's line ids, the unit takes it.
Build `claimable` in `unit_inventory` from `lines` (`region_id` per line) and the layout ids. Expected:
`n_id_collisions` 455 58 → 13, 456 32 → 0 (reviewer's probe); hand stays 0 and hand `units/` stays
byte-identical apart from fix-3 effects.

**Fix 5: continuation marks in the join (F07-R.2).** In 11182 the scribe repeats the break mark at the
start of the next line, which the HTR reads as `,`: 1,305 of 4,699 merges give "gero,ijeert"
(3352 336, 11281 170, 3772 47, 3822 20, 3353 16, 3350 13, 3823 10; print ≤ 3). Add
`LayerPreset.join_lead_marks: str = ""` and set `","` in `SG_HAND_V2` only. `join_unit` gets
`lead_marks=""`: on a `merge` boundary, after the existing `both_ends_fix` strip, if the next text
starts with a char in `lead_marks` **followed by a letter**, strip it too. Compute `lead[i + 1]` as
`len(original) - len(stripped)`, never a constant: this also fixes the latent `„„` case (the library
strips a second leading WBC; `["vande„", "„„selve", "meer"]` must give `line_offset` 2 and the same
text as `join_region`). `join_inventory_v2(inv, layer_root, wbcs, lead_marks="")`; `stage_join`
passes `run.preset.join_lead_marks` for v2. Record `lead_marks` in `join/stats/<inv>.json`. With the
default `""` `join_unit` must still equal `join_region` on every unit.

**Fix 6: small items.** (a) `stage_join` with `--allow-partial`: a v2 inventory without a decision
gets its own pin (`preset.wbc_pinned.get(inv, preset.wbc_pinned["default"])` as a set) when the
preset pins, else `default_set`. (b) `stage_units`: in print mode, when
`paragraphs-<inv>.tsv.gz` is missing, `run.log` a warning; write the REPUBLIC dir used (or `null`)
into `units/params.json` as `republic_dir`. (c) `n_offset_violations` and the join test compare the
**full** span: `text[cs:ce] == seg_text[lead:len(seg_text) - n_trailing_removed]`, where
`n_trailing_removed` is the number of trailing WBC chars the library dropped (1, or 2 for `--`,
`=-`, `-„`; 51 such rows on `15i-b`).

**Docs** (`docs/text_layer.md`): `types` and `units` **are** part of v2 `--stage all` (fix the two
"not part of `all`" sentences in *Type tables* and the units section); add `n_column_blocked` to the
layout scans schema row; say that a merged segment may lose two trailing break chars (fix 6c); define
`rule:pn_line_offchain` as "a pn_line-shaped band line not confirmed by the page-number chain";
document `sort_split_lines`, `join_lead_marks`, `republic_dir`; in *Published SG pilot* add a
*Known issues* list (F07-R.1–R.4, with the counts above). **Changelog:** `### Step 15ir: phase 07
review fixes` under *Unreleased*, added with Edit: the new fields and kwargs, the changed counts and
tags, and the missing names from earlier entries (`units.parse_republic_id`, `read_republic_ids`,
`load_segments`, `REPUBLIC_PARAGRAPHS_DIR`, `FURNITURE_TYPES`, `HEADER_TYPES`, `NEAR_PX`,
`SPLIT_HALF_IOU`; `build.decide_v2`; `--pagexml-dir` optional for v2 wbc/join). Correct the 15fq2
entry's "(defaults unchanged)": the default `LayoutParams()` applies the column guard.
**LotA** (read LotA `CLAUDE.md` first): append to §6 of
`/Users/Shared/claude_projects/data_scopes/language-of-the-archive/docs/from-archival-structures.md`
a paragraph *Known issues in `text_layer_v2`* with the same four items and the 3 affected rows.

## Constraints

- v1 byte-identical: re-run the phase-05 check (from the AS root) and get `TOTAL 157 / 157`:
  ```
  NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
  S=data/text_layer_v2_dev/15ir-v1check; mkdir -p $S
  ls $NLP/output/voc_text_layer/voc_text_layer_v1/lines/ | sed 's/\.lines\.tsv\.gz$//' | paste -sd, - > $S/inv22.txt
  .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $S/r22 --stage all --inventories $(cat $S/inv22.txt) --write-events --workers 8
  .venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S
  ```
- Protected (hash before and after with the copied `protect.py`, format `size=N mtime path`, plus
  sha256 of the 6 tar and 2 zip sources): every dev layer except `15ir-*`, all of `shared-data/`
  (the published `text_layer_v2/` dirs included), LotA `data/ gold/ output/`, NLP
  `output/voc_text_layer`. Only LotA `docs/from-archival-structures.md` may change outside AS.
- `LEGACY_15FP` output must stay as it is (the existing legacy tests must pass unchanged).
- Don't touch `join.py`, `extract_v2.py`, `line_split.py`, `word_breaks.py`. Never `git commit`;
  never bump the version; `chmod g+w`; `.venv/bin/python`. No `rm -rf`, `nohup`, `&`.

## Tests (new or changed)

- page_structure: `test_split_sorts_lines` (two values, lines given out of y order → each piece in
  `_line_sort_key` order; one-value region returned as the same object; `sort_lines=False` keeps
  source order); `test_blocked_counted_once` (A1('1'), A2('1') overlap, B('2') overlaps both →
  exactly 1 `column_blocked` pair, ids of final regions); `test_furniture_not_merged` rewritten with
  `interleave_guard=False` so only `exclude` keeps "( 93" apart, and a twin with `exclude=()` that
  merges; `test_gutter_rule_validated`.
- layout: `test_merge_off` (`merge=False` → no `merged` change, `n_merges` 0); a print fixture with
  `column` values checking `split_at_column`, `column_value`, `furniture=1`.
- types: `test_chain_relabel` (three pages with candidates 10, 11, 99 and a non-candidate band
  number → only the in-chain candidate lines keep `rule:pn_line`); `test_date_line_with_year_not_split`;
  `test_types_params_validated`; one test for the colon form "20: Decemb:" (row 55).
- units: `test_legacy_twin_keeps_id`; `test_missing_republic_file_logged` (print mode).
- join_v2: `test_lead_mark_comma` ("gero„", ",ijeert" → "geroijeert", `line_offset` 1);
  `test_double_lead_wbc` (the `„„` case); the full-span check of fix 6c.
- word_breaks/build: `test_sg_hand_pins` runs `decide_v2` on `SG_HAND_V2` with stats for 3349 and
  3352 and checks the applied sets; `test_allow_partial_uses_pin`.
- Command: `.venv/bin/python -m pytest -q -p no:cacheprovider` (count from `--collect-only -q`).

## Acceptance criteria

Compare `15ir-a` with `15i-b` on decompressed table text (`repro.py` copy).
1. Build exit 0 for hand and print, 0 failures, no `--ignore-sanity`, `sanity_pass` true.
2. **Hand:** `scans/ regions_meta/ lines/ layout/` identical in all 14 (layout stats: only the
   counters may differ, and hand counters are 0). `types/` differs only in `type_method`
   (pn relabel) and the 11 hand date lines of fix 3a. `join/` differs only in the units of fix 5 and
   fix-3a lines. **Print:** `layout/regions` and `layout/lines` differ only in `split_at_column`
   regions; `n_guard_blocked` / `n_column_blocked` each ≤ the old count.
3. Fix 1: 0 `split_at_column` (and `merged+split_at_column`) regions with a > 50 px backward jump;
   report the remaining `unchanged` ones (expected 127 in 455) and the units count per inventory.
4. Fix 3: 0 `rule:date_prefix` segments on lines that fullmatch `date_line`; new `rule:pn_line` /
   `_offchain` counts per inventory.
5. Fix 4: `n_id_collisions` per inventory; the formerly bumped twins now `same_id`.
6. Fix 5: 0 merges in hand whose right side starts with `,` + letter; lower-case right-side share per
   inventory (11182 expected near 0.98, from 1,305 of 1,416 non-lower-case right sides being `,`).
7. Offsets: 0 violations with the full-span check over all segments; with `lead_marks=""`,
   `join_unit` equals `join_region` on every unit.
8. Audit (`audit.py --layer 15ir-a`) and `rows116.py --layer 15ir-a`: list every changed invariant
   and every changed row (expected: rows 153, 185, 275 may change).
9. v1 `TOTAL 157 / 157`; full AS suite green; NLP `venv/bin/python -m pytest -q -p no:cacheprovider
   tests` (in NLP) green; protected hashes unchanged.

## Error analysis

- Fix 1: read 20 changed print units (seeded `random.Random(31)`): is the new line order the reading
  order? Categories: right / still wrong / ambiguous (title pages, tables).
- Fix 5: read 30 removed commas (`Random(32)`, ≥ 10 from 11182): right (repeated break mark) / wrong
  (a real comma) / unclear.
- Fix 3b: read 20 relabelled segments: real page number off the chain / not a page number.
- Fix 4: read 10 twins: is the unit the legacy region (same lines, same text)?
Findings `F07-15ir.n` (draft) in a `### Step 15ir: phase 07 review fixes` section of
`reports/phase-07-sg-units-and-pilot.md`, placed after *Step 15i2* and before *Review*.

## Do not touch

`shared-data/` (UA-013 (a): no publication), the earlier dev layers, the report's *Summary*,
*Review* and *Follow-ups* sections.

## When done

1. Report section and findings as above; `make reports` clean.
2. Append the template's `PROGRESS.md` block; overwrite `STATUS.md` (next: the review of 15ir, the
   user's release v0.12.0; then phase 08 planning).
