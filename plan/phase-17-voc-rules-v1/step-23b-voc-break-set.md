# Step 23b: VOC break set with `¬` (preset `voc_1.04.02_v6`, rebuild of the 567)

> **Phase:** 17, VOC rules v1. **Depends on:** nothing. Runs in parallel with 23a (and the dev stages of 23c/23d).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

The VOC HTR ends 116,871 lines (0.66%) with `¬`, a word-break mark. It often repeats the mark at the start of the next
line, where it reads as `„`. The phase-16 layer pins the SG break set `-=„`, so it never joins a `¬` (F16-22a.5),
although the pooled word-break rule wants `¬` in 418 of 426 eligible inventories.

This step adds a new layer version (D17: published or protected layers are never rebuilt) with `¬` in the pinned
set, rebuilds the 567 inventories into a new root, and measures join precision by hand.

Nothing else in phase 17 reads `wbc/` or `join/`, so no other step waits on this one.

## Inputs

Paths are relative to the AS root (checked 2026-11-01).
- `archival_structures/text_layer/presets.py`, l.249–255 (anchor `VOC_1_04_02_V5SG = dataclasses.replace(`):
```python
VOC_SOURCE_PATTERN = r"(?P<inv>[0-9]+[A-Z]*)\.zip"
VOC_1_04_02_V5SG = dataclasses.replace(
    SG_1_01_02_V5, name="NL-HaNA_1.04.02_text_layer_v5", archive_id="NL-HaNA_1.04.02",
    scan_id_prefix="NL-HaNA_1.04.02", source_pattern=VOC_SOURCE_PATTERN, throughput_fit=None,
    wbc_pinned={"default": SG_1_01_02_V5.wbc_pinned["default"]},
    volume_mode={k: v for k, v in SG_1_01_02_V5.volume_mode.items() if k != "pins"},
    description="NL-HaNA 1.04.02 (VOC) text layer: the SG v5 rules unchanged (phase 16 step 22a)")
```
  - Registry: `PRESETS` (l.262–265, anchor `PRESETS = {`); the key `"voc_1.04.02_v5sg"` is last.
  - `wbc_pinned` replaces every inventory's set with `pinned.get(inv, pinned["default"])` (`build.py`
    `_apply_pinned`, l.751).
- **Join** (`archival_structures/text_layer/join_v2.py` `join_unit`, l.29): on a merge boundary, `both_ends_fix` drops
  one leading break character of the next line, and pagexml removes the line-final break char(s).
  - Guards (`join.classify_boundary`): `g_standalone` (the mark after whitespace, or a line of marks only),
    `g_digit`, `g_digit_left`, `g_punct_left`, `g_digit_right`. A guarded boundary is joined with a space.
  - VOC hand mode has `join_lead_marks=","`.
- **Planning probe** (`reports/data/phase-17-voc-rules-v1/planning/wbc_probe.py`: 870 `¬`-ended lines in 1120, 1274,
  1557, 2448, 1917, 3826, joined with `join_unit([a, b], "-=„¬", lead_marks=",")`):
  - 856 `merge`, 13 `g_digit_right`, 1 `g_standalone`;
  - 176 of the 870 next lines start with `„`, which `both_ends_fix` drops (`praefe¬` + `„rentie` → `praeferentie`).
- Phase-16 layer (protected, read only): `data/text_layer_voc_22a/1.04.02/NL-HaNA_1.04.02_text_layer_v5/`. Inputs:
  `data/text_layer_voc_inputs/1.04.02/` (567 symlinks). Its `join/units/<inv>.tsv.gz` columns are `scan_id unit_id
  unit_type word_break_chars n_breaks_merged ends_with_break_char text`. Its `join/segments/<inv>.tsv.gz` columns are
  `scan_id unit_id seg_order line_id seg_idx char_start char_end line_offset break_char_end merged_with_next
  boundary_kind`.
- Audit: `reports/data/phase-13-register-index-volumes/step-19e/audit.py`. It runs its v5 checks only when the root
  name matches `r"_v5$"` (l.403), so v6 needs a step copy (below).
  - 22a's audit: `reports/data/phase-16-voc-generalisation/step-22a/audit.json` (`n_fail_judged = 0`; 148
    reported-only fails of "merges with lower-case right side").
- Protect: `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT` and
  `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 23`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/presets.py` | `VOC_1_04_02_V6 = dataclasses.replace(VOC_1_04_02_V5SG, name="NL-HaNA_1.04.02_text_layer_v6", wbc_pinned={"default": "-=„¬"}, description="NL-HaNA 1.04.02 (VOC) text layer v6: v5sg plus ¬ as a word break (phase 17 step 23b)")`; register `"voc_1.04.02_v6"` |
| `tests/test_text_layer_voc_preset.py` | `test_v6_fields` (`VOC_1_04_02_V6` equals v5sg apart from `name`, `wbc_pinned` and `description`, compared via `dataclasses.asdict`); `test_v6_registered` |
| `tests/test_text_layer_join_v2.py` | `test_join_unit_not_sign`: `join_unit(["proce¬", "„deeren voort"], "-=„¬", lead_marks=",")` returns `("procedeeren voort", [(0, 5, 0), (5, 17, 1)], [("merge", "¬"), ("", "")])`; with `"-=„"` it returns `("proce¬ „deeren voort", …, [("plain", ""), …])`; `test_join_unit_not_sign_digit_guard`: `join_unit(["anno¬", "1738 den"], "-=„¬", lead_marks=",")` returns text `"anno¬ 1738 den"` and boundary `("g_digit_right", "¬")` (all three run at planning) |
| `reports/data/phase-17-voc-rules-v1/step-23b/` | `audit.py` (copy of the 19e audit with `r"_v[56]$"` at l.403, nothing else changed), `audit.json`, `compare.py`, `compare.json`, `joins_sample.tsv`, `joins_read.tsv`, `guarded_read.tsv`, protect lists |
| `docs/text_layer.md` | a short paragraph *VOC v6* after the VOC paragraph of phase 16 (anchor `voc_1.04.02_v5sg`) |
| `docs/shared_library/changelog.md` | `## Unreleased` at the top if absent (above `## v0.21.0`), then `### Step 23b: VOC preset v6 (¬ word break)`; "no sibling imports affected" |
| `reports/phase-17-voc-rules-v1.md` | `### Step 23b: …`, findings `F17-23b.n` |

## Behaviour to implement

1. Add the preset and the tests. Run the new tests, then the full suite: 1,424 tests were collected at planning;
   expect +3 or +4, all green.
2. Run `protect_voc.py step-23b/protect_voc_before.txt` and `protect.py step-23b/protect_before.txt --skip 23`.
3. Build:
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures \
  --label 23b --want 6 --min 2 --wait -- .venv/bin/as-build-text-layer --preset voc_1.04.02_v6 \
  --pagexml-dir data/text_layer_voc_inputs/1.04.02 \
  --out-dir /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/23b-a/1.04.02 \
  --stage all --workers '{workers}'
```
   - Then link `data/text_layer_voc_23b` → `…/text_layer_voc/23b-a`. Root:
     `data/text_layer_voc_23b/1.04.02/NL-HaNA_1.04.02_text_layer_v6/`.
   - 22a took 1 h 51 min on 2 workers. Run it in the foreground with the tool's longest timeout, or as a background
     task, logging to the scratchpad. Coreshare's grant is fixed at the start.
4. Audit: `.venv/bin/python -I step-23b/audit.py --layer data/text_layer_voc_23b --out step-23b/audit.json`.
5. `compare.py` (single process; writes `compare.json`):
   - **(a) Upstream identical.** For every inventory, compare the decompressed text of `scans/`, `lines/`, `mode/`,
     `layout/lines/`, `layout/pages/`, `types/segments/`, `units/units/` and `units/segments/` in 22a and v6, using
     sha256 of the decompressed bytes. Expect 0 differences. A difference stops the step.
   - **(b) Join changes explained.** For each inventory, read 22a's and v6's `join/units`. Count changed units and
     units changed **without** a `¬` in their 22a text. Expect 0 of the latter; any is a stop.
   - **(c) Remaining `¬`.** Count `¬` in v6 join texts and classify each by the v6 `join/segments` boundary of its
     segment: `guarded` (boundary kind ≠ merge, ≠ plain), `unit_end` (the last segment), `not_line_final` (the `¬`
     is inside the line), `other`. Expect `other` = 0. Report each class and the totals against 22a's 116,881.
6. **Hand-read sample** (`joins_sample.tsv`, seed 20261101):
   - Draw 80 merge boundaries uniformly from all v6 segments with `merged_with_next = 1` whose source line (in
     `lines/`, by `line_id`) ends in `¬` after stripping trailing spaces.
   - For each, give the two raw lines and the v6 joined text at the boundary (`text[char_end-30 : char_end+30]` of the
     unit; `char_end` from `join/segments`).
   - Read them in order until 60 are judged; the rest are reserves.
   - Write `joins_read.tsv` (`k inv line_id left right joined verdict note`).
7. Read 20 `guarded` `¬` boundaries (all, if fewer) into `guarded_read.tsv`, with the category: digit, standalone
   mark, other. Say whether each should have joined.

**Verdict rubric (fixed before reading).**
- `right`: the two halves form one word, in any historical spelling, with nothing lost but the break marks (a lead
  `„`, `,` or `¬` dropped is right). HTR errors inside the halves do not count.
- `wrong`: two separate words were glued, or a letter or number was lost or kept wrongly (a stray `„` inside the
  word).
- `unclear`: you cannot tell which word is meant. It falls outside the 60 and is replaced by a reserve.

## Constraints

- Change only `presets.py` (one constant, one registry entry) in the library. No change to `join_v2.py`, `join.py`,
  `build.py` or `word_breaks.py`, and no new preset field.
- The 22a layer and its inputs are read only. The drive is written only under `…/text_layer_voc/23b-a/`. A hook
  denial or `Permission denied` is final.
- Never `git commit`; never bump the version; `chmod g+w` everything (local and on the drive).
- Agent shell: no `rm -rf`, `nohup`, `&` or `$TMPDIR`; zsh (quote `'{workers}'`); BSD `sed -i ''`; never
  `git stash`/`checkout`/`reset`; a new output dir per run (`23b-b` if you must restart).
- Parallel sessions: edit only your own `STATUS.md` row. Edit the changelog, report and `PROGRESS.md` with Edit only.
- Do not open `reports/data/phase-17-voc-rules-v1/step-23a/` (fresh gold).

## Tests

- `tests/test_text_layer_voc_preset.py::test_v6_fields`, `::test_v6_registered`;
  `tests/test_text_layer_join_v2.py::test_join_unit_not_sign`, `::test_join_unit_not_sign_digit_guard`.
- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_voc_preset.py tests/test_text_layer_join_v2.py`,
  then the full suite. Take counts from `--collect-only -q`.

## Acceptance criteria

1. New tests pass; full suite green.
2. 567 inventories built, `failures=0`; audit `n_fail_judged = 0`. Compare every audit key with 22a's
   `audit.json`: list the keys that differ (expected: join-related only, e.g. the lower-case merge share), with
   both values.
3. `compare.json`: (a) 0 upstream differences; (b) 0 changed units without a `¬`; (c) `other` = 0. Report the
   remaining `¬` by class, and the number of changed units.
4. **Bar:** at least 57 of 60 judged `¬` joins are `right` (Wilson interval reported). A miss is a negative result:
   report it and keep the layer; no retuning.
5. The protect pairs are clean, apart from the new paths and named shared-data changes.

## Error analysis

- Every `wrong` and `unclear` join (verbatim, with the two lines).
- The 20 guarded boundaries by category, and whether a digit guard on `¬` is right for VOC (`¬` before `1`
  splitting a number?).
- 10 changed units picked at random (seed 20261101): the 22a text against the v6 text, around the change.
- Expected findings: the join precision; the remaining-`¬` classes; any audit key that moved.

## Do not touch

`folio_map.py`, `index_links.py`, `index_pages.py`; the 22a layer; the 23a, 23c–23e step dirs.

## When done

1. Add `### Step 23b: VOC break set with ¬` to `reports/phase-17-voc-rules-v1.md` (results, error analysis, findings
   `F17-23b.n`, status `draft`), then run `make reports`.
2. Append the `PROGRESS.md` block (template in `plan/templates/STEP.md`).
3. Overwrite only the 23b row of `STATUS.md`. Under *Facts later steps need*, add the v6 root.
