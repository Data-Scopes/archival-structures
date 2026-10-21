# Step 15hj2: Pinned word-break sets for the SG presets (UA-010 (c))

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15hj.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`. If you have to explore the code
> base anyway, record it under *Brief defects*.

## Context

Step 15hj derived per-inventory word-break (WBC) sets with the v1 rule. The pooled sanity
anchors failed: `:` passes the rule in hand_local and `.` passes it in print. `join` therefore
refuses to run without `--ignore-sanity`. The derived sets were:
- hand_tar: `„` in 3349, 3350 and 3353; `=„` in 3352; empty in 3351 and 3354;
- hand_local: `-=„`, except `-.=„` in 3822 and `-:=„` in 11281;
- print: `-.=„` in 455 and `-=„` in 456.

The hand-read samples showed:
- `.` is right 11/20 times in 455 and 4/10 in 3822;
- `:` in 11281 is right 9/10, because it is that scribe's hyphen;
- the rule misses `„` in 3351 and 3354, where 12 of 12 line ends read were real `„` breaks.

The user chose **UA-010 (c)** on 2026-10-21: pin the SG sets to `-=„` everywhere, with `-:=„`
for 11281. This step adds pinned sets to the presets, re-runs `wbc` and `join` on a copy of the
15hj layer, and checks the result. `join` must then run **without** `--ignore-sanity`.

## Inputs

- `archival_structures/text_layer/presets.py`. In `class LayerPreset`, the step-15hj fields are
  ```python
  word_break_rule: Optional[Mapping] = None     # None = word_breaks.DEFAULT_WORD_BREAK_RULE
  word_break_stat: str = "default"              # "robust": end_rel_frac from the "„"-robust frequency
  wbc_sanity_in: tuple = ("¬",)                 # chars that must be word breaks on pooled counts
  wbc_sanity_out: tuple = (":", ".")            # chars that must not be
  ```
  `SG_HAND_V2` and `SG_PRINT_V2` (grep `= LayerPreset(`) both set `wbc_sanity_in=()`.
- `archival_structures/text_layer/build.py`:
  - `stage_wbc` (grep `def stage_wbc`) calls `decide_v2(run, stats, inventory_lines)` for v2. It
    then writes `wbc/corpus_default.json` and `wbc/wbc_per_inventory.tsv` (columns
    `inventory_number, n_lines, mode, wbc_set, differs_from_default, char_status`), updates the
    manifest with `default_set`, `pooled_rule_set` and `sanity_pass`, and logs a warning if
    `sanity_pass` is false.
  - `decide_v2` (grep `def decide_v2`) runs `decide_word_breaks` for `default` and `robust`, and
    writes `wbc_per_inventory_{name}.tsv` and `corpus_default_{name}.json`. It returns
    `results[preset.word_break_stat]` = `(per_inventory_rows, corpus_dict)`.
  - `stage_join` (grep `def stage_join`) exits if `not corpus["sanity_pass"] and not
    run.args.ignore_sanity`.
- `archival_structures/text_layer/word_breaks.py`, `decide_word_breaks` (L300–371). Its row dict
  and `corpus` dict keys are what `decide_v2` returns. `wbc_set` is `"".join(sorted(chars))`.
  `read_word_break_sets(layer_root)` (L374) reads `wbc_set` per inventory and
  `corpus["default_set"]`.
- Layer: `data/text_layer_v2_dev/15hj-a/{hand_tar,hand_local,print}/`. The run root is
  `<sub>/<preset name>/`, where hand is `NL-HaNA_1.01.02_text_layer_v2` and print is
  `NL-HaNA_1.10.94_text_layer_v2`. Inventories: `hand_tar` 3349–3354; `hand_local` 3771, 3772,
  3773, 3821, 3822, 3823, 11182 and 11281; `print` 455 and 456.
- CLI: `.venv/bin/python -m archival_structures.text_layer.build --out-dir <dir>/<sub> --preset
  <sg_1.01.02_v2|sg_1.10.94_v2> --stage wbc --inventories <all of that sub-layer>`, then the same
  with `--stage join`. Add `--force` if a stage skips work that is already done. 15hj's report
  section, *Step 15hj* in `reports/phase-07-sg-units-and-pilot.md`, has its acceptance and
  lower-case-share scripts under `reports/data/phase-07-sg-units-and-pilot/step-15hj/`. Reuse
  them by layer argument or by a copy in `step-15hj2/`.
- The v1 check command is in *Constraints*.

## Deliverables

| File | Change |
|---|---|
| `text_layer/presets.py` | `LayerPreset.wbc_pinned: Optional[Mapping[str, str]] = None`: `{"default": chars, "<inv>": chars, …}`; `None` = derive (today). `SG_HAND_V2`: `{"default": "-=„", "11281": "-:=„"}`. `SG_PRINT_V2`: `{"default": "-=„"}` |
| `text_layer/build.py` | `decide_v2` applies `wbc_pinned` (below) |
| tests | `tests/test_text_layer_join_v2.py` or `tests/test_text_layer_build.py`, wherever `decide_v2`/`stage_wbc` are tested now (grep). See *Tests* |
| `docs/text_layer.md` | The v2 wbc section: pinned sets, why (UA-010), and how `sanity_pass` is computed then |
| `docs/shared_library/changelog.md` | `### Step 15hj2: pinned WBC sets` under `## Unreleased`, below the last phase-07 entry, with Edit. Additive; no sibling imports |
| `reports/data/phase-07-sg-units-and-pilot/step-15hj2/` | scripts and JSON |
| `reports/phase-07-sg-units-and-pilot.md` | A new `### Step 15hj2: pinned WBC sets` section after § Step 15hj; findings `F07-15hj2.n` |

## Behaviour to implement

In `decide_v2`, after computing both variants exactly as today (the `_default`/`_robust` files
are still written, for the record), if `preset.wbc_pinned` is set, take the chosen variant's
`(rows, corpus)` and replace the decision:
- per row: `wbc_set = "".join(sorted(pinned.get(inv, pinned["default"])))`;
  `mode = "pinned"`; `differs_from_default = sorted(set) != sorted(pinned["default"])`;
  `char_status` = the derived statuses as before, each with a `/pinned_in` or `/pinned_out` suffix
  for the chars of the derived and pinned sets, so that `char_status` shows both. Keep `n_lines`.
- corpus: keep every key, and set `default_set` and `default_set_names` to the pinned default.
  Add `"pinned": {…}` (the mapping), `"derived_default_set"` (the old `default_set`),
  `"pooled_rule_sanity_pass"` (the old `sanity_pass`), and set `sanity_pass` to the anchors
  checked on the **pinned default set**: every `sanity_in` char in it and no `sanity_out` char
  in it. Per-inventory pins are explicit user decisions and are not checked against the
  anchors. Record that in `rule_text`, e.g. append `" Sets pinned by the preset (wbc_pinned)."`.
- `stage_wbc`'s manifest update also gets `"pinned": bool(preset.wbc_pinned)`.

With `wbc_pinned=None`, all output is byte-identical to today's, for v1 and for v2.

## Constraints

- Change nothing else in `word_breaks.py`, `join.py`, `join_v2.py` or the rule.
- **v1 untouched.** Re-run the phase-05 check from the AS root. It must print `TOTAL 157 / 157`.
  ```
  NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
  S=data/text_layer_v2_dev/15hj2-v1check; mkdir -p $S
  ls $NLP/output/voc_text_layer/voc_text_layer_v1/lines/ | sed 's/\.lines\.tsv\.gz$//' | paste -sd, - > $S/inv22.txt
  .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $S/r22 --stage all --inventories $(cat $S/inv22.txt) --write-events --workers 8
  .venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S
  ```
- Layers are copies: `cp -R data/text_layer_v2_dev/15hj-a data/text_layer_v2_dev/15hj-b`, then
  `wbc` and `join` (preset defaults, **no** `--ignore-sanity`) on all three sub-layers.
- **Protected:** every earlier dev layer under `data/text_layer_v2_dev/` (including `15hj-a/`
  and `15hj-v1check/`), all of `/Users/Shared/claude_projects/data_scopes/shared-data/`, NLP's
  `output/voc_text_layer/`, and LotA's `data/`, `gold/` and `output/`. Hash them before and after
  into `data/text_layer_v2_dev/15hj2-protected-{before,after}.txt`, in the format of
  `15hj-protected-*.txt`.
- Never `git commit`; `chmod g+w` new files and directories; `.venv/bin/python`; no `rm -rf`,
  `nohup` or `&`.

## Tests

- `test_decide_v2_pinned`: on a small synthetic stats frame where `.` passes the rule, a preset
  with `wbc_pinned={"default": "-=„", "X": "-:=„"}` gives `wbc_set` `-=„` for the other
  inventories and `-:=„` for X, `mode == "pinned"`, `sanity_pass` True, and
  `pooled_rule_sanity_pass` False.
- `test_decide_v2_unpinned_unchanged`: with `wbc_pinned=None`, the output equals today's (the
  existing tests keep passing).
- `test_build_v2_cli_all_runs_the_chain` still passes.
- Full suite (1,098 at 15hj; take the count from `--collect-only -q`).

## Acceptance criteria

1. In `15hj-b`, `wbc/wbc_per_inventory.tsv` has `-=„` for every inventory except 11281
   (`-:=„`). `corpus_default.json` has `sanity_pass: true` in all three sub-layers, and `join`
   ran without `--ignore-sanity`. The manifest shows `pinned: true` and `ignore_sanity: false`.
2. `scans/ regions_meta/ lines/ layout/ types/ units/` are byte-identical to `15hj-a`'s (they
   aren't rebuilt; check by hash). `wbc/parts/` and `wbc/wbc_stats.tsv` are identical too.
3. 15hj's join checks on `15hj-b`: offsets 0 violations, `join_unit == join_region` 0
   differences, legacy text 0 failures on `identical` units, 0 digit–digit merges. These must
   hold.
4. Per inventory, merges before (15hj-a) and after (15hj-b) per break char, plus the lower-case
   right-side share (15hj's measure). Expected (not measured): 3822 rises to ≈ 0.957, 455 to
   ≈ 0.891, and 3351 and 3354 gain `„` merges. Inventories that had no `-` or `=` in their derived
   set (hand_tar) gain `-`/`=` merges: report those counts and their lower-case share.
5. v1 `TOTAL 157 / 157`; full AS suite green; protected hashes unchanged.

## Error analysis

Use `random.Random(23)`. Read the texts from `join/`.
- **The new merges in hand_tar** (chars `-`, `=` and `„` added in 3349–3354): 20 merges, or all
  of them if fewer. Classify each as *right* (a real word break), *wrong* (two words glued), or
  *unclear*. This tests the pin for inventories where the rule had left `-`/`=` out.
- **The `.` merges removed in 455 and 3822:** 10 each. Is the text now right (two words kept
  apart) or wrong (a word now split at a real `.` break)?
- **11281 `:`:** 5 merges, as a sanity check.

Expected findings: the per-inventory merge deltas and the precision of the pinned sets where they
differ from the derived ones.

## Do not touch

- `types.py`, `units.py`, `layout.py`, `page_structure.py`; the siblings; `shared-data/`.

## When done

1. Write the report section, with criteria 1–5 with numbers, the error analysis and findings
   `F07-15hj2.n` (status `draft`), with rows in the *Findings* table. Run `make reports` and fix
   any warnings.
2. Append the progress block (format in `plan/templates/STEP.md`) to `PROGRESS.md`.
3. Overwrite `STATUS.md` (40 lines max). The join layer is `15hj-b`; next step: 15i Part A.
   Update brief 15i (rule 10):
   - its step 1 note: the WBC sets are pinned by the presets (UA-010 (c)), so `--stage all` runs
     through `join` without `--ignore-sanity`;
   - its step 2: compare against `15hj-b`. With pinned sets, the `wbc_set` column and all of
     `join/` must equal `15hj-b`'s byte for byte. Only `wbc_stats.tsv`, `char_status`, and the
     derived or pooled fields of `corpus_default.json` may differ (one layer of 14 instead of
     6 + 8);
   - its protected list: add `15hj-b` and `15hj2-v1check`.
4. Move UA-010 from `docs/USER_ACTIONS.md` to `docs/USER_ACTIONS_resolved.md`, with the date and
   the outcome.
