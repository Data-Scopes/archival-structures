# Step 15l: continuation marks and the upper-case guard

> **Phase:** 09, Text layer v3 preparation. **Depends on:** 15k (its layer `15k-a` is your baseline).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to
> explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Three join follow-ups, all in the v2 join (`join_v2.py`). The v1 join (`join.py`) stays as it is.
1. **Hand continuation marks** (F07-15ir.9). Step 15ir strips a `,` + letter at the start of a
   merged next line (the scribe's repeated break mark). 70 merges remain: 65 `, ` + letter
   ("fue-" + ", rant") and 5 `,,`.
2. **Print 455 `J`.** 2,175 of 455's 38,965 merges have a right side `J` + lower case ("hou|Jdende",
   "ver|Jsoeck"). That looks like the repeated hyphen read as `J`. 456 has 32, and the ones read
   are real words ("Junii", "Jurisdictie"), so the mark is pinned per inventory, like `wbc_pinned`
   (F09-P.4).
3. **`g_upper_right` guard** (F07-15hj.6). A merge whose right side starts upper-case is often
   wrong: a hyphenated compound ("West|Vrieslandt"), a line-order error ("geexa|Missive") or a
   name. Sometimes it is right ("Hol|Steijn"). It is built off. The user decided (P3) to switch
   it on per sub-layer only if ≥ 27 of 40 read upper-case merges are wrong.

You read the samples **first** (bars fixed at planning), set the presets, then build `15l-a` once.

## Inputs

- Baseline layer `data/text_layer_v2_dev/15k-a/{hand,print}`.
- Planning scripts (`reports/data/phase-09-text-layer-v3-prep/planning/`, `.venv/bin/python -I`, AS
  root): `merges.py` (right-side class per merge, all boundary kinds, seeded sample; read its
  docstring), `compare.py`, `protect.py`. Planning counts on `15ir-a`: `planning/merges_15ir-a.json`.
  Hand `upper` 4,219, print `upper` 4,447 (455 2,043, 456 2,404); `J+lower` 455 2,175, 456 32,
  hand 34 (names: "Jacob", "Johan"; untouched); hand `comma_sp` 65, `comma2` 5; print `comma_sp` 3
  (untouched, the print preset has no lead marks).
- Code anchors (checked 2026-10-21):
  - `text_layer/join_v2.py`: `join_unit` L28–91 (merge branch L50–67, quoted below; spans L69–91),
    `join_inventory_v2` L140–188 (calls `join_unit(texts, wbcs, lead_marks=lead_marks)` L154;
    offset check L164–169; stats L178–186 with `"guards": list(ALL_GUARDS)`, `"lead_marks"`).
  - `text_layer/join.py` (do not edit): `classify_boundary(prev, nxt, wbcs, guards=ALL_GUARDS)`
    L45–76 returns `("merge", char)` or a guard name; `ALL_GUARDS` L41–42 (v1's guard set).
  - `text_layer/build.py`: `stage_join` L643–687. The currency check is at L672–676
    (`previous.get("guards") == list(ALL_GUARDS)` and, for v2, `previous.get("lead_marks", "") ==
    run.preset.join_lead_marks`). The v2 task tuple `(inv, str(run.root), wbcs,
    run.preset.join_lead_marks)` is at L678.
  - `text_layer/presets.py`: `LayerPreset` L8–37 (frozen dataclass; `wbc_pinned:
    Optional[Mapping[str, str]] = None` L34, `join_lead_marks: str = ""` L37), `SG_HAND_V2` L97–117
    (`join_lead_marks=","`), `SG_PRINT_V2` L123–141.
  - Tests: `tests/test_text_layer_join_v2.py` (46 tests; lead-mark tests L79–90; `join_unit ==
    join_region` checks L58–60, L96).
  - `docs/text_layer.md` *Unit text and offsets* L358–417.

```python
# join_v2.py L50-67
    for i in range(n - 1):
        prev, nxt = lines[i], lines[i + 1]
        kind, char = classify_boundary(prev, nxt, wbcs, guards)
        boundaries[i] = (kind, char or "")
        if kind == "plain":
            current.append(i + 1)
            continue
        if kind != "merge":
            runs.append(current)
            current = [i + 1]
            continue
        if both_ends_fix and nxt[0] in wbcs:
            lines[i + 1] = nxt[1:]
        if lead_marks and lines[i + 1][:1] in lead_marks and lines[i + 1][1:2].isalpha():
            lines[i + 1] = lines[i + 1][1:]
        lead[i + 1] = len(nxt) - len(lines[i + 1])
        current.append(i + 1)
```

## Part 1: read before you code (the bars)

Run `merges.py --layer data/text_layer_v2_dev/15k-a --json reports/data/phase-09-text-layer-v3-prep/step-15l/merges_15k-a.json --sample-out reports/data/phase-09-text-layer-v3-prep/step-15l/merges_verdicts.tsv --sample 40 --seed 51`.
Record the counts against planning (drift from 15j's reordering is expected in print `upper`). Fill
`verdict` in every sample row. Unclear always counts against adoption.
- `J+lower`, 455 (30 rows): `mark` (the repeated break mark: removing `J` gives the word) / `word` /
  `unclear`. **Bar:** `join_lead_marks_lower={"455": "J"}` iff `mark` ≥ 27. 456 (all rows): same
  verdicts, no bar (planning: real words). If 456 has ≥ 27 `mark` of 32, stop and ask.
- `comma_sp` / `comma2`, hand (70 rows): `mark` / `real_comma` / `unclear`. **Bar:** the extended rule
  stays iff `mark` ≥ 63; else stop and ask.
- `upper`, hand and print (40 each): `wrong` (the merged form is worse than kept apart: a compound
  with a capital, a line-order error, a name or sentence start) / `right` (one word continued with a
  capital) / `unclear`. **Bar per sub-layer:** `join_upper_guard=True` in that preset iff `wrong` ≥ 27.
Write the outcomes into the report before building. Don't re-read to change an outcome.

## Deliverables and behaviour

- `presets.py`: `LayerPreset.join_lead_marks_lower: Optional[Mapping[str, str]] = None`
  (per inventory, keys an inventory or `"default"`, like `wbc_pinned`) and
  `join_upper_guard: bool = False`; the SG values from Part 1, each with a comment giving its bar
  outcome. VOC_V1 is unchanged.
- `join_v2.join_unit(texts, wbcs, guards=ALL_GUARDS, both_ends_fix=True, lead_marks="",
  lead_marks_lower="", upper_guard=False)`. In the merge branch, after the `both_ends_fix` strip,
  with `t = lines[i + 1]`:
  1. if `lead_marks`: strip `re.match(f"[{re.escape(lead_marks)}]{{1,2}} ?(?=[^\\W\\d_])", t)`
     (one or two marks, an optional single space, then a letter). This keeps the old `,` + letter
     result and adds `,,` and `, `;
  2. if `lead_marks_lower` and `t[:1] in lead_marks_lower` and `t[1:2].islower()`: `t = t[1:]`;
  3. if `upper_guard` and `t[:1].isupper()`: undo, so no strip and no merge.
     `boundaries[i] = ("g_upper_right", char or "")`, `lines[i + 1] = nxt`, close the run as the
     `kind != "merge"` branch does, `continue`;
  4. else `lines[i + 1] = t`; `lead[i + 1] = len(nxt) - len(t)`; `current.append(i + 1)`.
  With every new argument at its default, the output must equal the old `join_unit`.
- `join_inventory_v2(inv, layer_root, wbcs, lead_marks="", lead_marks_lower="", upper_guard=False)`
  passes them through, counts `g_upper_right` like any boundary kind, and adds
  `"lead_marks_lower"` and `"upper_guard"` to `join/stats/<inv>.json`. `is_merge` stays
  `kind == "merge"`.
- `build.stage_join`: for v2, `lml = (pins.get(inv, pins.get("default", "")) if (pins :=
  run.preset.join_lead_marks_lower) else "")`. The task tuple becomes `(inv, root, wbcs,
  join_lead_marks, lml, run.preset.join_upper_guard)`, and the currency check also compares
  `lead_marks_lower` and `upper_guard`. `"guards"` stays `list(ALL_GUARDS)`.
- Docs (*Unit text and offsets*): the extended lead-mark rule, `join_lead_marks_lower`,
  `join_upper_guard`, the boundary kind `g_upper_right`, and the bar outcomes.

## Constraints

- Build `15l-a` (check `coreshare.py status` first):
  ```
  O=data/text_layer_v2_dev/15l-a; mkdir -p $O/hand $O/print
  python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15l-hand --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir $O/hand --preset sg_1.01.02_v2 --stage all --workers {workers}
  python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15l-print --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir $O/print --preset sg_1.10.94_v2 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
  ```
- **Don't edit `join.py`** or `ALL_GUARDS`. They are v1's guard set and its currency check. v1:
  the 15j brief's v1 command with `S=data/text_layer_v2_dev/15l-v1check` → `TOTAL 157 / 157`.
- Protected: `planning/protect.py data/text_layer_v2_dev/15l-protected-{before,after}.txt --skip 15l-`; empty `diff`.
- Don't touch `layout.py`, `page_structure.py`, `types.py`, `units.py`, `word_breaks.py`.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`; no `rm -rf`, `nohup`, `&`.

## Tests (`tests/test_text_layer_join_v2.py`)

- `test_lead_mark_comma_space` (`["fue-", ", rant"]`, `lead_marks=","` → "fuerant", `spans[1][2]`
  (`lead_dropped`) 2); `test_lead_mark_double_comma` (`["behou-", ",, den"]` → "behouden", 3); the
  existing `test_lead_mark_comma` unchanged.
- `test_lead_mark_lower_j` (`["hou-", "Jdende"]`, `lead_marks_lower="J"` → "houdende", `lead_dropped`
  1; `["den-", "JAN"]` → `J` before a capital is not stripped).
- `test_upper_guard` (`["West-", "Vrieslandt"]` with `upper_guard=True` → kind `g_upper_right`,
  text keeps both; off → merged) and `test_upper_guard_after_lead_marks` (`["hou-", "Jdende"]` with
  both on → merged, since the `J` strip comes first).
- `test_defaults_equal_old_join_unit`: on the existing parametrised cases, all new args at default
  give the same `(text, spans, boundaries)` as `join_region` (as the current checks at L58–60 do).
- build: `test_join_lower_marks_per_inventory` (preset pin `{"455": "J"}`, inventories 455 and 456 →
  stats `lead_marks_lower` "J" and "").
- Full suite `.venv/bin/python -m pytest -q -p no:cacheprovider` (count from `--collect-only -q`).

## Acceptance criteria

Compare `15l-a` with `15k-a` (`compare.py`; `merges.py --json` on both).
1. Builds exit 0, 0 failures, `sanity_pass` true; `n_offset_violations` 0 in every `join/stats`.
2. Hand: `comma_sp` + `comma2` right sides 70 → 0 (planning count; use the 15k-a count). Print
   `comma_sp` unchanged.
3. If the J bar passed: 455 `J+lower` → 0, and 455's lower-case right-side share goes from about 0.892
   to about 0.948. Otherwise unchanged. 456 is unchanged either way.
4. For each sub-layer with the guard on: `g_upper_right` = its `upper` count on 15k-a (± the few `,`
   + capital cases the comma rule now exposes), and merges fall by that number. For a sub-layer with the guard off,
   nothing changes.
5. `scans/ lines/ layout/ types/ units/ wbc/` identical to 15k-a; `join/` differs only in the units
   holding the boundaries of criteria 2–4, and `join/stats` also in its new keys.
6. AS suite green; v1 157/157; NLP suite green (`venv/bin/python -m pytest -q -p no:cacheprovider tests`
   in `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`); hashes unchanged.

## Error analysis

- Part 1: counts per verdict and class, and 3 verbatim examples per category (from `merges_verdicts.tsv`).
- After the build, read 10 changed units per adopted change (`Random(91)`) and check that the new
  text is the intended one.
- Findings `F09-15l.n`: each bar outcome (adopted or a negative result), the share changes, and what
  the `right` upper-case merges have in common (a guard that would keep them).

## Do not touch

`shared-data/`, earlier dev layers, `join.py`, the modules listed above, other steps' report sections.

## When done

1. `### Step 15l: continuation marks and the upper-case guard` in
   `reports/phase-09-text-layer-v3-prep.md`; `make reports` clean.
2. Changelog `### Step 15l: …` (Edit): new preset fields, `join_unit` / `join_inventory_v2` kwargs,
   the extended `,` rule (hand output changes), `g_upper_right` (v2 only), stats keys.
3. `PROGRESS.md` block; overwrite `STATUS.md` (next: 15m, baseline `15l-a`).
