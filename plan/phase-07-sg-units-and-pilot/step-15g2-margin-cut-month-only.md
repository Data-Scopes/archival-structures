# Step 15g2: Margin-prefix cut for month-only prefixes (UA-009 (b))

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15g, 15fq2.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`. If you have to explore the code
> base anyway, record it under *Brief defects*.

## Context

Step 15g's in-line rule 2 (`rule:margin_prefix`) cuts a line that starts in the margin at the
space nearest the main left edge. It cut 18,983 lines. Of the 9 cuts read and decided, 2 were
right, so the result was negative (F07-15g, UA-009). The month-only prefix ("Augt. | haer
Ho:Mog:", 423 lines, typed `date_header`) was fine. The user chose UA-009 (b) on 2026-10-21:
**keep only cuts whose prefix is month-only, and make no other margin cuts.** This step makes
that change and re-runs `types` on the 15fq2 layout as `15g-c`. 15h then runs on `15g-c`.

Checked at planning (Opus, on `step-15g/acceptance_15g-b.json`): none of the R4 test rows uses a
non-month `rule:margin_prefix` cut. Row 19 uses the month cut "Augt.", which stays. "Diesbagh"
(row 134b) and "Nieuwe jaren" (55b) are `rule:page_margin`, and row 9b is `rule:extra_role`.

## Inputs

- `archival_structures/text_layer/types.py` (413 lines):
  - `class TypeParams` (L41–74; `margin_tol` L53, `cut_max_dist` L54);
  - `segment_page` (L175). The rule-2 block starts at `if (not done_prefix and in_margin and not
    whole` (L212–231). Inside it, after `if sp and nxt:`, the code is:
    ```python
    prefix = text[sp[0]:sp[1]]
    is_month = bool(pats["month_only"] and pats["month_only"].fullmatch(prefix))
    pieces.append((sp[0], sp[1], "date_header" if is_month else "marginalia",
                   "rule:margin_prefix"))
    splits["rule:margin_prefix"] += 1
    start, end = nxt
    ```
    A line that is not cut keeps `start, end` and goes whole to `_whole_line(...)`, which picks
    its type (e.g. `rule:page_margin` → `marginalia`, or `main_text`).
- `tests/test_text_layer_types.py`: helpers `_params(**kw)`, `_line(i, text, x, y, w, role,
  col)`, `_body()`, `_segment(lines, params)`, `_of(segs, line_id)`. The tests
  `test_margin_prefix_month_only` (L66) and `test_margin_prefix_other_is_marginalia` (L76) pin
  15g's behaviour.
- Layer: `data/text_layer_v2_dev/15g-b/{hand_tar,hand_local,print}/`. This is `15fq-c` plus
  15g's `types/`; the run root is `<sub>/<preset name>/`.
- Types CLI: `.venv/bin/python -m archival_structures.text_layer.build --out-dir <dir>/<sub>
  --preset <sg_1.01.02_v2|sg_1.10.94_v2> --stage types --inventories <invs>
  [--types-params '<json>'] [--force]`. Hand `hand_tar` is 3349–3354. `hand_local` is 3771, 3772,
  3773, 3821, 3822, 3823, 11182 and 11281. Print is 455 and 456. If the stage refuses because
  `types/` exists, add `--force`.
- `reports/data/phase-07-sg-units-and-pilot/step-15g/acceptance.py [LAYER_DIR]` writes
  `step-15g/acceptance_<layer>.json`. `acceptance_15g-a.json` and `acceptance_15g-b.json` are
  protected; your run writes `acceptance_15g-c.json`.
- `reports/data/phase-07-sg-units-and-pilot/step-15g/ea_verdicts.tsv` (`kind, scan, verdict,
  note`). Its 10 `split:rule:margin_prefix` rows are 15g's reads.

## Deliverables

| File | Change |
|---|---|
| `text_layer/types.py` | `TypeParams.margin_cut: str = "month_only"` (`"month_only"` \| `"all"` (15g) \| `"off"`). Under `"month_only"`, a rule-2 cut whose prefix doesn't fullmatch `month_only` is not made, and the line stays whole. Under `"off"`, rule 2 is skipped. `params.json` records the field |
| `tests/test_text_layer_types.py` | See *Tests* |
| `docs/text_layer.md` | § *Type tables*: the rule-2 sentence and the new param |
| `docs/shared_library/changelog.md` | `### Step 15g2: margin cut month-only` under `## Unreleased`, below 15fq2's entry, with Edit. Default types output changes; no sibling imports affected |
| `reports/data/phase-07-sg-units-and-pilot/step-15g2/` | `acceptance.py`, `ea.py`, `ea_verdicts.tsv`, JSON |
| `reports/phase-07-sg-units-and-pilot.md` | A new `### Step 15g2: margin cut month-only` section after § Step 15fq2; findings `F07-15g2.n` |

Don't change `presets.py`; the core default is enough, since the SG presets already supply
`month_only`. If `month_only` is empty, as in the core default patterns, `"month_only"` makes no
cuts at all.

## Constraints

- Change nothing else in `types.py`: not `date_prefix`, `pn_suffix`, the whole-line rules or
  the thresholds.
- Layers are copies:
  - `cp -R data/text_layer_v2_dev/15g-b data/text_layer_v2_dev/15g-c`, then `--stage types`
    with the preset defaults on all three sub-layers;
  - `cp -R data/text_layer_v2_dev/15g-b data/text_layer_v2_dev/15g2-all`, then `--stage types`
    with `--types-params '{"margin_cut": "all"}'`. This is the regression run.
- **Protected:** `15d-a/`, `15fr-a/`, `15fp-a/`, every `15fq*` layer, `15g-a/`, `15g-b/`, all of
  `/Users/Shared/claude_projects/data_scopes/shared-data/`, and
  `step-15g/acceptance_15g-{a,b}.json`. Hash them before and after into
  `data/text_layer_v2_dev/15g2-protected-{before,after}.txt`. Use the format of
  `15fq2-protected-*.txt`: `sha256 mtime path` for the SG source archives, and `size=N mtime
  path` for every other file.
- Never `git commit`; `chmod g+w` new files and directories; `.venv/bin/python`; no `rm -rf`,
  `nohup` or `&`.

## Tests

- `test_margin_prefix_month_only` stays as it is and passes under the default.
- `test_margin_prefix_other_is_marginalia` → rename it to `test_margin_prefix_other_not_cut`.
  Under the default, the "Diesbagh en dan de rest…" line gives **one** segment (its type is
  whatever `_whole_line` gives; assert `len(got) == 1` and that `char_start, char_end` cover
  the stripped text). With `_params(margin_cut="all")` it gives 15g's two segments, the first
  `marginalia`.
- `test_margin_cut_off`: with `margin_cut="off"`, the "Augt." line is one segment.
- `test_build_stage_types` still passes, and `margin_cut` is in `types/params.json`.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_types.py`,
  then the full suite (1,050 at 15fq2; take the count from `--collect-only -q`).

## Acceptance criteria

Compare tables decompressed, with pandas (`dtype=str, keep_default_na=False`).
1. **Regression:** `15g2-all` `types/{segments,regions}` equal `15g-b`'s in 16/16 (same rows and
   order).
2. **The cut set:** in `15g-c`, every `rule:margin_prefix` segment is `date_header`, and its
   text fullmatches the month pattern. Per inventory, the number of `rule:margin_prefix` splits
   equals the number of `15g-b` cuts with a `date_header` prefix (about 423 in all, by 15g).
3. **Locality:**
   - Lines with no non-month `rule:margin_prefix` cut in `15g-b` have identical segments in
     `15g-c`.
   - Every line that had one now has exactly the segments that 15g's other rules give. There is
     no prefix piece, and a `rule:pn_suffix` piece stays where it was.
   - Report the count of changed lines per inventory. Expected: 18,983 − (month cuts).
4. **15g acceptance on `15g-c`:** coverage (0 missing, overlap or mismatch) and R4 rows 50, 52,
   55, 55b, 134, 134b, 19, 9a and 9b all `ok`. Report the per-type segment counts against
   `15g-b`, mainly the drop in `marginalia`.
5. Full AS suite green; protected hashes unchanged.

## Error analysis

Use `random.Random(22)`. Read texts and boxes from the tables.
- **30 formerly cut lines** (non-month prefix in `15g-b`), stratified 10 from 3349–3354, 10 from
  3771–3823 and 10 from 11182/11281 and print; if a stratum has fewer, take all of them. For each
  line, give its `15g-c` type and method, and whether that type is right for the whole line.
  Categories: *right main* (a main-text line that starts a little left), *right marginalia* (the
  whole line is a margin note), *wrong* (a line of real margin note plus main text, now one
  segment: the cost of (b)), and *ambiguous*. Give the share of *wrong*: that is what (b) gives
  up.
- Re-read 15g's two right cuts (3353_0318, 3349_0380: "Praside | d'Heer van Kersbergen", "Magt.
  van Herstal | ende Ingezetenen"). They are now lost; say what the line is typed as.

Expected findings: the cut count, the per-type shift, and the cost of (b).

## Do not touch

- `page_structure.py`, `layout.py`, `presets.py`, `units.py`; the siblings; `shared-data/`.

## When done

1. Write the report section, with criteria 1–5 with numbers, the error analysis and findings
   `F07-15g2.n` (status `draft`), with rows in the *Findings* table. Run `make reports` and fix
   any warnings.
2. Append the progress block (format in `plan/templates/STEP.md`) to `PROGRESS.md`.
3. Overwrite `STATUS.md` (40 lines max). The types layer is `15g-c`; next step: 15h on `15g-c`.
   Update brief 15h (rule 10): change its *Layer* line from `15g-b` to `15g-c`, and add `15g-c`
   and `15g2-all` to its protected list.
4. Move UA-009 from `docs/USER_ACTIONS.md` to `docs/USER_ACTIONS_resolved.md`, with the date
   and the outcome (criterion 2's count).
