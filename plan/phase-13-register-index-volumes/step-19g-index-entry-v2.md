# Step 19g: index entry rule v2, tested on fresh gold

> **Phase:** 13, Register and index volumes (text layer v5). **Depends on:** 19a, 19c, 19d (code, gold, clones);
> 19f only if the phase `STATUS.md` names its clones as the base. **Conditional:** run only if `docs/USER_ACTIONS.md`
> UA-042 (or UA-041) is answered with this step. Written by the phase review of 2026-10-28.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

On page sides flagged as index pages (`layout/pages`, `index_side == 1`, step 19c) the hand units should be index
entries. Step 19d chose E5 (the print rule: `indent` + `short_prev`) on 10 dev sides and failed its bar on 20 test
sides (P 0.8683, R 0.7092). The review found the cause in the cues, not mainly in column interleaving.
`indent` fires on the indented continuation lines of hanging entries, often a page-reference list that ran over
(`358, 540, 546.`). E5 has no cue for entries after a line ending in a page number or for entries starting with a date.
Lines typed `index` by v4's types stage (`rule:index_page`) are in no main unit, so no rule reaches them. Those 30 gold
sides are now spent (the review ran variants on them), so this step chooses on them and tests **once on 20 fresh sides
that you read first**, against the same bar.

## Inputs (checked 2026-10-28)

- `archival_structures/text_layer/units.py`: `INDEX_CUES` L38 (`outdent`, `indent`, `short_prev`, `date_start`,
  `end_ref_prev`); `INDEX_ENTRY_DEFAULTS` L39 = `{"cues": ["outdent", "date_start", "end_ref_prev"], "outdent_frac":
  0.5, "indent_frac": 0.5, "short_frac": 0.1, "end_pattern": r"\d+\s*[.,:;]?\s*$", "date_pattern": None}`;
  `UnitParams.index_entry` (keys checked against the defaults, cues against `INDEX_CUES`, dropped from `to_dict`
  when `None`); `entry_cues(mains, header_between, cfg)` L123: per main `i >= 1` a list of fired cue names in
  `INDEX_CUES` order plus `header` (always on); `t_cur`/`t_prev` = stripped segment `text`; `outdent` =
  `(x[i-1] - x[i]) / med_h >= outdent_frac`. `split_region(segs, params, print_mode, index_side=False)` L175: copies
  the segs (`segs = [dict(s) for s in segs]`), groups runs of non-main types (`<prefix>:run` units), then cuts the
  `main_text` segments; entry mode (L222: `not print_mode and index_side and params.index_entry is not None`) cuts
  where `entry_cues` fired; methods `hand:index_start`, `hand:index:<cue>+<cue>`. `build_units_page(...,
  index_side=False)` L258; `load_segments(root, inv)` L472; `unit_inventory` L489 reads `layout/pages`.
- `archival_structures/text_layer/presets.py`: `SG_INDEX_DATE_START` L96, `SG_INDEX_ENTRY_V5` L104 (E5).
- Types: `types._is_index_page` types every line of a page `index` (`type_method` `rule:index_page`) when ≥ 50% of
  its lines are short and ≥ 30% end in a number: 963 sides in the 125 hand volumes, 408 on flagged sides.
- **Base layout** (`BASE`): the phase `STATUS.md` names it under *Facts later steps need* ("19g base"):
  `19f-a`/`19f-z` (column order v2) or `19c-a`/`19c-z` (v4 hand layout). If STATUS names none, stop and ask.
  Path `data/text_layer_v2_dev/<clone>/1.01.02/NL-HaNA_1.01.02_text_layer_v4/`. v4 hand units: `15y-b`, `15z-b`;
  v4 print builds of pinned index volumes: `15z-a` (3463: `15x-a`).
- Gold: **selection set** = all 30 sides of `reports/data/phase-13-register-index-volumes/step-19a/entry_sides.tsv`
  with `side_verdict == "index"` (both splits) and their labels in `step-19a/entry_gold.tsv`; side metadata in
  `planning/reads_entries.tsv`. **Fresh test sides**: `review/fresh_entry_sides.tsv` (`read_id` 101–128, `group`,
  `role` read/reserve, `inv`, `layer`, `scan_id`, `page`, `n_lines`; drawn by the review from detector-flagged sides
  with seed 20261124, volumes without a 19a read side where possible).
- Scripts to copy and adapt (read them): `step-19a/show_side.py` (text listing of one side), `step-19d/entry_eval.py`
  (scoring), `step-19d/units_stats.py`, `step-19d/errors_sample.py`.
- Tests: `tests/test_text_layer_units.py` (24 tests; helpers `seg(...)`, `region(...)`). Full suite 1,355 collected
  on 2026-10-28 (plus 19f's tests if 19f ran).

## Part A: fresh gold, before any code (read it yourself; no rule, no script labels)

Copy `step-19a/show_side.py` to `step-19g/show_side.py`, reading `review/fresh_entry_sides.tsv` instead of the
planning lists. Read the 20 `read` sides in `read_id` order. Give each a `side_verdict`: `index` (entries pointing to
resolutions by date or page/folio number, under headings), `not index` (running text, lists without a reference per
entry, title pages), `unreadable input` (more than half the lines garbled). A side that is not `index` is replaced by
the first unused `reserve` of its group (`used_for` = the replaced id). For an `index` side label **every listed line**:
- `S`: the first line of an entry. A date-only line followed by the entry text: the date line is `S`, the text `C`. A
  sub-entry with its own date or its own page/folio reference is a new entry (`Advis R: V: State daarop. 95.` → `S`);
  an undated sub-entry that is a separate document is `S`.
- `C`: a continuation of the entry above (also a line of only page numbers that continues a reference list).
- `H`: a line in no entry (lemma or section heading, page number, running header, catchword, year line).
- `?`: you cannot decide (left out of P and R).
- A line holding text of both columns (HTR gutter merge): label it by its first part and write `merged` in `note`; if
  the first part is a heading tail followed by an entry start, label it `S`.
Write `step-19g/fresh_gold.tsv` (`read_id, scan_id, page, line_id, k, label, note`) and `step-19g/fresh_sides.tsv`
(`read_id, group, role, side_verdict, used_for, n_S, n_C, n_H, n_unclear`), appending per side. When all are read,
record `shasum -a 256` of both files in your `PROGRESS.md` block; never edit them afterwards. Fewer than 18 `index`
sides after the reserves → stop and report.

## Part B: code

`INDEX_ENTRY_DEFAULTS` gains `"no_cut_pattern": None` and `"main_types": ["main_text"]` (old configs behave as before).
- `entry_cues`: when `no_cut_pattern` is not `None` and `re.fullmatch(no_cut_pattern, t_cur)`, no cue fires for main
  `i` except `header` (a page-reference list that ran over never starts an entry).
- `split_region`: in entry mode, right after the segs are copied and before the runs are grouped, every segment whose
  `line_type` is in `cfg["main_types"]` (with `cfg = {**INDEX_ENTRY_DEFAULTS, **params.index_entry}`) gets
  `line_type = "main_text"`, so `index`-typed lines on flagged sides join the mains and their units are `main_text`.
  Outside entry mode nothing changes.
- `presets.py`: `SG_INDEX_NO_CUT = r"[\d\s.,:;]+"`; later `SG_INDEX_ENTRY_V5B` = the chosen variant (comment: the
  selection and test numbers). Comment the code with the rationale (phase 13 review, F13-R.2, F13-R.5).

Tests (`tests/test_text_layer_units.py`): `no_cut_pattern` suppresses `end_ref_prev` on `"358, 540, 546."` but a
`header` still cuts; `main_types=["main_text", "index"]` turns an `index` run on an index side into main units cut at
entry cues; with `index_side=False` or the default config the output equals today's; round trip of the new keys.

## Part C: selection and test (`step-19g/entry_eval_v2.py`, adapted from 19d's `entry_eval.py`)

Same prediction and scoring as 19d. Per side: the side's `(region_dict, segs)` from the **BASE** clone, then
`build_units_page(sid, page, regions, UnitParams(index_entry=cfg), False, taken=set(), claimable=None,
index_side=True)`. Starts = first line of each `main_text`/`date_header` unit in `_key` order; a `main_text` unit
right after a `date_header` unit in the same `layout_column` is not a start. TP = start on `S`, FP = start on `C`, FN =
`S` without a start (count separately when its line is in no `main_text`/`date_header` unit, with its `line_type`);
`H`/`?` ignored. Micro P, R, F1. Baselines on the same sides: v4 hand units (`15y-b`/`15z-b`), the BASE clone's own
units, E5 (`SG_INDEX_ENTRY_V5`), and v4 print for sides of 3433, 3436, 3439, 3442, 3445, 3466, 3469, 3472, 3475, 3489
(`15z-a`) and 3463 (`15x-a`).

Variants (fixed 2026-10-28; all with `outdent_frac` 0.5, `short_frac` 0.1, `date_pattern` = `SG_INDEX_DATE_START`,
`no_cut_pattern` = `SG_INDEX_NO_CUT`):

| Variant | `cues` | `main_types` |
|---|---|---|
| X1 | `outdent`, `date_start`, `end_ref_prev` | `main_text` |
| X2 | as X1 | `main_text`, `index` |
| X3 | X1 + `short_prev` | `main_text` |
| X4 | X3 | `main_text`, `index` |

1. `entry_eval_v2.py select`: X1–X4 and the baselines on the 30 selection sides; choose the highest F1 (ties: lower X).
   Reference only (the review's diagnostics, same sides, guard applied after the fact): E3 + guard P 0.896 R 0.864
   on the v4 layout, P 0.940 R 0.882 with the columns re-ordered.
2. `entry_eval_v2.py fresh --variant X<k>`: **once**, on the fresh `index` sides; it refuses a second run (write
   `test_done.json` as 19c's `select.py` does). Also check that every fresh side is flagged in BASE's `layout/pages`.

## Part D: dev build

`cp -cR` BASE-a → `19g-a`, BASE-z → `19g-z`, `chmod -R g+w`; `--stage units --unit-params '{"index_entry": <chosen
cfg as JSON>}'` with `--preset sg_1.01.02_v4 --out-dir data/text_layer_v2_dev/<clone>/1.01.02 --inventories-file
reports/data/phase-13-register-index-volumes/planning/<hand list>` through coreshare (`python3
/Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then `… run --project archival-structures
--label 19g-units --want 4 --min 1 --wait -- <command> --workers {workers}`). Unit counts on flagged sides against
BASE (adapt `units_stats.py`); units on unflagged pages must be identical.

## Constraints

- **Protected outputs:** `protect.py … step-19g/protect_before.txt --skip 19b 19c 19d 19f 19g`
  (`reports/data/phase-09-text-layer-v3-prep/planning/protect.py`) and a `find … | sort | xargs shasum -a 256` list of
  `15y-b`, `15z-b`, `15z-a`, `15x-a`, the BASE clones and `data/text_layer_v4_full`; `_after` at the end; `diff`.
- Never run `git stash`, `checkout`, `reset` or anything that changes the working tree. Never `git commit`; `chmod
  g+w`; `.venv/bin/python`; zsh; no `rm -rf`, `nohup`, `&`; shared files only with Edit; never the external drive.
- An error category that names a layout cause (two columns in one region) is checked against `layout/regions` and
  `layout/lines` of BASE, not inferred from the listing (the review found 14 of 26 such verdicts wrong in 19d).

## Acceptance criteria

1. Tests green; full suite green (counts from `--collect-only`).
2. Part A done before any code ran on the fresh sides (hashes in PROGRESS); ≥ 18 `index` sides.
3. Selection numbers of X1–X4 and the baselines; the choice follows the rule.
4. **Entry bar v2** (pre-registered 2026-10-28 by the phase review; fresh sides, once): micro P ≥ 0.90 **and**
   R ≥ 0.85 **and** F1 > the v4 hand F1 on the same sides. Fail → negative result, no tuning, no second test, Decide
   UA (next free ID). Pass → STATUS: the chosen cfg and clone paths for 19e.
5. Part D: unflagged units identical; protected lists unchanged apart from your clones and other projects'
   `shared-data/` changes (name them).

The user decides when the step is done.

## Error analysis

Read every FP and every FN on the fresh sides (all if ≤ 40 of each kind, else 40 of each with
`random.Random(20261127)` over the sorted list), each with two lines before and after in region order and its layout
region id. Categories: two columns in one region (checked in the layout tables), line in no main unit (its
`line_type`), entry with no cue (say what would have marked it), continuation cut (which cue), gutter merge, gold
ambiguity. Compare per group (G1–G4) and with E5. Findings: the bar result, the chosen variant, the error causes,
the unit counts.

## Do not touch

`layout.py`, `page_structure.py`, `index_pages.py`, `types.py`, `build.py`, published layers, dev layers other than
your clones, the 19a gold files, the review files (`fresh_entry_sides.tsv` is read only), `shared-data/`, siblings.

## When done

1. Report: `### Step 19g: index entry rule v2` after 19f's section (what changed, the fresh gold, selection table,
   test bar with baselines, unit counts, error analysis; findings `F13-19g.n` as `draft`); `make reports` clean.
2. Changelog (Edit): `### Step 19g: index entry rule v2`: the `no_cut_pattern` and `main_types` keys, their
   defaults, `presets.SG_INDEX_NO_CUT`, `SG_INDEX_ENTRY_V5B`. "Affected sibling imports: none."
3. `PROGRESS.md` block (template of `plan/templates/STEP.md`); overwrite `STATUS.md`: 19g done, the bar result, facts
   for 19e.
