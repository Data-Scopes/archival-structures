# Step 15hj: Word breaks and unit text with character offsets (v2 `wbc` and `join`)

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15h.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md`; what you need from it is copied in here. If you
> have to explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

R1: a unit's text must be the concatenation of its lines, with a character offset per line, so
that any token span maps back to line boxes. The SG audit adds: detect word breaks per inventory,
never merge across region ends, never merge digit–mark–digit (money notation `8835„5„`), and
record per row what was merged. This step makes the v2 `wbc` and `join` stages work on 15h's
units, and returns offsets. It also carries four phase-05 follow-ups: the preset carries the
word-break rule and the sanity anchors; a `„`-robust statistic (F05-R.4), adopted only if a
labelled sample supports it; a subset-run guard; and a manifest history.

## Inputs

Layer: the 15h layer named in `STATUS.md` (planned `data/text_layer_v2_dev/15h-a/{hand_tar,
hand_local,print}/`; confirmed, 15h used `15h-a`; table columns as below, unchanged). `unit_method` values: hand
`hand:run|region|region_cut_at_header`, print `print:run|region|region_start|<cues>`. Tables used:
- `units/units/<inv>.tsv.gz`: `scan_id, page, unit_id, unit_order, unit_type, unit_method,
  layout_region_id, layout_column, x, y, w, h, n_segments, n_lines, first_line_id, last_line_id`;
- `units/segments/<inv>.tsv.gz`: `scan_id, unit_id, seg_order, line_id, seg_idx`;
- `types/segments/<inv>.tsv.gz`: `scan_id, line_id, seg_idx, char_start, char_end, …`. The
  segment text is `lines.text[char_start:char_end]`;
- `units/doc_id_map/<inv>.tsv.gz`: `old_doc_id, unit_id, link, old_status, …`;
- `lines/<inv>.lines.tsv.gz` (`text`).

Code (AS HEAD `73e1517`; 15fq–15h edit `build.py` and `presets.py` before you):
- `text_layer/join.py`: `library_join(texts, wbcs)` L27 calls
  `pagexml.helper.pagexml_helper.make_text_region_text(lines, word_break_chars=set(wbcs))`, which
  returns `(text, line_ranges)`. Each range is a dict `{"start", "end", "line_id", "parent_id"}`.
  `library_join` drops the ranges. `classify_boundary(prev, nxt, wbcs, guards=ALL_GUARDS) ->
  (kind, char)` L45, with kinds `plain`, `merge`, `g_standalone`, `g_digit`, `g_digit_left`,
  `g_punct_left`, `g_digit_right`. `join_region(texts, wbcs, …, guards=ALL_GUARDS,
  both_ends_fix=True) -> str` L79–144 drops empty texts, removes a leading WBC from the next line
  on a `merge` (both-ends fix), cuts runs at guarded boundaries, joins each run with
  `library_join`, and joins the runs with `" "`. `_make_line(text, idx)` L20 makes dummy lines.
- `text_layer/word_breaks.py`: `word_break_stats(regions, inventory_number) -> (rows, info)`
  L125 (regions = iterable of line-text lists); `WORD_BREAK_STATS_COLUMNS` L210;
  `write_word_break_stats_part(inv, layer_root)` L223 (reads v1 through
  `io.read_region_lines`, which needs the v1 `line_text` column, so it can't read v2);
  `pooled_stats` L232; `rule_passes(n_end_attached, join_vocab_frac, end_rel_frac, rule)` L256;
  `decide_word_breaks(stats, inventory_lines, rule=DEFAULT_WORD_BREAK_RULE, *,
  sanity_in=SANITY_IN, sanity_out=SANITY_OUT) -> (per_inventory, corpus)` L262;
  `read_word_break_sets(layer_root)` L336; `SANITY_IN = ["¬"]`, `SANITY_OUT = [":", "."]` L70–71.
  `end_rel_frac = (end_freq/total_end) / (all_freq/total_all)` (L188).
- `text_layer/build.py`: `stage_wbc` L393–443, `stage_join` L453–485. v2 refuses `wbc`/`join` at
  L546–549 ("not available for v2 presets yet (phase 07)"). `Run.update_manifest(stage, extra)`
  L153–185 overwrites `stages[stage]`. Under `--stage all`, v2 runs only extract (+ wordfix).
- `text_layer/extract_v2.py`: `legacy_region_text(line_texts)` L44 = `library_join(non-empty
  texts, ["-", "„"])`, which reproduces the legacy TSV text (F06-15d.1).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/join_v2.py` (new) | `join_unit(...)`, `read_unit_texts(layer_root, inv)`, `write_word_break_stats_part_v2(inv, layer_root, robust)`, `join_inventory_v2(inv, layer_root, wbcs)`, the column lists |
| `text_layer/word_breaks.py` | Additive: `robust_counts(lines, chars) -> {c: n_excluded}` and `end_rel_frac_robust` in the v2 rows only. **v1 output must stay byte-identical** |
| `text_layer/presets.py` | Fields `word_break_rule: Optional[Mapping] = None`, `word_break_stat: str = "default"`, `wbc_sanity_in: tuple = ("¬",)`, `wbc_sanity_out: tuple = (":", ".")`. SG presets: `wbc_sanity_in=()`. `word_break_stat` is set by the adoption test below |
| `text_layer/build.py` | v2 `wbc`/`join`; `--allow-subset-wbc`; manifest `history`; v2 `--stage all` = extract (+ wordfix) → layout → types → units → wbc → join |
| `tests/test_text_layer_join_v2.py` (new), `tests/test_text_layer_build.py` | See *Tests* |
| `docs/text_layer.md` | Section *Unit text and offsets (step 15hj)*; update *Building a layer* for v2 `all` |
| `docs/shared_library/changelog.md` | `### Step 15hj: v2 word breaks and join` under *Unreleased*, with Edit |
| `reports/data/phase-07-sg-units-and-pilot/step-15hj/` | `acceptance.py`, `ea.py`, `ea_verdicts.tsv` |
| `reports/phase-07-sg-units-and-pilot.md` | Fill in `### Step 15hj: …`; findings `F07-15hj.n` |

## Behaviour to implement

**`join_unit(texts, wbcs, guards=ALL_GUARDS, both_ends_fix=True) -> (text, spans, boundaries)`.**
`texts` are a unit's segment texts in `seg_order` (all non-empty). It must return exactly
`join_region(texts, wbcs, guards=guards, both_ends_fix=both_ends_fix)` as `text`. `spans[i] =
(char_start, char_end, lead_dropped)`, so that `text[char_start:char_end]` equals
`texts[i][lead_dropped:]`, minus its trailing WBC when boundary *i* is a `merge`. `boundaries[i]
= (kind, char)` for the boundary after segment *i* (`""` after the last). Build it as `join_region`
does: same classification, same both-ends fix, same runs. Take the in-run offsets from
`make_text_region_text`'s `line_ranges`, and add each run's start (runs are joined with one space).
**Check `line_ranges` first** in a test: on `["Cor-", "respondentie"]` with `{"-"}`, and on a
plain pair, assert what `start`/`end` point to. If they don't cover what you need, compute the
offsets yourself, keeping the text equal to `join_region`'s.

**v2 `wbc`.** `read_unit_texts(layer_root, inv)` yields `(unit_row, [segment texts])` for every
unit, in `unit_order`. The stats part feeds the `main_text` units to `word_break_stats`. Where the
preset says `word_break_stat="robust"`, it also adds:
- `all_freq_robust` = `all_freq` minus the occurrences of `c` that are (a) part of a run of `c` at
  line start, or (b) between two digits (ignoring adjacent `c`);
- `end_rel_frac_robust = (end_freq/total_end) / (all_freq_robust/total_all)`. `rule_passes` then
  takes it in place of `end_rel_frac`.

The v2 parts file has `WORD_BREAK_STATS_COLUMNS + ["all_freq_robust", "end_rel_frac_robust"]`
(always computed for v2, so the adoption test can compare). Decisions go through
`decide_word_breaks(…, rule=preset.word_break_rule or DEFAULT_WORD_BREAK_RULE,
sanity_in=list(preset.wbc_sanity_in), sanity_out=list(preset.wbc_sanity_out))`. Pass a rule copy
whose `end_rel_frac` column is the robust one when the stat is robust. Write the same files as v1
under `wbc/`.

**Adoption test for the robust statistic** (F05-R.4: in VOC, the rule missed `„` because `„` is
frequent mid-line and at line starts). Decide it on hand-read data, before switching:
1. Run v2 `wbc` with `default` on all 16 inventories and record the per-inventory sets. Compute
   the robust decision from the same parts.
2. If the two decisions are equal everywhere: keep `default`, record it as a finding, and skip 3.
3. Otherwise read 40 line pairs (`random.Random(19)`) inside `main_text` units, in the
   inventories where the decisions differ. The first line ends in letter + the differing char.
   Label each pair: a genuine word break (left + right make one word), or not. **Adopt `robust` for
   that preset if ≥ 90% are genuine breaks** (when robust adds the char) or **≤ 10%** (when it
   removes it). Otherwise keep `default`.

**v2 `join`.** Per unit, `join_unit(segment texts, wbcs[inv])`. Every unit type is joined.
- `join/units/<inv>.tsv.gz`: `scan_id, unit_id, unit_type, word_break_chars, n_breaks_merged,
  ends_with_break_char, text`.
- `join/segments/<inv>.tsv.gz`: `scan_id, unit_id, seg_order, line_id, seg_idx, char_start,
  char_end, line_offset, break_char_end, merged_with_next, boundary_kind`. Here `line_offset =
  types.char_start + lead_dropped`: a unit position `p` in `[char_start, char_end)` is line
  position `line_offset + (p − char_start)` in `lines.text`.
- `join/stats/<inv>.json`: boundary kinds counted, `wbc_set`, guards.

**Phase-05 follow-ups.**
- *Subset-run guard* (v1 and v2): `stage_wbc` exits with a message if the run's inventories are a
  strict subset of the inventories with a `lines/<inv>.lines.tsv.gz` in the layer, unless
  `--allow-subset-wbc`. Full runs are unchanged.
- *Manifest history:* in `Run.update_manifest`, if `stages[stage]` already exists, append it to
  `manifest["history"][stage]` (a list) before overwriting. Fresh builds get no `history` key.
- v2 `--stage all` runs the whole chain in order. `wbc`'s sanity failure stops before `join`, as
  in v1.

## Constraints

- **v1 is untouched:** the VOC preset's defaults equal today's constants, and the v1 stats
  columns and files are unchanged. Re-run the phase-05 check from the AS root:
  ```
  NLP=/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection
  S=data/text_layer_v2_dev/15hj-v1check; mkdir -p $S
  ls $NLP/output/voc_text_layer/voc_text_layer_v1/lines/ | sed 's/\.lines\.tsv\.gz$//' | paste -sd, - > $S/inv22.txt
  .venv/bin/as-build-text-layer --pagexml-dir $NLP/pagexml --out-dir $S/r22 --stage all --inventories $(cat $S/inv22.txt) --write-events --workers 8
  .venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py $S
  ```
  It must print `TOTAL 157 / 157`.
- Run on a copy: `cp -R` the 15h layer to `data/text_layer_v2_dev/15hj-a/`. Protected: every
  earlier dev layer, `shared-data/`, NLP's `output/voc_text_layer/`, LotA `data/`/`gold/`/`output/`.
  Hash them (15fq's method) into `15hj-protected-*.txt`.
- Don't change `join.py`, `extract_v2.py`, `line_split.py`, `layout.py`, `types.py` or `units.py`
  behaviour.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`.

## Tests

`tests/test_text_layer_join_v2.py`:
- `test_line_ranges_semantics` (pins what `make_text_region_text` returns);
- `test_join_unit_equals_join_region` (a table of at least 12 cases: plain, `-` merge, `„`
  merge with the both-ends fix (`"vande„"` + `"„selve"`), money `"8835„"` + `"5„"` (`g_digit`),
  standalone dash, digit right; for each, `text == join_region(...)` and the span invariant);
- `test_join_unit_no_guards_equals_library_join`;
- `test_robust_counts` (line-start run and digit–mark–digit excluded);
- `test_subset_guard` and `test_manifest_history` (in `test_text_layer_build.py`, through
  `build.main`);
- `test_v2_all_chain` on the fixture layer (`--stage all`, preset `sg_1.01.02_v2`): every stage's
  directory exists, and `join/segments` offsets satisfy the invariant.

Run the new tests, then the full suite.

## Acceptance criteria

1. **Offsets (audit §3.9):** for every unit of all 16 inventories, `text[char_start:char_end]`
   equals the segment's text after `lead_dropped` and the merged trailing WBC. Between consecutive
   spans there is `""` after a `merge` and `" "` otherwise. 0 violations.
2. **Equality:** `join_unit(...)[0] == join_region(...)` for every unit (0 differences).
3. **Legacy text (audit §3.3):** for every hand unit whose legacy region is `identical` in
   `doc_id_map`, `join_unit(texts, ["-","„"], guards=(), both_ends_fix=False)[0] ==
   legacy_region_text(line texts)`. Report the count and 0 failures.
4. **Decisions:** the per-inventory WBC sets (default and robust), `sanity_pass`, and the adoption
   result. Report the boundary kinds per inventory, and the digit–digit merges (expect 0: `g_digit`).
5. **Word breaks (audit §3.10):** the share of `merge` boundaries whose right side starts lower
   case, per hand inventory (target ≥ 0.95; report it, don't tune).
6. v1 `TOTAL 157 / 157`; full AS suite green; protected hashes unchanged.

## Error analysis

`random.Random(19)`.
- 30 `merge` boundaries (15 hand, 15 print): a right join? Read the joined word.
- 20 guarded boundaries (`g_*`), and every `merge` with an upper-case or digit right side up to
  20: right or wrong?
- 10 print units whose last segment ends in `-` (a word continued in the next column or page;
  units never cross): list them for 15i's audit.
- The adoption sample, if run (above).

## Do not touch

- `join.py`, the v1 extract; siblings; `shared-data/`; republic-project.

## When done

1. Fill in `### Step 15hj: …` in the phase report: criteria with numbers, the error analysis,
   findings `F07-15hj.n` (`draft`). Expected: the WBC sets per inventory, the adoption result
   (adopted or not, with its sample), the lower-case share. Then `make reports`.
2. Append the progress block; overwrite `STATUS.md` (the join layer path and the final
   `word_break_stat` per preset under *Facts later steps need*; next step 15i). Update brief 15i
   if a path or column differs.
