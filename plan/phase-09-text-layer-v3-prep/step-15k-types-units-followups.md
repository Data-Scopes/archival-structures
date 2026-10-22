# Step 15k: page-number types, crosswalk role, units

> **Phase:** 09, Text layer v3 preparation. **Depends on:** 15j (its layer `15j-a` is your baseline).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to
> explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Four phase-07 follow-ups in the `types` and `units` stages.
- (A) Pn-shaped band lines that the page-number chain doesn't confirm (`rule:pn_line_offchain`,
  3,293 segments) still say `line_type="page_number"`, and most are not page numbers (F07-15ir.6).
  Some are: they fit the chain but lost the page's candidate slot to another band number.
- (B) Bottom-band years are typed `page_number` (37 off-chain).
- (C) The REPUBLIC crosswalk maps a paragraph's `line_end` id on a line AS split to the first
  half (553 ids).
- (D) `claimable` in `unit_inventory` is quadratic (F07-R.11), and the library hard-codes LotA's
  REPUBLIC path.

The retype is decided per band by reading, against bars fixed at planning (decision P6). You
read **first**, set the presets, then build once.

## Inputs

- Baseline layer `data/text_layer_v2_dev/15j-a/{hand,print}` (15j); sources as in the build command.
- Planning scripts (`reports/data/phase-09-text-layer-v3-prep/planning/`, `.venv/bin/python -I`, from the
  AS root): `pn_offchain.py` (counts by sub/band/shape and `gap`, seeded sample),
  `crosswalk.py` (match × role), `compare.py A B`, `protect.py`. The planning sample
  `planning/pn_sample_planning.tsv` (drawn on `15ir-a`, seed 41: 20 rows each of hand-top,
  hand-bottom, print-top, print-bottom = non-gap, non-year off-chain; all 37 non-gap years; 20 gap
  rows), and `pn_15ir-a.json`, `crosswalk_15ir-a.json`.
- Code anchors (checked 2026-10-21):
  - `text_layer/types.py`: `TYPES` L26–27 (`main_text, page_number, running_header, date_header,
    catch_word, signature_mark, marginalia, index, other`), `TypeParams` L40–82 (frozen; patterns
    `pn_line … signature` `str = ""`; `top_n=3`, `bottom_n=2`; `__post_init__` L70–75 validates
    `margin_cut`, `top_n`, `bottom_n`; `from_dict` = `cls(**dict(d))`), `_pn_candidate` L161–176
    (returns `(number, "pn_line"|"pn_suffix", line)`; number = first `\d{1,4}`), `_is_year` L179,
    `_whole_line` L277–308 (`page_number`/`rule:pn_line` at L285 for band lines that fullmatch
    `pn_line`, except top-band years), `type_inventory` L319–424 (pass 1 L351–363: `cands`,
    `chain = longest_increasing_chain([...])`, `cand_by_key`, `chain_set`; pass 2 L365–387 with the
    15ir relabel at L379–385, quoted below).
  - `text_layer/presets.py`: `SG_TYPE_PATTERNS` L83–92 (`year_line` `r"\W*1[67]\d\d\W*"`),
    `SG_HAND_TYPES_PARAMS = dict(SG_TYPE_PATTERNS)` L93, `SG_PRINT_TYPES_PARAMS` L94.
  - `text_layer/units.py`: `REPUBLIC_PARAGRAPHS_DIR` L35–36, `REPUBLIC_COLUMNS` L44,
    `republic_crosswalk` L248–307 (`split_original` L280–293: `found = inside[0]`),
    `read_republic_ids` L318–326, `unit_inventory` L393–491 (`claimable` L421–423; crosswalk
    L468–478; stats L483–491, `"seconds"` included).
  - `text_layer/build.py`: `stage_units` L427–476 (imports the constant L430–431; default L435;
    warning L445–452; `params.json` L453; `republic_path=` L460–461); `--republic-dir` L750–752.
  - `docs/text_layer.md`: *Type tables* L257–301, *Units* L302–357 (`republic_lines` row L344,
    crosswalk text L351–357, `republic_dir` L305–311).

```python
# types.py L379-385 (pass 2, per page key)
        cand = cand_by_key.get(key)
        confirmed_id = (cand[2].line_id if key in chain_set and cand and cand[1] == "pn_line"
                        else None)
        for s in page_segs:
            if s["type_method"] == "rule:pn_line" and s["line_id"] != confirmed_id:
                s["type_method"] = "rule:pn_line_offchain"
# units.py L421-423
        claimable = {r["region_id"]: frozenset(legacy_lines[(sid, r["region_id"])])
                     for r in legacy if r["scan_id"] == sid and r["region_id"] not in layout_ids
                     and legacy_lines.get((sid, r["region_id"]))}
def republic_crosswalk(republic_ids, lines_by_scan, tol=NEAR_PX)   # rows [rid, scan, line_id, match]
def read_republic_ids(path)     # distinct line_start/line_end ids, first-seen order
```

## Part 1: read before you code (the bars)

Copy `planning/pn_sample_planning.tsv` to `reports/data/phase-09-text-layer-v3-prep/step-15k/pn_verdicts.tsv`
and fill `verdict` for every row: `pn` (this page's number, also if misread, e.g. "552 )" between
550 and 552), `not_pn` (signature, quire/ledger/catch number, marginal number, a year, noise) or
`unclear`. Use the context columns (this page's confirmed number, the previous and next page's, the
other band numbers). The `band` column there is y < page_h/2, an approximation of the code's band.
**Bars (fixed at planning; unclear counts as `pn`):**
- per stratum `hand-top`, `hand-bottom`, `print-top`, `print-bottom`: the band is retyped to `other`
  in that sub-layer's preset iff `not_pn` ≥ 16 of 20;
- stratum `year` (37 rows, pooled): `offchain_years_other=True` in both SG presets iff `not_pn` ≥ 30;
- stratum `gap` (20): no bar; report the `pn` share (planning read 8 of 8 as real page numbers).
Write the four outcomes into the report before building. Don't re-read rows to change an outcome.

## Deliverables and behaviour

**A/B, `types.py`.** New `TypeParams` fields: `offchain_retype: bool = False`,
`offchain_other_bands: tuple = ()`, `offchain_years_other: bool = False`; validate
`set(offchain_other_bands) <= {"top", "bottom"}` (`ValueError`). Default `TypeParams()` output
is unchanged. When `offchain_retype` is on, after the 15ir relabel, for each segment with
`type_method == "rule:pn_line_offchain"` on page `key`:
1. `n` = first `\d{1,4}` of the line text (`by_id[s["line_id"]].text.strip()`, with
   `by_id = {l.line_id: l for l in ordered}` and `ordered = contexts[key][0]`).
2. `p`, `q` = the chain numbers (`cands[i][1][0]` for `i in chain`, both kinds) of the nearest page
   before and after `key` in the pass-2 page order that is in `chain_set`. If both exist and
   `p < n < q`: `type_method = "rule:pn_line_gap"`, `line_type` stays `page_number`.
3. Else, if `_is_year(text, pats)` and `offchain_years_other`: `line_type = "other"`,
   `type_method = "rule:year_offchain"`.
4. Else, if the line's band is in `offchain_other_bands`: `line_type = "other"` (`type_method`
   stays `rule:pn_line_offchain`). Band = `"top"` if the line is in `ordered[:params.top_n]`, else
   `"bottom"`.
Set `offchain_retype=True` in both `SG_HAND_TYPES_PARAMS` and `SG_PRINT_TYPES_PARAMS` (as new dicts;
`SG_PRINT_TYPES_PARAMS` keeps `column_override`), with the bands and the years flag from Part 1.
Comment the bar outcome next to each value ("step 15k, P6: hand-top 7/20 not_pn → kept", etc.).

**C, `units.py`.** `read_republic_roles(path) -> dict` maps each id to `start`, `end` or `both`, in the
same first-seen order as `read_republic_ids` (keep that function). `republic_crosswalk(...,
roles=None)` appends `roles.get(rid, "") if roles else ""` to every row, and in `split_original`
takes `inside[-1]` when the role is `end` (`inside[0]` otherwise). `REPUBLIC_COLUMNS` gains `"role"`.
`unit_inventory` calls `roles = read_republic_roles(path)`, then `republic_crosswalk(list(roles),
lines_by_scan, roles=roles)`. Add `republic_crosswalk_roles` (`"match|role"` counts) to the stats.
Rationale for the docstring: REPUBLIC reads an unsplit merged line as part of the paragraph, so a
paragraph that ends there ends in our last half (planning read 10 of 10, F09-P.5).

**D, `units.py` and `build.py`.** Group `legacy` by scan once (`legacy_by_scan = defaultdict(list)`
before the scan loop) and build `claimable` from `legacy_by_scan[sid]`. The output must be identical.
Delete `REPUBLIC_PARAGRAPHS_DIR`. In `stage_units`, `republic_dir = Path(run.args.republic_dir) if
run.args.republic_dir else None`. In print mode with `None`, `run.log("[units] WARNING: no
--republic-dir given: no REPUBLIC crosswalk")`, write `republic_dir: null`, and pass
`republic_path=None`. Keep the per-file warning when a dir is given. After the change,
`grep -rn language-of-the-archive archival_structures` must give nothing.

**Docs** (`docs/text_layer.md`): *Type tables* gets `rule:pn_line_gap`, `rule:year_offchain`,
`other` for retyped off-chain numbers, and the three params. *Units* gets the `role` column, the
`split_original` rule, and the rule that `--republic-dir` is required for the crosswalk (no default).

## Constraints

- Build (check `coreshare.py status` first); print **must** pass `--republic-dir`:
  ```
  O=data/text_layer_v2_dev/15k-a; mkdir -p $O/hand $O/print
  python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15k-hand --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir $O/hand --preset sg_1.01.02_v2 --stage all --workers {workers}
  python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15k-print --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir $O/print --preset sg_1.10.94_v2 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
  ```
- Protected: `planning/protect.py data/text_layer_v2_dev/15k-protected-{before,after}.txt --skip 15k-`
  before and after; empty `diff`. v1: the 15j brief's v1 command with `S=data/text_layer_v2_dev/15k-v1check`
  → `TOTAL 157 / 157`.
- Don't touch `layout.py`, `page_structure.py`, `join.py`, `join_v2.py`, `word_breaks.py`, `extract_v2.py`.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`; no `rm -rf`, `nohup`, `&`.

## Tests

`tests/test_text_layer_types.py` (15 now), `tests/test_text_layer_units.py` (11 now):
- `test_offchain_gap_kept`: chain pages 10, 11, 13 plus a non-candidate band line "( 12 )" on the page
  between 11 and 13 → `rule:pn_line_gap`, `page_number`;
- `test_offchain_band_retyped`: with `offchain_other_bands=("bottom",)`, a bottom off-chain "41" →
  `other`; a top one stays `page_number`; with `offchain_retype=False` both stay (old behaviour);
- `test_bottom_year_offchain`: a bottom "1717." off-chain → `other` / `rule:year_offchain` when the
  flag is on;
- `test_types_params_validated`: extend with `offchain_other_bands=("middle",)`;
- `test_crosswalk_role_end_maps_last_half` (one REPUBLIC id as `line_end` over two AS halves → second
  half, role `end`; as `line_start` → first half); extend `test_republic_crosswalk_exact_near_split_original`
  for the role column;
- `test_no_republic_dir_no_crosswalk` (print mode, no `--republic-dir`: warning, `republic_dir`
  null, no `republic_lines/` file); adapt `test_missing_republic_file_logged`;
- `test_claimable_per_scan`: two scans with legacy twins: same ids as before.
Full suite: `.venv/bin/python -m pytest -q -p no:cacheprovider` (give the count from `--collect-only -q`).

## Acceptance criteria

Compare `15k-a` with `15j-a` (`compare.py`), and run `pn_offchain.py` and `crosswalk.py` on both.
1. Builds exit 0, 0 failures, `sanity_pass` true.
2. Before you build, re-run `pn_offchain.py --layer data/text_layer_v2_dev/15j-a` and record any drift
   from planning (`15ir-a`: off-chain hand 2,464 / print 829; gap hand 184 / print 4; non-gap years
   hand 34 / print 3). Hand should not drift (15j leaves hand types alone).
3. **Types:** `rule:pn_line_gap` = the 15j-a gap count (planning: 188). The `other` retypes per
   stratum equal the non-gap counts of the retyped bands (planning: hand top 1,356, bottom 890;
   print top 456, bottom 366), and the years count matches (planning: 37). No other segment
   changes `line_type` or `type_method` (`compare.py` on `types/segments`: only those two
   columns, only these rows).
4. **Units:** `units/units`, `segments` and `doc_id_map` differ from 15j-a only in units that hold
   retyped segments: their `unit_type` / `unit_method` and, where a retyped segment now sits next to an
   existing `other` run (`split_region` joins adjacent segments of one non-main type), the unit
   rows. List these joins with their count. `units/stats.json` 455 `seconds` ≤ 5 (15ir-a: 10.0).
5. **Crosswalk:** every row gets a role, with the same match × role counts as planning
   (455 `split_original|end` 135, 456 378). Exactly the `split_original|end` rows (513) change
   `line_id`, and each moves to the last half of its line.
6. `join/` differs only in the units of criterion 4. AS suite green; v1 157/157; NLP suite green
   (`venv/bin/python -m pytest -q -p no:cacheprovider tests` in
   `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`); hashes unchanged.

## Error analysis

- Part 1 verdicts (above), with the categories and counts per stratum, and 3 verbatim examples each.
- In the built layer, read 10 retyped segments per retyped stratum (`Random(81)`): did the rule do
  what the verdicts predicted?
- `crosswalk.py --layer data/text_layer_v2_dev/15k-a --sample-out step-15k/crosswalk_sample.tsv --seed 61`
  (10 per inventory): does the paragraph end in the mapped half? Categories: *right*, *wrong*, *unclear*.
- Findings `F09-15k.n`: the bar outcomes (results or negative results), gap precision, crosswalk
  correctness, and the units timing.

## Do not touch

`shared-data/`, earlier dev layers, the modules listed under *Constraints*, other steps' report sections.

## When done

1. `### Step 15k: page-number types, crosswalk role, units` in `reports/phase-09-text-layer-v3-prep.md`
   (results table for criteria 1–6, error analysis, findings `draft`); `make reports` clean.
2. Changelog `### Step 15k: …` under *Unreleased* (Edit). List the new `TypeParams` fields,
   `type_method` values, `read_republic_roles`, the `roles` kwarg, the `role` column, and the
   removal of `units.REPUBLIC_PARAGRAPHS_DIR` (no sibling imports it, checked 2026-10-21). Say that
   print builds now need `--republic-dir`.
3. `PROGRESS.md` block; overwrite `STATUS.md` (next: 15l, baseline `15k-a`). Stop and ask if a
   count in criteria 3–5 is off by more than the 15j drift explains.
