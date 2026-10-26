# Step 15x: drop-cap-aware print unit rule

> **Phase:** 12, Hand and print by line-box height. **Depends on:** 15wr (its layer `15wr-a` is the base; its read bar
> passed). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then
> this file. **Read nothing else unless this brief names it.** Anchors checked 2026-10-25 and re-checked against the
> code and `15wr-a` by the 15wr review (2026-10-26). Record any exploration
> under *Brief defects*. The read bar and read list below were fixed by the phase review **before any output of the
> new rule existed**; do not change them.

## Context

In printed 1.01.02 volumes the print unit rules cut a paragraph after its first one or two lines. A paragraph opens
with a dropped initial (its own region), and the next one or two lines sit 1.1–1.7 line heights right of the column
edge beside it, so the `indent` cue fires there (3771: 8 of 10 pages worse than the hand rules, F12-15v.4). The same
rules correctly separate the *WAER op gedelibereert* decisions and *Dat …* clauses (F12-R.3). In five *gedrukte
resoluties* volumes the `indent`-only cuts 1 or 2 main lines after the previous cut are 44.9% of all print cuts, but
only 53 of the 4,687 cuts at decision/*Dat* lines (F12-R.6). The user chose this fix (UA-026 (b)): an `indent`-only cut
within 2 main lines of the last kept cut is dropped, for print-mode 1.01.02 only.

## Inputs

- `archival_structures/text_layer/units.py` (513 lines):
  - `UnitParams` L45–61 (`indent_frac`, `short_frac`, `gap_factor`, all `float = None`; `to_dict` = `dataclasses.asdict`;
    `from_dict` rejects unknown keys);
  - `cues_fired(feat, params) -> list` L94: cue names in order `indent`, `short_prev`, `gap`, `header`;
  - `split_region(segs, params, print_mode)` L109. Its print branch, L136–141:
    ```python
    cuts = {}   # i -> cues
    if print_mode:
        for i, feat in enumerate(paragraph_features(mains, header_between), start=1):
            cues = cues_fired(feat, params)
            if cues:
                cuts[i] = cues
    ```
    `i` indexes `mains` (the `main_text` segments of one layout region); piece methods are `print:region`,
    `print:region_start` (first piece) and `print:` + `"+".join(cues)`.
- `archival_structures/text_layer/presets.py`: `LayerPreset` (last fields `mode_presets`, `volume_mode`, L44–47);
  `SG_PRINT_UNIT_PARAMS = {"indent_frac": 0.5, "short_frac": 0.1, "gap_factor": None}` (L147); `SPLIT_GATE_V4` L184
  (15wr; leave it as it is); `MODE_RULE_FIELDS` L185; `SG_1_01_02_V4` L188–192 (its `volume_mode` has `"split_gate":
  SPLIT_GATE_V4`); `SG_1_10_94_V4` L193; `inventory_preset(preset, mode)` L204:
  ```python
  src = PRESETS[preset.mode_presets[mode]]
  return dataclasses.replace(preset, **{f: getattr(src, f) for f in MODE_RULE_FIELDS},
                             mode_presets=None, volume_mode=None)
  ```
  `build.Run.mode_params` and `preset_for` call `inventory_preset`, so a change there reaches the units stage and
  `units/params.json` (`{"hand": …, "print": …}`).
- Tests: `tests/test_text_layer_units.py` (`seg`, `_para_segs`, `_cuts` helpers L13–62; `test_print_paragraph_cues`
  L65); `tests/test_text_layer_modes.py` (`test_v1_v3_presets_unchanged` L235 pops `mode_presets` and `volume_mode` before
  comparing with `FROZEN_PRESETS`; `test_inventory_preset` L242 asserts the print rule fields equal `sg_1.10.94_v3`'s).
- Base layer: `data/text_layer_v2_dev/15wr-a/{1.01.02,1.10.94}` (from 15wr, reviewed; never written). Gated volumes and the
  print-mode list: 15wr's `reports/data/phase-12-hand-print-box-height/step-15wr/gate.tsv` (`inv, series, share, n_rows,
  split, pooled_mid_start, diff`; 87 rows: the 85 print-mode 1.01.02 volumes, `split` `True` for 32 and `False` for 53,
  plus 455/456 with series `1.10.94` and empty `share`/`split`: leave those two out). The 53 gated volumes, comma-separated:
  `step-15wr/gated.txt`. Series per volume: `reports/data/phase-12-hand-print-box-height/step-15w/modes.tsv` (`inv,
  archive, mode, method, value, margin, n_scans, scan_minority_share, silver, series, ref_value, ref_set`).
- What `15wr-a` holds (15wr report, confirmed by its review): the 31 *gedrukte resoluties* and 3463 are split (two-column,
  share ≥ 0.415); the 25 *nouvelles*, 27 *plakkaten* and 9486 are not (share ≤ 0.226), so their lines are whole. Units
  per scan in `15wr-a`: *gedrukte resoluties* 61.12, *nouvelles* 25.87, *plakkaten* 22.21, 9486 11.86, 3463 27.25. 3463 is a
  handwritten index built as `print` (F12-R.5, F12-R.9); it gets the override like every print volume, is in no bar, and
  is reported as a count only.
- Read list: `reports/data/phase-12-hand-print-box-height/review/reads_15x.tsv` (20 scans: 3769 0416, 0600; 3772 0344,
  0369; 3773 0200, 0353; 3774 0281, 0514; 3795 0179, 0360; 3816 0355, 0460; 3823 0032, 0196; 3828 0115, 0195; 3844 0260,
  0323; 3849 0108, 0257).
- Design script (reads today's cuts, applies no rule): `reports/data/phase-12-hand-print-box-height/review/dropcap_design.py`.

## Deliverables

| File | Change |
|---|---|
| `units.py` | `UnitParams.drop_cap_lines: int = None` (last field); `to_dict` drops the key when it is `None`; the rule in `split_region` |
| `presets.py` | `LayerPreset.mode_overrides: Optional[Mapping[str, Mapping]] = None` (last field); `inventory_preset` applies it; `SG_1_01_02_V4.mode_overrides = {"print": {"unit_params": {**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}}}` with a comment (UA-026 (b), F12-R.6, P18) |
| `tests/test_text_layer_units.py`, `tests/test_text_layer_modes.py` | tests below |
| `data/text_layer_v2_dev/15x-a/` | v4 from scratch, all 198 volumes |
| `reports/data/phase-12-hand-print-box-height/step-15x/` | scripts, compare, `suppressed.tsv`, `verdicts.tsv`, `pages.tsv` |
| `docs/text_layer.md` | v4 section: the rule (one paragraph) and the *Known issues* paragraph updated with the outcome |
| `docs/shared_library/changelog.md` | `### Step 15x: drop-cap-aware print units`, with Edit, after 15wr's entry |
| `reports/phase-12-hand-print-box-height.md` | `### Step 15x: drop-cap-aware print unit rule`, a post-review addendum after § Step 15wr; findings `F12-15x.n` |

## Behaviour to implement

In the print branch, keep the index `last` of the last kept cut (0 = the region's first main line):

```python
last = 0
for i, feat in enumerate(paragraph_features(mains, header_between), start=1):
    cues = cues_fired(feat, params)
    if (cues == ["indent"] and params.drop_cap_lines is not None
            and i - last <= params.drop_cap_lines):
        continue          # step 15x: a line beside a dropped initial, not a new paragraph
    if cues:
        cuts[i] = cues
        last = i
```

Only a cut whose **only** cue is `indent` is dropped. `short_prev`, `gap`, `header` and combinations always cut. With
`drop_cap_lines=None` the output is byte-identical to today (455/456, every v2/v3 layer, hand). `UnitParams.to_dict()`
must not contain `drop_cap_lines` when it is `None`, so `units/params.json` of 1.10.94 and of v2/v3 layers does not
change; with 2 it is written. `inventory_preset`: after copying the mode preset's `MODE_RULE_FIELDS`, update them with
`(preset.mode_overrides or {}).get(mode, {})`. A key outside `MODE_RULE_FIELDS` raises `ValueError`. The result has
`mode_presets=None, volume_mode=None, mode_overrides=None`. Hand mode and 1.10.94 get no override. Unit method names do
not change.

Gate: only `sg_1.01.02_v4` in `print` mode (the 31 *gedrukte resoluties*, 25 *nouvelles*, 27 *plakkaten*, 9486, 3463).
Column interleaving in the *nouvelles* was a split defect that 15wr fixed. This step does not address column layout.

## Constraints

- Hand inventories and 1.10.94 byte-identical to `15wr-a`. No change to the cues' thresholds, to `drop_cap_lines` (2) or
  to the bar (P18, P19). `MODE_RULE_FIELDS` stays the same six fields.
- `layout.py`, `types.py`, `line_split.py`, `modes.py`, `hand_print.py` unchanged.
- Protected (hash before and after; run from the AS root; `mkdir -p reports/data/phase-12-hand-print-box-height/step-15x`
  first): `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
  reports/data/phase-12-hand-print-box-height/step-15x/protect_before.txt --skip 15x` (and `_after`), plus
  `find data/hand_print_geom reports/data/phase-10-hand-print-per-region reports/data/phase-12-hand-print-box-height/step-15u data/text_layer_v2_dev/15w-a data/text_layer_v2_dev/15wr-a -type f | sort | xargs shasum -a 256 > reports/data/phase-12-hand-print-box-height/step-15x/protect2_before.txt`
  (and `_after`). `shared-data/` and LotA files change by other projects (15wr saw LotA `gold/06-terms-1.01.02.json`
  change); a difference there that is not yours is expected, report it and go on.
- Never `git commit`; never bump the version; `chmod g+w`; `.venv/bin/python`; no `rm -rf`, `nohup`, `&`; shared files
  (report, changelog, `STATUS.md`, `PROGRESS.md`, `docs/USER_ACTIONS.md`) only with Edit. Next free UA ID: check both
  UA files (UA-028 was the highest on 2026-10-25).
- If `docs/USER_ACTIONS_resolved.md` shows UA-025 answered (a) (15w-a published as a v4 pilot) before this step, say in
  the report that the published pilot predates 15wr/15x. Do not touch `shared-data/`.

## Tests

- `test_print_drop_cap_lines` (units): six lines (`_para_segs`) with lines 1 and 2 indented (`x=360, w=440`),
  `indent_frac=1.0`: without `drop_cap_lines` the sizes are `[1, 1, 4]`; with `drop_cap_lines=2` `[6]`. Lines 1–3
  indented gives `[3, 3]` (the third indent is 3 lines after the last cut). An indented line 1 after a short line 0
  (`w=200`, `short_frac=0.2`) still cuts (`indent+short_prev`). An indent at line 4 after a cut at line 3 (`short_prev`)
  is dropped.
- `test_unit_params_to_dict_omits_none_drop_cap`: `UnitParams(**SG_PRINT_UNIT_PARAMS).to_dict()` has no
  `drop_cap_lines`; `from_dict({..., "drop_cap_lines": 2})` round-trips.
- `test_inventory_preset_overrides` (modes): `sg_1.01.02_v4` print `unit_params == {**SG_PRINT_UNIT_PARAMS,
  "drop_cap_lines": 2}`, hand unchanged, `sg_1.10.94_v4` print equals `sg_1.10.94_v3`'s; a bad override key raises.
  Update `test_v1_v3_presets_unchanged` to pop `mode_overrides` too, and `test_inventory_preset`'s print assertion for
  `sg_1.01.02_v4` `unit_params` (only that field may differ).
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_units.py tests/test_text_layer_modes.py`,
  then the full suite (collect first; give the counts before and after; after 15wr: 1,248 collected, 1,237 passed,
  11 skipped). LotA: no code change, no run needed.

## Steps

1. Code and tests; protect `_before`; `mkdir -p data/text_layer_v2_dev/15x-a && chmod g+w data/text_layer_v2_dev/15x-a`.
2. `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then from the AS root (the
   script asks coreshare for workers itself; 15wr was granted 3 and took about 62 min):
   `.venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir data/text_layer_v2_dev/15x-a`
   (expect 196 + 2), then the same without `--check-only`.
3. Compare with `15wr-a`: `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/compare.py data/text_layer_v2_dev/15wr-a data/text_layer_v2_dev/15x-a --sub-map 1.01.02:1.01.02,1.10.94:1.10.94 --explained-invs <the 85 print-mode 1.01.02 inventories> --out reports/data/phase-12-hand-print-box-height/step-15x/compare_15wr-a.json`.
4. Audit `15x-a` with `step-15w/audit.py` (0 judged failures).
5. `step-15x/suppressed.py`: for every print-mode 1.01.02 inventory, the **suppressed cuts** = lines that start a
   `main_text` unit in `15wr-a` but are inside a unit in `15x-a` (match by `scan_id`, `line_id`, `seg_idx` from
   `units/segments`). Write `suppressed.tsv` (inv, series, scan_id, line_id, text of the line before and of the line)
   and counts per volume and series; units per page and median `main_text` unit lines per series, before and after.
6. Bars and reads (below); report; docs; changelog.

## Acceptance criteria

1. Tests pass; suite green (counts).
2. Compare: the 111 hand inventories and 455/456 equal `15wr-a` in every per-inventory file. In the 85 print-mode
   1.01.02 inventories only `units/units`, `units/segments`, `units/doc_id_map`, `wbc/parts` and `join/*` may differ. In
   the corpus files only `units/params.json` (print entry gains `"drop_cap_lines": 2`), `units/stats.json` (entries of the
   85) and `wbc/*` corpus files may differ (`pinned`, `default_set` identical). Anything else: stop. `compare.py` drops the
   JSON keys `seconds` and `elapsed*` before comparing; the raw `layout|types|units/stats.json` always differ in
   `seconds`, which is not a difference. `mode/<inv>.json` must be equal for all 198 (15x does not touch the gate).
3. Audit: 0 judged failures (`15wr-a`: 3,593 pass, 32 reported lower-case merge shares, 0 judged; self-test ok).
4. The three bars below, computed and applied as fixed.

## Error analysis and the bar (pre-registered 2026-10-25, before the rule existed)

Context for each read: the text of the two lines before and after the line in question, their `x` offset from the
column edge in line heights, and the page's unit list in both builds (`15wr-a`, `15x-a`). Verdicts in `verdicts.tsv`.
Write the read file with every unit's full text and read it whole (15wr read only the first 14–16 lines of each
unit and had to check the rest by geometry; its review re-read them in full).

- **A. Precision of the dropped cuts** (the 20 scans of `reads_15x.tsv`). All suppressed cuts on those scans, or a
  sample of 60 (`random.Random(20261029).sample`, sorted list as input) if there are more. `right`: the line continues
  the paragraph (it sits beside the initial; the sentence runs on). `wrong`: the line starts a new paragraph, decision,
  *Dat …* clause, list item, signature or heading. `unclear` counts as `wrong`. **Pass iff right / read ≥ 0.90.**
- **B. Decisions and clauses kept** (automatic, all 31 *gedrukte resoluties* volumes). Take the lines whose text
  matches `^(W[AE]+R op|Dat |DAt |IS (na|goet)|Is (na|goet))` and that start a `main_text` unit in `15wr-a`. **Pass iff
  ≥ 0.97 of them still start a unit in `15x-a`** (design expectation about 0.99, F12-R.6).
- **C. Pages** (the same 20 scans). Each page's units in `15wr-a` and `15x-a` side by side: `better` (closer to the
  printed paragraphs), `same`, `worse`, `mixed`. **Pass iff `worse` ≤ 2 and `better` ≥ 12.**
- **Pass = A and B and C.** Expectation (not a bar, F12-R.6): roughly 45% fewer print cuts in the *gedrukte
  resoluties*; 3771 units per page from 45.5 to about 35 (hand rules 25.9).
- **Stop rule.** If any bar fails: report a negative result (finding with the numbers), set `SG_1_01_02_V4.mode_overrides`
  back to `None` (keep the field, `drop_cap_lines` and their tests), do not change `drop_cap_lines`, cues or bars, and
  do not rebuild. `15wr-a` stays the v4 dev reference. Add a UA (Decide: keep the print rules, use the hand unit rules
  for printed 1.01.02, or a new design in a later step) and stop.
- **Diagnostic, no pass/fail: *nouvelles* and *plakkaten*.** Count suppressed cuts per series. Read 10 per series
  (`random.Random(20261028)` over that series' sorted suppressed cuts; all if fewer), with the same verdicts. If fewer
  than 7 of 10 are `right` in a series, write a finding and a UA proposing to apply the rule only to the two-column
  volumes (`split_gate.split` true in `mode/<inv>.json`: the 31 *gedrukte resoluties* and 3463). Do not change the gate
  in this step. 3463: count its suppressed cuts only.

## Do not touch

`data/text_layer_v2_dev/{15m-a,15n-a,15o-pub,15i-src,15v-a,15w-a,15wr-a}`, `data/hand_print_geom/`,
`reports/data/phase-10-*`, `reports/data/phase-12-*/step-15u/`, `step-15w/`, `step-15wr/`, `review/`, published layers.

## When done

1. Report section (post-review addendum after § Step 15wr): what changed; acceptance table; counts per series before
   and after; bars A, B and C with numbers, categories and verbatim examples (right and wrong); the diagnostic;
   conclusion. Findings `F12-15x.n` (`draft`); `make reports` clean.
2. `docs/text_layer.md`: on a pass, one paragraph on the rule (`drop_cap_lines=2`, print-mode 1.01.02 only) and remove
   the drop-cap sentence from *Known issues*. On a fail, say so in *Known issues*.
3. Append the block below to `PROGRESS.md`; overwrite `STATUS.md`: 15x done, next the 15x review (Opus), UA IDs.

```markdown
### Step 15x: drop-cap-aware print unit rule
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `UnitParams.drop_cap_lines`; `LayerPreset.mode_overrides`; `inventory_preset` applies overrides;
  `sg_1.01.02_v4` print units
- **Tests:** <command> → <result>
- **Acceptance:** <1–4, with numbers; bars A, B, C>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15x](../../reports/phase-12-hand-print-box-height.md#step-15x-drop-cap-aware-print-unit-rule); findings F12-15x.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <UA IDs or "none">
```

Changelog entry text (adapt the numbers): "`text_layer.units.UnitParams` gains `drop_cap_lines` (default `None`; omitted
from `to_dict` when `None`). With it set, an `indent`-only print cut within that many main lines of the last kept cut
is dropped. `text_layer.presets.LayerPreset` gains `mode_overrides` (last field); `inventory_preset` applies it.
`sg_1.01.02_v4` sets `drop_cap_lines=2` for print-mode volumes (unpublished preset; behaviour change). v1–v3 and
`sg_1.10.94_v4` output unchanged. Affected sibling imports: none."
