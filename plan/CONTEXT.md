# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-27, review of phase 11 (Opus).

## 1. Text layer v2/v3/v4: what exists now (after phase 12 and its addenda 15wr, 15x)

- **CLI:** `.venv/bin/as-build-text-layer --preset sg_1.01.02_v3|sg_1.10.94_v3|sg_1.01.02_v4|sg_1.10.94_v4
  --pagexml-dir D --out-dir O --stage all` (extract → layout → types → units → wbc → join; each stage also runs alone;
  `--*-params` JSON is merged over the preset). **1.10.94 needs `--republic-dir`** for a crosswalk (no default since
  15k); 1.01.02 gets none (F12-15v.5).
- **Schema:** `docs/text_layer.md` is the contract (incl. *Text layer v3* and *Text layer v4*); LotA writes its reader (D18).
- **Presets** (`text_layer/presets.py`):
  - `SG_HAND_V2`: layout `merge=False`; `wbc_pinned` `-=„` default, `-:=„` for 11281, `-„` for 3349–3351, 3353, 3354;
  - `SG_PRINT_V2`: `merged_line_split="on"`, `column_override=True`, `unit_params` `SG_PRINT_UNIT_PARAMS`
    (`indent_frac=0.5, short_frac=0.1, gap_factor=None`), `wbc_pinned` `-=„`; `SG_*_V3` = v2 renamed;
  - **v4** (phase 12): `SG_1_01_02_V4`, `SG_1_10_94_V4` carry `mode_presets` (`hand` → `sg_1.01.02_v3`, `print` →
    `sg_1.10.94_v3`) and `volume_mode` (`box_height`, `BOX_HEIGHT_V4 = {"cut": 87.75, "min_region_chars": 100}`;
    1.10.94 `fixed`/`print`). `inventory_preset(preset, mode)` copies the six `MODE_RULE_FIELDS`, then applies
    `LayerPreset.mode_overrides[mode]` (whole fields; other keys raise). `sg_1.01.02_v4`: `volume_mode["split_gate"]` =
    `SPLIT_GATE_V4` (`modes.column_start_share` ≥ 0.35 or unmeasurable → split; 15wr, P16) and `mode_overrides` print
    `unit_params` with `drop_cap_lines=2` (an `indent`-only print cut within 2 main lines of the last kept cut is dropped;
    15x, P18; `UnitParams.to_dict` omits the key when `None`). 1.10.94 has neither;
  - `VOC_V1`: byte-identical to NLP's v1 (`TOTAL 157 / 157`, command in step 15j's brief).
- **v4 build** (`text_layer/modes.py`): extract pre-pass (`line_split.prepass_rows`, raw rows) → `decide_mode` →
  `mode/<inv>.json` (+ `mode/scans/`, `mode/regions/`) → extract with the mode's rules; later stages read the mode
  (`read_mode`); `layout/types/units/params.json` = `{"hand": …, "print": …}`. Run kit
  `scripts/run_text_layer_v4_full.py` (`--src ARCHIVE=DIR`, log `run_v4.log`, audit at the end).
- **Layers:**
  - dev: `data/text_layer_v2_dev/<step>-<letter>/`, one per run, never overwritten. `15m-a/{hand,print}` (v3),
    `15v-a` (v4, 16 volumes), `15w-a` (v4, 196 + 2 local volumes; has the F12-R.4 split defect), `15wr-a` (+ split gate),
    **`15x-a` (+ drop-cap rule): the reviewed v4 dev reference**, all protected. Staged tars are linked
    from `data/text_layer_v4_staged/NL-HaNA_1.01.02/` (182);
  - published, never rebuilt (D17): `text_layer_v2` (= `15i-b`) and the `text_layer_v3` pilot (15o, = `15n-a`). Both run
    the hand rules on the printed 3771–3773, 3821–3823 (known-gap notes). No v4 layer is published (UA-025).
- **Audit tooling:** v4 `reports/data/phase-12-hand-print-box-height/step-15w/audit.py --layer <base> --out F`
  (discovers sub-layers, inventories, sources; v4 `mode/` invariants; `--self-test`). Older: phase-09
  `step-15m/{audit,rows116}.py`, metrics in `reports/data/phase-09-text-layer-v3-prep/planning/` (`compare`, `protect`, …);
  phase-12 comparer `step-15v/compare.py` (`--sub-map`, `--ignore-paths`, `--explained-invs`).
- **`analysis/hand_print.py`:** `hrel` (phase 10, negative) and the box-height API (15u: `BoxHeightParams`,
  `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`). Caches `data/hand_print_geom/15q-a/`,
  `15u-a/` (protected).
- **pagexml-tools:** still 0.9.0 (UA-007 waits for 0.9.1); traps and workarounds in F07-P.7, F07-15fp.11, F07-P.9.

## 2. Accepted deviations (phases 07, 09, 12): later briefs must assume these

**Layout** (`analysis/page_structure.py`, `text_layer/layout.py`):
- Line order (15j, P4): merges and column-split pieces sort by printed row (`line_sort="row"`, rows within 0.2 ×
  median box h, then x); print `unchanged` regions too (`sort_unchanged_lines`); hand keeps XML order. 26 same-row
  swaps remain (F09-15j.4).
- Gutter rule `auto`: `gap` when ≥ 95% of openings have a gap (3354, 3771–3823, 455, 456), else `main_edge` (D6).
- **Hand runs without region merges** (`merge=False`, F07-15fq.3). Print regions are split on the line `column` field
  (missing = one group); the column merge guard is on by default (15fq2). The interleave guard is bridged in 55 print
  merges, on purpose (F09-P.1). Blocked counters count pairs of final regions only.

**Types** (`text_layer/types.py`):
- Segments carry types; only `date_prefix`, `margin_prefix` (month-only, within 0.04·`page_w`, D21) and `pn_suffix`
  split a line. Main left edge and centre per main column (keyed by `column` under `column_override`). `page_w` is the
  scan width. Whole date/pn/year lines are never split by rule 1; `rule:pn_line` only in-chain; 15k off-chain classes
  (`rule:pn_line_gap`, `rule:year_offchain`, print bottom-band `other`). Read `type_method` to count pages.

**Units** (`text_layer/units.py`):
- Hand units are layout regions minus furniture, cut at headers. Print units are paragraphs (cues `indent`,
  `short_prev`, `gap` (off), `header`) within a column piece. R5 id rule (D13) with `claimable` (15ir): collisions 0.
- `doc_id_map` covers every legacy region. Crosswalk classes `exact`, `near` (5 px), `split_original`, `split_half`,
  `unmatched`, column `role`; no REPUBLIC default path.

**Word breaks and join:** `„`-robust statistic exists, not adopted (F07-15hj.2); SG sets pinned (D22, D23); `wbc` runs
per layer root; `join_unit` = `join_region` with `lead_marks=""`; offsets exact; lead marks (15ir, 15l) as in the
presets; `join_upper_guard` exists but is off (F09-15l.4).

**v4 (phase 12):**
- One mode per volume (UA-019 (b)); the scan-level box height is a diagnostic, not a label (F12-15u.4).
- The 15w audit keeps the lower-case merge share as an invariant with `reported: true`, outside `n_fail_judged`
  (needed for `15m-a` 178/4). Read `n_fail_judged`.
- The printed 1.01.02 volumes get the 1.10.94 print rules plus the split gate (only the two-column *gedrukte
  resoluties* and 3463 are split) and the drop-cap rule (all 85 print volumes, also the single-column *nouvelles* and
  *plakkaten*, where it helps on readable text but merges some *nouvelles* letter parts, F12-R.13; UA-029). 0.7% of
  decision/*Dat* starts are lost to the drop-cap window (F12-15x.3). Audit on `15x-a`: 33 reported shares (12118 from
  the drop-cap rule, F12-R.14).

## 3. Data facts that shape SG work

- Hand 1.01.02 and print 456 have no region labels; 455 has Laypa labels, mostly on empty regions (F07-P.1, F07-P.2).
- Print 1.10.94 regions are page-level blocks interleaving both columns (F07-15fp.3); the HTR merges a page's date
  header, first line and page number into one TextLine (F07-P.10); hand line-end words are often a separate region
  (F07-15fq.9).
- Break marks: `-`, `=`, `„` (and `:` in 11281); `=` often a compound marker; `.` a break half the time; a repeated mark
  reads `,` (hand), `J` (455 only) or a capital (F09-15l.5).
- **Hand/print (phases 10, 12).** No PageXML attribute marks print (F10-P.2). Raw char-weighted median line-box height
  separates them in one scan campaign: print 65–87 px, hand 88–182 (cut 87.75, F12-15u.1); dividing by page width breaks
  it (F12-R.1). The hand volumes below 100 px are **index** volumes (86.5–97; 251 index-like titles in the EAD;
  F12-R.5). Box heights of 455/456: 75.0 / 82.5.
- **1.01.02 print series differ in layout** (F12-R.4): *gedrukte resoluties* (3760–3864) are two-column like
  455/456 (column-start share 0.50) with paragraph-level regions and dropped initials (F12-R.6); *nouvelles* (letters
  of envoys, F12-P.1) and *plakkaten* (landscape sheets, `page_w` about 4,000) are single-column (≤ 0.23). REPUBLIC
  1.01.02 line boxes come from another line detection: the crosswalk matches 0.5% (F12-15v.5, .6).
- Upper-case merges: hand 4,222, print 4,447 in `15m-a`; about half are wrong. Lower-case merge share below 0.95 in 75 of
  198 `15w-a` volumes, capitalised nouns (F12-15w.9).

## 4. Open follow-ups for the next phase

From phase 12 (reviewed 2026-10-25; 15wr, 15x reviewed 2026-10-26; phase done):
- The user's UA-024 (full v4 run; cost constants from 15wr, unchanged by 15x), UA-025 (pilot from `15x-a` or none),
  UA-029 (drop-cap rule in the single-column series; (b) would be a small step gating the override on
  `split_gate.split`, (d) a letter-part exception with its own read bar).
- UA-028 (a), phase-12 P21: after the full v4 run, a step lists every `print` volume outside the printed EAD series,
  reads them, pins the handwritten ones to `hand` (per-inventory pin in the v4 preset) and rebuilds only those (the
  list also checks scan-resolution shifts, F12-R.1). No EAD title rule in the build.
- Fit a throughput model from the full run's `status.tsv`.

From phase 09: hand-bottom off-chain page numbers (8 `unclear` rows, F09-R.5); `_pn_candidate` should pick the band
number that fits the chain; a mark rule for capitals, then re-test `g_upper_right` (F09-15l.5); pagexml-tools 0.9.1 pin
(UA-007). Phase 08: LotA duplicate filter (F08-R.1); phase-11 candidates F08-R.2, F08-17b.2.

From phase 11 (reviewed 2026-10-27): NLP's AS is a **non-editable 0.13.0 copy** in `venv/`; UA-032 (editable install)
gates a shim step (NLP `unsupervised_segmentation.py` → re-export of the five ported names, `separator_sheets.py` →
AS module; `early_end_extract_body.py` may use `region_types`). Checks against NLP run with `cd NLP &&
PYTHONNOUSERSITE=1 PYTHONPATH=<AS> venv/bin/python …` (no install needed). Open: TextTiling edge padding (F11-R.2, only
with that shim and an NLP re-run); catch-word flag as an END feature for NLP (F11-R.1); F08-R.2, F08-17b.2 (P8); republic
`docs/from-republic.md` §8 items 2–3 and §10 (mid-session start lines; a `session` layer is partial).

## 5. Planning notes: recurring brief defects

- **Protected hashes.** Name the method: `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip
  <own prefixes>`, plus a `find … | xargs shasum -a 256` list for caches; `shared-data/` changes by other projects are
  expected and must be named in the acceptance.
- **Measure every expected number on the layer the step starts from**, with all combined fixes applied, or say it is for
  one fix alone (15ir).
- **The changelog** has no `## Unreleased` heading right after a release; the first step adds it.
- **Name the layer root per sub-layer**; `wbc` decisions depend on it. **When a step re-runs a downstream stage, say
  which later briefs name its layer**, and update them (rule 10).
- **Ports carry their rationale** (comments, decision IDs; F08-R.4).
- **The agent shell:** no `rm -rf`, `nohup` or `&`, a new dir per run, `chmod g+w`, zsh (write the `coreshare.py run …`
  prefix out). Sibling suites with their own interpreter (LotA: `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p
  no:cacheprovider`, 381 passed + 1 xfailed on 2026-10-25).
- **Run every command a brief quotes once at planning, as written**, and every behaviour it predicts for ported code
  (11: TextTiling on constant input; empty-row shares of a reference CSV). macOS `zcat` fails on `.gz`: use `gzip -dc`.
- **Reference numbers at the frozen precision** (15q); quote a planning number only under the step's exact definition
  (F10-R.2). A failed ratio feature: report numerator and denominator before naming a cause (F10-R.1). Give every
  harness subcommand its own output prefix.
- **Say whether a rule reads the line or the segment text** (15k).
- **Hand-read bars work** (phases 09, 12): fixed before reading, one verdict file each, an `unclear` rule, the reader's
  context named. Draw the read list in the planning/review session when the step would otherwise draw it from the
  output of the rule it judges (`review/reads_15wr.tsv`, `reads_15x.tsv`).
- **When rules move to a new series, read every stage's output that can fail there, per series** (F12-R.7): 15w read
  units of the *nouvelles* but no split lines, and the structural audit passed on a million wrong splits.
- **Selected volumes are not a sample:** UA-017's volumes were chosen as contradictions; do not turn their error rate
  into a corpus rate (F12-15w.5 → F12-R.5).
- **A brief that asks for both "reported only" and a pass/fail count** contradicts itself (15w audit); say which.
- **Give read bars an `unreadable input` verdict** apart from `unclear` and `wrong`, reported separately and left out
  of the precision denominator (F12-R.15): 15x's *plakkaten* trigger fired on four unreadable fragments.
