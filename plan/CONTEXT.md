# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-28, review of phase-12 step 15yr (Opus).

## 1. Text layer v2/v3/v4: what exists now (after phase 12 and its addenda 15wr–15z)

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
    15x, P18; `UnitParams.to_dict` omits the key when `None`) and `volume_mode["pins"]` = `MODE_PINS_V4` (16 read volumes:
    14 index volumes → `hand`, *plakkaten* 12163/12165 → `print`; method `pinned`, measured result kept; 15y/15z, P24, P28).
    1.10.94 has none of these;
  - `VOC_V1`: byte-identical to NLP's v1 (`TOTAL 157 / 157`, command in step 15j's brief).
- **v4 build** (`text_layer/modes.py`): pre-pass (`line_split.prepass_rows`, raw rows) → `decide_mode(rows, preset, inv)` →
  `mode/<inv>.json` (+ `scans/`, `regions/`) → extract with the mode's rules; later stages `read_mode`; `*/params.json` =
  `{"hand": …, "print": …}`. Every v2 stage skips an inventory whose done row is newer than its upstream's latest row
  (`build.is_current`, 15y) and skips one whose upstream chain has a last row not `done` (`upstream_ok`, 15yr); sizes come
  from recorded extract rows (`recorded_sizes`). Run kit `scripts/run_text_layer_v4_full.py` (`--src`, `--rebuild ARCHIVE=F`,
  `THROUGHPUT_V4` with whole-layer terms; no `--estimate`; extract rows time the whole call since 15yr, older rows × 1.817);
  `scripts/stage_text_layer_v4_full.py` stages a drive run's tables and mirrors deletions (cap 100).
- **Layers:**
  - dev (`data/text_layer_v2_dev/`, one dir per run, protected): `15m-a` (v3), `15v-a`, `15w-a`, `15wr-a`, `15x-a`,
    **`15y-b` (`15x-a` + 3463 pinned): the v4 dev reference**, `15y-a`, `15z-a`/`15z-b` (the 37 read volumes before/after
    pins). Tars linked from `data/text_layer_v4_staged/NL-HaNA_1.01.02/` (182) and `data/text_layer_v4_staged_15z/` (37);
  - full v4 run (UA-024, pins rebuilt UA-035) on the user's drive only (39 GiB; 266 `print`, 8,856 `hand`); the staged
    `data/text_layer_v4_full/` has its status, `mode/`, `scans/`, `wbc/`, `audit.json`, 22 volumes (+ 24 stale split files);
  - published, never rebuilt (D17): `text_layer_v2` (= `15i-b`), the `text_layer_v3` pilot (15o, = `15n-a`), and the
    `text_layer_v4` pilot (= `15y-b`, 196 + 2 volumes; equal to the full run on those, UA-038).
- **Audit tooling:** v4 `reports/data/phase-12-hand-print-box-height/step-15w/audit.py --layer <base> --out F`
  (`mode/` invariants incl. `pinned`; `--self-test`; keys `<archive>/<inv>`; built = every layer stage's last row done). Comparer `step-15v/compare.py`
  (`--sub-map`, `--explained-invs`); phase-09 `planning/` metrics (`compare`, `protect`, …).
- **`analysis/hand_print.py`:** `hrel` (phase 10, negative) and the box-height API (15u: `BoxHeightParams`,
  `volume_box_height`, `scan_box_heights`, `fit_box_height_cut`, `box_height_mode`). Caches `data/hand_print_geom/15q-a/`,
  `15u-a/` (protected).
- **pagexml-tools:** still 0.9.0 (UA-007 waits for 0.9.1); traps and workarounds in F07-P.7, F07-15fp.11, F07-P.9.

## 2. Accepted deviations (phases 07, 09, 12): later briefs must assume these

**Layout** (`analysis/page_structure.py`, `text_layer/layout.py`):
- Line order (15j, P4): merges and column-split pieces sort by printed row (`line_sort="row"`, rows within 0.2 ×
  median box h, then x); print `unchanged` regions too (`sort_unchanged_lines`); hand keeps XML order, which interleaves
  two-column hand regions (≥ 0.6% of hand units, F12-R.22). 26 same-row swaps remain (F09-15j.4).
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

From phase 12 (reviewed 2026-10-25; addenda 15wr, 15x reviewed 2026-10-26; 15y, 15z reviewed 2026-10-27):
- Phase done (15yr reviewed 2026-10-28); release v0.17.0. The 24 stale split files in `data/text_layer_v4_full` go with the
  user's next staging. A per-mode extract fit would predict print-heavy subsets better (+12% on `15x-a`, F12-15yr.4).
- **Register and index volumes (next text-layer phase).** Hand units are PageXML regions, and two-column hand regions keep
  XML line order: ≥ 0.6% of hand units interleave columns, mostly *tweede serie* registers (427 volumes) and indexes (F12-R.22;
  `review/review_15yz.py interleave_hand`); hand-pinned indexes are under-segmented, print `short_prev` cuts fit entries better
  (F12-R.21, F12-15z.3). Candidates: row-sort and column-split two-column hand regions; an entry cut for index pages. Read bars
  fixed in advance; a fix ships as a new layer version (D17). Also open: parallel layout/types/units (P29, 55% of stage spans);
  mixed volumes 4016, 2600, 9264 stay as built (UA-036 (a)); text reads agreed with images on 175/176 scans (F12-15z.7).

From phase 09: hand-bottom off-chain page numbers (8 `unclear` rows, F09-R.5); `_pn_candidate` should pick the band
number that fits the chain; a mark rule for capitals, then re-test `g_upper_right` (F09-15l.5); pagexml-tools 0.9.1 pin
(UA-007). Phase 08: LotA duplicate filter (F08-R.1); phase-11 candidates F08-R.2, F08-17b.2.

From phase 11 (released v0.16.0): the NLP shim step is unblocked (UA-032; NLP `unsupervised_segmentation.py` and
`separator_sheets.py` → AS; `early_end_extract_body.py` may use `region_types`; check with `cd NLP && PYTHONNOUSERSITE=1
PYTHONPATH=<AS> venv/bin/python …`). Open: F11-R.1, F11-R.2, F08-R.2, F08-17b.2; republic `docs/from-republic.md` §8, §10.

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
- **Hand-read bars work** (phases 09, 12): fixed before reading, one verdict file each, an `unclear` rule, an `unreadable
  input` verdict outside the precision denominator (F12-R.15), the reader's context named; the planner draws the read list
  when the step would draw it from the output it judges.
- **When rules move to a new series, read every stage's output that can fail there, per series** (F12-R.7): 15w read
  units of the *nouvelles* but no split lines, and the structural audit passed on a million wrong splits.
- **Selected volumes are not a sample:** UA-017's volumes were chosen as contradictions; do not turn their error rate
  into a corpus rate (F12-15w.5 → F12-R.5).
- **A brief that asks for both "reported only" and a pass/fail count** contradicts itself (15w audit); say which.
- **Validate a fitted model on held-out units, not on the sums it was fitted to** (OLS with an intercept reproduces its own
  sums; F12-R.18), and **check what a timing column covers before modelling it** (v4 extract `seconds` left out the pre-pass,
  F12-R.17). A brief that says a command does *not* read something must have checked it (15y: "the other sources are not read").
- **The session publishes AS layers in `shared-data/` itself** (user, 2026-10-27): copy/delete the layer data, edit
  `README.md`; whether to publish or replace stays a Decide UA. Before a delete: look, check write access and sibling links,
  `rm -r` the exact path; after a copy: `chmod -R g+w`, `diff -rq`, changelog line. Only AS's own layers.
