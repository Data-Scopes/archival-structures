# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-28, final review of phase 13 (Opus); trimmed to budget.

## 1. Text layer v2–v5: what exists now (after phase 13)

- **CLI:** `.venv/bin/as-build-text-layer --preset <key> --pagexml-dir D --out-dir O --stage all` (extract → layout →
  types → units → wbc → join; each stage alone too; `--*-params` JSON merged over the preset). 1.10.94 needs
  `--republic-dir` (crosswalk); 1.01.02 gets none (F12-15v.5).
- **Schema:** `docs/text_layer.md` is the contract (incl. *Text layer v3*, *v4*, *v5*); LotA writes its reader (D18).
- **Presets** (`text_layer/presets.py`):
  - `SG_HAND_V2`: layout `merge=False`; `wbc_pinned` `-=„` (11281 `-:=„`; 3349–3351, 3353, 3354 `-„`). `SG_PRINT_V2`:
    `merged_line_split="on"`, `column_override=True`, `SG_PRINT_UNIT_PARAMS` (`indent_frac=0.5, short_frac=0.1`, gap off),
    `wbc_pinned` `-=„`. `SG_*_V3` = v2 renamed. `VOC_V1` = NLP's v1 byte for byte;
  - **v4** (phase 12): `SG_1_01_02_V4`/`SG_1_10_94_V4` carry `mode_presets` (`hand` → `sg_1.01.02_v3`, `print` →
    `sg_1.10.94_v3`) and `volume_mode` (`box_height`, cut 87.75; 1.10.94 `fixed`/`print`). `inventory_preset(preset,
    mode)` copies the six `MODE_RULE_FIELDS`, then `mode_overrides[mode]` (whole fields). 1.01.02 only: `split_gate`
    (column-start share ≥ 0.35), print `drop_cap_lines=2`, `pins` `MODE_PINS_V4` (14 index volumes → `hand`, 12163/12165 → `print`);
  - **v5** (phase 13, v0.18.0): `SG_1_01_02_V5` = v4 + `mode_overrides["hand"]`: layout `index_sides` `SG_INDEX_SIDES_V5`
    (side rule `col_share ≥ 0.25` and `max(end_num, date_start) ≥ 0.15`, gap fill 8 → `layout/pages/`, hand only) and
    units `index_entry` `SG_INDEX_ENTRY_V5B` (flagged sides: cues outdent, date_start, end_ref_prev, short_prev; no-cut
    guard; `index`-typed lines join the mains; `hand:index_start`, `hand:index:<cues>`). In no preset (bars failed):
    `hand_column_split` (`pieces` 19b, `reorder` 19f), E5. New fields are left out of `to_dict` when `None`.
- **Build** (`text_layer/modes.py`): pre-pass → `decide_mode` → `mode/<inv>.json` → extract with the mode's rules;
  `*/params.json` = `{"hand": …, "print": …}`; stages skip current inventories (`build.is_current`) and broken chains
  (`upstream_ok`). Run kit `scripts/run_text_layer_v4_full.py` (`--src`, `--rebuild ARCHIVE=F`, `THROUGHPUT_V4`, calls
  coreshare; `--version {v4,v5}`, `ARCHIVES_BY_VERSION`, v5 = 1.01.02 only); `scripts/stage_text_layer_v4_full.py`.
- **Layers:**
  - dev (`data/text_layer_v2_dev/`, one dir per run, protected): **`15y-b` = v4 dev reference** (196 volumes), `15z-b`
    (37 read volumes after pins; wins for them); **`19e-a` = v5 dev build** (231: 125 hand, 106 print; inputs
    `data/text_layer_v5_19e_{src15z,inputs}`); clones `19b-*`–`19g-*`. Tars: `data/text_layer_v4_staged{,_15z}/`;
  - full v4 run on the user's drive only (39 GiB; 266 `print`, 8,856 `hand`); staged `data/text_layer_v4_full/` (status,
    `mode/`, `scans/`, `wbc/`, `audit.json`, 22 volumes + 24 stale split files). Full v5: UA-045;
  - published, never rebuilt (D17): `text_layer_v2` (= `15i-b`), `text_layer_v3` pilot (= `15n-a`), `text_layer_v4`
    pilot (= `15y-b`, 196 + 2 volumes). A v5 pilot (= `19e-a`) is UA-046.
- **Audit:** `reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer <base> --out F` (15w audit + 4 v5
  checks on a `*_v5` layer; `--self-test`; read `n_fail_judged`); the run kit calls it. Comparer `step-15v/compare.py`;
  phase-09 `planning/` metrics (`compare`, `protect`); interleave metric `phase-13-…/step-19f/interleave.py`.
- **`analysis/hand_print.py`:** box-height API; caches `data/hand_print_geom/15q-a/`, `15u-a/` (protected).
  **pagexml-tools** still 0.9.0 (UA-007 waits for 0.9.1); workarounds F07-P.7, F07-15fp.11, F07-P.9.

## 2. Accepted deviations (phases 07, 09, 12, 13): later briefs must assume these

**Layout:** merges and column-split pieces sort by printed row (`line_sort="row"`, rows within 0.2 × median box h, then x),
print `unchanged` regions too; **hand keeps XML order**, so two-column hand regions interleave (26 same-row swaps,
F09-15j.4). Gutter rule `auto` (`gap` when ≥ 95% of openings have one, else `main_edge`, D6). Hand runs without region
merges (F07-15fq.3); print regions split on the line `column` field; column merge guard on (15fq2); the interleave guard
is bridged in 55 print merges on purpose (F09-P.1).

**Types:** segments carry types; only `date_prefix`, `margin_prefix` (month-only, within 0.04·`page_w`, D21) and
`pn_suffix` split a line. Main left edge and centre per main column; `page_w` = scan width. Whole date/pn/year lines are
never split by rule 1; `rule:pn_line` in-chain only; off-chain classes per 15k. Read `type_method` to count pages.

**Units:** hand units = layout regions minus furniture, cut at headers; print units = paragraphs (`indent`, `short_prev`,
`header`) within a column piece. R5 ids (D13) with `claimable`: 0 collisions. `doc_id_map` covers every legacy region;
crosswalk classes `exact`, `near`, `split_original`, `split_half`, `unmatched`; no REPUBLIC default path.

**Word breaks and join:** `„`-robust statistic not adopted (F07-15hj.2); SG sets pinned (D22, D23); `wbc` per layer root;
`join_unit` = `join_region` with `lead_marks=""`; offsets exact; `join_upper_guard` off (F09-15l.4).

**v4 (phase 12):** one mode per volume (UA-019 (b)); scan-level box height is a diagnostic (F12-15u.4). The audit keeps the
lower-case merge share as `reported: true`, outside `n_fail_judged`. Printed 1.01.02: 1.10.94 print rules, split gate (only
*gedrukte resoluties* and 3463 split), drop-cap rule in all 85 (merges some *nouvelles* parts, F12-R.13, F12-15x.3).

**v5 (phase 13):**
- No column fix: 722 interleaved hand units remain off index sides (F13-19e.5); split and reorder failed (F13-19b.2, F13-19f.2).
- On flagged sides `index`-typed lines are in `main_text` units while `types/segments` keeps `index`; a `date_header` unit
  followed by its entry counts as one entry; gap fill flags 41% of the flagged sides (mostly right, F13-R.14).
- Detector false positives accepted: rosters, accounts, letters re-cut at entry cues (54 known sides, 0.18% of hand units,
  no text lost; F13-R.13). Missed: one-column and garbled indexes (F13-19c.5), `marginalia`-typed entry starts (F13-R.12).

## 3. Data facts that shape SG work

- Hand 1.01.02 and print 456 have no region labels; 455 has Laypa labels, mostly on empty regions (F07-P.1, F07-P.2).
  Print 1.10.94 regions are page-level blocks interleaving both columns (F07-15fp.3); the HTR merges date header, first
  line and page number into one TextLine (F07-P.10); hand line-end words are often a separate region (F07-15fq.9).
- Break marks `-`, `=`, `„` (`:` in 11281); `=` often a compound marker; `.` a break half the time (F09-15l.5).
  Upper-case merges: about half wrong; lower-case merge share < 0.95 in 75 of 198 volumes (F12-15w.9).
- **Hand/print.** No PageXML attribute marks print (F10-P.2). Raw char-weighted median line-box height separates them:
  print 65–87 px, hand 88–182 (cut 87.75, F12-15u.1); not per page width (F12-R.1). Hand volumes below 100 px are
  **index** volumes (F12-R.5). 455/456: 75.0 / 82.5.
- **1.01.02 print series** (F12-R.4): *gedrukte resoluties* (3760–3864) two-column with paragraph regions and dropped
  initials (F12-R.6); *nouvelles* and *plakkaten* single-column. REPUBLIC 1.01.02 line boxes: crosswalk matches 0.5% (F12-15v.5).
- **Index pages (phase 13).** In the index volumes and the front section of each *tweede serie* half-year register (scans
  ~2–60). Two styles (F13-19a.3): page-reference indexes (an entry ends in page numbers) and dated summary indexes under a
  lemma/respect term (an entry starts with a date, often one column). Lemma headings are typed `marginalia`; v4 types whole
  short-line pages `index` (F13-R.5). Rosters and accounts share the cues (F13-R.13). Inventory GT
  `ground_truth/NL-HaNA_1.01.02/inventory_metadata-…json`: `index_page` sections in 485 of 674 inventories; `respect_page`
  (59) is the short list of respect terms before an index; GT page ↔ scan: verso 2·scan − 2, recto 2·scan − 1.
- **REPUBLIC segmentation v1** (`docs/from-republic-segmentation.md`): `shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/`
  (443 inv) and `…/NL-HaNA_1.10.94/…` (455, 456), Parquet by `inv` (`lines`, `sessions`, `resolutions`, `paragraphs` with
  first/last line ids, `marginalia`, `legacy_id_map`). Resolution pages only, not the index volumes. **Scoring only.** Weak
  hand resolution starts (recall 0.558), ~750 spurious hand sessions. `.venv` has pyarrow 26.0.0 (UA-044).

## 4. Open follow-ups for the next phase

From phase 13 (done; v0.18.0 recommended; report *Follow-ups*): the user decides the full v5 run (UA-045) and a v5 pilot
(UA-046). Rule work, each with its own pre-registered bar on fresh reads (all phase-13 gold is spent): a detector
false-positive guard (repeated end pairs of rosters, amount-shaped line ends, isolated flagged sides; F13-R.13); column
order v3 (row pairing, split x from the overlap band; F13-R.10; the rule for the 722 interleaved units left);
`marginalia`-typed entry starts (F13-R.12); continuation cuts after amounts (F13-19g.4); one-column and garbled indexes
(F13-19c.5). Gold rubric: a locator is a page number or a date; structure decides when the HTR lost it (F13-R.15). Once
v5 is published, any of these ships as a new layer version (D17).

From phase 12: a per-mode extract fit (F12-15yr.4); 24 stale split files in `data/text_layer_v4_full`; parallel
layout/types/units (P29); mixed volumes 4016, 2600, 9264 stay as built (UA-036 (a)).

From phases 08, 09, 11: hand-bottom off-chain page numbers (F09-R.5); `_pn_candidate` should pick the chain's band
number; a mark rule for capitals, then `g_upper_right` (F09-15l.5); LotA duplicate filter (F08-R.1); F08-R.2, F08-17b.2,
F11-R.1, F11-R.2; the NLP shim step (UA-032: `unsupervised_segmentation.py`, `separator_sheets.py` → AS);
`docs/from-republic.md` §8, §10.

From republic-segmentation (2026-10-28): (1) candidate source for the print REPUBLIC crosswalk (`--republic-dir` reads
LotA-only `paragraphs-<inv>.tsv.gz`): `republic_segmentation_v1` paragraph first/last line ids for 455/456, after checking
they are `<scan_id>-line-x-y-w-h` ids; another segmentation (C07d.1) than the 2026-03 export, so crosswalk numbers change;
(2) silver negatives for the index detector: **done** in 19e (18 of 34,428 resolution-page sides flagged, F13-19e.4);
19e read it by absolute path, so the next step that uses it links it under `data/` and adds AS to the row's *used by*;
(3) its v2 retypes adopt AS-like types (`date_header`, `running_header`, `resumption`).

## 5. Planning notes: recurring brief defects

- **Protected hashes.** `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip <own prefixes>`, plus a
  `find … | xargs shasum -a 256` list for caches; `shared-data/` changes by other projects are expected and named.
- **Expected numbers:** measure each on the layer the step starts from (19e quoted v4's interleave for a v5 build), with
  all combined fixes, or say it is for one fix; quote at the frozen precision and under the step's exact definition
  (F10-R.2); for a failed ratio give numerator and denominator before a cause (F10-R.1). One output prefix per subcommand.
- **Changelog:** no `## Unreleased` heading right after a release; the first step adds it. **Layer roots:** one per
  sub-layer (`wbc` depends on it); a step that re-runs a downstream stage updates the later briefs naming its layer.
- **Ports carry their rationale** (F08-R.4). **Say whether a rule reads the line or the segment text** (15k).
- **The agent shell:** no `rm -rf`, `nohup` or `&`; a new dir per run; `chmod g+w`; zsh (quote `'{workers}'` before a
  redirect; `echo ==` fails); never `git stash`/`checkout`/`reset`. Sibling suites with their own interpreter (LotA:
  `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider`, 381 passed + 1 xfailed).
- **Run every command a brief quotes once at planning** (19e: `--check-only` needs an existing `--out-dir`); macOS `zcat`
  fails on `.gz`. A brief that says a command does *not* read something must have checked it (15y).
- **A run kit's final audit must know the new layer's checks** (19e: the v4 audit ran on v5, fixed in review, F13-R.16);
  a UA that gives commands lists every directory they need.
- **Read shared data through a link under `data/`** and add AS to the catalogue row's *used by* (shared-data §3).
- **Hand-read bars:** fixed before reading, one verdict file, an `unclear` rule, `unreadable input` outside the denominator
  (F12-R.15), the reader's context named; the planner draws the read list when the step would draw it from the output it
  judges; hard cases defined in the rubric. **Gold is spent once a variant is scored on it**: later tests need fresh
  reads, labelled and hashed before any code runs (F13-R.9). A user check page states the label definition (F13-R.15).
- **An error category that names a layout cause is checked against the layout tables** (F13-R.1).
- **When rules move to a new series, read every stage's output that can fail there** (F12-R.7). **Selected volumes are
  not a sample** (F12-R.5). "Reported only" and a pass/fail count in one brief contradict each other (15w audit).
- **Validate a fitted model on held-out units** (F12-R.18); check what a timing column covers first (F12-R.17).
- **A blocking UA:** the brief says what a step does while it is open (wait, or a diagnostic that spends no held-out gold).
- **The session publishes AS layers in `shared-data/` itself** (user, 2026-10-27): copy/delete the layer data, edit
  `README.md`; whether to publish stays a Decide UA. Before a delete: look, check write access and sibling links, `rm -r`
  the exact path; after a copy: `chmod -R g+w`, `diff -rq`, changelog line. Only AS's own layers.
