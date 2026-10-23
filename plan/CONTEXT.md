# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-22, phase-09 review (Opus).

## 1. Text layer v2/v3: what exists now (after phase 09)

- **CLI:** `.venv/bin/as-build-text-layer --preset sg_1.01.02_v3|sg_1.10.94_v3 --pagexml-dir D
  --out-dir O --stage all` (extract → layout → types → units → wbc → join; each stage also runs alone;
  `--*-params` JSON is merged over the preset). **Print needs `--republic-dir`** (no default since 15k).
- **Schema:** `docs/text_layer.md` is the contract (incl. *Text layer v3*); LotA writes its reader (D18).
- **Presets** (`text_layer/presets.py`):
  - `SG_HAND_V2`: layout `merge=False`; `wbc_pinned` `-=„` default, `-:=„` for 11281, `-„` for
    3349–3351, 3353 and 3354;
  - `SG_PRINT_V2`: `merged_line_split="on"`, `column_override=True`, fitted `unit_params`
    (`indent_frac=0.5, short_frac=0.1, gap_factor=None`), `wbc_pinned` `-=„`;
  - phase-09 additions are in §2; `SG_*_V3` (`sg_*_v3`) = v2 renamed `*_text_layer_v3`;
  - `VOC_V1`: byte-identical to NLP's v1 (`TOTAL 157 / 157`, command in step 15j's brief).
- **Layers:**
  - dev layers: `data/text_layer_v2_dev/<step>-<letter>/`, one per run, never overwritten. Sources:
    `15i-src/` (symlinks: 6 tar and 8 local hand inventories); print reads the `shared-data` zips;
  - the published pilot `shared-data/*/text_layer_v2/` = `15i-b` (v0.11.0 code, `coverage: partial`),
    with the F07-R.1–R.4 *Known issues* (UA-013 (a));
  - `15m-a/{hand,print}/` (v3 presets) is the current code's output (= `15l-a`; dev only). Compare
    new work against it; `15ir-a` is the phase-09 start;
  - never rebuild into a published dir. A new version gets a new name (D17); the full SG run is
    `text_layer_v3`. The user runs it from an external drive with `scripts/run_text_layer_v3_full.py` (step 15n,
    UA-015); sessions never touch the drive and only get the log's summary lines.
- **Audit tooling:** `reports/data/phase-09-text-layer-v3-prep/step-15m/{audit,rows116}.py --layer <base>`
  (glob `*_text_layer_v*`; `rows116.py` needs `rows116_verdicts.tsv` beside it). Metrics:
  `reports/data/phase-09-text-layer-v3-prep/planning/` (`order`, `pn_offchain`, `merges`, `crosswalk`,
  `compare`, `protect`; README there).
- **pagexml-tools:** still 0.9.0 (UA-007 waits for 0.9.1). Its traps and their workarounds are in
  F07-P.7, F07-15fp.11 and F07-P.9.

## 2. Accepted deviations (phases 07 and 09): later briefs must assume these

**Layout** (`analysis/page_structure.py`, `text_layer/layout.py`):
- Line order (15j, P4): merges and column-split pieces sort by printed row (`line_sort="row"`, rows
  within 0.2 × median box h, then x); print `unchanged` regions too (`sort_unchanged_lines`, flag
  `lines_resorted`, ids kept, P2); hand keeps XML order. `LEGACY_15FP` keeps the baseline key.
  26 same-row swaps remain (header rows, numeric cells; F09-15j.4).
- The gutter rule is `auto`: `gap` when ≥ 95% of openings have a gap (3354, 3771–3823, 455, 456),
  else `main_edge` (D6).
- **Hand runs without region merges** (`SG_HAND_V2` `merge=False`, F07-15fq.3). Hand layout regions
  are therefore the legacy regions, split only at the gutter.
- Print regions are split on the line `column` field. The column merge guard is on by default: the
  default `LayoutParams()` changed in 15fq2.
- Furniture: `page_text_bottom` is the lowest line bottom of the page. `metadata["column_split_from"]`
  is a list. `LEGACY_15FP` reproduces 15fp.
- The blocked counters count pairs of final regions only. The interleave guard is per pair and is
  bridged in 55 print merges, on purpose (F09-P.1).

**Types** (`text_layer/types.py`):
- Segments, not lines, carry types. Only three rules split a line: `date_prefix`, `margin_prefix`
  and `pn_suffix`.
- `margin_prefix` cuts at the space nearest the main left edge, within 0.04·`page_w`, and **only
  month-only prefixes** (`margin_cut="month_only"`, D21).
- The main left edge and centre are per main column (`col_min_char_frac` 0.25), keyed by the print
  line `column` (`column_override`).
- Whole date, page-number or year lines skip the margin rule. A year in the top band is
  `running_header`. `maert` is in the month list.
- `page_w` is the scan width, not the page width.
- 15ir: a whole date/pn/year line is never split by rule 1. `rule:pn_line` is kept only on the
  in-chain candidate line; other pn-shaped band lines are `rule:pn_line_offchain`. 15k (P6): one
  between its neighbours' chain numbers is `rule:pn_line_gap` (`page_number`, 188); off-chain years
  are `other`/`rule:year_offchain` (37); print bottom-band off-chain is `other` (367); hand bands and
  print top stay `page_number` (bars missed; F09-15k.1, F09-R.5). Read `type_method` to count pages.

**Units** (`text_layer/units.py`):
- Hand units are layout regions minus furniture, cut at headers. Print units are paragraphs (any
  cue) within a column piece.
- The R5 rule (D13): the unit with the most main text inherits the layout region id. Every legacy
  and layout id of a scan is reserved, and collisions get `w+1`. Since 15ir a unit holding all lines of
  a legacy region whose box it shares takes that id (`claimable`): collisions 0 everywhere in `15ir-a`.
- `doc_id_map` covers every legacy region; empty ones get `unit_id=""` and `link=none`.
- Crosswalk classes: `exact`, `near` (5 px), `split_original` (first half; last half for role `end`,
  15k), `split_half` (IoU ≥ 0.5) and `unmatched`; column `role`. No REPUBLIC default path:
  `--republic-dir` or no crosswalk (`republic_dir: null`). `claimable` is built per scan.

**Word breaks and join** (`word_breaks.py`, `join_v2.py`, `build.py`):
- The `„`-robust statistic exists (`word_break_stat="robust"`) but is not adopted (F07-15hj.2).
- SG sets are pinned (`wbc_pinned`, D22, D23). `sanity_pass` is computed on the pinned default.
  `pooled_rule_sanity_pass` records the unpinned rule, which fails for hand_local and print.
- `wbc` runs per layer root: hand_tar, hand_local and print each get their own corpus default.
  15i built one hand root over all 14 inventories.
- `join_unit` equals `join_region` with `lead_marks=""`. Offsets are exact (`text[cs:ce] ==
  line.text[line_offset:…]`), and `n_offset_violations` checks the full span. A merge can drop two
  trailing break chars (51 rows).
- Manifests keep the history of each stage. The subset guard on `wbc` is on.
- Lead marks (15ir, 15l): hand `join_lead_marks=","` drops one or two marks plus an optional space
  before a letter; `join_lead_marks_lower` (per inventory) drops `J` before a lower-case letter in 455
  only. `join_upper_guard` (`g_upper_right`, v2 only) exists but is off: negative result, F09-15l.4.
  `join.py`/`ALL_GUARDS` are v1's and unchanged.

## 3. Data facts that shape SG work

- Hand 1.01.02 and print 456 have no region labels. 455 has Laypa labels, but most typed regions
  are empty (F07-P.1, F07-P.2). 455 also has 130,737 empty legacy regions.
- Print regions are page-level blocks that interleave both columns (F07-15fp.3).
- The HTR merges a page's date header, first text line and page number into one TextLine
  (F07-P.10).
- Hand line-end words are often a separate region at the line's row (F07-15fq.9).
- Break marks:
  - the hand scribes use `-`, `=` and `„`, and `:` in 11281;
  - `=` is often a compound marker (F07-15hj2.3, F07-15i2.3);
  - `.` is a word break only about half the time (F07-15hj2.2);
  - a repeated mark at the line start is read as `,` (hand, F07-R.2), as `J` in print 455 only
    (F09-P.4), and as a capital ("Secreta|Pris") in 10–16 of 40 upper-case merges (F09-15l.5).
- Upper-case merges: hand 4,222, print 4,447 in `15m-a`; about half are wrong (bar for a guard missed).
- The 116-row acceptance: predicted share free 0.526, bound 0.560 (static verdicts); rows 23 and 188
  now read as fixed (0.543 / 0.569, F09-R.3). Glue repair is off (UA-003). LotA checks after
  re-alignment (D19).

## 4. Open follow-ups for the next phase

From phase 09 (reviewed 2026-10-22; report *Follow-ups*):
- The full `text_layer_v3` run (the user runs it, UA-015) and its publication (the layer may not fit locally). Print
  inventories other than 455/456 have no REPUBLIC paragraphs. Fit an SG throughput model from the run's `status.tsv`.
- A full-run audit: the step-15m audit has fixed pilot lists and reads `15i-src` (F09-15n.3).
- Hand-bottom off-chain numbers: crop verdicts for the 8 `unclear` sample rows, then the bar (F09-R.5).
- `_pn_candidate`: pick the band number that fits the chain (the 188 gap rows lost the slot).
- A mark rule for capitals (F09-15l.5), then re-test `g_upper_right` with the same 27/40 bar.
- pagexml-tools 0.9.1 pin and the 0.9.0 workarounds (UA-007; not on PyPI on 2026-10-22).

From phase 08 (reviewed 2026-10-21; hand-off in its `PLAN.md`):
- LotA decides whether to filter duplicates on `adjacent` (0.95 re-shoots; the 9 `deelopname`
  re-shoots are never adjacent) and whether to drop numerals from its lexicon (F08-R.1).
- Phase-10 candidates: re-shoot recall below J 0.6 (F08-R.2); a classifier run under
  `page_duplication` (F08-17b.2).

From the siblings (recorded 2026-10-21):
- NLP: the `text_extent` options are a phase-10 step (UA-014 (a); `plan/PLAN.md` row 10).
- republic `docs/from-republic.md`:
  - §8 items 2–3 are still open: the `ground_truth/README.md` counts are stale (726,737
    resolutions, 121,124 sessions), and no SG `session` converter exists;
  - §10: `session_metadata` start lines miss mid-session meeting days (2,749 candidates; 95
    confirmed). An SG `session` layer built from it is `partial` for meeting days.

## 5. Planning notes: recurring brief defects

- **Protected hashes.** Name the method: `reports/data/phase-09-text-layer-v3-prep/planning/protect.py
  OUT --skip <own prefixes>` (`size=N mtime path`, plus sha256 of the source archives).
- **Measure every expected number on the layer the step starts from,** not on an earlier one.
  Phase 07 got this wrong for 15g2's change count, 15h's "0 collisions" and 15i2's "3351/3354
  unchanged". When a step combines fixes, measure each number with all of them applied, or say it is
  for one fix alone (15ir: 13 collisions were 0 with fix 1; 11 date lines were 19 over all inventories).
- **The changelog** has no `## Unreleased` heading right after a release; the first step adds it.
- **Name the layer root per sub-layer** (hand_tar, hand_local, print), or the single pilot root.
  `wbc` decisions depend on it.
- **When a step re-runs a downstream stage, say which later briefs name its layer,** and update
  them (rule 10). Phase 07 inserted four follow-up steps mid-phase.
- **Ports carry their rationale:** list the comments to keep (calibration notes, decision IDs;
  F08-R.4), and which block of a copied planning script to drop.
- **The agent shell:** no `rm -rf`, `nohup` or `&`, a new dir per run, and `chmod g+w`. Sibling
  suites run with their own `venv/bin/python`. NLP and LotA import AS from the working tree. The
  shell is zsh: write the `coreshare.py run …` prefix out, never in a variable (phase 09, 15j).
- **Run every command a brief quotes once at planning, as written** (phase 09: `order.py` takes
  `--out`, `rows116.py` needs its verdicts file beside it, UA-015's copy command was wrong).
- **Say whether a rule reads the line or the segment text** (15k: "1718" of "Maart 1718").
- **Hand-read bars work** (phase 09: five bars, all fixed before reading, one verdict file each);
  give the `unclear` rule and say what context the reader has (text alone left 8 rows undecidable).
