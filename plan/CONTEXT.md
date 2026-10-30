# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-30, review of phase 15 (Opus); trimmed to budget.

## 1. Text layer v2–v5 and index links: what exists now (after phase 15)

- **CLI:** `.venv/bin/as-build-text-layer --preset <key> --pagexml-dir D --out-dir O --stage all` (extract → layout →
  types → units → wbc → join; each stage alone too; `--*-params` JSON merged over the preset). 1.10.94 needs
  `--republic-dir` (crosswalk); 1.01.02 gets none (F12-15v.5). **Schema:** `docs/text_layer.md` is the contract (v3–v5
  sections); LotA writes its reader (D18).
- **Presets** (`text_layer/presets.py`): `SG_HAND_V2` (`merge=False`; `wbc_pinned` `-=„`, 11281 `-:=„`, 3349–3351/3353/3354
  `-„`); `SG_PRINT_V2` (`merged_line_split="on"`, `column_override=True`, `SG_PRINT_UNIT_PARAMS`); `SG_*_V3` = v2 renamed;
  `VOC_V1` = NLP's v1. **v4** (phase 12): `SG_1_01_02_V4`/`SG_1_10_94_V4` with `mode_presets` (`hand`/`print` → the v3
  presets) and `volume_mode` (`box_height`, cut 87.75); `inventory_preset(preset, mode)` copies `MODE_RULE_FIELDS`, then
  `mode_overrides[mode]`; 1.01.02 only: `split_gate` (≥ 0.35), print `drop_cap_lines=2`, `MODE_PINS_V4`. **v5** (phase 13):
  `SG_1_01_02_V5` = v4 + hand `index_sides` `SG_INDEX_SIDES_V5` (side rule + gap fill 8 → `layout/pages/`) and units
  `index_entry` `SG_INDEX_ENTRY_V5B` (`hand:index_start`, `hand:index:<cues>`). Not in any preset (bars failed):
  `hand_column_split` (`pieces`, `reorder`), E5. New fields are left out of `to_dict` when `None`.
- **Build** (`text_layer/modes.py`): pre-pass → `decide_mode` → `mode/<inv>.json` → extract with the mode's rules;
  stages skip current inventories and broken chains. Run kit `scripts/run_text_layer_v4_full.py` (`--src`, `--rebuild
  ARCHIVE=F`, coreshare, `--version {v4,v5}`); `scripts/stage_text_layer_v4_full.py`.
- **Layers:**
  - dev (`data/text_layer_v2_dev/`, one dir per run, protected): **`15y-b` = v4 dev reference** (196 volumes), `15z-b`
    (37 read volumes after pins); **`19e-a` = v5 dev build** (231: 125 hand, 106 print); `20a-pin` = `19e-a` rebuilt on
    pagexml-tools 0.9.1 (byte-identical); clones `19b-*`–`19g-*`;
  - full v4 and v5 runs on the user's drive only (v5: UA-045, 9,122 volumes, audit 0 judged); staged review files
    `data/text_layer_v4_full/`, `data/text_layer_v5_full/`;
  - published, never rebuilt (D17): `text_layer_v2` (= `15i-b`), `text_layer_v3` pilot (= `15n-a`), `text_layer_v4`
    pilot (= `15y-b`), `text_layer_v5` pilot (= `19e-a`, UA-046 (a)).
- **Audit:** `reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer <base> --out F` (read
  `n_fail_judged`); comparer `phase-12-…/step-15v/compare.py`; phase-09 `planning/` metrics (`compare`, `protect`).
- **pagexml-tools 0.9.1** pinned (`>=0.9.1`, `MIN_PAGEXML_TOOLS` (0, 9, 1), 20a); both `assign_column_roles` workarounds
  gone; nested regions parse; a region without Coords or lines is still dropped. Manifests built before 20r record
  archival-structures 0.14.1 (stale `.venv` metadata, F14-R.7; `git_head` is right); `poetry.lock` is stale (phase-01
  precondition). `analysis/hand_print.py`: box-height API; caches `data/hand_print_geom/15q-a/`, `15u-a/` (protected).
- **Index links (phases 14–15):** `analysis/folio_map.py` (`build_folio_map(…, params)` → `FolioMap.locate(N)` →
  `FolioTarget(…, confidence, opening)`; `FOLIO_MAP_V2` = G1 jump cut, G2 tied numbers, G3 bracket confidence, G4
  opening; default params = v1); `analysis/index_links.py` (`front_index_run`, `read_volume_inputs`, `extract_locators`
  L1–L7 in `SG_LOCATOR_PARAMS`, `volume_links` (v1); `read_side_lines`, `side_reading_order`, `assemble_entries` A1–A3,
  `entry_locators`; `volume_links_v2` + `V2_CONFIG` = A0 + L7 + `FOLIO_MAP_V2`); builder `scripts/build_index_links.py
  --version {v1,v2}`; `docs/index_links.md`. v1: dev `data/index_links_dev/20r-a/`, full run `data/index_links_full/`
  (412 volumes, 902,031 links); v2: dev `21d-a/` (41 volumes), full run UA-056. Neither published (UA-053 a, UA-057).
  **Spent gold:** 20c, and 21a's side-level gold of 29 volumes; untouched: the 371 drive-only front-index volumes.
  REPUBLIC via `data/republic_segmentation_v1`.

## 2. Accepted deviations (phases 07, 09, 12–15): later briefs must assume these

**Layout:** merges and column-split pieces sort by printed row (`line_sort="row"`), print `unchanged` regions too; **hand
keeps XML order**, so two-column hand regions interleave (F09-15j.4). Gutter rule `auto` (D6). Hand runs without region
merges (F07-15fq.3); print regions split on the line `column` field; column merge guard on (15fq2; bridged in 55 print
merges on purpose, F09-P.1). **Types:** only `date_prefix`, `margin_prefix` (D21) and `pn_suffix` split a line; whole
date/pn/year lines are never split; `rule:pn_line` in-chain only; read `type_method` to count pages. **Units:** hand
units = layout regions minus furniture, cut at headers; print units = paragraphs within a column piece; R5 ids (D13);
`doc_id_map` covers every legacy region; no REPUBLIC default path. **Word breaks and join:** SG sets pinned (D22, D23);
`wbc` per layer root; `join_unit` = `join_region` with `lead_marks=""`; offsets exact; `join_upper_guard` off (F09-15l.4).

**v4 (phase 12):** one mode per volume (UA-019 (b)); the audit keeps the lower-case merge share as `reported: true`,
outside `n_fail_judged`. Printed 1.01.02: 1.10.94 print rules, split gate (only *gedrukte resoluties* and 3463 split),
drop-cap rule in all 85 (merges some *nouvelles* parts, F12-R.13, F12-15x.3).

**v5 (phase 13):**
- No column fix: 722 interleaved hand units remain off index sides (F13-19e.5); split and reorder failed (F13-19b.2, F13-19f.2).
- On flagged sides `index`-typed lines are in `main_text` units while `types/segments` keeps `index`; a `date_header` unit
  followed by its entry counts as one entry; gap fill flags 41% of the flagged sides (mostly right, F13-R.14).
- Detector false positives accepted: rosters, accounts, letters re-cut at entry cues (54 known sides, 0.18% of hand units,
  no text lost; F13-R.13). Missed: one-column and garbled indexes (F13-19c.5), `marginalia`-typed entry starts (F13-R.12).

**Index links (phases 14–15):** phase 14 F 33/35, L P 61/66 (failed), E 33/35. Phase 15 on fresh gold: F 62/63 (v1
the same, F15-R.4), L P 0.981; L R 0.856, A 0.892, E 0.825 failed, no better than v1 (entries are still v1's
`main_text` units). A1–A3 not selected: heading attachment costs more than they gain (F15-21c.1/.7). Embedded entries
are gold-neutral for links (F15-R.2). 3712 maps as `pagination`; L7 drops its out-of-chain locators.

## 3. Data facts that shape SG work

- Hand 1.01.02 and print 456 have no region labels; 455's Laypa labels are mostly on empty regions (F07-P.1, F07-P.2).
  Print 1.10.94 regions interleave both columns (F07-15fp.3); the HTR merges date header, first line and page number
  into one TextLine (F07-P.10). Break marks `-`, `=`, `„` (`:` in 11281); `.` a break half the time (F09-15l.5);
  upper-case merges about half wrong (F12-15w.9).
- **Hand/print.** No PageXML attribute marks print (F10-P.2); char-weighted median line-box height: print 65–87 px, hand
  88–182 (cut 87.75, F12-15u.1); hand volumes below 100 px are **index** volumes (F12-R.5). **1.01.02 print series**
  (F12-R.4): *gedrukte resoluties* (3760–3864) two-column with dropped initials; *nouvelles*, *plakkaten* single-column.
- **Index pages (phase 13).** In the index volumes and the front section of each *tweede serie* half-year register. Two
  styles (F13-19a.3): page-reference indexes and dated summary indexes under a lemma/respect term. Lemma headings are
  typed `marginalia` (F13-R.5). Inventory GT `ground_truth/NL-HaNA_1.01.02/inventory_metadata-…json`: `index_page` in 485
  of 674 inventories; GT page ↔ scan: verso 2·scan − 2, recto 2·scan − 1.
- **Front indexes (phases 14–15).** 41 of 125 local hand volumes have one (P3 rule); all 29 sides read in 21a are
  two-column, 3.4% of lines merge both columns; the index is right for 82 of 89 fresh targets (F15-21a.4). The resolutions after it are foliated
  on rectos (21,432 of 21,437 anchors); numbering restarts after the index (offset +16 to +84 scans). The index is right
  for 50 of 52 gold targets; the matter is on folio N's verso more often than its recto (25 vs 16); printed numbers are
  garbled or missing for about a quarter of folios and drift 1–5 scans. Silver per link: entry word on the target 0.707
  vs 0.218 at N+3; off-by-one is mostly run-on (N+1) or the facing verso (N−1) (F14-R.4). 3712 is a dated index.
- **REPUBLIC segmentation v1** (`docs/from-republic-segmentation.md`): `shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/`
  (443 inv) and `…/NL-HaNA_1.10.94/…` (455, 456), Parquet by `inv`. Resolution pages only. **Scoring only.** Weak hand
  resolution starts (recall 0.558), ~750 spurious hand sessions; page coverage is near total, so a page-level check is at
  ceiling (F14-20e.6). `.venv` has pyarrow 26.0.0.

## 4. Open follow-ups for the next phase

**Status 2026-10-30:** phase 15 done and reviewed. Next: phase 16, VOC (1.04.02), what generalises (staging UA-054
answered (a)). Other candidate archives named by the user: 2.10.01, 2.10.50 (4 inventories local).

**From phase 15** (report *Follow-ups*): UA-055 (user check), UA-056 (v2 full run), UA-057 (publication, recommended
no). An index-links v3, each item with a bar on new gold (drive-only volumes or 29 more local sides): entry assembly as
v1 units plus the side's `marginalia` lines (53 of 67 test locator misses, F15-21c.6) or A1 with a heading rule needing a
trailing `.` and a following number-ended entry (F15-21c.7, F15-R.5); HTR-split folio numbers (F15-R.3); still open
from phase 14: 3681/3601, dated indexes, separate index volumes.

**From phase 13:** rule work, each with its own bar on fresh reads: a detector false-positive guard (roster end pairs,
amount-shaped line ends, isolated flagged sides; F13-R.13); column order v3 (row pairing, split x from the overlap
band; F13-R.10); `marginalia`-typed entry starts (F13-R.12); continuation cuts after amounts (F13-19g.4); one-column and
garbled indexes (F13-19c.5). Any of these ships as a new layer version (D17).

Older: a per-mode extract fit (F12-15yr.4); parallel layout/types/units (P29); hand-bottom off-chain page numbers
(F09-R.5); `_pn_candidate` should pick the chain's band number; a capitals mark rule, then `g_upper_right` (F09-15l.5);
LotA duplicate filter (F08-R.1); F08-R.2, F08-17b.2, F11-R.1, F11-R.2; `docs/from-republic.md` §8, §10; the print
REPUBLIC crosswalk from `republic_segmentation_v1` paragraph line ids (455/456).

## 5. Planning notes: recurring brief defects

- **Protected hashes.** `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip <own prefixes>` before
  and after; `shared-data/` changes by other projects are expected and named. A step without a pair: the review diffs
  against the last after-list (phase 14).
- **Expected numbers:** measure each on the layer the step starts from, under the step's exact definition and the
  library's rule (20d's LOO for 3712 was measured recto-only); for a failed ratio give numerator and denominator before
  a cause (F10-R.1). One output prefix per subcommand.
- **Changelog:** the first step after a release adds `## Unreleased`. **Layer roots:** one per sub-layer.
- **Ports carry their rationale** (F08-R.4). **Say whether a rule reads the line or the segment text** (15k).
- **The agent shell:** no `rm -rf`, `nohup` or `&`; a new dir per run; `chmod g+w`; zsh (quote `'{workers}'`; `echo ==`
  fails); never `git stash`/`checkout`/`reset`; no `sed -i` (BSD needs `''`): edit with Python or Edit, and a step that
  sets a default reads it back before a test run (20e's test ran first on a placeholder). Sibling suites with their own
  interpreter: LotA `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider` (380 passed, 1 skipped,
  1 xfailed), NLP the same with `tests` (66 passed).
- **Run every command a brief quotes once at planning**; macOS `zcat` fails on `.gz`. A brief that says a command does
  *not* read something must have checked it (15y). A run kit's final audit must know the new layer's checks (F13-R.16);
  a UA that gives commands lists every directory they need.
- **Read shared data through a link under `data/`** and add AS to the catalogue row's *used by* (shared-data §3).
- **Hand-read bars:** fixed before reading, one verdict file, an `unclear` rule, `unreadable input` outside the
  denominator (F12-R.15), the reader's context named; size a draw on the expected yield of usable rows (20c: 28 of 40
  had a target) and say up front whether reserves may be read as extra gold; hard cases defined in the rubric. **Gold
  is spent once a variant is scored on it**: later tests need fresh reads, labelled and hashed before any code runs
  (F13-R.9). A user check page states the label definition (F13-R.15).
- **A brief that operationalises a plan decision** must match its text (P9 said `N[.,:;]` then a capital; the brief also
  took a bare space, F14-R.2), and a brief header must not assume an earlier bar passed.
- **New scripts under `scripts/`** need a `!/scripts/<name>` line in `.gitignore` (F14-R.6).
- **Side-level gold** says where text without a line of its own goes (F15-R.2); a mid-step draw covers only the
  split already read; page-number tools also see line-end numbers. **Parallel Sonnet steps** (phase 15): Edit only your
  own `STATUS.md` row; wait on a sentinel file, not a STATUS phrase; gold sessions are told which dirs not to open.
- **An error category that names a layout cause is checked against the layout tables** (F13-R.1). **When rules move to
  a new series, read every stage's output that can fail there** (F12-R.7). **Selected volumes are not a sample**
  (F12-R.5). "Reported only" and a pass/fail count in one brief contradict each other.
- **Validate a fitted model on held-out units** (F12-R.18); check what a timing column covers first (F12-R.17).
- **A blocking UA:** the brief says what a step does while it is open (wait, or a diagnostic that spends no held-out gold).
- **The session publishes AS layers in `shared-data/` itself** (user, 2026-10-27): copy/delete the layer data, edit
  `README.md`; whether to publish stays a Decide UA. Before a delete: look, check write access and sibling links, `rm -r`
  the exact path; after a copy: `chmod -R g+w`, `diff -rq`, changelog line. Only AS's own layers.
