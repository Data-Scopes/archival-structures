# AS planning context

> **Audience:** Opus phase-planning and phase-review sessions. Read it after `plan/PLAN.md`.
> Implementation sessions do not read it: a brief copies in the facts it needs from here. Each phase
> review updates it, so briefs start from the code as it is, not as it was planned. Keep it under
> ~150 lines. History belongs in the phase `PROGRESS.md` files.
> **Last updated:** 2026-10-29, review of phase 14 (Opus); trimmed to budget.

## 1. Text layer v2–v5 and index links: what exists now (after phase 14)

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
- **Index links (phase 14):** `analysis/folio_map.py` (`build_folio_map(scan_ids, sides, numbers, *, exclude, params)` →
  `FolioMap` (`mode` foliation/pagination/none; `locate(N)` → `FolioTarget(number, front, back, position, method,
  anchor_dist, confidence)`; `table()`)); `analysis/index_links.py` (`front_index_run`, `read_volume_inputs` from a v5
  layer, `extract_locators` L1–L4 with `SG_LOCATOR_PARAMS`, `DEFAULT_LOCATOR_VARIANT = "L2"`, `volume_links`); builder
  `scripts/build_index_links.py` (resumable; 20r adds failed-volume records and `progress.log`); schema
  `docs/index_links.md`. Layer `NL-HaNA_1.01.02_index_links_v1`: dev build `data/index_links_dev/20e-a/` (41 volumes,
  101,282 links); full run (UA-052) on the drive, its `volumes.tsv`/`manifest.json` in `data/index_links_full/` (412
  front-index volumes of 8,856, 629,912 entries, 902,031 links); **not published** (UA-053 a). The 20c gold
  (`step-20c/entry_gold.tsv`) is **spent** for map and locator variants (user-checked: F14-20c.8); 29 of the 41 local
  front-index volumes are untouched by any gold, and 371 front-index volumes are outside the local dev layer. REPUBLIC via `data/republic_segmentation_v1`.

## 2. Accepted deviations (phases 07, 09, 12–14): later briefs must assume these

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

**Index links (phase 14):** bar F failed (33/35), bar L failed on precision (L2 61/66), bar E passed (33/35); v1 ships
as built. L2 is the default (dev tie rule); L3 is not adopted (its test numbers come from a mistaken run, F14-R.2). An
entry is a `main_text` unit on a front-index side, so first-line fragments carry no locator (about 1 in 9 entries). The
map is a leaf model (recto of S + verso of S+1); 3712 (dated index, verso years) maps as `pagination`; 6 volumes end
their chain on a year-like anchor and link years `high` (F14-R.3). 20c's test gold = 40 read + 8 reserves (F14-R.1).

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
- **Front indexes (phase 14).** 41 of 125 local hand volumes have one (P3 rule). The resolutions after it are foliated
  on rectos (21,432 of 21,437 anchors); numbering restarts after the index (offset +16 to +84 scans). The index is right
  for 50 of 52 gold targets; the matter is on folio N's verso more often than its recto (25 vs 16); printed numbers are
  garbled or missing for about a quarter of folios and drift 1–5 scans. Silver per link: entry word on the target 0.707
  vs 0.218 at N+3; off-by-one is mostly run-on (N+1) or the facing verso (N−1) (F14-R.4). 3712 is a dated index.
- **REPUBLIC segmentation v1** (`docs/from-republic-segmentation.md`): `shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/`
  (443 inv) and `…/NL-HaNA_1.10.94/…` (455, 456), Parquet by `inv`. Resolution pages only. **Scoring only.** Weak hand
  resolution starts (recall 0.558), ~750 spurious hand sessions; page coverage is near total, so a page-level check is at
  ceiling (F14-20e.6). `.venv` has pyarrow 26.0.0.

## 4. Open follow-ups for the next phase

**From phase 14** (report *Follow-ups*): 20r, the user check (UA-050), the full run (UA-052) are done; v1 is not
published (UA-053). An **index-links v2** phase, every rule with a pre-registered bar on fresh gold drawn from the 29
untouched local front-index volumes (and the 371 on the drive only): map v2 (drop anchors above the scan count and
verso years, 22 of 412 full-layer volumes by estimate; duplicate-number tie rule; a confidence that sees the number gap;
F14-R.3, F14-20d.3/.4, F14-20e.10); locator v2 (P9's punctuation-ended sub-entries, left word boundaries for
`amount_before` and glued digits, which the user check of `Cunes94` supports, dated-index handling; F14-R.2, F14-R.5,
F14-20c.8); indexes citing folios outside the chain (3681, 3601; F14-20e.9); entry assembly (first-line fragments,
column-merged units; F14-20c.1/.7); an `opening` target (F14-R.4). Out of P1 scope: separate index volumes (EAD volume
map), dated summary indexes, unit-level targets.

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
- **An error category that names a layout cause is checked against the layout tables** (F13-R.1). **When rules move to
  a new series, read every stage's output that can fail there** (F12-R.7). **Selected volumes are not a sample**
  (F12-R.5). "Reported only" and a pass/fail count in one brief contradict each other.
- **Validate a fitted model on held-out units** (F12-R.18); check what a timing column covers first (F12-R.17).
- **A blocking UA:** the brief says what a step does while it is open (wait, or a diagnostic that spends no held-out gold).
- **The session publishes AS layers in `shared-data/` itself** (user, 2026-10-27): copy/delete the layer data, edit
  `README.md`; whether to publish stays a Decide UA. Before a delete: look, check write access and sibling links, `rm -r`
  the exact path; after a copy: `chmod -R g+w`, `diff -rq`, changelog line. Only AS's own layers.
