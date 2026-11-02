# Phase 17: VOC rules v1

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **State:** planned 2026-11-01 (Opus 5.5); seeded the same day after the phase-16 review (UA-060 (a)).

## Goal

Phase 16 ran the SG components on the VOC archive (NL-HaNA 1.04.02) unchanged. The text-layer build carried over.
The folio map, the index-side detector and index links failed their bars (F16-22e.1/.2). This phase writes VOC rules
for those three components and for the word breaks. Each is tested once, against a fixed bar, on gold from 24 fresh
VOC volumes that no earlier phase touched. The phase ends with:
- a VOC layer version with `¬` joined;
- a VOC folio-map option set, a one-column TOC detector and locator L9, each with a bar verdict;
- a frozen fresh gold set: folio anchors read by the user on the images, plus TOC pages, locators and link targets
  read by agents.

## Owns

- `archival_structures/text_layer/presets.py`: new preset `voc_1.04.02_v6` (23b); nothing else in `text_layer/`.
- `archival_structures/analysis/folio_map.py`: VOC options, off by default (23c).
- `archival_structures/analysis/index_pages.py`: TOC detector functions (23d).
- `archival_structures/analysis/index_links.py`:
  - `front_index_run(min_after_frac=)` (23d);
  - L9, the monotone filter and `VOC_LOCATOR_PARAMS["L9"]` (23e).
- Tests for these; `docs/text_layer.md`, `docs/index_links.md` (paragraphs); `reports/data/phase-17-voc-rules-v1/`.
- Drive: `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/{23a-a,23b-a}/`, with
  links `data/text_layer_voc_23a`, `data/text_layer_voc_23b` and `data/text_layer_voc_inputs_23a/`.

## Sources

Read at planning:
- the phase-16 report (*Summary*, *Findings*, steps 22a–22e, *Review*, *UA-059*, *Follow-ups*);
- the phase-16 `PLAN.md` decisions;
- `plan/CONTEXT.md` §1–§5;
- the GT README VOC rows;
- the shared-tools README (`checkpage`, `handprint`);
- NLP `CLAUDE.md` (evaluation protocol; starts are NLP's).
- Code survey (Explore, 2026-11-01): `folio_map.py` (201 lines), `index_links.py` (476), `index_pages.py` (70), the
  preset and wbc/join paths, the 22b–22d eval scripts. The suite at planning collects 1,424 tests.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 23a | Fresh gold: build 40 fresh volumes; the user reads folio crops (UA-061); agents read TOC pages, locators and link targets | nothing | layer built, 0 failures; agent gold frozen and hashed (`agent_gold.done`); folio page handed over; after UA-061, `folio_gold.done` | `step-23a-fresh-gold.md` |
| 23b | VOC break set with `¬`: preset `voc_1.04.02_v6`, rebuild of the 567 | nothing | 567 built, audit 0 judged; extract→units identical to 22a; every changed unit had a `¬`; hand-read `¬` joins ≥ 57/60 | `step-23b-voc-break-set.md` |
| 23c | VOC folio map (pick, ties, clean, pieces, extrapolation) | dev: nothing; test: 23a `folio_gold.done` | 48-cell dev grid on the GM anchors; bar F ≥ 0.95 (leaf) on the fresh anchors; McNemar against the 22b cell | `step-23c-voc-folio-map.md` |
| 23d | One-column TOC detector, ledger guard, front run scaled to volume length | dev: nothing; test: 23a `agent_gold.done` | 96-cell dev grid on the 22 GLOBALISE volumes; bar D (P ≥ 0.95, R ≥ 0.90, scan level) on the fresh labels | `step-23d-voc-toc-detector.md` |
| 23e | L9 locators and the monotone filter; links with the 23c map | 23c (selected cell), 23d (module), 23a `agent_gold.done` | 12-cell dev grid (GLOBALISE starts); bars L (P ≥ 0.95, R ≥ 0.90) and E ≥ 0.90 on the fresh link gold | `step-23e-voc-locators-l9.md` |

## Why this order

- Every bar is tested on the fresh gold of 23a, so 23a comes first. Its agent gold (TOC, locators, targets) does not
  wait for the user; the folio gold does.
- 23b is independent: the map, the detector and the locators read `scans/`, `lines/`, `layout/` and `types/`, which
  `¬` does not change; only `wbc/` and `join/` differ. So 23b runs in parallel with 23a.
- 23c and 23d own disjoint modules, and their dev stages need nothing from 23a. They may start in wave 1, and each
  test stage waits on its sentinel file.
- 23e edits `index_links.py` after 23d. It needs 23c's selected map for its grid, so it runs last.
- Waves: (23a ∥ 23b ∥ 23c-dev ∥ 23d-dev) → (23c-test after UA-061; 23d-test) → 23e.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope:** break set, folio map, TOC detector, L9; 5 steps. VOC unit order (F16-22a.7) goes to a later phase | all | User, 2026-11-01 |
| P2 | **All phase-16 gold is dev data now:** both GM splits (1,841 anchors), all 22 GLOBALISE volumes including 2775, and the UA-059 corrections (7 anchors, applied to a dev copy only; the frozen file is not edited) | 23c–23e | A test split is spent for testing, not for development. Phase-17 tests use only 23a's fresh gold |
| P3 | **Fresh volumes,** drawn at planning: 24 from the 4,394 OBP inventories that are on the drive and outside the 567 (seed 20261101; `planning/fresh_volumes.tsv`); ranks 25–40 are reserves. Reserve rule: if fewer than 12 of the 24 have a front register TOC, 23a reads reserves in rank order (TOC and link gold only) until 12 do or the reserves run out | 23a | No phase has read these volumes. The OBP gives silver for each. The planner read file names and sizes only |
| P4 | **Fresh layer:** all 40 are built with `voc_1.04.02_v5sg` unchanged | 23a, 23c–23e | Same rules as the dev layer. Map, detector and locator inputs are identical under v6 (P13 of phase 16; CONTEXT §1) |
| P5 | **Folio gold: the user reads ~240 page-top crops** (10 random scans per sample volume, seed 20261101; blind: no HTR text, no map). For each crop the user enters the number on the front sheet's top right (on openings, the right page) or picks `none` / `unreadable`. Optional fields: the left-page number (openings) and a number on a sheet beneath. One gold row per crop. An anchor is (inv, number, scan) for the main number and for the left number; numbers beneath are reported, never scored | 23a, 23c | User, 2026-11-01 (option: ~250 crops). HTR digit misreads are part of what the map gets wrong (F15-21a.9), so the number is read on the image. Expected yield about 150 anchors |
| P6 | **TOC, locator and target gold: Sonnet reader subagents in 23a**, from the HTR text. Readers are blind to detector flags (`layout/pages`) and maps. They label scans 1..max(60, last TOC scan + 10) per volume; sample 8 lines per register TOC (seeded); give the start folios each line names; find each folio's target scan by text. All of it is frozen and hashed before any phase-17 code reads the fresh volumes. The 23a session checks 30 labels itself | 23a, 23d, 23e | Phase-15 practice (side and link gold from agent reads). The label is semantic, not a digit |
| P7 | **Bars, fixed 2026-11-01 before any read:** F ≥ 0.95 (leaf = map front scan or the next one holds the gold scan; strict reported); D: scan-level P ≥ 0.95 and R ≥ 0.90 over the labelled scans; L: P ≥ 0.95 and R ≥ 0.90 on the (line, N) multiset of the sampled lines; E ≥ 0.90; `¬` joins: hand-read precision ≥ 57/60. Wilson intervals. F and D also get an exact two-sided McNemar test against the phase-16 configuration on the same items, reported as "better than phase 16" at p < 0.05; L and E report the phase-16 cell (C3) next to the selected one | 23b–23e | Phase 14–16 thresholds, kept so the numbers stay comparable. The paired test separates progress from a pass, since every planning prototype stays far below 0.95 |
| P8 | **Grids are fixed in the briefs.** Select on the max dev measure; on a tie (band in the brief), take fewer changes. `select.json` is written before `test_done.json`. Each test runs once. A failed bar is a negative result with no retuning | 23c–23e | Phase 13–16 practice |
| P9 | **Library changes are additive and default to today's behaviour.** Each is checked on SG 3444 (19e-a) as in phase 16: `locate(n)` equal for n = 0..1,499 under `FolioMapParams()` and `FOLIO_MAP_V2`; L1–L8 locators unchanged on the GLOBALISE TOC lines. A named VOC constant (`VOC_FOLIO_MAP_V1`, `VOC_TOC_SIDES_V1`) is added only when its bar passes | 23c–23e | Shared-library rule; P13 of phase 16 |
| P10 | **Break set = new preset version** `voc_1.04.02_v6` (name `NL-HaNA_1.04.02_text_layer_v6`) = v5sg with `wbc_pinned={"default": "-=„¬"}`, nothing else. Full rebuild into a new drive root; 22a is untouched (D17). The audit runs with a step copy that applies the v5 checks to `_v6` | 23b | Planning probe: 856 of 870 `¬` boundaries merge, and the repeated `„` on the next line is dropped by `both_ends_fix` (F17-P.2). The audit runs its v5 checks only on names ending `_v5` |
| P11 | **The TOC detector looks only at the first W scans** (W ∈ {45, 60}); mid-volume TOCs are out of scope. It lives in `analysis/index_pages.py` and reads lines; it is not a text-layer stage | 23d, 23e | GLOBALISE TOCs start at scans 5–15 and run up to 40 scans. The window lifts dev F1 from 0.32 to 0.62 (F17-P.5). The layer needs no rebuild |
| P12 | **L9 is a new variant;** L8 is left byte-equal. The monotone filter is a separate function, applied per TOC in reading order | 23e | 22d's numbers must stay reproducible |
| P13 | **Starts are for scoring only:** GLOBALISE starts (dev) and agent-read targets (test) are never fitted to and never become a start-detection feature | 23a, 23e | NLP owns start detection (NLP `CLAUDE.md`) |

## Hazards

- **Drive.** Read only `PageXML/Globalise/pagexml_v2.1/` and the AS layer roots. Write only under
  `claude-projects-external/archival-structures/data/text_layer_voc/`. A hook denial or `Permission denied` is final.
  If the drive is unmounted, the layer links dangle: stop.
- **Protected.** Run `planning/protect_voc.py` (the 22a layer, its inputs, the phase-16 gold, `fresh_volumes.tsv`,
  and later the 23a layer and gold) and the phase-09 `protect.py` (`--skip 23`) before and after each step that
  writes data.
- **Blindness.**
  - 23c–23e never open `step-23a/dumps/`.
  - They open `step-23a/gold/` only in their test stage.
  - 23a readers never open `layout/pages/` or run map or locator code.
  - The folio page shows no HTR text or predictions.
- **Gold row grain.**
  - `toc_gold.tsv`: up to 4 rows per scan; collapse with max.
  - `folio_gold.tsv`: one row per crop. The same number can appear on two crops (tied numbers); both rows are
    scored.
  - `toc_labels.tsv`: one row per scan.
  - `link_lines.tsv`: one row per sampled line.
  - `link_targets.tsv`: one row per (line, folio).
- **GM gold convention.** End anchors often sit on the scan after the numbered recto (single-page printed ends: 110
  at +1 against 257 at 0), and 33 single-page begin anchors sit 2 scans before (F17-P.4). No 23c rule may shift
  positions to fit the GM begin convention; the fresh gold is read where the number is written.
- **Scan ids and sorting.** `NL-HaNA_1.04.02_<inv>_<NNNN>`; inventory ids can carry a suffix (sort by (number, suffix)).
- **Agent shell.** No `rm -rf`, `nohup`, `&` or `$TMPDIR`; zsh (quote `'{workers}'`); BSD `sed -i ''`; never
  `git stash`/`checkout`/`reset`; a new output dir per run; coreshare for more than one worker; `chmod g+w`
  locally and on the drive.
- **Parallel sessions.** Edit only your own row of `STATUS.md`. Edit the changelog, the report and `PROGRESS.md`
  with Edit only. The changelog has no `## Unreleased` heading at planning: the first step to add an entry adds it.
- **Import cycle.** `index_pages.py` must not import `text_layer.presets`: `page_structure` imports
  `index_pages`. Copy the month alternation.

## Review checklist

- [ ] Every acceptance number re-run by the reviewer; the 23a gold hashes match `gold/SHA256SUMS`, and
      `select.json` precedes `test_done.json` in 23c–23e.
- [ ] AS suite green (`.venv/bin/python -m pytest -q -p no:cacheprovider`); no sibling code changed.
- [ ] SG defaults unchanged on 3444 (P9); L8 byte-equal (P12); 22a layer unchanged (`protect_voc.py` pairs).
- [ ] Error analysis read individual cases as each brief lists, in the report; `make reports` clean.
- [ ] Gold quality:
  - the 30-label self-check in 23a;
  - the user's `unreadable` share;
  - folio rows with a number beneath (layered sheets).
- [ ] Leakage: no rule written from fresh text, and fresh volumes are disjoint from the 567 (assert in 23a). VOC
      constants exist only for passed bars.
- [ ] Changelog entries for 23b–23e under `## Unreleased`, with "no sibling imports affected" stated.
- [ ] `Brief defects` answered: templates or briefs fixed.

## Hand-off

Reviewed 2026-11-01 (Opus 5.5; [REVIEW](REVIEW.md); accept with follow-ups). All five steps are done; 23c's test stage
(after UA-061) was reviewed in an addendum on 2026-11-02.

**Results.** No bar passed, and no VOC constant exists:
- `¬` joins 56/60 (reviewer 55/60);
- D: P 0.583, R 0.276; not better than PH16 (p 0.147);
- L: P 0.556, R 0.400;
- E: 0.529 (9/17), on only 25 gold folios;
- F: 79/109 = 0.725 on the user's image gold (22b's map 81/109 = 0.743; McNemar p 0.5); 18 of 30 misses are map-side.

**What the next phase gets:**
- additive library options, all off by default:
  - preset `voc_1.04.02_v6`;
  - folio-map `pick_by_chain`, `tie_keep`, `clean_tol`, `pieces`;
  - `toc_sides` with `TOC_SIDES_DEFAULTS`;
  - `front_index_run(min_after_frac)`;
  - L9/L9g, `monotone_mask`;
- the v6 layer: `data/text_layer_voc_23b/` on T7_Data;
- the fresh layer (`data/text_layer_voc_23a/`, 40 volumes) and its agent gold, both spent for TOC and links.
  The folio gold (`step-23a/gold/folio_gold.tsv`, 109 numbered of 240) is spent too.

**Next-phase candidates** (CONTEXT §4):
- locators with a page-level folio cue, on larger `loc=yes` link gold;
- TOC features for dated registers and enclosure lists;
- a `¬` filler rule;
- VOC unit order.
