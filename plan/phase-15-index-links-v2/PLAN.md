# Phase 15: Index links v2 (map guards, locators, entries from lines)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).

## Goal

When the phase ends:
- AS builds **`NL-HaNA_1.01.02_index_links_v2`** from a v5 text layer. It has a folio map that ignores year-like
  chain ends and tied printed numbers and whose confidence sees inconsistent brackets, plus an opening side.
  Locators are punctuation-bounded with word-boundary exclusions, and entries are assembled from **lines** in column
  order rather than taken from units.
- Each rule has a bar set in advance and is tested once on fresh gold from the 29 untouched local front-index volumes.
- v1 rebuilds byte-identically.
- The full run and publication are UAs.

## Owns

- `archival_structures/analysis/folio_map.py` (21b), `archival_structures/analysis/index_links.py` (21c, 21d),
  `scripts/build_index_links.py` (21d), `docs/index_links.md` (21d), and their tests.
- Data: `data/index_links_dev/21d-*`, `reports/data/phase-15-index-links-v2/`.
- Not owned: `page_structure.py` and `presets.py` (imported only; `_reorder_lines` and `SG_HAND_COLUMN_ORDER_V5`), every
  `text_layer/` module, every text layer (v5 is not rebuilt; D17).

## Sources

- `plan/CONTEXT.md` §1 (*Index links*), §2 (*Index links (phase 14)*, *v5*), §3 (*Front indexes*), §4 (*From phase 14*, and
  the phase-13 column-order item), §5.
- `reports/phase-14-index-links.md`: *Review* (F14-R.2–R.5), *After the review*, *Follow-ups*; 20d's miss table.
- Planning outputs (2026-10-30), all in `reports/data/phase-15-index-links-v2/planning/`:
  - `probe_pool.py` → `pool_volumes.tsv` (29 volumes; chain ends vs scan counts, dev 41 and full 412);
  - `peek_side.py` → `planner_seen.tsv` (3459 0015v and 3444 0020v printed, excluded from the draw);
  - `map_v2_proto.py` → `map_v2_proto.tsv` (G1–G3 prototype, reproduces the phase-14 LOO exactly);
  - `assembly_proto.py` (line rule and reorder, run on spent volumes only);
  - `draw_sides.py` → `reads_sides.tsv`;
  - `protect15.py`.
- Explorer facts (2026-10-30): the current `folio_map`/`index_links`/builder API, test counts (13 + 10), v5 table
  headers, the 20c gold columns. No sibling imports either module.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 21a | Fresh gold reads: index sides, entries, locators, targets | nothing | 29 sides read line by line (≥ 11 dev, ≥ 14 test `index`); targets 2/dev side, 4/test side (≥ 50 test usable); confirm page + UA; hashes | `step-21a-gold-reads-v2.md` |
| 21b | Folio map v2 | 21a (test, for bar F only) | G1–G4 additive, v1 tables identical; planner numbers reproduce (LOO v2 20,952/21,176); bar F ≥ 0.95 on test targets | `step-21b-folio-map-v2.md` |
| 21c | Locators v2 and entry assembly from lines | 21a (dev, then test), 21b | 16-cell dev grid, fixed selection rule; test once: L P ≥ 0.95, R ≥ 0.90; attribution A ≥ 0.90; E ≥ 0.90 | `step-21c-locators-entries-v2.md` |
| 21d | The `index_links_v2` layer | 21b, 21c | `--version v2`; v1 rebuild = `20r-a`; dev build of 41 volumes; compare + silver; docs; full-run UA | `step-21d-index-links-v2-layer.md` |

## Why this order

- 21a is reading only, and it comes first so that no phase-15 rule exists when the gold is read. Dev is read before
  test, so 21c's selection can start early.
- 21b's rules were fixed at planning (no variants), so it needs only the test targets, for one run. Its LOO and
  regression checks need no gold.
- 21c needs the frozen map for L7 and E.
- 21d only assembles what 21b/21c chose. It runs whatever the bars said, and the review decides publication.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope: the 1.01.02 front indexes, as in phase 14** | all | User, 2026-10-29/30. Out of scope: separate index volumes, dated summary indexes, unit-level targets, and the out-of-chain volumes 3681/3601 (drive-only) |
| P2 | **Gold pool: the 29 local front-index volumes no gold touched** | 21a | User, 2026-10-30 (local only, nothing to stage). The 12 phase-14 gold volumes are spent; sides read in phase 13 (19a, 19g) and by the planner are excluded |
| P3 | **Split by volume, stratified** by v1 no-locator unit share (≥ 0.25 = `high`, mostly two-column indexes): dev 13 (6 high), test 16 (7 high); 1 read + 1 reserve side per volume; seed 20261030 | 21a–21c | The 13 high volumes are where the assembly matters; stratifying keeps both splits balanced |
| P4 | **Side-level gold:** per line `col`, `label` (S/C/H/F/X, as phase 13), `entry` id in the reader's reading order, `locators` (with `N@A` for mid-line ends); targets drawn after the side is read (2 dev, 4 test per side) | 21a | One read gives locator gold (≈ 300+ test locators), entry gold and link targets; phase 14's unit-drawn gold baked in the unit cuts |
| P5 | **Map v2 rules fixed at planning, no variants.** G2: drop every point of a number tied within 2 positions, before the LIS. G1: cut the chain where Δn > 1.0·Δpos + 10, keep the longest piece. G3: interpolated `high` needs dist ≤ 3 and \|Δn − Δpos\| ≤ 2 for the bracket. G4: opening = verso of the front scan. The mode decision is unchanged | 21b | Prototype on 41 volumes: LOO 0.9848 → 0.9894; 7 chain ends fixed (6 year/inventory numbers, 3471's misread 485); 20c spent gold 49 → 50 of 51. **No count bound:** 3712's numbers continue from the previous volume (348–684 on 533 sides), so a scan or side bound drops real anchors (tried and rejected) |
| P6 | **Locator variants** L5 (L2 + `N[.,:;]` before the capital, P9 of phase 14 as written), L6 (L5 + L3 exclusions with left word boundaries: glued digits, `amount_before` on the whole prefix), L7 (L6 + range) | 21c | F14-R.2, F14-R.5; at planning, v1's L3 drops 252 in `Hof 252.` (`amount_before` hits `Hof `) |
| P7 | **Entry assembly variants:** A0 = v1 units; A1 = line rule (an entry closes after a line ending in a number; a numbers-only line joins the previous entry) on the v5 unit order, `main_text` + `marginalia` lines; A2 = A1 after the phase-13 19f reorder per side (`_reorder_lines`, `SG_HAND_COLUMN_ORDER_V5`); A3 = A2 + heading rule (no digit, ≤ 4 words at entry start) | 21c | Planner peeks: 3444 interleaves two columns line by line (units cut at every line); 3459's right column is typed `marginalia` (no entry in v1). The reorder fires on 26–85% of front sides of spent volumes |
| P8 | **Selection:** max dev link F1 over the 4 × 4 grid; ties go to the simpler (A0 < … < A3, then L2 < L5 < L6 < L7); `select.json` before the test | 21c | One objective covering locator and attribution; fixed before any read |
| P9 | **Bars** (fixed 2026-10-30, before any read): F ≥ 0.95 (map, test targets); L P ≥ 0.95, R ≥ 0.90 (`(line, N)` multiset); A ≥ 0.90 (attribution among matched locators: the predicted entry's first line = the gold entry's); E ≥ 0.90 (located, right entry, right scan). Wilson intervals reported; v1 baseline beside them | 21b, 21c | Phase-14 bars kept for F/L/E; A is new. 35 rows could not separate 0.943 from 0.95; about 60 test targets narrow that |
| P10 | **The layer** `index_links_v2`: new `entries/` table, links carry `entry_id`, `line_id`, opening; v1 via `--version v1` (default) byte-identical. Publication is a Decide UA at review | 21d | D17-style versioning; v1 stays reproducible |
| P11 | **Silver reported only:** word share on target vs N+3 (phase-14 definition), overall and in the high stratum | 21d | It is noisy; it shows whether the assembly helps where it should |
| P12 | **Stop rules:** a failed bar is a negative result, reported, no tuning on test; a v1 regression difference stops 21d | all | Phase 13/14 practice |

## Hazards

- **Side order:** always the `layout/pages` / scans-table order, never a number parsed from a scan id.
- **Gold bias:** the 21a reader must not open `planning/` (except the read list) or any phase-14 data/link output
  (except the one page builder it is pointed to).
- **3712-style volumes** whose numbering continues from the previous volume: no count bounds.
- **`_reorder_lines` is private** in `page_structure.py` and runs on duck-typed lines (`baseline=None`). A change
  there changes A2/A3. No active phase owns `page_structure.py`.
- **Spent gold:** the phase-14 20c gold may inform (dev-level), never select; 21c's spent-volume read is a check only.
- **The changelog's top section** is `## v0.19.0` while v0.19.0 is untagged. 21b stops before editing the changelog
  if the user has not released.
- **The agent shell:** no `rm -rf`, `nohup` or `&`; zsh; a new dir per run; `chmod g+w`; coreshare for > 1 worker.
- **Disk:** the shared volume had 19 GiB free on 2026-10-30. Dev builds are small; nothing large is staged in this
  phase.

## Open questions for planning

Settled 2026-10-30 by the user: index-links v2 first (this phase), the gold pool is local only (P2), the second archive
is phase 16 (1.04.02 VOC, seeded; staging UA-054). Settled by the planner: everything else (P3–P12). None open.

## Review checklist

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`. No sibling is touched (no sibling suite needed).
- [x] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report (`reports/phase-15-index-links-v2.md`), one section per step.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Every public API change is in `docs/shared_library/changelog.md` (no sibling imports affected).
- [x] No archive-specific knowledge hardcoded in AS core: patterns and thresholds are parameters with named defaults.
- [x] Leakage: gold read before code (21a file times and hashes); selection on dev only (`select.json` before
      `test_done.json`); each test once; no library code reads gold; silver scoring only.
- [x] 21a gold sound: entry ids consistent, reserves by rule, targets drawn after the side read.
- [x] v1 regression: `21d-v1` equals `20r-a`.
- [x] `Brief defects` entries answered: templates or briefs fixed.

## Hand-off

Reviewed 2026-10-30 (Opus 5.5): **accept with follow-ups**. See [REVIEW](REVIEW.md); the report is final. One Opus
session ran the steps as Sonnet subagents (21a in 7 sessions; 21b and 21c in parts, around the gold), then reviewed
them. Every number reproduces, and v1 rebuilds byte-identically.

Bars on the fresh 21a gold (29 sides, 63 usable test targets):
- F 62/63 passed (v1 the same);
- L P 0.981 passed;
- L R 0.856, A 0.892 and E 0.825 failed.

The fixed rule selected A0 + L7, so v2 keeps v1's entries. No review-fixes step: the two text defects were fixed in
the review (F15-R.3, R.7).

User actions:
- UA-055 (check 20 links);
- UA-056 (v2 full run, optional);
- UA-057 (publication; recommended (a), do not publish).

Release **v0.20.0**: folio map v2, locators L5–L7, line-level entry assembly, `volume_links_v2` and the builder's
`--version v2`; all backwards compatible. Next: phase 16 (VOC). An index-links v3 (hybrid assembly, new gold) is in
`plan/CONTEXT.md` §4.
