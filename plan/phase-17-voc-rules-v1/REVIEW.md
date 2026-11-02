# Phase 17 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-11-01 (the orchestrating session that also ran the 23a readers; see F17-R.4)
- **Verdict:** accept with follow-ups. All five steps are final; the 23c test stage is reviewed in the addendum at the end.

## Checklist

- [x] **Acceptance re-run; gold hashes; select before test.** `shasum -a 256` of the four gold files equals
  `gold/SHA256SUMS` (`toc_labels` 6f66ce8e…4d49, `link_lines` ce48293a…235a, `link_targets` fbe9a053…7ee6,
  `folio_sample` ade42b01…5d86). `select.json` precedes the test in 23d (15:29:08Z; `test_done` 16:08Z) and 23e
  (16:12:09Z, from dev only; `test_done` records it). 23c: `select.json` 15:37:13Z, no test yet. Bars D, L and E
  recomputed from the gold by the reviewer: same counts (table below).
- [x] **AS suite** `.venv/bin/python -m pytest -q -p no:cacheprovider tests`: 1,432 passed, 11 skipped (1,443 collected;
  1,424 at planning + 19 new). The single failure 23b saw (`test_content_words_uses_non_content_words`) does not
  reproduce. **No sibling code changed:** `find` in LotA and NLP for files newer than 2026-11-01 06:00 → none.
  Sibling suites not re-run: nothing they import changed behaviour (all changes additive, default-equal).
- [x] **P9/P12/D17.** SG check re-run (`step-23c/eval_map.py --stage sg`): 0 differences on 3444 for n = 0..1,499 under
  `FolioMapParams()` and `FOLIO_MAP_V2`. `step-23e/eval_links17.py l8check`: 0 differences over 7,715 lines. 22a layer
  unchanged: 23b's `protect_voc.py` pair differs only in 23a's new layer and gold; `protect.py --skip 23` identical.
- [x] **Error analysis in the report; `make reports` clean.** Each step reads the cases its brief lists (sections 23a–23e).
- [~] **Gold quality.** 23a self-check 30/30. The user's `unreadable` share and the layered-sheet rows wait on UA-061.
- [x] **Leakage.** 40 fresh volumes disjoint from the 567 (asserted in `stage_inputs.py`, re-checked). No rule was written
  from fresh text: 23d selected the defaults before the gold froze; 23e's grid is dev-only; the 23a harmonisation rules
  were set before any phase-17 code ran on the fresh volumes (F17-R.4). No VOC constant exists (no bar passed).
- [x] **Changelog** `## Unreleased`: 23b, 23c, 23d, 23e entries, each "No sibling imports affected"; matches `git diff`
  (`presets.py` +1 preset; `folio_map.py` 7 params + `FolioMap.pieces`; `index_pages.py` 4 names; `index_links.py`
  `min_after_frac`, 4 `LocatorParams` fields, L9/L9g, `monotone_mask`).
- [x] **Brief defects** answered (below; CONTEXT §5 updated).

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 23a | 40 built, 0 failures, `n_fail_judged = 0` | 0 / 0 | audit.json read | yes |
| 23a | gold frozen and hashed | SHA256SUMS | 4/4 hashes equal | yes |
| 23b | 567 built, audit 0 judged | 10,074 pass, 143 fail, 0 judged | audit re-run: identical JSON (10,074 / 143 / 0 judged) | yes |
| 23b | upstream identical; changed units all have `¬` | 0/5,670; 68,534 changed, 0 without `¬` | `compare.py` re-run: identical JSON | yes |
| 23b | `¬` joins ≥ 57/60 | 56/60 (fails) | reviewer read: 55/60 (k = 47 wrong, F17-R.3) | fails either way |
| 23c | SG check 0 differences | 0 | 0 | yes |
| 23c | dev SEL leaf | 1311/1833 | (dev only; test waits on UA-061) | — |
| 23d | bar D (P ≥ 0.95, R ≥ 0.90) | P 95/163, R 95/344 | same | yes (fails) |
| 23d | McNemar SEL vs PH16 | b 94, c 116, p 0.147 | same | yes |
| 23e | L8 byte-equal | 0 / 7,715 | 0 / 7,715 | yes |
| 23e | bars L, E | P 10/18, R 10/25, E 9/17 | same | yes (fail) |

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| F17-R.1 | observation | `step-23d/scan_eval.tsv` | Bar D's failure does not depend on the gold definition: on `loc=yes` pages SEL recalls 58/201 (PH16 82/201); without 2282 55/115 = 0.48 (PH16 72/115 = 0.63); over all toc pages without 2282 and 2167 86/203 = 0.42 (PH16 60/203). SEL's dev gain over PH16 came from enclosure lists, not registers with folios | report F17-R.1; follow-up |
| F17-R.2 | debt (planning) | `PLAN.md` P6 | The link gold was too small to decide bars L and E: 8 sampled lines per register TOC gave 25 gold folios, because 85% of sampled lines name none. Even 25/25 has a Wilson lower bound of 0.867. CONTEXT §5 already says to size a draw on the expected usable yield; the planner did not | report F17-R.2; CONTEXT §5 |
| F17-R.3 | observation | `step-23b/joins_read.tsv` k = 47 | "als bij nae¬ / oijt voor desen" joins to "naeoijt"; "bij nae" (bijna) and "oijt" (ooit) are two words, so wrong, not unclear: 55/60. The `¬` failure is VOC scribes' use of `¬` as a line-end filler after complete words and abbreviations (5 of 5 wrong; 1 of 15 reserves) | F17-23b.1 revised |
| F17-R.4 | observation | `step-23a/reads/`, brief amendments | The orchestrating session ran the readers and set harmonisation rules i–iv and link rules a–c after the reads, before any phase-17 code ran on fresh volumes; every change is logged (51 toc labels, 25 link edits). Definitional, applied to all rows, no leakage; it is the reviewer reviewing its own amendments, so they are listed for the user to see | report F17-R.4 |
| R-a | debt (process) | `reports/phase-17-voc-rules-v1.md`, `STATUS.md` | 23c rewrote the shared report with a Python script instead of Edit (hazard rule); the step reports no loss, and the reviewer's read-through finds every step's section and findings rows present. Parallel steps pushed `STATUS.md` to 54 lines (budget 40); overwritten here | CONTEXT §5 |
| F17-R.5 | open-issue | `step-23b/compare.json` | 9,525 `¬` remain at unit ends: the word continues in the next unit (page or region break). This is the VOC unit-order follow-up (F16-22a.7), not a guard gap | follow-up |
| R-b | observation | 23b brief | The brief's 116,881 (`¬` in the 22a join text, from F16-22a.5) and 23b's 116,920 (in unit texts) count different things; no effect on any criterion | none |

No defects: no review-fixes brief.

## Error analysis read by the reviewer

- **23b joins:** read all 65 scored and 15 reserve rows of `joins_read.tsv`. The 4 wrong are right calls; k = 47 is a
  fifth (F17-R.3); k = 19, 50, 52, 56 are fairly unclear. The 9 `„` drops at the next line start are all right. The 20
  guarded reads hold: none should join; 4 have the continuation past an interleaved number.
- **23d:** recomputed P/R from `scan_eval.tsv` and the gold, and stratified by `loc=`, by 2282 and 2167 (F17-R.1).
  The step's FP reading (2487's paragraph numbers, 35 of 68) and FN reading (in-window enclosure lists and dated
  registers) hold.
- **23e:** recomputed L and E; the 8 E misses are as the step says (item numbers on `loc=no` pages, 6 of 8). The
  post-hoc no-mono number (P 13/28, R 13/25) is labelled post-hoc and not used for a verdict.
- **23c:** re-ran the SG check; read the diff of `_chain`, `_tie_filter`, `_clean_chain` and the pieces fallback:
  defaults take the old path. The dev reading (pick is real, ties are a GM convention) holds.

## Brief quality

- 23a: `read_volume_inputs` opens `layout/pages/` (blindness); the dump's text column and "front register TOC" were not
  defined; the brief assumed the step session can spawn readers (it cannot). Rules for enclosure lists, bare numbers and
  neighbouring-line folios were missing → CONTEXT §5 (gold rubrics name the hard cases; reader agents are run by the
  orchestrator).
- 23b: no classification rule for `compare` (c); line column unnamed; count mismatch (R-b) → CONTEXT §5 (name the
  column; expected numbers measured under the step's definition — an existing note, not applied).
- 23d: the "fewer changes" tie rule selects the defaults when the defaults are in the band; mutable default in a
  signature; FP categories without a gap-fill slot → CONTEXT §5.
- 23e: an L8 expectation ("5038. . . 5107." → []) was wrong at planning; the prototype covered 21 volumes, not 22 →
  "run every command a brief quotes once at planning" (existing note) also covers expected outputs in a brief's tests.

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; findings `confirmed` or `revised` (23c dev findings
      confirmed as dev results); status final except the 23c test stage
- [x] `reports/index.md`: findings appended (with Edit), phase row added
- [x] `make reports` runs without warnings
- [x] Review-fixes brief: not needed
- [x] `docs/shared_library/changelog.md` complete for 23b–23e
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: none needed (no sibling-facing change)
- [x] Release recommendation: v0.22.0 (additive API: preset v6, folio-map options, TOC detector functions,
      `min_after_frac`, L9/L9g, `monotone_mask`). Releasing before UA-061 is safe: the 23c test adds no code unless bar F
      passes, and then only a constant

## Addendum: 23c test stage (2026-11-02, Opus 5.5)

UA-061 done by the user (240 crops; in-page IIIF zoom added and the Export bug fixed on request). `summarise_folio.py` wrote
`folio_gold.tsv` (sha256 7411576…a7af; 109 `number`, 131 `none`, 0 unreadable) and `folio_gold.done` (16:23:30Z), then
stopped at a `chmod` of a file it did not own (script fixed). The test (Sonnet) ran once at 16:26:54Z.

| Criterion | Logged | Re-run by the reviewer | Match |
|---|---|---|---|
| gold hash before the test | equal | `shasum` equal to `SHA256SUMS` and `.done` | yes |
| `select.json` before `test_done.json` | 2026-11-01 15:37 / 2026-11-02 16:26 | read | yes |
| bar F (SEL leaf ≥ 0.95) | 79/109 = 0.725 (0.634-0.800): fails | recomputed from `test_eval.tsv`: 79, strict 78 | yes |
| BASE, McNemar | 81/109; b 2, c 0, p 0.5 | same; discordant 2954-0017, 3793-1105 | yes |
| protect diff, suite | empty; 1,432 passed, 11 skipped | not re-run (no code changed) | — |

| # | Kind | File | Finding | Action |
|---|---|---|---|---|
| F17-23c.10 | revised | `step-23c/context_test.txt` | 4 misses classed "HTR blank at the gold scan" are 3 restarts (the neighbours carry the local numbering; the map answers from another numbering: 2167 f69, 7694 f31, 8840 f39) and 1 one-opening offset (4325 f675). Map-side misses 18 of 30, HTR 8 | report class table and finding revised; follow-up |
| F17-23c.11 | revised | `step-23a/gold/folio_gold.tsv` | 111 of 124 single-page `none` items are even scans (versos of foliated leaves). The step's "any rule that needs a number per scan is capped" is withdrawn; true unnumbered 20/240 | revised |
| F17-23c.9 | revised | report | reworded: fresh vs GM is not a paired comparison; GM's convention is not what keeps the map from 0.95 | revised |
| R-c | observation | `folio_gold.tsv` notes | 5 of BASE's 28 misses are on golds the user marked unsure; without them 81/104 = 0.779. Fails either way | report Review |
| R-d | debt (process) | `step-23c/misses_read.tsv` | The `context` stage rewrote the dev reading file and blanked its two read columns (the reviewer's brief also failed to say "copy aside first" before running). Rebuilt from the 23c session's annotation script: rows identical, sha256 back to 0eedae4f… | CONTEXT §5 |

F17-23c.7 and .8 confirmed. No defects; no review-fixes brief. Report, `reports/index.md` (5 rows, phase row), the Hand-off,
`plan/PLAN.md`, CONTEXT §3/§4/§5 and `STATUS.md` updated; `make reports` clean. Release unchanged: v0.22.0, and nothing from
the test adds code (no `VOC_FOLIO_MAP_V1`).
