# Phase 14 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-29
- **Verdict:** accept with follow-ups. Every logged number reproduces, the code does what the briefs say, and the two
  failed bars (F 33/35, L precision 61/66) are fair negative results of pre-registered rules, not implementation faults.
  One packaging defect was fixed in the review (the builder was ignored by git, F14-R.6); a small review-fixes step
  (20r: builder failure handling and progress log, stale install metadata, docs) is written. L2 stays the default.

## Checklist

- [x] **Acceptance re-run** (table below), each from the step's own script, in a scratch copy of
      `reports/data/phase-14-index-links/` where a script writes into its step dir; outputs `cmp`-equal.
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,380 passed, 11 skipped (equal to 20e's log).
      **NLP:** `PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests` → 66 passed. **LotA:** → 380
      passed, 1 skipped, 1 xfailed (the brief's "381 passed" counted the skip; F14-20a brief defect).
- [x] **Error analysis** read individual cases in every step and is in the report, one section per step. The steps'
      categories hold up (report *Review*); the reviewer's own analysis adds F14-R.2–R.5.
- [x] **Phase report final**; 40 findings appended to `reports/index.md` (by append, not rewrite) and the phase row
      updated; `make reports` clean; anchors checked in `reports/_build/phase-14-index-links.html`.
- [x] **Changelog:** *Unreleased* has 20a, 20b, 20d, 20e plus the post-phase-13 layout fix and the v5 pilot note; each
      matches the diff (`MIN_PAGEXML_TOOLS` (0, 9, 1), requirement `>=0.9.1`, both `assign_column_roles` workarounds gone
      (`page_structure.py` `assign_column_roles`, around L822), nested regions parse; new `folio_map`/`index_links` names; NLP shim names).
      "Affected sibling imports" is right: NLP's version test and the two NLP shim modules; none for 20d/20e.
- [x] **Ported code with its tests; NLP switched after parity:** 20b ran the phase-11 parity scripts before the edit
      (0 mismatches); the reviewer re-ran `scripts/verify_as_segmentation_shim.py --mode both` → 15/15 PASS, exit 0.
- [x] **No archive knowledge in core:** month, ordinal, quantity and amount patterns live in `SG_MONTHS` /
      `SG_LOCATOR_PARAMS` (`index_links.py` L66–94); `NUMBER_PATTERN` and `FolioMapParams` are named defaults; the layer
      name and SG params are in the builder script only.
- [x] **NLP originals saved; shims keep every name:** `NLP/output/as-phase-14-originals/src/` holds both files; all 10 +
      6 names import from the shims (reviewer's import check), `segment_dp` and `apply_rule` resolve to AS modules.
- [x] **Leakage:** the map reads `page_number` segments, the scans table and flags (`index_links.py` L42–60); locators
      read the unit text; L4's range comes from the map; no library code reads gold. Variant chosen on dev
      (`select.tsv` 16:04:09) before the first test run (16:04:35). The test ran twice (L3 by mistake, then L2); L2 was
      fixed by the rule before either, so its result is clean, and L3 is not adopted (F14-R.2). REPUBLIC only in
      `silver.py`. The 8 extra test rows were read before any phase-14 code (F14-R.1).
- [x] **`20a-pin` vs `19e-a`:** re-run `compare.py` → 4,591 equal, 0 differ, 0 missing.
- [x] **Protected trees:** 20e took no before/after pair. The reviewer re-ran `protect.py` and diffed it against 20a's
      `protect_after.txt`: `19e-a`, every other dev layer and `shared-data/` (with the source tar sha256) unchanged; only
      2 LotA fixture mtimes differ (the LotA suite rewrites them). Diff in `reports/data/phase-14-index-links/review/`.
- [x] **Brief defects** answered below.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 20a | regression build vs `19e-a` | 4,591 equal, 0 differ | 4,591 equal, 0 differ | yes |
| 20a | suites | AS 1,375 + 11 skipped; NLP 66; LotA 380 + 1 s + 1 xf | (now) AS 1,380 + 11; NLP 66; LotA 380 + 1 + 1 | yes |
| 20a | link and *used by* | `data/republic_segmentation_v1`, AS in the row | link resolves; row ends `AS (… scoring only, phase 14)` | yes |
| 20b | verify both | 15/15 PASS, weight 0.0293559853524414, 41,810 rows, 169 sheets | same | yes |
| 20c | gold counts | 68 rows; test 35 targets (read 28), dev 16; 50/52 on folio N | same (`summary.py`) | yes |
| 20c | confirm page | 20 items | 20 in `check_items.json` | yes |
| 20d | `front_index_run` | 41/41 | 41/41 | yes |
| 20d | LOO | 21,010/21,334 = 0.9848 | same; `loo.tsv` equal | yes |
| 20d | bar F | 33/35 = 0.9429 (read 27/28), dev 16/16 | same; `gold_eval.tsv` equal | yes |
| 20e | selection | L1 0.9818, L2–L4 0.9825 → L2 | same; `select.tsv`, `select_diffs.tsv` equal | yes |
| 20e | bar L | P 61/66 = 0.9242, R 61/61 | same; `test_rows.tsv` equal | yes |
| 20e | bar E | 33/35 = 0.9429 (read 27/28) | same; `bar_e.tsv` equal | yes |
| 20e | L3 misrun | P 60/62, R 60/61, E 32/35 | same (L3 through `test_bars.py`'s code) | yes |
| 20e | dev build | 41 vols, 72,558 / 102,588 / 101,282 | rebuild byte-identical (83 files); default run over `19e-a`: 41 done, 84 `no_front_index` | yes |
| 20e | silver | 0.4548 vs 0.1000; REPUBLIC 0.9999 vs 0.9977 | same; `silver.tsv` equal | yes |

Not re-run: 20a's own protect pair (superseded by the reviewer's protect run), 20e `sample_links.py` (read its output).

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (fixed) | `.gitignore` L217 | `/scripts/*` ignored `scripts/build_index_links.py`, which the tracked test `test_builder_two_volumes_and_resume` loads and UA-052 runs; a clean checkout would fail the suite (F14-R.6) | fixed in review: `!/scripts/build_index_links.py` |
| 2 | defect | `.venv` site-packages; `text_layer/provenance.py` L17 | manifests record archival-structures 0.14.1 (stale editable-install metadata), including `19e-a`, the v5 pilot, the v5 full run and `index_links_v1`; `git_head` is right (F14-R.7) | 20r: `make update_install`, a docs line |
| 3 | defect | `scripts/build_index_links.py` L99–107 | one failing volume aborts a parallel run (`f.result()` in order) and the parallel path logs nothing until the end; the full run (UA-052) is 9,122 volumes | 20r |
| 4 | debt | `analysis/folio_map.py` L103–130; `page_numbering.confidence_band` | year-like anchors above the scan count end the chain in 6 of 41 volumes; numbers up to the year interpolate onto the last scans with confidence `high` (94 links, 90 `high`); map tables of 1,597–3,528 rows (F14-R.3) | map v2 (next index-links phase); documented by 20r |
| 5 | debt | `analysis/index_links.py` L126 | L2's segment end accepts a bare space before a capital, so `14 Aug`, `11 Schip` count; P9 said `N[.,:;]` then a capital. The code follows the brief; the brief deviated from P9 (F14-R.2) | locator v2 on fresh gold |
| 6 | debt | `analysis/index_links.py` L93, L96 | `amount_before` and the candidate pattern have no left word boundary (`Denhof 354.`, `Oorlog1789`) (F14-20e.4, F14-R.5) | locator v2 |
| 7 | debt | `page_numbering.longest_increasing_chain` L74–79 | with equal numbers the later occurrence wins (3590 `23`, 3254 `192`), one of the two bar-F misses | map v2, measured on fresh gold |
| 8 | observation | `analysis/folio_map.py` L61–66 | `_target` scans `self.sides` and builds a set on every call (O(sides) per locate); 0.47 s per volume, so harmless | none |
| 9 | observation | `poetry.lock` | still pins pagexml-tools 0.9.0 against `>=0.9.1` | `plan/PLAN.md` precondition `poetry lock` (user) |

## Error analysis read by the reviewer

- **All test locator errors** (`test_rows.tsv`, 5 FP, 0 FN) and the dev differences (`select_diffs.tsv`): as 20e says. 4 of
  5 FPs are a number + space + capitalised word, the gap between P9 and the brief (F14-R.2).
- **Dev build, by what follows a locator** (`review/l2kinds.txt`): trailing 53,548, inside a run 44,035, punctuation then a
  capital 3,702, bare space then a capital 901, glued to a letter on the left 402. Read 20 of each of the last three kinds
  and 20 more glued ones: space+capital about 7 real / 11 dates or counts / 2 unclear; glued at least 34 of 40 noise
  (F14-R.5).
- **Year-like anchors** (`review/yearanchor.txt`): all six volumes, the bad anchor, the last real anchor and every link
  above it; examples 3265 1650/1653 → 0870 `high`, 3522 750 → 0600 `high` (F14-R.3).
- **Link-level silver and offsets** (`review/silver_links.txt`, `review/offset.txt`): target ≥ 1 word 0.707 vs control
  0.218; for single-locator entries with ≥ 3 words the best folio of N±3 is N for 0.865. Read 6 N+1 and 6 N−1 cases:
  3522 161 (matter runs on to folio 162's recto, printed numbers 160–162 consistent), 3590 1095 (only the index side has
  the name), 3280 121 (`Maurice de Nassau` on 0167 verso, the facing page of 121r), 3611 621 (convoy entries: no body
  hit, inconclusive). So off-by-one is mostly content (run-on, facing page), not the map (F14-R.4).
- **Gold vs typed page numbers** (`review/goldcheck.txt`): 35 of 38 `printed` targets carry N as a typed page number on
  the target recto; 4 of 13 `neighbours` targets do too (F14-R.1).
- **The steps' analyses hold up**: 20c's categories recount; 20d's two bar-F misses are the duplicate (3590) and the
  untyped number (3611); 20e's bar-E misses are those two.

## Brief quality

| Brief defect (step) | Answer |
|---|---|
| LotA "381 passed" is 380 passed + 1 skipped (20a) | `plan/CONTEXT.md` §5 test counts corrected |
| `sed -i` without `''` fails on macOS (20a, 20c, and the 20e misrun) | `plan/CONTEXT.md` §5: edit files with Python or Edit; a default set by a step is checked by reading it back before a test run |
| AS sep/subseq test count 51, not 25 (20b) | counts in briefs are measured at planning (already in CONTEXT §5 *Expected numbers*); noted |
| No rule for first-line fragments; a dated index without locators; 30-of-40 test bar tight at a 0.75 yield (20c) | CONTEXT §5: size a read draw on the expected yield of usable rows, with reserves read up front when the yield is uncertain; first-line fragments → entry assembly follow-up |
| Recto-share rule misclassifies 3712 (verso years); "within 0.002" check off by 0.0010 for that reason; duplicate tie rule unspecified (20d) | map v2 follow-ups (CONTEXT §4); the planner's LOO used recto-only anchors for 3712, so the brief's expected number was not measured under the library's rule (CONTEXT §5 *Expected numbers* already says this) |
| "20d's bar F passed" written into the 20e header; no place to record a tie; per-inventory sidecar needed for `volumes.tsv` (20e) | a brief that depends on an earlier bar says what to do if it failed (20e's body did; the header did not); the sidecar is accepted (`meta/<inv>.json`) |
| `amount_before` without a word boundary (20e) | locator v2 follow-up |
| P9 vs the brief's L2 (reviewer) | CONTEXT §5: when a brief operationalises a plan decision, the planner checks that the brief's rule and its test table match the decision text |

## Updates applied

- [x] Phase report `reports/phase-14-index-links.md`: *Summary*, *Review* and *Follow-ups* written; every finding is
      `confirmed` or `revised` (F14-20e.3 revised); F14-R.1–R.7 added; status *final*
- [x] `reports/index.md`: the phase's 40 findings appended, its phase row updated
- [x] `make reports` runs without warnings; anchors checked in `reports/_build/`
- [x] Review-fixes brief written (`step-20r-review-fixes.md`)
- [x] `docs/shared_library/changelog.md` complete for this phase (20r adds its own entry)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: none needed (NLP's `docs/from-archival-structures.md` §1 states the re-export rule
      generically; 20b is recorded in AS's changelog and report)
- [x] Release recommendation: v0.19.0 after 20r (see `STATUS.md`)
- [x] `.gitignore` fix (F14-R.6); UA-052 updated (L2, after 20r, coreshare); UA-053 (publication) created
