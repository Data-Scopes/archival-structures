# Phase 08: Region quality and duplicates (C16, C17, `page_duplication`)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **Status: done; reviewed** 2026-10-21 (accept with follow-ups; [REVIEW](REVIEW.md)). Planned 2026-10-21 (Opus). Seeded by this planning session from
> `upstream_inventory.md` §3 C16/C17 and §4 rows 16–17 (no seed directory existed).

## Goal

AS owns LotA's two generic corpus-integrity tools, and LotA calls them:
- `analysis/region_quality.py`: the usable / fragment / noise filter, text-based, with the lexicon
  injected as a set of words;
- `analysis/duplicate_pages.py`: the MinHash/LSH duplicate (re-shoot) detector over
  ordered page texts.

LotA's `corpus/quality.py` and `corpus/integrity.py` keep their signatures and become thin
wrappers. Their stored outputs reproduce exactly. A new corruption operator, `page_duplication`,
simulates re-shoots in the AS harness.

## Owns

- AS (new): `archival_structures/analysis/region_quality.py`,
  `archival_structures/analysis/duplicate_pages.py`, `tests/test_analysis_region_quality.py`,
  `tests/test_analysis_duplicate_pages.py`, `tests/fixtures/region_quality/`.
- AS (additive): `stream_analysis/corruption/operators.py`, `metrics.py`
  (`observed_start_labels`), `simulate.py` (`CASE_NAMES`, `_apply_case`), `__init__.py`, the three
  `tests/test_stream_analysis_corruption*.py`, `docs/challenges/page_order_problems.md`,
  `docs/challenges/simulation_harness.md`.
- LotA: `archival_language/corpus/quality.py`, `archival_language/corpus/integrity.py`,
  `tests/test_corpus_quality.py`, `tests/test_corpus_integrity.py`, a new §7 in
  `docs/from-archival-structures.md`, and the new output dir `output/integrity/as-step17/`.
  LotA's own active work (its phase 3, titles) touches none of these files (checked 2026-10-21:
  they have no uncommitted changes).
- `reports/data/phase-08-region-quality-and-duplicates/`.

## Sources

- `docs/shared_library/upstream_inventory.md` §3 C16 (L396–408), C17 (L410–421), §4 rows 16–17
  (L460–461), D8 (L529–531).
- `plan/CONTEXT.md` §4 (phase-07 follow-ups; moved out of this phase, P1).
- LotA `plan/CONTEXT.md` Rule 7–9 (L420–506): the stored artefacts, the quality bias and the
  duplicate-detection summary.
- LotA `archival_language/corpus/quality.py` (436 lines), `corpus/integrity.py` (554 lines),
  `tests/test_corpus_quality.py` (15 tests), `tests/test_corpus_integrity.py` (16 tests),
  `report/05-region-quality-filter.md` L52–55, `report/09-region-quality-retune.md` L53–93.
  Read through an Explore subagent on 2026-10-21. The relevant bodies are copied into briefs 16
  and 17.
- AS `stream_analysis/corruption/` (`operators.py` 229 lines, `metrics.py` 200, `simulate.py` 172)
  and its three test files (61 tests).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 16 | Region-quality filter (C16) | nothing | AS: 300/300 gold rows equal LotA's stored `-after` scoring and 300/300 the `-before` scoring (with the exemption threshold set to `inf`); report numbers recomputed. LotA: wrappers, gold re-check 0 mismatches, 200 seeded inventories 0 mismatches against `inventory-quality.tsv`, suite green | [`step-16-region-quality.md`](step-16-region-quality.md) |
| 17 | Duplicate-page detector (C17) | nothing | AS: the LotA algorithm over `(page_id, text, position)`, coefficients pinned. LotA: wrapper. A full SG run through LotA's CLI is byte-identical to the stored 48,473-row candidate table and the manifest; 13/13 re-shoots | [`step-17-duplicate-pages.md`](step-17-duplicate-pages.md) |
| 17b | `page_duplication` operator | nothing (17 is not needed) | Operator, first-occurrence start labels, harness case, docs; all 61 existing corruption tests green and unchanged except one rename | [`step-17b-page-duplication-operator.md`](step-17b-page-duplication-operator.md) |

## Why this order

The three steps are independent: they touch disjoint AS modules, and 16 and 17 touch different
LotA files. They can run in any order, or in parallel. Each step then appends its own changelog
entry and report section with Edit. The suggested order is 16 → 17 → 17b:
- 16 is the smallest port;
- 17's full run takes about 67 min, run in the background;
- 17b is new work and needs no LotA.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D8 | Small gold extracts may be copied into AS `tests/` (user, 2026-10-14) | 16 copies the 300 gold rows plus a 2,837-term lexicon subset into `tests/fixtures/region_quality/`, with a provenance README | A fixture regression test on real gold. The subset is every cleaned gold token that is in the lexicon, so features are identical to the full 233,538-term lexicon |
| P1 | Phase-07 text-layer follow-ups (CONTEXT §4) are **not** in phase 08. They become a new phase 09, *Text layer v3 preparation*; the optional phase becomes 10 (user, 2026-10-21) | No phase-08 step touches `text_layer/` | Disjoint modules; keeps phase 08 at 3 steps |
| P2 | Only `page_duplication`; `page_drop` deferred (user, 2026-10-21) | Each unit independently, with probability `rate`, gets one copy inserted **immediately after** it | All 13 documented SG re-shoots are adjacent (F08-P.4). Drop needs its own start-label design |
| P3 | Under duplication, a canonical start page is a start **at its first observed occurrence only** (user, 2026-10-21) | `observed_start_labels` labels the first position whose `canonical_index` is a true start, not later repeats | Backwards compatible: no existing operator repeats a canonical index |
| P4 | Region quality is text-based. Lexicon: `AbstractSet[str]` of lower-case words. bbox: optional `(x, y, w, h)`. Thresholds: a frozen `QualityThresholds` dataclass whose defaults are LotA's current (post-retune) values. The rule weights stay module constants | LotA keeps its commented `THRESHOLDS` dict and passes `QualityThresholds.from_dict(THRESHOLDS)`; a LotA test asserts the dict equals the AS defaults | Keeps LotA's documented dict (LotA Rule 8 says to quote it) without two sources of truth |
| P5 | Duplicate detection takes `(page_id, text, position)` triples. `position` is the scan number; two pages are adjacent when their positions differ by exactly 1 | LotA passes `page.scan_number`. Sequence-order adjacency is **not** used | All 23 rows of the 13 re-shoots are same-side pairs across scans N and N+1, which are not neighbours in sequence order (F08-P.5) |
| P6 | Exact reproduction, not "close" | 16: score \|Δ\| ≤ 1e-9, class and rules equal. 17: `cmp`-identical TSVs | Pure ports; any drift is a bug |
| P7 | AS gets no gensim and no datasketch | The lexicon is injected; MinHash stays hand-rolled (numpy) | Neither is installed in `.venv`; gensim has no Python 3.14 build (LotA `pyproject.toml`) |

## Hazards

- **LotA `resolve_path` refuses paths outside its allowed roots.** The AS scratchpad is
  rejected (`PathOutsideRootsError`, 2026-10-21). LotA CLI runs write under LotA `output/`, in a
  new dir. Analysis scripts that only read call the functions directly. (17)
- **Stale LotA files:** `output/quality/1.01.02-gold-scored.tsv` and `-supplement.tsv` equal
  the **pre**-retune `-before` files. The current code reproduces `-after` (F08-P.1). (16)
- **Stored dtypes:** in the candidate TSV, `scan_number_b` is float (9 rows have no scan number),
  so a rebuilt frame must be built the same way (a list of dicts, `pd.DataFrame(rows, columns=…)`)
  for byte identity. (17)
- **The full integrity pass** takes ~67 min with peak RSS ~4.9 GiB. Run it with the Bash tool's
  `run_in_background`, never `&` or `nohup`. Don't run anything heavy alongside it. (17)
- **`test_provenance_points_at_the_unit_in_its_position`** asserts
  `len(observed) == len(canonical)` and is parametrised over the four operators. Do not add
  `page_duplication` to it; give the new operator its own provenance test. (17b)
- **`test_default_cases_are_all_four`** compares against `set(CASE_NAMES)`, so it passes with
  five; rename it to `test_default_cases_are_all_case_names`. (17b)
- **LotA `CLAUDE.md`:** `venv/bin/python` (3.14), `make test`, `make lint` (ruff); never commit;
  `chmod g+w`. LotA imports AS from the working tree. (16, 17)
- **Archive literals:** LotA tests forbid `"main_text"`/`"margin"` in `quality.py` and
  `1.01.02`/`NL-HaNA` in `integrity.py`. The AS modules carry the same tests. (16, 17)

## Open questions for planning

None left. Settled 2026-10-21: the scope (P1), the operators (P2), start labels (P3), the
fixture (D8 covers it), the APIs (P4, P5), and the acceptance inputs, which were measured
(F08-P.1–P.4).

## Review checklist

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; LotA `make test` and
      `make lint` with LotA's `venv/`.
- [x] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report (`reports/phase-08-region-quality-and-duplicates.md`), one section per step.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Every public API change is in `docs/shared_library/changelog.md`, with the affected
      sibling imports.
- [x] Ported code came with its tests; the sibling switched its import only after those tests
      passed here.
- [x] No archive-specific knowledge hardcoded in AS core. The thresholds are documented as
      tuned on SG Dutch.
- [x] LotA copies deleted (`_clean_token_for_lookup`, `_clip`, `_shingles`, `_minhash_*`,
      `_lsh_candidates`, constants), wrappers keep their signatures.
- [x] Leakage: the gold metrics use LotA's parity split as declared (odd `row_no` tune, even
      held-out); no threshold is tuned in this phase.
- [x] `Brief defects` entries answered: templates or briefs fixed.
- [x] 17's full-run TSVs `cmp`-identical; `reshoots.py` 13/13 on the new table.
- [x] 17b: the existing 61 corruption tests unchanged except the one rename.

## Hand-off

Later phases can assume the following (release: v0.13.0 recommended; not released yet):

- **`analysis.region_quality`** (text plus an injected lexicon, `QualityThresholds`) and
  **`analysis.duplicate_pages`** (`duplicate_page_candidates` over `(page_id, text, position)`) are
  AS-owned. LotA `corpus/quality.py` and `corpus/integrity.py` are thin wrappers, and their stored
  outputs reproduce exactly.
- **Thresholds:** a threshold change goes through AS. LotA's `test_thresholds_match_as_defaults`
  catches drift.
- **Lexicon:** numerals in the injected lexicon count as hits (F08-R.1).
- **Duplicates:** adjacency is by position, never by sequence order. `adjacent` is the strong
  signal (0.95 re-shoots), and the Jaccard is not (F08-R.2).
- **Corruption harness:**
  - it has a fifth case, `page_duplication`, with a per-page rate;
  - `observed_start_labels` labels the first occurrence of a canonical index only;
  - a default `simulate_inventory` run writes five cases.
- **Follow-ups** (report *Follow-ups*; `plan/CONTEXT.md` §4):
  - LotA's `adjacent` filter and the numerals in its lexicon are LotA decisions;
  - measuring re-shoot recall below J 0.6 and a classifier run under duplication are phase-10
    candidates.
