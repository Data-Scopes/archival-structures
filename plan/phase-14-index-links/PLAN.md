# Phase 14: Index → resolution links (folio map, locators)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).

## Goal

Referential structure, the proposal's second half: an index entry in a handwritten 1.01.02 register points to the
folio that holds its resolution. When the phase ends, (1) AS maps a printed folio number to its scan sides (a generic
**folio map** from the text layer's page-number segments), (2) AS extracts the **locators** (folio numbers) from the
index entries of v5's front indexes and links each to its target sides, both with pre-registered bars on fresh gold,
and (3) the links ship as a derived layer `index_links_v1` built from a v5 layer, with a dev build and a run kit for
the full v5 layer. First, a maintenance step pins pagexml-tools 0.9.1 and links shared data properly; a second switches
NLP to the phase-11 ports (user, 2026-10-29).

## Owns

- 20a: `pyproject.toml` (pin), `archival_structures/text_layer/provenance.py` (`MIN_PAGEXML_TOOLS`),
  `analysis/page_structure.py` (`assign_column_roles` workarounds, module docstring), `text_layer/build.py` docstrings,
  `docs/text_layer.md` (pagexml lines), `tests/test_text_layer_provenance.py`, `tests/test_text_layer_extract.py` (only if
  0.9.1 changes the nested-region behaviour); `data/republic_segmentation_v1` (new link), the shared-data row's *used by*.
- 20b: NLP `src/startpage/unsupervised_segmentation.py`, `src/startpage/separator_sheets.py`, a new NLP verify script,
  `output/as-phase-14-originals/`.
- 20c–20e: `archival_structures/analysis/folio_map.py`, `archival_structures/analysis/index_links.py` (new),
  `scripts/build_index_links.py` (new), `docs/index_links.md` (new), their tests; data `data/index_links_dev/`,
  `reports/data/phase-14-index-links/`.
- Not owned: every `text_layer/` stage module (v5 is not rebuilt; D17), `layout.py`, `units.py`, `presets.py`.

## Sources

- `plan/CONTEXT.md` §1, §3 (*Index pages*), §4, §5.
- Planning outputs `reports/data/phase-14-index-links/planning/`: `draw_reads.py` → `front_index_volumes.tsv` (41
  volumes), `reads_entries.tsv` (72 rows); `folio_proto.py` → `folio_proto.tsv`.
- Explorer facts (2026-10-29): `analysis/page_numbering.py` API; types `page_number` segments; inventory GT has no
  printed numbers for hand volumes; `republic_segmentation_v1` schema; NLP shim pattern; pagexml-tools workaround sites.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 20a | pagexml-tools 0.9.1 pin, data link | UA-048 (sibling venvs) for its last part | 0.9.1 installed and pinned; workarounds dropped where tests pass; `20a-pin` (231 volumes) equal to `19e-a` in every table; AS, NLP, LotA suites green; `data/republic_segmentation_v1` link + *used by* | `step-20a-pagexml-pin.md` |
| 20b | NLP switches to the phase-11 ports | 20a | originals saved; two shim modules; verify script: shim = originals (0 mismatches, 41,810 change-point rows, 169 sheets); NLP suite green | `step-20b-nlp-segmentation-shim.md` |
| 20c | Gold reads: locators and link targets | nothing (parallel with 20a/20b) | 60 entries read (reserves by rule): locators, target folio scan, content verdict; confirm page of 20 + UA | `step-20c-link-gold-reads.md` |
| 20d | Folio map | 20c (for its test) | `analysis/folio_map.py`; LOO floor ≥ 0.97 on the 41 volumes; bar F on test gold ≥ 0.95 | `step-20d-folio-map.md` |
| 20e | Locators, links, `index_links_v1` | 20c, 20d | variant L1–L4 chosen on dev; test P ≥ 0.95, R ≥ 0.90, end-to-end ≥ 0.90; dev build on `19e-a`; silver checks; docs; UA for the full run | `step-20e-index-links.md` |

## Why this order

20a first (user): later steps build on 0.9.1, and its regression build shows whether 0.9.1 changes any v5 table. 20b
needs the sibling venvs that 20a's UA updates. 20c is reading only and runs in parallel with 20a/20b; it reads before
any phase-14 rule exists. 20d's test needs 20c's test gold; 20e needs the frozen map. A failed bar is a negative result.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Scope: front indexes linked inside their own volume.** The 41 v5 dev volumes with a front index (planning rule below) | 20c–20e | User, 2026-10-29. Separate index volumes point to other volumes (needs an EAD volume map), date locators (dated summary indexes) and unit-level targets (which resolution on the folio) are follow-ups |
| P2 | **Maintenance as step 20a/20b** | 20a, 20b | User, 2026-10-29 |
| P3 | **Front index** of a volume (`front_index_run`): sides in `layout/pages` order; the flagged sides before the first run of ≥ 20 consecutive unflagged sides; ≥ 10 of them, the first within the first 10 sides, ≥ 300 sides after the run; volumes with > 60% flagged sides are index volumes (out) | 20c draw (done), 20e code | Planning: 41 of 125 local hand volumes; 3274/3290 entries are mostly one-region `hand:region` units, so an **entry = a `main_text` unit on a front-index side**, any `unit_method` |
| P4 | **Locators are folio numbers.** Resolutions are foliated (recto only), the front index is paginated, folio numbering restarts after it. Folio N → recto of scan S(N) and verso of the next scan | 20c, 20d, 20e | Explorer + prototype: 21,432 of 21,437 chain anchors on rectos after the index; text checks 3349/3590/3265 |
| P5 | **Folio map** (generic, `analysis/folio_map.py`): one anchor per side (first page-number segment whose text matches `^\D{0,3}(\d{1,4})\D{0,3}$`), excluded sides (front index, flagged sides) give none; foliation iff ≥ 0.9 of anchors on rectos (then recto anchors only, position = scan position), else pagination (position = side position); main chain = `longest_increasing_chain`; locate N with `interpolate_folio_to_scan(N, chain, extrap=True, max_scan, edge=6)`, round half up; beyond the chain by > 6 numbers → none; method printed / interpolated / extrapolated; `confidence_band` (3, 10) | 20d copies it; no tuning | Prototype (`folio_proto.py`): chain covers 21,437 of 28,349 rectos after the index (0.7562); LOO under the 20d definition (`loo_check.py`, scans-table positions) 0.9858 (21,030 / 21,334) |
| P6 | **Gold by agent text reads** (20c) before any phase-14 code: per entry the locators, the first folio locator's target scan (found by reading printed numbers, bisecting; no map), the content verdict. A **confirm page** (not blind: it shows the agent's target) of 20 links for the user; it gates publication, not the bars | 20c | Phase 13 practice (P3 there); a blind link search in the viewer would take the user hours |
| P7 | **Split by volume** (draw seed 20261129): dev 3254, 3471, 3394, 3308 (20 entries); test 3256, 3456, 3401, 3611, 3320, 3712, 3590, 3352 (40); one reserve per volume, used only for an entry judged `not entry`/`unreadable input` | 20c–20e | Map and locator choices see dev only; 3712 (offset −336, no restart) is a test hard case |
| P8 | **Bars.** F (20d): predicted recto scan of the gold target locator = gold scan for ≥ 0.95 of test entries with a gold target; LOO exact-position share ≥ 0.97 over the 41 volumes (floor, stop rule). L (20e): locator micro P ≥ 0.95 and R ≥ 0.90 on test (multiset of numbers per entry). E (20e): end-to-end, the gold target locator extracted and mapped to the gold scan, ≥ 0.90 of test entries with a gold target | 20d, 20e | Fixed 2026-10-29 before any read or code. Map params freeze after F; L/E use the frozen map |
| P9 | **Locator variants** (20e), selected on dev by max F1 (ties: lower): L1 trailing number run; L2 + numbers ending a sub-entry (`N[.,:;]` then a capital); L3 + exclusions (month or ordinal after, amount prefix, quantity word after); L4 = L3 + range filter against the volume's folio map | 20e | Explorer samples: `252: 568: 581:`, `172, 464.`, `… 198. Misse Uts: … 258.`, false `6 maanden`, `6e:` |
| P10 | **Derived layer** `NL-HaNA_1.01.02_index_links_v1` from a v5 layer (`volumes.tsv`, `folio_map/`, `links/`, `manifest.json`); v5 is not rebuilt (D17). Publication is a Decide UA at the review | 20e | Keeps v5 published tables fixed; links can version on their own |
| P11 | **Silver, reported only:** entry words (≥ 6 letters) found on the target folio vs on folio N+3; REPUBLIC `republic_segmentation_v1` lines on the target sides for the 13 covered front-index volumes, through `data/republic_segmentation_v1` (scoring only) | 20e | No bar: words and REPUBLIC are noisy; controls make them readable |
| P12 | **pagexml-tools 0.9.1 in the siblings first** (UA-048): AS's `MIN_PAGEXML_TOOLS` is re-exported by NLP (`test_pagexml_tools_version_is_at_least_080`); both sibling venvs hold an editable checkout labelled 0.9.0 | 20a part 6, 20b | Explorer, 2026-10-29 |
| P13 | **Stop rules:** a failed bar → negative result, report, no tuning on test. A 20a table difference → stop and UA (v5 builds would no longer reproduce) | all | Phase 10/12/13 practice |

## Hazards

- **Side order:** always the `layout/pages` / scans-table order, never a number parsed from the scan id (the 2026-10-29
  layout fix: `3548_0003A`, `5751B_P001`; multi-part volumes).
- **Locator-only lines on index sides are typed `page_number`** (`rule:pn_line_offchain`): exclude index sides from anchors.
- **Interpolation on openings:** `numbering_mode` reads recto-only foliation on SG openings as `pagination` (slope 1.0
  scans per number); decide the mode by the recto share (P5), not by `numbering_mode`.
- **Gold bias:** the 20c reader must not open `folio_proto.tsv` (offsets) or any map output.
- **REPUBLIC line ids** come from another HTR run (0 exact box matches in 3265): match by `page_id` only.
- **Shared venvs:** 20a changes `.venv` (pagexml-tools); 20c reads only TSVs and is unaffected.
- **The agent shell:** no `rm -rf`, `nohup`, `&`; zsh; a new dir per run; `chmod g+w`; coreshare for > 1 worker.

## Open questions for planning

Settled 2026-10-29: scope (P1) and maintenance placement (P2) by the user; the rest by the planner. Open for the user:
UA-048 (sibling pagexml-tools).

## Review checklist

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer rather than taken from the log.
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; NLP and LotA suites with their interpreters.
- [x] Error analysis read individual cases, as listed in each brief, and is written up in the
      phase report (`reports/phase-14-index-links.md`), one section per step.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Every public API change is in `docs/shared_library/changelog.md`, with the affected
      sibling imports.
- [x] Ported code came with its tests; NLP switched only after AS parity (20b).
- [x] No archive-specific knowledge hardcoded in AS core: patterns and thresholds are parameters with named defaults.
- [x] NLP originals saved; shims keep every original name.
- [x] Leakage: map and locator choices on dev volumes only; each test once; REPUBLIC scoring only; no feature reads gold.
- [x] `20a-pin` vs `19e-a`: every table equal (or the stop rule fired).
- [x] `Brief defects` entries answered: templates or briefs fixed.

## Hand-off

Reviewed 2026-10-29 (Opus 5.5): **accept with follow-ups** ([REVIEW](REVIEW.md); report final). Every number reproduces.
Bars: F 33/35 = 0.943 **failed** (one row; both misses are misread or untyped printed numbers); L (L2) P 61/66 = 0.924
**failed**, R 61/61; E 33/35 = 0.943 passed. L2 stays the default: chosen on dev before the test; L3's better test
numbers come from a mistaken run and are not adopted (F14-R.2). The 20c extra test rows were sound (F14-R.1).
Fixed in review: the builder was ignored by git (F14-R.6). Step 20r (review fixes, Sonnet): builder records failed
volumes and logs progress, `make update_install` for the stale 0.14.1 metadata (F14-R.7), docs limits (F14-R.3–R.5).
Then release **v0.19.0** (20a pin, layout side-order fix, `folio_map`, `index_links`, builder) and UA-052 (full run, L2).
User actions, all resolved 2026-10-29: UA-050 (check page: 11/11 test links confirmed, F14-20c.8), UA-052 (full run: 412
front-index volumes, 902,031 links, F14-20e.9/.10), UA-053 (a: v1 not published). Follow-ups for an index-links v2
phase (map guard, locator rule, entry assembly, openings; fresh gold from the 29 untouched local and 371 drive-only
front-index volumes) are in `plan/CONTEXT.md` §4 and the report's *Follow-ups*.
