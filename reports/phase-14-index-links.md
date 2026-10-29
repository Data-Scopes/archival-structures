# Phase 14 report: Index → resolution links (folio map, locators)

> **Status:** final (reviewed 2026-10-29).
> **Phase plan:** [`plan/phase-14-index-links/PLAN.md`](../plan/phase-14-index-links/PLAN.md).
> **Code state:** AS unreleased after v0.18.0 (v0.19.0 recommended after step 20r); siblings: NLP (step 20b), LotA unchanged.
> **Last updated:** 2026-10-29, by the phase review (Opus 5.5).

## Summary

Phase 14 links the entries of the front indexes of 41 handwritten 1.01.02 resolution volumes to the scans of the folios
they cite. A generic folio map (`analysis.folio_map`) chains the printed folio numbers on the rectos and interpolates the
gaps; leave-one-out it places 0.9848 of the anchors on the right scan. On agent-read test gold it places the cited folio
on the right scan in 33 of 35 entries (0.943), one row short of the pre-registered 0.95, so **bar F failed** (both misses
are misread or untyped printed numbers). The locator rule chosen on dev (L2, the trailing number runs of each sub-entry)
finds every gold locator (R 61/61) but also takes day numbers of a dated index, a count and a heading
(P 61/66 = 0.924 < 0.95), so **bar L failed** on precision; end to end, the gold locator is extracted and placed on the
right scan in 33 of 35 (**bar E passed**, 0.943 ≥ 0.90). The derived layer `index_links_v1` is built for the 41 local
volumes (72,558 entries, 101,282 links) with a run kit for the full v5 layer (UA-052). The review keeps L2 (L3's better
test precision is known only from a mistaken test run), finds that year-like end anchors in 6 volumes give year numbers
`high`-confidence links (F14-R.3), and measures link-level silver: 71% of the links have an entry word on the target
sides against 22% at folio + 3 (F14-R.4). Maintenance: pagexml-tools 0.9.1 is pinned (both workarounds gone, the v5
regression build is byte-identical) and NLP's segmentation and separator-sheet modules are shims over AS (A/B
identical). Two narrow bar failures make v1 a documented negative result; a v2 (map guard, locator rule, entry assembly)
needs fresh gold from the 29 front-index volumes no gold has touched. After the review: the user's check of 20 gold
links confirms all 11 test links (F14-20c.8); the full run finds front indexes in 412 volumes (629,912 entries,
902,031 links, F14-20e.9); v1 is not published (UA-053).

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F14-P.1 | 41 of the 125 local hand volumes of the v5 dev build `19e-a` have a front index by the rule of P3 (≥ 10 flagged sides before the first run of 20 unflagged, starting within the first 10 sides, ≥ 300 sides after, ≤ 60% flagged); they hold 72,311 `main_text` units (with ≥ 3 characters of text) on front-index sides | data-property | sg, index | confirmed | [Planning](#planning) |
| F14-P.2 | Index entries are not always `hand:index*` units: on the flagged sides of 3274 and 3290, 2,045 and 1,416 `main_text` units are one-region `hand:region` units (the HTR made each entry its own region), so an entry is any `main_text` unit on a front-index side | data-property | sg, index, units | confirmed | [Planning](#planning) |
| F14-P.3 | The resolutions after a front index are foliated: 21,432 of 21,437 page-number chain anchors in the 41 volumes are on rectos; the front index itself is paginated and folio numbering restarts after it (scan − folio ≈ +16 to +84), except 3712 (−336) | data-property | sg, page-numbering | confirmed | [Planning](#planning) |
| F14-P.4 | Index locators are folio numbers; folio N = recto of one scan plus verso of the next: 3349 locator 207 is printed on scan 0242's recto and its entry word is on 0243's verso; 3265 locator 247 (number not read by the HTR) matches the marginal note on 0293 verso, REPUBLIC session 1659-04-18 | data-property | sg, index, page-numbering | confirmed | [Planning](#planning) |
| F14-P.5 | The page-number chain covers 21,437 of 28,349 rectos after the front index (0.7562); leave-one-out interpolation of a chain anchor's scan position is exact for 21,030 of 21,334 (0.9858; lowest 3611 0.925, 3265 0.928) | result | sg, page-numbering | confirmed | [Planning](#planning) |
| F14-P.6 | `layout/pages` has no rows for scans without regions (5–12 per volume, e.g. 3254: 625 scans, 618 in the table), so scan positions for folio counting must come from the scans table | data-property | sg, layout | confirmed | [Planning](#planning) |
| F14-P.7 | The pagexml-tools 0.9.1 wheel on PyPI has `extra` in `CHILD_PROPERTIES` and the fixed `PageXMLPage.stats`; NLP's and LotA's venvs hold an editable checkout that still reports 0.9.0, and NLP re-exports AS's version check, so raising `MIN_PAGEXML_TOOLS` needs the siblings first (UA-048) | open-issue | pagexml-tools, nlp, lota | confirmed | [Planning](#planning) |
| F14-P.8 | The 24 stale split files of F12-15z.10 are already gone from `data/text_layer_v4_full` (none of the 14 hand-pinned volumes has a `split_model`/`split_stats` file); `plan/CONTEXT.md` listed them as open | data-property | sg, text-layer | confirmed | [Planning](#planning) |
| F14-P.9 | REPUBLIC `republic_segmentation_v1` hand line ids come from another HTR run (0 of 104,652 exact box-id matches in 3265), so phase 14 compares with it by `page_id` only; it covers 13 of the 41 front-index volumes | data-property | sg, republic | confirmed | [Planning](#planning) |
| F14-20a.1 | pagexml-tools 0.9.1 retires both `assign_column_roles` workarounds (2 of 2): `extra` parentage and the empty-page coords fallback; the 44 tests of `test_analysis_page_structure.py` pass without them | method | sg | confirmed | [Step 20a](#step-20a-pagexml-tools-091-pin-data-link) |
| F14-20a.2 | 0.9.1 parses nested TextRegions (fixture 9999A_0003: 2 regions, `n1a` nested); 7 extract tests were updated, and the regression build gives 4,591 of 4,591 files equal to `19e-a`, so no real v5 table changes (0 nested-region failures in the 9,122-volume v5 run) | method | sg | confirmed | [Step 20a](#step-20a-pagexml-tools-091-pin-data-link) |
| F14-20a.3 | A region with neither Coords nor lines is still dropped on 0.9.1 (1 of the 2 known limits remains) | data-property | sg | confirmed | [Step 20a](#step-20a-pagexml-tools-091-pin-data-link) |
| F14-20b.1 | NLP's `unsupervised_segmentation` and `separator_sheets` are now thin re-exports of the phase-11 ports (6 + 3 names from AS, 4 + 3 kept in NLP) and an A/B against the byte-copied originals is identical: weight 0.0293559853524414, 41,810 / 41,810 change-point rows, 169 / 169 sheets, 12 / 12 `apply_rule` arrays over 20,912 pages | result | nlp, shared-library | confirmed | [Step 20b](#step-20b-nlp-switches-to-the-phase-11-ports) |
| F14-20c.1 | Of the 68 gold units (60 read + 8 extra test reserves), 52 (0.76) carry at least one folio locator and 16 carry none: 7 are first-line fragments whose numbers sit on continuation lines in other units, 4 are lemma headings or a page number, 5 are date-led rows of 3712; a unit-only locator extractor therefore misses about 1 in 9 real entries (7 of 64) | data-property | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.2 | The index points to the right folio: of 52 entries with a target, 50 have their matter on folio N (recto 16, verso 25, both 9); 1 is on the back of folio N-1 (3456 locator 98) and 1 is absent from N-1..N+1 (3471 locator 94; revised by F14-20c.8: 94 is an HTR artefact, not a locator) | result | sg | revised | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.3 | The matter is on the verso (the left page of the next scan) more often than on the recto: verso 25 vs recto 16 of 50 hits, so a link to the recto scan alone covers only 25 of 50 (recto + both, 0.50) and a link must carry both scans | result | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.4 | Printed folio numbers are found for 38 of 52 targets; 13 are counted from neighbours and 1 stays unclear (3256 locator 175: 173 on scan 197, 176 on scan 199, so one of 174/175 is not numbered); the HTR garbles the numbers (`121.` for 12, `99.` for 209, `57.` for 757, `324.` for 524) and the scan-to-folio offset drifts by 1 to 5 inside volumes (3352: 37 to 42; 3256: 26 to 23) | data-property | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.5 | Volume 3712 (1578-1595) has a chronological summary index with date prefixes ("14 Aug: Consent van Rijssel ...") and no folio locators in any of its 6 gold units (5 read + 1 extra), so it contributes 0 of 6 test targets; the front-index rule of F14-P.1 does not tell volumes with folio indexes from volumes with dated ones | negative-result | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.6 | Of the 40 test read rows only 28 have a target with `folio_evidence` other than `unclear` (needed 30); reading the 8 test reserves as extra gold (orchestrator decision) gives 35 of 48, dev has 16 of 20 (needed 15); the 40-row draw loses 12 rows to 4 headings, 5 rows of 3712, 2 first-line fragments and 1 unclear folio, so a 30-of-40 bar is tight at a 0.75 share | negative-result | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.7 | Some units merge two index columns into one text, so the entry words and the number do not belong together (3456 locator 98: "Misf: Calkoen ... Hamilton geeft over een missr. van de Hertog van Sleeswijk Holstijn ... 98."; 3590 locator 310: ",varenbeek, restitueeren 310."); 3 of the 60 gold rows are such mixed units and were judged only weakly | data-property | sg | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20c.8 | The user's check of 20 agent links (UA-050; 9 dev, 11 test) confirms the folio for 18 and the matter for 17 (0.85, Wilson 0.64–0.95), 1 unsure; all 11 test links are confirmed. The 2 wrong links are both in dev volume 3471 and both are entry-side errors, not folio errors (the HTR reads 94 and 452 at the top of the agent's rectos, so 20d's dev hits stand): in `Cunes94, 108.` the glued 94 is an HTR artefact and the reference is folio 108; `… te doen. 452.` merges two index columns and the matter is not on folio 452 (a false content match by the agent). One more column-merged unit (3308 locator 115) was matched correctly | result | sg, index, gold | confirmed | [Step 20c](#step-20c-gold-reads-locators-and-link-targets) |
| F14-20d.1 | Leave-one-out interpolation of a chain anchor's scan position is exact for 21,010 of 21,334 (0.9848; planning 0.9858) with the library map; lowest 3712 0.847, 3611 0.925, 3265 0.928 | result | sg, page-numbering | confirmed | [Step 20d](#step-20d-folio-map) |
| F14-20d.2 | Bar F (folio N located on the right scan, frozen rules): test 33 of 35 (0.943, read rows alone 27 of 28 = 0.964) misses the 0.95 bar by one row; dev 16 of 16; printed 24/25 and interpolated 9/10 on test | result | sg, index, page-numbering | confirmed | [Step 20d](#step-20d-folio-map) |
| F14-20d.3 | Misses are number-reading problems, not interpolation failures: a duplicated printed number (3590: `23` on 0070 and 0072, chain keeps the later) and skipped printed numbers (3611: 483 -> 487 over two scans, 486 not typed `page_number`); of 30 sampled LOO misses 28 are off by one scan, 7 skip/duplicate, 2 chain error, 21 pagination-mode (3712) | result | sg, page-numbering | confirmed | [Step 20d](#step-20d-folio-map) |
| F14-20d.4 | 3712 is foliated on rectos (number +1 per recto, chain 348..684) but verso years (1578, 1593, ...) pass the number pattern, lowering the recto share to 0.748 so the map is built as `pagination` on side positions and LOO falls to 0.847 (recto-only 0.946); it has 0 folio-locator targets, so links are unaffected | data-property | sg, page-numbering | confirmed | [Step 20d](#step-20d-folio-map) |
| F14-20d.5 | The page-number chain covers 21,211 of 28,042 rectos after the front index in the 40 foliated volumes (0.7564, planning 0.7562); the remaining quarter is located by interpolation | data-property | sg, page-numbering | confirmed | [Step 20d](#step-20d-folio-map) |
| F14-20e.1 | Locator variants on the 20 dev rows: L1 P 1.000 R 0.964 (27/27, 27/28), L2 = L3 = L4 P 0.966 R 1.000 (28/29, 28/28); F1 0.9818 vs 0.9825, so by the tie rule L2 is chosen, although the dev rows hold no date, ordinal or month case that would separate L2 from L3 | result | sg, index | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.2 | Bar L (test, read + extra, L2) NOT met on precision: P 61/66 = 0.924 (< 0.95), R 61/61 = 1.000; read rows alone P 50/55 = 0.909, R 50/50; all 5 false positives are date or count numbers or a heading (3712 day numbers 3, "11 Schip" 1, an index page-number heading 1), no locator is missed | result | sg, index | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.3 | Bar E (gold locator extracted and folio on the right scan, L2) met: 33/35 = 0.943 (read rows alone 27/28 = 0.964); the locator is found in 35/35, both misses are the frozen map's (3611 locator 486 and 3590 locator 23, the bar-F misses of 20d), so the folio map, not the locator rule, limits end-to-end accuracy. *Revised by the review:* bar E counts gold targets that are found (a recall); the 5 false locators of bar L add links that E cannot see, so the share of emitted links that point to the right scan is about 0.924 × 0.943 ≈ 0.87 on the test rows (estimate, F14-R.2) | result | sg, index, page-numbering | revised | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.4 | By mistake the test ran once with L3 (module default not yet set) before the intended L2 run: L3 gave P 60/62 = 0.968, R 60/61 = 0.984 (bar L met) and bar E 32/35; it drops the 3 date numbers but also a true locator ("Graaf van Denhof 354.": `amount_before` reads "f " in "Denhof" as an amount sign). L3 was not chosen on dev, so this is post-hoc and not a pass | method | sg, index | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.5 | Dev build of 41 volumes: 72,558 entries, 102,588 locators (1.41 per entry, 54,905 entries with at least one), 101,282 linked (printed 73,984, interpolated 26,259, extrapolated 1,039), 1,306 outside the map; 19.4 s single-process (0.47 s per volume) | result | sg, index | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.6 | Silver: 118,464 of 260,497 entry words (>= 6 letters) are on the target sides (0.455) against 26,061 (0.100) on folio + 3; REPUBLIC is no discriminating check, since 39,160 of 39,165 links (0.9999) and 39,076 controls (0.9977) land on a side that has a resolution | result | sg, index, republic | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.7 | Of 20 random dev-build links, 14 have an entry word on the target sides, 3 cannot be tested (entry words under 6 letters) and 3 name-only entries have no word match (HTR spelling); the lowest word shares (0.21 to 0.37) are late-18th-century volumes (3630, 3611, 3590, 3587, 3623) with generic lemmata ("Brieven van Notificatie") listing up to 20 folios each, whose words appear at some locators only | data-property | sg, index | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.8 | Six of the 40 foliated volumes end their chain with an anchor above their scan count (3126 1591 on 369 scans; 3265 1659; 3290 1674; 3307 1683; 3320 1689; 3522 3522), apparently a year or an inventory number read as a page number; the map is frozen, so folios near the end of these volumes interpolate towards a false anchor and L4's range filter cannot work there | data-property | sg, page-numbering | confirmed | [Step 20e](#step-20e-locators-links-and-index_links_v1) |
| F14-20e.9 | The full run of `index_links_v1` (L2) on the v5 full layer (UA-052) considers 8,856 inventories, finds a front index in 412 (408 foliated, 4 paginated) and fails none: 629,912 entries, 914,994 locators, 902,031 linked (0.986), in 247 s; the 41 dev volumes come out identical to the 20r dev build in every `volumes.tsv` column, and 371 front-index volumes lie outside the local dev layer. Three volumes link under 0.6 of their locators: 3712 (dated index, F14-20c.5) and two foliated volumes whose index cites folios outside their chain (3681: 129 of 465, chain 32–237; 3601: 982 of 1,827, chain 2–649) | data-property | sg, index | confirmed | [After the review](#after-the-review-full-run-and-publication) |
| F14-20e.10 | Year-like or impossible chain ends are more frequent in the full layer: by an estimated rule (last anchor ≥ 1000 and more than 2.5 times the chain length, which catches all 6 dev cases of F14-20e.8) 22 of 412 volumes are affected (dev: 6 of 41), holding 80,304 of the 914,994 locators; 16 of the 22 end at a number between 1480 and 1699. The full layer's scan counts are not local, so the rule is not checked against the scan count | open-issue | sg, page-numbering, index | confirmed | [After the review](#after-the-review-full-run-and-publication) |
| F14-R.1 | The 20c gold deviation is sound and fairly reported: the 8 extra test rows are the draw's own reserves (one per test volume, seed 20261129), read with the same procedure after the 28-of-30 shortfall and before any phase-14 code (`entry_gold.tsv` last written 14:32:58, `folio_map.py` created 14:35:27), so read + extra is the bar population and 33/35 is the bar F result; the read-only 27/28 is not an alternative pass, and 35 rows cannot separate 0.943 from 0.95 (Wilson 0.81–0.98). The target recto carries the locator as a typed page number in 35 of the 38 `printed` rows (the other 3: once inside a text line, twice not in the typed segments) and in 4 of the 13 `neighbours` rows; the gold has no human check yet (UA-050) | method | sg, index, gold | confirmed | [Review](#review) |
| F14-R.2 | L2 stays the default: the dev tie rule chose it before any test run, and adopting L3 after its mistaken test run (P 60/62) would be a choice on test. 4 of L2's 5 test false positives (`1 Febr`, `10 Mey`, `14 Aug`, `11 Schip`) are a number followed only by a space and a capitalised word, which P9 did not include (`N[.,:;]` then a capital) but the brief's rule does; in the dev build 901 of 102,588 locators (0.9%) are of that kind, and of 20 read about 7 are locators and 11 are dates or counts. A v2 rule must be chosen on fresh gold | negative-result | sg, index | confirmed | [Review](#review) |
| F14-R.3 | The year-like end anchors of F14-20e.8 also link years: in the 6 volumes, 94 locators above the last real anchor (years cited in entries such as 1650 and 1653 in 3265, and garbled numbers) are placed on the last one or two scans, 90 of them with confidence `high`, because `confidence_band` looks only at the scan distance to an anchor; their folio-map tables have 1,597–3,528 rows for 369–1,031 scans. Over the 41 volumes, 949 locators exceed the volume's scan count (impossible folios) and 129 of them are linked | open-issue | sg, page-numbering, index | confirmed | [Review](#review) |
| F14-R.4 | Link-level silver: of 94,056 located links with a control, 70.7% have at least one entry word (≥ 6 letters) on the target sides against 21.8% on folio + 3, and the control wins in 2.6%; for 15,209 single-locator entries with ≥ 3 words, folio N is the best or tied-best of N−3..N+3 for 12,298 of the 14,211 with a hit (0.865), N+1 for 682, N−1 for 249. Read cases: N+1 is mostly matter running on from N's verso (3522 locator 161: `kis-`/`jes met Porcelijn … Doesburg` at the top of folio 162), N−1 the facing verso of the opening (3280 locator 121: Maurice de Nassau on 0167 verso, beside 121r), like 20c's `adjacent` row | result | sg, index | confirmed | [Review](#review) |
| F14-R.5 | `extrapolated` links are mostly noise: 939 of the 1,039 are single-digit numbers; 402 locators have a letter right before the digits (`U1`, `f300`, `Oorlog1789`, `Thesing7.`), at least 34 of 40 read are HTR fragments or amounts; a left word boundary would drop them but also the dev gold locator of `Cunes94` | data-property | sg, index | confirmed | [Review](#review) |
| F14-R.6 | `scripts/build_index_links.py` was ignored by git (`/scripts/*` in `.gitignore`), although the tracked test `test_builder_two_volumes_and_resume` loads it and UA-052 runs it; fixed in the review with `!/scripts/build_index_links.py` | bug-fixed | packaging, index | confirmed | [Review](#review) |
| F14-R.7 | Every manifest built in `.venv` records archival-structures 0.14.1 (`package_info` reads the stale `archival_structures-0.14.1.dist-info` of the editable install), including the v5 dev build `19e-a`, the published v5 pilot, the v5 full run and `index_links_v1`; `git_head` identifies the code correctly | open-issue | provenance, packaging | confirmed | [Review](#review) |

## Steps

### Planning

The planner (Opus, 2026-10-29) measured the front-index volumes and drew the read list
(`reports/data/phase-14-index-links/planning/draw_reads.py` → `front_index_volumes.tsv`, `reads_entries.tsv`, seed
20261129), prototyped the folio chain (`folio_proto.py` → `folio_proto.tsv`) and measured the LOO floor under step 20d's
definition (`loo_check.py`). Explorer facts (page numbering API, GT, REPUBLIC, NLP shim, pagexml-tools sites) are in
`plan/phase-14-index-links/PROGRESS.md`. Findings F14-P.1–P.9.

### Step 20a: pagexml-tools 0.9.1 pin, data link

**Status: done** (Sonnet 5.5, 2026-10-29; resumed after UA-049 (a)). The first pass stopped at part 2.4 (nested regions parse on 0.9.1, 7 extract tests failed); after UA-049 (a) the regression build ran and the 7 tests were updated. Notes: `reports/data/phase-14-index-links/step-20a/notes.md`.

**What changed.** `.venv` has pagexml-tools 0.9.1 (`poetry.lock` still records 0.9.0). `pyproject.toml` requires
`>=0.9.1`, `MIN_PAGEXML_TOOLS = (0, 9, 1)`, the two provenance tests assert it. In `analysis/page_structure.py`
`assign_column_roles` lost both workarounds (the `set_as_parent(extra)` loop and the `page_coords` hull fallback); the
module docstring is updated. `build.py` and `docs/text_layer.md` state `>=0.9.1`. Link `data/republic_segmentation_v1`
created; shared-data row has AS under *used by*.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | 0.9.1 in `.venv`, pin, workarounds | 0.9.1; pin and constant (0, 9, 1); workaround 1 dropped (`test_page_parentage` passes), workaround 2 dropped (44 of 44 in `test_analysis_page_structure.py`); 2 of 2 dropped |
| 2 | AS suite 1,358 passed, 11 skipped | 1,375 passed, 11 skipped (the 17 extra tests come from other steps in the working tree; 0 failures) |
| 3 | `compare.py` 19e-a vs 20a-pin | **4,591 equal, 0 differ, 0 missing**; `audit.json` `n_fail_judged` 0, `n_pass` 4,196 (not 4,192: 20a-pin's audit has 4 extra layer-level v5 invariants under `1.01.02/v5`, all pass; `n_fail` 47 as in 19e-a) |
| 4 | protect diff | shared-data `README.md` (my edit) and 2 LotA `test-staged-fixture-2.5b` files whose mtime changed (same size; LotA tests rewrite them) |
| 5 | link and used-by | `ls data/republic_segmentation_v1/lines` lists `inv=10 inv=12 inv=3097`; README cell edited |
| 6 | siblings | both venvs 0.9.1; NLP 66 passed; LotA 380 passed, 1 skipped, 1 xfailed (brief expected 381 passed) |

**Error analysis.** (First pass.) Read the 7 failing tests' assertions against the 9999A fixtures. With 0.9.1 the scan `NL-HaNA_1.04.02_9999A_0003.xml` (region `n1` containing `n1a`) parses: `regions` 6 not 4, line rows 7 not 6, `n_scans` 4 not 3, `nested_text_regions` 1, no `parse_errors`. All 7 failures had this one cause; none came from the dropped workarounds. Resolution: the full v5 run (9,122 volumes) and `19e-a` have 0 scans failing on nested regions (only 6 METS `doc.xml` parse errors), and the regression build confirms 0 differing files, so only the fixture changes. The 7 tests now assert the 0.9.1 values (`test_nested_regions_fail_in_pagexml` became `test_nested_regions_parse`); `docs/text_layer.md` section renamed to *Known pagexml-tools 0.9.1 limits*. Old text kept below for the record.
`NL-HaNA_1.04.02_9999A_0003.xml` (region `n1` containing `n1a`) parses: `regions` 6 not 4, line rows 7 not 6, `n_scans` 4
not 3, `nested_text_regions` 1, no `parse_errors` entry. `known_limits_check.py` confirms: nested regions parse
(`page.stats['text_regions']` 2); a region with neither Coords nor lines is still dropped. All 7 failures are this one cause; no
failure comes from the two dropped workarounds. Whether real 1.01.02 scans have nested regions (so v5 tables would
change) is exactly what the skipped regression build would show.

**Findings.** F14-20a.1 to .3 are in the Findings table.

### Step 20b: NLP switches to the phase-11 ports

**What changed (NLP only; no AS code).** Originals saved byte for byte (`cp -p`) in
`NLP/output/as-phase-14-originals/src/`. `src/startpage/unsupervised_segmentation.py` imports `MAX_SEGMENT_LENGTH`,
`_geometric_length_penalty`, `segment_dp`, `calibrate_length_penalty_weight`, `textiling_depth_scores`,
`predicted_start_rate` from `archival_structures.stream_analysis.overview.subsequence_detection`; `build_similarity_sequences`,
`ChangePointResult`, `boundaries_to_page_results`, `run_unsupervised_segmentation` keep their bodies. `src/startpage/separator_sheets.py`
imports `is_separator_sheet`, `detect_sheets`, `apply_rule` from `archival_structures.analysis.separator_sheets`, aliases
`_SHEET_RX = VOC_SHEET_PATTERN`, `MAX_SHEET_CHARS = VOC_MAX_SHEET_CHARS`, and keeps `_scan_no`. New
`NLP/scripts/verify_as_segmentation_shim.py` (modes current / originals / both, subprocess per mode). Data:
`reports/data/phase-14-index-links/step-20b/` (`pre18b/`, `pre18c/`, `verify/`, `verify_summary.txt`).

**Results against the acceptance criteria.**

| Criterion | Result |
|---|---|
| 1. Pre-edit parity (phase-11 scripts, before any edit) | 18b: weight NLP = AS = 0.0293559853524414; `segment_dp` 44 / 44, textiling 22 / 22, change points 41,810 rows, `predicted_start_rate` 22 / 22, 0 mismatches. 18c: `is_separator_sheet` 20,912, `detect_sheets` 169, 12 `apply_rule` configs and `next_scan_id` 20,912, 0 mismatches |
| 2. Verify `both` (shims vs originals) | exit 0, 15 / 15 PASS: weight same repr; change-point frames `DataFrame.equals` after sort, 41,810 rows; sheet sets 169 = 169; `apply_rule` none/sheet/next/dict x 0.3/0.5/0.7 each 20,912 rows, 0 differ |
| 3. Names importable | all 10 + 6 original names resolve; the five importing scripts (`evaluate_unsupervised_segmentation`, `evaluate_generale_missiven`, `count_separator_sheets`, `evaluate_separator_sheet_rule`, `evaluate_separator_convention_inference`) import cleanly |
| 4. Tests | NLP 66 passed; AS `test_subsequence_detection_unsupervised.py` + `test_analysis_separator_sheets.py` 51 passed (the brief said 25) |

**Error analysis.** Nothing differed, so there were no rows to classify. Instead I read the output the rules produce.
The 169 sheets: 140 in 2555, 13 in 2775, 8 in 1274 (`NL-HaNA_1.04.02_1274_0686`, ...), the same concentration the
module docstring describes. At threshold 0.5 on the fixed random probabilities the rule leaves 10,416 starts
(`none`) and 10,384 (`sheet` and `next`): 32 starts are merged into units. The deliberate AS differences
(unknown convention raises `ValueError`; `sorted(sheets)` order) are not exercised by NLP's callers, which pass
only none/sheet/next or a dict of sheet/next, and gave no diff here. A convention dict missing an inventory still maps to `none`.

**Findings.** F14-20b.1 is in the Findings table.

### Step 20c: gold reads (locators and link targets)

**What changed.** No library code. Added `reports/data/phase-14-index-links/step-20c/`: `show_entry.py` and `show_scan.py`
(print-only readers), `entry_gold.tsv` (60 rows, every label decided by reading the entry and the target folio; no script
decided a label), `summary.py`, and a confirm page (`build_check_page.py` -> `check_page.html`, `check_items.json`, UA-050).
Brief: [`step-20c-link-gold-reads.md`](../plan/phase-14-index-links/step-20c-link-gold-reads.md). No reserve was used: no read
unit was `not entry` or `unreadable input`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. `entry_gold.tsv` rows | 60, each `read_id` once | 68 (60 read + 8 extra), 68 unique; verdicts `entry` 64, `heading` 4 |
| 2. rows with a target and `folio_evidence` != `unclear` | test >= 30, dev >= 15 | test 28 on the 40 read rows (not met), **35 over read + extra (met)**; dev 16 |
| 3. confirm page | 20 items, links, JS syntax, UA | 20 items, links on every item, syntax ok, UA-050 |

Criterion 2 failed on the 40 test read rows (28; 29 rows have a target, one with unclear numbering) and the step stopped
as the brief says. **Deviation (orchestrator decision, 2026-10-29):** the 8 test reserve rows (ids 30, 36, 42, 48, 54, 60,
66, 72) were then read as extra gold, with the same procedure and rubric, `used_for` = `extra`, before any phase-14 code
ran and without replacing any read row. Over read + extra rows, test has 35 rows with a target and evidence other than
`unclear`: criterion 2 is met. The dev reserves were not read. The confirm page keeps its 20 items as drawn from the 60
read rows. The numbers below that are not labelled "read" include the extras; the paragraphs from "Per volume" on describe
the 60 read rows unless a count is given for 68.

`summary.py` output:

```
rows: 68 unique read_id: 68
reserves used as replacement: 0 | extra test reserves: 8

== dev: 20 rows
 verdict: {'entry': 20}
 locators 0/1/>=2: 4 11 5 | total locators 28
 rows with a target: 16 | and folio_evidence != unclear: 16
 folio_evidence: {'neighbours': 4, 'printed': 12}
 mean scans_opened (rows with target): 5.3
 content: {'verso': 7, 'recto': 8, 'absent': 1}
 index accuracy (recto+verso+both)/rows with target: 15/16 = 0.94
== test:read: 40 rows
 verdict: {'entry': 36, 'heading': 4}
 locators 0/1/>=2: 11 20 9 | total locators 50
 rows with a target: 29 | and folio_evidence != unclear: 28
 folio_evidence: {'unclear': 1, 'printed': 21, 'neighbours': 7}
 content: {'verso': 15, 'recto': 7, 'both': 6, 'adjacent': 1}
 index accuracy (recto+verso+both)/rows with target: 28/29 = 0.97
== test:extra: 8 rows
 verdict: {'entry': 8}
 locators 0/1/>=2: 1 5 2 | total locators 11
 rows with a target: 7 | and folio_evidence != unclear: 7
 folio_evidence: {'printed': 5, 'neighbours': 2}
 content: {'verso': 3, 'recto': 1, 'both': 3}
 index accuracy: 7/7 = 1.00
== test (read + extra): 48 rows
 verdict: {'entry': 44, 'heading': 4}
 locators 0/1/>=2: 12 25 11 | total locators 61
 rows with a target: 36 | and folio_evidence != unclear: 35
 folio_evidence: {'unclear': 1, 'printed': 26, 'neighbours': 9}
 content: {'verso': 18, 'recto': 8, 'both': 9, 'adjacent': 1}
 index accuracy (recto+verso+both)/rows with target: 35/36 = 0.97
== all: 68 rows
 locators 0/1/>=2: 16 36 16 | total locators 89
 rows with a target: 52 | and folio_evidence != unclear: 51
 folio_evidence: {'neighbours': 13, 'printed': 38, 'unclear': 1}
 content: {'verso': 25, 'recto': 16, 'absent': 1, 'both': 9, 'adjacent': 1}
 index accuracy (recto+verso+both)/rows with target: 50/52 = 0.96 (49/51 excluding unclear numbering)
```

Per volume (rows with a target; in brackets the read rows alone, the extra reserve is the difference; `scans_opened` is my count of distinct scans looked at for the entry,
including the first probes of a volume, so it is approximate):

| Split | Inv | Scans | Targets | Locators | Content | Scan to folio offset |
|---|---|---|---|---|---|---|
| dev | 3254 | 625 | 3 | 3 | verso 2, recto 1 | 29 |
| dev | 3471 | 517 | 3 | 4 | verso 1, recto 1, absent 1 | 56, 55 after pos ~480 (scan ids skip: pos 500 is id 0501) |
| dev | 3394 | 776 | 5 | 7 | verso 2, recto 3 | 28, 27 after pos ~560 |
| dev | 3308 | 596 | 5 | 14 | recto 3, verso 2 | 26, 25 after pos ~110 |
| test | 3256 | 689 | 5 (4 read) | 16 | verso 3, recto 1, both 1 | 26 down to 23; gap at 198 |
| test | 3456 | 582 | 5 (4) | 5 | verso 2, recto 2, adjacent 1 | 49 |
| test | 3401 | 634 | 5 (4) | 8 | both 3, recto 1, verso 1 | 31 |
| test | 3611 | 1038 | 6 (5) | 6 | recto 3, verso 2, both 1 | 59, 58 near pos 930 |
| test | 3320 | 1031 | 5 (4) | 10 | verso 5 | 40 |
| test | 3712 | 345 | 0 (0) | 0 | - | dated index, no folio locators |
| test | 3590 | 1205 | 5 (4) | 8 | verso 2, both 3 | 47 |
| test | 3352 | 1028 | 5 (4) | 8 | recto 1, both 1, verso 3 | 37 to 42, drifting |

**Error analysis.**

- *What was read:* all 60 units with their side (the lines above and below), and for the 45 with a target both pages of
  folio N (recto of the target scan, verso of the next scan); for the two misses also the adjacent folios (3471 N-1..N+1,
  3456 N-1). The reading was by me, line by line, on the layer `19e-a`; no scan image was opened (text only, as the
  brief says).
- *Categories (60 units):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | one locator | 31 | `Heer Crouse 383.` (3254) |
  | two or more locators | 14 | `weduwe Speelman 31. 146. 162, 284, 289.` (3320, five) |
  | first-line fragment, locators outside the unit | 7 | `Overquartier van Gelderlant.` (3254; `sum 469, 534.` is another unit) |
  | heading / page number | 4 | `Admiraliteriter.` (3256), `11.` (3456), `Regensburg.` (3352), `Gouverneurs v: Provincien.` (3712) |
  | date-led rows, no folio locator | 4 | `14 Aug: Consent van Rijssel Douay en Orchies inde continuatie.` (3712) |

  Non-locator numbers seen in the units: count 1 (`van 11 Schip de Tason`, 3590), date 3 (`1 Febr`, `10 Mey`, `14 Aug`, all
  3712), garbled 1 (`wijnijstg6`, 3308, not taken), glued 1 (`Cunes94`, 3471, taken as 94), amount 0, ordinal 0, year 0.
- *Content of the 52 targets (45 read + 7 extra):* recto 16, verso 25, both 9, adjacent 1, absent 1 (0.96 found on folio N).
  The 7 extra rows add nothing unexpected: all found on folio N (verso 3, both 3, recto 1); 3 of them have the matter in a
  margin note on the verso; one (3712) is another dated fragment with no locator. The categories and hard cases below were
  tallied on the 60 read rows.
- *Hard cases (verbatim, with my decision):*
  1. 3256 `Coms. Ruse 175, 182, ...`: scan 197 prints 173, scan 199 prints 176, scan 198 prints nothing. "Comr. Ruse" is a
     margin note on the back of scan 198's recto, so I took that scan as folio 175, but one of 174/175 is unnumbered,
     so `folio_evidence` is `unclear` (the row is excluded from the counts above).
  2. 3456 `Misf: Calkoen, houdende de Noodsa, Hamilton geeft over een missr. van de Hertog van Sleeswijk Holstijn, ... 98.`:
     folio 98 (scans 0147 recto / 0148 verso) is about Curacao; the Hamilton credential is on the back of folio 97
     (0147 verso). Decision: `adjacent`, `147:verso`; the unit merges two columns.
  3. 3471 `Abraham d'Silva Cunes94, 108.`: no Cunes or Silva on folio 94 or 93-95. Decision: `absent` (the number is glued to
     the name and taken as 94).
  4. 3308 `Wat bij Gelderlandt is ingehouden uijt de quote van Zee saken tot den Rijn en ijsel 496`: 496 is printed on
     scan 0521; the verso has the quotes for the building of 18 ships but names neither Gelderland nor the Rijn. Decision:
     `verso`, noted weak.
  5. 3590 `,varenbeek, restitueeren 310.`: a column-mixed fragment; neither word is on folio 310, but the neighbouring index
     lines say `Verhellouw in S. Bosch` and the margin of the verso says `Amps. van Verhellou in S Bosch`. Decision: `verso`,
     noted weak.
  6. 3401 `Holland, fluijt De jonge Floris 209.`: the page number reads `99.` (garbled 209); 207 is printed two scans earlier.
     Decision: `neighbours`, matter on the recto margin.
- *Conclusion:* the index itself is reliable as a pointer (50 of 52), so the gold is limited by the extraction side: the
  unit rule cuts entries at lines, headings and fragments carry no locator, and the page numbers on the scans are often
  garbled or missing (14 of 52 not printed), so the folio map of 20d must count scans from several anchors and tolerate
  drift of up to 5 scans over a volume. The target is two scans (recto of the folio scan, verso of the next).
- *Open point:* 3712 has a dated index; whether its dates can be resolved to pages is for 20d/20e to decide, not here.

**Findings:** F14-20c.1, F14-20c.2, F14-20c.3, F14-20c.4, F14-20c.5, F14-20c.6, F14-20c.7, F14-20c.8.

**Open issues.** none. The confirm page (UA-050) covers 20 of the 44 read rows with a target; the 8 extras are not on it.

**User check (UA-050, 2026-10-29).** The user answered all 20 items on the confirm page (with image crops and the full
page text added on their request); `check_verdicts.json`, summary by `check_summary.py` -> `check_summary.txt`.
Question 1 (folio N on the agent's recto): 18 Yes, 2 No. Question 2 (matter on folio N): 17 Yes, 2 No, 1 Unsure. All
three non-Yes items are in dev volume 3471; the 11 test items are all Yes/Yes, so the test bars of 20d and 20e rest on
gold whose checked part holds. The items, read one by one:
- 7 (3471, `Abraham d'Silva Cunes94, 108.`, locator 94, agent `absent`): the 94 glued to the name is an HTR artefact;
  the reference is folio 108 (user). The agent's scan 0150 does carry the printed 94, so the folio-to-scan fact is
  right and the error is the locator. This explains the one `absent` row of F14-20c.2. Every 20e variant took the 94
  (no row in `step-20e/select_diffs.tsv`), so the correction costs each variant one dev false positive and does not
  change the choice of L2.
- 10 (3471, `binnen de Stadt van't Zas van gevergtevierd te doen. 452.`, agent `recto`): the unit merges two index
  columns (F14-20c.7) and the matter is not on folio 452. The agent's match (`Pieter de Munck en An. Cardon ... Hulst
  ... Eed`) was false; scan 0508 does carry the printed 452.
- 9 (3471, `... Zeeland over Ails: 71.`, agent `verso`): Unsure; the user sees the resolution on the second scan but no
  link to `Ails`. The agent's evidence is a contagious disease at Messina, so `Ails` may be a garbled `Sicilie`;
  unresolved.
- 23 (3308): also a column-merged unit; the right-hand column's entry is the one on the folio (Yes/Yes).
- 13 (3394): Yes/Yes, but the user places the matter on the first scan's recto where the agent said verso; links carry
  both scans, so nothing depends on it.

`entry_gold.tsv` is not edited: it is the agent's read as the steps used it, and this check corrects it here.

### Step 20d: folio map

**What changed.** New `archival_structures/analysis/folio_map.py` (`NUMBER_PATTERN`, `FolioMapParams`, `FolioTarget`,
`FolioMap`, `build_folio_map`) and `analysis/index_links.py` (`front_index_run`, `VolumeInputs`, `read_volume_inputs`), with
17 tests (`tests/test_analysis_folio_map.py`, `tests/test_analysis_index_links.py`) and a changelog entry (no sibling
import affected). Rules exactly as in the brief, no parameter search. Scripts and outputs in
`reports/data/phase-14-index-links/step-20d/`: `eval_folio.py` (+ `eval_folio.log`), `volumes.tsv`, `loo.tsv`,
`gold_eval.tsv`, `loo_misses.py` (+ `loo_misses.txt`). Brief: [`step-20d-folio-map.md`](../plan/phase-14-index-links/step-20d-folio-map.md).

**Results against the acceptance criteria.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. `front_index_run` | 41 volumes, equal `front_first`/`front_last` | **met**: 41 of 41 (also `n_after` equal) |
| 2. LOO pooled exact share | >= 0.97, within 0.002 of 0.9858 | **met**: 21,010 / 21,334 = **0.9848** (planning 0.9858, difference 0.0010); lowest 3712 0.8473 (172/203), 3611 0.9250, 3265 0.9281, 3283 0.9477, 3590 0.9511 |
| 3. Bar F (test, read + extra) | >= 0.95 | **not met: 33 / 35 = 0.9429** |
| 4. Tests | green | new 17 passed; full suite 1,375 passed, 11 skipped (20a's extract tests were already updated) |

Bar F detail. Test read rows only: 27 / 28 = 0.9643. Dev: 16 / 16 = 1.0000. By method, test: printed 24 / 25 (0.960),
interpolated 9 / 10 (0.900), extrapolated 0 / 0. Dev: printed 14 / 14, interpolated 2 / 2. Modes: 40 volumes
`foliation`, 1 `pagination` (3712), 0 `none`. Chain coverage in the 40 foliated volumes: 21,211 anchors / 28,042 rectos
after the front index = 0.7564 (planning 0.7562).

**Negative result under the pre-registered bar** (35 rows, 2 misses; the bar needs at most 1). Per the brief the rules are
not changed. 20e waits for the user (STATUS.md). The two misses are one chain/duplicate and one skip case, both with a
gold target that is right (below); a single miss more or less would have decided the bar, so 35 rows cannot separate 0.943
from 0.95 (95% interval Wilson about 0.81 to 0.98).

**Error analysis.** Read all 2 bar F misses (dev has none) with `show_scan.py`, the 41 LOO misses of dev volumes and 3712
(30 sampled with `random.Random(20261131)`, printed with chain neighbours and the page-number texts of the neighbouring
scans/sides in `loo_misses.txt`), and 3712's numbering.

*Bar F misses.*
- Read 62, 3590 locator 23, target 0070, predicted 0072 (printed, `high`). The page numbers of positions 70..73 read
  `23. / 244. / 23. / 26.`: 23 is printed on 0070 (right) and again on 0072 (an HTR misread of 25; 0071 reads `244` for 24).
  The patience-sorting chain keeps a strictly increasing sequence and, for the tied 23, the later scan. Category: **chain
  error** (a duplicated number; the tie goes to the later occurrence).
- Read 48 (extra), 3611 locator 486, target 0545, predicted 0546 (interpolated, `high`). Printed numbers around it:
  `483.` on 0544, none on 0545, `487.` on 0546. The number 486 is on 0545's recto but inside a text line (`... den Raadven
  486.`), not typed `page_number`, so 0545 is not an anchor; 483 -> 487 over two scans (484 and 485 never printed) puts 486
  at 545.5, rounded to 546. Category: **skip/duplicate** (numbering skips; the printed number exists but is not typed).

*LOO misses, 30 sampled of 41 (3254 8, 3394 1, 3471 1, 3712 31; dev volume 3308 has none).*

| Category | n | Example |
|---|---|---|
| other: pagination mode on a foliated volume (3712, side positions) | 21 | N=371 at side 85, predicted 86: rectos alternate with year-labelled versos, so the same number step covers 2 or 3 sides |
| skip/duplicate | 7 | 3254 N=191/192 (`192.` on 0221 and `192` on 0222, chain keeps 0222), N=309/310 (same pattern), N=172 (168@197, 172@200: three numbers on two scans), 3394 N=540 (539 skipped), 3712 N=647 (`647.` on two rectos) |
| chain error | 2 | 3471 N=457 (the chain ends `457@512, 485@515`; `485` is a misread, the true next number was not printed), 3712 N=414 (`414.` where 412 was meant, then `413` on the next scan; error 5 scans) |
| blank scans, restart, beyond chain, gold error | 0 | |

Misses are almost all off by one scan (28 of 30; the largest are 3712 N=414 with +5 and N=490 with -2). Duplicated printed
numbers (the HTR reading the same number on two consecutive rectos) cause 5 of the 7 skip/duplicate cases and, in the
chain, make the earlier or later scan win by the tie rule; they also explain why LOO misses come in pairs (N and N+1).

*3712.* It is the volume of dated entries (F14-20c.2) and its numbering is a **foliation**, not a pagination: the
rectos carry a number that rises by one per recto (scan 103 `428.`, 104 `429`, 105 `430.`, ...; chain 348..684 over 345
scans, no restart, offset about -336 as planning found). Many versos carry a **year** (`1578`, `1593`, `1594`, 110 versos have a page-number segment, mostly years), which the number pattern (1 to 4 digits) takes as a page number. That lowers the recto share to 0.7476 (321
rectos of 431 anchors), below the 0.9 threshold, so the map is built in `pagination` mode on side positions
(533 sides on 345 scans, irregular), and the LOO share falls to 0.8473 (planning, recto-only: 0.946). The map for 3712
is therefore unreliable at the scan level (+-1 side). It does not affect bar F (0 test targets in 3712) or 20e's
folio-locator links, as the entries of 3712 have none. The recto-share rule was not changed (frozen); the fix would be a
number pattern limited to 3 digits or a recto/verso test on the year-like values (follow-up).

**Findings.** F14-20d.1 to .5 are in the Findings table.

**Open issues.** Bar F failed by one row (33/35). 20e is blocked until the user decides (STATUS.md, UA-051).

### Step 20e: locators, links and index_links_v1

**What changed.** `archival_structures/analysis/index_links.py` gained `Locator`, `LocatorParams`, `SG_MONTHS`,
`SG_LOCATOR_PARAMS`, `DEFAULT_LOCATOR_VARIANT` (= `L2`), `LINK_COLUMNS`, `extract_locators`, `volume_links`;
new `scripts/build_index_links.py` (layer `NL-HaNA_1.01.02_index_links_v1`; 4 more tests in
`tests/test_analysis_index_links.py`, 9 in the file, full suite 1,380 passed, 11 skipped); `docs/index_links.md`;
changelog entry; UA-052. Scripts and outputs: `reports/data/phase-14-index-links/step-20e/` (`select.py`, `test_bars.py`,
`silver.py`, `sample_links.py`; `select.tsv`, `select_diffs.tsv`, `test_rows.tsv`, `bar_e.tsv`, `silver.tsv`,
`silver_words_links.tsv`, logs). Dev build `data/index_links_dev/20e-a/`. Folio map and gold: frozen / as in 20c, 20d; the
run is on the frozen map after the bar F miss (UA-051 (a)). Brief: [`step-20e-index-links.md`](../plan/phase-14-index-links/step-20e-index-links.md).

**Selection (20 dev rows, 28 gold locators).**

| Variant | TP | predicted | P | R | F1 |
|---|---|---|---|---|---|
| L1 | 27 | 27 | 1.0000 | 0.9643 | 0.9818 |
| L2 | 28 | 29 | 0.9655 | 1.0000 | 0.9825 |
| L3 | 28 | 29 | 0.9655 | 1.0000 | 0.9825 |
| L4 | 28 | 29 | 0.9655 | 1.0000 | 0.9825 |

Choice by the rule (max F1, ties to the lower variant): **L2**. The only difference between variants on dev: L1 misses
66 in "de weduwe Renhausen, 66. Johan Oirrock 108, 124. 162." (mid-entry locator), L2-L4 take the garbled "wijnijstg6"
(a "6" glued to a word) as a locator. The dev rows hold no date, ordinal, month or amount case, so they cannot separate
L2 from L3; the tie is not evidence that L3's extra rules are useless.

**Results against the acceptance criteria (test: 48 rows, read + extra; read-only in brackets).**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Selection | table, rule | L2 (above) |
| 2a. Bar L precision | >= 0.95 | **not met: 61 / 66 = 0.9242** (read only 50 / 55 = 0.9091) |
| 2b. Bar L recall | >= 0.90 | met: 61 / 61 = 1.0000 (50 / 50) |
| 2c. Bar E | >= 0.90 | **met: 33 / 35 = 0.9429** (27 / 28 = 0.9643); locator found 35 / 35, map right 33 / 35 |
| 3. Dev build | 41 volumes, files, counts | 41 rows in `volumes.tsv`, 41 `links` + 41 `folio_map` files, manifest counts equal the sums |
| 4. Silver | totals with controls | below |
| 5. Docs, tests | | done; 1,380 passed, 11 skipped |

**Bar L is a negative result** (pre-registered L2, selected on dev). Without 3712, whose three false positives are
day-of-month numbers, the test would read 61/63 = 0.968; that is a post-hoc reading and not a pass. **Test-run
disclosure:** the test was run twice. The first run used the module default before it was set (placeholder `L3`; the
`sed` that should have set `L2` failed on macOS), so it measured L3 by mistake; the log is kept as
`test_bars_L3_misrun.log`. L3 on test: P 60/62 = 0.968, R 60/61 = 0.984, bar E 32/35 = 0.914 (read only 26/28). The
selection was made on dev before any test run, and the choice (L2) stands by the rule. Whether L3 should become the
default is left to the phase review; it is a post-hoc comparison on a test set that has been seen.

**Silver (dev build, no bar).** Located links with a located control (folio + 3): 94,056. Entry words of >= 6 letters on
the target sides: 118,464 / 260,497 = **0.4548**; on the control sides 26,061 / 260,497 = **0.1000**. REPUBLIC (13
covered volumes, 39,165 links with a control): the target sides have a REPUBLIC line with a `resolution_id` for 39,160
(0.9999), the control sides for 39,076 (0.9977): almost every page in these volumes has a resolution, so this check
cannot discriminate (ceiling). Per volume in `silver.tsv`.

**Dev build.** 41 volumes (40 `foliation`, 3712 `pagination`), 72,558 entries, 102,588 locators, 101,282 linked
(98.7%): printed 73,984, interpolated 26,259, extrapolated 1,039, none 1,306; confidence high 99,507, medium 715, low
1,060. Time: 19.4 s single-process for the 41 front-index volumes (0.47 s per volume), no coreshare needed (status showed
6 free cores). Entries per volume 506 to 3,030; 15 volumes have more than 1.8 locators per entry in L2 (lists of folios
and merged columns, e.g. 3265: 2,983 entries, 6,453 locators).

**Error analysis.**

*Locator errors (all 6 FPs and 1 FN across dev and test, so the 40-item cap did not bind).*

| Category | n | Where |
|---|---|---|
| date (day of month before a month name) | 3 | test 3712: "... opden 1 Febr: al daar.", "10 Mey. Gelderland ...", "14 Aug: Consent ..." (L3 drops all three) |
| quantity (count in the entry) | 1 | test 3590 "van 11 Schip de Tason." (tail fragment; `schepen` is in the quantity list but "Schip" is not) |
| other (page-number heading) | 1 | test 3456 "11." (heading that is the page number of the index; gold: none) |
| garbled digits | 1 | dev "wijnijstg6." (a "6" glued to a word) |
| mid-entry locator missed (L1 only) | 1 | dev "Renhausen, 66. Johan Oirrock 108, 124. 162." |
| ordinal, amount, year, split number, gold error | 0 | |

L2 has no FN on dev or test; every miss of the other variants is a locator followed by more text (L1) or a true locator
removed by a rule (L3: "Denhof 354." by `amount_before`, 1 test row, shown only by the L3 misrun).

*Bar E misses (2 of 35).* Both are the **map**, not the locator: 3611 locator 486 (target 0545, predicted 0546,
interpolated: 486 is printed on 0545 inside a text line, not typed as page number) and 3590 locator 23 (target 0070,
predicted 0072: `23` read on two scans, the chain keeps the later). They are 20d's two bar-F misses; they were read there.

*20 random dev-build links* (`random.Random(20261202)`, `sample_links.py`/`sample_links.txt`, target sides' lines that
share a >= 6-letter word with the entry). Matter present (an entry word on the front or back side): 14 of 20 (e.g.
3611 "Declaratie, 133." on 0192/0193; 3456 "Hertog van Wirtenberg ... 152." on 0201's recto with 7 shared words; 3290
"Gasthuijs Costen ... 307" on the verso 0374 with 6). Not testable, entry words under 6 letters: 3 (Caron 624,
Giese 38, Obdam 836). No match: 3 name-only entries (3274 "Pierta da Silve 439, 534", 3320 "Schuerman", 3254
"Caeckmes"), which the HTR spells differently or which sit on the other locator. No clear wrong target. This is in line
with 20c's reading of the gold (the matter on folio N in 50 of 52 entries, F14-20c.2); the sample is too small to
differ from it.

*10 volumes with the lowest silver word share* (3630 0.21, 3611 0.24, 3590 0.27, 3587 0.29, 3623 0.29, 3572 0.30, 3493
0.35, 3522 0.36, 3507 0.36, 3455 0.37; all `foliation`; read in `silver.tsv`, with zero-hit links of 3630, 3611, 3590
printed): these are the late (18th-century) volumes with abbreviated and generic lemmata ("Brieven van Notificatie,
353. 361, 398, 450, 640, 688. 765.", "Reqte. van Praesident en Leeden ..." where the HTR spells the matter differently);
a lemma with up to 20 locators has its words at some of them only. Not a map failure: 3630, 3611, 3590 have 656/1,822,
871/2,449 and 529/1,492 links with >= 2 entry words and no hit. In 6 of the 40 foliated volumes the chain ends in a
year-like anchor (F14-20e.8; 3522 is one of the ten).

**Findings.** F14-20e.1 to .8 in the Findings table.

**Open issues.** Bar L precision missed; which variant becomes the default for the full run is a decision for the phase
review (UA-052 option (b)). UA-050 (confirm page) still blocks publication of `index_links_v1`. Protected trees: no file
under `data/text_layer_v2_dev/19e-a` has an mtime of today; no before/after hash pair was taken (the protect script was
run only after the work and failed on its output path, so there is no baseline).

### Step 20r: review fixes

**What changed.** `scripts/build_index_links.py`: module-level `run_one` wraps `process`; a volume that raises is recorded
(`failed`, message in manifest `last_run.failed`), gets no files (a rerun retries it), the run goes on and `main` returns 1;
the parallel path uses `as_completed`; every finished volume prints and appends a line to `<layer>/progress.log`.
`.venv` install metadata refreshed with `make update_install` (version 0.18.0 now). `docs/index_links.md` (bar results, three
limits), `docs/text_layer.md` (one sentence), changelog entry. New test `test_builder_failed_volume_is_recorded`; the test
helper `_load_builder` now registers the module in `sys.modules` and puts `scripts/` on `sys.path` (needed to pickle
`run_one` for the pool). Regression build: `data/index_links_dev/20r-a/`.

**Results against the acceptance criteria.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Tests | index-links file green; suite 1,381 passed, 11 skipped | 10 passed; 1,381 passed, 11 skipped |
| 2. Regression | 41 + 41 + 1 equal | links/folio_map 82 equal, 0 differ; `volumes.tsv` equal; `last_run.failed == {}`; `package.version == "0.18.0"` |
| 3. Install | prints 0.18.0 | 0.18.0 (`make update_install` printed `chmod` errors on `.venv/share/jupyter/...` and exited 1, after the install; harmless) |
| 4. Docs, `make reports` | as brief | done, see below |

**Error analysis.** The regression `progress.log` has 41 lines, all `done`, empty message column; `last_run` is
`{inventories_considered: 41, outcomes: {done: 41}, failed: {}}`. In the failing-volume test the recorded message names the
missing file (`FileNotFoundError: ... units/units/2.tsv.gz`); the rerun after restoring it retried only volume 2, and the
`--workers 2` run (parallel path) skipped both. No new findings.

## Review

*Phase review, Opus 5.5, 2026-10-29. Code review: [`plan/phase-14-index-links/REVIEW.md`](../plan/phase-14-index-links/REVIEW.md).
Reviewer scripts and outputs: `reports/data/phase-14-index-links/review/`.*

**Re-run.** Every acceptance number reproduces: AS suite 1,380 passed, 11 skipped; NLP 66 passed; LotA 380 passed,
1 skipped, 1 xfailed. 20a `compare.py 19e-a 20a-pin`: 4,591 equal, 0 differ. 20b NLP verify `both`: 15/15 PASS, exit 0.
20c `summary.py`: same counts. 20d `eval_folio.py` (in a scratch copy): 41/41 runs, LOO 21,010/21,334, bar F 33/35 (read
only 27/28), `gold_eval.tsv`, `loo.tsv`, `volumes.tsv` byte-equal. 20e `select.py` and `test_bars.py`: same table, L2,
bar L 61/66 and 61/61, bar E 33/35; all four output files byte-equal; L3 through the same script gives the misrun's
numbers (P 60/62, R 60/61, E 32/35). Builder: a rebuild of the 41 volumes is byte-identical (41 `links` + 41
`folio_map`, `volumes.tsv`), and the default run over all of `19e-a` (3 workers via coreshare) gives the same 41
volumes and 84 `no_front_index`. Silver: `silver.tsv` byte-equal. Protected trees: no 20e before/after pair exists, so the
review hashed again and compared with 20a's after-list: `19e-a`, every other dev layer and `shared-data/` (including
the source tar hashes) are unchanged; only 2 LotA test-fixture mtimes differ (rewritten by the LotA suite).

**The gold deviation in 20c (F14-R.1).** Sound. The planner's draw already held one reserve per volume; the extras are the
8 test reserves, read with the same procedure, decided on the sample-size shortfall (28 of 30) and finished before any
phase-14 code existed (file times above). The bar population was therefore fixed before any score, and 33/35 is bar F's
result. The reports give the read-only numbers beside it and never call 27/28 a pass; the step-20d text says that one row
decides the bar and that 35 rows cannot separate 0.943 from 0.95. The extras shift the test weight slightly towards
one row per volume, which is harmless. The gold remains one reader's (UA-050 open); the reviewer's check of the target
scans against typed page numbers found nothing wrong (35 of 38 `printed` rows carry N as a typed page number on the target
recto; the other 3 are the untyped or garbled cases 20c names).

**The variant (F14-R.2).** L2 stays the default and bar L is reported as failed. The selection rule picked L2 on dev before
any test run (`select.tsv` 16:04:09, first test run 16:04:35), so the L2 test result is clean. The L3 run was a mistake,
and choosing L3 now because it passes on test would select on test: the test gold is spent for locator variants. The
L3 numbers stay in the report as information. Two points for a v2. First, P9 described L2 as numbers that end a sub-entry
*with punctuation* (`N[.,:;]` then a capital), but the brief's operational rule (and the code, faithfully) also accepts
a bare space, so `14 Aug`, `10 Mey`, `1 Febr` and `11 Schip` count; 4 of the 5 test FPs are that difference. In the dev
build such locators are 901 of 102,588 (0.9%); of 20 read (`review/l2kinds.txt`) about 7 are real (`Contrarole: 998
Verlof`, `Verstecq 471 Reqte.`), 11 are dates or counts (`9 Jan: 1784`, `van 25 Scheepen`) and 2 unclear. Second, L3's
`amount_before` needs a left word boundary (`Denhof 354.`). Both need fresh gold to be chosen.

**Own error analysis.**
- *Year-like end anchors (F14-R.3, `review/yearanchor.txt`).* In 3126, 3265, 3290, 3307, 3320 and 3522 the chain ends in
  1591, 1659, 1674, 1683, 1689 and 3522, three or four scans from the end. Every number between the last real anchor and that
  value interpolates onto the last one or two scans with an anchor distance below 1 scan, so `confidence` is `high`: 94
  links, 90 `high`, e.g. 3265 entries citing the years 1650 and 1653 are linked to scan 0870. The map tables of these volumes
  have 1,597–3,528 rows. Across the 41 volumes 949 locators exceed the volume's scan count and cannot be folios; 820 are
  correctly left unlocated, 129 are linked. A guard (in foliation, drop anchors above the scan count, or a jump test on the
  chain) and a confidence that also looks at the number gap belong in a map v2.
- *Link-level silver (F14-R.4, `review/silver_links.txt`, `review/offset.txt`).* 20e's word share pools word tokens and is
  dominated by long multi-locator lemmata. Per link: 70.7% of links have an entry word on the target sides, 21.8% on folio
  + 3, and the control beats the target in 2.6%. For single-locator entries with ≥ 3 words the best folio within ±3 is N
  for 0.865 of those with a hit, N+1 for 0.048 and N−1 for 0.018. Read cases show that N+1 is mostly the matter running
  over from the back of folio N onto the next recto (3522 locator 161; folios 160–162 printed on 0209–0211, the entry's
  words at the top of 0211 recto), and N−1 is the facing verso of the same opening
  (3280 locator 121, `Maurice de Nassau` on 0167 verso beside 121r), like the one `adjacent` row of 20c (3456 locator 98).
  This is consistent with bar F's ~0.94 and says the leaf model (recto of S + verso of S+1) is right for most links; an
  `opening` side (verso of S) could be added as a third target in v2.
- *Low numbers (F14-R.5, `review/numstats.txt`, `review/l2kinds.txt`).* 2,969 locators are single digits; 939 of the 1,039
  `extrapolated` links are among them. 402 locators have a letter right before the digits; of 40 read, at least 34 are
  HTR fragments or amounts (`U1`, `f300 S Jaars`, `Oorlog1789`, `Thesing7.`). `extrapolated` and `low` links should be read
  as unverified.
- *The steps' own analyses hold up.* 20c's categories recount from `entry_gold.tsv`; 20d's two bar-F misses are as
  described (3590 duplicate 23, 3611 untyped 486); 20e's FP list is complete (`test_rows.tsv`: 5 FP, 0 FN) and its
  bar-E misses are 20d's. 20e's 20-link sample is small but agrees with F14-R.4.

**Leakage.** None found. The map reads only `page_number` segments, the scans table and the flags; locators read only the
unit text; L4's range comes from the map. Variant choice on dev only; the test ran twice, but the second (L2) run's
variant was fixed before the first. REPUBLIC is read only in `silver.py`. No gold file is read by library code.

**Code.** The modules follow their briefs; the archive-specific values are in `SG_LOCATOR_PARAMS`/`SG_MONTHS` and the
builder. Defects: the builder was ignored by git while a tracked test loads it (F14-R.6, fixed in the review); manifests
record a stale package version (F14-R.7); the parallel builder aborts on the first failing volume and logs no progress
(debt). Step 20r fixes the last two and documents F14-R.3–R.5 in `docs/index_links.md`.

## After the review: full run and publication

**User check (UA-050, 2026-10-29).** See [Step 20c](#step-20c-gold-reads-locators-and-link-targets), *User check*
(F14-20c.8).

**Full run (UA-052, 2026-10-29).** The user ran `scripts/build_index_links.py` (L2, through coreshare) on the v5 full
layer on the machine with the drive and copied `volumes.tsv` and `manifest.json` to `data/index_links_full/`; the links
stay on the drive. Code: the manifest records version 0.18.0, `git_head` a7b537a and the same six changed files as the
20r dev build (v0.19.0 is not yet released).

| | value |
|---|---|
| inventories considered / with a front index / failed | 8,856 / 412 (408 foliated, 4 paginated) / 0 |
| entries / locators / linked | 629,912 / 914,994 / 902,031 (0.986) |
| locators per entry | 1.45 (dev 1.41) |
| time | 247 s |

Error analysis on `volumes.tsv` (the links themselves are not local, so no link was read):
- The 41 dev volumes are identical to `data/index_links_dev/20r-a/` in all 12 columns.
- Link rate per volume: median 0.990, but three volumes link under 0.6. 3712 (62 of 299) is the dated index of
  F14-20c.5. In 3681 (129 of 465, chain 32–237) and 3601 (982 of 1,827, chain 2–649), most locators fall outside the
  chain range ± 6; either the index covers more than the volume or the foliation restarts. Not checked (F14-20e.9).
- Lowest recto shares are the paginated volumes (3711 0.63, 3712 0.75, 3244 0.80).
- Chain ends: 22 volumes end at a year, an inventory-like number or a garbled number by the rule of F14-20e.10 (9, 59,
  3126, 3244, 3255, 3260, 3265, 3266, 3267, 3276, 3285, 3290, 3307, 3320, 3340, 3522, 3554, 3608, 3609, 4573, 4588,
  4771). Large volumes do run past folio 1000 (3616: 845 anchors, last 1002), so the rule needs the ratio; the
  scan-count test of F14-20e.8 needs the full layer's scans table.

**Publication (UA-053, 2026-10-29).** The user chose (a): `index_links_v1` is not published. The dev builds stay in
`data/index_links_dev/`, the full build on the drive with its tables in `data/index_links_full/`. A v2 is published
after an index-links v2 phase meets its bars on fresh gold. That phase's gold pool: the 29 local front-index volumes no
gold has touched, plus the 371 front-index volumes outside the local dev layer (their text is on the drive only).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F14-R.6, F14-R.7, F14-R.3–R.5 | Builder per-volume failure handling and progress log; refresh the stale install metadata; `docs/index_links.md` limits for year anchors, extrapolated links, openings | step 20r ([brief](../plan/phase-14-index-links/step-20r-review-fixes.md)) |
| F14-20e.5, UA-052 | Full run of `index_links_v1` (L2) on the v5 full layer | done 2026-10-29 (F14-20e.9) |
| F14-20e.2, F14-20e.4, F14-R.2, F14-R.5, F14-20c.8 | Locator v2: punctuation-ended sub-entries (P9 as written), `amount_before` and digits with a left word boundary (the user check shows `Cunes94` is an HTR artefact, so the boundary no longer costs a gold locator), date-index handling; chosen on dev, tested on fresh gold | next index-links phase (fresh gold from the 29 untouched local and 371 drive-only front-index volumes) |
| F14-20d.3, F14-20d.4, F14-20e.8, F14-20e.10, F14-R.3 | Map v2 (22 of 412 full-layer volumes affected, estimate): drop year-like anchors (above the scan count; verso years in the recto share), duplicate-number tie rule, a confidence that sees the number gap; new bar F on fresh gold | next index-links phase |
| F14-20e.9 | Volumes whose index cites folios outside the chain (3681, 3601): read their links (on the drive) | next index-links phase |
| F14-20c.1, F14-20c.7 | Entry assembly: first-line fragments whose locators sit in continuation units (about 1 in 9 entries), units that merge two index columns | next index-links phase |
| F14-R.4, F14-20c.3 | An `opening` target (the verso of the folio's own scan) beside the leaf; measure on fresh gold | next index-links phase |
| F14-20c.5, P1 | Dated summary indexes (3712), separate index volumes (an EAD volume map), unit-level targets (which resolution on the folio) | not planned (P1 scope) |
| UA-053 | Publication of `index_links_v1` | decided 2026-10-29: not published; a v2 after the next index-links phase |
| F14-R.7 | Manifests of `19e-a`, the v5 pilot and the v5 full run say 0.14.1; their `git_head` is right | recorded in `docs/text_layer.md` by 20r; no rebuild |
| F14-P.7 | `poetry.lock` still records pagexml-tools 0.9.0 | `plan/PLAN.md` *Preconditions still open from phase 01* (`poetry lock`) |

## Reproduce

- Planning: `.venv/bin/python -I reports/data/phase-14-index-links/planning/draw_reads.py`, `folio_proto.py`,
  `loo_check.py` (AS root; read `data/text_layer_v2_dev/19e-a`).
- 20c: `.venv/bin/python -I reports/data/phase-14-index-links/step-20c/summary.py`.
- 20d: `.venv/bin/python -I reports/data/phase-14-index-links/step-20d/eval_folio.py` (writes into `step-20d/`).
- 20e: `select.py`, `test_bars.py` (spent: do not use for a choice), `silver.py`, `sample_links.py` in `step-20e/`; dev
  build: `.venv/bin/python scripts/build_index_links.py --layer data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5
  --out-dir <new dir> --inventories-file reports/data/phase-14-index-links/step-20e/dev_inventories.txt`.
- Review: `.venv/bin/python -I reports/data/phase-14-index-links/review/<script>.py` (`yearanchor`, `l2kinds`, `numstats`,
  `offset`, `goldcheck`), each with its `.txt` output beside it.
