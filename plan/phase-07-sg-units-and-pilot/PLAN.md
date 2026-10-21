# Phase 07: SG units, types and the pilot layer (LotA hand-off R4–R6 and acceptance)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. **Status: reviewed** 2026-10-21 (Opus): accept with follow-ups; review
> fixes in 15ir ([REVIEW](REVIEW.md)). Planned 2026-10-20; seeded 2026-10-17 by phase-06 planning.

## Goal

From phase 06's v2 lines, AS builds the **units** LotA aligns on, and publishes a pilot layer
for hand 3349–3354 and print 455/456 in `shared-data`. Each unit has:
- its text, joined with per-inventory word-break decisions;
- the character offsets of every line (R1);
- line and region **types** (R4);
- a mapping from the old `doc_id` to the new one (R5).

The phase also produces a manifest with audit invariants (R6), and the evidence the user needs
for the acceptance on the 116 labelled rows.

## Owns

- `analysis/page_structure.py` and `text_layer/layout.py` (15fp, 15fq), and the `layout` stage in
  `build.py`;
- `text_layer/` modules `types.py` (15g), `units.py` (15h), `join_v2.py` (15hj), and additive
  changes to `word_breaks.py` (v1 output byte-identical);
- the v2 `types`, `units`, `wbc`, `join` stages and v2 `--stage all` in `build.py`;
  `Run.update_manifest` (history); the subset-run guard in `stage_wbc`;
- `presets.py` (new fields `layout_params`, `types_params`, `unit_params`, `word_break_rule`,
  `word_break_stat`, `wbc_sanity_in/out`, `wbc_pinned` (15hj2), `join_lead_marks` (15ir));
- `docs/text_layer.md` (schema sections);
- the pagexml-tools pin (15i step 0, after UA-007);
- `shared-data/NL-HaNA_1.01.02/text_layer_v2/` and `shared-data/NL-HaNA_1.10.94/text_layer_v2/`,
  their catalogue rows, and a section in LotA `docs/from-archival-structures.md` (15i Part B,
  written once, after the user agrees).

## Sources

- `docs/from-language-of-the-archive.md` R1, R4–R6, §3–4.
- The SG audit §2 (schema advice) and §3 (validation checklist).
- Phase-06 `PLAN.md` (P1–P12) and its report.
- Phase-05 `REVIEW.md` rows 4, 6, 7.
- republic-project: `republic/helper/pagexml_helper.py` L151–289,
  `republic/parser/pagexml/republic_page_parser.py` `split_column_regions` (L33),
  `page_date_parser.py` L258–359 (read for 15fp; summarised in its brief).
- LotA `archival_language/corpus/tsv_source.py` (header check L43, L169–179; `parse_doc_id`
  L51–69).
- LotA `corpus/parallel_text.py` §7 (`find_session_headers` L506; it needs a weekday, so bare
  "21. April" headers are not found).

## Steps

Run in this order; each needs the one before. Six steps were planned, one over the budget of five
(D11). User follow-ups (15fq2, 15g2, 15hj2, 15i2) and the review fixes (15ir) made it eleven.

| Step | Title | Content |
|---|---|---|
| 15fp | Page layout ([brief](step-15fp-page-layout.md)) — **done** 2026-10-19 | Port of republic's region merge; page split at a fitted gutter; columns; `main`/`extra`. Validation negative (F07-15fp.2–.5) |
| 15fq | Layout fixes ([brief](step-15fq-layout-fixes.md)) | (1) `main_edge` gutter for inventories with a gap on < 95% of openings; (2) print regions split on the line `column` field; (3) furniture (page number, date, year, catch word) kept out of merges; (4) interleave guard. 15fp reachable as `LEGACY_15FP`. New seeded validation; merges-off fallback |
| 15fq2 | Column merge guard ([brief](step-15fq2-column-merge-guard.md)), after 15g | UA-008 (b): pieces whose lines carry different non-empty `column` values never share a merge set (union-find, no threshold). Layer `15fq-c`; 15g's `types` re-run as `15g-b`. 15h runs on `15g-b` |
| 15g | Line and region types, R4 ([brief](step-15g-line-region-types.md)) | Segments: the hand top line is split into date header / text / page number (F07-P.10); the page-number chain; nine classes with method tags; print column override; Laypa check (455). Test rows 9, 19, 50, 52, 55, 134 |
| 15g2 | Margin cut month-only ([brief](step-15g2-margin-cut-month-only.md)), after 15fq2 | UA-009 (b): `rule:margin_prefix` cuts only month-only prefixes (`TypeParams.margin_cut`); types re-run as `15g-c`. 15h runs on `15g-c` |
| 15h | Units and mapping ([brief](step-15h-units-and-mapping.md)) | Hand units = layout region minus furniture, cut at headers; print units = paragraphs within a page column (cues fitted on a dev split against REPUBLIC paragraphs); reading order; the R5 id rule and `doc_id_map`; REPUBLIC line crosswalk. Test row 80 |
| 15hj | Word breaks and join ([brief](step-15hj-word-breaks-and-join.md)) | v2 `wbc`/`join` on units, with per-line offsets (`join_unit` equals `join_region`); the `„`-robust statistic, adopted only on a labelled sample (F05-R.4); the preset carries the rule and anchors; subset guard; manifest history; v2 `--stage all` |
| 15hj2 | Pinned WBC sets ([brief](step-15hj2-pinned-wbc-sets.md)), after 15hj | UA-010 (c): `LayerPreset.wbc_pinned`; SG sets `-=„`, 11281 `-:=„`; `wbc`/`join` re-run as `15hj-b` without `--ignore-sanity`; v1 157/157 |
| 15i | Pilot build and publish ([brief](step-15i-pilot-build-and-publish.md)) | Part A: pin 0.9.1 if released; one hand layer of 14 + print 455/456 from the PageXML; reproducibility; audit §3; R6 counts; 116-row evidence; crops for the user's row check; a UA. Part B (after the UA): `text_layer_v2` in `shared-data`, catalogue rows, LotA note |
| 15i2 | hand_tar pin without `=` ([brief](step-15i2-drop-eq-hand-tar.md)), between 15i Part A and Part B | UA-011 (b): `=` dropped for 3349–3351, 3353, 3354; hand rebuilt as `15i-b/hand`, print copied; audit and 116 rows re-run; new UA to publish |
| 15ir | Review fixes ([brief](step-15ir-review-fixes.md)), after the user's commit + v0.11.0 | F07-R.1–R.5: sort column-split print lines; blocked counts per final pass; types guard + `_offchain` relabel; legacy-twin ids; `join_lead_marks` (`,`); small items, tests, docs, LotA known-issues note. Dev layer `15ir-a`; no publication (UA-013) |

## Decisions (phase planning, 2026-10-20)

Pre-planning decided D1–D5 (`PROGRESS.md`, pre-planning block).

| # | Decision | Why |
|---|---|---|
| D6 | Gutter rule per inventory: `gap` (15fp) if a zero-coverage gap exists on ≥ 95% of openings, else `main_edge` (the verso's main text right edge, guarded) | Measured: 3354, 3771–3823, 455, 456 have ≥ 98.4%, the rest ≤ 87.4% (F07-P.12). On 15fp's read scans, `main_edge` fixes 14/14 cut-offs and the whole-page miss (F07-P.11) |
| D7 | Print regions are split on the line `column` field before the merge | F07-15fp.3: print regions are page-level |
| D8 | Furniture is decided by text pattern (page number, date, year) plus a bottom-band catch word, at region level, before the merge. The patterns live in the SG presets | Flags all 14 "furniture absorbed" merges of 15fp (F07-P.13); no Dutch month names in `analysis/` |
| D9 | Interleave guard added to 15fq | User, 2026-10-20 (planning question) |
| D10 | Fallback if merges miss 0.90 again: the layout with `merge=False` for that side; 15g–15i go on | User, 2026-10-20 |
| D11 | 15h is split into 15h (units, mapping) and 15hj (join). The phase has six steps; it is not split, because 15fp was inserted at the user's request and a third renumbering costs more | One brief would exceed ~250 lines |
| D12 | No `auto` rule for merged-line splitting: `merged_line_split` stays `off` for 1.01.02 (its printed volumes have 0.00–0.14% candidates, F06-15e.4) and `on` for 1.10.94 | Open question settled by measurement |
| D13 | `doc_id`: in each layout region, the unit with the most `main_text` inherits the layout region id (= the legacy id when unchanged). Other units get `legacy_doc_id` of their own box, so LotA's `parse_doc_id` still parses them. `doc_id_map` covers every legacy region | R5; LotA crops parse the box from the id |
| D14 | 15g types line **segments**; only three rules split a line (date prefix, margin prefix, page-number suffix in the chain). Split geometry is char-proportional and flagged | F07-P.10: the HTR merged the page's date and page number into its first line |
| D15 | The split print text: offsets index the halves' `text`, and the mapping goes through `split_from`. The merged original's text is not stored (F06-R.4) | Region ids don't change with line splits |
| D16 | pagexml-tools: 15fq–15hj stay on the 0.9.0 wheel; 15i pins 0.9.1 once released | The local copy's `stats` crashes the print splitter (F07-P.9, UA-007) |
| D17 | Pilot path `text_layer_v2/` under each archive in `shared-data`, `coverage: partial`; the full SG run becomes `text_layer_v3` | User, 2026-10-20. The README forbids in-place rebuilds |
| D18 | AS states the schema (new tables, new file names); LotA writes its reader. The legacy 6-column `paragraphs-…` files are not reproduced | Audit §2.11; hand-off R5 |
| D19 | The ≥ 80% (UA-001) is checked by LotA after re-alignment; 15i gives a per-row prediction and an upper bound | AS can't re-align; see hazard 4 |
| D20 | 15fq2 (column merge guard) runs after 15g, before 15h; it re-runs 15g's `types` on the new layout (`15g-b`) | User, UA-008 (b), 2026-10-21. 145 print regions held both columns; 15g was already running on `15fq-b` |
| D21 | 15g2 (margin cut month-only) after 15fq2, before 15h; types layer `15g-c` | User, UA-009 (b), 2026-10-21: margin_prefix cuts 2/9 right; month-only prefixes fine. No R4 row uses a non-month cut (checked) |
| D22 | 15hj2 pins the SG word-break sets (`-=„`, 11281 `-:=„`) before 15i; `sanity_pass` is checked on the pinned default set | User, UA-010 (c), 2026-10-21: derived sets took `.` (455, 3822; right 11/20, 4/10) and missed `„` (3351, 3354); `:` is 11281's hyphen (9/10) |
| D23 | `=` is not pinned where the rule did not derive it (hand_tar except 3352); the pilot becomes `15i-b` | User, UA-011 (b), 2026-10-21: the added `=` merges were 11/20 right (F07-15hj2) |
| D24 | Review fixes go into a dev layer (`15ir-a`); the published `text_layer_v2` stays until UA-013. Fixes are gated so v1 and `LEGACY_15FP` stay byte-identical; `unchanged` regions keep their source line order (R5) | Review, 2026-10-21: D17 forbids in-place rebuilds; the defects touch 3 of the 116 rows |

## Hazards

1. **Protected:** `shared-data/` (written only in 15i Part B), LotA `data/`, `gold/` and `output/`,
   NLP `output/voc_text_layer/`, every earlier dev layer under `data/text_layer_v2_dev/`. Each
   brief hashes them before and after.
2. **v1 must stay byte-identical** when 15hj touches `stage_wbc`, `word_breaks.py` and the
   manifest. 15hj re-runs `TOTAL 157 / 157`.
3. **Briefs downstream of 15fq name layer paths and columns** that 15fq–15hj create. Each step
   updates the later briefs if it deviates (rule 10).
4. **The ≥ 80% target looks out of reach on this layer** (F07-P.14): about 13 artefact rows are
   glued print words (repair off, UA-003), several are HTR/OCR errors, and 4 need LotA's
   re-alignment. 15i reports the bound; the user decides then.
5. Agent shell: no `rm -rf`, `nohup` or `&`; a new dir per run; pip installs need `chmod g+w`.
6. 15i reads the user's local PageXML for 8 hand inventories (`data/PageXML/…`) and the
   shared-data archives for the rest, through a symlink source dir. Never write through it.

## Pre-planning: the republic layout port (2026-10-18, Opus, at the user's request)

The user asked to port republic's PageXML preprocessing before phase 07. The evidence is in
the report's *Planning* section (F07-P.1–P.8), and the brief is
[`step-15fp-page-layout.md`](step-15fp-page-layout.md).

- **What is ported:** the overlapping-region merge (`merge_overlapping_text_regions` and its
  helpers) and the column-assignment criterion of `split_column_regions`.
- **What is new:** a gutter fitted per inventory, because AS's `classify_inventory_structure`
  fails on SG (F07-P.5) and the separation methods disagree (F07-P.6).
- **What is not ported:** the date-header machinery. It needs the neural `line_class`, `date`
  region types and REPUBLIC inventory metadata (F07-P.8). Hand SG and print 456 have no region
  labels at all (F07-P.1).
- **pagexml-tools already has** `regions_overlap`, `parse_derived_coords`, the overlap ratios
  and a page split at a given point. It has no gutter detection, no column building that
  respects `extra`, and no line types. Its traps are listed in F07-P.7 and UA-005.

Consequences noted at pre-planning (superseded in part by 15fp's results and D13–D14):

- **15g** gets `page`, `layout_column`, `layout_role` and the position relative to the page
  and column, instead of Laypa tags. For print, "takes the Laypa tags" holds for 455 only, and
  even there most typed regions are empty (F07-P.2).
- **15h**:
  - hand units = layout regions;
  - the R5 mapping = `layout/regions.source_region_ids`;
  - `change=unchanged` keeps the `doc_id` byte-identical.

## Open questions for planning: settled 2026-10-20

| Question | Settled by |
|---|---|
| `auto` rule for merged-line splitting in printed 1.01.02 | D12: no `auto` |
| Layer naming and versioning in `shared-data` | D17 (user): `text_layer_v2`, partial; full run v3 |
| The schema LotA reads | D13, D18; schemas in `docs/text_layer.md` (15fq–15hj), LotA note in 15i Part B |
| Split print layer | `15fr-a/print/` (15fr ran); the input of 15fq |
| Mapping and offsets of split lines via `split_from` | D15 |
| Types override `column` for furniture in print | 15g `column_override` |
| F05-R.4 `„`-robust statistic | 15hj, adoption test on 40 hand-read pairs |
| Glued words | closed (UA-003 (a), 2026-10-18); stays off |
| Acceptance target (UA-001: ≥ 80% of 116 rows, baseline 56%) | 15i evidence + bound; LotA checks after re-alignment (D19, hazard 4) |

## Review checklist

As in `plan/templates/PHASE_PLAN.md`, plus: the audit invariants pass; there is no write through
an existing symlink; the v1 inputs are untouched; and the user signs off on the 116-row result.

## Hand-off

Reviewed 2026-10-21 (Opus): **accept with follow-ups** ([REVIEW](REVIEW.md)). Commit the current
tree and release it as **v0.11.0** (the code that built the published pilot), then run 15ir and
release **v0.12.0**. The accepted deviations of every step are collected in
[`plan/CONTEXT.md`](../CONTEXT.md), which is the context later planning reads.

- **For whoever uses the layer:**
  - `shared-data/NL-HaNA_1.01.02/text_layer_v2/` and `NL-HaNA_1.10.94/text_layer_v2/` are the pilot
    (`coverage: partial`; schema in `docs/text_layer.md`). Align on `join/units` + `join/segments`;
    map old ids through `units/doc_id_map`.
  - Known issues until 15ir / UA-013: print line order in column-split units (F07-R.1), the `,`
    continuation mark (F07-R.2), the legacy twins (F07-R.3), and page-number method tags (F07-R.4).
  - The hand layer has no region merges (`merge=False`, F07-15fq.3). Glue is not repaired (UA-003).
- **For phase 08 (plan next; region quality and duplicates):**
  - the follow-ups in the report's table: the `g_upper_right` guard in `join.py` (F07-15hj.6); the
    crosswalk role column; the interleave bridge; bottom-band years; the REPUBLIC default path out of
    the library;
  - the full SG run `text_layer_v3` needs the 15ir code and the UA-013 answer;
  - pagexml-tools 0.9.1 (UA-007), then drop the four workarounds (F07-P.7, F07-15fp.11).
- **For the user:** UA-013; the commit and release; the 116-row sign-off after LotA re-aligns (D19).
