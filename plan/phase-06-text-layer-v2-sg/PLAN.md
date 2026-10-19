# Phase 06: Text layer v2 — SG line geometry and layout repair

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`). Created and planned 2026-10-17 (Opus) from LotA's
> hand-off `docs/from-language-of-the-archive.md` (R1–R6) and the phase-05 hand-off.

## Goal

AS reads the States General PageXML (hand NL-HaNA 1.01.02 and print NL-HaNA 1.10.94) into a **v2
line layer**. That layer has one row per text line with geometry, baseline, confidence, page side
and the legacy region `doc_id` (R1, R5). It also has one row per scan, including scans without
text (D2 of the audit).

For print, lines merged across two columns are split with a verified port of republic's
`long_line_splitter.py` (R2). Glued words are repaired by a measured, flagged word segmentation
(R3). Every edit keeps the raw text. VOC v1 stays byte-identical.

Line and region types, units with character offsets, the id mapping, the pilot build and the LotA
acceptance belong to phase 07 (P1).

## Owns

- **AS:**
  - new `archival_structures/text_layer/extract_v2.py`, `line_split.py`, `word_split.py`;
  - additive changes to `text_layer/io.py` (tar.gz input, a resource-fork skip flag, v2 discovery)
    and `text_layer/presets.py` (v2 preset fields and the SG presets);
  - new `tests/test_text_layer_extract_v2.py`, `_line_split.py`, `_word_split.py`, and
    `tests/fixtures/text_layer/sg/`;
  - the `nltk` core dependency in `pyproject.toml`;
  - a v2 section in `docs/text_layer.md`.
- **Siblings:** nothing is edited. LotA and republic-project files are read only.
- No other phase is active.

## Sources

- `docs/from-language-of-the-archive.md` (all), `docs/from-republic.md` §3, §9.
- `docs/audits/sg_paragraph_tsv_audit.md` §1–3.
- `upstream_inventory.md` C14/C15 L368–394, §6 L498–549.
- Phase-05 `PLAN.md` hand-off and `REVIEW.md` rows 4–8.
- Three Explore surveys (AS `text_layer`, LotA's consumer side, data and the splitter) and the
  planning checks below.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 15d | v2 extract: lines with geometry, scans, legacy `doc_id` | — | legacy regression 100% `doc_id`+text on 14 SG invs with a TSV; v1 157/157 byte-identical; scan coverage = input files | [step-15d](step-15d-extract-v2.md) |
| 15e | Merged-line splitter (port of republic) | 15d | A/B vs the original on REPUBLIC pages of 455/456: identical; counts split/unsplit per inventory; 60 splits read | [step-15e](step-15e-merged-line-split.md) |
| 15f | Glued-word repair | 15e | held-out synthetic P/R reported with fixed thresholds; 50 real repairs read; flags per line | [step-15f](step-15f-glued-word-repair.md) |

Run in sequence: 15d → 15e → 15f. None needs user input.

## Why this order

15d fixes the line schema and the reader that 15e and 15f plug into. 15e changes the line set, so
it adds lines, ids and line ends, and that alters the text that 15f's word statistics see. So
the splitter runs before the word repair.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | Two phases | 06 = R1 (lines) + R2 + R3. 07 = R4 types, units with char offsets, R5 mapping, R6 manifest/audit, the pilot build, the 116-row acceptance, publication. Old 07/08 become 08/09 | One phase would need 6 steps (budget 2–5) |
| P2 | v2 is a new code path | `extract_v2` is new. `io` changes are additive and default to v1 behaviour. The v1 reproduction (157/157) is re-run in 15d | v1 is a published layer (phase 05) |
| P3 | Inputs | Hand tar.gz 3349–3354 and print zips 455/456 in `shared-data` (read only). AS local dirs 3771–3773, 3821–3823, 11182, 11281. Skip `__MACOSX/` and `._*` members | Planning checks C1, C2 |
| P4 | Dev outputs | `data/text_layer_v2_dev/<step>-<run>/` in AS (gitignored, writable). A new dir per run. **Nothing is written to `shared-data` in phase 06** | Publication is phase 07, gated on the user |
| P5 | Legacy `doc_id` | `f"{scan_id}-text_region-{x}-{y}-{w}-{h}"`, with `scan_id` = `scan.id` minus `.jpg` and the box from the region `Coords` in PageXML pixels (no rescale). Regions are walked recursively | Reproduces the TSVs 100% (C1); LotA keys on it |
| P6 | `region_type` retired in v2 | Keep `region_tag` (PageXML `structure` type, untagged = `untagged`). Classes come in 07 | User decision 3 (2026-10-14) vs R4: R4 is met in 07 by a new, method-tagged class |
| P7 | Raw text is never edited | `text_raw` = sanitised TextEquiv; `text` = after edits; `edit_flags` records each edit; split halves keep `split_from` | R3 "flag per line"; reversible |
| P8 | Port verbatim, adapt outside | `line_split.py` ports the republic algorithm with its MIT attribution. Behaviour change is allowed only in the adapter (pages, columns and parameters from PageXML). A/B against the original on the same pages must be identical | User: "AS should port it". Fidelity first, then adaptation |
| P9 | In-corpus scorers | The bigram scorer (15e) and the word frequencies (15f) are fitted on the run's own lines. Merged and glued lines are excluded from the fit. No labels are involved | No ground truth exists; this avoids a 156 MB external pickle |
| P10 | No tuning to taste | 15f fixes its thresholds on a dev split and reports the test split once. A weak result is reported, not tuned | `CLAUDE.md` methodology |
| P11 | Splitting is gated | Preset field `merged_line_split ∈ {off, on}`: on for print 1.10.94, off for hand. 15e reports the merged-line candidate rate for **all** 16 inventories so that 07 can define `auto` for printed 1.01.02 volumes (3771–3823 are printed) | C5: no hand line crosses the gutter; the thresholds are print-specific |
| P12 | nltk explicit | `nltk>=3.9` becomes a core dep, imported lazily. Changelog entry. `poetry lock` stays with the user | It is already installed transitively (fuzzy-search) |

## Hazards

1. **Protected inputs:** `shared-data/**` (all of it), LotA `data/` and `gold/`, NLP
   `output/voc_text_layer/` and `shared-data/voc_text_layer_v1`, republic-project. Read only.
   15d hashes the shared-data input archives before and after. Copied into 15d, 15e, 15f.
2. AppleDouble `._*.xml` members in the 1.01.02 tar files are binary, so decoding them as UTF-8
   crashes. `__MACOSX/` entries are in the zips. Copied into 15d.
3. The print PageXML has thousands of empty regions (8,070 of 8,686 in 40 scans of 455). Keep
   them as regions with `n_lines = 0`. Don't drop them silently. Copied into 15d.
4. The republic original mutates pages in place in places. A/B runs work on deep copies.
   Copied into 15e.
5. The agent shell can't `rm -rf`, `nohup` or `&`. Use a fresh dir per run. Install with
   `.venv/bin/python -m pip`, then `chmod g+w`. Copied into all.
6. Disk: 38 GB free on 2026-10-17. A pilot of 16 inventories is a few hundred MB. Copied into 15d.

## Planning checks (2026-10-17, scratchpad `p06/`; siblings untouched)

- **C1 legacy reproduction (F06-P.1).** pagexml-tools 0.9.0 on the staged PageXML, recursive
  regions, `make_text_region_text(lines_with_text, word_break_chars=['-','„'])`. Every
  `doc_id` and every text matched the TSV in all four inventories checked:

  | Inventory | Source | `doc_id`s | Texts |
  |---|---|---|---|
  | 3349 | tar.gz | 12,093/12,093 | 12,093/12,093 |
  | 3351 | tar.gz | 4,144/4,144 | 4,144/4,144 |
  | 3353 | tar.gz | 10,600/10,600 | 10,600/10,600 |
  | 3771 | local dir | 11,696/11,696 | 11,696/11,696 |

  So R5 is achievable, and UA-013's "different HTR run" doesn't apply to these files.
- **C2 words (F06-P.2).** `' '.join(Word texts) == line text` for 100% of lines (455: 33,659 of
  33,660; 3349: 14,324 of 14,324). The `Word` elements carry no lost spaces, so R3 needs text
  segmentation. Tokens with ≥18 word characters in 150 scans: 1,251 in 455 against 16 in 3349.
- **C3 print layout (F06-P.3).** 455 scans are about 4,681 px wide, with four column starts
  (two pages of two columns). Line width p50 is about 905 px. Lines ≥1,100 px wide with ≥45
  characters: 3.9%. Region tags: index, resolution, catch-word, page-number, header,
  signature-mark, date, plus `untagged`. Hand 3349: all regions are tagged `Text`, and 0 lines
  cross the scan midline ±150 px.
- **C4 REPUBLIC line ids (F06-P.4).** Of 1,603 `line_start`/`line_end` ids in LotA's
  `paragraphs-455.tsv.gz`, 1,504 match a PageXML line box exactly and 22 within 5 px. So the
  same HTR run underlies them, and phase 07 can build a crosswalk. REPUBLIC paragraphs also
  split words across paragraphs (`gel-` | `sen en geresumeert`).
- **C5 the original runs.** `republic.parser.pagexml.long_line_splitter` imports under AS
  `.venv` with `sys.path` set to republic-project. REPUBLIC page JSON for 455/456 is at LotA
  `data/pages/raw/raw_pages-45{5,6}.jsonl.gz`, loadable with republic
  `helper/pagexml_helper.json_to_pagexml_page`. The notebook parameters were
  `LineSplitter(1100, 900, 45, 450, 2350)` with an NLTK PMI scorer (`make_scorer`, notebook
  cell 32).

## Review checklist

Reviewed 2026-10-18; evidence in [`REVIEW.md`](REVIEW.md). `[~]` = passes after 15fr.

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, and re-run by the
      reviewer.
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider --continue-on-collection-errors`.
- [x] Error analysis read individual cases (each brief lists them) and is in the phase report.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [~] Changelog: the v2 API, the presets, the `io` additions and the `nltk` dep.
- [~] Ported code carries its attribution and its ported test (`test_split_word_candidates`).
- [x] No archive-specific constants in core: SG and print values live in presets.
- [x] v1 157/157 still byte-identical; no write under `shared-data/` or a sibling.
- [x] Leakage: 15f's evaluation split is by scan, and the vocabulary excludes the test scans.
- [x] `Brief defects` answered.

## Hand-off

Reviewed 2026-10-18 (Opus): **accept with follow-ups**. Run step 15fr
([brief](step-15fr-review-fixes.md)) before the release. Release after 15fr as **v0.10.0**.

- **For whoever uses the layer:**
  - Build v2 with `as-build-text-layer --preset sg_1.01.02_v2|sg_1.10.94_v2` into a fresh
    `--out-dir`. `--stage all` means extract (plus the splitter for print); `wbc`/`join` don't
    exist for v2 yet.
  - Region `doc_id`s and region text equal the legacy TSVs (F06-15d.1).
  - Use line boxes, not 455's region boxes (F06-15d.3), and exclude 455's empty regions
    (F06-15d.2).
  - A split half's `text_raw` is a piece of the HTR line; reach the original through
    `split_from` (F06-R.4).
  - Glued words are not repaired (UA-003).
- **For phase 07 (plan next):**
  - The split print layer is `data/text_layer_v2_dev/15fr-a/print/` after 15fr (factor 1.19),
    and `15e-a` before. The hand layer is `15d-a/hand_tar/` and `hand_local/`.
  - The `doc_id` mapping and char offsets for split lines must go through `split_from`. Decide
    whether to store the merged original's text (87 lines lose a double space; F06-R.4).
  - The `auto` splitting rule for printed 1.01.02 volumes needs more than the candidate rate
    (F06-15e.4). The splitter's residual failures (glued hyphens, indented lines, page
    bottoms; F06-15e.7, F06-R.2) belong in the audit, not in the port.
  - Page numbers and date headings in 456 get an arbitrary column (F06-15e.8). 15g's types
    should override `column` for them.
  - Phase-05 carry-overs for 15h: the preset carries the wbc rule and anchors, a subset-run
    guard (also for `wordfix`, should it return), a manifest history, and F05-R.4's
    `„`-robust statistic.
  - The UA-003 redesign question is closed (answered (a)).
- **For the user:** see the review chat (release, `poetry lock` for `nltk`, `.gitignore` for
  `.venv-3.11/`, LotA's `shared-data` path roots, republic-project's splitter under
  pagexml-tools ≥ 0.8).
