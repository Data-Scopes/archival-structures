# From the sister project `republic` (States General resolutions)

*Compiled 2026-10-16 from republic's `docs/` (`project_state.md`, `background.md`,
`docs/reports/phase*/`) and from checks run 2026-10-15 against republic's database and AS's
`ground_truth/NL-HaNA_1.01.02/`. `republic/` = `/Users/Shared/claude_projects/data_scopes/republic/`.
Read this file instead of republic's docs, and open a cited republic file only when you act on
that item. Never read republic's `docs/project_state.md` in full (3,000 lines); grep it.*

## What republic is

republic analyses the REPUBLIC export of the *Resoluties van de Staten-Generaal* (NL-HaNA 1.01.02,
1576–1796), covering 726,737 resolutions in 445 inventories. It mines decision-side formulas
(hinge > branch > clause), classifies resolutions by procedure and topic, and splits each
resolution into its proposition and its decision. It does no layout, page or region work. Its
unit is the resolution and its paragraphs, so it complements AS rather than overlapping it.
The database is `republic/data/processed/republic.duckdb`, plus Parquet in
`data/processed/parquet/`. Its tables are:

| table | rows | note |
|---|---|---|
| `resolutions` | 726,737 | `session_date_corrected`, `exclusion_reason` (see §2) |
| `sessions` | 95,999 | **derived from resolutions**, so sessions without resolutions are missing; do not use as session truth |
| `paragraphs` | 1,164,928 | `text`, `is_marginalia`, `page_ids` (no text-region ids) |
| `paragraph_languages` | 1,164,928 | see §5 |
| `evidence` | 2,456,120 | REPUBLIC phrase-matcher evidence from the export, with offsets |
| `entity_mentions` / `entities` | 11,895,168 / 12,169 | 8 layers (PER LOC ORG HOE COM DAT RES NAM) |
| `greffiers` | 13 | tenure dates of the griffiers |

## 1. Corrections to AS's record

- **The counts in `ground_truth/README.md` are stale.** Reading every record on 2026-10-15
  gave:
  - `resolution_metadata`: **726,737** resolutions (the README says 713,319);
  - `session_metadata`: **121,124** sessions in 491 files (the README says 120,294).
- **`resolution_metadata` is the same export republic uses**, not independent truth.
  - The resolution ids are identical, and `proposition_type` agrees on all 726,737
    resolutions.
  - It is the export from *after* the user's 2026-09-05 fix: the placeholder
    `proposition_type` `afhankelijk` no longer occurs.
  - LotA's `data/metadata/` and `output/ground_truth/1.01.02/resolutions.tsv.gz` are further
    copies of the same data.
  - Treat `proposition_type`, `resolution_type` and the dates as REPUBLIC's automatic output
    (silver), except where §2 corrects them.
- **`inventory_metadata-NL-HaNA_1.01.02.json` is identical** to republic's
  `resources/inventory_metadata.json`. Both have 674 entries and the same periods, and both
  include the user's fix of inventory 4806's period to 1579-01-01–1579-07-30. The 1795 in
  the earlier version was a typo.
- **The republic corrections are staged** (user, 2026-10-16) at
  `ground_truth/NL-HaNA_1.01.02/republic_corrections/source/`, but the README has no row for
  them yet (see §2 and §8).

## 2. Corrections and exclusions to the REPUBLIC metadata (user-confirmed)

There are two files, both JSON lists with one record per resolution. Each record has its
reason, `confirmed_by: user` and a confirmation date.

- **`session_date_corrections.json`** holds 169 records with the fields `resolution_id`,
  `field`, `recorded_value`, `corrected_value` and `reason`.
  - **Inventory 3311 (167 records):** every date is **exactly 28 days late**, a systematic
    misrecognition. The user checked this against the scans. After the shift back, every
    date falls within the period of 3311 or of the next inventory, 3312.
  - **Inventories 4562 and 4822 (one record each):** the year's second digit was misread
    (6 read as 9), giving 1924 for 1624 and 1982 for 1682.
- **`excluded_resolutions.json`** holds 143 records with the fields `resolution_id`, `reason`,
  `inventory_num` and `session_date`.
  - **Inventory 4844 (141 records):** **erroneous resolution-boundary detection** at the scans
    concerned. The user called them "clearly mistakes"; the cause was not identified.
  - **Inventory 4591 (2 records):** extracts of earlier resolutions quoted inside a later one.
    They duplicate a decision already counted and are not new resolutions.

**Consequence for AS's session and resolution layers.**
- `session_metadata` still has the uncorrected dates. Of the 95,998 sessions both sides share,
  17 differ, all of them sessions that republic corrected. So:
  - a converter that builds a `session` layer from `session_metadata` should apply the date
    corrections;
  - one that builds a `resolution` layer should drop or flag 4844's boundaries, because they
    are known to be wrong.

**The check that found them.** It compares each resolution's `session_date` with the
`period_start`/`period_end` of its inventory in `inventory_metadata`, and is never a local
ordering check. Pre-1650 and `secreet` volumes are routinely out of chronological order
within a volume, because resolutions were entered into the fair copy late.
- 689 of 723,769 resolutions (0.095%) fell outside their inventory's period, in 46
  inventories.
- They had three unrelated causes. Two of the model's three first explanations were wrong;
  one of them assumed the metadata was right when the metadata was the typo (4806).
- Lesson: **check both directions, and never auto-fix.** The check was run as a one-off and is
  not saved as a script. Details are in `republic/docs/project_state.md` open issue #3 (grep
  `Chronology QA`).

## 3. Session-level ground truth already inside AS's `session_metadata`

This is not visible from the README, which calls the dataset "complete" without saying how its
elements were made.

- **`date.evidence.type` shows where each session start came from:**

  | `date.evidence.type` | sessions | what it is |
  |---|---|---|
  | `manual_annotation` | **69,216** (358 inventories) | hand-annotated session starts with `scan_id`, `line_ids`, `page_num` and date: **line-level, sub-page session starts** |
  | `PhraseMatch` | 22,624 | automatic date-phrase match |
  | (none) | 29,284 | no start evidence |

  The manual annotations bear directly on the user's hunch that early modern archives have
  sub-page document starts (`docs/cross-project-review.md` §0.10). When this dataset is
  converted to `InventoryGroundTruth`, keep provenance per element: `manual_annotation` as
  gold, and `PhraseMatch` and none as silver, rather than one coverage value for the whole
  layer.
- **`metadata.resolution_ids` is empty in all 121,124 records.** Sessions and resolutions join
  only through the resolution's `metadata.session_id`.
- **25,126 sessions have no resolution** in `resolution_metadata`. Of these, 18,249 are
  handwritten and 6,877 printed. 11,146 are in inventories with no resolution records at all.
  A `resolution` layer built from `resolution_metadata` is therefore **partial** for those
  inventories, not complete.
- **`attendants_list_id` is null everywhere**, so the attendance lists are not in this export.
- **Parallel volume series duplicate sessions** (checked 2026-10-16 on sessions of at least 50
  words).
  - For **ordinaris, 1630s–1670s**, two inventory series cover the same days. Examples:
    3210–3221 and 3256–3265 for 1650–1659, and 3198/3245 and 3207/3253 in the 1640s. That is
    about 8,200 duplicated session-days, with the most in the 1650s and 1660s.
  - `resolution_metadata` (republic's export) contains **only one** of each pair. The sessions
    of the other series are all `in_republic = false`.
  - Of the 25,126 sessions without resolutions, 15,264 are stubs under 50 words. Of the 9,862
    substantial ones, 8,371 fall on a day and `resolution_type` that republic already covers
    with another volume's session, so most of them are parallel copies. Any session count, or session-start evaluation, over
    `session_metadata` counts those meetings twice unless one series is chosen.
  - **Speciaal** (1640s–1680s) and **secreet** (1780s–1790s) volumes also overlap in dates,
    but their content is mostly distinct.
- **Ordinaris is printed from exactly 1703** (`text_type = gedrukt`), while secreet and speciaal
  stay handwritten. So within ordinaris, any print/hand difference is fully confounded with
  before and after 1703.
- **32,725 sessions carry session-level evidence.** Labels, counted over at most the first 50
  items per session, include `president` 70,215, `session_year` 36,045, `attendants` 27,256,
  `attending` 25,559, `presiding` 25,493, `reviewed` 23,932 and `rest_day` 7,196. These are
  automatic phrase matches marking the session-opening block, so they are candidate start cues
  for an SG preset, not labels.

## 4. Start and end cues for SG resolutions (for an SG preset or start detection)

All of these are text cues, with character offsets inside a resolution's or paragraph's text.
- **Opening formulas.** REPUBLIC's `proposition_type` is assigned from a resolution's opening
  formula, which names the document the proposition rests on: "Ontfangen een missive …",
  "Op de requeste van …", "Is ter Vergaderinge gelesen het Advis …". The formulas are in
  republic's `evidence` table, with offsets.
  - Exception: `resolutie` and `ordonnantie` open with a decision phrase ("Is geresolveert …").
  - `onbekend` covers oral propositions and resolutions without a proposition part.
  - See `republic/docs/reports/phase1/phase1-step1-proposition-type-over-time.md` (the audit of
    opening formula against topic).
- **Decision hinges.** These are the closing half of most resolutions: "WAAR op gedelibereert
  zynde, is goedgevonden en verstaan …" and the noun hinge "na deliberatie …".
  - Production matches are in `republic/resources/vocab_classification/formula_pipeline_matches.parquet`:
    1,367,457 match rows with node name, offset and matched string.
  - They cover **640,140 of 726,737 resolutions (88.08%)**. Coverage varies by period; see
    `phase2-step4-decision-side-formula-mining.md`.
  - The decision side ends the resolution, so a hinge followed by a new opening formula marks
    a boundary.
  - A resolution with two hinges and two opening formulas is a candidate merge error; one with
    neither is a candidate split error. This is an untested idea, but it is the same failure
    as inventory 4844.
- **Resumption paragraphs.** These open a session after 1637: "DE Resolutien gisteren genomen
  zyn geleesen ende geresumeert …".
  - A full-text search for `geresumeert` finds 22,294 resolutions. 76% are the first resolution
    of their session, and 77% are tagged `onbekend`.
  - This makes them a strong session-start cue for post-1637 volumes.
- **The 1637 break.** The resumption procedure was introduced in 1637. Riemsdijk/Thomassen,
  CHR-2023 and republic's own `proposition_type` drift all place a break there. Expect cues and
  layouts to change at that date.

## 5. Language per paragraph

- **Method.** `paragraph_languages` comes from lingua, restricted to nl/fr/la/de/en, run over
  all 1,164,928 resolution paragraphs in 364 s. Script: `republic/scripts/detect_language.py`.
- **Results.** 97.6% of paragraphs got a confident label, and 98.1% agree with the export's
  sparse `lang` field. Coverage is 95–99% in every period. The export's `lang` has a gap from
  1705 onward, which is a pipeline artefact.
- **Joining.** Paragraphs carry only `page_ids`, not text-region ids, so a join to AS's text
  layer is by page or by geometry, not by id.
- **Relevance to AS.** AS's `docs/audits/sg_paragraph_tsv_audit.md` lists lingua output for
  `sg_letters/` only; this table covers the resolutions.

## 6. Text-normalisation and period resources (`republic/resources/`)

- **`abbreviation_map.tsv`**, supplied by the user on 2026-09-05.
  - 1,673 SG abbreviations with expansions, in 9,283 in-context example rows keyed by HTR
    `line_id` (`NL-HaNA_1.01.02_<inv>_<scan>-line-x-y-w-h`).
  - 1,414 abbreviations have one observed expansion. 259 are ambiguous, e.g. `reqte` →
    `requeste`/`requestrante`.
  - Relevant to text layer v2 and to word-break or normalisation work.
- **`period_buckets.json`.** 11 stylometric periods from Koolen, Hoekstra, Oddens & Sluijter
  (CHR 2023), plus 1576–1580 and 1581–1596.
  - In the 17th century the boundaries follow griffier changes; in the 18th they follow
    spelling changes.
  - **`greffiers.json`** has the 13 tenures. A natural stratifier for SG splits and per-period
    error analysis.
- **Stopword lists.** `vocab_classification/global_stopwords.json` is a fixed cross-period
  list. `stopwords_by_period.json` holds per-period lists, which are valid **within one period
  only**, so use the global list for anything cross-period.
- **`thomassen_document_type_glossary.json`.** The SG document-type glossary from Thomassen
  (2015) vol. 2, pp. 807–819 (term, Dutch definition, English gloss), transcribed by vision
  read. Spot-check it before relying on exact wording. Relevant to LotA facets as well.
- **`data/qa/spelling_variant_pairs.json`** and **`data/qa/lemma_token_frequencies.json`**.

## 7. Code and findings worth knowing

- **Chained fuzzy search** (`republic/scripts/vocab_building/formula_searcher.py`).
  - One `FormulaNode` per hinge, branch or clause, each owning a `FuzzyPhraseSearcher`.
  - A child node searches only its parent match's local window (200 chars), not the whole
    corpus.
  - The full corpus takes about 67 min at 6-way chunking (`row_number() % n` over DuckDB).
  - **Two lessons from fuzzy-search:**
    1. A `variants` list only *adds* match targets. To stop a string matching through the base
       phrase's own Levenshtein tolerance, add it to `distractors`. Example: `accordeert` matches
       `geaccordeert` at 0.952.
    2. Overlapping base and superset-variant matches are correct package behaviour; choosing
       the longest is the caller's policy.
- **Successor variety** (`scripts/vocab_building/successor_variety.py`). It finds where a
  formula branch or clause ends, instead of relying on a fixed word count or whole-string
  similarity. Possibly reusable for formulaic boundary detection in other archives.
- **Ingestion** (`republic/scripts/build_database.py`). Builds JSONL, then Parquet, then
  DuckDB with full-text search. It applies corrections and exclusions at ingestion as
  flagged rows, never deletions; queries add `WHERE exclusion_reason IS NULL`.
- **Embeddings** (republic Phase 5.3, a `proposition_type` probe).
  - Accuracy: off-the-shelf GysBERT 90.5%, domain-adapted GysBERT 90.0%, TF-IDF 88.8%,
    FastText trained on the full corpus 81.4%. Domain adaptation showed no gain on this task.
  - **HTR noise crowds genuine spelling variants out of FastText neighbour lists.** A
    minimum-frequency filter on the candidate pool lifted variant recall@10 from 0.157 to
    0.673 at a 1,000-occurrence cutoff. This applies to the FastText model LotA and NLP share
    (`min_count 100`).
  - The evaluation also had a self-match bug: recall@1 was structurally near zero whenever the
    query word was in the vocabulary.

## 8. Suggestions, ranked

1. **Catalogue and apply the corrections.** They are staged at
   `ground_truth/NL-HaNA_1.01.02/republic_corrections/source/`. Add a README row: source
   republic, 2026-09-04, user-confirmed against the scans, partial coverage, used by AS and
   LotA. Apply both files in any SG `session` or `resolution` converter.
2. **Convert `session_metadata` with per-element provenance**, separating `manual_annotation`
   (69,216 sessions) from the automatic starts. It is the only line-level start gold for SG
   that AS has.
3. **Correct the README counts** (§1), and mark the `resolution` layer partial for the
   inventories that have sessions but no resolutions (§3).
4. **Use republic's formula offsets as features for an SG preset** (§4): opening formula,
   hinge and resumption paragraph. They are cheap, cover 88% of resolutions, and are already
   computed.
5. **Read lingua languages for resolutions from republic** (§5) rather than re-running
   detection.

## 9. Data-staging protocol and the first moves (2026-10-16)

- **The user adopted a protocol for all projects** (republic UA-014). It is now the top of
  `/Users/Shared/claude_projects/data_scopes/shared-data/README.md`. It extends D5: source data
  and derived layers that more than one project reads live in `shared-data/`; they are
  versioned and never overwritten; projects stage them by symlinking under `data/`; the producer
  writes, everyone else reads. AS's ground-truth store keeps its own rules.
- **Moved by the user:** LotA's SG paragraph TSVs (`NL-HaNA_1.01.02`, 9,122 files) to
  `shared-data/NL-HaNA_1.01.02/paragraphs/source/`, and the SG EAD to
  `shared-data/NL-HaNA_1.01.02/ead/source/`. LotA's old paths are symlinks.
- **For AS:** when the regenerated SG TSVs from your audit (`docs/audits/sg_paragraph_tsv_audit.md`)
  exist, they go into a new `shared-data/NL-HaNA_1.01.02/paragraphs/source_YYYY-MM-DD/`, not over
  the current files. LotA and republic then relink deliberately.
- **Planned:** republic will run LotA's print–hand aligner on years after 1703–04 and publish
  `shared-data/NL-HaNA_1.01.02/print_hand_alignment_v1/` (republic UA-016).

## 10. Session boundaries from resumption paragraphs (2026-10-21)

Republic Phase 3.1 (UA-049 (a)). The files are under
`/Users/Shared/claude_projects/data_scopes/republic/output/phase-03-1/`. The write-ups are
`step01/section.md` and `step02/section.md`. The user checked 40 of the step 01 labels: 38 agree,
2 unsure, 0 disagree.

- **This refines §4's start cue.** From 16 May 1637 the resumption opens 91–96% of ordinaris
  sessions up to 1702.
  - In print it is in only 1–3% of sessions in the *staged* resolution text. Correction from the user (2026-10-21): every printed session opens with a resumption, but the resolution segmentation mostly attached it to the attendance list, which republic has not staged.
  - The handwritten registers keep it, in about 0.85 of sessions to 1753 and about 0.65 to
    1785.
  - It is absent in 1786 (inv. 3633–3638) and 1790–93, which is a register convention: the line
    is back in 1788.
  - Before 1637, `geresumeert` is an in-text use, not the procedure.
- **Missed meeting-day starts:** `step02/session_boundary_candidates.csv` has 2,749 rows, one for
  each resumption in the middle of a session (ordinaris 2,691).
  - 1,380 of them have a cue just before: a president line, a weekday-date line, attendance, a
    name list (the printed header), "Nihil actum est", or a page break. 964 of those cues look
    like a header.
  - 1,369 have no cue. The ones read are a new day straight after the previous day's last
    decision.
  - `gt_sessions` confirms only 95, 28 of them by manual annotation. Its start lines miss these
    boundaries too. Example: `session-3248-num-30` (1642) holds 17 meeting days over 80 pages.
- **Sessions missing from the resolution dump:** `step02/missing_sessions_from_links.csv` lists
  969 `gt_sessions` sessions that have text but no resolutions in republic's database. A
  resumption in the next session implies each of them took place.
- **Out-of-order session dates:** 44 rows in `step02/links.parquet` with
  `mismatch_class = date_error`.
- **Segmentation errors the user noted during the check.** Each is a paragraph ID; the scans are
  in `paragraphs.page_ids`.
  - The last line of the attendance list is merged into the resumption paragraph:
    `session-3304-num-152-para-2`, `session-4807-num-25-para-2`.
  - The resumption is merged with the following resolution: `session-3196-num-193-para-2`,
    `session-3196-num-244-para-2`, `session-3251-num-24-para-2`.
  - Line segmentation mixes the resumption with lines of the next resolution, and possibly of
    the adjacent column: `session-3791-num-64-para-19`, `session-3804-num-174-para-27`,
    `session-3345-num-131-para-3`.
  - Unclear, but probably line and paragraph segmentation together:
    `session-4813-num-1-para-237` (scan `NL-HaNA_1.01.02_4813_0143`).
  - A paragraph whose pages are out of order: `session-3196-num-141-para-33` starts on scan
    `NL-HaNA_1.01.02_3196_0248`, then runs through 0243–0247. After the resumption the text reads
    like index entries.

## 11. Record-boundary flags from proposition/decision segmentation (2026-10-25)

Republic Phase 3.2 (UA-067 part 2 (b)). Republic splits each resolution record of the 2026-03
DuckDB export into proposition, formula and decision spans, and flags records whose boundaries
look wrong upstream; it never repairs them. Full hand-over, with definitions, read examples and
scan IDs: `/Users/Shared/claude_projects/data_scopes/republic/output/phase-03-2/upstream_handover.md`
(written for republic-segmentation, which now does the repair). Write-up:
`republic/report/19-p3.2-segmentation.md`.

- **Flags:** `output/phase-03-2/step04/upstream_flags.parquet` (78,118 rows over 73,020 of
  726,594 records; counts per period in `upstream_flag_counts.csv`). 17 of 18 flagged records
  read are right.
  - `candidate_merge` (49,033): 2 or more items (formula plus decision) in one record. 15–16% of
    records in 1629–50, 7–11% for 1652–1702, 3–4% in print.
  - `starts_with_decision` (27,611): the record opens with a formula; its proposition is in
    another record or absent. 5–9% for 1581–1702, under 0.5% in print.
  - `day_log` (1,234): before 1638, 3 or more items in one record (a day's log).
  - `mask_overreach` (240): real items inside text that republic excluded as a resumption or
    header, a sign of merges with header or attendance text.
- **What the flags miss:** glued pre-1638 records without a recognised formula (about 22% of a
  60-record read), and second resolutions with no formula.
- **The user's notes from the scans** (three label checks, `step03/`, `step03c/` and
  `step04b/user_check_notes.md`): records holding 2 to 6 resolutions (for example
  `session-3285-num-164-resolution-2`, six, scans `NL-HaNA_1.01.02_3285_1001`–`1002`); the
  marginalia of a whole day attached to one record (`session-3181-num-20-resolution-4`, scans
  3181_0061–0062); a record missing its decision (`session-4830-num-206-resolution-4`, scan
  4830_0358). Before 1638 there are **decisionless resolutions** ("ter kennisgeving"), which are
  separate resolutions, not fragments. The user marked multi-resolution records systematically
  only for the first ~35 items of one page, so the notes undercount.
- **For §4's start cues:** a record may open with "In deliberatie geleijt sijnde" or "By resumtie
  gedelibereert zynde op …" (about 1.5% of resolutions from 1740); both open a resolution.
