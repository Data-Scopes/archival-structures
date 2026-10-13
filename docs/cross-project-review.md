# Cross-project review: archival-structures, language-of-the-archive, nlp-start-page-detection

*Compiled 2026-10-11 from each project's docs, reports, plans and code. Abbreviations:*

- **AS** = this repo, `archival-structures`.
- **LotA** = `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/`. The path in the
  original request had `claude-projects` with a hyphen; the real directory uses an underscore.
- **NLP** = `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/`.

LotA and NLP already exchanged findings with each other on 2026-10-08:
`LotA/docs/from-nlp-start-page-detection.md` and `NLP/docs/from-language-of-the-archive.md`.
Neither covers AS beyond the EAD parser, so this review concentrates on **AS ↔ LotA** and
**AS ↔ NLP**. It adds to the LotA ↔ NLP exchange only where AS changes the picture.

Nothing in any of the three projects was modified while compiling it. The user's decisions on
section 10 (2026-10-13) are recorded in section 0, and the work done on them is in
`docs/shared_library/` and `ground_truth/README.md`.

---

## 0. Decisions (user, 2026-10-13)

Answers to the questions in section 10:

1. **AS becomes the shared library.** The plan for moving code in is in
   `docs/shared_library/upstream_inventory.md`.
2. The shared VOC text layer already exists. Its reusable tooling moves into the shared library
   for downstream work.
3. The user reviews and commits; sessions never commit. This is now in AS's `CLAUDE.md`.
4. **NLP's GLOBALISE data is leading.** The stale AS copies were replaced on 2026-10-13.
5. **All ground truth is staged centrally** under AS `ground_truth/` (catalogue in its
   `README.md`), and the siblings get a note telling them so.
6. **`NL-HaNA` (hyphen) is canonical.** Underscore names are renamed or replaced.
7. The user stages more Generale Missiven inventories from a list
   (`docs/staging/generale_missiven_inventories.csv`). The overview is used for partial labels
   as proposed in question 7.
8. **Audit the SG paragraph TSVs.** The user regenerates them, given a list of what to fix and
   what to watch for: `docs/audits/sg_paragraph_tsv_audit.md`.
9. **The granularity reading is a hypothesis to test.** Which archives and characteristics to
   stage is under discussion. Candidates are NL-AsdSAA 5075 (with some deed-level ground truth),
   NL-HaNA 2.10.50 and 2.10.01. NL-AsnDA is hard: image quality is low and ground truth is
   difficult to obtain.
10. **Framing B (pairwise) is the default**, but which framing suits which archives,
    inventories and tasks is itself a core research question, and there are framings beyond
    single pages and pairs. The user's hunch is that early modern archives tend to have
    sub-page document starts and more modern archives top-of-page starts.
11. The `„` word-break convention was observed at **line ends** in resolutions. Region ends
    have not been examined.
12. **The ground-truth format stays open.** Provenance is free-form, with recommended keys
    (`docs/ground_truth_format.md`).
13. **"Section" is ambiguous** and recursive across levels; it is now defined in the glossary
    (`docs/definitions/concepts-physical-to-logical-structure.md`).

---

## 1. Summary: the ten things that matter most

1. **AS has no document-level ground truth, but the other two projects together hold a lot of
   it.** AS's simulation harness says real canonical streams exist only at *section*
   granularity: 46% of SG files have a single section, so corruption Cases 2 and 4 are
   impossible there. Available elsewhere:
   - NLP: 2,367 VOC document starts (with sub-document levels) over 20,905 pages.
   - The GLOBALISE index: 227,526 documents in 4,981 VOC inventories.
   - NLP Step 4.1b's folio → scan alignment: silver documents for 380 inventories.
   - The user's SG sessions and resolutions (~120K and ~730K, with scan ids and region
     coordinates), which fill exactly the `session`/`resolution` layers in AS's
     `ground_truth_format.md`. That format defines those layers, but no file populates them.

   A converter from each source into `InventoryGroundTruth` is the single highest-leverage task
   across the three projects.
2. **NLP has a far more mature evaluation methodology than AS.** It has:
   - Pk, WindowDiff, ±1-tolerance F1, segment exact match, and worst-inventory and macro scores
     per inventory;
   - start-rate calibration and a bootstrap over inventories;
   - document-atomic chunk splits, nesting and leakage rules, at least 3 seeds, and trivial
     baselines.

   AS's `corruption/metrics.py` re-implements part of this (P/R/F1 and WindowDiff, no Pk), and
   only against an oracle and a single "nothing changes" baseline. NLP found that the
   majority-class baseline beats several real models on Pk/WindowDiff. AS needs that floor.
3. **NLP's negative results answer the proposal's question of "when does cheap unsupervised
   analysis suffice?"** — at least for VOC letter bundles, and only partly so far:
   - DINOv2, ink density and gap geometry each *hurt or failed to help* document-start
     detection.
   - Unsupervised segmentation (DP, TextTiling) scores below the majority class.
   - AS's DINOv2 work succeeds at a coarser level: section and sub-sequence boundaries, runs of
     homogeneous page types.

   This points to a **granularity rule**: vision and layout find section-level units, and text
   finds document-level ones. That rule is worth stating explicitly in AS's docs and in the
   position paper. The open question is whether it holds outside VOC letter bundles.
4. **Transitions beat starts.** NLP's biggest single gain, 0.682 → 0.732–0.768, came from
   modelling page *pairs*, which is AS's Framing B. Yet AS's `document_segmentation.md` makes
   Framing A the default. About 50% of VOC starts are mid-page, which is direct evidence for AS's
   still-stub sub-page segmentation section.
5. **LotA depends on AS code that is uncommitted.** LotA imports the following from AS, and all
   of these files are untracked (`??`) in AS's git status, with the package still at 0.3.0:
   - `ead_tree`, `ead_views.unit_of_description_frame`, `ead_fields`, `ead_digital`;
   - `InventoryNumber`, in about 27 modules.

   That is a reproducibility risk for LotA today.
6. **LotA has found real bugs in AS that are still unfixed in the working tree:**
   - `ead_tree._text_of` joins text with `itertext()`, which glues words across `<lb/>`, e.g.
     `'eerstedeel'`, `'derdenSeptember'`.
   - `make_iventory_paragraphs.py` has these problems:
     - hardcoded external-drive paths;
     - an SG-only scan-id regex;
     - per-line geometry is discarded;
     - scans with no text regions are dropped (5.08% of SG scans);
     - suffixed inventories (`4848A`, `5745A/B`) collapse into their base number;
     - `„` is used as a word break in SG.
7. **Part of LotA's code belongs upstream in AS, as the shared library:**
   - `element_text` (LotA's correct replacement for `_text_of`);
   - a stable EAD node id;
   - the scope-note (`<scopecontent>`) shape parser and reference extractor;
   - structural `<unitdate>` excision with character spans;
   - the region-quality filter, validated at keep P/R 0.97/0.97;
   - the MinHash duplicate/re-shoot detector, 13/13 documented SG re-shoots found.
8. **Part of NLP's code is generic and belongs in AS:**
   - `evaluation.py` and the chunk splits;
   - the sequence decoders: CRF, semi-Markov duration, BiLSTM;
   - the page-number / foliation alignment (longest-increasing chain plus interpolation);
   - the per-inventory word-break detector.

   The foliation tool is also the "external ordering signal" that AS's `page_order_problems.md`
   says is needed to catch document-level swaps.
9. **AS has unsupervised layout tools that neither sibling uses,** because they need line
   geometry and the siblings' text layers carry only region boxes. The tools are `text_extent`,
   `empty_regions`, `opening_detection`, `line_clustering` and `subsequence_detection`.
   - LotA reimplemented a page splitter, a layout/table classifier, a quality filter and a
     duplicate detector, despite its own rule D1 ("call upstream, never reimplement").
   - NLP reimplemented gap and table features, and its table heuristic still doesn't generalise
     (report/40).
   - The fix is either line geometry in the shared VOC text layer, or region-bbox-only entry
     points in AS.
10. **The same datasets exist in several copies:**
    - The GLOBALISE index exists only in NLP (`metadata/digitised_indexes/`). AS has no copy.
    - AS holds an **earlier Dataverse release** of the segmentation dataset, both zipped and
      extracted. It has 20 of 22 inventories; 1388 and 1529 were added in the release NLP
      has. 19 of the 20 shared files are byte-identical to NLP's. The exception is 1539, which
      still has the duplicate row you removed on 2026-09-30.
    - There were two different SG EAD files: an older AS-only export (7,024 `item`s, underscore
      prefix) and the hyphenated `NL-HaNA_1.01.02.xml` (7,026), which LotA pins. *Resolved
      2026-10-13:* the hyphenated file is canonical, and the old export is in
      `data/metadata/eads/superseded/`.

---

## 2. How the three projects map onto the proposal

| Proposal layer | AS | NLP | LotA |
|---|---|---|---|
| **Stage 1: unsupervised exploration** (stream analysis, stream clustering, sub-sequence identification, stratified sampling) | `stream_analysis/overview` (DINOv2 embeddings, layout features, UMAP+HDBSCAN, subsequence detection, cluster neighbourhood), `sequence_analysis`, `groundtruth/stratified_sampling`, `active_learning` | inventory fingerprints and Ward clusters, which recover the series hierarchy (Cramér's V 0.597) but don't predict model difficulty; EAD-stratified sampling | scan/text coverage, census, quality filter |
| **Information structure** (document elements, continuation) | line/page-layout clustering, relational patterns, `empty_regions`, `text_extent`, `sequence_patterns.detect_cross_page_continuation` | region classifier, region-relation and continuation classifiers (`src/region_relations/`), region-MIL cross-encoder | `layout_type` (running_text/list/table/other) |
| **Administrative structure** (document boundaries, ordering, nesting) | `boundary_detection`, corruption harness + metrics, ground-truth format with section/session/resolution layers, `boundary_annotation` | the whole project: start pages, CRF/semi-Markov/BiLSTM, boundary levels (session/document/insertion), GLOBALISE alignment | (out of scope by design; consumes document units in Phase 9) |
| **Archival structure** (EAD, linking to finding aids) | `parsers/ead_*` rewrite: 47 finding aids, 20 repositories, D1–D23 | EAD metadata extraction via AS's *legacy* `ead_parser` | EAD hierarchy, scope-note parsing, title normalisation, facets, associative relations, digitisation coverage |
| **Hierarchical integration / toolkit** | the package itself | — | — |

On this mapping, AS is the natural home for the reusable toolkit that the proposal's 7b
("modular, interoperable techniques") describes. NLP and LotA are research applications that
generate methods and evidence. Much of the advice below follows from taking that seriously.

---

## 3. NLP → AS

### 3.1 Evaluation methodology (adopt first)

- **`NLP/src/startpage/evaluation.py`** provides: `pk`, `window_diff`, `tolerance_prf` (±k),
  `segment_exact_match`, per-inventory macro and worst-inventory scores,
  `start_rate_calibration`, a bootstrap CI over inventories, and `evaluate_all`.
  - It works on generic label sequences and handles chunk edges via `positions`/`is_last_page`.
  - Target: a new `stream_analysis/evaluation.py`, or a merge into `corruption/metrics.py`.
    AS currently has P/R/F1 and WindowDiff but no Pk; `simulation_harness.md` lists Pk as
    "scoped but not implemented".
  - Reconcile the two WindowDiff implementations: check that they agree on a shared fixture.
- **Splits:** `long_subsequence_chunk_split.py` and `splits.py` give document-atomic contiguous
  chunks with context padding (`role = core/context`), plus validators: no document crosses
  partitions, no duplicate ids. AS has no split methodology. Anything trained on top of the
  harness will need one.
- **Trivial baselines:** `scripts/run_trivial_baselines.py` has majority class, every-k, and a
  length prior. In NLP the majority class reaches Pk/WindowDiff 0.303 and beats real models.
  Every AS harness result should report these floors.
- **Leakage rules** (`NLP/CLAUDE.md`):
  - Any component model's training split must nest inside the downstream split, and this should
    be asserted in code. report/20's 0.822 collapsed to 0.635 without it.
  - Never compute "pages since last start" from ground truth.
  - Per-inventory centring is allowed; label-dependent normalisation is not.

  These belong in AS's `docs/challenges/` as standing methodology.
- **Held-out by inventory.** NLP's held-out-inventory F1 is 0.42–0.56, against 0.80 within
  inventory. Any AS claim about boundary detection across inventories should be checked under an
  inventory hold-out.

### 3.2 Generic sequence code

- `crf_sequence.py` (linear-chain CRF), `semi_markov.py` (lognormal durations,
  `decode_semi_markov`) and `bilstm_sequence.py` all work over any per-page score sequence. They
  would sit next to `stream_analysis/sequence_analysis.py`, which has transition matrices and
  run-length encoding (RLE) but no decoder.
- Finding: four different forms of sequence structure all landed at 0.78–0.80 primary F1,
  inside the inventory-bootstrap CI of ±0.14. So the decoder choice matters less than the
  per-page and per-pair evidence.

### 3.3 Page numbering and foliation (AS has nothing here)

- `globalise_alignment.py` takes the longest increasing chain of HTR-detected page numbers,
  interpolates folio → scan, detects recto/verso numbering mode, and assigns confidence bands.
  - Accuracy within ±1 scan: 91.3% on dev, 74.1% on the report set, and 93.5% in the
    high-confidence band.
- From report/40:
  - 280 of ~412 inventories foliate by leaf (step 2).
  - Against each inventory's own step, transitions are 52.8% ok, 29.4% skip and 16.2% reset.
  - Catchword chains break 48.1% of the time unless matching is prefix-aware.
- Proposed AS module: `analysis/page_numbering.py`. Uses:
  - the `page_num` ↔ scan mapping in the ground-truth format;
  - the external ordering signal that `page_order_problems.md` asks for under document-level
    swaps;
  - an ordering-principle detector, which the proposal names under Stage 2 ("identifying
    ordering principles").

### 3.4 Realistic corruptions

- AS's `corruption/operators.py` has: intra-document swap, foreign insertion, swap plus
  insertion, and document-level swap.
- Real faults that NLP observed but AS lacks operators for:
  - **duplicate scans and re-shoots:** 7602 pp. 28–29 repeat 26–27; there are also 84
    `SAME AS` pointers in 9 inventories;
  - dropped pages;
  - numbering restarts and separator sheets (report/76: restarts 7, duplicate numbers 5,
    separator sheets 9).
- NLP's report/18 found that `order_swapped` is the hardest negative for continuation models:
  0.43–0.47 accuracy, against 0.74–0.84 for `different_doc`. So intra-document swaps are the
  hard case to detect, which supports AS's Case 1 analysis.

### 3.5 Findings for AS's `findings.md` and challenge docs

- **Sub-page segmentation is the common case, not an edge case.**
  - About 50% of VOC starts are mid-page.
  - Per inventory this ranges from 0% to 82%; mid-page starts are concentrated in copy-books
    (2548: 82%, 3142: 80%, 1539: 74%).
  - Region-MIL places 73% of transitions on the correct side of the page boundary from text
    alone.

  This should replace the "stub" status in `document_segmentation.md`'s sub-page section.
- **Nesting:**
  - VOC copy-resolutions nest session → resolution → inserted letter (report/32).
  - Ledgers need an "account" unit rather than a page (report/38).
  - Session-header precision is only 0.011–0.019 on 2555 (report/66).
- **Genre mix of the 22 inventories:** 83% letters or unclassified, 7.4% ledger, 5.1% translated
  letters (report/22). None are resolution books.
- **Text cues at boundaries:**
  - "Extract…" as the first word (report/40).
  - A closing-formula regex fires on 73% of END pages against 14% of INSIDE pages (report/73).
  - Header-tag mining identifies the series convention: session-dated, letter-book, court,
    journal (reports 34–35).
- **Fuzzy matching threshold is 0.85 on VOC.** Lower values bring back a false-positive class
  diagnosed three times (reports 40, 42, 43). Relevant to the proposal's formula-detection and
  variant-handling work.

### 3.6 Schema

- NLP's `docs/annotation_schema.md` adds:
  - `Boundary level` (session/document/insertion);
  - `Structure unit` (account);
  - `Annotation provenance` (manual/model_verified/silver).
- AS's `annotation/ground_truth.py` has no provenance field. The proposal (7c) explicitly
  requires recording "which boundaries were detected automatically, which were verified
  manually, what confidence levels were assigned". Add `provenance` and `confidence` per
  element, and define one mapping from NLP's CSV to AS's YAML.

---

## 4. LotA → AS

### 4.1 Bugs to fix in AS (checked against the current working tree)

| Where | Problem | LotA fix to port |
|---|---|---|
| `parsers/ead_tree.py:211` `_text_of`; the equivalent in `ead_fields.py` | `itertext()` joining glues words across `<lb/>` (9 glued SG titles, e.g. `'eerstedeel'`, the scope note `"Kopie.Exh. St.-Gen."`). It also drops the space before `<unitdate>`: `'derdenSeptember'`, 36 SG nodes. This also affects the `ead_views` `title`/`path` columns | `LotA/archival_language/ead/scopecontent_survey.element_text`; `facets/title_norm.py` (3.4 F4) |
| `make_iventory_paragraphs.py` | hardcoded external-drive (`T7_Data`) paths (lines 176–193) | argparse or config |
| same, line 110 | `NL-HaNA_1.01.02` scan-id regex fallback, and a stray `print(scan.id)` | `LotA/ead/adapters.py` scan-id parsing |
| same, line 163 | per-line geometry discarded in `docs_to_paragraph_file` | keep the line bboxes, plus the first and last line text that NLP's continuation pairs need |
| same | scans with no text regions dropped (5.08% of SG scans; the TSVs undercount scans by 5.32%) | emit an explicit empty-scan row. AS's own `boundary_detection` depends on empty pages, so this script deletes AS's own signal |
| same | inventory id taken from the bare directory name, which collapses suffixed inventories (8 SG files hold 1.8M words that belong to `4848A`, `5745A/B`, …) | resolve through `InventoryNumber` |
| same, line 93 | `word_break_chars=['-','„']` for SG; LotA measured that SG `„` is terminal punctuation at region ends (8 of 10 read) | per-inventory detection (see §5.3); first decide whether line-end and region-end behave differently |
| `pagexml-tools` (inherited) | `get_page_main_text_range` degenerates to `Interval(0, 99999)` on pages built by `split_scan_pages`. The user has ruled this a design decision: regions belong under `.columns`/`.extra` | build pages so regions sit under columns/extra. **Audited 2026-10-13** (`docs/audits/sg_paragraph_tsv_audit.md`): only the AS `data/sg_letters/` copy has the fallback labels. The LotA copy uses a real range that assumes one text column, so it fails on two-column printed resolutions (e.g. 3771: 99.3% of tokens labelled `margin`) |

### 4.2 Code to upstream into AS

- **A stable node id**, `f"{archive}:{index_path}"` (`LotA/model/node.py`), into `ead_tree` and
  `ead_views`.
  - Today `EADComponent` has no id and is unhashable, its equality check recurses over children,
    and `unit_of_description_frame` keys on `id()`.
- **The scope-note parser** (`LotA/ead/scopecontent.py`, 15 shapes plus reference extraction),
  as `ead_fields.parse_scope_notes`.
  - This is AS's referential structure: VOC scope notes are 98.6% `cross_reference` ("zie inv.
    nr. N").
  - **First fix LotA's own bug:** 83 VOC notes point into a *different* archive ("Aanwinsten,
    1.11.01.01, inv.nr. 687") and are extracted as bare inventory numbers, so they would
    mis-join to VOC 687.
- **Structural `<unitdate>` excision with character spans** (`title_norm.py`), into
  `ead_fields`. The `str.find` approach left 6,264 VOC dates ambiguous and 125 not locatable.
- **Digitisation evidence declared per archive** (`ArchiveConfig.digitisation_mechanisms`), into
  `ead_digital`, as an alternative to a single `is_digitised`.
- **Region-quality filter** (`corpus/quality.py`: usable/fragment/noise). It was tuned on
  half of a 150-row gold set and reaches keep P/R 0.97/0.97 on the other 75 held-out rows
  (corrected 2026-10-13; an earlier draft said 300 gold regions). `numeric_content_exempt`
  fixes a 3× over-prediction of noise on 1–3-token regions. It targets AS's
  `text_analysis`/`token_analysis`, whose running-text checks are unvalidated.
- **Duplicate/re-shoot detector** (`corpus/integrity.py`).
  - Method: MinHash LSH plus confirmation by 5-gram Jaccard ≥ 0.6.
  - It found 13 of 13 documented SG re-shoots.
  - Every adjacent-scan pair is *added* as an extra candidate next to the MinHash candidates
    (corrected 2026-10-13; an earlier draft said the search was restricted to adjacent scans).
  - In AS it pairs naturally with a new duplication corruption operator (§3.4).

### 4.3 LotA findings that answer AS's open EAD questions (`docs/ead_parser/status.md`)

- **"What does a handle on every component mean?"** At the Nationaal Archief, a handle sits on
  17,936 of 17,937 SG units and on all 16,447 VOC units. So it is assigned when the inventory
  number is created, and is no evidence of digitisation.
  - `<dao>` (a METS manifest) gives SG 60.7%.
  - `<dao>` means images exist, not text: 1,748 SG `dao` inventories have no HTR.
- **item/subfile:** `unit_of_description_frame` omits 7,026 SG `item` and 2,222 `subfile`
  components, which describe individual documents.
  - These matter for AS's open question "is a digitised document an `item` component or a run of
    assets?". They are document-level ground truth from the finding aid itself.
  - LotA records the user's ruling that "filegroups aren't digitised".
- **Empty `<unittitle/>`:** 542 of 34,291 in NL-AsdSAA_5075 and 27 of 187 in AsdSAA_89.
- **Levels carry facets:** in AsdSAA_5075 the `series` level *is* the notary's name, so facet
  extraction must read the ancestor path. This connects AS's D8 ("levels are data") to LotA's
  associative-relations work.

### 4.4 Corpus findings for AS's analysis modules

- **`opening_detection` `min_share`:** portrait single leaves are 1.21% of SG scans (30,627).
  Every inventory has some, and 96.6% have exactly 2 or 4 (covers). That is a principled
  calibration for the `min_share` problem in `findings.md`, where the default of 0.15 fails at
  2 of 630.
- **Empty pages:** LotA's per-scan list of SG scans with no text regions (5.08% of all SG) is a
  ready input for `boundary_detection.empty_page_transitions` at full-archive scale.
- **Blank page ≠ zero-region page:**
  - 40.8% of VOC `Empty`/`Cover` scans produce at least one region;
  - 0.78% of other scans produce none.

  This argues for AS's image-side empty-page check alongside the PageXML one.
- **Reading order:** LotA found 64.8% of cross-region joins implausible.
  `analysis/sequence_patterns.build_line_sequence` explicitly trusts PageXML region/line order,
  per its docstring. LotA and NLP both decided to ignore PageXML reading order. AS should either
  derive the order geometrically or document the risk.
- **Layout fixtures with free ground truth:**
  - VOC 9190: ledger on pp. 69–108.
  - VOC 8030: muster roll pp. 878–941, list pp. 945–948, tables from p. 950.
  - SG branch `1.01.02:1.1.0`.

  These test AS's `text_extent` table-vs-running-text separation (`mean_width`/`equal_frac`),
  which was validated on 6 inventories only.

---

## 5. AS → NLP

### 5.1 The EAD parser upgrade

- NLP calls only AS's legacy `ead_parser.get_inventory_info(max_subseries_depth=…)`, in
  `scripts/extract_ead_inventory_metadata{,_full}.py`.
- It worked around depth limits in report/33, but VOC 1.04.02 is six levels deep.
- Switch to `ead_tree.parse_ead` plus `ead_views.unit_of_description_frame`, with
  `legacy_compatible_frame` as a bridge.
- Also stop deduplicating on inventory number: 796 VOC inventory numbers sit on more than one EAD
  node; VOC 8362 binds ten letter series into one volume.

### 5.2 Ground-truth and robustness tooling

- `stream_analysis/corruption` (simulate, ARI): NLP has no page-order robustness testing. Once a
  converter from the 22 annotations to `InventoryGroundTruth` exists, the harness can run on
  NLP's data directly. With about 8.6 pages per document, these are the short-and-numerous
  documents that AS's harness says it lacks.
- `groundtruth/boundary_annotation.annotate_page_boundaries` (sequential pairwise annotation)
  and `active_learning.ActiveLearner` match NLP's planned GT extension
  (`docs/ground_truth_extension_spec.md` Task D) and report/66's verify-or-reject candidates.
- `groundtruth/stratified_sampling` (per cluster) complements NLP's EAD-stratified
  `select_ead_sample.py`.

### 5.3 Layout

- NLP's 1-D `layout_features.compute_region_gap_features` is a simpler version of
  `empty_regions.compute_page_empty_regions` and `text_extent.classify_text_extent`.
- NLP's table heuristic `flag_table_like_regions` is only partly fixed (report/30) and doesn't
  generalise (report/40). AS's `mean_width`/`equal_frac` separated table registers from running
  text with no overlap.
- **Caveat:** NLP's evidence says geometry adds nothing on top of a text cross-encoder. So this
  is mainly useful for the *genre* question (ledger vs. prose), not for start detection itself.
- `early_end` (`text_extent`) could be tested directly against NLP's END labels; see §7.2.

### 5.4 Not relevant to NLP

`opening_detection` (VOC scans are single pages), colour/ink clustering, Allen relations.

---

## 6. AS → LotA

- **`opening_detection.find_width_groups`/`classify_page_types`** need only scan widths, which
  LotA's `scan_stats` has. This would fix the 30,627 portrait leaves that the bbox-ladder page
  splitter wrongly splits, and give an independent baseline for 2.3B.
  - LotA's `corpus/integrity.page_units` was validated on one inventory, with a straddle rate up
    to 57.6%.
- **`empty_regions.compute_page_empty_regions`** works from region boxes plus page dimensions,
  which LotA can construct from TSV `doc_id` bboxes and `scan_stats`. This gives Phase 9 boundary
  evidence without line geometry.
- **`text_extent`** needs line geometry, so it is only feasible on the 504 staged VOC PageXML
  zips. Those are exactly where LotA's ledger-gap question sits.
- **`subsequence_detection`** suits mixed volumes (VOC 8362; 1,473 GLOBALISE inventories mix
  settlements), but needs images, which LotA hasn't staged.
- **`datasets/vocabulary.py`** `generic:` types (running_text, table, list, marginalia,
  numbering, quire_marks) nearly coincide with LotA's layout classes and the Laypa tags. A
  single tag vocabulary across the three projects is within reach. The `main_text`/`margin` ↔
  Laypa tag mapping that LotA and NLP still have to decide could be expressed in it.
- **AS's numbered-decision log** (EAD status D1–D23) is already LotA's model. LotA's step-brief
  and gold-sheet conventions (README with decision procedure, calibration cases without
  suggested labels, tune/held-out split declared up front, `-done.tsv`) would be worth adopting
  in AS for its own annotation rounds.

---

## 7. Tensions and contradictions

1. **DINOv2.** AS's `stream_analysis.md` calls DINOv2 "recommended" and adjacent similarity "a
   strong signal for visual transitions between document types". NLP found DINOv2 lowered F1
   from 0.635 to 0.57–0.58 for document starts (report/13). Both can be true at different
   granularities: AS's successes are section and sub-sequence boundaries (NL-HaNA_2.10.50_1:
   12 of 12 empty pages followed by the same cluster). AS's docs should state the granularity.
2. **`early_end` as a boundary marker.**
   - AS's `findings.md` asserts this from 2 letter-copy books and 1 deed volume, with no ground
     truth.
   - NLP shows geometry alone scores 0.356, and mid- vs top-of-page F1 0.76, but adds nothing on
     top of text.
   - NLP's END labels can test AS's claim directly. Until then, mark it as a hypothesis.
3. **Per-inventory thresholds.** AS's `suggest_threshold` uses a per-inventory percentile. NLP
   found per-volume calibration negative three times. It also found that the *given* start rate
   (e.g. from GLOBALISE) is the only quantity that predicts held-out F1 (rho +0.579).
4. **Framing A vs B.** See §1.4. The evidence favours making Framing B (plus sub-page) the
   default in `document_segmentation.md`.
5. **The word-break character `„` in SG.**
   - AS's paragraph script treats it as a word break.
   - LotA reads it as terminal punctuation at region ends.
   - NLP finds it *is* a prose line-wrap mark in VOC, but most region-final `„` are money
     notation (`8835„5„`).

   Line-end and region-end behaviour probably differ. This needs one measured answer per archive
   before any SG regeneration.
6. **LotA's D1 vs practice.** "Layout is called from AS, never reimplemented" doesn't hold, for
   a structural reason: AS's tools need line geometry and the TSV/VOC layers don't carry it.
   Either the shared VOC layer carries line geometry, or AS grows region-bbox-only entry points,
   or D1 is reworded.
7. **Decision numbering.** All three projects use D1, D2, …, and LotA already has to write
   "upstream D17". A project prefix would remove the ambiguity: `AS-EAD-D17`, `LotA-D1`,
   `NLP-…`.
8. **Stale cross-references.** LotA's `CONTEXT.md` §5 cites AS status D1–D21 and a
   "40-finding-aid corpus"; AS is now at D23 and 47 finding aids.

---

## 8. Shared data inventory (duplicates and gaps)

| Dataset | Where | Issue |
|---|---|---|
| GLOBALISE digitised VOC index (227,526 documents) | `NLP/metadata/digitised_indexes/` only | **AS has no copy** (corrected 2026-10-11: an earlier draft placed it in `doi-10.34894-xaj12c/`, which is the segmentation dataset) |
| GLOBALISE VOC document segmentation (the 22 annotated inventories) | `AS/ground_truth/GLOBALISE/doi-10.34894-xaj12c.zip` plus its extracted folder, `NLP/annotations/` | **AS has the earlier release:** 20 CSVs (no 1388, 1529); zip and extracted folder are identical. 19/20 match NLP byte-for-byte; 1539 still has the duplicate row removed 2026-09-30 |
| Overview of the *Generale Missiven* in 1.04.02 (`doi-10.34894-srdmfu`) | AS only | Unused in all three projects. **It is document-level ground truth with explicit ends:** 950 missives (1614–1793) across 549 inventories, each with start/end scan, start/end folio, date and TANAP description. Median 1 missive per inventory, median span 49 scans. Overlap: none with NLP's 22 annotated inventories, and 29 missives in 21 of NLP's 499 text-layer inventories. Known noise: 76/924 end-scan filenames disagree with the `Eindscan` number, 23 adjacent missives overlap in scans, and a manual-check column flags duplicates and undigitised items |
| SG sessions (~120K) and resolutions (~730K), with scan ids and region coordinates | user's holdings; not staged anywhere | would populate AS's `session`/`resolution` layers, unblock LotA 2.3B, and give AS sub-page ground truth |
| SG section/title-page ground truth (633 YAMLs) | AS `data/ground_truth/` | could seed LotA's SG page-type strata |
| SG EAD | `NL-HaNA_1.01.02.xml` in AS and LotA (identical) | *resolved 2026-10-13*; old 7,024-item export moved to `superseded/` |
| SG paragraph TSVs | AS `data/sg_letters/`; LotA `data/paragraphs/` | both generated by the script in §4.1, so both inherit its defects |
| VOC region layer (1.92M regions, Laypa tags) | `NLP/data/paragraphs/region_paragraphs.tsv.gz` | LotA and NLP decided to share one VOC layer; AS could be its producer (§9) |
| FastText model | NLP `resources/fasttext/`, symlinked by LotA | fine |

---

## 9. Suggested order of work

1. **Stabilise AS as the shared base.**
   - Commit the new parsers, corruption, boundary-annotation and test files, and tag a version.
     LotA's imports should pin it.
   - Fix `_text_of` using LotA's `element_text`, and coordinate with LotA's 3.4 compensation so
     the space isn't restored twice.
   - Add the stable node id.
2. **Ground-truth converters into `InventoryGroundTruth`, with provenance:**
   - NLP's 22 annotation CSVs → a `document` layer, with sub-document boundary levels;
   - GLOBALISE index + NLP 4.1b alignment → silver `document` layer;
   - SG sessions/resolutions → `session`/`resolution` layers, once staged.
3. **Evaluation:** move NLP's `evaluation.py`, chunk splits and trivial baselines into AS. Rerun
   the corruption harness on the new layers with the full metric suite and floors.
4. **Upstream the generic tools:**
   - from NLP: the foliation/page-number alignment, the word-break detector, the sequence
     decoders;
   - from LotA: the scope-note parser, the quality filter, the duplicate detector (plus a
     duplication corruption operator).
5. **Rewrite `make_iventory_paragraphs.py`** as the producer of the shared text layer (VOC and
   possibly SG). It should keep line geometry, first and last line text, empty scans, suffixed
   inventory ids, Laypa tags, and a word-break provenance column. That resolves D1's tension
   and gives AS's layout tools to both siblings.
6. **Docs:**
   - qualify the DINOv2 and `early_end` claims;
   - promote sub-page segmentation and Framing B in the challenge docs;
   - add NLP's leakage and split rules as standing methodology;
   - record the granularity rule from §1.3 as a finding.

---

## 10. Questions for you

**Project architecture**

1. Should AS become the shared library that NLP and LotA depend on? It would hold the generic
   evaluation, splits, decoders, foliation alignment, quality filter, duplicate detector, EAD
   scope-note parser and node id. The siblings would keep only archive-specific research code.
   Or do you prefer each project to stay self-contained, copying code across?
2. If yes: should AS (step 9.5) produce the shared VOC text layer that LotA and NLP agreed on,
   instead of LotA's Phase 7? And should that layer carry per-line geometry, so AS's layout
   tools become usable downstream?
3. AS's new parser, corruption and test files are uncommitted. Will you review and commit them
   (as in the sibling projects, "never `git commit`"), or should a session do it? Should AS adopt
   the siblings' rule?

**Data**

4. Which copy of the GLOBALISE data is canonical? Should AS's stale annotation zip be replaced
   with NLP's current `annotations/`?
5. Can the SG sessions/resolutions be staged somewhere AS can read them, given that LotA's D8
   says "under LotA's `data/` only"? Should AS own the converted ground-truth YAMLs and LotA
   read those?
6. Which SG EAD copy is canonical: AS's older underscore-named export or LotA's hyphenated one?
7. How do you want to use the *Generale Missiven* overview (`doi-10.34894-srdmfu`)? Its labels
   are partial: one document type, not every boundary in a volume. So it can measure
   **recall** of starts *and ends*, but not precision, across hundreds of inventories outside
   the 22. That makes it a direct test for NLP's `early_end` problem and for generalisation,
   and a clean "missive" unit for LotA. It would also be a natural first non-SG
   `InventoryGroundTruth` converter for AS, with a `partial` coverage flag. Only 21 of its
   inventories have NLP text today, so most use would need new HTR/PageXML staged.
8. Should the existing SG paragraph TSVs be audited or regenerated, given the `region_type`
   fallback and the dropped empty scans? Both LotA's SG vocabulary counts and AS's
   `data/sg_letters/` depend on them.

**Methodology**

9. Do you accept the granularity reading in §1.3 (vision and layout for sections, text for
   documents) as a working hypothesis? Which archive outside VOC letter bundles would you test
   it on first? NL-AsnDA notary deeds would be a natural candidate once they have
   document-level ground truth.
10. Should Framing B (pairwise) become the default in AS's segmentation docs and harness?
11. For `„` in SG: is your existing "well established" SG word-break convention (LotA D2) about
    line ends, while LotA's measurement was at region ends? If so, the contradiction may only be
    apparent.
12. For provenance in the ground-truth format: are NLP's three levels
    (manual/model_verified/silver) enough, or do you want a free-form method, parameters and
    confidence record, as the proposal's 7c and Arakelyan et al. suggest?

**Access**

13. *Resolved 2026-10-11:* permissions were opened up, and everything this report cites has now
    been read. The open question is about terminology.

    **Glossary check.** The glossary defines *Document* as an intellectually coherent unit that
    may be one of many in a bound volume, which matches how this report uses "document". It has
    no entry for the AS sense of *section*: a run of pages within an inventory that the corruption
    harness and the ground-truth format treat as a unit, coarser than a document (e.g. an SG
    session block). It mentions sections only as a document element (§2) and in DAR's
    zones→sections→documents nesting (§6), where a section sits *below* a document. That is the
    opposite direction to the harness's usage. A glossary entry for the cross-document "section"
    would settle this. It matters for the granularity reading in §1.3 and question 9.
