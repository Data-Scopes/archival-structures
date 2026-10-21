# Sub-project: regenerate the SG hand and print inputs for the hand–print alignment (AS hand-off)

Audience: Opus (planning/review). Written 2026-10-17 after UA-012 (a) and UA-013 (b). Owner of the
regeneration: AS (archival-structures). LotA owns the consumer side (alignment, gold sheet).

## 1. Why

Step 2.7 part A (print–hand differences, inventories 455/456 ↔ 3349–3353) cannot be annotated on
its current inputs. Evidence: the user labelled 116 of 300 sheet rows (`gold/02d-…-rescaled.tsv`);
51 (44%) have a primary label that names an input artefact (`print_ocr_error` 21, `segmentation`
16, `hand_htr_error` 7, `misaligned` 4, `unclear` 3), and the notes repeat five causes:

1. Print: missing whitespace (words glued, e.g. "Datwydersnoch").
2. Print: hyphenated line-end words fused with the next line ("Cor-" + "respondentie").
3. Print: lines of two columns (Dutch/French) merged into one line, so paragraphs mix columns.
4. Hand: page numbers, date headers and marginalia inside the main-text region (rows 9, 19, 50, 52, 55, 80, 134).
5. Paragraphs not separated (row 284) and hand/print alignments that make no sense (44, 92, 188, 229).

The sheet's images are also wrong for a structural reason: crops are paragraph-level (print:
union of the paragraph's `line_start`/`line_end` boxes; hand: whole TextRegion) because the TSVs
carry no per-line geometry (see `STATUS.md`, 2026-10-17). Rescaling coordinates to full scan size
(`cli/rescale_sheet_coords.py`) was correct but could not fix this.

## 2. What AS is asked to produce

A new versioned layer (shared-data protocol: `<layer>_vN/` with `manifest.json`, producer AS,
inputs and counts), covering at least SG hand 3349, 3350, 3352, 3353 and print 1.10.94 455, 456,
scalable to all SG inventories (the AS audit already says the 1.01.02 TSVs are to be regenerated).

Requirements, in priority order:

- **R1 Line geometry.** One row per text line (or a sidecar): scan id, line id, bbox (in the
  coordinate space of `Page@imageWidth/Height`, plus the page size in the manifest so consumers
  can rescale to the full NA scan; the info JSONs are in LotA `data/scan_info/`), column side,
  region id, text, and the paragraph it belongs to. Paragraph text must be recoverable as the
  concatenation of its lines with character offsets per line, so that any token span can be mapped to
  line boxes. This is what lets a sheet row point at the actual lines.
- **R2 Column splitting.** Lines merged across two columns must be split before paragraphs are
  built. The reference method is republic
  `republic/parser/pagexml/long_line_splitter.py` (read, not run, by LotA): a line is "merged" when
  it is wider than `min_merged_width` and has at least `default_line_num_chars` characters;
  candidate split word positions are taken within `default_line_num_chars ± 15` characters (and a
  separate hyphenation case); each candidate is scored with NLTK bigram collocation scores
  against the neighbouring lines (previous right column, next left column; best = fewest negative
  scores, then the most positive sum); split coords/baselines are made per half and lines are then
  assigned to a left or right column by the column boundary. The user reports it works partly.
  AS decides whether to reuse it (it needs `pagexml`, `nltk`, `numpy`) or reimplement it; either way
  **report how many lines were split and how many merged lines remained unsplit**.
- **R3 Whitespace.** Detect and repair glued words in print (and in hand: row 299). The
  PageXML word elements and baseline/word geometry may carry spaces the line text lost; if so,
  use them; otherwise state the method (dictionary/n-gram segmentation) and its error rate.
  Keep a flag per line of what was changed.
- **R4 Region types.** `region_type` must not assume one text column (AS audit finding: 3771 has
  99.3% `margin`). Page numbers, running headers, date headers and marginalia must not be labelled
  `main_text`. The hand headers ("21. April", "29. Augti.") belong to the session-date logic in
  LotA (`corpus/parallel_text.py` section 7): keep them as their own type, not deleted.
- **R5 Stability.** Keep `doc_id` byte-identical to the current TSVs where a region is unchanged
  (LotA gold files key on it); a split or merged region gets a new id, with a mapping table old → new.
  The 6-column header is rejected by LotA's `tsv_source.py`; a changed schema needs a LotA
  update (LotA will write it; AS states the new schema).
- **R6 Manifest and audit.** Counts per inventory (scans, lines, regions, split lines, whitespace
  repairs, rows without text), the AS audit invariants, and the known gaps (5.32% of scans without a row, 20 files with suffixed sub-inventories).

## 3. Acceptance (what the user checks)

Run on the 116 labelled rows' scans first (455, 456, 3349–3353):

1. Of the 116 rows, how many are now free of the five artefacts? Target to be set by the user
   before the run; the baseline is 56% free of a primary artefact label.
2. Row-level check on 30 random rows plus the user's examples (file positions 39, 60, 241): the line
   boxes for a token span cover the text a human sees on the scan.
3. `find`-style invariants from the audit all pass; no write through existing symlinks; v1 inputs
   untouched.

The user decides when this is done.

## 4. LotA side after delivery (not started)

1. Update `tsv_source.py` and `corpus/parallel_text.py` for the new schema; map units to line boxes
   (replaces paragraph-level crops); keep the rescale to full NA scan size (`rescale_sheet_coords`).
2. Re-run the aligner unchanged (republic runs it on more years afterwards, UA-016 there; its
   output `print_hand_alignment_v1/v2` in shared-data was built on the old inputs and needs
   a v3 on the new layer).
3. Re-draw the 02d sheet, carry over labels for rows whose texts are unchanged (same hand and
   print tokens), and let the user re-label only the changed ones; report the carry-over rate.
4. Error analysis, as always: read the failures, not just the counts.

## 5. Interim (LotA only, no AS dependency)

Artefact filter for re-drawing a smaller sheet from the current inputs: drop units where either
side has a glued token over a length threshold, a print paragraph whose first and last line
boxes sit in different columns, a `full` page crop, or a hand region that is a header/page number. Use the user's 116 labels
to calibrate and score the filter (precision on the 51 artefact rows). Not started.

## 6. Open questions for the user

- Does AS have capacity to regenerate before Phase 9, or should LotA do the interim filter only?
  - Yes, AS has the capacity to regenerate.
- Is republic's `long_line_splitter.py` free to be reused by AS (cross-repo dependency), or should AS port it?
  - AS should port it so it becomes part of AS.
- Should the new layer cover all SG inventories now (the AS audit suggests so) or the six above first?
  - It should cover all SG inventories.