# Audit: States General paragraph TSVs (NL-HaNA 1.01.02)

Date: 2026-10-13. Scope: the per-inventory files `paragraphs-NL-HaNA_1.01.02_<inv>.tsv.gz` made by
`make_iventory_paragraphs.py` (AS repository root). This audit only diagnoses. It changed no data
and no code.

**Terms used below.**
- A *row* is one PageXML text region. The files are called "paragraphs", but each row is a text
  region, not a paragraph.
- `region_type` is the script's two-way label, `main_text` or `margin`.
- A *main-text range* is the horizontal interval `[left, right]` that `get_page_main_text_range`
  estimates for each page side (`even`/`odd`). A region gets `main_text` when it overlaps the box
  formed by this range and a vertical band (from the top fraction to the bottom fraction of the
  page) by more than 70%.
- The *degenerate range* is the fallback `Interval(0, 99999)`. With it, only the vertical band
  decides the label.
- A *word break* (hyphenation) is a word split across two lines and marked with a character
  (`-`, `„`, `=`, `¬`).

**Copies audited.**

| Copy | Files | Rows | Columns |
|---|---|---|---|
| LotA `data/paragraphs/NL-HaNA_1.01.02/` | 9,122 | 20,601,168 (2,394,169 scans; 876 M whitespace tokens) | the script's 6 columns |
| AS `data/sg_letters/` | 869 (inv. 11118–12082, all also in LotA) | 3,825,058 | 8 columns: `lang`, `prob` added by `notebooks/detect-letter-language.ipynb` (lingua) |

The two copies have identical `doc_id` sets and identical `text` for all 3,825,058 shared rows. They
differ in row order, and they disagree on `region_type` for **702,566 rows (18.4%)**, almost all
(701,279) labelled `main_text` in AS and `margin` in LotA. Section 1, D1 explains why.

**Reproduction.** I re-ran the script's own functions on all 8 SG inventories whose PageXML is
local (`data/PageXML/NL-HaNA/NL-HaNA_1.01.02/`: 3771–3773, 3821–3823, 11182, 11281).
- Using a snapshot of pagexml-tools commit `a997bb9` (2025-05-26, the first commit that contains
  `inventory_stats`), the run reproduces the LotA copy **exactly** for all 8 inventories: 99,772
  rows, 100% identical `doc_id`, `text` and `region_type`.
- With the installed pagexml-tools 0.8.1 (`.venv`), the script **crashes**: `KeyError:
  'text_regions'` at `page.stats['words']` (line 33). With that check patched out, it produces the
  degenerate range.

---

## 1. Summary of defects

| # | Defect | Evidence | Severity | Fix |
|---|---|---|---|---|
| D1a | **The LotA labels use a real but single-column main-text range, and this mislabels pages with two text columns.** | The LotA labels are not a vertical-band fallback. Region x-centres (from `doc_id` bboxes, relative to the page half) differ by label: 88% of `main_text` regions fall at 0.4–0.8 of the page width, while 48% of `margin` regions fall at 0.0–0.3. But in inventory 3771 the fitted ranges are even 824–1701 and odd 677–1553, on printed pages with two text columns per page. As a result, **87.2% of rows and 99.3% of tokens are `margin`**. On scan `3771_0100`, the page numbers `( 152 )` and `153 )` are `main_text`, while the resolution text itself ("Ontfangen een Missive van den Heere…") is `margin`. Corpus-wide, **803 inventories (8.8%) have more than 50% of their tokens labelled `margin`, and 323 have more than 90%**. Together these hold 9.8% of all tokens; 276 of the 803 are numbered 3096–3870. Five inventories have under 1% `margin` rows (12136, 3642, 3646, 4822, 8330). These may have fallen back to the degenerate range (unverified). | High for any use of `region_type` as "body vs marginalia" | Do not use `region_type` as a body/margin label until it is recomputed with a method that allows multiple columns. Keep the old value only under a new name (see §2). |
| D1b | **The AS `data/sg_letters` labels are the vertical-band fallback.** Any regeneration with the current pagexml-tools also gives the fallback. | AS copy: 3.9% `margin` overall, and 282 of 869 files have under 1% `margin`. For 11182 the AS labels agree **99.73%** with my 0.8.1 (degenerate) re-run, and the LotA labels agree 100% with the `a997bb9` re-run. Under 0.8.1, inventory 3771 becomes 11,694 of 11,696 `main_text`, which agrees with LotA only 12.8%; 3821 agrees 29.6%. The cause is that `PageXMLPage.regions` in 0.8.x reads only `columns`/`extra`, while `split_scan_pages` puts regions in `text_regions`. | High: two incompatible label sets are circulating under one column name | Build pages so that regions sit under `columns`/`extra` (the user's ruling), or pass `page.text_regions` explicitly. Record the method in a provenance column. |
| D2 | **Scans without text are dropped.** | **134,633 of 2,528,802 scans (5.32%)** in `data/scan_stats-1.01.02` have no row: 128,410 have zero text regions (5.08%) and 6,223 have regions but zero words (0.25%). 9,052 of 9,122 files are affected; for example, inventory 100 lacks 109 of 737 scans. No TSV scan is missing from the stats (0). In addition, **434,969 text-less regions (2.07% of regions)** are dropped. In 3771 this is exact: 11,894 regions minus 198 text-less ones gives 11,696 rows. | High for boundary detection and coverage statistics | Emit one explicit row per scan without text, and per text-less region if wanted (§2). |
| D3 | **Suffixed inventories collapse into the bare number.** | **20 files hold 56,119 rows whose `scan_id` belongs to a different, suffixed inventory**, but the `inv_num` column carries the bare number. Examples: 7462 holds 7462A and 7462B only, 14,413 rows. 12495 holds A–D. 7366 holds 12 sub-inventories, A–L. 9420 mixes 485 rows of 9420 with 5,742 rows of 9420A. 8828, 8819, 11086, 9368, 4859 and others also mix the bare and the suffixed inventory. 4848 is worse: its scan ids carry no suffix at all, although the EAD (Encoded Archival Description, the finding aid) has 4848A (LotA `STATUS.md`). That case cannot be recovered from the `scan_id`. Because the main-text range and the rescale factor are fitted per directory, mixed inventories also share one layout fit. | Medium–high (facet joins, per-inventory statistics) | Take the inventory id from the scan id and validate it against the EAD (`InventoryNumber`). Write one file per real inventory. |
| D4 | **The `„` handling is right at line ends but blind to the other break conventions.** The apparent contradiction with LotA is explained below the table. | The script calls `make_text_region_text(word_break_chars=['-','„'])` without a word-break detector. Every non-final line that ends in `-` or `„` is joined to the next line unconditionally, the mark is deleted, and no space is inserted. A leading `„` on the next line is removed only if the previous line ended in `„`. **The last line of a region passes through untouched.** Local PageXML measurements are in the `„` subsection below. In the TSV text, **204,470 rows end in `„`**; 94.2% of these follow a letter and 1.6% a digit. Unmerged breaks remain: **484,876** occurrences of ` „<lowercase>` (a line-initial continuation mark that survived, e.g. `voorge= „komen`, `geem, „ploieert`), **570,788** occurrences of `<lower>= <lower>` (`=` is not treated as a break character), and 29,057 of `= „`. These are pattern counts; I checked samples but did not validate them by hand. | Medium (token counts, n-grams and vocabulary) | Detect breaks per inventory (see the `„` subsection). Record which characters were merged in a provenance column, and keep the raw line text. |
| D5 | **Line geometry is discarded.** | `line_ranges` (the character offsets of each line) is computed at line 93 and then thrown away. The original PageXML `TextRegion/@id` is overwritten with a bbox-based id. The full list of lost fields is in the D5 subsection. | High for layout analysis, and for NLP continuation pairs | Write a companion lines file (§2). |
| D6 | **The script cannot be reproduced.** | It hardcodes paths on the external drive `T7_Data` (lines 176, 190, 193). It takes the archive and inventory from a fixed path position (`split('/')[7:9]`, line 177). The EAD-based selection is dead code: line 210 overwrites it, yet the EAD file `ead_metadata.tsv` is still read. It skips any output that already exists (line 180), so stale or partial files survive. Gzip writes are not atomic. `Pool(6)` is hardcoded. A stray `print(scan.id)` sits at line 112. `ignore_errors=True` silently drops unparsable scans from zip archives. **Versions:** the code that produced the TSVs is unreleased. The LotA files have mtime 2025-04-28, but `inventory_stats` was first committed on 2025-05-26 (`a997bb9`). AS `pyproject.toml` requires pagexml-tools ≥0.8.0, which crashes the script. The editable checkout in `venv/` cannot be imported (`orjson` is missing). | High | Use a CLI with explicit input/output/metadata paths, a pinned pagexml-tools version and commit recorded in a manifest, atomic writes, a logged list of dropped and errored scans, and an overwrite flag. |
| D7a | **Rescaling is inverted for small scans.** | `rescale_scans` brings scans larger than 5000 px down to 5000 px. Scans smaller than 5000 px are multiplied by the *average downscale factor* (<1), so they shrink further. **8,837 of 9,122 inventories mix both kinds; 274,081 scans are affected.** This mixes pixel scales inside the main-text-range fit. I found this by reading the code; I did not quantify its effect on the labels. | Low–medium | Scale every scan to a common target, or compute layout in relative coordinates. |
| D7b | **`translate_pages` uses the wrong sign.** | The translation vector is `(-left, +top)`; it should be `-top`. This is latent: the top was 0 on every split page checked. | Low | Fix the sign. |
| D7c | **Row order is not reading order.** | Per page, all `main_text` regions are written (sorted geometrically), then all `margin` regions. The PageXML `ReadingOrder` element is present but ignored. Pages are written even first, then odd. Scan order follows directory parse order; there are 31 scan-number decreases in 19 files, mostly the mixed inventories. | Medium for any adjacency or continuation use | Write explicit `page_side`, `region_index` and `reading_order_index` columns, and never rely on file order. |
| D7d | **Partial shots duplicate text.** | 3,177 rows in 15 files come from `_deelopname<N>` scans (partial photographs of a folio). In 4781 there are 1,786 such rows, and 11 scan numbers there have both the base scan and the partials. 98 scan ids in 6 files do not fit the usual pattern (`7364A_R`, `5751B_P001`); for these the scan-number regex fallback fails. | Low–medium | Add `scan_variant` and `scan_num` columns, and flag partial shots. |
| D7e | **The `doc_id` scheme is fragile.** | The `doc_id` is `{scan_id}-text_region-{x}-{y}-{w}-{h}`, with the bbox in original scan pixels (it is set before rescaling). It does not record the page side or the original region id. There are no collisions today (0 duplicate `doc_id`s). LotA gold files `01-`, `02-`, `02c-` and `03-*.tsv` are keyed on it. | Medium (stability) | Keep this `doc_id` unchanged for continuity, and add `region_xml_id` and `page_id`. |
| D7f | **Encoding is fine today but unguarded.** | Every file is valid UTF-8, and all 9,122 have the 6-column header. There are 0 malformed rows and 0 tabs, CRs or `"` in the text. But the writer does no escaping, so one tab or newline in HTR text would break a row. | Low | Escape the text or assert on it, and validate after writing. |
| D7g | **Region tags carry no information here.** | The local SG PageXML has `structure {type:Text}` on 100% of regions (3771 and 11182), so SG has no Laypa class information. | Info | Add an optional `region_tag` column anyway, for VOC parity. |

### D4 in detail: why the `„` "contradiction" is only apparent

The script deletes every line-final `„` inside a region when it joins the lines, so those marks
are invisible in the TSV. The only `„` left at a row's end is the one at the **end of a region**.
That is the population LotA measured. LotA could not have observed line-end `„` from these files.

**Local resolutions** (3771–3773, 3821–3823; 780,030 lines):
- `„` occurs **6,525** times:
  - 6,484 (99.4%) at a line end that is not a region end;
  - 32 at a region end;
  - 8 at a line start;
  - 1 inside a line.
- The line after a line-end `„` starts lowercase in **96.3%** of cases. The baseline for lines
  that end in a letter is 75.4%.
- The joined word (fragment plus the next line's first word) occurs elsewhere in the same
  inventories in **87.9%** of cases (5,559 of 6,322 testable).

**Local letters** (11182, 11281):
- `„` occurs 10,388 times: 8,680 at a line end, 79 at a region end, 1,621 at a line start, and 13
  inside a line.
- The next line starts lowercase in 78.5% of cases. In 15.6% it starts with `„` (a doubled mark
  at the start of the continuation line).

So in this sample the user's convention holds: line-end `„` is a word break. Region-end `„` is a
different and mixed population.
- In the local resolutions it looks like the same word break cut at a region boundary
  (`te authori„`, `van den Com„`).
- In the corpus sample it is mixed: there are fragments (`ordon„`, `de mis„`) and also full words
  (`als volgt„`, `dient„`).

A crude vocabulary test calls 57% of the region-end `„` "full words", but this test is weak for
short fragments. LotA's "8 of 10 terminal" verdict applies only to region ends, and it is a very
small sample.

### D5 in detail: fields lost

The following fields are lost:
- the line id and line polygon/bbox;
- the baseline;
- the raw (pre-merge) line text;
- the line order and line count;
- the first and last line text;
- the line character offsets (`line_ranges`);
- the region polygon (only the bbox survives, in `doc_id`);
- the page side;
- the scan width and height, and the rescale factor;
- the main-text range used;
- the PageXML reading-order index;
- HTR confidence;
- the region tag;
- the original region id.

---

## 2. When regenerating, do this

1. **Pin and record the toolchain.** Pin pagexml-tools to an exact version or commit. Write
   `manifest.json` next to the output with the script git hash, the pagexml-tools version and
   commit, the input PageXML location, the run date, and the word-break settings. Do not run
   against the current 0.8.1 unmodified: it crashes, and once patched it gives the degenerate
   range.
2. **Fix the region access.** Put regions under `columns`/`extra` when building pages from
   `split_scan_pages`, or call the range code with `page.text_regions`. Then **assert that no
   inventory returns `Interval(0, 99999)`** and log those that do.
3. **Replace the single-column assumption.** Detect the number of text columns per page side
   before labelling. 803 inventories show the two-column failure pattern. If no such method is
   ready, write the label as `region_type_geom_v1` with a `region_type_method` column, and leave
   `region_type` empty rather than wrong.
4. **Take inventory ids from the scan ids, validated against the EAD.** Write one file per real
   inventory: `7462A` and `7462B`, not `7462`. For directories whose scan ids lack the suffix
   (4848), use an explicit mapping table and record it in the manifest.
5. **Emit an explicit row for every scan without text, and for every text-less region.** Use
   `row_kind ∈ {region, empty_region, empty_scan}`, with `text = ""` and the reason
   (`no_regions`, `no_words`, `parse_error`, `split_error`).
6. **Detect word breaks per inventory.** Measure line-end enrichment for `-`, `„`, `=`, `¬` and
   `—`, and handle the doubled-mark convention (the break character at the line end *and* `„` at
   the next line start, including `=`/`,` followed by `„`). Never merge across region ends. Never
   merge digit–mark–digit sequences (money notation such as `8835„5„`). Record per row which
   characters were merged.
7. **Write a companion lines file** (preferred over widening the region file):
   `lines-NL-HaNA_1.01.02_<inv>.tsv.gz`, one row per line, with the region's `doc_id`.
8. **Keep `doc_id` byte-identical** to the current scheme, so that the LotA gold files keep
   joining. Add `region_xml_id` and `page_id`.
9. **Fix the rescaling** (one target scale for every scan) and the `translate_pages` sign.
10. **Write atomically** (to a temporary file, then rename), replace skip-if-exists with an
    `--overwrite` flag, log dropped and errored scans to `errors.tsv`, and remove the hardcoded
    paths, the dead EAD selection and the stray prints.
11. **Coordinate with LotA before changing the column set.** `tsv_source.py` rejects any file
    whose header is not exactly the 6 current columns (§4). Either version the filename, for
    example `regions-v2-…`, or update the reader at the same time.

**Recommended region file schema** (one row per region, text-less region, or scan without text):

| Column | Meaning |
|---|---|
| `archive_id` | `NL-HaNA_1.01.02` (full id; the current value is a bare `1.01.02`) |
| `inv_num` | suffix-safe EAD inventory number, e.g. `7462A` |
| `inv_dir` | the source directory name, e.g. `7462` (for traceability) |
| `scan_id`, `scan_num`, `scan_variant` | e.g. `…_0065`, `65`, `deelopname1` or empty |
| `page_id`, `page_side` | `{scan_id}-{even\|odd}` (or `single` when not split), and the side |
| `row_kind` | `region` / `empty_region` / `empty_scan` |
| `empty_reason` | `no_regions` / `no_words` / `parse_error` / `split_error`, or empty |
| `doc_id` | unchanged scheme |
| `region_xml_id` | the original `TextRegion/@id` |
| `region_index`, `reading_order_index` | geometric order on the page, and the PageXML `ReadingOrder` index (empty if absent) |
| `x`, `y`, `w`, `h` | region bbox in original scan pixels |
| `scan_w`, `scan_h` | scan size in pixels |
| `region_tag` | Laypa/structure tag, or empty |
| `region_type`, `region_type_method` | label and method, e.g. `geom_singlecol_v1`, `multicol_v2`, `fallback_band` |
| `n_lines`, `n_tokens` | counts |
| `first_line_text`, `last_line_text` | raw, unmerged |
| `word_break_chars` | characters treated as breaks for this inventory, e.g. `-„=` |
| `n_breaks_merged` | joins performed inside this region |
| `ends_with_break_char` | last line ends in a break character (the region-end case, not merged) |
| `text` | merged text |

**Companion lines file**, one row per line: `doc_id`, `line_xml_id`, `line_index`, `x`, `y`,
`w`, `h`, `baseline` (points string), `text_raw`, `char_start`, `char_end` (offsets into the
region `text`), `break_char_end`, `merged_with_next` (bool), and `conf` (if present).

---

## 3. Validation checklist for regenerated output

Each item is an invariant to check, with the expected value.

1. **Scan coverage.** For each inventory, the set of distinct `scan_id` equals the scan set in
   `scan_stats-1.01.02` (or METS) exactly: no scan missing and none extra. Corpus-wide, expect
   2,528,802 scans, with about 128,410 `empty_scan/no_regions` and about 6,223
   `empty_scan/no_words`. These numbers apply if the HTR input is unchanged.
2. **Region coverage.** Rows with `row_kind ∈ {region, empty_region}` should total
   Σ `text_regions` in the scan stats (21,036,137). Rows with `row_kind = region` should total
   about 20,601,168. For 3771, expect 11,894 regions, 198 of them text-less.
3. **Text regression.** With word-break handling set to the old `['-','„']`, text should match
   today's TSV for 100% of `doc_id`s. Check this on the 8 local inventories before changing the
   word-break settings.
4. **No degenerate ranges.** No inventory uses `Interval(0, 99999)` unless `region_type_method`
   says `fallback_band`. Expect 0 such inventories, or a short logged list.
5. **Label sanity.** In an inventory with a single text column (e.g. 11182), `main_text` should
   hold the majority of tokens: today 98.1% in LotA. In the two-column resolutions (3771, 3821),
   `margin` token share must fall from 99.3% and 97.9% to a minority. Spot-check scan `3771_0100`:
   the resolution text must be `main_text`, and `( 152 )` must not be.
6. **Inventory ids.** `inv_num` equals the inventory part of `scan_id` for 100% of rows. Every
   `inv_num` resolves in the EAD, except `12588`, which is known to be absent. No file mixes
   inventories; expect 0, against 20 files today.
7. **Uniqueness.** `doc_id` is unique per file (it is today), and `(doc_id)` is unique corpus-wide.
8. **Schema.** Every file has the identical header. The values satisfy `row_kind`, `page_side`
   and `region_type` ∈ their allowed sets. Text contains no tab, CR or LF. Every file is valid
   UTF-8.
9. **Lines file.** Each region's `n_lines` equals its row count in the lines file. Each region
   `text` equals the concatenation implied by `char_start`/`char_end`. Every line bbox lies
   inside its region bbox, allowing a small tolerance.
10. **Word breaks.** Line-end break merges should be ≥95% "next line starts lowercase" on the 6
    local resolutions (today's `„` gives 96.3%). After the fix, residual ` „<lower>` and
    `<lower>= <lower>` counts should drop sharply from 484,876 and 570,788. No merge should join
    digit to digit.
11. **Stable keys.** Every `region_id` in LotA `gold/01-`, `02-`, `02c-` and `03-*.tsv` still
    exists in the new files.

---

## 4. Downstream consumers

**AS.** No module under `archival_structures/` reads these TSVs. Only notebooks do, and all of
them point at the external drive `T7_Data`:
- `notebooks/correspondence-word-analysis.ipynb` copies the files into `data/sg_letters` and
  uses `text`.
- `notebooks/detect-letter-language.ipynb` reads the 6 columns and writes the 8-column variant
  (`lang`, `prob`).
- `notebooks/language-statistics.ipynb` uses `archive_id`, `inv_num`, `region_type`, `lang` and
  `text`, and pivots on `main_text`/`margin`.
- `notebooks/topic_modelling/analyse_paragraph_language.ipynb` uses `inv_num`, `lang` and
  `text`.
- `notebooks/scan-regions-to-paragraphs.ipynb` is a prototype of the script. It writes a
  different file name, `paragraphs-inv_<n>.tsv.gz`.

If any language statistics were derived from the `sg_letters` copy, they carry the D1b fallback
labels.

**LotA.** All reads go through `archival_language/corpus/tsv_source.py`
(`TSVParagraphSource`). This reader:
- requires the header to be **exactly** `archive_id, inv_num, scan_id, doc_id, region_type,
  text`; any other header makes the whole file `bad_header` and it is skipped;
- requires `inv_num` to equal the inventory in the file name; otherwise the row is
  `inv_mismatch`;
- parses the bbox out of `doc_id`.

`ead/adapters.py` fixes the file name pattern and parses `scan_id`. The consumers, and the
fields each depends on, are:

| Module | Fields |
|---|---|
| `corpus/quality.py` | `region_type`, bbox, `text` |
| `corpus/scan_coverage.py` | `scan_id`; it also assumes scans without text are absent |
| `corpus/integrity.py` | bbox, `scan_number`, `row_index` (file order!), `text` |
| `cli/geometry_probe.py` | bbox, `region_type`, `text` |
| `cli/page_split_validation.py`, `cli/page_split_cases.py` | bbox x, used to derive the page |
| `cli/document_units.py` | `doc_id`, bbox, `region_type` |
| `cli/region_quality*.py`, `gold/region_sampling.py` | `region_type`, `text` |
| `cli/parallel_text.py` | `main_text`/`margin`, bbox, `text` |
| `cli/corpus_census.py` | `row_index` |

The gold files `gold/01-`, `02-`, `02c-` and `03-*.tsv` key on `doc_id` (as `region_id`).

**What regeneration may break in LotA:**
- a new header (the reader rejects the file);
- suffixed file names (LotA's `InventoryNumber` handles them, but the census expects the current
  file set);
- explicit empty rows (they change the counts in `scan_coverage`, the census and `quality`);
- changed row order (`integrity` and the census use `row_index`);
- any change to `doc_id` (the gold files stop joining).

**NLP.** I found no reader of the SG TSVs. NLP's VOC text-layer scripts
(`scripts/build_voc_text_layer.py`) are a schema precedent, not a consumer.

---

## 5. Open questions for the user

1. **Which copy is canonical?** The AS `data/sg_letters` copy carries fallback labels and the
   LotA copy carries single-column labels. Can you tell which external-drive directory each came
   from (`paragraphs/`, `paragraphs-lang/`, `paragraphs_with_lingua_language/`), and which run
   produced them?
2. **Which pagexml-tools code produced the April 2025 TSVs?** The `a997bb9` snapshot reproduces
   them exactly, but it is dated after the files. Was there uncommitted local code?
3. **How should the label be redesigned?** Should `region_type` be recomputed with column
   detection (LotA's `bbox+text` classifier, or AS's `text_extent`), or retired in favour of
   `region_type_method`-tagged variants?
4. **Should the doubled-mark word-break convention be merged in SG?** That is, `=`/`,`/`„` at a
   line end followed by `„` at the next line start, and `=` alone. Which inventories or hands use
   `=` as the break mark?
5. **How should region-end `„` be handled?** Should it be merged across regions when the next
   region in reading order starts lowercase, or always be left untouched and only flagged?
6. **What should happen to partial shots?** Should `_deelopname` scans be kept (duplicate text)
   or dropped? And is there an authoritative mapping for unsuffixed directories such as 4848 →
   4848A?
7. **What needs staging?** Regeneration needs the PageXML for all 9,122 inventories, the
   EAD/`ead_metadata.tsv`, and ideally METS scan lists. Locally only 8 SG inventories exist. Which
   inputs should be staged to a non-external location?

**Not verified:**
- the effect of D7a on the labels;
- whether the 5 near-zero-`margin` inventories used the fallback;
- the hand-checked precision of the ` „<lower>` and `= ` residue patterns;
- the line order inside regions (taken from parsing as-is).
