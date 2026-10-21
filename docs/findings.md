# Findings

Beyond API documentation, building this package surfaced a set of substantive,
validated-against-real-data methodological findings -- the kind of thing someone reusing this
package would otherwise have to rediscover the hard way. Each keeps its concrete validation
evidence (numbers, scan ids), not just the abstract lesson.

## Inventory structure and opening detection

**Width-ratio + gap-merge grouping classifies inventory structure robustly.** Sequential
gap-merging on sorted scan widths
({func}`archival_structures.analysis.opening_detection.find_width_groups`) correctly classified
all 6 independently-labelled real inventories (4 books, 2 mixed folders) as "book of openings"
vs "mixed", including a known edge case: some book scans show a sheet glued onto a page (masking
the text behind), with the *next* scan showing it folded back -- both states photograph at full
opening width, so this doesn't disturb width-based grouping.

**`classify_inventory_structure`'s default `min_share` is too strict for a long book with
proportionally rare covers.** `min_share=0.15` (each width group must be at least 15% of all
scans) works for the 6 originally-validated inventories, but `NL-AsnDA_0114.11_1` has only 2
single-page covers out of 630 scans (0.3%) -- nowhere near 15% -- so with the default it
classifies as `mixed` even though it's a genuine book of openings (628 of 630 scans are
openings). Lowering `min_share` (e.g. to `0.002`) on the *full* corpus fixes it; classifying on
a smaller sample doesn't help, since a sample may not contain a cover scan at all. `min_share` is
a real tuning knob, not a bug -- it trades off how rare a genuine minority group is allowed to be
against how easily pure noise gets mistaken for one.

## Line and page-layout clustering

**Line/page-layout clustering needs pages split into verso/recto first**, not run on whole
two-page scans -- un-split scans conflate the left and right column's geometry into one
coordinate frame. Validated on `NL-HaNA_2.10.50_1`: splitting raised the fraction of line
clusters spanning both verso and recto sides from not-at-all to 30/32.

**{class}`~archival_structures.analysis.grid_analysis.GridPattern` (page-layout clustering)
degenerates to all-noise on zero-line pages** -- an empty page's all-zero grid pollutes the
global pattern vocabulary. Filter out pages with no lines before clustering.

**`PageXMLTextRegion` granularity varies by archive, so empirical adjacency thresholds can't be
scoped to "lines within the same region".**
{func}`~archival_structures.analysis.relational_patterns.derive_max_vertical_dist` was originally
scoped to lines within the same `TextRegion` (a region's own lines are known, by PageXML's own
structure, to belong together). On `NL-HaNA_2.10.50_1`, every `TextRegion` in the real ATR output
contains exactly one line (a region per table cell), so no within-region adjacent pair ever
exists -- the function raised `ValueError` outright on real data despite working fine on every
synthetic test fixture, which all used multi-line regions.
{func}`~archival_structures.analysis.relational_patterns.corpus_adjacent_line_vertical_gaps` now
pools over each whole page's lines instead, still only counting genuine, unambiguous
vertical-adjacency pairs (`get_neighbouring_line_pairs`'s `'below'` relation requires direct
horizontal overlap, not just page proximity) -- works for both corpora without depending on a
region granularity that isn't guaranteed.

**Relational line-neighbourhood clustering
({mod}`archival_structures.analysis.relational_patterns`,
{mod}`archival_structures.analysis.relational_layout_clustering`) is a genuinely complementary
signal to {class}`~archival_structures.analysis.grid_analysis.GridPattern`, not a strict
improvement.** Validated on both `NL-HaNA_2.10.50_1` and `NL-AsnDA_0114.11_1` (see the
[relational layout clustering](notebooks/page-analysis/relational-layout-clustering-table-vs-deeds-demo) demo
notebook): on the table register, where `GridPattern` finds almost no structure (81% noise), the
relational fingerprint finds far more (7% noise) -- but its clusters don't track verso/recto
symmetry at all (0/48 same-cluster pairs), consistent with that register's two opening-halves
genuinely differing. On the notary deeds, where `GridPattern` already works very well (39% noise,
96% verso/recto symmetry), the relational fingerprint does *worse* on both counts (65% noise, 33%
symmetry on a small surviving sample) -- its symbol vocabulary is large relative to corpus size
there (320 symbols over 125 pages, built from a 27-cluster line-type vocabulary with 16% noise),
making the resulting TF-IDF vectors sparser than `GridPattern`'s own (421 patterns, same pages).
Not evidence the relational signal is uninformative on a corpus geometry already handles well --
more likely that the line-type vocabulary feeding it (a generic HDBSCAN run) and HDBSCAN's own
clustering parameters both need corpus-specific tuning before the relational fingerprint can
compete with how well geometry alone already does there.

**The relational symbols are legible, once you look at the actual lines behind them, and they
recur across both corpora.** Cropping the page thumbnail around the `(line, neighbour)` pair
behind each cluster's top distinctive symbol (see the same demo notebook) surfaces three
recurring patterns rather than noise: a *row-label-vs-data-column* pattern (`right:DC` between
two different line-clusters, e.g. `'Generaal en Chef'` disconnected from `'met L. Mo. Schip
Amsterdam'` on the same row of the table register -- and the table register surfaces *several
distinct instances* of this pattern as separate relational clusters, which `GridPattern`, with no
notion of column identity, can't distinguish); a *paragraph-wrap* pattern (`below:PO` with the
*same* line-cluster on both sides, e.g. `'Gervol onslagen als resident'` continuing into `'der
preangee Regentschappen'`) -- the wrapped-sentence pattern the relational extension was
originally motivated by; and a *marginal-annotation-vs-body-text* pattern in the notary deeds
(a short marginal note such as `'4test R Cuetoneel'` sitting `right:DC`/`below:PO` next to a
number-bearing body line). This is independent evidence the relational fingerprint is reading
real document structure, not an artefact of TF-IDF on an arbitrary symbol vocabulary.

## Structural whitespace and boundary detection

**`make_empty_regions` is O(n²) in the number of PageXML text regions per page, but
pre-grouping regions into vertical blocks reduces this to near-constant.**
`pagexml.helper.spatial_helper.make_empty_regions` uses an iterative BFS/DFS carving algorithm:
each candidate whitespace rectangle is split on the first overlapping text region, producing
2-4 new candidates, until no text region overlaps remain. For `NL-AsnDA_0114.11_1` (notary deeds,
0-6 text regions per page) the function completes in < 0.1 s per page. For `NL-HaNA_2.10.50_1`
(table register, where every individual table cell is a separate `PageXMLTextRegion`),
scan 0005 with 77 regions takes 1.1 s and scan 0006 with 218 regions takes up to 800 s
(over 13 minutes) for a single page.
{func}`~archival_structures.analysis.empty_regions.group_regions_into_blocks` resolves this:
when a page has more than 20 text regions, `compute_page_empty_regions` first merges them into
vertical blocks (sorted by top coordinate; a new block starts when the gap exceeds
`0.7 × min_rel_height × page_height`). Scan 0006's 218 regions collapse to 2 blocks;
`make_empty_regions` on those 2 blocks completes in < 0.001 s -- a 734,000× speedup.
The key invariant: any vertical gap large enough to survive the minimum-size filter
(`≥ min_rel_height × page_height`) is wider than the grouping threshold
(`0.7 × min_rel_height × page_height`), so no significant gap is collapsed by the merge.
Across 15 HaNA pages the grouped corpus extraction completes in 0.02 s (0.002 s/page),
making full-corpus whitespace analysis practical even for densely-structured documents.

**After minimum-size filtering (rel_h ≥ 3%, rel_w ≥ 5%), the table register has structural
whitespace but not content-boundary signals.** With the grouping optimisation, `NL-HaNA_2.10.50_1`
yields 93 significant empty regions across 15 pages (6.2 per page), located predominantly at
`top` (34), `left` (27), `right` (16), and `bottom` (16). These correspond to consistent
structural margins (header area, column gutters) rather than content-boundary indicators --
the table's row structure does not produce mid-page gaps large enough to flag. In contrast,
`NL-AsnDA_0114.11_1` produces 177 significant empty regions across 39 pages (4.5 per page)
that reflect genuine document structure: predominantly `top`-located (73) and `bottom`-located
(48) regions in pages with deed-separator gaps, and `left`-located (43) regions in the blank
verso/recto halves that the notarial blank-recto convention creates (see next finding).
HDBSCAN finds 15 clusters from those 177 regions (33% noise), consistent with the variety of
structural whitespace contexts (full blank halves, top-of-page header areas, deed-separator
strips of varying heights, etc.).

**NL-AsnDA's blank-recto convention accounts for 21% empty pages; NL-HaNA has 12 empty pages
in 517, only visible as recurring section dividers when enough scans are loaded.** Across 300
scans (599 pages) of `NL-AsnDA_0114.11_1`, 127 pages (21%) are entirely blank, with one
blank recto half per two-page opening throughout. Across 300 scans (517 pages) of
`NL-HaNA_2.10.50_1`, 12 pages (2%) are empty: 5 in the front matter and 7 recurring
*section dividers* spread through the register. With only 30 scans these 7 interior empty
pages are invisible (too few to sample the interior). At 300 scans the recurring pattern
becomes clear: a blank verso half followed by a sparse-header recto (~12 lines), then a
return to standard dense table pages. See the
[boundary detection across pages](notebooks/sequence-analysis/boundary-across-pages-demo) demo notebook.

**`GridPattern` degenerates on dense table registers; DINOv2 + transcript features recover
the structure.** On `NL-HaNA_2.10.50_1`, `GridPattern` produces 84% noise (8 layout
clusters from 505 pages, all tiny) because virtually every table page has the same dense
grid. Combining DINOv2 CLS embeddings (PCA-50) with simple transcript features (normalised
line count, region count, verso/recto indicator) at equal weight (0.5 / 0.5) reduces noise
to 3% and gives a 4-cluster solution: cluster 0 (recto halves, 216 pages), cluster 1
(single-page full scans, 77 pages), cluster 2 (section-boundary pages: 9 empty + 3 sparse
headers, 12 pages), and cluster 3 (verso halves, 195 pages). Every one of the 12 empty pages
is immediately followed by a cluster 0 page (12/12), confirming a clean and recurring
section-boundary structure. DINOv2 groups the blank verso pages and the sparse section-header
recto pages together in cluster 2 because both look visually similar (mostly blank); this is
a feature rather than a bug -- it naturally identifies the full *boundary unit* (blank + header)
as a single cluster.

**Boundary-affinity analysis on NL-AsnDA surfaces 'Eerste blad' and deed-type labels as
within-page structural markers.** With a 5% minimum-height filter (focusing on structural gaps
between deed blocks, not individual-line spacing), `find_boundary_adjacent_symbols` identifies
127 boundary-adjacent relational symbols across 31 pages. The top-affinity symbols are
`(cluster -1, right, DC, cluster 33)` (affinity = 3.0, meaning it appears at whitespace
boundaries 3× more than in the corpus overall) and `(cluster 3, right, DC, cluster 8)` (affinity
1.5). The adjacent lines whose text is most diagnostic include `'Eerste blad'` (Dutch: 'first
page/leaf', a section label placed before a major boundary) and `'Trransport'` (OCR for
'Transport', the deed-type label at the start of a new deed block). The `DC` (Disconnected)
relation dominates high-affinity symbols, consistent with the intuition that lines immediately
adjacent to structural whitespace have no spatial overlap with their nearest neighbours (they
*are* the boundary). See the
[boundary detection within pages](notebooks/page-analysis/boundary-within-pages-demo) demo notebook.

## Full-text page detection from text-extent margins

**The full-text page fraction alone cannot separate all table registers from running-text books,
but two line-uniformity features close the gap completely.** Across six inventories
(`estimate_stable_margins` + `classify_corpus_text_extents` from
{mod}`archival_structures.analysis.text_extent`, 10th-percentile stable margin, ±5% tolerance):

| Inventory | Type | Full-text % | mean_width | equal_frac |
|-----|-----|------|------|------|
| NL-HaNA_1.01.02_11182  | running text (letters) | 97% | 0.280 | 0.392 |
| NL-HaNA_1.01.02_11281  | running text (letters) | 98% | 0.275 | 0.401 |
| NL-AsnDA_0114.11_1     | running text (deeds)   | 90% | 0.236 | 0.306 |
| NL-HaNA_2.10.50_85     | table register         | 69% | 0.150 | 0.139 |
| NL-HaNA_2.10.50_1      | table register         | 67% | 0.164 | 0.143 |
| NL-HaNA_2.10.50_619    | table register         | 21% | 0.092 | 0.107 |

The full-text fraction alone is not sufficient: the two densest table registers (_1, _85)
reach 67-69%, overlapping with the notary deeds (90%).  Two additional features separate them
completely with no overlap:

* **`mean_width`** (mean relative line width): table-register pages have narrow lines
  (0.09--0.16) because each PageXML line wraps a single table cell entry (a date, a name,
  a number); running-text pages have wide lines (0.24--0.28) spanning the full column.

* **`equal_frac`** (fraction of consecutive row-group pairs with Allen-``equal`` horizontal
  extent, within 2% tolerance): table cells share a left-column boundary but have variable
  right extents (depending on cell content), giving 0.10--0.14.  Running-text lines
  consistently span the full column width from line to line, giving 0.31--0.40.

**`early_end` pages are enriched for document ends, but they are not document-boundary
markers** (revised 2026-10-21; the original claim follows). The original observation was made
without ground truth. In the two letter-copy books (11182, 11281), nearly all non-full-text pages
are `early_end` (22 and 18 respectively, out of ~750--1100 non-empty pages), likely the last page
of each letter, where text ends before filling the column. In the notary deeds (AsnDA), the 28
`early_end` pages similarly seemed to mark deed endings.

*Tested by NLP (Step 6.4, `report/84-early-end-vs-end-labels.md`) on 22 NL-HaNA 1.04.02 VOC
inventories with START/END labels (20,905 scans), at the defaults (10th-percentile stable
margins, tolerance 0.05):*
- `early_end` flags 14.9% of pages. 14.0% of those are END pages, against a 6.6% base rate (lift
  2.1, CI [1.6, 2.7]; enriched in 19 of 22 inventories).
- It catches 31.5% of END pages (F1 0.19, worst inventory 0), below a closing-formula regex
  (recall 71%, F1 0.33). `late_start` is at chance (lift 0.97).
- Why:
  - about half of VOC starts are mid-page, so the next document fills the page;
  - full-page END pages show no white space;
  - most false positives are within 0.034 of a stable margin of only 0.04--0.05;
  - edge fragments and catch-words pin the bottom margin.
- A second, recall-only test on 21 Generale Missiven inventories (NLP Step 6.3) shows no clear
  enrichment: 4 of 28 last scans, lift 1.6 over interior scans, overlapping CIs.

Use it at most as a weak auxiliary signal. NLP's two requests for `text_extent` are in
[from-nlp-start-page-detection.md](from-nlp-start-page-detection.md) (UA-014).

See the [full-text page detection](notebooks/page-analysis/full-text-page-detection-demo) demo notebook.

## Sequence-pattern mining

**`detect_cross_page_continuation`'s distance thresholds need to scale with the scan's actual
pixel resolution.** `max_vertical_gap`/`max_horizontal_diff` are absolute pixel distances, and
the defaults (`150`/`100`) were too strict to find *any* cross-page element on real ~4000-7000px
scans: a page's own top/bottom margins alone are several hundred pixels, so two lines that
genuinely continue the same clause across a page break are already further apart than 150px
before any real "gap" is considered. Raising the thresholds to `max_vertical_gap=800,
max_horizontal_diff=300` (matched to the scans' own resolution) surfaced 34 genuine cross-page
elements on a sample of `NL-AsnDA_0114.11_1`'s notary deeds -- confirmed by reading the actual
line text, which continues mid-clause from the bottom of one page to the top of the next.
The same scaled thresholds found *zero* cross-page elements in `NL-HaNA_2.10.50_1`'s tabular
register, which isn't a threshold artefact -- a table's rows genuinely don't span page breaks.

## Ink colour and missing-transcription detection

**Naive Otsu + mean-luminosity inversion detection for ink/paper separation is fragile to small
artefacts.** A small light sticker on an otherwise brownish/dark page can pull the text-line
mean luminosity high enough to flip the "is this page inverted" decision for the whole page.
Fix: {func}`~archival_structures.clustering.colour_clustering.find_ink_luminosity_class` uses
multiotsu + connected-component shape instead -- real ink fragments into many small per-glyph
components regardless of polarity, while a uniform artefact (sticker, glare, stain) forms one or
a few large ones, so picking the "many small components" class is robust to which one happens to
be literally darker or lighter. Validated on a synthetic reproduction of the failure case.

**Missing-transcription detection must compare against the page's own background baseline, not
fragment-to-fragment.** `pagexml.helper.spatial_helper.make_empty_regions` chops one real empty
area into many small rectangular fragments; comparing a fragment's ink-fraction against the
distribution of *other* fragments contaminates the comparison when several of those fragments
are pieces of the same real signal.
{func}`~archival_structures.clustering.colour_clustering.find_missing_text_candidates` instead
compares each candidate region's ink-fraction against the page's overall background ink-fraction
(computed once, across all non-text pixels).

**Two-page openings must be split into verso/recto before missing-transcription detection too,
if the sides differ tonally.** On `NL-AsnDA_0114.11_1_0004` (verso darker than recto), a known
vertical-text example was *invisible* analysed as part of the whole opening (ink-fraction 0.157,
*below* the page's own median of 0.205) but clearly flagged once split and analysed on just the
recto page (0.20 vs the recto's own background baseline of 0.11, with 2 of 5 flagged candidates
directly overlapping the known annotation).

**Detection quality is fundamentally resolution-limited.** At the 300px-wide thumbnails
available locally for `NL-AsnDA_0114.11_1`, individual ink strokes alias into noise-sized
fragments -- the method reliably catches bolder/larger missed-text blobs but not faint/thin text
like the vertical-text example above, even after the verso/recto fix. This is a
data-availability constraint, not something further threshold-tuning can fix.

## Multi-colour text detection

**Counting multiotsu luminosity classes with many connected components doesn't discriminate
single- from multiple-ink-colour pages at all** -- a *single* ink colour's antialiased fade from
a dark core to the paper colour reliably produces 2-3 such classes by itself (validated: ~3
"ink-like" classes on almost every one of 75 real sampled scans from `NL-AsdSAA_89_3.1`,
regardless of how many actual ink colours were present). The signal that actually works is
**chroma spread**: the standard deviation of the LAB a/b channels among pixels from a clean
binary ink/paper split
({func}`~archival_structures.clustering.colour_clustering.score_multi_colour_text`'s
`ink_chroma_spread`) -- different ink colours separate in the a/b plane in a way lightness
shading alone doesn't. Found a genuine, visually-confirmed multi-colour example (black type +
red handwritten annotation) as the top-ranked candidate on the first 50-scan random sample.

## Ink density from images alone (no PageXML)

**The multiotsu ink-like density threshold needs to be an order of magnitude lower for a
whole-image mask than for a PageXML text-line mask.** The value validated for text-line masks
(0.15, see the multi-colour text detection finding above) left
{func}`~archival_structures.stream_analysis.overview.layout_analysis.extract_layout_features`
finding *zero* ink-like classes on 10 of 15 sampled `NL-HaNA_2.10.50_1` thumbnails when applied
to a whole-image mask instead. The mechanism: a whole-image mask is dominated by background/paper
pixels far more than a mask already restricted to confirmed text, and at typical thumbnail
resolution individual ink strokes alias into fewer, larger connected components relative to the
class's own pixel count -- so the same many-small-components signature structurally reads lower
even for genuine ink, not because the ink itself is less "ink-like". Validated on
`NL-AsdSAA_89_3.1` (heterogeneous document types/layouts, often multiple ink colours per scan)
and `NL-HaNA_2.10.50_1` (stable ink colour, variable paper tone including scans with a lighter
sheet glued on top): across both corpora, genuine ink-like classes sat at 0.03-0.28 density,
paper/border classes at 0.000-0.005 -- a threshold of 0.02 cleanly separates them without ever
falling back to an empty mask on either corpus.

**Pure-image multi-colour-text screening (no PageXML) agrees with PageXML-anchored screening,
but only moderately.** Screening the same 50-scan random sample of `NL-AsdSAA_89_3.1` both ways
-- {func}`~archival_structures.clustering.colour_clustering.score_multi_colour_text` on a
PageXML text-line mask vs. `extract_layout_features`'s `ink_chroma_spread` on the whole image --
gives Spearman ≈0.65 between the two `ink_chroma_spread` rankings, with 8 of the top 15
candidates shared between them. Useful as a triage signal when no transcription is available at
all, but not a substitute for the PageXML-anchored version when a transcription exists: restricting
the fit to confirmed text pixels gives it a cleaner signal, undiluted by background/paper pixels.
See the [multi-colour explorer](notebooks/page-analysis/pagexml-image-multicolour-explorer) demo
notebook's pure-image comparison section.

## Reconciling annotation formats

**Three different tools have produced ground-truth-shaped labels for the same scans, in three
incompatible formats, two of them coincidentally sharing one file-path convention.**
{class}`~archival_structures.datasets.annotations.ScanAnnotation` (used by the
[scan annotation](notebooks/annotation/annotate-scans) notebook) keys by PageXML `scan_id` and writes to
`annotations-<scan_id>.json`. {mod}`archival_structures.stream_analysis.groundtruth.interactive_annotation`
(a bulk image tagger, see [Stream analysis](stream_analysis.md)) keys by thumbnail *path*
instead and writes `{image_path: [label, ...]}` JSON elsewhere entirely. The
older `image_drawing.ThumbnailSelectionTagger` region-drawing tool predates `ScanAnnotation` but
writes to the *same* `annotations-<name>.json` path convention, with a completely different
shape (a JSON list of `{thumb_box, orig_box}` region dicts, not a `ScanAnnotation` dict) -- 73
real files for `NL-AsnDA_0114.11_1` existed in exactly this older format, only not *yet*
colliding with `ScanAnnotation` because the older tool happened to use `.png`-suffixed
filenames where the real PageXML `scan_id` is `.jpg`.

{func}`~archival_structures.datasets.annotations.import_bulk_image_labels` bridges the bulk
tagger's labels in: 2892 real labels (`book_opening`/`table`/`title_page`/`book_cover` for
`NL-HaNA_2.10.50`) were imported by treating `book_opening` as `OpeningLabel.is_opening` and
mapping the remaining three onto `generic:table`/`generic:title_page`/`generic:cover` tags
(see [Vocabulary](vocabulary.md)) added to `ScanAnnotation.tags`. `separation_x` is left unset
-- the bulk tagger has no spatial annotation, only whole-image tags.

{func}`~archival_structures.datasets.annotations.migrate_legacy_region_annotations` converts
the older region-drawing tool's files in place: each region keeps its own box and gets a
mapped tag (`marginalium` -> `generic:marginalia`, `closing` -> `generic:closing`, etc.;
labels with no mapping are preserved verbatim as `doctype:legacy:<label>` rather than dropped)
as a `ScanAnnotation.regions` entry, recovering 4027 real region tags across 73 scans that
were otherwise crashing `load_scan_annotation` (a JSON list isn't a dict, so
`ScanAnnotation.from_dict` raised `AttributeError` as soon as anything scanned the whole
`annotations/` tree). The original file content is preserved as `legacy-<original filename>`
before being overwritten.

`import_bulk_image_labels` never overwrites `opening` if already set on an existing saved
`ScanAnnotation` -- there's no way to tell an auto-suggested value from a human-confirmed one
once saved (the notebook pre-fills `opening` with a `get_opening_features` suggestion before
any review), so anything already on disk is treated as authoritative. Tags themselves don't
need the same guard: since `ScanAnnotation.tags` is multi-valued, adding a tag is never
destructive -- it's simply appended if not already present.

## Working with real archival image data

**Thumbnail filename conventions vary per archive with no single lookup strategy.** Observed
three distinct conventions across example archives: exact match to the PageXML scan id
(`NL-HaNA`), a `thumb-width_300-scan-<stem>.<ext>` glob pattern (`NL-AsnDA`), and filenames
sharing *no* substring at all with the scan id, requiring positional pairing by sorted order
after verifying the counts line up (`NL-AsdSAA_89_3.1`, `KLAB08969000001.jp2` &harr;
`0001_00001.jpg`).

**IIIF thumbnail generation rounds pixel dimensions**, which matters for pixel-exact coordinate
round-trips: when reducing a scan to a fixed width, the scaled height rounds to the nearest
integer pixel, so width-based and height-based scale factors diverge slightly (by ~0.1-0.2px in
observed cases) -- expected and inherent, not a bug, but something a coordinate-transform
consumer should round-trip-tolerant for rather than assume exactness.

## EAD finding aids

These come from phase 02 (LotA moving its EAD logic into AS). The full evidence is in
`reports/phase-02-ead-for-lota.md`; the IDs in brackets are its finding IDs.

**An archive's own id is often not its `<eadid>`, so pass `archive_id` to the frame builders.**
{func}`~archival_structures.parsers.ead_views.unit_of_description_frame` and
{func}`~archival_structures.parsers.ead_views.to_frame` prefix `node_id` with
`finding_aid.default_archive_id` unless given `archive_id`. In 7 of LotA's 9 archives the two
differ: LotA's `NL-AsdSAA_89` has eadid `89`, and `GB-3624_oh` has `gb3624-oh`. Without the
argument, the frame's node ids don't join to the caller's own ids (F02-03b.2).

**A handle is not evidence that scans exist.**
{attr}`~archival_structures.parsers.ead_digital.DigitisationSummary.is_digitised` is True for
any of the four mechanisms, including a bare handle. The Nationaal Archief assigns a handle to
every inventory number, so on SG (`NL-HaNA_1.01.02`) `is_digitised` is true for 17,936 of 17,937
units, but only 10,857 (60.5%) have a `<dao>`. Use
{meth}`~archival_structures.parsers.ead_digital.DigitisationSummary.has_evidence` with the
mechanisms that count as evidence for that archive (`{"dao"}` for the NA). The 7,079 SG units
with a handle but no `<dao>` come in whole series, e.g. 4,012 of 4,468 in series 5.2, against 55
of 3,847 in the resolutions. That fits series not yet scanned rather than scattered missing
`<dao>`s. In `NL-ZlCO_0266`, 338 of 386 units have only a handle (F02-04.2, F02-R.3).

**Scope notes point into other archives; never join such a reference to the current
inventory.** 91 SG and VOC scope notes name another archive's code before the inventory number.
83 point to Aanwinsten (`Zie Aanwinsten, 1.11.01.01, inv.nr. 687`), but there are also
`2.01.27.01`, `1.01.02` and `1.10.110`.
{class}`~archival_structures.parsers.ead_scope_notes.ScopeReference` carries the code in
`target_archive`, so `inv.nr. 687` there must not be read as VOC's own 687 (F02-05.1, F02-05.5).
`target_archive` is None both when no code is written and when the code equals the
`archive_id` passed in, by plain string comparison. Pass the bare NA code (`1.04.02`), not a
prefixed id (`NL-HaNA_1.04.02`), or a note naming its own archive will look cross-archive
(F02-R.6). Notes without an `inv.nr.` cue (`zie Aanwinsten, 1.11.01.01 (1893 29r)`) yield no
reference at all (F02-05.6).

**Structural `<unitdate>` excision is exact, and its markers live in the text walk, not the
tree.** {func}`~archival_structures.parsers.ead_fields.split_unittitle_dates` brackets each
direct `<unitdate>` child with control characters during the text walk. lxml rejects control
characters in element text, so they cannot be put into a copied tree (F02-P.3). On all 40,207
titles of SG, VOC and `NL-HaNA_2.10.01` with a `<unitdate>` child, every span reproduces the
child's text exactly, and no title has a `<unitdate>` deeper than a direct child (F02-R.5).
Searching for the date string instead would be wrong: in 6,263 VOC titles the same date string
also occurs as plain text (`1699 1699`) (F02-06.4).

**Cutting the date often leaves a stray separator in the title prose.** After excision,
`prose` ends in `,`, `;` or `:` in 10,004 of 22,197 SG titles (45%), typically `..., aan
griffier Fagel,` from `..., aan griffier Fagel, 1690 april 18`; 119 SG titles contain `, ,`
(`Brieven van ..., , , en`). VOC has 56 and `NL-HaNA_2.10.01` has 4. Strip trailing separators
if the prose feeds a tokeniser or an n-gram count (F02-R.4, F02-06.5).

**A line break before a quote adds no space, and in this corpus that costs nothing.**
{func}`~archival_structures.parsers.ead_text.element_text` drops the `<lb/>` space before
`. , ; : ! ? ) ]` and before the quotes `'`, `"`, `’`, `”`, so `1674<lb/>'` is `1674'`.
The trade-off is that an ASCII quote that *opens* a quotation after a line break loses its space
(`a<lb/>"b"` gives `a"b"`). Across LotA's 9 archives' titles and the SG and VOC scope notes, the
quote rule changed exactly 3 SG titles, all closing quotes, so the trade-off has no instance
there (F02-03c.4, F02-R.7).

## Sequence decoding, scan ids and page numbering

These come from phase 04 (NLP's decoders and folio alignment, and the scan-id parsing of LotA
and NLP, moving into AS). The full evidence is in
`reports/phase-04-decoders-and-page-numbering.md`; the IDs in brackets are its finding IDs. The
VOC numbers are for `NL-HaNA_1.04.02`.

**A semi-Markov decoder is only as good as the start scores it gets, and emission-only CRF
scores are a poor input on unseen inventories.** `decode_semi_markov` in
`stream_analysis.decoding.semi_markov` adds a lognormal segment-length prior to per-page start
scores. Fed `start_score_from_emissions` from a 3-state CRF, it matches the plain CRF on
long-chunk dev (F1 0.783 against 0.781; F04-10b.2). On the inventory-held-out test split it
halves F1 (0.168 against 0.352), because it predicts 1.0–4.5% starts against 11.7% true, with
segments up to the 250-page cap (F04-P.3, F04-10b.3). The prior is not at fault: its fitted
median (3) matches the gold medians (2–3). The cause is the input. A start at page *i* pays
off only when its score is above roughly 1 nat. On held-out inventories the emission-only score
is negative at most gold starts (median −0.65, 22% above 0, against +4.63 and 84% on long-chunk
dev), and it is the same on the held-out *training* inventories, so this is not a
generalisation gap. The CRF finds those starts through its END → START transition, which the
emission-only score leaves out (F04-R.1). Feed the decoder start scores that include the
transitions, such as the CRF's posterior start marginals. That has not been measured yet.

**With a flat duration prior, ties put forced starts late.** With equal scores and
`max_duration=3`, a 10-page sequence decodes into segments of lengths [1, 3, 3, 3]: each
position keeps the longest segment that ends there (F04-10.5). Expect this whenever the scores
carry no information, e.g. on blank stretches.

**Reproducing a decoder exactly needs the same normal CDF.** `log_duration_pmf` uses
`scipy.special.ndtr`, which equals `scipy.stats.norm.cdf` bit for bit. An `erfc`-based
replacement shifts 18 of 500 log-pmf values by up to 6e-13, enough to change a byte comparison
of stored outputs (F04-P.4).

**The Nationaal Archief scan-id grammar holds without exception on VOC, but scan numbers don't
start at 1.** `ScanId.parse` (`model.scan_id`) parses all 21,623 ground-truth rows and all
162,468 distinct ids in 254 sampled text-layer inventories (every suffixed one plus 200
numeric ones), always with the inventory of the file they came from (F04-11.1, F04-11.2).
Suffixed inventories (`4848A`, `2352A`–`E`) carry the suffix in their scan ids too. The first
scan is often `0002`, `0003` or `0007` (`1604B` starts at 0007), so don't treat scan number − 1
as a position in the inventory (F04-11.3). `InventoryNumber` sorts all 6,914 VOC inventories
in the same order as NLP's regex sort keys; 54 of them have a suffix (`A`–`E`, `I`, `II`)
(F04-P.5).

**Page-number anchors can be read straight from a region table.** `page_number_anchors`
(`analysis.page_numbering`) takes the last non-empty `page-number` region per scan and keeps it
if it is all digits. On the VOC text layer this gives the same anchors as NLP's mined CSV in 466
of 467 inventories (64,505 pairs; one extra pair, on a scan outside NLP's scan spine). The
alignment accuracy on the 22 annotated inventories is identical (F04-12.2, F04-12.3). Document
order works as well as reading order here: 3,946 scans have several page-number regions, and
"last wins" in document order agrees with the reading-order value on all of them. A third of the
scans with a page-number region (32,508 of 97,014) end in a region that isn't all digits and
give no anchor. A pattern that tolerates a trailing `.` or `:` could add anchors, but this has
not been measured (F04-12.4).

**How well folio references locate documents.** On the 22 annotated VOC inventories (1,496
documents), the baseline alignment finds the exact scan for 61.9% of documents and comes within
±2 scans for 93.0%. The chosen configuration (numbering-mode detection, extrapolation, first
duplicate wins) reaches 77.9% exact and 94.8% within ±2 (F04-12.3). The `confidence` column
is informative but not a guarantee: 42 of 1,281 `high` documents (3.3%) are more than 2 scans
off, from misread digits in the anchors, non-linear numbering or noisy chains (F04-12.6). All
`chain+extrapolated` rows are `low` (F04-12b.3).

**The alignment keeps NLP's quirks on purpose, and they are visible in the output.** The
longest increasing chain is strict and a later duplicate wins. Rounding is Python's banker's
rounding (`round(12.5) == 12`). `max_scan=0` or `None` means no upper clamp. A row clamped only
at the inventory end still gets the `+extrapolated` suffix. Fixing any of these would change
NLP's published numbers, so each would need its own measured change (F04-12.5).

## States General PageXML and the v2 text layer

These come from phase 06 (`text_layer` v2 for the States General, hand `NL-HaNA_1.01.02` and
print `NL-HaNA_1.10.94` 455/456). The full evidence is in
`reports/phase-06-text-layer-v2-sg.md`; the IDs in brackets are its finding IDs.

**The legacy SG region `doc_id`s and texts can be reproduced exactly from the PageXML.** The
`doc_id` is `f"{scan_id}-text_region-{x}-{y}-{w}-{h}"`, with the box taken from the region
`Coords` in PageXML pixels (no rescale) and regions walked recursively. The text is
pagexml-tools' line join with the old word-break characters `['-', '„']`. The v2 extract
(`text_layer.extract_v2.legacy_doc_id`) matches the legacy TSVs on 151,246 of 151,246 rows in
14 inventories, both `doc_id` and text (F06-15d.1). So a regenerated layer can keep the keys that
LotA's gold files join on.

**In print 455, region polygons don't bound their lines: use line boxes.** In 14 hand
inventories and print 456, every line box lies within its region box (5 px tolerance). In 455,
67.3% of the lines (109,071) extend beyond their region polygon, mostly by less than 30 px
horizontally (F06-15d.3). Any crop, overlap test or column assignment on 455 should use the hull
of the line boxes, not the region `Coords`.

**pagexml-tools ≥ 0.8 breaks REPUBLIC's merged-line splitter; AS's port carries the fix.**
Republic's `long_line_splitter` imports cleanly but raises `IndexError` on every page under
pagexml-tools ≥ 0.8.0, because `get_all_text_regions` no longer looks into columns. The AS port
(`text_layer.line_split`) has the fix built in and matches the original (with the 0.7.x
behaviour restored) on 2,856 of 2,856 REPUBLIC resolution pages. HTR text with a double space
where two columns meet (115 lines in 455 + 456) also raised `ValueError` until the text was
normalised first (F06-15e.6). Run a ported original once under the current pins, not only import
it.

**Synthetic glued-word precision says little about real repairs in historical Dutch.** A
unigram segmenter fitted on the corpus's own lines reached a boundary precision of 0.984 (print)
and 0.9995 (hand) on synthetic glued pairs and triples (F06-15f.1). On 50 real repairs read per
side, only 31 were correct in print (0.62) and 8 in hand (0.16), both under the 0.90 bar fixed in
advance, so repair stays off (F06-15f.2). The errors are genuine words broken at a frequent
prefix or suffix: `verlatende` → `verlaten de`, `voorquam`, `overlast`, and in hand `-en`, `-de`,
`op`, `voor`, `aen` + stem. A training-vocabulary unigram model cannot tell `opstellen` from
`op stellen`, and period spellings such as `vandenselven` are one word (F06-15f.4, F06-R.5).
Evaluate a segmenter on read real repairs, not on synthetic glue.

**PageXML `Word` elements don't restore lost spaces.** In the SG PageXML, the `Word` texts joined
with single spaces equal the line text in 455 for 33,659 of 33,660 lines, and in 3349 for 14,324
of 14,324 (F06-P.2). Where the HTR glued two words, the `Word` elements are glued too, so
whitespace repair needs text segmentation.

## Region quality and duplicate pages

These come from phase 08: `analysis.region_quality` and `analysis.duplicate_pages`, both ported
from LotA, and the `page_duplication` corruption case. They were measured on the States General
(`NL-HaNA_1.01.02`) HTR. The full evidence is in
`reports/phase-08-region-quality-and-duplicates.md`; the IDs in brackets are its finding IDs.

**A lexicon for a text-quality filter should hold words only; numerals in it count as hits.**
`region_quality` scores a region partly by the share of its tokens of 4+ characters that are in
an injected lexicon. LotA passes its fastText vocabulary (233,538 terms), which holds 9,298
all-digit terms. So "3761" is a "word": the tally "3761 -10 3", noise in the gold, gets
`strong_lexicon_hits` and is predicted usable (score 0.5). The roman numeral "xxiiij £" is lifted
from fragment to usable the same way (F08-R.1). This adds to the filter's known weak spot, the
1–3-token band. There, 10 of the 16 held-out keep/drop errors are numbers, times and tallies:
the numeric exemption waives `low_alpha_content`, but `long_nonalpha_run` still pushes them to
noise. The gold labels for such regions are themselves inconsistent (F08-16.3). Prose is
reliable: combined held-out keep/drop accuracy is 0.893 (F08-16.2). Filter numerals out of the
lexicon, or treat the short band as "keep", rather than trusting `noise` there.

**Re-shoots are found by scan adjacency, not by text similarity alone, and adjacency means scan
number, not sequence order.** A re-shoot of a two-page opening pairs page 1 of scan N with page 1
of scan N+1. Those two pages are not neighbours in sequence order, so only adjacency by scan
number catches them. All 13 documented SG re-shoots (23 rows) are such same-side pairs
(F08-P.5, F08-17.2). The Jaccard over character 5-grams can't separate re-shoots from formulaic
pages:
- documented re-shoots start at 0.615, just above the 0.6 threshold, because HTR variation alone
  lowers it;
- passport grants, session headings and requests by different persons reach 0.76 (F08-R.2);
- adjacency does separate them. Adjacent candidate pairs are 0.95 re-shoots (19 of 20 read,
  Wilson interval 0.76–0.99), far-apart pairs only 0.13 (4 of 30; F08-17.3, F08-17.4).

**Partial-shot scans (`deelopname`) are re-shoots that adjacency never sees.** All 9 SG
candidate pairs without a scan number pair a scan with its `..._deelopname1`/`2` scan or a
separately filed shot (`5751B_P001`, the 1626 letter on the *Wapen van Amsterdam*). They are the
same text at J 0.70–0.92, and only the MinHash/LSH stage finds them (F08-17.5). Filtering the
candidates on `adjacent` alone drops them.

**A duplicated start page must not be labelled a second start.** The `page_duplication` case
inserts a copy right after a page, so the copy carries the same canonical index as its original.
If both positions are labelled starts, a perfect classifier is scored as wrong: at rate 0.5 the
start F1 against such labels falls to 0.67 (3 documents, inv 3771) and 0.79 (11 documents, GM
1363). `observed_start_labels` therefore labels the first occurrence only (F08-17b.2). The
same-document pair labels are hardly affected: the pair share moves by under 0.01, because a
copy and its original are in the same document. Duplication is a problem for start-page
detection, not for grouping (F08-17b.3).

## Tooling

**ipywidgets v8 silently drops the old `overflow_x`/`overflow_y` Layout properties** (no error,
just a swallowed `DeprecationWarning`) in favour of a single `overflow` property -- a
fixed-height scrollable widget container built with the old property names looks fine in code
but squishes/overlaps its children at render time instead of scrolling, since it never actually
got scrolling behaviour. Fixed in the [scan annotation](notebooks/annotation/annotate-scans) notebook.

**A `requires-python` upper bound of `<4.0` is a problem when a dependency caps out much lower.**
`torch`'s `triton` dependency only supports Python `<3.15`, but this project declared
`requires-python = ">=3.11,<4.0"`. Poetry's resolver needs a dependency set valid across the
*entire* declared range, and since no `torch`/`triton` version reaches anywhere near `4.0`,
resolution failed outright (`poetry lock`/`poetry install` errored, not just produced a
suboptimal solution) -- worth knowing because the error message itself talks about `triton`'s
own Python requirement, not the project's, making the actual cause (the project's upper bound
being unrealistically high) easy to miss. Fix: narrow `requires-python` to `>=3.11,<3.15`,
matching what the heaviest dependency can actually support.

## Rough edges found while documenting, since fixed

A handful of latent bugs/dead-code paths were found while writing docstrings for existing,
untested code paths -- each was first documented in place rather than silently "corrected"
(the intended behaviour wasn't always obvious from the surrounding code alone), then fixed once
a repo-wide grep confirmed no caller depended on the old (broken) behaviour:

- {meth}`archival_structures.analysis.neighbourhood_analysis.LineNeighbourHood.get_rel_neighbour`
  checked whether a line was a member of its *own* neighbour list (always false) instead of
  returning the actual neighbours, so `left()`/`right()`/`top()`/`bottom()` always returned
  `None`. `top()`/`bottom()` also used relation keys (`'top'`/`'bottom'`) that didn't match what
  the rest of the class stores (`'above'`/`'below'`). Both fixed; the never-populated
  `self.above`/`.below`/`.left`/`.right` attributes (separate from the real state in
  `has_rel_neighbour`) were removed.
- {func}`archival_structures.analysis.token_analysis.tokens_are_running_text` returned `True`
  when *fewer* than `min_tokens` tokens were content words -- read backwards from the name.
  Flipped to return `True` when *enough* tokens are content words.
- {func}`archival_structures.clustering.colour_clustering.map_bg_ratio`'s `'unk'`/`'bg'`
  branches were swapped relative to what the label names mean (`'bg'` is now returned for
  colours much more common in the background, `'unk'` for the ambiguous middle case).
- {func}`archival_structures.clustering.image_clustering.get_luminosity_peaks` hardcoded
  `region_type='empty'` regardless of what pixels it was given, so text-region peaks were
  mislabelled `'empty'` too. Now takes `region_type` as a parameter, passed through correctly by
  its caller (`cluster_pixel_luminosity`) for both the empty- and text-region pixel sets.
- {func}`archival_structures.parsers.ead_parser.get_subsubseries_titles` checked for a
  `'subsubseries'` key that `parse_subseries` never actually sets, so it always returned an
  empty list. Fixed to use the actual shape `parse_subseries` builds: nested `<c
  level="subseries">` elements get appended to the same flat `subseries` list in nesting order,
  so genuine subsubseries titles are everything after the first (top-level) entry.
- `pagexml.model.physical_document_model.vertical_distance` (the external `pagexml-tools`
  dependency, not this repo) mismeasures the gap between baseline-bearing lines: it takes a
  `hasattr(doc, 'baseline')` branch that's true for essentially every `PageXMLTextLine` (the
  attribute exists, as `None`, even without real baseline data) and returns
  `abs(bottom1 - bottom2)` -- a baseline-to-baseline offset -- without checking whether the
  lines' vertical extents actually overlap, rather than an overlap-aware gap.
  {func}`archival_structures.analysis.neighbourhood_analysis.get_neighbouring_line_pairs` called
  this directly to apply `max_vertical_dist`, so `LineNeighbourHood`'s distance filter was itself
  unreliable for real (baseline-bearing) ATR output. Routed around locally (not fixed upstream)
  via {func}`~archival_structures.analysis.neighbourhood_analysis.line_vertical_gap`, which always
  computes the real top/bottom gap; a regression test with slanted, overlapping baselines
  reproduces the upstream bug's wrong (nonzero) answer alongside the corrected (zero) one.
- {class}`archival_structures.parsers.read.EADReader` turned out not to be a bug at all --
  earlier documentation wrongly assumed it would be called with this module's own
  `ET.Element`-based `read_ead()` output. Its one real caller (a scratch notebook) instead
  builds its own `BeautifulSoup` tree directly from an OAI-EAD API response, which is exactly
  what `EADReader`'s BS4-style API expects. Corrected the docs to say so instead of "fixing"
  working code.
