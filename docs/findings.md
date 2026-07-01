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
[relational layout clustering](notebooks/relational-layout-clustering-table-vs-deeds-demo) demo
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

**NL-AsnDA's blank-recto convention accounts for 19% empty pages; NL-HaNA's blanks are
concentrated in the front matter (9%).** Across 20 scans (39 pages) of `NL-AsnDA_0114.11_1`,
6 pages are entirely blank (15%), fitting the pattern where each deed's opening has one half
left blank. Across 30 scans (~53 pages) of `NL-HaNA_2.10.50_1`, 5 pages are empty (9%),
all in the front matter (cover, title page, flyleaves). This structural difference directly
affects which sequence position has boundary significance: for the deeds, every other page
opening boundary is an empty-page boundary; for the table register, empty pages only mark the
transition from front matter to content. See the
[boundary detection across pages](notebooks/boundary-across-pages-demo) demo notebook.

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
[boundary detection within pages](notebooks/boundary-within-pages-demo) demo notebook.

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

## Reconciling annotation formats

**Three different tools have produced ground-truth-shaped labels for the same scans, in three
incompatible formats, two of them coincidentally sharing one file-path convention.**
{class}`~archival_structures.datasets.annotations.ScanAnnotation` (used by the
[scan annotation](notebooks/annotate-scans) notebook) keys by PageXML `scan_id` and writes to
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

## Tooling

**ipywidgets v8 silently drops the old `overflow_x`/`overflow_y` Layout properties** (no error,
just a swallowed `DeprecationWarning`) in favour of a single `overflow` property -- a
fixed-height scrollable widget container built with the old property names looks fine in code
but squishes/overlaps its children at render time instead of scrolling, since it never actually
got scrolling behaviour. Fixed in the [scan annotation](notebooks/annotate-scans) notebook.

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
