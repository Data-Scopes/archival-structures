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
`annotations-<scan_id>.json`. `archival_structures.stream_analysis.groundtruth
.interactive_annotation` (a bulk image tagger, outside this package's published scope) keys by
thumbnail *path* instead and writes `{image_path: [label, ...]}` JSON elsewhere entirely. The
older `image_drawing.ThumbnailSelectionTagger` region-drawing tool predates `ScanAnnotation` but
writes to the *same* `annotations-<name>.json` path convention, with a completely different
shape (a JSON list of `{thumb_box, orig_box}` region dicts, not a `ScanAnnotation` dict) -- 73
real files for `NL-AsnDA_0114.11_1` existed in exactly this older format, only not *yet*
colliding with `ScanAnnotation` because the older tool happened to use `.png`-suffixed
filenames where the real PageXML `scan_id` is `.jpg`.

{func}`~archival_structures.datasets.annotations.import_bulk_image_labels` bridges the bulk
tagger's labels in: 2892 real labels (`book_opening`/`table`/`title_page`/`book_cover` for
`NL-HaNA_2.10.50`) were imported by treating `book_opening` as `OpeningLabel.is_opening` and
the remaining three (which never co-occur with each other in practice, confirmed against the
real label co-occurrence counts) as `page_layout`. `separation_x` is left unset -- the bulk
tagger has no spatial annotation, only whole-image tags.

{func}`~archival_structures.datasets.annotations.migrate_legacy_region_annotations` converts
the older region-drawing tool's files in place: each region marks a whole zone (e.g. a
multi-line marginal-note column), so every PageXML line whose own box is at least
half-covered by a region gets that region's label written to `ScanAnnotation.lines`, recovering
4027 real line-type labels (`closing`, `marginalium`, `table`, `body`, ...) across 73 scans that
were otherwise crashing `load_scan_annotation` (a JSON list isn't a dict, so
`ScanAnnotation.from_dict` raised `AttributeError` as soon as anything scanned the whole
`annotations/` tree). The original file content is preserved as `legacy-<original filename>`
before being overwritten, since the box-overlap matching is a heuristic, not a guarantee.

Both functions follow the same conservative merge policy: never overwrite a field already set
on an existing saved `ScanAnnotation`, only fill in `opening`/`page_layout`/`lines` entries that
are currently unset. There's no way to tell an auto-suggested value from a human-confirmed one
once saved (the notebook pre-fills `opening` with a `get_opening_features` suggestion before any
review), so anything already on disk is treated as authoritative.

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
- {class}`archival_structures.parsers.read.EADReader` turned out not to be a bug at all --
  earlier documentation wrongly assumed it would be called with this module's own
  `ET.Element`-based `read_ead()` output. Its one real caller (a scratch notebook) instead
  builds its own `BeautifulSoup` tree directly from an OAI-EAD API response, which is exactly
  what `EADReader`'s BS4-style API expects. Corrected the docs to say so instead of "fixing"
  working code.
