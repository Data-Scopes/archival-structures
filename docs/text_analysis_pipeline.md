# The text-analysis pipeline

Four capabilities, building on each other, for treating one inventory number's worth of PageXML
transcriptions as a structured corpus:

1. **Opening detection and splitting** ({mod}`archival_structures.analysis.opening_detection`) --
   decide whether a scan is a two-page spread, split it into independent verso/recto pages, and
   classify a whole inventory number as a *book of openings* versus a *mixed* folder/booklet.
   See the [opening detection](notebooks/opening-detection-demo) and
   [inventory structure](notebooks/inventory-structure-demo) demo notebooks.
2. **Page-layout clustering** ({mod}`archival_structures.analysis.page_layout_clustering`,
   built on {mod}`archival_structures.analysis.grid_analysis`) -- cluster whole pages by the
   spatial arrangement of their text lines, via a grid-pattern TF-IDF fingerprint. See the
   [page-layout clustering](notebooks/page-layout-clustering-demo) demo notebook.

   A complementary fingerprint, {mod}`archival_structures.analysis.relational_patterns`
   (clustered by {mod}`archival_structures.analysis.relational_layout_clustering`), encodes line
   *type* (from task 3) and the RCC-8 spatial relation
   ({mod}`archival_structures.analysis.region_calculus`) to each line's immediate below/right
   neighbour, via {mod}`archival_structures.analysis.neighbourhood_analysis`'s
   `LineNeighbourHood` -- e.g. "a body-text line with a shorter line directly below it" -- rather
   than raw pixel geometry. The neighbourhood distance threshold is derived empirically per
   corpus (`relational_patterns.derive_max_vertical_dist`) from the corpus's own adjacent-line
   gap distribution. See the
   [relational layout clustering](notebooks/relational-layout-clustering-table-vs-deeds-demo)
   demo notebook, which finds this fingerprint picks up structure the pixel-pattern one misses on
   a table register, but is noisier than the pixel-pattern fingerprint on a corpus (notary deeds)
   it already handles well -- a genuinely complementary view, not a strict improvement.

   **Structural whitespace and boundary detection:**
   {mod}`archival_structures.analysis.empty_regions` extends the analysis to the *absence* of
   text. `compute_page_empty_regions` geometrically computes whitespace rectangles within each
   page and filters them by minimum relative size; `cluster_empty_regions` groups them by shape
   and position; `find_boundary_adjacent_symbols` connects them to the relational-pattern layer
   by computing the RCC-8 symbols for lines immediately adjacent to each significant whitespace
   region; and `boundary_symbol_scores` ranks symbols by how often they appear at these
   boundaries relative to their corpus-wide frequency. See the
   [empty region clustering](notebooks/empty-region-clustering-demo) and
   [boundary detection within pages](notebooks/boundary-within-pages-demo) demo notebooks.

   {mod}`archival_structures.analysis.boundary_detection` works at the page-sequence level:
   `is_empty_page` detects blank or near-blank pages; `build_page_sequence` assembles the
   ordered page sequence with cluster labels and empty-page flags; and
   `empty_page_neighbor_clusters`/`empty_page_transitions` summarise which page-layout clusters
   appear near empty pages -- answering "which layout cluster typically ends or begins a document
   section?" See the
   [boundary detection across pages](notebooks/boundary-across-pages-demo) demo notebook, which
   contrasts the table register's front-matter blanks with the notary deeds' regular blank-recto
   convention.

   {mod}`archival_structures.analysis.text_extent` measures the *vertical extent* of each page's
   text using four relative margins (top, bottom, left, right).  `compute_corpus_text_extents`
   builds a DataFrame of these margins for all non-empty pages; `estimate_stable_margins` finds
   the typical minimum margin via a low percentile; and `classify_corpus_text_extents` labels
   each page as `full_text`, `late_start`, `early_end`, or `short` depending on how far its
   margins deviate from the stable reference.  This supports **Task 1** in the overall analysis
   pipeline: detecting which pages in an inventory have a full column of running text, and
   characterising each inventory by its *full-text page fraction* -- a signal that distinguishes
   running-text books (high fraction) from sparse table registers or mixed-document books (low
   fraction). See the
   [full-text page detection](notebooks/full-text-page-detection-demo) demo notebook, which
   compares six inventories: three HaNA table registers, two HaNA letter-copy books, and one
   notary-deeds book.
3. **Line clustering** ({mod}`archival_structures.analysis.line_clustering`, with a
   peak-detection alternative from {mod}`archival_structures.clustering.peaks`) -- cluster
   individual text lines by indentation/width/height into a vocabulary of recurring line types
   (body text, closing lines, marginalia, ...). See the
   [line clustering](notebooks/line-clustering-demo) demo notebook.
4. **Sequence-pattern mining** ({mod}`archival_structures.analysis.sequence_patterns`, using
   {mod}`archival_structures.analysis.region_calculus` and
   {mod}`archival_structures.analysis.allen_interval` for spatial relations) -- order lines into
   a corpus-wide reading sequence and segment it into document elements, including elements that
   span a page break. See the
   [sequence-pattern mining](notebooks/sequence-patterns-demo) demo notebook, which compares the
   table register against the notary deeds: the deeds show genuine cross-page continuations of
   legal boilerplate, the table's rows don't.

Pages 2 and 3 both crucially depend on splitting first (task 1): clustering whole two-page
scans conflates the left and right page's geometry into one coordinate frame. See
[Findings](findings.md) for the validated evidence.

Two notebooks compare how task 2 and task 3's clusters behave differently across two structurally
different kinds of inventory -- a table-like register versus notary deeds:
[line clustering](notebooks/line-clustering-table-vs-deeds-demo) and
[page-layout clustering](notebooks/page-layout-clustering-table-vs-deeds-demo).

## Supporting modules

{mod}`archival_structures.analysis.page` computes the height-profile/gap-detection machinery
opening detection is built on. {mod}`archival_structures.analysis.neighbourhood_analysis`
computes reading-order line adjacency, used by sequence-pattern mining and by relational layout
clustering above.
{mod}`archival_structures.analysis.text_analysis` and
{mod}`archival_structures.analysis.token_analysis` compute character/token-level statistics
(content-word density, punctuation, numbers, ...) for characterising line/region content.

```{eval-rst}
.. automodule:: archival_structures.analysis.opening_detection
   :members:

.. automodule:: archival_structures.analysis.page_layout_clustering
   :members:

.. automodule:: archival_structures.analysis.grid_analysis
   :members:

.. automodule:: archival_structures.analysis.relational_patterns
   :members:

.. automodule:: archival_structures.analysis.relational_layout_clustering
   :members:

.. automodule:: archival_structures.analysis.empty_regions
   :members:

.. automodule:: archival_structures.analysis.boundary_detection
   :members:

.. automodule:: archival_structures.analysis.text_extent
   :members:

.. automodule:: archival_structures.analysis.line_clustering
   :members:

.. automodule:: archival_structures.analysis.sequence_patterns
   :members:

.. automodule:: archival_structures.analysis.region_calculus
   :members:

.. automodule:: archival_structures.analysis.allen_interval
   :members:

.. automodule:: archival_structures.analysis.page
   :members:

.. automodule:: archival_structures.analysis.neighbourhood_analysis
   :members:

.. automodule:: archival_structures.analysis.text_analysis
   :members:

.. automodule:: archival_structures.analysis.token_analysis
   :members:

.. automodule:: archival_structures.clustering.peaks
   :members:
```
