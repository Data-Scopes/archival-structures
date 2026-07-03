# archival-structures

Tools for analysing PageXML/ATR transcriptions and scan images of archival documents:
detecting and splitting two-page book openings, clustering text lines and page layouts,
mining cross-page document-element sequences, ink-colour and missing-transcription detection,
and parsing EAD/METS archival finding-aid metadata.

Full documentation (including the per-module API reference) lives in [`docs/`](docs/) and is
built with Sphinx; see [Documentation](#documentation) below.

## Techniques and tasks

Archival images and transcriptions are organised as
`<institute_id>/<archive_id>/<inventory_num_id>/<scan>`. The core idea behind this package is
that one inventory number's worth of scans is a structured, ordered corpus, not a set of
independent images -- so the analysis is built up in layers:

1. **Opening detection and splitting** (`archival_structures.analysis.opening_detection`) --
   decide whether a scan is a two-page spread, split it into independent verso/recto pages, and
   classify a whole inventory number as a *book of openings* versus a *mixed* folder/booklet.
2. **Page-layout clustering** (`archival_structures.analysis.page_layout_clustering`) -- cluster
   whole pages by the spatial arrangement of their text lines, via a grid-pattern TF-IDF
   fingerprint. A complementary fingerprint, `archival_structures.analysis.relational_patterns`
   (clustered by `relational_layout_clustering`), instead encodes each line's own type and its
   RCC-8 spatial relation to its immediate below/right neighbour -- relational line-neighbourhood
   patterns a pixel-pattern fingerprint can't represent.
   - **Structural whitespace** (`archival_structures.analysis.empty_regions`) -- detects and
     clusters significant whitespace regions within pages (computed geometrically, not from
     PageXML region markup) and scores which relational patterns are over-represented adjacent
     to those whitespace boundaries.
   - **Cross-page boundaries** (`archival_structures.analysis.boundary_detection`) -- detects
     blank or near-blank pages in the page sequence, and identifies which page-layout clusters
     systematically appear before or after them.
   - **Text-extent margins** (`archival_structures.analysis.text_extent`) -- measures how far
     from each page edge the first and last transcribed lines sit (relative top, bottom, left,
     right margins); classifies each page as `full_text`, `late_start`, `early_end`, or `short`;
     and characterises each inventory by its full-text page fraction -- a lightweight signal for
     distinguishing running-text books from sparse table registers or mixed-document archives.
3. **Line clustering** (`archival_structures.analysis.line_clustering`) -- cluster individual
   text lines by indentation/width/height into a vocabulary of recurring line types (body text,
   closing lines, marginalia, ...).
4. **Sequence-pattern mining** (`archival_structures.analysis.sequence_patterns`) -- order lines
   into a corpus-wide reading sequence and segment it into document elements, including elements
   that span a page break.

Tasks 2 and 3 both depend on splitting first (task 1) -- clustering whole two-page scans
conflates the left and right page's geometry into one coordinate frame.

Alongside the text-analysis pipeline:

- **Ink colour, multi-colour text, and missing transcriptions**
  (`archival_structures.clustering.colour_clustering`) -- robust ink/paper separation via
  multiotsu + connected-component shape (resistant to small artefacts like a sticker or stain),
  screening pages for more than one ink colour via LAB chroma spread, and flagging untranscribed
  page regions whose pixels look like genuine ink rather than blank paper.
- **Coordinate-space bridging** (`archival_structures.model.image`,
  `archival_structures.image`) -- converting between a scan's native pixel coordinates, a
  thumbnail's, and a canvas rendering of a selection, via an affine `Transform`; converting
  between PageXML `Coords` and this package's own `Box` type; ipywidgets-based interactive
  region drawing/tagging.
- **Ground-truth annotation** (`archival_structures.datasets.annotations`) -- a multi-level
  `namespace:type(:subtype)?(#N)?` tag vocabulary (see
  [`docs/vocabulary.md`](docs/vocabulary.md)) for labelling scans/pages/lines/cross-page
  elements, plus ipywidgets notebook apps for producing it one scan
  (`archival_structures.datasets.annotations`) or one cluster
  (`archival_structures.datasets.bulk_tagging`) at a time.
- **Stream analysis** (`archival_structures.stream_analysis`) -- a separate concern from the
  PageXML pipeline: embeddings + UMAP/HDBSCAN clustering, layout features, optional VLM tagging,
  and active-learning ground-truth creation for a plain directory of document images (no PageXML
  required) -- see [`docs/stream_analysis.md`](docs/stream_analysis.md).
  - **Sequence pattern analysis** (`archival_structures.stream_analysis.sequence_analysis`)
    -- label-agnostic tools for analysing ordered sequences of cluster labels (from visual
    or layout clustering): run-length encoding and noise-run merging, cluster n-gram mining,
    tandem repeat detection (recurring cluster sub-sequences), and transition matrices.
  - **Subsequence detection** (`archival_structures.stream_analysis.overview.subsequence_detection`)
    -- detects visually homogeneous (book-like) subsequences within a heterogeneous scan sequence
    using adjacent cosine similarity between DINOv2 embeddings; threshold-based and optional
    change-point (ruptures) boundary detection; scores each segment by mean similarity, cluster
    entropy, and optional opening consistency.
- **EAD/METS parsing** (`archival_structures.parsers`) -- a separate concern from the
  PageXML/image pipeline: parsing the archival finding-aid metadata (series/subseries/file
  structure, page manifests) that describes an archive's holdings.

See [`docs/findings.md`](docs/findings.md) for the concrete, validated-against-real-data lessons
learned while building this -- several of the choices above (e.g. splitting before clustering,
chroma spread over luminosity-class counting for multi-colour detection) turned out to matter a
lot more than they first appeared to.

## Demo notebooks

Organised into three groups under [`notebooks/demo/`](notebooks/demo/):

**Page & scan analysis** ([`notebooks/demo/page-analysis/`](notebooks/demo/page-analysis/)):

- [`opening-detection-demo.ipynb`](notebooks/demo/page-analysis/opening-detection-demo.ipynb) -- per-scan
  opening detection and splitting.
- [`full-text-page-detection-demo.ipynb`](notebooks/demo/page-analysis/full-text-page-detection-demo.ipynb) --
  detecting full-text pages from top/bottom text-extent margins; comparing six inventories
  (three HaNA table registers, two HaNA letter-copy books, one notary-deeds book) by their
  full-text page fraction, margin distribution, and line-width/equal-extent features.
- [`empty-region-clustering-demo.ipynb`](notebooks/demo/page-analysis/empty-region-clustering-demo.ipynb) --
  detecting and clustering significant whitespace regions within pages; contrasting the tiny
  inter-cell gaps in a table register against the structural blank areas in notary deed pages.
- [`boundary-within-pages-demo.ipynb`](notebooks/demo/page-analysis/boundary-within-pages-demo.ipynb) --
  which relational line-neighbourhood patterns (RCC-8 symbols) are over-represented immediately
  adjacent to significant whitespace regions -- the within-page boundary markers.
- [`line-clustering-demo.ipynb`](notebooks/demo/page-analysis/line-clustering-demo.ipynb) and
  [`line-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/page-analysis/line-clustering-table-vs-deeds-demo.ipynb)
  -- clustering text lines by indentation/width, and comparing that across a table-like register
  versus notary deeds.
- [`page-layout-clustering-demo.ipynb`](notebooks/demo/page-analysis/page-layout-clustering-demo.ipynb) and
  [`page-layout-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/page-analysis/page-layout-clustering-table-vs-deeds-demo.ipynb)
  -- clustering pages by text-line layout, and the same table-vs-deeds comparison.
- [`relational-layout-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/page-analysis/relational-layout-clustering-table-vs-deeds-demo.ipynb)
  -- clustering pages by line-type-and-neighbour-relation fingerprint instead of raw geometry,
  compared against the geometric clustering above.
- [`pagexml-image-region-linking.ipynb`](notebooks/demo/page-analysis/pagexml-image-region-linking.ipynb) --
  drawing PageXML regions on a thumbnail, and converting a manually-drawn selection back into a
  new PageXML region.
- [`pagexml-image-multicolour-explorer.ipynb`](notebooks/demo/page-analysis/pagexml-image-multicolour-explorer.ipynb)
  -- screening a sample of scans for multi-colour text and missing-transcription candidates.

**Sequence & stream analysis** ([`notebooks/demo/sequence-analysis/`](notebooks/demo/sequence-analysis/)):

- [`inventory-structure-demo.ipynb`](notebooks/demo/sequence-analysis/inventory-structure-demo.ipynb) --
  classifying a whole inventory number as a book of openings vs a mixed folder.
- [`boundary-across-pages-demo.ipynb`](notebooks/demo/sequence-analysis/boundary-across-pages-demo.ipynb) --
  which page-layout clusters appear near blank pages in the page sequence -- the across-page
  boundary markers; contrasts the table register's front-matter blanks against the notary deeds'
  regular blank-recto convention.
- [`sequence-analysis-overview-demo.ipynb`](notebooks/demo/sequence-analysis/sequence-analysis-overview-demo.ipynb)
  and [`sequence-analysis-groundtruth-demo.ipynb`](notebooks/demo/annotation/sequence-analysis-groundtruth-demo.ipynb)
  -- embeddings + clustering, optional VLM tagging, and active-learning ground-truth creation
  for a plain directory of document images (no PageXML required).
- [`sequence-patterns-demo.ipynb`](notebooks/demo/sequence-analysis/sequence-patterns-demo.ipynb) -- mining
  recurring n-gram patterns and cross-page document elements, comparing the table register
  against the notary deeds.
- [`subsequence-detection-demo.ipynb`](notebooks/demo/sequence-analysis/subsequence-detection-demo.ipynb) --
  detecting book-like subsequences within a heterogeneous scan sequence (`NL-AsdSAA_89_3.1`)
  using adjacent DINOv2 cosine similarity; validates against a known book run and identifies
  additional candidates.
- [`cluster-sequence-analysis-demo.ipynb`](notebooks/demo/sequence-analysis/cluster-sequence-analysis-demo.ipynb) --
  sequence pattern analysis of cluster label sequences for `NL-HaNA_2.10.50_1` (visual and
  layout clustering) and `NL-AsnDA_0114.11_1` (layout clustering); demonstrates
  `run_length_encode`, `find_tandem_repeats`, `find_frequent_ngrams`, and `label_transition_matrix`.
- [`resolution-cluster-sequence-demo.ipynb`](notebooks/demo/sequence-analysis/resolution-cluster-sequence-demo.ipynb) --
  layout cluster sequence analysis for six resolution-book inventories from `NL-HaNA_1.01.02`
  (3771–3823); discovers candidate section boundaries from cluster sequence patterns without
  using the available ground-truth section metadata.

**Annotation** ([`notebooks/demo/annotation/`](notebooks/demo/annotation/)):

- [`annotate-scans.ipynb`](notebooks/demo/annotation/annotate-scans.ipynb) -- ipywidgets ground-truth
  annotation app.
- [`bulk-tag-annotation-demo.ipynb`](notebooks/demo/annotation/bulk-tag-annotation-demo.ipynb) -- tagging
  many scans at once by cluster, with a structured namespace/type/subtype tag builder instead
  of free text.

### Demo data

The notebooks above need real PageXML/thumbnail data (~341MB across 7 inventory numbers) that
isn't committed to this repo -- only the package code is. Download `demo-data.zip` from the
[latest release](https://github.com/Data-Scopes/archival-structures/releases) and extract it at
the repository root:

```bash
unzip demo-data.zip -d .
```

This recreates `data/PageXML/`, `data/thumbs/`, and `data/annotations/` with exactly the
inventory numbers the demo notebooks reference, so they run unchanged once extracted.

The source archives, inventory numbers, and citation information for the demonstration data are
documented in [`docs/archives.md`](docs/archives.md).

## Installation

```bash
poetry install
```

Requires Python >=3.11,<3.15 -- `torch`'s `triton` dependency caps out at Python <3.15, so the
project's declared Python range matches that rather than the more typical `<4.0`.

## Documentation

Built with Sphinx; requires the optional `docs` dependency group:

```bash
poetry install --with docs
cd docs
make html
```
