# archival-structures

Tools for analysing the structure of digitised archival documents: detecting and splitting
two-page book openings, clustering page layouts and text lines, detecting document boundaries,
mining cross-page sequence patterns, and analysing scan images visually when ATR transcriptions
are not available.

Full documentation (including the per-module API reference) lives in [`docs/`](docs/) and is
built with Sphinx; see [Documentation](#documentation) below.

## The problem

Many archival institutions have digitised large parts of their holdings, and those scan images
are increasingly accessible online. But a scan number is not a useful address for researchers:
what they need is the document, the section, the session — the meaningful unit within the
collection. An archival inventory number (a bound register, a notarial protocol, a folder of
correspondence) is not a bag of independent images; it is a structured, ordered sequence with
recurring page layouts, document conventions, and transitions that are legible to anyone familiar
with the collection but invisible to software that processes each scan in isolation. Making that
structure explicit — automatically and at scale — is what this package is for.

For a more detailed discussion of the problem and how these techniques help, see
[Improving access to digitised archival collections](docs/introduction.md).

## What this package does

Starting from raw scan images and their PageXML ATR transcriptions, `archival-structures` builds
up a picture of each inventory number's internal structure in layers:

- **Detect and split two-page openings** so that books and book-like folders can be analysed at
  page rather than spread granularity
- **Cluster pages by layout** to build a vocabulary of recurring page types for that inventory
- **Detect structural whitespace and blank pages** to locate likely boundaries between documents
  or sections
- **Cluster individual text lines** to distinguish body text, headings, marginalia, catchwords,
  and other recurring line types
- **Mine the sequence of page types** to identify where document elements begin and end,
  including elements that span a page break
- **Work from scan images alone** (without PageXML) using visual embeddings and sequence pattern
  analysis when transcriptions are not available or not yet trusted

## Techniques

### 1. Opening detection and splitting

Many archival books and book-like collections — including folders where sheets of paper are
scanned two at a time, showing the verso of one sheet and the recto of the next — produce
two-page spread scans. Before any per-page analysis, each scan must be classified as a spread
or a single page and, if a spread, split into independent verso and recto images. The inventory
as a whole is also classified as a *book of openings* or a *mixed* collection of single-page
documents. Without this step, all subsequent layout analysis conflates the geometry of two
different pages into one coordinate frame.

`archival_structures.analysis.opening_detection`

### 2. Page-layout clustering

Pages within an inventory tend to fall into a small number of recurring layout types — a title
page, a section of dense running text, a sparse table, a page with extensive marginalia. Cluster
pages by the spatial arrangement of their transcribed text lines to make that vocabulary of
layout types explicit and assign a label to every page. Two complementary approaches are
provided:

- A **grid-pattern TF-IDF fingerprint** captures overall spatial distribution of lines across
  the page.
- A **relational fingerprint** (`archival_structures.analysis.relational_patterns`) encodes each
  line's own type together with its geometric relation (above, below, overlapping, ...) to its
  immediate neighbours — patterns the grid approach cannot distinguish, such as the difference
  between a body-text line followed by an indented clause versus a body-text line followed by a
  marginal note at the same vertical position.

`archival_structures.analysis.page_layout_clustering`,
`archival_structures.analysis.relational_layout_clustering`

#### Structural whitespace

Large whitespace regions within a page often signal where one document ends and the next begins.
The package detects and clusters these regions geometrically (without relying on PageXML region
markup) and identifies which line-neighbourhood patterns are over-represented adjacent to them —
making document boundaries visible even on pages where no blank line or explicit separator was
transcribed.

`archival_structures.analysis.empty_regions`

#### Cross-page boundaries

Blank or near-blank pages in the scan sequence frequently mark the transition between documents
or sections. The package identifies which layout clusters systematically appear before or after
such pages, turning a visual observation into a computable boundary signal.

`archival_structures.analysis.boundary_detection`

#### Text-extent margins

A lightweight measure of how much of each page is covered by transcribed text — how far from
each edge the first and last lines sit. Classifies each page as `full_text`, `late_start`,
`early_end`, or `short`, and summarises each inventory by its full-text page fraction: a quick
signal for distinguishing a running-text book from a sparse table register or a mixed-document
archive.

`archival_structures.analysis.text_extent`

### 3. Line clustering

Cluster individual text lines by indentation, width, and height into a vocabulary of recurring
line types — body text, closing formulas, marginal notes, catchwords, page numbers, and so on.
Line-type labels feed into the relational fingerprint above and into the sequence-pattern
analysis below.

`archival_structures.analysis.line_clustering`

### 4. Sequence-pattern mining

With per-page or per-line labels in hand, mine the ordered sequence for recurring patterns:
n-grams of label types, tandem repeats (the same sub-sequence appearing back to back), and
transition probabilities between label pairs. Identifies document elements that span a page
break and can recover candidate section or document boundaries without prior ground truth.

`archival_structures.analysis.sequence_patterns`,
`archival_structures.stream_analysis.sequence_analysis`

### Additional tools

**Visual clustering without PageXML** — when ATR transcriptions are not available or not
trusted, cluster scan images directly using DINOv2 visual embeddings and UMAP/HDBSCAN. Sequence
pattern analysis can then run on the visual cluster labels rather than layout labels, giving
access to the same sequence-level insights from images alone.
`archival_structures.stream_analysis`

**Subsequence detection** — detects visually homogeneous (book-like) sub-runs within a
heterogeneous scan sequence using adjacent cosine similarity between DINOv2 embeddings; useful
for identifying bound volumes within a mixed folder archive.
`archival_structures.stream_analysis.subsequence_detection`

**Ink colour, multi-colour text, and missing transcriptions** — robust ink/paper separation,
screening pages for more than one ink colour, and flagging page regions that look like genuine
ink but carry no transcription.
`archival_structures.clustering.colour_clustering`

**Ground-truth annotation** — a structured tag vocabulary and ipywidgets notebook apps for
labelling scans one at a time or one cluster at a time; and a YAML-based ground truth format for
recording document-level annotations (sections, sessions, resolutions, …) with scan-level
bounding box coordinates.
`archival_structures.datasets.annotations`, `archival_structures.annotation`

**EAD/METS parsing** — parsing the archival finding-aid metadata (series/subseries/file
structure, page manifests) that describes an archive's holdings.
`archival_structures.parsers`

See [`docs/findings.md`](docs/findings.md) for the concrete, validated-against-real-data lessons
learned while building this — several of the choices above (e.g. splitting before clustering,
chroma spread over luminosity-class counting for multi-colour detection) turned out to matter a
lot more than they first appeared to.

## Demo notebooks

Organised into three groups under [`notebooks/demo/`](notebooks/demo/):

**Page & scan analysis** ([`notebooks/demo/page-analysis/`](notebooks/demo/page-analysis/)):

- [`opening-detection-demo.ipynb`](notebooks/demo/page-analysis/opening-detection-demo.ipynb) --
  per-scan opening detection and splitting.
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
- [`sequence-patterns-demo.ipynb`](notebooks/demo/sequence-analysis/sequence-patterns-demo.ipynb) --
  mining recurring n-gram patterns and cross-page document elements, comparing the table register
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

- [`annotate-scans.ipynb`](notebooks/demo/annotation/annotate-scans.ipynb) -- ipywidgets
  ground-truth annotation app.
- [`bulk-tag-annotation-demo.ipynb`](notebooks/demo/annotation/bulk-tag-annotation-demo.ipynb) --
  tagging many scans at once by cluster, with a structured namespace/type/subtype tag builder
  instead of free text.

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
