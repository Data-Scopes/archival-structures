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
- **EAD/METS parsing** (`archival_structures.parsers`) -- a separate concern from the
  PageXML/image pipeline: parsing the archival finding-aid metadata (series/subseries/file
  structure, page manifests) that describes an archive's holdings.

See [`docs/findings.md`](docs/findings.md) for the concrete, validated-against-real-data lessons
learned while building this -- several of the choices above (e.g. splitting before clustering,
chroma spread over luminosity-class counting for multi-colour detection) turned out to matter a
lot more than they first appeared to.

## Demo notebooks

All in [`notebooks/demo/`](notebooks/demo/):

- [`annotate-scans.ipynb`](notebooks/demo/annotate-scans.ipynb) -- ipywidgets ground-truth
  annotation app.
- [`bulk-tag-annotation-demo.ipynb`](notebooks/demo/bulk-tag-annotation-demo.ipynb) -- tagging
  many scans at once by cluster, with a structured namespace/type/subtype tag builder instead
  of free text.
- [`inventory-structure-demo.ipynb`](notebooks/demo/inventory-structure-demo.ipynb) --
  classifying a whole inventory number as a book of openings vs a mixed folder.
- [`opening-detection-demo.ipynb`](notebooks/demo/opening-detection-demo.ipynb) -- per-scan
  opening detection and splitting.
- [`line-clustering-demo.ipynb`](notebooks/demo/line-clustering-demo.ipynb) and
  [`line-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/line-clustering-table-vs-deeds-demo.ipynb)
  -- clustering text lines by indentation/width, and comparing that across a table-like register
  versus notary deeds.
- [`page-layout-clustering-demo.ipynb`](notebooks/demo/page-layout-clustering-demo.ipynb) and
  [`page-layout-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/page-layout-clustering-table-vs-deeds-demo.ipynb)
  -- clustering pages by text-line layout, and the same table-vs-deeds comparison.
- [`relational-layout-clustering-table-vs-deeds-demo.ipynb`](notebooks/demo/relational-layout-clustering-table-vs-deeds-demo.ipynb)
  -- clustering pages by line-type-and-neighbour-relation fingerprint instead of raw geometry,
  compared against the geometric clustering above.
- [`pagexml-image-region-linking.ipynb`](notebooks/demo/pagexml-image-region-linking.ipynb) --
  drawing PageXML regions on a thumbnail, and converting a manually-drawn selection back into a
  new PageXML region.
- [`pagexml-image-multicolour-explorer.ipynb`](notebooks/demo/pagexml-image-multicolour-explorer.ipynb)
  -- screening a sample of scans for multi-colour text and missing-transcription candidates.
- [`sequence-patterns-demo.ipynb`](notebooks/demo/sequence-patterns-demo.ipynb) -- mining
  recurring n-gram patterns and cross-page document elements, comparing the table register
  against the notary deeds.
- [`stream-analysis-overview-demo.ipynb`](notebooks/demo/stream-analysis-overview-demo.ipynb)
  and [`stream-analysis-groundtruth-demo.ipynb`](notebooks/demo/stream-analysis-groundtruth-demo.ipynb)
  -- embeddings + clustering, optional VLM tagging, and active-learning ground-truth creation
  for a plain directory of document images (no PageXML required).

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
