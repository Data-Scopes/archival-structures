# archival-structures

Tools for analysing PageXML/ATR transcriptions and scan images of archival documents:
detecting and splitting two-page book openings, clustering text lines and page layouts,
mining cross-page document-element sequences, ink-colour and missing-transcription detection,
and parsing EAD/METS archival finding-aid metadata.

The package grew out of a recurring problem: an inventory number's worth of scans and PageXML
transcriptions is not a set of independent images, but a structured, ordered corpus -- with
two-page openings to split, recurring line/page layouts to recognise, document elements that
span a page break, and ink that an ATR pipeline missed entirely or mis-transcribed because it
appears in more than one colour. The modules here build up that structure step by step, each
validated against real archival data along the way -- see [Findings](findings.md) for the
concrete, sometimes counter-intuitive lessons learned in the process.

## Installation

```bash
poetry install
```

Building these docs locally requires the optional `docs` dependency group:

```bash
poetry install --with docs
cd docs
make html
```

The demo notebooks below need real PageXML/thumbnail data that isn't committed to the repo --
download `demo-data.zip` from the
[latest release](https://github.com/Data-Scopes/archival-structures/releases) and extract it at
the repository root (`unzip demo-data.zip -d .`) before running them.

## Contents

```{toctree}
:maxdepth: 2
:caption: Guides

text_analysis_pipeline
colour_and_ink
coordinate_bridging
datasets_and_annotation
ead_mets_parsing
findings
```

```{toctree}
:maxdepth: 1
:caption: Demo notebooks

notebooks/inventory-structure-demo
notebooks/opening-detection-demo
notebooks/line-clustering-demo
notebooks/line-clustering-table-vs-deeds-demo
notebooks/page-layout-clustering-demo
notebooks/page-layout-clustering-table-vs-deeds-demo
notebooks/sequence-patterns-demo
notebooks/pagexml-image-region-linking
notebooks/pagexml-image-multicolour-explorer
notebooks/annotate-scans
```
