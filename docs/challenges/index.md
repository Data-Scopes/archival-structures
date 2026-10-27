# Challenges: detecting and operationalising archival structures

A running inventory of the challenges involved in turning a digitised archival file (scans +
PageXML) into structured, navigable documents -- and the terminology needed to talk about them
precisely. Entries start as stubs (problem framing and formal definitions, no implementation) and
get filled in as the underlying methods get built out elsewhere in this package; see
[Findings](../findings.md) for validated results and [Introduction](../introduction.md) for the
broader motivation.

## Terminology

- [Archival terminology](archival_terminology.md) -- archive/fonds, series, subseries, file,
  inventory number.
- [Page-stream terminology](page_stream_terminology.md) -- scan, sheet, page, opening (book vs.
  folder), page stream vs. document stream.

## Challenge catalog

| Challenge | Status | Page |
|---|---|---|
| Document segmentation (page stream to document stream) | Stub | [document_segmentation.md](document_segmentation.md) |
| Page order problems (swaps, insertions, document-level swaps) | Stub | [page_order_problems.md](page_order_problems.md) |
| Simulation harness (corrupting real streams at controlled rates) | Design note | [simulation_harness.md](simulation_harness.md) |
| Leakage and splits (checks before believing a number) | Methodology | [leakage_and_splits.md](leakage_and_splits.md) |

More entries -- e.g. document type classification, cross-page element linking, completeness
checking -- belong here as they get scoped out.

```{toctree}
:maxdepth: 1
:hidden:

archival_terminology
page_stream_terminology
document_segmentation
page_order_problems
simulation_harness
leakage_and_splits
```
