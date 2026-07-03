# Improving access to digitised archival collections

## The scale of the problem

Many archival institutions have digitised large parts of their holdings — millions of scan images
are now accessible online. But access in the sense of *being able to find and read a specific
document* is still far harder than it should be. A typical search returns a scan number within an
inventory number: the researcher then has to browse forwards and backwards through the scan
sequence to find where the document they want actually starts. Section boundaries, document
transitions, and the internal organisation of a volume are usually described only at a high level
in the finding aid, if at all.

The gap between *scans are online* and *documents are accessible* is one of the main bottlenecks
in archival research at scale.

## What digitisation gives you — and what it doesn't

A modern digitisation pipeline typically produces two things for each inventory number: a set of
scan images and, increasingly, a set of Automatic Text Recognition (ATR) transcriptions in
[PageXML](https://github.com/PRImA-Research-Lab/PAGE-XML) format. PageXML records the coordinates
and text content of each recognised region and line on a page — a rich resource for text search
and analysis.

What neither the scans nor the PageXML records is *document structure*. ATR treats the page as a
collection of text regions. It transcribes a catchword at the bottom of a page with the same
confidence as the body text above it. It has no concept of where one letter ends and the next
begins within a folder, or where the index of a register stops and the running resolutions start.
The structure that makes an archival collection navigable exists in the physical object and in the
knowledge of researchers who have worked with it — but it is absent from the digital surrogate.

## The inventory as a structured corpus

An archival inventory number — a bound register, a notarial protocol, a folder of correspondence
— is not a bag of independent images. It is an ordered sequence with internal structure that
repeats in recognisable patterns.

Books and book-like collections (including folders whose sheets are scanned two pages at a time,
showing the verso of one sheet and the recto of the next) have a consistent two-page spread
format. Within both books and folders, recurring patterns emerge: title pages that look a certain
way, running-text pages that fill the available space, sparse index or table pages, blank pages
that separate one section from the next. In collections with a recurring document type — a series
of notarial deeds, a sequence of daily resolutions, a register of military personnel — the same
structural formula repeats across hundreds of pages and multiple inventory numbers.

These patterns are immediately legible to experienced researchers and archivists, but invisible to
software that processes each scan in isolation. Making them explicit — automatically and at scale —
is what this package is designed to do.

## How automatic structure detection helps

`archival-structures` builds up a picture of each inventory number's internal organisation in
layers, each of which unlocks the next:

**Splitting two-page spreads** is the necessary first step for book-like collections. A scan that
shows two pages simultaneously cannot be analysed at the page level without first separating
verso and recto. The package classifies each scan as a spread or a single page and, if a spread,
crops it into two independent page images. It also characterises the inventory number as a whole —
a *book of openings* or a *mixed* collection — which determines how the rest of the analysis
proceeds.

**Clustering pages by layout** builds a vocabulary of recurring page types for that specific
inventory. Rather than describing every page individually, the clustering assigns a small number
of labels (typically three to ten) that reflect the inventory's actual structural vocabulary:
running-text pages, title pages, index entries, sparse table pages, blank verso sides, and so on.
Two complementary methods are available: a grid-based approach that captures overall spatial
distribution, and a relational approach that encodes how each line type relates geometrically to
its neighbours — useful for distinguishing layouts that look similar at coarse resolution but
differ in fine-grained line structure.

**Detecting structural whitespace and blank pages** translates layout clusters into boundary
signals. Large blank regions within a page often mark where one document section ends and the
next begins. Blank or near-blank pages in the sequence frequently signal a transition between
documents. Identifying which layout patterns appear adjacent to these boundaries makes document
segmentation possible even in collections where document-level metadata is sparse.

**Mining the sequence of page types** takes the per-page labels and looks for recurring patterns
across the whole inventory — or across multiple inventories of the same type. Run-length
encoding, n-gram mining, tandem repeat detection, and transition matrices all operate on the
label sequence rather than the raw images, making them fast and applicable at scale. In a
collection of resolution books, for example, this analysis can recover candidate session
boundaries without any prior document-level ground truth.

**Working from scan images alone**, without PageXML, is supported through visual embeddings
(DINOv2) and UMAP/HDBSCAN clustering. This is useful when ATR transcriptions are not yet
available, when the handwriting recognition quality is too low to trust the text coordinates,
or when a quick structural overview is needed before committing to full transcription.

## What this enables

Concretely, the outputs of this package support:

- **Document-level navigation**: linking a search result to the first page of the document that
  contains it, rather than to a scan number in the middle of a run.
- **Completeness checking**: identifying pages that look like genuine handwritten or printed text
  but carry no ATR transcription — likely missed by the pipeline because the ink colour, script
  style, or page condition fell outside the training distribution.
- **Automated section segmentation**: dividing a volume into its constituent structural parts
  (index, main sequence, appendix) and flagging candidate boundaries for human review, rather
  than leaving that work entirely to manual inspection.
- **Cross-collection comparison**: comparing layout and sequence patterns across multiple
  archives and inventory types to identify similarities in document conventions or to detect
  outliers within a series.
- **Ground truth creation at scale**: the annotation tools in the package are designed to let
  a researcher validate or correct automated labels efficiently — one cluster at a time rather
  than one scan at a time — so that ground truth accumulates alongside the automated analysis.

## The demonstration collections

The demo notebooks use materials from three archives, chosen to illustrate different structural
challenges: a colonial military register with a dense table layout, a series of printed resolution
books with consistent multi-section structure, notarial deed protocols with a regular blank-verso
convention, and a mixed correspondence folder containing both loose sheets and bound items. Full
details of the source archives, inventory numbers, and citation information are in
[Example Archives](archives.md).
