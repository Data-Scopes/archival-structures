# Page-stream terminology

Consistent terms for the numbered sequence of images a digitisation run produces for one
[file / inventory number](archival_terminology.md), and for the different granularities that
sequence gets processed at across this package's docs. A typical starting point looks like a flat
directory of files named `<file_id>_page_0001.jp2`, `<file_id>_page_0002.jp2`, ... -- running
numbers that, on their own, say nothing about how many physical pages each image shows or where
one document ends and the next begins.

## Physical carriers

- **Sheet** -- a single physical piece of paper, with a front and a back.
- **Leaf / folio** -- codicological synonym for sheet, used for bound material (cf.
  Muzerelle's *Vocabulaire codicologique*, already referenced for the `carrier:` tag namespace in
  [Vocabulary](../vocabulary.md)). A leaf's two sides are its **recto** (front) and **verso**
  (back).
- **Bifolium** -- a sheet folded in half, forming two conjoined leaves (four pages) -- the basic
  physical unit of a bound register or book gathering.
- **Folder** (NL: *omslag*) -- a physical unit holding one or more loose, unbound sheets. Usually
  the physical carrier of a [file](archival_terminology.md).

## Imaging units

- **Scan** -- one digitised image file, the atomic unit the digitisation pipeline produces. A
  scan shows either one page or two (see *opening*, below), regardless of how many are actually
  written on.
- **Page** -- one written or printed surface. A single sheet contributes up to two pages (recto
  and verso); a loose sheet in a folder that's only imaged on one side contributes one.
- **Opening** -- two pages captured side by side in a single scan. Two physically different
  things get called "an opening" and are worth keeping distinct:
  - **Book opening**: the verso of one leaf and the recto of the next leaf of a *bound* volume,
    physically joined by the binding -- verso conventionally left, recto conventionally right.
    Splitting a book-opening scan yields two pages that are physically continuous (verso's
    binding edge is recto's binding edge).
  - **Folder opening**: two separate, physically unconnected sheets from a folder, photographed
    side by side purely as a scanning convenience (e.g. the left sheet and the right sheet of a
    loose bundle laid out on the scanning table together). There is no verso/recto relationship
    between the two halves -- each is an independent sheet, and that sheet's own reverse side (if
    imaged at all) appears as an entirely separate, later scan, not as the other half of the same
    opening.

  Both kinds currently classify identically as `PAGE_TYPE_OPENING` under
  {func}`archival_structures.analysis.opening_detection.is_opening` -- the codebase doesn't yet
  distinguish them. That's fine for splitting (both split the same way, down the visible gap
  between the two halves), but matters for anything that assumes true verso/recto pairing (e.g.
  treating the split halves as physically continuous), which only holds for a genuine book
  opening.

## The page and document streams

- **Scan stream** -- the raw sequence of scan files as digitised, in scan order, before any
  opening splitting: `(scan_1, scan_2, ..., scan_k)`.
- **Page stream** -- the sequence obtained after splitting every opening scan into its two
  constituent pages (a single-page scan stays as one entry): `P = (p_1, p_2, ..., p_n)`, with
  `n >= k`. This is the sequence [document segmentation](document_segmentation.md) actually
  operates on.
- **Document stream** -- the sequence of documents `D = (d_1, ..., d_m)` obtained by segmenting
  the page stream: which pages belong together, grouped and ordered. In the simple case each
  `d_i` is a contiguous span of `P`; [Page order problems](page_order_problems.md) covers the
  ways that assumption breaks down in real material.

## A terminology clash worth flagging

The observed filename convention -- `<file_id>_page_0001.jp2`, numbering scans sequentially --
calls each *scan* a "page", even though a scan can show one page or two (an opening). Anything
that reads scan filenames as if the running number already indexed the page stream one-to-one
will be off by a factor of up to 2 for opening-heavy inventories. Keep "scan" and "page" distinct
in code and docs even though the filenames on disk don't.

## See also

- [Archival terminology](archival_terminology.md) -- the level above this one: archive, series,
  subseries, file, inventory number.
- [Document segmentation](document_segmentation.md) -- turning a page stream into a document
  stream.
