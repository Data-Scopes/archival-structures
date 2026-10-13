# Definitions: From Physical Archive to Navigable Logical Structure

A working vocabulary spanning archival science, codicology, information science, and information
history -- built to talk precisely about what gets lost between a digitised scan and a genuinely
navigable document, and about the devices creators historically relied on to make their own
physical records findable.

The full glossary, with citations, is in
[Concepts: From Physical Archive to Navigable Logical Structure](concepts-physical-to-logical-structure.md)
-- archival science and codicology, document/information science, retrieval devices, digitisation
standards, information history, and (as of its newest section) Document Analysis and Recognition
(DAR): physical/logical layout analysis, segmentation, reading order, and ground-truth formats.
This page collects the diagrams that go with it.

## Overview diagrams

```{figure} digitisation-pipeline.svg
:alt: The digitisation pipeline, from physical object to navigable interface
:width: 100%

Four stages from physical object to operational interface. Digitisation alone (stage 2) only
copies the physical form -- the logical structure becomes navigable only once markup and
retrieval logic (stages 3-4) explicitly rebuild the finding devices the original creator relied
on.
```

```{figure} physical-vs-logical-structure.svg
:alt: Physical structure and logical structure as two parallel, loosely aligned hierarchies
:width: 100%

Physical containment (archive > box > folder > item > page > column) and logical containment
(collection > document > section > paragraph) as two hierarchies that don't line up 1:1 -- a
paragraph can cross a page break, a folder can hold parts of several documents.
```

```{figure} wayfinding-devices.svg
:alt: Devices creators used to organise and aid retrieval, grouped by function
:width: 100%

The finding devices from the glossary's "paratext and apparatus" section, grouped by the
function they perform: orientation on the page, location and citation, cross-document
navigation, and provenance -- plus the ordering principles (alphabetical, chronological, ...)
any of them can use as a sort key.
```

```{figure} dar-pipeline.svg
:alt: The Document Analysis and Recognition pipeline, from a page image to a machine-readable logical structure
:width: 100%

Glossary section 6's own pipeline: physical layout analysis segments a page image into zones,
zone classification labels their content type, logical layout analysis assigns semantic roles
and a reading order, and document structure nests the result into sections and documents,
recorded in a ground-truth format (PAGE, ALTO). The core claim: stage 2 already commits to a
segmentation before stage 4 knows what any zone means, which is why a wrong physical cut can't
be fixed by better labelling downstream.
```

```{figure} Digitised_archive_structure_reduced.png
:alt: The archival description hierarchy mapped onto the flat, per-file digitisation output it produces
:width: 100%

The pipeline in miniature: an archival description hierarchy (archive, series, subseries,
subsubseries, file) on the left, and the flat, per-file run of digitised images and
transcriptions it produces on the right -- each file getting its own numbered image/
transcription sequence, with no further structure encoded within it.
```

```{figure} hierarchy-to-digitisation-output.svg
:alt: A style-matched diagram of the archival hierarchy mapped onto flat per-file digitisation output
:width: 100%

The same idea redrawn to match the diagrams above, with the mapping made explicit: fonds,
series, and file (section 1) are the containers the archive's own arrangement defines, and the
pipeline reproduces exactly this much of it -- each file gets its own numbered run of images and
transcriptions (section 4: digital surrogate, OCR/HTR). A file can bundle several Documents
(section 2) -- loose correspondence, a bound volume with its own index and a run of entries --
and nothing in the image/transcription numbering marks where one Document ends and the next
begins. That gap is exactly what the NL-AsdSAA 89.14 worked example below, and document
segmentation (section 6), exist to close.
```

## Worked examples (real archival material)

Examples from actual inventories, showing where these concepts meet real page sequences.
The administrative-devices example has been carried through to a second real instance and a
generalised diagram; the page-type-cluster example now has a different-collection-type variant
below it, and its structural edge case is further down the page.

```{figure} hierarchical-structures-cropped.png
:alt: A sequence of scans annotated with page-type clusters, showing hierarchical page structure
:width: 100%

Fifteen consecutive scans from a single inventory, annotated with the page-type clusters
(coloured regions) assigned to each page -- an example of the recurring layout vocabulary
described in the [Introduction](../introduction.md).
```

```{figure} hierarchical-structures-military-register-clusters.png
:alt: Page-type clusters for a colonial military control-book, contrasted with the resolutions book's vocabulary
:width: 100%

The same idea, a different collection type: eight scans from NL-HaNA 2.10.50, inventory 10 (a
"Controleboek" of European officers serving in the colonial army), read for recurring page
types rather than clustered algorithmically. Where the resolutions book's vocabulary is front
matter / index / session-opening / resolution, this pre-printed control-book's is cover /
endpaper / blank flyleaf / dense form-table record -- plus one type the resolutions book never
needs: a physically pasted correction slip glued over an earlier entry (scan 7).
```

```{figure} administrative_devices-hierarchical_structures_resolutions.png
:alt: Annotated scans from a resolution book showing an index section and a resolutions section
:width: 100%

*Nationaal Archief, NL-HaNA 1.01.02, inventory 3809 -- resolutions of the States General, 1754.*
The volume's own hierarchical structure: an index section (scans 5-64) and a resolutions
section (scans 65-490), with sessions, attendance lists, and individual resolutions marked
within a single scan -- administrative devices doing the organisational work described in
[Concepts](concepts-physical-to-logical-structure.md).
```

```{figure} administrative_devices-hierarchical_structures_resolutions-inv3821.png
:alt: A second annotated resolutions-book example, inventory 3821, showing the index/resolutions boundary
:width: 100%

*Nationaal Archief, NL-HaNA 1.01.02, inventory 3821 -- resolutions of the States General, 1766.*
A second instance of the same pattern, at the scan-level boundary between the index (ending
page 92) and the resolutions proper (restarting its own pagination at 169): an index-entry pair
of scans, then the resolutions' own title opening, its first dated session, and two further
resolutions marked within their scans. Section boundaries and page numbers come from this
inventory's own structural ground truth; the boxes marking session opening/resolutions are a
first, approximate pass -- read from the thumbnail layout rather than the transcription, so
worth checking against the actual pages. No attendance list happens to be visible in this
particular span (compare with inventory 3809 above, where a similar visual survey found one).

## Generalising the pattern

The two resolutions-book examples above are one instance of a structure that recurs across
many kinds of serial administrative record: a volume with its own front matter and internal
index, followed by a main sequence of dated (or otherwise ordered) units, each opening with a
marker of its place in that sequence and containing one or more individual entries.

```{figure} administrative-devices-generalised.svg
:alt: A generalised diagram of a structured register's internal composition, independent of any one archive
:width: 100%

The volume-level structure (front matter, index, main sequence) on the left, and one unit of
the main sequence expanded on the right (opening/dateline, an optional attendance or presence
list, then its entries) -- with three other record types the same shape fits: a notarial
protocol (unit = act), a correspondence register (unit = date received), alongside the
resolutions book (unit = session).
```

The index at the front of the volume is a retrieval device in the sense of the glossary's
section 3: it re-orders the volume's own content around an independent access key rather than
the volume's reading order -- the same inverse relationship between an index and a table of
contents described there. That it keeps its own separate pagination (ending at 92, with the
resolutions restarting at 169) is itself a small piece of evidence that the index was produced,
and is read, as a structurally distinct unit from what follows it.

Each unit of the main sequence -- a day's session, a notarial act, a received letter -- is a
Document in the sense of section 2: an intellectually coherent whole that need not coincide with
a physical page boundary, since a session's resolutions routinely run on past a page turn. The
dateline or other marker that opens it is administrative metadata (section 2 again): it records
where the unit sits in a chronological, or otherwise chosen, ordering principle (section 3).
The entries nested inside it are the document elements one level further down the same container
hierarchy that, on the physical side (section 1), bottoms out in the fonds &rarr; series &rarr;
file/inventory number &rarr; item chain archival description uses to describe the same volume
from the outside. An attendance or presence list, where one appears, is administrative metadata
of a different kind again -- not a retrieval device and not quite a document element, but a
record of who exercised authority over that particular unit, which is exactly the contextual
detail that lets a search result be attributed to a specific meeting rather than just a page.

## Structural edge cases

The page-type-cluster example above assumes the scan sequence is already a faithful, in-order
rendering of document order. It usually isn't. The
[challenges catalog](../challenges/page_order_problems.md) formalises the ways that assumption
fails; here is the sharpest of them worked through.

```{figure} hierarchical-structures-edge_case-interleaving.png
:alt: A canonical page stream and an observed, interleaved page stream for two documents, with the segmentation failure explained
:width: 100%

Foreign-document insertion ([Case 2](../challenges/page_order_problems.md#case-2-foreign-document-insertion-interleaving)):
two 2-page deeds digitised as one member of their shuffle product. Neither of
[document segmentation](../challenges/document_segmentation.md)'s two framings survives this
intact -- one has no vocabulary for a document resuming after an interruption, the other gets
every pairwise judgement right and still splits `D1` into two spurious one-page groups, because
both assume a document occupies one contiguous run of the stream.
```

```{toctree}
:maxdepth: 1
:hidden:

concepts-physical-to-logical-structure
```
