# Concepts: From Physical Archive to Navigable Logical Structure

A working glossary drawing on archival science, codicology/manuscript studies, information science, and information history.

---

## 1. Physical / Material Structure (Archival Science & Codicology)

Terms describing the *container* and *carrier* — what you can hold, and how it's arranged in space.

| Concept | Definition |
|---|---|
| **Fonds** | The whole body of records organically created, accumulated, and used by a person, family, or organisation in the course of its activities — the top level of archival hierarchy. [1] |
| **Series / Sub-series** | A group of records within a fonds that share a common filing arrangement, function, or activity (e.g. "correspondence 1920–1935"). [1] |
| **File / Item** | Progressively smaller archival units: a *file* is a grouping of items on one subject or transaction; an *item* is the smallest indivisible unit (a letter, a form). [1] |
| **Provenance** | The principle that records should be traceable to the organisation/person that created them; also the documented chain of custody. [1] |
| **Original order (respect des fonds)** | The principle of preserving the order in which the creator organised the records, since that order itself carries evidential meaning. [1] |
| **Box / Folder** | Physical storage containers — often arbitrary (driven by shelf space), and *not* necessarily aligned with intellectual/logical divisions. |
| **Codex** | A bound book made of folded and stitched sheets, as opposed to a scroll — the dominant physical form for manuscripts and books. |
| **Quire / Gathering** | A set of folded sheets (bifolia) nested together and sewn as a structural unit of a codex. |
| **Bifolium / Leaf / Folio** | A bifolium is one folded sheet (= two leaves = four pages); a folio is one leaf, numbered on its recto. |
| **Recto / Verso** | The front (recto) and back (verso) of a leaf. |
| **Foliation vs. Pagination** | Foliation numbers *leaves* (so each number covers 2 pages, recto+verso); pagination numbers *pages* individually — a key historical shift. |
| **Collation** | The physical make-up of a book (how quires are gathered and ordered) as reconstructed by bibliographic examination. |
| **Signature mark / Catchword** | Marks printers/scribes used to keep gatherings in the right order: a signature is a letter/number on the first leaf of a quire; a catchword is the first word of the next page printed at the foot of the current one. |
| **Column (mise-en-page)** | The physical layout grid of a page — columns, ruling, margins — as distinct from the content it carries. |
| **Support / Watermark** | The physical writing material (parchment, paper) and identifying marks embedded in it, used for dating/provenance. |

---

## 2. Logical / Intellectual Structure (Information & Document Science)

Terms describing *what the content means and how it's organised conceptually* — independent of any one physical carrier.

| Concept | Definition |
|---|---|
| **Document** | An intellectually coherent unit of content (a letter, a report, a chapter) — may span multiple physical items or be one of many documents bound in a single physical volume. |
| **Document element / Structural unit** | A generic term for any structural component: section, paragraph, heading, list, table, figure, footnote. |
| **Section** | An ambiguous, level-free term: a contiguous group of units within a larger unit, formed by some shared aspect (type, topic, date, correspondent, procedure). A section can occur at several levels. A volume (inventory file) can have sections that group pages by type or topic; each such section holds one or more documents, chapters, sub-sections, paragraphs or pages; and a document inside a volume section can have its own within-document sections. The hierarchy can therefore recurse (document within section within document within section …), although a repeat of the *same* type at several levels is probably rare within one inventory file. Because of this ambiguity, a section should always be qualified by its level and parent unit (e.g. *volume section*, *within-document section*) or by its type (e.g. *session*, *index*, *register part*), never used bare as a fixed level of the hierarchy. |
| **Logical structure vs. Layout structure** | A classic distinction (from document engineering, e.g. ODA/ISO 8613): *logical* structure is the hierarchy of meaning (chapter→section→paragraph); *layout* structure is the visual/spatial rendering (page→column→line) — they can diverge (a paragraph can span a page break). [2] |
| **Hierarchy of headings** | Nested title levels (H1/H2/H3 …) expressing the intellectual subordination of content, independent of font size. |
| **Reading order** | The intended sequence in which structural units should be consumed — not always identical to physical/spatial order (e.g. multi-column layouts, sidebars, marginal glosses). |
| **Container hierarchy** | The nested levels used to model a resource: e.g. EAD's fonds > series > file > item, or TEI's `<div>` > `<p>` > `<s>`. [3] [1] |
| **FRBR: Work / Expression / Manifestation / Item** | A model (from library/information science) separating: the abstract *Work* (Hamlet), its *Expression* (a specific text/translation), its *Manifestation* (a specific edition), and the *Item* (this physical copy) — extremely useful for separating "logical content" from "physical carrier." [4] |
| **Metadata: descriptive / structural / administrative** | *Descriptive* metadata identifies content (title, author); *structural* metadata records how parts relate and in what order (page 1 follows cover, chapter 2 follows chapter 1); *administrative* metadata records rights, provenance, technical facts. |
| **Markup / Encoding** | The practice of explicitly tagging logical structure in a machine-readable way (e.g. XML), making implicit structure operationalised and navigable. |

---

## 3. Devices Creators Use to Organise & Support Retrieval (Paratext & Apparatus)

The "finding devices" question — how creators embed navigation aids into the artefact itself.

| Concept | Definition |
|---|---|
| **Paratext** | All the material that surrounds and presents a text without being "the text" itself — titles, prefaces, tables of contents, indexes, running heads, notes. [5] |
| **Apparatus (apparatus criticus)** | The scholarly infrastructure attached to a text: variant readings, footnotes, cross-references — originally developed to support textual criticism and navigation between versions. |
| **Marginalia / Gloss** | Notes, comments, or explanations written in the margins — can be by the original creator (authorial) or later readers (reception evidence); interlinear glosses sit *within* the text. |
| **Rubrication / Rubric** | Headings or key structural markers highlighted (originally in red ink) to visually signal structure — an early "structural markup" device. |
| **Running head / Running title** | Repeated header text at the top of each page (chapter title, section name) enabling orientation without reading page numbers. |
| **Pagination / Foliation as navigation** | Sequential numbering enabling precise citation and location — a prerequisite technology for indexes and cross-references to work at all. |
| **Table of contents** | A structural map presented in reading (sequential) order, listing divisions and their locations. |
| **Index** | A retrieval device organised *not* by document order but by an independent access key (subject, name, keyword) pointing to locations — the inverse of a table of contents. [6] |
| **Colophon** | A statement (often at the end of a manuscript/book) giving production facts: scribe, date, place — an early metadata device. |
| **Incipit / Explicit** | The opening ("it begins…") and closing ("it ends…") words of a text, historically used as an identifier before standardised titles existed. |
| **Cross-reference ("see also")** | An explicit pointer from one location to a related one elsewhere, enabling non-linear navigation. |
| **Tab / Thumb index** | Physical cut-in tabs on the page edge enabling direct access to a section (e.g. in Bibles, dictionaries, ledgers). |
| **Ordering principles** | The logical key by which a set of entries is sequenced: **alphabetical**, **chronological**, **numerical**, **geographical**, **systematic/topical** (by subject classification), or **hierarchical/by importance**. Choice of ordering principle *is itself* a retrieval device. |

---

## 4. Digitisation & Encoding Concepts (Making Structure Operational)

| Concept | Definition |
|---|---|
| **Digital surrogate** | A digital reproduction (image/text) standing in for the physical original — a *representation*, not the object itself. |
| **OCR / HTR** | Optical Character Recognition (printed text) / Handwritten Text Recognition — technologies converting page images into machine-readable text. |
| **Transcription: diplomatic vs. normalised** | *Diplomatic* transcription preserves original spelling/layout exactly; *normalised* transcription regularises spelling/punctuation for readability — a choice affecting how "faithful" the logical layer is to the physical one. |
| **TEI (Text Encoding Initiative)** | An XML standard for encoding the logical/textual structure of documents (divisions, paragraphs, notes, variants) — widely used in manuscript/scholarly digitisation. [3] |
| **EAD (Encoded Archival Description)** | An XML standard specifically for encoding archival finding aids, preserving the fonds→series→file→item hierarchy in machine-readable form. [7] |
| **METS (Metadata Encoding & Transmission Standard)** | A wrapper standard binding together descriptive, administrative, and *structural* metadata for a digital object — literally the format that operationalises "which page images belong to which logical divisions." [8] |
| **IIIF (International Image Interoperability Framework)** | A standard for serving/annotating page images that lets structural/logical anchors (a specific column, a specific line) be addressed and linked directly. [9] |
| **Controlled vocabulary / Thesaurus / Taxonomy / Ontology** | Increasingly formal systems for standardising the terms used to index and retrieve content, from simple term lists to full logical models of relationships. |
| **Finding aid** | The descriptive/navigational document (traditionally paper, now often EAD/XML) that mediates between an archive's physical arrangement and a researcher's logical/topical queries. |

---

## 5. Information History Concepts (Context on Why These Devices Emerged)

| Concept | Definition |
|---|---|
| **Order of knowledge / ordering systems** | Historically contingent schemes (alphabetical vs. thematic vs. hierarchical) for arranging knowledge, studied as intellectual history. [6] |
| **Commonplace book** | A personal manuscript compilation of quotations/notes organised under recurring headings — an early personal "database" with user-generated logical structure layered onto miscellaneous physical entries. [6] |
| **Florilegium** | A medieval "anthology of flowers" — excerpted passages compiled and reorganised (often alphabetically or topically) independent of their original documents' order. [6] |
| **Reference genres (encyclopedia, dictionary)** | Genres whose entire physical form is designed around a chosen logical ordering principle (usually alphabetical) to support lookup rather than linear reading. |

---

## 6. Document Analysis and Recognition (DAR) Concepts

Terms from the computer-science field that builds the pipelines this package extends — converting a
document *image* into a structured representation. DAR's own physical/logical vocabulary grew up
independently of, but maps closely onto, the archival and document-engineering terms in sections 1–2;
where the two diverge, the difference is noted below.

| Concept | Definition |
|---|---|
| **Document Image Analysis (DIA)** | The overarching field: everything from binarisation and skew correction through segmentation to full document understanding, applied to a scanned or photographed page. [12] [18] |
| **Physical layout analysis** | Segmenting a page image into regions ("zones") on visual/geometric evidence alone (whitespace, connected components, pixel density) — with no claim yet about what a zone *means*. This is Mise-en-page (§1) and ODA's layout structure (§2) made into an algorithm. [11] [12] |
| **Logical layout analysis** | Assigning each physical zone a semantic role (title, running text, footnote, table, marginal note) and typically a reading order between zones — DAR's operational counterpart to the logical structure / document element distinction in §2. [12] [13] |
| **Top-down vs. bottom-up segmentation** | Two families of physical layout analysis algorithm: top-down methods (e.g. recursive X-Y cut) recursively slice the page along wide corridors of whitespace; bottom-up methods (e.g. the *docstrum*) start from individual connected components (characters, words) and cluster them upward into lines, then blocks. [10] [11] |
| **Zone / region classification** | Labelling each segmented zone by content type — text, image, table, separator, noise — usually a distinct step (and often a distinct classifier) from the segmentation that produced the zone. [13] |
| **Reading order detection** | Determining the sequence in which segmented zones should be read — non-trivial for multi-column layouts, sidebars, marginalia, or a table interleaved with running text; see also Reading order (§2). [15] |
| **Document structure analysis / Document image understanding** | The pipeline from physical + logical layout analysis through to a full logical-structure tree (zones nested into sections, documents); sometimes split into "layout analysis" (zones) versus "structure analysis" (nesting zones into larger units). [12] [13] [14] |
| **Ground-truth formats (PAGE, ALTO)** | Machine-readable schemas for recording a document image's physical/logical structure and OCR/HTR output together, used both as pipeline output and as an annotation format for training and evaluating DAR systems; the PageXML this package consumes follows the PAGE schema. [16] |
| **Historical Document Analysis (HDA)** | The DAR subfield specifically concerned with historical materials (manuscripts, early printed books, archival records), where degraded supports, non-standard or evolving layouts, and historical scripts break assumptions built into DAR systems designed for modern printed documents — the subfield this package's own methods sit in. [17] |

---

## References

1. International Council on Archives (ICA). *ISAD(G): General International Standard Archival Description*, 2nd ed. Ottawa: ICA, 2000.
2. International Organization for Standardization. *ISO/IEC 8613: Information Technology — Open Document Architecture (ODA) and Interchange Format*. Geneva: ISO, 1989.
3. TEI Consortium, eds. *TEI P5: Guidelines for Electronic Text Encoding and Interchange*. Text Encoding Initiative Consortium. https://tei-c.org/guidelines/p5/
4. IFLA Study Group on the Functional Requirements for Bibliographic Records. *Functional Requirements for Bibliographic Records (FRBR): Final Report*. Munich: K.G. Saur, 1998.
5. Genette, Gérard. *Paratexts: Thresholds of Interpretation*. Translated by Jane E. Lewin. Cambridge: Cambridge University Press, 1997.
6. Blair, Ann M. *Too Much to Know: Managing Scholarly Information before the Modern Age*. New Haven: Yale University Press, 2010.
7. Society of American Archivists. *Encoded Archival Description (EAD) Version 3*. https://www.loc.gov/ead/
8. Library of Congress Network Development and MARC Standards Office. *METS: Metadata Encoding and Transmission Standard*. https://www.loc.gov/standards/mets/
9. International Image Interoperability Framework (IIIF) Consortium. *IIIF Specifications*. https://iiif.io/
10. Wong, K. Y., Casey, R. G., and Wahl, F. M. "Document Analysis System." *IBM Journal of Research and Development* 26, no. 6 (1982): 647–656.
11. O'Gorman, Lawrence. "The Document Spectrum for Page Layout Analysis." *IEEE Transactions on Pattern Analysis and Machine Intelligence* 15, no. 11 (1993): 1162–1173.
12. Nagy, George. "Twenty Years of Document Image Analysis in PAMI." *IEEE Transactions on Pattern Analysis and Machine Intelligence* 22, no. 1 (2000): 38–62.
13. Mao, Song, Rosenfeld, Azriel, and Kanungo, Tapas. "Document Structure Analysis Algorithms: A Literature Survey." In *Document Recognition and Retrieval X*, Proceedings of SPIE, vol. 5010 (2003): 197–207.
14. Namboodiri, Anoop M., and Jain, Anil K. "Document Structure and Layout Analysis." In *Digital Document Processing*, edited by Bidyut B. Chaudhuri, 29–48. London: Springer, 2007.
15. Meunier, Jean-Luc. "Optimized XY-Cut for Determining a Page Reading Order." In *Proceedings of the Eighth International Conference on Document Analysis and Recognition (ICDAR 2005)*, 347–351.
16. Pletschacher, Stefan, and Antonacopoulos, Apostolos. "The PAGE (Page Analysis and Ground-Truth Elements) Format Framework." In *Proceedings of the 20th International Conference on Pattern Recognition (ICPR 2010)*, 257–260.
17. Antonacopoulos, Apostolos, and Downton, Andy C. "Special Issue on the Analysis of Historical Documents." *International Journal on Document Analysis and Recognition (IJDAR)* 9, no. 2–4 (2007): 75–77.
18. O'Gorman, Lawrence, and Kasturi, Rangachar. *Document Image Analysis*. Los Alamitos, CA: IEEE Computer Society Press, 1997.

---

*Use this as a working list — happy to expand any section, add further citations, or reorganise by report section.*
