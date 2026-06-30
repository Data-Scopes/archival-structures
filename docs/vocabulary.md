# Annotation vocabulary

{mod}`archival_structures.datasets.vocabulary` defines a multi-level, extensible tag vocabulary
for annotating scans, pages, and regions -- used by
{class}`~archival_structures.datasets.annotations.ScanAnnotation`'s `tags`/`pages`/`lines`/
`regions` fields.

## The tag grammar

Every tag is a string of the form

```
namespace:type(:subtype)?(#number)?
```

reusing SegmOnto's <https://segmonto.github.io> own `Type(:subtype)?(#N)?` grammar for zone/line
typing unchanged. The same grammar applies at every level of granularity a scan can be tagged
at -- the whole image, one page (verso/recto) of a two-page opening, or one line/region within a
page -- only what a tag list is *attached to* changes between levels, not the tags themselves:

- **Scan**: `ScanAnnotation.tags`, keyed by `scan_id` alone.
- **Page**: `ScanAnnotation.pages`, keyed by `scan_id` + side (`'verso'`/`'recto'`) -- the split
  point is `ScanAnnotation.opening.separation_x`, an existing coordinate, not a new one.
- **Region**: `ScanAnnotation.lines` (keyed by an existing PageXML line id) or
  {class}`~archival_structures.datasets.annotations.RegionTag` (keyed by a PageXML element id
  when one matches, or a raw pixel box as a fallback, for zones spanning more than one line).

Not every namespace is meaningful at every level -- e.g. `carrier:` only makes sense at the scan
level, since it describes the physical photographic unit, not a sub-region of it. That's a
property of the tag, the same way SegmOnto has zone-only and line-only types sharing one
grammar.

## The four namespaces

- **`carrier:`** -- what the *scan* physically is. Scan-level only. Suggested types:
  `page`, `opening`, `foldout`, `cut`. Informed by standard codicological terminology
  (cf. Muzerelle's *Vocabulaire codicologique*, online as
  [Codicologia](http://codicologia.irht.cnrs.fr/)).
- **`generic:`** -- universal content type, comparable across any archive or inventory number.
  Valid at any level. Mostly SegmOnto's own zone typology (renamed to drop the `Zone` suffix,
  since the same types apply at scan/page/region granularity here, not just to PAGE-XML-style
  regions), plus `list` and `form` -- two types SegmOnto doesn't have, since it comes out of
  manuscript/print layout analysis rather than administrative/record-keeping documents.
  `form`'s `label`/`value` subtypes borrow the question/answer (fixed-prompt vs.
  variable-response) distinction from form-understanding research (e.g. the FUNSD task).
  Suggested types: `running_text`, `table`, `list`, `form` (`label`/`value`), `marginalia`
  (`note`/`commentary`/`correction`/`variants`), `closing`, `heading`, `title_page`, `cover`,
  `running_title`, `seal`, `stamp` (`postal`/`curatorial`), `damage`
  (`corrosion`/`hole`/`mold`/`peeled`/`soaked`), `graphic`
  (`illustration`/`ornamentation`/`figure`), `numbering` (`page`/`other`), `quire_marks`
  (`signature`/`catchwords`), `digitization_artefact` (`ruler`/`test_card`), `drop_capital`
  (`historiated`/`floriate`/`flourish`/`voided`/`parted`), `music`, and `custom` (any subtype --
  SegmOnto's `CustomZone` escape hatch, for anything that doesn't fit yet).
- **`position:`** -- where a tagged scan/page/region falls in a document. Suggested types:
  `start`, `continuation`, `end`. Informed by diplomatics' classical tripartite document
  structure (protocol/text/eschatocol, i.e. opening formula / body / closing formula) at the
  structural level only -- not its full medieval-charter terminology (*intitulatio*,
  *corroboratio*, ...), which doesn't fit early-modern administrative material.
- **`doctype:`** -- archive- or inventory-specific document/block type (e.g. `doctype:deed`,
  `doctype:deed:loan_table`, `doctype:minutes`). Deliberately uncontrolled: unlike the other
  three namespaces, there is no suggested type list, because this is exactly the layer meant to
  be discovered iteratively per archive rather than standardised up front. A scan's directory
  path already identifies its archive/inventory number, so a `doctype:` tag doesn't repeat
  that -- `doctype:deed` means "a deed, in whichever archive this annotation lives under".

`is_known_tag` checks a tag against `CARRIER_TYPES`/`GENERIC_TYPES`/`POSITION_TYPES` for
documentation/autocomplete purposes; it does not gate what can be stored. Nothing in this
package rejects an unrecognised tag -- the whole point of the design is that the vocabulary
grows as new content types are encountered, the same way SegmOnto's own guidelines describe
`CustomZone` subtypes as "any convenient typology the user chooses".

## Worked example

An `NL-AsnDA_0114.11_1` opening where the verso continues a minutes register and the recto
starts a new deed with a closing block and a loan table:

```
scan:  carrier:opening
verso: generic:running_text, doctype:minutes, position:continuation
recto: generic:running_text, doctype:deed, position:start
  region (closing block): generic:closing, doctype:deed:closing
  region (loan table):    generic:table,   doctype:deed:loan_table
```

The `generic:` tags are what let analysis compare across `NL-AsnDA` and `NL-HaNA` even though
`doctype:` vocabularies never need to align between archives -- `doctype:` values aren't
controlled at all, and nothing stops `doctype:deed:closing` in one archive and
`doctype:testimony:closing` in another from later turning out to share a `generic:closing`
classification, which is the whole point of keeping the two namespaces separate.

```{eval-rst}
.. automodule:: archival_structures.datasets.vocabulary
   :members:
```
