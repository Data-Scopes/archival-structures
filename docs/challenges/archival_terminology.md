# Archival terminology

Consistent terms for the archival-description hierarchy -- the levels a finding aid (EAD)
describes an archive at, from the whole collection down to the unit this package actually
processes scans and PageXML for. Defined here so the rest of the docs, the code, and any future
discussion can use one vocabulary instead of drifting between "file", "item", "piece", and
"inventory number" for the same thing.

The levels below follow [ISAD(G)](https://www.ica.org/en/isadg-general-international-standard-archival-description-second-edition)
terminology where it applies, with the Dutch term used by the example archives (see
[Example archives](../archives.md)) alongside it. They also match, level for level, the tree
{mod}`archival_structures.parsers.ead_parser` walks (`series` > `subseries` (possibly nested) >
`filegroup`/`otherlevel` > `file`) and what
{mod}`archival_structures.parsers.mets_parser` calls `inventory_num`.

## The hierarchy

- **Archive / fonds** (NL: *archief*) -- the whole body of records created or accumulated by one
  record-forming body (a person, family, or institution) in the course of its activities,
  described as a unit by one finding aid. Example: `89 Archief van de Vereeniging
  Tentoonstelling De Vrouw` in [Example archives](../archives.md). Not something this package
  processes directly -- it's the top-level scope an EAD document covers.

- **Series** (NL: *rubriek*) -- a group of records within the archive sharing a common function,
  activity, or subject, forming one branch of the archive's classification scheme. EAD:
  `<c level="series">`, parsed by `ead_parser.parse_series` / discovered via `ead_parser.get_series`.

- **Subseries** -- a subordinate grouping within a series, following the same organising
  principle at a finer grain. EAD: `<c level="subseries">`, and can nest more than one level deep
  (a "subsubseries" is just a `subseries`-level `<c>` nested inside another one --
  `ead_parser.get_subsubseries_titles` recovers these from the flat `subseries` list a nested
  walk produces, see [Findings](../findings.md)).

- **File group** (NL: e.g. *bundel*) -- an intermediate grouping of files below subseries level,
  used by some archives' classification schemes and not others. EAD: `<c level="otherlevel">` or
  `<c level="filegrp">`, parsed by `ead_parser.extract_filegroup_info`. Optional: many series go
  straight from subseries to file with no file-group level in between.

- **File** (NL: *dossier* or, in inventory listings, simply the numbered entry) -- the unit one
  inventory number normally identifies: a group of documents relating to a single matter, case,
  transaction, or (for a bound volume) the volume itself. EAD: `<c level="file">`, parsed by
  `ead_parser.extract_file_info`. This is the level {mod}`archival_structures.parsers.mets_parser`
  treats as `inventory_num`, and the level {func}`~archival_structures.parsers.ead_parser.get_inv_num_unit`
  reads the inventory number (`unitid`) from.

- **Item / piece** (NL: *stuk*) -- an individual document within a file. EAD finding aids at the
  level of detail this package works with generally do *not* describe items individually --
  that's exactly the structure [document segmentation](document_segmentation.md) recovers
  automatically from the digitised page stream, rather than reading it off the finding aid.

## Inventory numbers correspond to files -- with caveats

An **inventory number** (NL: *inventarisnummer*) is the archive's own citation unit: the number
researchers use to request or cite a specific physical unit (e.g. `NL-HaNA_1.01.02_3771`). In
every EAD structure this package has parsed so far, the inventory number lives on the `file`-level
`<c>`'s `unitid` (`file_has_inv_num_unitid` / `unit_has_inv_num_unitid` in
{mod}`archival_structures.parsers.ead_parser`) -- so **inventory number = file**, in the sense
above, is the right default assumption for this codebase.

Two caveats worth keeping in mind before leaning on that equivalence elsewhere:

- A file-level inventory number can itself contain more than one *item* in the archival-science
  sense above (e.g. a folder of several distinct letters, or a bound register containing several
  distinct resolutions/deeds) -- "inventory number = file" does not imply "inventory number =
  single document". The scans under one inventory number are exactly the *page stream* that
  [document segmentation](document_segmentation.md) turns into a *document stream*.
- Not every archive's EAD necessarily puts the citable `unitid` at the `file` level -- this
  package's parser currently assumes it does (`get_inventory_files_info` filters to `file`-level
  `<c>`s with an inventory-number-shaped `unitid`), which has held for every EAD export it's been
  run against so far ([Example archives](../archives.md)), but hasn't been checked against every
  possible archive/EAD variant.

## See also

- [Page-stream terminology](page_stream_terminology.md) -- the parallel vocabulary for the
  scans/images *within* one file/inventory number, which is where this package's own analysis
  starts.
- [EAD/METS parsing](../ead_mets_parsing.md) -- how the hierarchy above is actually extracted
  from XML.
