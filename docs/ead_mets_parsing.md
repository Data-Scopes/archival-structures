# EAD/METS finding-aid parsing

A separate concern from the PageXML/image pipeline above: parsing the archival finding-aid
metadata that describes an archive's series/subseries/file structure (EAD --
Encoded Archival Description) and a single inventory number's page manifest and sequence
(METS -- Metadata Encoding and Transmission Standard, referenced from EAD via a `<dao
role='METS'>` element).

**The recommended path, as of the [EAD parser rewrite](ead_parser/plan.md):**
{mod}`archival_structures.parsers.ead_tree`'s `parse_ead`/`parse_ead_string` build a lossless
tree of `EADComponent`s from an EAD `<dsc>` (description of subordinate components) -- `<c>` or
depth-numbered `<c01>`..`<c12>`, whatever the component's `@level` is or isn't -- with no
level-specific logic and 100% measured component coverage across a 47-finding-aid, 3-country
corpus (up from the legacy parser's 21.5%; see the plan's step reports for the full evidence).
{mod}`archival_structures.parsers.ead_fields` interprets `<did>` fields off that tree
(`identify_inventory_number`, dates, physical description); {mod}`archival_structures.parsers.ead_digital`
interprets its four digitisation-linking mechanisms (`<dao>`, `<daogrp>`, Memorix `<assets>`,
handle `<unitid>`s); {mod}`archival_structures.parsers.ead_views` flattens the tree into
`pandas.DataFrame`s, including `legacy_compatible_frame`, a column-for-column replacement for
`ead_parser.get_inventory_info`. {mod}`archival_structures.parsers.ead_stream` is the same tree
builder over `lxml.etree.iterparse` instead, for a finding aid too large to parse in memory
(opt-in -- see its module docstring for when it is, and is not, worth its overhead compared to
`parse_ead`).

**The original implementation**, {mod}`archival_structures.parsers.ead_parser`, recursively
walks the same `<dsc>` tree with one function per expected EAD level, flattening the result into
a `pandas.DataFrame` with one row per file (`get_inventory_info`). It remains in the codebase,
used by `ead_views.legacy_compatible_frame`'s tests as the standard the rewrite is checked
against, but is not the recommended entry point for new code -- see
[the rewrite's findings](ead_parser/plan.md) for why.

{mod}`archival_structures.parsers.mets_parser` parses a METS document's file manifest and page
sequence. {mod}`archival_structures.parsers.ead_start_end_year` is a standalone,
lxml-based script for extracting just the start/end year of each inventory number's date range.
{mod}`archival_structures.parsers.read` provides `ead_parser`'s underlying XML loading
(`xml.etree.ElementTree`-based). It used to also hold `EADReader`, a separate,
BeautifulSoup-based reader for `download_thumbnails.py`'s one real caller; retired in the
rewrite's Step 8 once that caller was migrated onto `ead_tree`/`ead_fields`/`ead_digital` (see
`docs/ead_parser/reports/step-08-migration.md`), taking the BeautifulSoup dependency with it.

{class}`archival_structures.model.inventory_number.InventoryNumber` parses and sorts archival
inventory numbers in natural archival order (e.g. `42` before `42A` before `43`), and is what
`ead_fields.identify_inventory_number` resolves an inventory-number-shaped `<unitid>` into.

{mod}`archival_structures.parsers.ead_harvest` builds the EAD test corpus the rewrite is
measured against -- harvesting from publishers that serve a self-identifying client, and filing
manually downloaded files for those that do not. It also holds the namespace- and
spelling-agnostic component helpers (`local_name`, `is_component`, `extract_ead_identity`) that
EAD 2002's (and EAD3's) several profiles make necessary.
{mod}`archival_structures.parsers.ead_survey` is the independent measurement harness the plan's
coverage figures come from -- independent in the sense of sharing no code with `ead_tree`, so it
can catch the tree builder's own mistakes rather than only the legacy parser's.

```{eval-rst}
.. automodule:: archival_structures.parsers.ead_parser
   :members:

.. automodule:: archival_structures.parsers.ead_harvest
   :members:

.. automodule:: archival_structures.parsers.ead_survey
   :members:

.. automodule:: archival_structures.parsers.ead_tree
   :members:

.. automodule:: archival_structures.parsers.ead_fields
   :members:

.. automodule:: archival_structures.parsers.ead_digital
   :members:

.. automodule:: archival_structures.parsers.ead_views
   :members:

.. automodule:: archival_structures.parsers.ead_stream
   :members:

.. automodule:: archival_structures.parsers.mets_parser
   :members:

.. automodule:: archival_structures.parsers.ead_start_end_year
   :members:

.. automodule:: archival_structures.parsers.read
   :members:

.. automodule:: archival_structures.model.inventory_number
   :members:
```
