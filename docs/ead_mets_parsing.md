# EAD/METS finding-aid parsing

A separate concern from the PageXML/image pipeline above: parsing the archival finding-aid
metadata that describes an archive's series/subseries/file structure (EAD --
Encoded Archival Description) and a single inventory number's page manifest and sequence
(METS -- Metadata Encoding and Transmission Standard, referenced from EAD via a `<dao
role='METS'>` element).

{mod}`archival_structures.parsers.ead_parser` recursively walks an EAD `<dsc>` (description of
subordinate components) tree of nested `<c level="...">` elements, carrying each level's
series/subseries/filegroup context down to its descendants, and flattens the result into a
`pandas.DataFrame` with one row per file (`get_inventory_info`).
{mod}`archival_structures.parsers.mets_parser` parses a METS document's file manifest and page
sequence. {mod}`archival_structures.parsers.ead_start_end_year` is a standalone,
lxml-based script for extracting just the start/end year of each inventory number's date range.
{mod}`archival_structures.parsers.read` provides the underlying XML loading
(`xml.etree.ElementTree`-based) -- note that the `EADReader` class at the bottom of that module
is a separate, BeautifulSoup-based reader for a different caller (a scratch notebook that builds
its own BS4 tree), not interchangeable with the `ET.Element`-based functions above it; see its
docstring.

{class}`archival_structures.model.inventory_number.InventoryNumber` parses and sorts archival
inventory numbers in natural archival order (e.g. `42` before `42A` before `43`).

```{eval-rst}
.. automodule:: archival_structures.parsers.ead_parser
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
