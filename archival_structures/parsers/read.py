"""Loading EAD/METS XML documents for `archival_structures.parsers.ead_parser`/`mets_parser`.

`read_xml`/`read_ead`/`read_mets` below use `xml.etree.ElementTree`, and their output is what
`ead_parser`'s `parse_*` functions expect -- that's the package's working, tested EAD-parsing
path (superseded, for new code, by `archival_structures.parsers.ead_tree.parse_ead`/
`parse_ead_string`, which are namespace- and profile-agnostic in a way these are not -- see
`docs/ead_parser/plan.md`).

`EADReader`, a separate, parallel EAD reader written against a BeautifulSoup `Tag` API, used to
live at the bottom of this file. It was retired in Step 8 of the EAD parser rewrite
(`docs/ead_parser/reports/step-08-migration.md`): its one real caller
(`download_thumbnails.py`'s `DownloadNA.get_archive_file_info`, which built its own BeautifulSoup
tree directly from an OAI-EAD API response rather than going through this module) has been
migrated to `ead_tree.parse_ead_string` + `ead_fields.identify_inventory_number` +
`ead_digital.parse_dao`, and the BeautifulSoup dependency it implied is gone with it.
"""

import xml.etree.ElementTree as ET


def read_xml(xml_file: str = None, xml_string: str = None, root_name: str = None):
    """Parse `xml_file` or `xml_string` (give exactly one) into an `ET.Element`. If
    `root_name` is given and doesn't match the document's root tag, returns the first
    descendant with that tag instead of the root itself."""
    if xml_file is not None:
        root = read_xml_file(xml_file)
    elif xml_string is not None:
        root = read_xml_string(xml_string)
    else:
        raise ValueError(f"must pass either 'xml_file' or 'xml_string'.")
    if root_name is None or root.tag == root_name:
        return root
    else:
        return root.find(f".//{root_name}")


def read_xml_file(xml_file: str) -> ET:
    """Parse the XML file at `xml_file` into its root `ET.Element`."""
    tree = ET.parse(xml_file)
    return tree.getroot()


def read_xml_string(xml_string: str) -> ET:
    """Parse `xml_string` into its root `ET.Element`."""
    return ET.fromstring(xml_string)


def read_ead(ead_file: str = None, ead_string: str = None) -> ET:
    """`read_xml`, defaulting to the `<ead>` root element."""
    return read_xml(xml_file=ead_file, xml_string=ead_string, root_name='ead')


def read_mets(mets_file: str = None, mets_string: str = None) -> ET:
    """`read_xml`, defaulting to the `<mets:mets>` root element."""
    return read_xml(xml_file=mets_file, xml_string=mets_string,
                    root_name='{http://www.loc.gov/METS/}mets')
