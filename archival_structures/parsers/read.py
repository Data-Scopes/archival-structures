"""Loading EAD/METS XML documents (`xml.etree.ElementTree`-based) for
`archival_structures.parsers.ead_parser`/`mets_parser`.

`EADReader` (at the bottom of this file) is a separate, unfinished EAD-reading class written
against a BeautifulSoup-style API (`.attrs`, `.find_all`, `.name`) rather than
`xml.etree.ElementTree`'s (`.attrib`, `.findall`, `.tag`) -- the only XML library actually used
elsewhere in this module. Calling its methods on an `ET.Element` (e.g. the result of
`read_ead`) raises `AttributeError` as soon as any of those BeautifulSoup-only attributes are
accessed. It is referenced from one notebook (`notebooks/download_thumbnails.ipynb`) but has no
tests; `archival_structures.parsers.ead_parser`'s `parse_*` functions are the working,
`ET`-based EAD-parsing path. Documented as observed (describing apparent intent) rather than
silently fixed or removed.
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


class EADReader:
    """An alternative EAD reader (see this module's docstring for why it's currently
    non-functional against `ET.Element` input)."""

    @staticmethod
    def is_file(c):
        """Intended to check whether EAD element `c` has `level='file'`."""
        return 'level' in c.attrs and c.attrs['level'] == 'file'
    
    
    def _get_c_file_info(self, c, debug: int = 0):
        files_info = []
        unit_info = self.get_unit_info(c)
        if self.is_file(c):
            file_info = self.get_file_info(c)
            if debug > 0:
                print(f"EADReader._get_c_file_info - Appending file {file_info['name']}")
                print(f"\t{unit_info}")
            files_info.append(file_info)
        else:
            for child in c:
                if child.name == 'c':
                    if debug > 0:
                        print(f"\nEADReader._get_c_file_info - descending to child {child.name} attrs {child.attrs}")
                        print(f"\t{unit_info}")
                    child_file_info = self._get_c_file_info(child)
                    for cfi in child_file_info:
                        cfi['location'].append(unit_info)
                    if debug > 0:
                        print(f"EADReader._get_c_file_info - Extending with files {[fi['name'] for fi in child_file_info]}")
                    files_info.extend(child_file_info)
        return files_info
    
    def get_ead_file_info(self, ead, debug: int = 0):
        """Intended top-level entry point: collect file-level info for every `<c>` under
        `ead`'s `<dsc>`, recursively (`_get_c_file_info`)."""
        file_info = []
        dsc = ead.find('dsc')
        for child in dsc:
            if child.name == 'c':
                if debug > 0:
                    print(f"EADReader.get_ead_file_info - child {child.name} attrs {child.attrs}")
                c_file_info = self._get_c_file_info(child, debug=debug)
                file_info.extend(c_file_info)
        return file_info
    
    @staticmethod
    def has_handle_attr(element):
        """Intended to check whether `element` has `type='handle'`."""
        return 'type' in element.attrs and element.attrs['type'] == 'handle'

    def get_files(self, ead):
        """Intended to collect every `<c level='file'>` under `ead`."""
        return [c for c in ead.find_all('c') if self.is_file(c)]

    def get_file_identifier(self, file, debug: int = 0):
        """Intended to find `file`'s identifying `<unitid>` (the one with an `identifier`
        attribute), falling back to its name if none has one."""
        unit_ids = [unit_id for unit_id in file.find_all('unitid') if 'identifier' in unit_id.attrs]
        if len(unit_ids) > 1:
            print(f"EADReader.get_file_identifier - number of unit_ids > 1 - file: {file}")
        elif len(unit_ids) == 0:
            name = self._get_name(file)
            return name
        else:
            unit_id = unit_ids[0]
            return unit_id.text
            
    def get_file_handle(self, file):
        """Intended to find `file`'s `<unitid type='handle'>` text, if any."""
        unit_ids = [unit_id for unit_id in file.find_all('unitid') if self.has_handle_attr(unit_id)]
        if len(unit_ids) > 1:
            print(f"EADReader.get_file_handle - number of unit_ids > 1 - file: {file}")
        elif len(unit_ids) == 0:
            return None
        else:
            unit_id = unit_ids[0]
            return unit_id.text
    
    @staticmethod
    def get_file_dao(file):
        """Intended to find `file`'s `<dao>` `href` attribute, if any."""
        dao = file.find('dao')
        if dao is None:
            return None
        else:
            return dao.attrs['href']
    
        
    def get_unit_info(self, c):
        """Intended to extract `c`'s `level`/`<unitid>` id+label+text/`<unittitle>` text."""
        unitid = c.find('unitid')
        unittitle = c.find('unittitle')
        return {
            'level': c.attrs['level'],
            'id': unitid.attrs.get('identifier'),
            'label': unitid.attrs.get('label'),
            'name': unitid.text,
            'title': unittitle.text.strip() if unittitle else None
        }

    def get_file_info(self, file):
        """Intended to assemble `file`'s full info: `get_unit_info` plus its identifier,
        handle, digital-object reference, date, and asset list."""
        unit_id = file.find('unitid')
        file_info = self.get_unit_info(file)
        file_info['inv_num'] = self.get_file_identifier(file)
        file_info['handle'] = self.get_file_handle(file)
        file_info['dao'] = self.get_file_dao(file)
        file_info['location'] = []
        unitdate = file.find('unitdate')
        file_info['date'] = unitdate.text.strip() if unitdate else None
        file_info['date_norm'] = unitdate.attrs.get('normal') if unitdate else None
        file_info['assets'] = self._get_file_assets(file)
        return file_info
    
    def _get_file_assets(self, file):
        assets = file.find('nexus:assets')
        assets_info = []
        if assets is None:
            return []
        for asset in assets.find_all('nexus:asset'):
            asset_info = {
                'asset_order': asset.attrs.get('asset_order'),
                'name': asset.find('nexus:name').text.strip(), 
                'iiif': asset.find('nexus:iiif').text.strip()
            }
            assets_info.append(asset_info)
        return assets_info
    
    def _get_name(self, file):
        unitid = file.find('unitid') 
        return unitid.text
