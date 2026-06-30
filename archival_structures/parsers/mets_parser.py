"""Parsing METS (Metadata Encoding and Transmission Standard) XML: the file manifest and
page-sequence structure for one archived inventory number (referenced from EAD's `<dao
role='METS'>`, see `archival_structures.parsers.ead_parser.extract_file_info`).
"""

import xml.etree.ElementTree as ET
from typing import List

from archival_structures.parsers.read import read_mets

METS_NS = "{http://www.loc.gov/METS/}"
W3_NS = "{http://www.w3.org/1999/xlink}"
FILE_FIELDS = ['ID', 'USE', 'MIMETYPE', 'SIZE']
W3_FIELDS = ['href', 'type']


def mets_ns(tag: str):
    """`tag` qualified with the METS XML namespace, for use in `ElementTree` find/findall
    paths."""
    return f"{METS_NS}{tag}"


def parse_mets_archive(mets: ET.Element, archive_id: str = None, inventory_num: str = None):
    """Parse a METS document's root element into a dict with `archive_id`, `inventory_num`,
    `file_group` (per-USE file lists, see `get_file_group_info`), and `struct_map` (page
    sequence, see `get_struct_map`)."""
    file_groups = get_file_groups(mets)
    mets_archive = {
        'archive_id': archive_id,
        'inventory_num': inventory_num,
        'file_group': {fg.attrib['USE']: get_file_group_info(fg) for fg in file_groups},
        'struct_map': get_struct_map(mets)
    }
    return mets_archive


def get_struct_map(mets: ET.Element):
    """Map each `fptr`'s `FILEID` (under the `physSequence` `structMap`) to its enclosing
    `<div>`'s attributes -- i.e. the page/scan sequence and labelling for each file."""
    struct_map = mets.find(f".//{METS_NS}structMap")
    # name = c.find("did/unitid[@type='ABS']")
    phys_sec = struct_map.find(f".//{METS_NS}div[@LABEL='physSequence']")
    divs = phys_sec.findall(f".//{METS_NS}div")
    struct_map = {}
    for div in divs:
        for fptr in div.findall(f'.//{METS_NS}fptr'):
            if 'FILEID' not in fptr.attrib:
                raise ValueError(f"mets_parser.get_struct_map - no FILEID in fptr object\n"
                               f"fptr: {fptr.attrib}\n\n"
                               f"div: {div.text}\n\n")
            file_id = fptr.attrib['FILEID']
            struct_map[file_id] = {k: v for k, v in div.attrib.items()}
    return struct_map


def get_file_group_info(file_group: ET.Element):
    """`parse_file` applied to every `<mets:file>` in `file_group`."""
    files = get_files(file_group)
    return [parse_file(file) for file in files]


def get_file_groups(mets: ET.Element) -> List[ET.Element]:
    """`mets`'s `<mets:fileGrp>` elements (under `<mets:fileSec>`) -- one per file use/type
    (e.g. master images, thumbnails)."""
    file_sec = mets.find(f'.//{METS_NS}fileSec')
    return file_sec.findall(f'./{METS_NS}fileGrp')


def get_files(file_group: ET.Element) -> List[ET.Element]:
    """`file_group`'s direct `<mets:file>` children."""
    return file_group.findall(f'{METS_NS}file')


def parse_file(file: ET.Element):
    """Parse a `<mets:file>` element into a dict of its `FILE_FIELDS` attributes
    (`ID`/`USE`/`MIMETYPE`/`SIZE`, whichever are present) plus its `<mets:FLocat>` child's
    `href`/`type`/`LOCTYPE`."""
    file_info = {field: file.attrib[field] for field in FILE_FIELDS if field in file.attrib}
    file_locator = file.find(f'./{METS_NS}FLocat')
    if file_locator is not None:
        for field in W3_FIELDS:
            if f"{W3_NS}{field}" in file_locator.attrib:
                file_info[field] = file_locator.attrib[f"{W3_NS}{field}"]
        if 'LOCTYPE' in file_locator.attrib:
            file_info['LOCTYPE'] = file_locator.attrib['LOCTYPE']
    return file_info
