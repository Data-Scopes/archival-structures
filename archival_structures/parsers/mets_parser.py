import xml.etree.ElementTree as ET
from typing import List

from archival_structures.parsers.read import read_mets

METS_NS = "{http://www.loc.gov/METS/}"
W3_NS = "{http://www.w3.org/1999/xlink}"
FILE_FIELDS = ['ID', 'USE', 'MIMETYPE', 'SIZE']
W3_FIELDS = ['href', 'type']


def mets_ns(tag: str):
    return f"{METS_NS}tag"


def parse_mets_archive(mets: ET.Element, archive_id: str = None, inventory_num: str = None):
    file_groups = get_file_groups(mets)
    mets_archive = {
        'archive_id': archive_id,
        'inventory_num': inventory_num,
        'file_group': {fg.attrib['USE']: get_file_group_info(fg) for fg in file_groups},
        'struct_map': get_struct_map(mets)
    }
    return mets_archive


def get_struct_map(mets: ET.Element):
    struct_map = mets.find(f".//{METS_NS}structMap")
    # name = c.find("did/unitid[@type='ABS']")
    phys_sec = struct_map.find(f".//{METS_NS}div[@LABEL='physSequence']")
    divs = phys_sec.findall(f".//{METS_NS}div")
    struct_map = {}
    for div in divs:
        for fptr in div.findall(f'.//{METS_NS}fptr'):
            file_id = fptr.attrib['FILEID']
            struct_map[file_id] = div.attrib
    return struct_map


def get_file_group_info(file_group: ET.Element):
    files = get_files(file_group)
    return [parse_file(file) for file in files]


def get_file_groups(mets: ET.Element) -> List[ET.Element]:
    file_sec = mets.find(f'.//{METS_NS}fileSec')
    return file_sec.findall(f'./{METS_NS}fileGrp')


def get_files(file_group: ET.Element) -> List[ET.Element]:
    return file_group.findall(f'{METS_NS}file')


def parse_file(file: ET.Element):
    file_info = {field: file.attrib[field] for field in FILE_FIELDS if field in file.attrib}
    file_locator = file.find(f'./{METS_NS}FLocat')
    if file_locator is not None:
        for field in W3_FIELDS:
            if f"{W3_NS}{field}" in file_locator.attrib:
                file_info[field] = file_locator.attrib[f"{W3_NS}{field}"]
        if 'LOCTYPE' in file_locator.attrib:
            file_info['LOCTYPE'] = file_locator.attrib['LOCTYPE']
    return file_info
