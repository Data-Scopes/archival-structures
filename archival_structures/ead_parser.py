import re
import xml.etree.ElementTree as ET
import copy
from collections import defaultdict
from typing import Dict, Generator, List, Union

import pandas as pd


def unit_has_inv_num_unitid(unit: dict):
    return re.match(r"\d+", unit['unitid'])


def file_has_inv_num_unitid(file: dict):
    return any(unit_has_inv_num_unitid(unit) for unit in file['unitid'])


def get_inv_num_unit(file: dict):
    for unit in file['unitid']:
        if unit_has_inv_num_unitid(unit):
            return unit
    return None


def extract_filegroup_info(inv_num_file: dict):
    filegroups = []
    filegroup_ids = []

    if 'filegroup' in inv_num_file:
        if isinstance(inv_num_file['filegroup'], list):
            for filegroup in inv_num_file['filegroup']:
                filegroups.append(filegroup['title'])
                filegroup_ids.append(filegroup['id'])
        elif isinstance(inv_num_file['filegroup'], dict) and 'title' in inv_num_file['filegroup']:
            filegroups.append(inv_num_file['filegroup']['title'])
            filegroup_ids.append(inv_num_file['filegroup']['id'])

    return filegroups, filegroup_ids


def extract_subseries_info(inv_num_file: dict):
    subseries = []
    if 'subseries' in inv_num_file:
        if isinstance(inv_num_file['subseries'], list):
            for sub in inv_num_file['subseries']:
                subseries.append(sub['title'])
        elif isinstance(inv_num_file['subseries'], dict) and 'title' in inv_num_file['subseries']:
            subseries.append(inv_num_file['subseries']['title'])

    return subseries


def extract_file_info(inv_num_file: dict):
    files = []
    file_ids = []
    file_dates = []
    mets_file = None

    if 'file' in inv_num_file:
        file_list = None
        if isinstance(inv_num_file['file'], list):
            file_list = inv_num_file['file']
        elif isinstance(inv_num_file['file'], dict) and 'title' in inv_num_file['file']:
            file_list = [inv_num_file['file']]
        for file in file_list:
            try:
                files.append(file['title'])
                if 'identifier_text' in file:
                    file_ids.append(file['identifier_text'])
                else:
                    file_ids.append(get_inv_num_unit(file))
            except (KeyError, TypeError):
                print(file)
                print(inv_num_file)
                raise

            if 'unitdate' in file and file['unitdate'] is not None:
                unit_date = None
                if isinstance(file['unitdate'], list) and isinstance(file['unitdate'][0], dict):
                    unit_date = file['unitdate'][0]
                elif isinstance(file['unitdate'], dict):
                    unit_date = file['unitdate']
                if 'date' in unit_date:
                    file_dates.append(unit_date['date'])

            if 'dao' in file and file['dao'] is not None:
                dao = None
                if isinstance(file['dao'], list) and isinstance(file['dao'][0], dict):
                    dao = file['dao'][0]
                elif isinstance(file['dao'], dict):
                    dao = file['dao']
                if 'role' in dao and dao['role'] == 'METS' and 'href' in dao:
                    mets_file = dao['href']

    return files, file_ids, file_dates, mets_file


def extract_inv_num_file_info(inv_num_file, max_subseries_depth: int = None):
    unit = get_inv_num_unit(inv_num_file['file'])
    series = [inv_num_file['series']['title']]
    subseries = extract_subseries_info(inv_num_file)
    filegroups, filegroup_ids = extract_filegroup_info(inv_num_file)
    files, file_ids, file_dates, mets_file = extract_file_info(inv_num_file)

    if len(filegroups) == 0:
        filegroups = [None]
        filegroup_ids = [None]
    elif len(filegroups) > 1:
        filegroups = filegroups[:1]
        filegroup_ids = filegroup_ids[:1]

    subseries = subseries[:max_subseries_depth]
    if len(subseries) < max_subseries_depth:
        subseries += [None] * (max_subseries_depth - len(subseries))

    file_string = files[0] if len(files) > 0 else None
    file_date = file_dates[0] if len(file_dates) > 0 else None

    row = series + subseries + filegroups + filegroup_ids
    row += [file_string, file_date, unit['unitid'], mets_file]
    return row


def get_inventory_files_info(files_info):
    file_files = [fi for fi in files_info if 'file' in fi]
    unit_files = [fi for fi in file_files if 'unitid' in fi['file']]
    return [fi for fi in unit_files if file_has_inv_num_unitid(fi['file'])]


def get_inventory_info(ead_file, max_subseries_depth: int = 2):
    rep_ead = read_ead_file(ead_file)
    rep_dsc = get_desc(rep_ead)
    files_info = get_files_info(rep_dsc)
    inv_files_info = get_inventory_files_info(files_info)

    rows = []

    for inv_file_info in inv_files_info:
        if 'file' not in inv_file_info:
            continue
        row = extract_inv_num_file_info(inv_file_info, max_subseries_depth=max_subseries_depth)
        rows.append(row)

    subseries_cols = [f"subseries_{i + 1}" for i in range(max_subseries_depth)]
    columns = ['series'] + subseries_cols
    columns += [
        'filegroup',
        'inventory_range',
        'file',
        'unitdate',
        'inventory_num',
        'mets_file'
    ]

    return pd.DataFrame(rows, columns=columns)


def parse_other(other: ET.Element, tree_level: int = 0):
    other_info = {}
    for child in other:
        if child.tag == 'p':
            other_info[child.tag] = ''.join([t for t in other.itertext()])
        elif child.tag in {'list'}:
            pass
        else:
            raise ValueError(f'unexpected {other.tag} child {child.tag}')
    return other_info


def parse_odd(odd: ET.Element, tree_level: int = 0) -> Dict[str, any]:
    odd_info = {}
    for child in odd:
        if child.tag == 'p':
            odd_info[child.tag] = child.text
        elif child.tag in {'list'}:
            pass
        else:
            print(f"child.text: {child.text}")
            print(f"child.attrib: {child.attrib}")
            raise ValueError(f'unexpected odd child {child.tag}')
    return odd_info


def parse_otherfindaid(otherfindaid: ET.Element, tree_level: int = 0) -> Dict[str, any]:
    otherfindaid_info = {}
    for child in otherfindaid:
        if child.tag == 'p':
            otherfindaid_info[child.tag] = child.text
        else:
            raise ValueError(f'unexpected otherfindaid child {child.tag}')
    return otherfindaid_info


def parse_access(access: ET.Element, tree_level: int = 0) -> Dict[str, any]:
    access_info = {}
    for child in access:
        if child.tag == 'genreform':
            access_info = {
                child.tag: child.text
            }
            access_info.update(child.attrib)
        else:
            raise ValueError(f'unexpected controlaccess child {child.tag}')
    return access_info


def parse_physdesc(physdesc: ET.Element, tree_level: int = 0) -> Dict[str, any]:
    physical_info = {}
    # print('PHYSDESC:', physdesc.text)
    if physdesc.text != '':
        physical_info['extent'] = physdesc.text
    for child in physdesc:
        if child.tag in {'extent', 'physfacet'}:
            physical_info[child.tag] = child.text
        else:
            raise ValueError(f'unexpected physdesc child {child.tag}')
        physical_info.update(child.attrib)
    return physical_info


def parse_did(did: ET.Element, tree_level: int = 0) -> Dict[str, any]:
    did_info = {
        'unitid': [],
        'unitdate': None,
        'unittitle': None,
        'physdesc': None,
        'text': None,
        'dao': None
    }
    for child in did:
        # print('CHILD.TAG:', child.tag)
        if child.tag == 'dao':
            did_info['dao'] = {attr: child.attrib[attr] for attr in child.attrib}
            if child.text is not None and len(child.text.strip()) > 0:
                did_info['dao']['text'] = child.text
        if child.tag == 'unittitle':
            if child.find('unitdate') is not None:
                unitdate = child.find('unitdate')
                did_info[child.tag] = unitdate.text
                did_info['unitdate'] = {'date': unitdate.text}
                did_info['unitdate'].update(unitdate.attrib)
            else:
                # print('\t\tTITLE:', child.text)
                did_info[child.tag] = child.text
        elif child.tag == 'unitid':
            unitid = {
                child.tag: child.text
            }
            unitid.update(child.attrib)
            did_info[child.tag].append(unitid)
        elif child.tag == 'unitdate':
            did_info[child.tag] = {'date': child.text}
            did_info[child.tag].update(child.attrib)
        elif child.tag == 'physdesc':
            did_info['physdesc'] = parse_physdesc(child)
    return did_info


def get_series(dsc: ET.Element) -> Generator[ET.Element, None, None]:
    for child in dsc:
        if child.tag == 'c' and child.attrib['level'] == 'series':
            yield child


def get_subseries_titles(file_info: Dict[str, any]) -> List[str]:
    if isinstance(file_info, list):
        print(f"get_subseries_titles- file_info is of type list:\n", file_info)
    return [sub['title'] for sub in file_info['subseries']] if 'subseries' in file_info else []


def get_subsubseries_titles(file_info: Dict[str, any]) -> List[str]:
    if isinstance(file_info, list):
        print(f"get_subsubseries_titles- file_info is of type list:\n", file_info)
    if 'subseries' in file_info:
        return [sub['title'] for sub in file_info['subseries'] if 'subsubseries' in sub]
    else:
        return []


def parse_series(series: ET.Element, tree_level: int = 0, debug: int = 0):
    series_info = {
        'series': {}
    }
    files_info = []
    for child in series:
        if child.tag == 'did':
            did_info = parse_did(child)
            series_info['series'] = {
                'title': did_info['unittitle'],
                'id': did_info['unitid']
            }
            if tree_level <= 1 and debug > 0:
                print(f"\nparse_series - tree_level {tree_level} - series title: {did_info['unittitle']}")
            elif tree_level <= 4 and debug > 0:
                print(f"{'  ' * tree_level}parse_series - tree_level {tree_level} - series title: {did_info['unittitle']}")
        elif child.tag == 'c' and child.attrib['level'] == 'series':
            series_files_info = parse_series(child, tree_level=tree_level + 1)
            for s in series_files_info:
                if isinstance(s, str):
                    print('parse_series - series - files_info is string')
                    print(f'\tchild.tag: {child.tag}\ttext: {child.text}')
            # print(f"ead_parser.parse_series - child 'c' with level 'series':")
            # print(series_files_info)
            files_info.extend(series_files_info)
        elif child.tag == 'c' and child.attrib['level'] == 'subseries':
            subseries_files_info = parse_subseries(child, series_info, tree_level=tree_level+1)
            for s in subseries_files_info:
                if isinstance(s, str):
                    print('parse_series - subseries - files_info is string')
                    print(f'\tchild.tag: {child.tag}\ttext: {child.text}')
            files_info.extend(subseries_files_info)
        elif child.tag == 'c' and child.attrib['level'] == 'file':
            file_info = parse_file(child, series_info, tree_level=tree_level+1)
            files_info.append(file_info)
        elif child.tag == 'c' and child.attrib['level'] == 'otherlevel' and child.attrib['otherlevel'] == 'filegrp':
            filegroup_info = parse_filegroup(child, series_info, tree_level=tree_level+1)
            files_info.extend(filegroup_info)
        elif child.tag == 'odd':
            odd_info = parse_odd(child, tree_level=tree_level+1)
            files_info.append(odd_info)
        elif child.tag in {'scopecontent', 'userestrict'}:
            # TODO figure out what to do with these
            pass
        else:
            print("parse_series - series_info:", series_info)
            print(f'\tchild.tag: {child.tag}\tattrib: {child.attrib}')
            raise ValueError(f'unexpected series child {child.tag}')
    #print(f'parse_series - len(files_info): {len(files_info)}')
    # print(files_info[0])
    file_files = [fi for fi in files_info if 'file' in fi]
    unit_files = [fi for fi in file_files if 'unitid' in fi['file']]
    # and re.match(r"\d+", fi['file']['unitid'])
    # if len(unit_files) > 0:
        #print(unit_files[0]['file']['unitid'])
        #print(unit_files[-1]['file']['unitid'])
        #print(unit_files[0]['file']['unitid'], unit_files[0]['file']['subseries'])
        #print(unit_files[-1]['file']['unitid'], unit_files[-1]['file']['subseries'])
    return files_info


def parse_subseries(subseries, series_info, tree_level: int = 0, debug: int = 0):
    subseries_info = copy.deepcopy(series_info)
    if 'subseries' not in subseries_info:
        subseries_info['subseries'] = []
    #     try:
    #         subseries_info = {
    #             'series': {
    #                 'title': series_info['title'],
    #                 'id': series_info['id'][0]['unitid']
    #             }
    #         }
    #     except KeyError:
    #         print(series_info)
    #         raise
    files_info = []
    for child in subseries:
        if child.tag == 'did':
            did_info = parse_did(child, tree_level=tree_level+1)
            if 'unitid' not in did_info:
                subseries_info['subseries'].append({'title': did_info['unittitle']})
                print('parse_subseries - no unitid in did_info:', did_info)
                # raise KeyError("no 'unitid' in did_info")
            elif len(did_info['unitid']) == 0:
                subseries_info['subseries'].append({'title': did_info['unittitle']})
                if debug > 0:
                    print('parse_subseries - empty unitid list in in did_info:', did_info)
                # raise IndexError("empty 'unitid' list in did_info")
            else:
                subseries_info['subseries'].append({
                    'title': did_info['unittitle'],
                    'id': did_info['unitid'][0]['unitid']
                })
            if tree_level <= 3 and debug > 0:
                print(f"{'  ' * tree_level}parse_subseries - tree_level {tree_level} - subseries title: {did_info['unittitle']}")
        elif child.tag == 'c' and child.attrib['level'] == 'file':
            file_info = parse_file(child, subseries_info, tree_level=tree_level+1)
            if isinstance(file_info, str):
                print('parse_subseries - file - file_info is string:', file_info)
                print('\tchild.tag:', child.tag)
            files_info.append(file_info)
        elif child.tag == 'c' and child.attrib['level'] == 'otherlevel' and child.attrib['otherlevel'] == 'filegrp':
            filegrp_info = parse_filegroup(child, subseries_info, tree_level=tree_level+1)
            for file_info in filegrp_info:
                if isinstance(file_info, str):
                    print('parse_subseries - filegrp - file_info is string:', file_info)
                    print('\tchild.tag:', child.tag)
            files_info.extend(filegrp_info)
        elif child.tag == 'odd':
            subseries_info['odd'] = parse_odd(child, tree_level=tree_level+1)
        elif child.tag == 'c' and child.attrib['level'] == 'subseries':
            subsubseries_info = parse_subseries(child, subseries_info, tree_level=tree_level+1)
            for s in subsubseries_info:
                if isinstance(s, str):
                    print('parse_subseries - subseries - subseries_info is string:', s)
                    print('\tchild.tag:', child.tag)
            files_info.extend(subsubseries_info)
        elif child.tag in {'otherfindaid'}:
            otherfindaid_info = parse_other(child, tree_level=tree_level+1)
            if isinstance(otherfindaid_info, str):
                print('parse_subseries - otherfindaid - otherfindaid is string:', otherfindaid_info)
                print('\tchild.tag:', child.tag)
            files_info.append(otherfindaid_info)
        elif child.tag in {'bioghist', 'custodhist', 'head', 'p', 'note', 'list', 'item',
                           'processinfo', 'lb', 'arrangement', 'scopecontent', 'separatedmaterial',
                           'relatedmaterial'}:
            pass
            # subsubseries_info = parse_subseries(child, subseries_info)
            # files_info.extend(subsubseries_info)
        elif child.tag == 'c' and 'level' in child.attrib:
            if debug > 0:
                print(f"parse_subseries - skipping child with tag 'c' and attributes {child.attrib}")
            pass
        else:
            print('parse_subseries - subseries_info:', subseries_info)
            print(f'unexpected subseries child {child.tag}')
            print(f'\tchild.tag: {child.tag}\tattrib: {child.attrib}')
            raise ValueError(f'unexpected subseries child {child.tag}')
    # print(f'parse_subseries - len(files_info): {len(files_info)}')
    return files_info


def parse_filegroup(filegroup, subseries_info, tree_level: int = 0, debug: int = 0):
    filegroup_info = copy.deepcopy(subseries_info)
    files_info = []
    for child in filegroup:
        if child.tag == 'did':
            did_info = parse_did(child, tree_level=tree_level+1)
            filegroup_info['filegroup'] = {
                'title': did_info['unittitle'],
                'id': did_info['unitid'][0]['unitid']
            }
            if tree_level <= 4 and debug > 0:
                print(f"{'  ' * tree_level}parse_filegrp - tree_level {tree_level} "
                      f"- filegrp title: {did_info['unittitle']}")
        elif child.tag == 'c' and child.attrib['level'] == 'file':
            file_info = parse_file(child, filegroup_info, tree_level=tree_level+1)
            files_info.append(file_info)
        elif child.tag == 'c' and child.attrib['level'] == 'otherlevel' and child.attrib['otherlevel'] == 'filegrp':
            subfilegroup_info = parse_filegroup(child, filegroup_info, tree_level=tree_level+1)
            files_info.extend(subfilegroup_info)
            # raise ValueError(f'unexpected extra level of filegroup')
        elif child.tag in {'odd', 'otherfindaid', 'scopecontent', 'phystech', 'altformavail'}:
            filegroup_info['other'] = parse_other(child, tree_level=tree_level+1)
        elif child.tag == 'otherfindaid':
            filegroup_info['otherfindaid'] = parse_otherfindaid(child, tree_level=tree_level+1)
        elif child.tag in {'separatedmaterial', 'bioghist', 'bibliography', 'custodhist'}:
            filegroup_info[child.tag] = child.text
        else:
            print('parse_filegroup - subseries_info:', subseries_info)
            print('parse_filegroup - filegroup_info:', filegroup_info)
            print(f'unexpected filegroup child {child.tag}')
            print(f'\tchild.tag: {child.tag}\tattrib: {child.attrib}')
            raise ValueError(f'unexpected filegroup child {child.tag}')
    return files_info


def parse_file(file, subseries_info, tree_level: int = 0, debug: int = 0):
    # print('file.attrib:', file.attrib)
    file_info = {'file': {}}
    # print('subseries_info:', subseries_info)
    for field in subseries_info:
        # print('field:', field)
        if isinstance(field, dict):
            print('parse_file - subseries_info is of type dict:', subseries_info)
        if field in {'series', 'subseries', 'filegroup'}:
            file_info[field] = copy.deepcopy(subseries_info[field])
        #             file_info[field] = {
        #                 'id': subseries_info[field]['id'],
        #                 'title': subseries_info[field]['title']
        #             }
        else:
            file_info[field] = copy.deepcopy(subseries_info[field])
    for child in file:
        if child.tag == 'did':
            did_info = parse_did(child, tree_level=tree_level+1)
            file_info['file']['title'] = did_info['unittitle']
            if tree_level <= 4 and debug > 0:
                print(f"{'  ' * tree_level}parse_file - tree_level {tree_level} "
                      f"- file title: {did_info['unittitle']}")
            file_info['file']['unitid'] = []
            # print('\tdid:', did_info)
            for unitid in did_info['unitid']:
                file_info['file']['unitid'].append(unitid)
                if 'type' in unitid and unitid['type'] == 'ABS':
                    file_info['file']['id'] = unitid['unitid']
                elif 'type' in unitid and unitid['type'] == 'handle':
                    file_info['file']['handle'] = unitid['unitid']
                elif 'type' not in unitid and 'identifier' in unitid:
                    file_info['file']['identifier'] = unitid['identifier']
                    file_info['file']['identifier_text'] = unitid['unitid']
                elif 'type' in unitid and unitid['type'] in {'handle', 'blank', 'obsolete'}:
                    continue
                elif 'type' not in unitid:
                    file_info['file']['extra_id'] = unitid['unitid']
                else:
                    print('parse_file - cannot parse type of unitid dict')
                    print('subseries_info:', subseries_info)
                    print('file_info:', file_info)
                    print('did_info:', did_info)
                    print('unitid:', unitid)
                    raise KeyError('missing "type" in unitid')
            for field in did_info:
                if field in {'unitid', 'unittitle'}:
                    continue
                file_info['file'][field] = did_info[field]
        elif child.tag == 'c' and 'level' in child.attrib:
            file_info['file']['level'] = child.attrib['level']
        elif child.tag == 'c':
            print('parse_file - unexpected child of file with tag "c"')
            print('\tfile_info:', file_info)
            print('\tfile child c:', child.attrib)
            raise ValueError('unexpected child c of file')
        elif child.tag == 'controlaccess':
            file_info['file']['access'] = parse_access(child, tree_level=tree_level+1)
    # print('file_info:', file_info)
    return file_info


def get_files_info(dsc: ET.Element):
    files_info = []
    for series in get_series(dsc):
        series_files_info = parse_series(series)
        for series_file_info in series_files_info:
            if isinstance(series_file_info, dict) is False:
                print(f"get_files_info - series_file_info is not of type dict:", series_file_info)
                print(f'\tseries.tag: {series.tag}\tattrib: {series.attrib}')
        files_info.extend(series_files_info)
    return files_info


def get_series_files(dsc: ET.Element):
    files_info = get_files_info(dsc)
    print(f"get_series_files -  number of files_info elements:", len(files_info))
    series_files = defaultdict(list)
    subseries_files = defaultdict(list)
    subsubseries_files = defaultdict(list)

    for fi, file_info in enumerate(files_info):
        try:
            subseries_titles = get_subseries_titles(file_info)
            #print(f"len(subseries_titles): {len(subseries_titles)}")
            subsubseries_titles = get_subsubseries_titles(file_info)
        except TypeError:
            print('NO SUBSERIES TITLE:', fi, files_info[fi - 1])
            raise
        if 'file' not in file_info:
            continue
        
        if 'id' in file_info['file']:
            if len(subseries_titles) > 0:
                subseries_files[subseries_titles[0]].append(file_info['file']['id'])
            if len(subsubseries_titles) > 0:
                subsubseries_files[subsubseries_titles[0]].append(file_info['file']['id'])
            try:
                series_files[file_info['series']['title']].append(file_info['file']['id'])
            except TypeError:
                print(f"get_series_files - fail to parse series in file_info")
                print("\tfile_info['series']:", file_info['series'])
                raise
        else:
            # print(file_info)
            continue
    return series_files, subseries_files, subsubseries_files


def get_desc(root: ET.Element) -> Union[ET.Element, None]:
    for root_child in root:
        if root_child.tag == 'archdesc':
            archdesc = root_child
            for child in archdesc:
                if child.tag == 'dsc':
                    return child
    return None


def read_ead_file(ead_file: str) -> ET:
    tree = ET.parse(ead_file)
    root = tree.getroot()
    return root
