import xml.etree.ElementTree as ET


def read_xml(xml_file: str = None, xml_string: str = None, root_name: str = None):
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
    tree = ET.parse(xml_file)
    return tree.getroot()


def read_xml_string(xml_string: str) -> ET:
    return ET.fromstring(xml_string)


def read_ead(ead_file: str = None, ead_string: str = None) -> ET:
    return read_xml(xml_file=ead_file, xml_string=ead_string, root_name='ead')


def read_mets(mets_file: str = None, mets_string: str = None) -> ET:
    return read_xml(xml_file=mets_file, xml_string=mets_string,
                    root_name='{http://www.loc.gov/METS/}mets')
