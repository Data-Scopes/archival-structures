import unittest

import archival_structures.parsers.read as reader


class TestReadingXML(unittest.TestCase):

    def test_reading_from_file(self):
        ead_file = 'data/metadata/eads/NL-HaNA_2.10.36.22.xml'
        ead = reader.read_xml(xml_file=ead_file)
        self.assertEqual('ead', ead.tag)

    def test_reading_from_string(self):
        ead_file = 'data/metadata/eads/NL-HaNA_2.10.36.22.xml'
        with open(ead_file, 'rt') as fh:
            ead_string = fh.read()
            ead = reader.read_xml(xml_string=ead_string)
        self.assertEqual('ead', ead.tag)

    def test_reading_specific_element_as_root(self):
        ead_file = 'data/metadata/eads/NL-HaNA_2.10.36.22.xml'
        root_name = 'eadheader'
        root = reader.read_xml(xml_file=ead_file, root_name=root_name)
        self.assertEqual(root_name, root.tag)
