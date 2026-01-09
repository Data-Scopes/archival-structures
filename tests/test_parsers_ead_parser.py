import unittest

import archival_structures.parsers.ead_parser as ead_parser


class TestParsingEADs(unittest.TestCase):

    def test_reading_from_file(self):
        ead_file = 'data/metadata/eads/NL-HaNA_2.10.36.22.xml'
        ead = ead_parser.read_ead(ead_file=ead_file)
        self.assertEqual('ead', ead.tag)

    def test_reading_from_string(self):
        ead_file = 'data/metadata/eads/NL-HaNA_2.10.36.22.xml'
        with open(ead_file, 'rt') as fh:
            ead_string = fh.read()
            ead = ead_parser.read_ead(ead_string=ead_string)
        self.assertEqual('ead', ead.tag)
