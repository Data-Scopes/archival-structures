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


class TestSubsubseriesTitles(unittest.TestCase):

    def test_no_subseries_returns_empty(self):
        self.assertEqual([], ead_parser.get_subsubseries_titles({}))

    def test_single_level_subseries_has_no_subsubseries(self):
        file_info = {'subseries': [{'title': 'top-level subseries', 'id': '1'}]}
        self.assertEqual([], ead_parser.get_subsubseries_titles(file_info))

    def test_nested_subseries_reports_levels_after_the_first(self):
        # mirrors the flat, nesting-ordered list parse_subseries actually builds when it
        # recurses into a <c level="subseries"> nested inside another
        file_info = {'subseries': [
            {'title': 'top-level subseries', 'id': '1'},
            {'title': 'nested subseries', 'id': '1.1'},
        ]}
        self.assertEqual(['top-level subseries', 'nested subseries'],
                         ead_parser.get_subseries_titles(file_info))
        self.assertEqual(['nested subseries'], ead_parser.get_subsubseries_titles(file_info))
