"""Tests for `archival_structures.parsers.ead_start_end_year.get_begin_end_year`.

No test coverage existed for this module before Step 4 of the EAD parser rewrite started
reusing it (via `archival_structures.parsers.ead_fields.parse_unitdate`) and immediately hit a
`ValueError` on real corpus data: `superseded/NL-HaNA_1.01.02-2025-03-18.xml` has a
`<unitdate normal="1610/1625/1643">` (three discrete years, "1610, 1625 en 1643", not a two-part range), and the original
`date_begin, date_end = date.split("/")` unpacking assumed exactly two parts.
"""

import unittest

from archival_structures.parsers.ead_start_end_year import get_begin_end_year


class TestGetBeginEndYear(unittest.TestCase):

    def test_two_part_range(self):
        self.assertEqual(("1600", "1601"), get_begin_end_year("1600/1601"))

    def test_full_date_range(self):
        self.assertEqual(("1600", "1601"), get_begin_end_year("1600-01-01/1601-12-31"))

    def test_bare_single_year(self):
        self.assertEqual(("1614", "1614"), get_begin_end_year("1614"))

    def test_year_month(self):
        self.assertEqual(("1614", "1614"), get_begin_end_year("1614-10"))

    def test_full_single_date(self):
        self.assertEqual(("1614", "1614"), get_begin_end_year("1614-10-11"))

    def test_empty_string(self):
        self.assertEqual(("", ""), get_begin_end_year(""))

    def test_unrecognised_format_returns_empty(self):
        # length-based format detection (4/7/10 chars) is what the function actually uses, so
        # this needs a string outside all three lengths to exercise the true fallback branch
        self.assertEqual(("", ""), get_begin_end_year("unknown-format-string"))

    def test_three_part_list_of_discrete_years_does_not_raise(self):
        # superseded/NL-HaNA_1.01.02-2025-03-18.xml:248426 -- <unitdate normal="1610/1625/1643">1610, 1625 en 1643</unitdate>
        # first/last taken as begin/end, spanning the list rather than crashing on it
        self.assertEqual(("1610", "1643"), get_begin_end_year("1610/1625/1643"))

    def test_many_part_list_still_takes_first_and_last(self):
        self.assertEqual(("1700", "1750"), get_begin_end_year("1700/1710/1720/1730/1750"))


if __name__ == "__main__":
    unittest.main()
