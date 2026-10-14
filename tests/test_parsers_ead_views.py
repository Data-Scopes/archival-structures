"""Tests for `archival_structures.parsers.ead_views`.

`unit_of_description_frame` is tested against the exact constructs Step 2/4 found breaking a
`level="file"`-only assumption: a repository with no `file` components at all (its unit is an
`otherlevel` value), and a resolved `file` with an unresolved `item` nested inside it (which must
not become a duplicate row). `legacy_compatible_frame` is tested against
`ead_parser.get_inventory_info`'s actual column order and row selection.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_parser, ead_tree, ead_views

#: Amsterdam-shaped: label rule resolves at file level; a series/subseries has no number of its
#: own (correctly excluded from unit_of_description_frame).
LABELLED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test" countrycode="NL">1</eadid></eadheader>
  <archdesc><dsc>
    <c level="series">
      <did><unitid>I</unitid><unittitle>A series</unittitle></did>
      <c level="file">
        <did><unitid identifier="u1" label="Inventarisnummer">1</unitid>
        <unittitle>A file</unittitle>
        <unitdate normal="1700/1701">1700-1701</unitdate></did>
        <c level="item"><did><unittitle>An item</unittitle></did></c>
      </c>
    </c>
  </dsc></archdesc>
</ead>
"""

#: No `file` components at all -- the description unit is an `otherlevel` value, one level
#: nested under a `series` that itself has no number of its own -- the BHIC/Zwolle construct.
NO_FILE_LEVEL_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test" countrycode="NL">2</eadid></eadheader>
  <archdesc><dsc>
    <c level="series">
      <did><unitid>I</unitid><unittitle>A series</unittitle></did>
      <c otherlevel="Inschrijving" level="otherlevel">
        <did><unitid identifier="e1">42</unitid><unittitle>An entry</unittitle></did>
      </c>
      <c otherlevel="Inschrijving" level="otherlevel">
        <did><unitid identifier="e2">43</unitid><unittitle>Another entry</unittitle></did>
      </c>
    </c>
  </dsc></archdesc>
</ead>
"""


def _parse(xml: str) -> ead_tree.EADFindingAid:
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "fixture.xml"
        path.write_text(xml, encoding="utf-8")
        return ead_tree.parse_ead(path)


class TestToFrame(unittest.TestCase):

    def test_selects_only_the_named_level(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.to_frame(finding_aid, level="file")
        self.assertEqual(1, len(frame))
        self.assertEqual("A file", frame.iloc[0]["title"])

    def test_ancestry_columns_reflect_actual_depth(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.to_frame(finding_aid, level="item")
        self.assertIn("ancestor_1", frame.columns)
        self.assertIn("ancestor_2", frame.columns)
        self.assertEqual("A series", frame.iloc[0]["ancestor_1"])
        self.assertEqual("A file", frame.iloc[0]["ancestor_2"])

    def test_inventory_number_and_dates_are_populated(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.to_frame(finding_aid, level="file")
        self.assertEqual("1", frame.iloc[0]["inventory_number"])
        self.assertEqual("label:inventarisnummer", frame.iloc[0]["inventory_number_rule"])
        self.assertEqual("1700", frame.iloc[0]["year_begin"])

    def test_empty_selection_still_has_the_right_columns(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.to_frame(finding_aid, level="otherlevel:nonexistent")
        self.assertEqual(0, len(frame))
        self.assertIn("title", frame.columns)


class TestUnitOfDescriptionFrame(unittest.TestCase):

    def test_resolves_at_file_level_when_file_carries_the_number(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.unit_of_description_frame(finding_aid)
        self.assertEqual(1, len(frame))
        self.assertEqual("file", frame.iloc[0]["level"])
        self.assertEqual("1", frame.iloc[0]["inventory_number"])

    def test_unresolved_item_does_not_duplicate_its_resolved_parent(self):
        # the item has no unitid at all, so identify_inventory_number returns "none" for it --
        # it must not appear as its own row alongside the file
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.unit_of_description_frame(finding_aid)
        self.assertEqual(1, len(frame), "the unresolved item must not add a second row")

    def test_resolves_at_a_non_file_level_when_that_is_the_real_unit(self):
        # the BHIC/Zwolle construct: no <file> anywhere, the unit is an otherlevel value
        finding_aid = _parse(NO_FILE_LEVEL_EAD)
        frame = ead_views.unit_of_description_frame(finding_aid)
        self.assertEqual(2, len(frame))
        self.assertTrue(all(frame["level"] == "otherlevel:Inschrijving"))
        self.assertEqual(["42", "43"], list(frame["inventory_number"]))

    def test_the_series_itself_is_not_a_row_even_though_its_bare_unitid_would_parse(self):
        # "I" parses fine as an InventoryNumber (a single-letter token) -- not observed as a real
        # series-level convention anywhere in the corpus, but the row-selection logic must still
        # prefer the resolved descendant (the entries) over the resolved ancestor (the series)
        finding_aid = _parse(NO_FILE_LEVEL_EAD)
        frame = ead_views.unit_of_description_frame(finding_aid)
        self.assertNotIn("series", list(frame["level"]))
        self.assertEqual(2, len(frame))

    def test_a_resolved_series_with_a_resolved_child_yields_the_child_not_the_series(self):
        # the regression case directly: series "I" and file "1" both resolve; the file, being
        # more specific (deeper), must be the row -- an earlier version of this function
        # excluded by resolved *ancestor* and got this backwards
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>
  <c level="series"><did><unitid>I</unitid><unittitle>A series</unittitle></did>
    <c level="file"><did><unitid label="Inventarisnummer">1</unitid>
    <unittitle>A file</unittitle></did></c>
  </c>
</dsc></archdesc></ead>"""
        finding_aid = _parse(xml)
        frame = ead_views.unit_of_description_frame(finding_aid)
        self.assertEqual(1, len(frame))
        self.assertEqual("file", frame.iloc[0]["level"])
        self.assertEqual("A file", frame.iloc[0]["title"])


class TestFrameArchiveId(unittest.TestCase):

    def test_archive_id_override_prefixes_node_id(self):
        finding_aid = ead_tree.parse_ead_string(NO_FILE_LEVEL_EAD)
        default = ead_views.unit_of_description_frame(finding_aid)
        overridden = ead_views.unit_of_description_frame(finding_aid, archive_id="X")
        self.assertEqual(2, len(overridden))
        self.assertTrue(all(node_id.startswith("X:") for node_id in overridden["node_id"]))
        self.assertEqual([n.partition(":")[2] for n in default["node_id"]],
                         [n.partition(":")[2] for n in overridden["node_id"]])

    def test_default_archive_id_unchanged(self):
        finding_aid = ead_tree.parse_ead_string(NO_FILE_LEVEL_EAD)
        frame = ead_views.unit_of_description_frame(finding_aid)
        by_citation = {c.cite(finding_aid.path): c for c in finding_aid.walk()}
        components = [by_citation[citation] for citation in frame["citation"]]
        self.assertEqual([c.node_id(finding_aid.default_archive_id) for c in components],
                         list(frame["node_id"]))


class TestLegacyCompatibleFrame(unittest.TestCase):

    def test_column_order_matches_get_inventory_info_exactly(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.legacy_compatible_frame(finding_aid)
        self.assertEqual(
            ["series", "subseries_1", "subseries_2", "filegroup", "inventory_range", "file",
             "unitdate", "inventory_num", "mets_file"],
            list(frame.columns),
        )

    def test_row_content_for_a_simple_labelled_file(self):
        finding_aid = _parse(LABELLED_EAD)
        frame = ead_views.legacy_compatible_frame(finding_aid)
        self.assertEqual(1, len(frame))
        row = frame.iloc[0]
        self.assertEqual("A series", row["series"])
        self.assertIsNone(row["subseries_1"])
        self.assertEqual("A file", row["file"])
        self.assertEqual("1", row["inventory_num"])
        self.assertEqual("1700-1701", row["unitdate"])

    def test_unresolved_component_at_file_level_is_excluded(self):
        # matches legacy: only files with an inventory-number-shaped unitid are emitted at all
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>
  <c level="series"><did><unittitle>S</unittitle></did>
    <c level="file"><did><unittitle>No usable unitid</unittitle></did></c>
  </c>
</dsc></archdesc></ead>"""
        finding_aid = _parse(xml)
        frame = ead_views.legacy_compatible_frame(finding_aid)
        self.assertEqual(0, len(frame))


class TestAgreementWithLegacyParser(unittest.TestCase):
    """`legacy_compatible_frame` against the *real* legacy parser, on a real corpus file."""

    def setUp(self):
        self.ead_file = "data/metadata/eads/NL-HaNA_2.10.36.22.xml"
        if not Path(self.ead_file).exists():
            self.skipTest("corpus file not present in this checkout")

    def test_every_legacy_row_is_reproduced(self):
        import contextlib
        import io

        with contextlib.redirect_stdout(io.StringIO()):
            legacy = ead_parser.get_inventory_info(ead_file=self.ead_file)
        finding_aid = ead_tree.parse_ead(Path(self.ead_file))
        new = ead_views.legacy_compatible_frame(finding_aid)

        legacy_numbers = set(legacy["inventory_num"])
        new_numbers = set(new["inventory_num"])
        missing = legacy_numbers - new_numbers
        self.assertEqual(set(), missing,
                         f"legacy_compatible_frame must be a superset: missing {missing}")


if __name__ == "__main__":
    unittest.main()
