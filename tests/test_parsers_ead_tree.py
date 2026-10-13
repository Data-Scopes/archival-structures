"""Tests for `archival_structures.parsers.ead_tree`.

Covers the tree builder against hand-written fixtures for every construct that Step 0-2 found
the legacy parser dropping or crashing on: components directly under `<dsc>`, components with no
`@level`, free-text `otherlevel`, components nested below `file`, the numbered/namespaced
profile, and the `<opendata>` envelope. Then a coverage round-trip proving `tree_parser_components`
reaches 100% of a fixture's components -- the property Step 3 claims corpus-wide.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_survey, ead_tree

#: Recursive components, no namespace: a file directly under <dsc> (the NL-AsdSAA_395 /
#: NL-HaNA_1.05.06 construct Step 0 found return zero rows), a level-less component, an
#: otherlevel with free-text values, and item/subfile nested two deep under a file.
RECURSIVE_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external">
  <eadheader><eadid mainagencycode="NL-Test" countrycode="NL">1.23.45</eadid>
    <filedesc><titlestmt><titleproper>A test finding aid</titleproper></titlestmt></filedesc>
  </eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <head>Inventory</head>
      <c level="series">
        <did><unitid>I</unitid><unittitle>A series</unittitle></did>
        <c otherlevel="Hinderwetvergunning" level="otherlevel">
          <did><unitid identifier="a1">886</unitid><unittitle>Zwolle, Aalvangersweg 7</unittitle></did>
          <c level="file">
            <did>
              <unitid identifier="39104160">1</unitid>
              <unitid audience="internal" type="handle">http://hdl.handle.net/10648/abc</unitid>
              <unittitle>The first file</unittitle>
              <unitdate normal="1600/1601">1600-1601</unitdate>
              <dao role="METS" href="https://service.archief.nl/gaf/api/mets/v1/xyz"/>
            </did>
            <odd><p>A note about this file.</p></odd>
            <c level="item"><did><unittitle>An item</unittitle></did></c>
            <c otherlevel="subfile" level="otherlevel">
              <did><unittitle>A subfile</unittitle></did>
            </c>
          </c>
        </c>
        <c><did><unitid>x1</unitid><unittitle>A level-less stray</unittitle></did></c>
      </c>
      <c level="file">
        <did><unitid identifier="39104162">2</unitid><unittitle>A file straight under dsc</unittitle></did>
      </c>
    </dsc>
  </archdesc>
</ead>
"""

#: Depth-numbered components inside the EAD 2002 default namespace -- Het Utrechts Archief shape.
NUMBERED_NAMESPACED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external" xmlns="urn:isbn:1-931666-22-9">
  <eadheader><eadid mainagencycode="NL-UtHUA" countrycode="NL">842-3</eadid></eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <c01 level="series">
        <did>
          <unitid type="guid">609C5B9D68E44642E0534701000A17FD</unitid>
          <unittitle>A series</unittitle>
        </did>
        <c02 level="subseries">
          <did><unitid type="series_code">1.1.</unitid><unittitle>A subseries</unittitle></did>
          <c03 level="file">
            <did>
              <unitid type="guid">609C5B9D68E64642E0534701000A17FD</unitid>
              <unitid>1</unitid>
              <unitid audience="internal" type="handle">https://hetutrechtsarchief.nl/collectie/x</unitid>
              <unittitle>A file</unittitle>
            </did>
          </c03>
        </c02>
      </c01>
    </dsc>
  </archdesc>
</ead>
"""

#: The opendata.archieven.nl envelope: unnamespaced <opendata>, namespaced <ead> inside.
ENVELOPED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<opendata>
  <header><type>EAD</type><dataProvider>Gelders Archief</dataProvider></header>
  <data>
""" + NUMBERED_NAMESPACED_EAD.split("?>", 1)[1] + """
  </data>
</opendata>
"""

#: No <did> at all on one component -- a malformed-but-real-world edge case.
MISSING_DID_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid>1</eadid></eadheader>
  <archdesc><dsc>
    <c level="series"><did><unittitle>A series</unittitle></did>
      <c level="file"></c>
    </c>
  </dsc></archdesc>
</ead>
"""


def _write(directory: Path, name: str, content: str) -> Path:
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return path


class TreeTestCase(unittest.TestCase):

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def parse(self, name: str, content: str) -> ead_tree.EADFindingAid:
        return ead_tree.parse_ead(_write(self.dir, name, content))


class TestRecursiveProfile(TreeTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.parse("NL-Test_1.23.45.ead.xml", RECURSIVE_EAD)

    def test_identifies_the_document(self):
        self.assertEqual("NL-Test", self.result.agency_code)
        self.assertEqual("1.23.45", self.result.eadid)
        self.assertEqual("A test finding aid", self.result.title)
        self.assertIsNone(self.result.namespace)
        self.assertEqual("recursive", self.result.component_style)
        self.assertIsNone(self.result.parse_error)

    def test_both_dsc_level_components_are_roots(self):
        # the construct that made the legacy parser return zero rows for NL-HaNA_1.05.06 /
        # NL-AsdSAA_395: components directly under <dsc>, not nested inside a <series>
        self.assertEqual(2, len(self.result.roots))
        self.assertEqual("series", self.result.roots[0].level)
        self.assertEqual("file", self.result.roots[1].level)

    def test_dsc_level_head_is_retained_not_dropped(self):
        self.assertIn("head", self.result.dsc_extra)

    def test_every_component_is_reachable_by_walk(self):
        # series, otherlevel:Hinderwetvergunning, file, item, otherlevel:subfile,
        # (none)-level stray, file = 7
        self.assertEqual(7, self.result.n_components)

    def test_otherlevel_free_text_is_kept_verbatim(self):
        levels = [c.level for c in self.result.walk()]
        self.assertIn("otherlevel:Hinderwetvergunning", levels)
        self.assertIn("otherlevel:subfile", levels)

    def test_a_level_less_component_is_not_dropped_or_fatal(self):
        # @level is optional in EAD 2002; Step 2 found 2,593 of these in the corpus
        stray = next(c for c in self.result.walk() if c.title == "A level-less stray")
        self.assertEqual("(none)", stray.level)
        self.assertIsNone(stray.raw_level)

    def test_components_below_file_level_are_reached(self):
        # the legacy parser never recurses into a file's children at all
        file_component = next(c for c in self.result.walk() if c.level == "file"
                              and c.unitid_values() == ["1", "http://hdl.handle.net/10648/abc"])
        child_levels = {c.level for c in file_component.children}
        self.assertEqual({"item", "otherlevel:subfile"}, child_levels)

    def test_unmodelled_did_children_are_retained_not_discarded(self):
        file_component = next(c for c in self.result.walk() if c.level == "file"
                              and c.title == "The first file")
        self.assertIn("unitdate", file_component.did_extra)
        self.assertIn("dao", file_component.did_extra)
        self.assertEqual(1, len(file_component.did_extra["dao"]))

    def test_unmodelled_non_did_children_are_retained(self):
        file_component = next(c for c in self.result.walk() if c.level == "file"
                              and c.title == "The first file")
        self.assertIn("odd", file_component.other_children)

    def test_multiple_unitids_are_all_kept(self):
        file_component = next(c for c in self.result.walk() if c.level == "file"
                              and c.title == "The first file")
        self.assertEqual(["1", "http://hdl.handle.net/10648/abc"],
                         file_component.unitid_values())
        self.assertEqual("handle", file_component.unitids[1].type)

    def test_parent_and_ancestors_are_wired_up(self):
        item = next(c for c in self.result.walk() if c.level == "item")
        self.assertEqual(["series", "otherlevel:Hinderwetvergunning", "file"],
                         [a.level for a in item.ancestors()])
        self.assertIs(item.parent.children[0] if item.parent.children[0] is item else item,
                      item)

    def test_index_path_and_level_path_track_position(self):
        item = next(c for c in self.result.walk() if c.level == "item")
        # series (root 0) > otherlevel:Hinderwetvergunning (child 0) > file (child 0) > item (child 0)
        self.assertEqual((0, 0, 0, 0), item.index_path)
        self.assertEqual(("series", "otherlevel:Hinderwetvergunning", "file", "item"),
                         item.level_path)

    def test_components_carry_source_lines(self):
        first = self.result.roots[0]
        self.assertIsNotNone(first.sourceline)
        self.assertIn("NL-Test_1.23.45.ead.xml:", first.cite(self.result.path))


class TestNumberedNamespacedProfile(TreeTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.parse("NL-UtHUA_842-3.ead.xml", NUMBERED_NAMESPACED_EAD)

    def test_namespace_and_style_detected(self):
        self.assertEqual("urn:isbn:1-931666-22-9", self.result.namespace)
        self.assertEqual("numbered", self.result.component_style)
        self.assertIsNone(self.result.parse_error)

    def test_this_is_the_profile_the_legacy_parser_cannot_read_at_all(self):
        # read_ead returns None for this profile (Step 1, finding E1); the tree parses it fully
        self.assertEqual(1, len(self.result.roots))
        self.assertEqual(3, self.result.n_components)
        file_component = next(c for c in self.result.walk() if c.level == "file")
        self.assertEqual("A file", file_component.title)


#: The exact construct Step 2's error analysis E4 cites (NL-AmfAE_0001.01.ead.xml:35655): a
#: depth-numbered component with no @level, a GUID unitid and an empty <unittitle/>.
NUMBERED_LEVELLESS_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead xmlns="urn:isbn:1-931666-22-9">
  <eadheader><eadid mainagencycode="NL-AmfAE" countrycode="NL">0001.01</eadid></eadheader>
  <archdesc><dsc>
    <c01 level="series">
      <did><unitid>1</unitid><unittitle>A series</unittitle></did>
      <c02 level="subseries">
        <did><unitid>1.1</unitid><unittitle>A subseries</unittitle></did>
        <c03>
          <did><unitid type="guid">43EAE8404E6F4BE2B9B93A90A37EE021</unitid><unittitle/></did>
        </c03>
      </c02>
    </c01>
  </dsc></archdesc>
</ead>
"""


class TestNumberedLevelless(TreeTestCase):

    def test_numbered_style_levelless_component_from_step2_e4(self):
        result = self.parse("NL-AmfAE_0001.01.ead.xml", NUMBERED_LEVELLESS_EAD)
        self.assertEqual("numbered", result.component_style)
        stray = next(c for c in result.walk() if c.tag == "c03")
        self.assertEqual("(none)", stray.level)
        self.assertEqual(["43EAE8404E6F4BE2B9B93A90A37EE021"], stray.unitid_values())
        # an empty <unittitle/> is text=None, not a crash and not "(none)" mistaken for a title
        self.assertIsNone(stray.title)


class TestEnvelopedProfile(TreeTestCase):

    def test_ead_is_found_inside_the_opendata_envelope(self):
        result = self.parse("NL-AhGldA_2388.ead.xml", ENVELOPED_EAD)
        self.assertEqual("opendata", result.wrapper)
        self.assertEqual("urn:isbn:1-931666-22-9", result.namespace)
        self.assertEqual("NL-UtHUA", result.agency_code)
        self.assertEqual(3, result.n_components)


class TestMissingDid(TreeTestCase):

    def test_missing_did_is_flagged_not_fatal(self):
        result = self.parse("missing-did.xml", MISSING_DID_EAD)
        file_component = next(c for c in result.walk() if c.level == "file")
        self.assertTrue(file_component.missing_did)
        self.assertEqual([], file_component.unitid_values())
        self.assertIsNone(file_component.title)


class TestRobustness(TreeTestCase):

    def test_malformed_xml_is_recorded_not_raised(self):
        result = self.parse("broken.xml", "<ead><archdesc><dsc><c level='file'>")
        self.assertIsNotNone(result.parse_error)
        self.assertEqual(0, result.n_components)

    def test_missing_dsc_is_recorded_not_raised(self):
        result = self.parse("nodsc.xml", "<ead><eadheader><eadid>x</eadid></eadheader></ead>")
        self.assertEqual("no <dsc> element", result.parse_error)

    def test_non_ead_document_is_recorded_not_raised(self):
        result = self.parse("notead.xml", "<mets><fileSec/></mets>")
        self.assertEqual("no <ead> element found", result.parse_error)


class TestCoverageRoundTrip(TreeTestCase):
    """The acceptance criterion Step 3 makes for itself: 100% coverage via ead_survey."""

    def _assert_full_coverage(self, name: str, content: str):
        path = _write(self.dir, name, content)
        survey = ead_survey.survey_ead(path)
        report = ead_survey.coverage(survey, ead_tree.tree_parser_components)
        self.assertIsNone(report.error)
        self.assertEqual(1.0, report.recall, msg=str(ead_survey.format_coverage(report)))
        for level, level_coverage in report.by_level.items():
            self.assertEqual(1.0, level_coverage.recall, msg=f"level {level!r} incomplete")

    def test_full_coverage_recursive_profile(self):
        self._assert_full_coverage("NL-Test_1.23.45.ead.xml", RECURSIVE_EAD)

    def test_full_coverage_numbered_namespaced_profile(self):
        self._assert_full_coverage("NL-UtHUA_842-3.ead.xml", NUMBERED_NAMESPACED_EAD)

    def test_full_coverage_enveloped_profile(self):
        self._assert_full_coverage("NL-AhGldA_2388.ead.xml", ENVELOPED_EAD)


class TestExtraCensus(TreeTestCase):

    def test_reports_unmodelled_did_and_other_children(self):
        result = self.parse("NL-Test_1.23.45.ead.xml", RECURSIVE_EAD)
        did_extra, other = ead_tree.extra_census(result)
        self.assertEqual(1, did_extra["unitdate"])
        self.assertEqual(1, did_extra["dao"])
        self.assertEqual(1, other["odd"])


if __name__ == "__main__":
    unittest.main()
