"""Tests for `archival_structures.parsers.ead_survey`.

The survey is the instrument Step 2 of the parser rewrite measures every later step with, so it
is tested against small hand-written EAD fragments whose structure is known by construction --
not against harvested finding aids, which are not in the repository and change as the corpus
grows.

The fragments cover the four EAD 2002 spellings Step 1 found in the wild (recursive vs
depth-numbered components, namespace declared vs omitted) and the three digitisation-linking
mechanisms (`<dao>`, Memorix `<assets>`, `<unitid type="handle">`).
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_survey

#: Recursive components, no namespace, a `<dao>` digitisation link, and a file with two
#: sub-components -- the Nationaal Archief shape.
RECURSIVE_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external">
  <eadheader><eadid mainagencycode="NL-HaNA" countrycode="NL">1.23.45</eadid></eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <c level="series">
        <did><unitid>I</unitid><unittitle>A series</unittitle></did>
        <c otherlevel="filegrp" level="otherlevel">
          <did><unitid>1-2</unitid><unittitle>A file group</unittitle></did>
          <c level="file">
            <did>
              <unitid identifier="39104160">1</unitid>
              <unitid audience="internal" type="handle">http://hdl.handle.net/10648/abc</unitid>
              <unittitle>The first file</unittitle>
              <dao role="METS" href="https://service.archief.nl/gaf/api/mets/v1/xyz"/>
            </did>
            <c level="item"><did><unittitle>An item</unittitle></did></c>
            <c otherlevel="subfile" level="otherlevel">
              <did><unittitle>A subfile</unittitle></did>
            </c>
          </c>
          <c level="file">
            <did><unitid identifier="39104161">2</unitid><unittitle>The second file</unittitle></did>
          </c>
        </c>
      </c>
      <c level="file">
        <did><unitid identifier="39104162">3</unitid><unittitle>A file straight under dsc</unittitle></did>
      </c>
    </dsc>
  </archdesc>
</ead>
"""

#: Depth-numbered components inside the EAD 2002 default namespace, inventory number marked by
#: nothing at all, digitisation linked by a handle `<unitid>` -- the Het Utrechts Archief shape.
NUMBERED_NAMESPACED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external" xmlns="urn:isbn:1-931666-22-9">
  <eadheader><eadid mainagencycode="NL-UtHUA" countrycode="NL">842-3</eadid></eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <c01 level="series">
        <did>
          <unitid type="guid">609C5B9D68E44642E0534701000A17FD</unitid>
          <unitid type="series_code">1.</unitid>
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

#: Memorix inline assets in their own namespace -- the Stadsarchief Amsterdam shape.
ASSETS_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead xmlns:nexus="urn:memorix.io" audience="external" source="nexus">
  <eadheader><eadid identifier="abc">30312</eadid></eadheader>
  <archdesc>
    <dsc>
      <c level="series">
        <did><unitid label="Rubriekscode" identifier="r1">1</unitid><unittitle>A series</unittitle></did>
        <c level="file">
          <did>
            <unitid identifier="u1" label="Inventarisnummer">1</unitid>
            <unittitle/>
            <nexus:assets allowAssets="true">
              <nexus:asset asset_order="0">
                <nexus:name>A22796000001.jp2</nexus:name>
                <nexus:iiif>https://example.invalid/iiif/3/x/info.json</nexus:iiif>
              </nexus:asset>
              <nexus:asset asset_order="1">
                <nexus:name>A22796000002.jp2</nexus:name>
                <nexus:iiif>https://example.invalid/iiif/3/y/info.json</nexus:iiif>
              </nexus:asset>
            </nexus:assets>
          </did>
        </c>
      </c>
    </dsc>
  </archdesc>
</ead>
"""


def _write(directory: Path, name: str, content: str) -> Path:
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return path


class SurveyTestCase(unittest.TestCase):
    """Writes the fragments to a temporary directory, since `survey_ead` takes a path."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def survey(self, name: str, content: str) -> ead_survey.EADSurvey:
        return ead_survey.survey_ead(_write(self.dir, name, content))


class TestSurveyRecursiveProfile(SurveyTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.survey("NL-HaNA_1.23.45.ead.xml", RECURSIVE_EAD)

    def test_identifies_the_document(self):
        self.assertEqual("NL-HaNA", self.result.agency_code)
        self.assertEqual("1.23.45", self.result.eadid)
        self.assertEqual("Nationaal Archief", self.result.repository)
        self.assertIsNone(self.result.namespace)
        self.assertEqual("recursive", self.result.component_style)

    def test_counts_every_level_including_below_file(self):
        self.assertEqual(
            {"series": 1, "otherlevel:filegrp": 1, "file": 3, "item": 1, "otherlevel:subfile": 1},
            dict(self.result.levels),
        )
        self.assertEqual(7, self.result.n_components)

    def test_records_components_directly_under_dsc(self):
        # the construct that made the parser return zero rows for NL-HaNA_1.05.06
        self.assertEqual({"series": 1, "file": 1}, dict(self.result.dsc_child_levels))

    def test_reports_ancestry_paths_to_leaves(self):
        self.assertEqual(
            {("series", "otherlevel:filegrp", "file", "item"): 1,
             ("series", "otherlevel:filegrp", "file", "otherlevel:subfile"): 1,
             ("series", "otherlevel:filegrp", "file"): 1,
             ("file",): 1},
            dict(self.result.leaf_paths()),
        )

    def test_max_depth_counts_nesting_not_levels(self):
        self.assertEqual(4, self.result.max_depth)

    def test_records_unitid_shapes(self):
        # the three <unitid identifier="..."> on the file components
        self.assertEqual(3, self.result.unitid_shapes["@identifier"])
        self.assertEqual(1, self.result.unitid_shapes["type=handle, audience=internal"])
        self.assertEqual(2, self.result.unitid_shapes["(bare)"])

    def test_counts_dao_once_not_twice(self):
        # <dao> sits inside <did>, which is walked both as a non-component child and for its
        # own children -- it must not be counted in both passes
        self.assertEqual(1, self.result.digital_links["<dao>"])

    def test_components_carry_source_lines(self):
        first_file = self.result.components_at("file")[0]
        self.assertIsNotNone(first_file.sourceline)
        self.assertIn("NL-HaNA_1.23.45.ead.xml:", first_file.cite(self.result.path))


class TestSurveyNumberedNamespacedProfile(SurveyTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.survey("NL-UtHUA_842-3.ead.xml", NUMBERED_NAMESPACED_EAD)

    def test_detects_namespace_and_numbered_components(self):
        self.assertEqual("urn:isbn:1-931666-22-9", self.result.namespace)
        self.assertEqual("numbered", self.result.component_style)

    def test_numbered_components_are_counted_by_their_level_attribute(self):
        self.assertEqual({"series": 1, "subseries": 1, "file": 1}, dict(self.result.levels))
        self.assertEqual(("series", "subseries", "file"),
                         self.result.components_at("file")[0].level_path)

    def test_display_unitid_prefers_the_inventory_number_over_a_guid(self):
        # the guid is listed first, so primary_unitid alone would print 32 hex characters
        component = self.result.components_at("file")[0]
        self.assertEqual("609C5B9D68E64642E0534701000A17FD", component.primary_unitid)
        self.assertEqual("1", component.display_unitid)

    def test_handle_unitid_is_recognised_as_a_digitisation_link(self):
        self.assertEqual(1, self.result.digital_links['<unitid type="handle">'])
        self.assertNotIn("<dao>", self.result.digital_links)


class TestSurveyAssetsProfile(SurveyTestCase):

    def test_memorix_assets_are_counted_in_their_own_namespace(self):
        result = self.survey("NL-AsdSAA_30312.ead.xml", ASSETS_EAD)
        self.assertEqual(1, result.digital_links["<assets>"])
        self.assertEqual(2, result.digital_links["<asset>"])
        self.assertEqual(1, result.unitid_shapes["label=Inventarisnummer, @identifier"])

    def test_agency_code_falls_back_to_the_filename(self):
        # Amsterdam's <eadid> carries no mainagencycode
        result = self.survey("NL-AsdSAA_30312.ead.xml", ASSETS_EAD)
        self.assertEqual("NL-AsdSAA", result.agency_code)
        self.assertEqual("Stadsarchief Amsterdam", result.repository)


#: The same namespaced, numbered EAD wrapped in the `<opendata>` envelope that
#: opendata.archieven.nl serves 16 of the corpus's 40 finding aids in. The envelope is *not* in
#: the EAD namespace while the `<ead>` inside it is, so reading the document root's tag reports
#: the wrong namespace and looking for a bare `<ead>` root misses it entirely.
ENVELOPED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<opendata>
  <header><type>EAD</type><dataProvider>Gelders Archief</dataProvider></header>
  <data>
""" + NUMBERED_NAMESPACED_EAD.split("?>", 1)[1] + """
  </data>
</opendata>
"""


class TestSurveyEnvelopedProfile(SurveyTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.survey("NL-AhGldA_2388.ead.xml", ENVELOPED_EAD)

    def test_the_envelope_is_reported(self):
        self.assertEqual("opendata", self.result.wrapper)

    def test_namespace_comes_from_the_ead_not_the_envelope(self):
        # the <opendata> root is unnamespaced; reading it would report no namespace at all
        self.assertEqual("urn:isbn:1-931666-22-9", self.result.namespace)

    def test_components_are_found_inside_the_envelope(self):
        self.assertEqual({"series": 1, "subseries": 1, "file": 1}, dict(self.result.levels))
        self.assertEqual("numbered", self.result.component_style)

    def test_the_eadid_inside_the_envelope_identifies_the_document(self):
        self.assertEqual("NL-UtHUA", self.result.agency_code)
        self.assertEqual("842-3", self.result.eadid)


class TestSurveyRobustness(SurveyTestCase):

    def test_malformed_xml_is_recorded_not_raised(self):
        result = self.survey("broken.xml", "<ead><dsc><c level='file'>")
        self.assertIsNotNone(result.parse_error)
        self.assertEqual(0, result.n_components)

    def test_ead_without_dsc_is_recorded_not_raised(self):
        result = self.survey("nodsc.xml", "<ead><eadheader><eadid>x</eadid></eadheader></ead>")
        self.assertEqual("no <dsc> element", result.parse_error)


class TestCoverage(SurveyTestCase):

    def setUp(self):
        super().setUp()
        self.result = self.survey("NL-HaNA_1.23.45.ead.xml", RECURSIVE_EAD)

    def test_a_parser_reaching_everything_scores_one(self):
        def perfect(path):
            return [(c.level, c.display_unitid) for c in self.result.components]

        report = ead_survey.coverage(self.result, perfect)
        self.assertEqual(1.0, report.recall)
        self.assertTrue(all(c.recall == 1.0 for c in report.by_level.values()))

    def test_a_file_only_parser_scores_zero_below_file_level(self):
        # this is the shape of the current parser: files only, nothing else
        def files_only(path):
            return [("file", c.display_unitid) for c in self.result.components_at("file")]

        report = ead_survey.coverage(self.result, files_only)
        self.assertEqual(1.0, report.by_level["file"].recall)
        self.assertEqual(0.0, report.by_level["item"].recall)
        self.assertEqual(0.0, report.by_level["otherlevel:subfile"].recall)
        self.assertLess(report.recall, 1.0)

    def test_any_unitid_of_a_component_counts_as_a_match(self):
        # a parser may report the handle or the identifier rather than the inventory number
        def by_handle(path):
            return [("file", "http://hdl.handle.net/10648/abc")]

        report = ead_survey.coverage(self.result, by_handle)
        self.assertEqual(1, report.by_level["file"].n_matched)

    def test_missing_components_are_reported_with_citations(self):
        report = ead_survey.coverage(self.result, lambda path: [])
        missing = report.by_level["file"].missing_examples
        self.assertEqual(3, len(missing))
        self.assertEqual(["1", "2", "3"], [c.display_unitid for c in missing])
        self.assertTrue(all(":" in c.cite(self.result.path) for c in missing))

    def test_a_parser_that_raises_is_measured_as_zero_not_propagated(self):
        def broken(path):
            raise TypeError("'NoneType' object is not iterable")

        report = ead_survey.coverage(self.result, broken)
        self.assertIn("TypeError", report.error)
        self.assertEqual(0.0, report.recall)

    def test_duplicate_output_rows_do_not_inflate_coverage(self):
        def duplicating(path):
            return [("file", "1")] * 5

        report = ead_survey.coverage(self.result, duplicating)
        self.assertEqual(1, report.by_level["file"].n_matched)
        self.assertEqual(5, report.by_level["file"].n_parsed)


class TestCorpusSurvey(SurveyTestCase):

    def setUp(self):
        super().setUp()
        self.corpus = ead_survey.CorpusSurvey([
            self.survey("NL-HaNA_1.23.45.ead.xml", RECURSIVE_EAD),
            self.survey("NL-UtHUA_842-3.ead.xml", NUMBERED_NAMESPACED_EAD),
            self.survey("NL-AsdSAA_30312.ead.xml", ASSETS_EAD),
        ])

    def test_reports_every_repository(self):
        self.assertEqual(
            ["Het Utrechts Archief", "Nationaal Archief", "Stadsarchief Amsterdam"],
            self.corpus.repositories,
        )

    def test_profiles_distinguish_namespace_spelling_and_envelope(self):
        self.assertEqual(
            {("no namespace", "recursive", "bare <ead>"): 2,
             ("namespaced", "numbered", "bare <ead>"): 1},
            dict(self.corpus.profiles()),
        )

    def test_totals_sum_across_documents(self):
        self.assertEqual(5, self.corpus.totals("levels")["file"])

    def test_unique_to_one_repository_names_the_over_fitting_risks(self):
        unique = self.corpus.unique_to_one_repository("levels")
        self.assertEqual("Nationaal Archief", unique["item"])
        self.assertEqual("Nationaal Archief", unique["otherlevel:subfile"])
        # a level shared by two repositories is not an over-fitting risk
        self.assertNotIn("series", unique)

    def test_shared_levels_report_their_repositories(self):
        self.assertEqual(
            ["Het Utrechts Archief", "Nationaal Archief", "Stadsarchief Amsterdam"],
            self.corpus.repositories_by("levels")["file"],
        )


class TestFormatting(SurveyTestCase):

    def test_survey_summary_mentions_profile_and_paths(self):
        text = ead_survey.format_survey(self.survey("a.xml", NUMBERED_NAMESPACED_EAD))
        self.assertIn("numbered", text)
        self.assertIn("urn:isbn:1-931666-22-9", text)
        self.assertIn("series > subseries > file", text)

    def test_coverage_table_shows_recall_per_level(self):
        result = self.survey("NL-HaNA_1.23.45.ead.xml", RECURSIVE_EAD)
        text = ead_survey.format_coverage(ead_survey.coverage(result, lambda p: []))
        self.assertIn("item", text)
        self.assertIn("0.0%", text)
        self.assertIn("TOTAL", text)


if __name__ == "__main__":
    unittest.main()
