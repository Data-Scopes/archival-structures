"""Tests for `archival_structures.parsers.ead_harvest`.

Covers the corpus-description instrument (`describe_ead`, `ingest_directory`, the manifest) and,
in `TestEADProfileFixtures`, the two EAD 2002 spellings that Step 1 of the parser rewrite found
in the wild (see docs/ead_parser/reports/step-01-corpus.md):

- components as recursive `<c level="...">`, no default namespace (Nationaal Archief,
  Stadsarchief Amsterdam);
- components as depth-numbered `<c01>`..`<c12>` inside the EAD 2002 default namespace
  (opendata.archieven.nl / De Ree exports).

The fixtures are deliberately hand-written and tiny: they pin the shapes the Step 3 tree builder
has to accept without depending on any harvested file being present on disk.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_harvest

#: Recursive `<c>` components, no namespace -- the NL-HaNA / NL-AsdSAA spelling.
RECURSIVE_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external">
  <eadheader>
    <eadid mainagencycode="NL-TestA" countrycode="NL">1.23.45</eadid>
    <filedesc><titlestmt><titleproper>A recursive test finding aid</titleproper></titlestmt></filedesc>
  </eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <c level="series">
        <did><unitid>1</unitid><unittitle>A series</unittitle></did>
        <c level="subseries">
          <did><unitid>1.1</unitid><unittitle>A subseries</unittitle></did>
          <c otherlevel="filegrp" level="otherlevel">
            <did><unitid>1-2</unitid><unittitle>A file group</unittitle></did>
            <c level="file">
              <did><unitid>1</unitid><unittitle>A file</unittitle></did>
              <c level="item"><did><unittitle>An item</unittitle></did></c>
              <c otherlevel="subfile" level="otherlevel">
                <did><unittitle>A subfile</unittitle></did>
              </c>
            </c>
          </c>
        </c>
      </c>
    </dsc>
  </archdesc>
</ead>
"""

#: Depth-numbered `<c01>`..`<c04>` inside the EAD 2002 default namespace -- the De Ree spelling.
#: `read_ead` returns `None` for this profile today, because it matches the root tag against the
#: bare string `'ead'`.
NUMBERED_NAMESPACED_EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead audience="external" xmlns="urn:isbn:1-931666-22-9"
     xmlns:xlink="http://www.w3.org/1999/xlink">
  <eadheader>
    <eadid mainagencycode="NL-TestB" countrycode="NL">842-3</eadid>
    <filedesc><titlestmt><titleproper>A numbered test finding aid</titleproper></titlestmt></filedesc>
  </eadheader>
  <archdesc level="fonds">
    <dsc type="combined">
      <c01 level="series">
        <did id="MF1">
          <unitid type="guid">609C5B9D68E44642E0534701000A17FD</unitid>
          <unitid type="series_code">1.</unitid>
          <unittitle>A series</unittitle>
          <unitdate calendar="gregorian" era="ce" normal="1567/1989"/>
        </did>
        <c02 level="subseries">
          <did id="MF2"><unitid type="series_code">1.1.</unitid><unittitle>A subseries</unittitle></did>
          <c03 level="file">
            <did id="MF3"><unitid>1</unitid><unittitle>A file</unittitle></did>
            <c04 level="item"><did><unittitle>An item</unittitle></did></c04>
          </c03>
        </c02>
      </c01>
    </dsc>
  </archdesc>
</ead>
"""


class TestComponentRecognition(unittest.TestCase):

    def test_plain_c_is_a_component(self):
        self.assertTrue(ead_harvest.is_component("c"))
        self.assertIsNone(ead_harvest.component_depth_name("c"))

    def test_numbered_components_are_recognised(self):
        for number in ("01", "02", "09", "10", "12"):
            with self.subTest(number=number):
                self.assertTrue(ead_harvest.is_component(f"c{number}"))
                self.assertEqual(number, ead_harvest.component_depth_name(f"c{number}"))

    def test_namespaced_components_are_recognised(self):
        self.assertTrue(ead_harvest.is_component("{urn:isbn:1-931666-22-9}c03"))
        self.assertEqual("03", ead_harvest.component_depth_name("{urn:isbn:1-931666-22-9}c03"))

    def test_non_components_are_rejected(self):
        # c13 is past EAD 2002's c12 limit; the others are ordinary EAD elements that merely
        # start with a c, and must not be counted as components.
        for tag in ("c13", "c0", "c1", "controlaccess", "custodhist", "corpname", "did", ""):
            with self.subTest(tag=tag):
                self.assertFalse(ead_harvest.is_component(tag))


class TestEADProfileFixtures(unittest.TestCase):

    def test_recursive_profile_is_described(self):
        described = ead_harvest.describe_ead(RECURSIVE_EAD.encode())
        self.assertEqual("NL-TestA", described["agency_code"])
        self.assertEqual("1.23.45", described["eadid"])
        self.assertEqual("A recursive test finding aid", described["title"])
        self.assertIsNone(described["namespace"])
        self.assertEqual("recursive", described["component_style"])
        self.assertEqual(
            {"series": 1, "subseries": 1, "otherlevel:filegrp": 1, "file": 1,
             "item": 1, "otherlevel:subfile": 1},
            described["level_counts"],
        )

    def test_numbered_namespaced_profile_is_described(self):
        described = ead_harvest.describe_ead(NUMBERED_NAMESPACED_EAD.encode())
        self.assertEqual("NL-TestB", described["agency_code"])
        self.assertEqual("842-3", described["eadid"])
        self.assertEqual("urn:isbn:1-931666-22-9", described["namespace"])
        self.assertEqual("numbered", described["component_style"])
        self.assertEqual({"series": 1, "subseries": 1, "file": 1, "item": 1},
                         described["level_counts"])

    def test_both_profiles_describe_the_same_hierarchy_depth(self):
        # the point of the fixtures: the two spellings are equivalent EAD, so anything that
        # reports structure must report the same levels for both
        recursive = ead_harvest.describe_ead(RECURSIVE_EAD.encode())["level_counts"]
        numbered = ead_harvest.describe_ead(NUMBERED_NAMESPACED_EAD.encode())["level_counts"]
        shared = {"series", "subseries", "file", "item"}
        self.assertEqual({k: v for k, v in recursive.items() if k in shared},
                         {k: v for k, v in numbered.items() if k in shared})


class TestIngestDirectory(unittest.TestCase):

    def test_files_are_renamed_from_their_own_eadid(self):
        with TemporaryDirectory() as tmp:
            source, corpus = Path(tmp) / "downloads", Path(tmp) / "corpus"
            source.mkdir()
            corpus.mkdir()
            # a browser-assigned name, as the opendata portal hands them out
            (source / "609C5B9D536B4642E0534701000A17FD_oai_ead.xml").write_text(
                NUMBERED_NAMESPACED_EAD, encoding="utf-8")

            ingested = ead_harvest.ingest_directory(source, corpus)

            self.assertEqual(1, len(ingested))
            self.assertEqual("NL-TestB_842-3.ead.xml", ingested[0].path.name)
            self.assertTrue((corpus / "NL-TestB_842-3.ead.xml").exists())
            self.assertEqual("urn:isbn:1-931666-22-9", ingested[0].namespace)
            self.assertEqual(4, ingested[0].n_components)

    def test_non_ead_xml_is_skipped_not_filed(self):
        with TemporaryDirectory() as tmp:
            source, corpus = Path(tmp) / "downloads", Path(tmp) / "corpus"
            source.mkdir()
            corpus.mkdir()
            (source / "not-ead.xml").write_text("<mets><fileSec/></mets>", encoding="utf-8")
            (source / "broken.xml").write_text("<ead><unclosed>", encoding="utf-8")

            self.assertEqual([], ead_harvest.ingest_directory(source, corpus))
            self.assertEqual([], list(corpus.iterdir()))

    def test_already_ingested_file_is_skipped_unless_overwriting(self):
        with TemporaryDirectory() as tmp:
            source, corpus = Path(tmp) / "downloads", Path(tmp) / "corpus"
            source.mkdir()
            corpus.mkdir()
            (source / "a.xml").write_text(RECURSIVE_EAD, encoding="utf-8")

            self.assertEqual(1, len(ead_harvest.ingest_directory(source, corpus)))
            self.assertEqual([], ead_harvest.ingest_directory(source, corpus))
            self.assertEqual(1, len(ead_harvest.ingest_directory(source, corpus, overwrite=True)))


class TestManifest(unittest.TestCase):

    def test_manifest_records_profile_and_level_counts(self):
        with TemporaryDirectory() as tmp:
            source, corpus = Path(tmp) / "downloads", Path(tmp) / "corpus"
            source.mkdir()
            corpus.mkdir()
            (source / "a.xml").write_text(RECURSIVE_EAD, encoding="utf-8")
            (source / "b.xml").write_text(NUMBERED_NAMESPACED_EAD, encoding="utf-8")
            ingested = ead_harvest.ingest_directory(source, corpus)

            manifest = corpus / "corpus.tsv"
            ead_harvest.write_manifest(ingested, manifest)
            lines = manifest.read_text(encoding="utf-8").strip().split("\n")

            self.assertEqual(ead_harvest.MANIFEST_COLUMNS, lines[0].split("\t"))
            self.assertEqual(3, len(lines))
            rows = {line.split("\t")[0]: dict(zip(ead_harvest.MANIFEST_COLUMNS, line.split("\t")))
                    for line in lines[1:]}
            self.assertEqual("recursive", rows["NL-TestA_1.23.45.ead.xml"]["component_style"])
            self.assertEqual("", rows["NL-TestA_1.23.45.ead.xml"]["namespace"])
            self.assertEqual("numbered", rows["NL-TestB_842-3.ead.xml"]["component_style"])
            self.assertEqual("urn:isbn:1-931666-22-9",
                             rows["NL-TestB_842-3.ead.xml"]["namespace"])
            self.assertEqual("1", rows["NL-TestA_1.23.45.ead.xml"]["subfile"])

    def test_append_does_not_duplicate_existing_rows(self):
        with TemporaryDirectory() as tmp:
            source, corpus = Path(tmp) / "downloads", Path(tmp) / "corpus"
            source.mkdir()
            corpus.mkdir()
            (source / "a.xml").write_text(RECURSIVE_EAD, encoding="utf-8")
            ingested = ead_harvest.ingest_directory(source, corpus)
            manifest = corpus / "corpus.tsv"

            ead_harvest.write_manifest(ingested, manifest)
            ead_harvest.write_manifest(ingested, manifest, append=True)

            lines = manifest.read_text(encoding="utf-8").strip().split("\n")
            self.assertEqual(2, len(lines), "appending the same finding aid must not duplicate it")


class TestOpendataURLs(unittest.TestCase):

    def test_listing_url_carries_the_ead_filter_and_organisation(self):
        url = ead_harvest.opendata_listing_url(adtid=39)
        self.assertIn("filter%5Bconditions%5D%5Btype%5D=oai_ead", url)
        self.assertIn("filter%5Bconditions%5D%5Badtid%5D=39", url)

    def test_download_url_shape(self):
        self.assertEqual(
            "https://opendata.archieven.nl/download/39/EAD/609C5B9D536B4642E0534701000A17FD",
            ead_harvest.opendata_download_url(39, "609C5B9D536B4642E0534701000A17FD"),
        )

    def test_download_guide_names_each_organisation_and_its_listing(self):
        guide = ead_harvest.download_guide([39, 236])
        self.assertIn("Het Utrechts Archief", guide)
        self.assertIn("Noord-Hollands Archief", guide)
        self.assertIn("adtid%5D=39", guide)
        self.assertIn("403", guide, "the guide should say why a script cannot do this")


if __name__ == "__main__":
    unittest.main()
