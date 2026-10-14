"""Tests for `archival_structures.parsers.ead_digital`.

Fixtures for all four digitisation-linking mechanisms Step 2/5 found in the corpus: `<dao>`
(Nationaal Archief), `<daogrp>` (opendata.archieven.nl-family, structured as an XLink extended
link group of per-scan thumb/reference pairs -- not a single href), Memorix `<nexus:assets>`
(Stadsarchief Amsterdam, truncated at 10), and `<unitid type="handle">` (the majority mechanism,
used where none of the other three is present).
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_digital, ead_tree


def _component(xml: str) -> ead_tree.EADComponent:
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "fixture.xml"
        path.write_text(
            f'<ead><eadheader><eadid>1</eadid></eadheader>'
            f'<archdesc><dsc>{xml}</dsc></archdesc></ead>',
            encoding="utf-8",
        )
        finding_aid = ead_tree.parse_ead(path)
    assert finding_aid.parse_error is None, finding_aid.parse_error
    return finding_aid.roots[0]


class TestDao(unittest.TestCase):

    def test_dao_is_parsed_from_inside_did(self):
        component = _component(
            '<c level="file"><did><unittitle/>'
            '<dao role="METS" linktype="simple" '
            'href="https://service.archief.nl/gaf/api/mets/v1/xyz"/>'
            '</did></c>'
        )
        results = ead_digital.parse_dao(component)
        self.assertEqual(1, len(results))
        self.assertEqual("https://service.archief.nl/gaf/api/mets/v1/xyz", results[0].href)
        self.assertEqual("METS", results[0].role)

    def test_no_dao_returns_empty_list(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        self.assertEqual([], ead_digital.parse_dao(component))


class TestDaoGroup(unittest.TestCase):

    def test_daogrp_scans_are_paired_thumb_then_reference(self):
        component = _component(
            '<c level="file" xmlns:xlink="http://www.w3.org/1999/xlink"><did><unittitle/></did>'
            '<daogrp xlink:type="extended">'
            '<daodesc><p>scan 1-0001</p></daodesc>'
            '<resource xlink:type="resource" xlink:label="start"/>'
            '<daoloc xlink:href="https://proxy.example/thumb/A" xlink:type="locator" '
            'xlink:label="thumb" xlink:role="thumbnail"/>'
            '<daoloc xlink:href="https://proxy.example/embed/A" xlink:type="locator" '
            'xlink:label="reference"/>'
            '<resource xlink:type="resource" xlink:label="start"/>'
            '<daoloc xlink:href="https://proxy.example/thumb/B" xlink:type="locator" '
            'xlink:label="thumb" xlink:role="thumbnail"/>'
            '<daoloc xlink:href="https://proxy.example/embed/B" xlink:type="locator" '
            'xlink:label="reference"/>'
            '</daogrp></c>'
        )
        groups = ead_digital.parse_daogrp(component)
        self.assertEqual(1, len(groups))
        group = groups[0]
        self.assertEqual(["scan 1-0001"], group.notes)
        self.assertEqual(2, group.n_scans)
        self.assertEqual("https://proxy.example/thumb/A", group.scans[0].thumbnail_url)
        self.assertEqual("https://proxy.example/embed/A", group.scans[0].reference_url)
        self.assertEqual("https://proxy.example/thumb/B", group.scans[1].thumbnail_url)

    def test_daogrp_is_a_sibling_of_did_not_inside_it(self):
        component = _component(
            '<c level="file" xmlns:xlink="http://www.w3.org/1999/xlink"><did><unittitle/></did>'
            '<daogrp xlink:type="extended"/></c>'
        )
        self.assertEqual([], component.did_extra.get("dao", []))
        self.assertEqual(1, len(component.other_children.get("daogrp", [])))

    def test_no_daogrp_returns_empty_list(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        self.assertEqual([], ead_digital.parse_daogrp(component))


class TestAssets(unittest.TestCase):

    def test_assets_are_parsed_and_sorted_by_order(self):
        component = _component(
            '<c level="file" xmlns:nexus="urn:memorix.io"><did>'
            '<unitid label="Inventarisnummer">1</unitid><unittitle/>'
            '<nexus:assets allowAssets="true" allowDownloads="true">'
            '<nexus:asset asset_order="1">'
            '<nexus:name>B.jp2</nexus:name><nexus:iiif>https://x/2/info.json</nexus:iiif>'
            '</nexus:asset>'
            '<nexus:asset asset_order="0">'
            '<nexus:name>A.jp2</nexus:name><nexus:iiif>https://x/1/info.json</nexus:iiif>'
            '</nexus:asset>'
            '</nexus:assets></did></c>'
        )
        asset_list = ead_digital.parse_assets(component)
        self.assertEqual(2, len(asset_list))
        self.assertEqual(["A.jp2", "B.jp2"], [a.name for a in asset_list.assets])
        self.assertTrue(asset_list.allow_downloads)
        self.assertFalse(asset_list.truncated)

    def test_ten_assets_are_flagged_truncated(self):
        assets_xml = "".join(
            f'<nexus:asset asset_order="{i}"><nexus:name>{i}.jp2</nexus:name>'
            f'<nexus:iiif>https://x/{i}/info.json</nexus:iiif></nexus:asset>'
            for i in range(10)
        )
        component = _component(
            f'<c level="file" xmlns:nexus="urn:memorix.io"><did><unittitle/>'
            f'<nexus:assets allowAssets="true">{assets_xml}</nexus:assets></did></c>'
        )
        asset_list = ead_digital.parse_assets(component)
        self.assertEqual(10, len(asset_list))
        self.assertTrue(asset_list.truncated,
                        "Step 0 found every 10-asset list in the corpus is a truncated preview")

    def test_fewer_than_ten_assets_are_not_flagged_truncated(self):
        component = _component(
            '<c level="file" xmlns:nexus="urn:memorix.io"><did><unittitle/>'
            '<nexus:assets allowAssets="true">'
            '<nexus:asset asset_order="0"><nexus:name>A.jp2</nexus:name></nexus:asset>'
            '</nexus:assets></did></c>'
        )
        self.assertFalse(ead_digital.parse_assets(component).truncated)

    def test_no_assets_returns_none(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        self.assertIsNone(ead_digital.parse_assets(component))


class TestHandleUrls(unittest.TestCase):

    def test_handle_typed_unitid_is_extracted(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid>1</unitid>'
            '<unitid audience="internal" type="handle">'
            'https://hetutrechtsarchief.nl/collectie/x</unitid>'
            '<unittitle/></did></c>'
        )
        self.assertEqual(["https://hetutrechtsarchief.nl/collectie/x"],
                         ead_digital.parse_handle_urls(component))

    def test_no_handle_returns_empty_list(self):
        component = _component('<c level="file"><did><unitid>1</unitid><unittitle/></did></c>')
        self.assertEqual([], ead_digital.parse_handle_urls(component))


class TestSummary(unittest.TestCase):

    def test_mechanisms_lists_only_whats_present(self):
        component = _component(
            '<c level="file"><did><unittitle/>'
            '<dao role="METS" href="https://service.archief.nl/x"/></did></c>'
        )
        summary = ead_digital.summarise_digitisation(component)
        self.assertEqual(["dao"], summary.mechanisms)
        self.assertTrue(summary.is_digitised)

    def test_undigitised_component_reports_no_mechanisms(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        summary = ead_digital.summarise_digitisation(component)
        self.assertEqual([], summary.mechanisms)
        self.assertFalse(summary.is_digitised)
        self.assertIsNone(summary.n_scans_known)

    def test_n_scans_known_from_assets(self):
        component = _component(
            '<c level="file" xmlns:nexus="urn:memorix.io"><did><unittitle/>'
            '<nexus:assets allowAssets="true">'
            '<nexus:asset asset_order="0"><nexus:name>A.jp2</nexus:name></nexus:asset>'
            '<nexus:asset asset_order="1"><nexus:name>B.jp2</nexus:name></nexus:asset>'
            '</nexus:assets></did></c>'
        )
        summary = ead_digital.summarise_digitisation(component)
        self.assertEqual(2, summary.n_scans_known)

    def test_n_scans_known_is_none_for_a_bare_dao_or_handle(self):
        # a <dao>/handle URL points at a whole-object manifest, not a countable page list
        component = _component(
            '<c level="file"><did><unittitle/>'
            '<dao role="METS" href="https://service.archief.nl/x"/></did></c>'
        )
        self.assertIsNone(ead_digital.summarise_digitisation(component).n_scans_known)


class TestHasEvidence(unittest.TestCase):

    _DAO = ('<c level="file"><did><unittitle/>'
            '<dao role="METS" href="https://service.archief.nl/x"/></did></c>')
    _HANDLE = ('<c level="file"><did><unitid>1</unitid>'
               '<unitid audience="internal" type="handle">'
               'https://hetutrechtsarchief.nl/collectie/x</unitid>'
               '<unittitle/></did></c>')

    def test_dao_is_evidence_when_declared(self):
        summary = ead_digital.summarise_digitisation(_component(self._DAO))
        self.assertTrue(summary.has_evidence({"dao"}))
        self.assertFalse(summary.has_evidence({"daogrp"}))

    def test_handle_only_is_not_evidence_for_dao_archive(self):
        summary = ead_digital.summarise_digitisation(_component(self._HANDLE))
        self.assertTrue(summary.is_digitised)
        self.assertFalse(summary.has_evidence({"dao"}))
        self.assertTrue(summary.has_evidence({"handle"}))

    def test_empty_declaration_is_never_evidence(self):
        summary = ead_digital.summarise_digitisation(_component(self._DAO))
        self.assertFalse(summary.has_evidence(()))

    def test_unknown_mechanism_name_raises(self):
        summary = ead_digital.summarise_digitisation(_component(self._DAO))
        with self.assertRaises(ValueError):
            summary.has_evidence({"doa"})

    def test_bare_string_raises(self):
        summary = ead_digital.summarise_digitisation(_component(self._DAO))
        with self.assertRaises(TypeError):
            summary.has_evidence("dao")


if __name__ == "__main__":
    unittest.main()
