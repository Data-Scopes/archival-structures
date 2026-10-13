"""Tests for `archival_structures.parsers.ead_fields`.

`identify_inventory_number` is tested against the three disjoint conventions Step 1/2 measured
(Amsterdam's label, the Nationaal Archief's untyped `@identifier`, Het Utrechts Archief's
mark-by-absence), plus the GUID-starts-with-a-digit hazard that makes the legacy parser's
`re.match(r"\\d+", ...)` unsafe on the wider corpus (quantified in the Step 4 report). Dates and
physical description are tested against the embedded-`<unitdate>`-in-`<unittitle>` construct
Step 0 found the legacy parser mishandling.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.model.inventory_number import InventoryNumber
from archival_structures.parsers import ead_fields, ead_tree


def _component(xml: str) -> ead_tree.EADComponent:
    """Parse a `<c>...</c>` fragment (wrapped in a minimal EAD document) and return that
    component -- the first (and only) root."""
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


class TestAmsterdamLabelRule(unittest.TestCase):

    def test_labelled_inventarisnummer_is_identified(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid identifier="u1" label="Inventarisnummer">42A</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("label:inventarisnummer", result.rule)
        self.assertEqual(InventoryNumber("42A"), result.inventory_number)

    def test_rubriekscode_is_not_mistaken_for_the_label_rule(self):
        # Rubriekscode also has @identifier + a label, but is not "Inventarisnummer"; it must
        # not satisfy rule 1, and (since it has an @identifier and no @type) must not satisfy
        # rule 3 either -- Rubriekscode marks a classification code, not a file
        component = _component(
            '<c level="series"><did>'
            '<unitid identifier="r1" label="Rubriekscode">1</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertNotEqual("label:inventarisnummer", result.rule)
        self.assertNotEqual("identifier:untyped", result.rule)

    def test_label_and_identifier_together_prefer_the_label_rule(self):
        # both rule 1 and rule 3's conditions are satisfied; rule 1 must win, since it is the
        # more direct signal (this is the actual shape of every Amsterdam file component)
        component = _component(
            '<c level="file"><did>'
            '<unitid identifier="u1" label="Inventarisnummer">1</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("label:inventarisnummer", result.rule)


class TestNationaalArchiefIdentifierRule(unittest.TestCase):

    def test_untyped_identifier_is_identified(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid identifier="39104160">7474</unitid>'
            '<unitid audience="internal" type="handle">http://hdl.handle.net/x</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("identifier:untyped", result.rule)
        self.assertEqual(InventoryNumber("7474"), result.inventory_number)

    def test_abs_type_rule_fires_when_present(self):
        # measured to occur 0 times in the current corpus (Step 4 report), but it is a
        # documented convention and the legacy parser relies on it, so it is kept and tested
        component = _component(
            '<c level="file"><did><unitid type="ABS">99</unitid><unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("type:ABS", result.rule)


class TestUtrechtShapeRule(unittest.TestCase):

    def test_the_bare_unitid_wins_once_guid_and_handle_are_excluded(self):
        # NL-UtHUA_842-3's actual shape: no label, no @identifier anywhere -- only a shape
        # heuristic over what's left once known-non-identifying unitids are excluded can work
        component = _component(
            '<c level="file"><did>'
            '<unitid type="guid">609C5B9D68E64642E0534701000A17FD</unitid>'
            '<unitid>1</unitid>'
            '<unitid audience="internal" type="handle">'
            'https://hetutrechtsarchief.nl/collectie/x</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("shape:only-candidate", result.rule)
        self.assertEqual(InventoryNumber("1"), result.inventory_number)

    def test_a_guid_that_starts_with_a_digit_is_still_excluded(self):
        # the actual hazard: 29% of file components corpus-wide have a GUID-typed unitid
        # starting with a digit, listed before the real identifier
        component = _component(
            '<c level="file"><did>'
            '<unitid type="guid">67A31B2F3A94451C852E5545242E6C1B</unitid>'
            '<unitid>2</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual(InventoryNumber("2"), result.inventory_number)
        self.assertNotEqual(InventoryNumber("67A31B2F3A94451C852E5545242E6C1B"),
                            result.inventory_number)

    def test_a_bare_guid_shaped_value_is_excluded_even_with_no_type_attribute(self):
        # defence in depth: the shape itself is the hazard, not just the @type="guid" label
        component = _component(
            '<c level="file"><did>'
            '<unitid>67A31B2F3A94451C852E5545242E6C1B</unitid>'
            '<unitid>3</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual(InventoryNumber("3"), result.inventory_number)

    def test_a_url_shaped_value_is_excluded(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid>https://example.invalid/x</unitid>'
            '<unitid>4</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual(InventoryNumber("4"), result.inventory_number)

    def test_series_code_is_excluded_by_shape_failure(self):
        # "2.1.1." has a trailing dot, which InventoryNumber rejects as an empty part -- so
        # series_code values self-exclude even without the explicit type exclusion
        component = _component(
            '<c level="subseries"><did>'
            '<unitid type="series_code">2.1.1.</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("none", result.rule)


class TestAmbiguousAndAbsent(unittest.TestCase):

    def test_two_equally_valid_candidates_are_reported_ambiguous(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid>12</unitid><unitid>34</unitid><unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("shape:ambiguous", result.rule)
        self.assertEqual(2, len(result.candidates))
        # still produces a deterministic (if provisional) answer rather than nothing
        self.assertIsNotNone(result.inventory_number)

    def test_no_unitid_at_all_is_reported_as_none_not_a_crash(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("none", result.rule)
        self.assertFalse(result)

    def test_only_excluded_unitids_is_reported_as_none(self):
        component = _component(
            '<c level="file"><did>'
            '<unitid type="handle">http://hdl.handle.net/x</unitid>'
            '<unitid type="obsolete">St90-832</unitid>'
            '<unittitle/></did></c>'
        )
        result = ead_fields.identify_inventory_number(component)
        self.assertEqual("none", result.rule)


class TestLegacyStyleComparison(unittest.TestCase):

    def test_legacy_style_picks_the_guid_the_new_rule_correctly_excludes(self):
        # this is the disagreement that matters: the legacy digit-leading regex has no concept
        # of @type and picks whichever unitid comes first and starts with a digit
        component = _component(
            '<c level="file"><did>'
            '<unitid type="guid">67A31B2F3A94451C852E5545242E6C1B</unitid>'
            '<unitid>2</unitid>'
            '<unittitle/></did></c>'
        )
        legacy = ead_fields.legacy_style_identify(component)
        new = ead_fields.identify_inventory_number(component)
        self.assertEqual("67A31B2F3A94451C852E5545242E6C1B", legacy)
        self.assertEqual(InventoryNumber("2"), new.inventory_number)
        self.assertNotEqual(legacy, str(new.inventory_number))


class TestUnitDate(unittest.TestCase):

    def test_plain_did_level_unitdate(self):
        component = _component(
            '<c level="file"><did><unittitle>A file</unittitle>'
            '<unitdate calendar="gregorian" era="ce" normal="1600/1601">1600-1601</unitdate>'
            '</did></c>'
        )
        date = ead_fields.parse_unitdate(component)
        self.assertEqual("did", date.source)
        self.assertEqual("1600", date.year_begin)
        self.assertEqual("1601", date.year_end)
        self.assertEqual("1600-1601", date.text)

    def test_unitdate_embedded_in_unittitle_is_recovered(self):
        # NL-HaNA_1.01.02 file 7474's exact shape: no did-level <unitdate> at all, the date
        # is the entirety of an otherwise-empty <unittitle>'s content
        component = _component(
            '<c level="file"><did>'
            '<unittitle><unitdate calendar="gregorian" era="ce" normal="1600/1601">'
            '1600-1601</unitdate></unittitle></did></c>'
        )
        date = ead_fields.parse_unitdate(component)
        self.assertIsNotNone(date, "the embedded unitdate must still be found")
        self.assertEqual("unittitle", date.source)
        self.assertEqual("1600", date.year_begin)
        self.assertEqual("1601", date.year_end)

    def test_a_did_level_unitdate_is_preferred_over_an_embedded_one(self):
        component = _component(
            '<c level="file"><did>'
            '<unittitle>A title <unitdate normal="1700">1700</unitdate></unittitle>'
            '<unitdate normal="1650">1650</unitdate>'
            '</did></c>'
        )
        date = ead_fields.parse_unitdate(component)
        self.assertEqual("did", date.source)
        self.assertEqual("1650", date.year_begin)

    def test_no_date_anywhere_returns_none(self):
        component = _component('<c level="file"><did><unittitle>No date</unittitle></did></c>')
        self.assertIsNone(ead_fields.parse_unitdate(component))

    def test_single_year_normal_gives_the_same_begin_and_end(self):
        component = _component(
            '<c level="file"><did>'
            '<unitdate normal="1614">11 oktober 1614</unitdate></did></c>'
        )
        date = ead_fields.parse_unitdate(component)
        self.assertEqual(("1614", "1614"), (date.year_begin, date.year_end))


class TestPhysDesc(unittest.TestCase):

    def test_bare_text_physdesc(self):
        component = _component('<c level="file"><did><physdesc>1 stuk</physdesc></did></c>')
        physdesc = ead_fields.parse_physdesc(component)
        self.assertEqual("1 stuk", physdesc.extent)

    def test_structured_physdesc_with_extent_and_physfacet(self):
        component = _component(
            '<c level="file"><did><physdesc>'
            '<extent>3 dozen</extent><physfacet>foto</physfacet>'
            '</physdesc></did></c>'
        )
        physdesc = ead_fields.parse_physdesc(component)
        self.assertEqual("3 dozen", physdesc.extent)
        self.assertEqual("foto", physdesc.physfacet)

    def test_no_physdesc_returns_none(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        self.assertIsNone(ead_fields.parse_physdesc(component))


class TestPhysLoc(unittest.TestCase):

    def test_amsterdam_style_depot_attribute(self):
        component = _component(
            '<c level="file"><did><physloc depot="c859d53d-…"/></did></c>'
        )
        physloc = ead_fields.parse_physloc(component)
        self.assertEqual("c859d53d-…", physloc.attributes["depot"])
        self.assertIsNone(physloc.text)


class TestControlAccess(unittest.TestCase):

    def test_genreform_is_extracted_from_outside_did(self):
        component = _component(
            '<c level="file"><did><unittitle/></did>'
            '<controlaccess><genreform source="local">Akte</genreform></controlaccess></c>'
        )
        forms = ead_fields.parse_controlaccess(component)
        self.assertEqual(1, len(forms))
        self.assertEqual("Akte", forms[0].text)
        self.assertEqual("local", forms[0].attributes["source"])

    def test_no_controlaccess_returns_empty_list(self):
        component = _component('<c level="file"><did><unittitle/></did></c>')
        self.assertEqual([], ead_fields.parse_controlaccess(component))


if __name__ == "__main__":
    unittest.main()
