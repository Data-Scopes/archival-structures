"""Tests for `archival_structures.parsers.ead_scope_notes`.

The first group was ported from LotA's `test_scopecontent.py` / `test_scopecontent_survey.py`;
the cross-archive tests are new (phase 02, step 5).
"""

import pickle
import re
import unittest
from pathlib import Path

from archival_structures.model.inventory_number import InventoryNumber
from archival_structures.parsers import ead_scope_notes as sn
from archival_structures.parsers import ead_tree

VOC = "1.04.02"


def _finding_aid(*components_xml: str) -> ead_tree.EADFindingAid:
    body = "".join(components_xml)
    fa = ead_tree.parse_ead_string(
        f'<ead><eadheader><eadid>{VOC}</eadid></eadheader>'
        f'<archdesc><dsc>{body}</dsc></archdesc></ead>'
    )
    assert fa.parse_error is None, fa.parse_error
    return fa


def _component_xml(scopecontent_xml: str) -> str:
    return f'<c level="file"><did><unittitle>t</unittitle></did>{scopecontent_xml}</c>'


def _component(scopecontent_xml: str) -> ead_tree.EADComponent:
    return _finding_aid(_component_xml(scopecontent_xml)).roots[0]


def _notes(text_xml: str, archive_id: str = VOC, **kwargs):
    return sn.parse_scope_notes(
        _component(f"<scopecontent>{text_xml}</scopecontent>"), archive_id, **kwargs
    )


def _inv_refs(note):
    return [r for r in note.references if r.kind in ("inventory", "unparsed")]


class TestPorted(unittest.TestCase):

    def test_lb_does_not_concatenate_words(self):
        notes = _notes("<p>Kopie.<lb/>Exh. St.-Gen. 19 december 1742.</p>")
        self.assertEqual(1, len(notes))
        self.assertNotIn("Kopie.Exh.", notes[0].text)
        self.assertIn("Kopie. Exh.", notes[0].text)

    def test_lb_before_punctuation_adds_no_space_in_segments(self):
        notes = _notes("<p>Eems<lb/>. Met<lb/>kopie<lb/>, en</p>")
        self.assertEqual(("Eems. Met kopie, en",), notes[0].segments)
        self.assertEqual(notes[0].text, notes[0].segments[0])

    def test_inventory_reference_parsed_into_inventory_number(self):
        notes = _notes("<p>Was Loketkas Admiraliteit, inv.nr. 12561.13.2. Zie ook inv. nr. 9015A.</p>")
        inv_refs = [r for r in notes[0].references if r.kind == "inventory"]
        self.assertEqual(2, len(inv_refs))
        self.assertEqual({"12561.13.2", "9015A"}, {str(r.inventory_number) for r in inv_refs})
        for r in inv_refs:
            self.assertIsInstance(r.inventory_number, InventoryNumber)

    def test_unparseable_reference_is_kept_as_unparsed(self):
        notes = _notes("<p>Zie inv. nrs. 8480-8482.</p>")
        unparsed = [r for r in notes[0].references if r.kind == "unparsed"]
        self.assertEqual(1, len(unparsed))
        self.assertEqual("8480-8482", unparsed[0].raw)
        self.assertIsNone(unparsed[0].inventory_number)

    def test_folio_span_is_kept_raw_not_expanded(self):
        notes = _notes("<p>zie inv. nr. 1070, f. 214-240.</p>")
        refs = notes[0].references
        folios = [r for r in refs if r.kind == "folio"]
        self.assertEqual(1, len(folios))
        self.assertEqual("f. 214-240", folios[0].raw)
        self.assertEqual("f. 214-240", folios[0].folio)
        inv_refs = [r for r in refs if r.kind == "inventory"]
        self.assertEqual(1, len(inv_refs))
        self.assertEqual("1070", str(inv_refs[0].inventory_number))

    def test_date_strings_are_raw_substrings_only(self):
        notes = _notes("<p>Exh. St.-Gen. 19 december 1742.</p>")
        self.assertEqual(("19 december 1742",), notes[0].date_strings)

    def test_scope_note_is_picklable(self):
        note = _notes("<p>zie inv. nr. 1070, f. 214-240.</p>")[0]
        self.assertEqual(note, pickle.loads(pickle.dumps(note)))

    def test_classify_shape_examples_from_the_taxonomy(self):
        cases = {
            "": "empty",
            "Exh. St.-Gen. 19 december 1742": "exhibitum_date",
            "Was Loketkas Admiraliteit, inv.nr. 12561.13.2.": "former_reference",
            "zie inv. nr. 9919.": "cross_reference",
            "Bevat index": "short_content_flag",
            "Kopie.": "physical_form_note",
            "De jaren 1729 en 1737 ontbreken": "gap_note",
            "Grotendeels in het Frans": "language_note",
            "Something else entirely": "unclassified",
        }
        for text, shape in cases.items():
            self.assertEqual((shape, shape), sn.classify_shape(text, ()), text)
        self.assertEqual(("list_markup",) * 2, sn.classify_shape("x", ("item", "list")))
        self.assertEqual(14, len(sn.NA_SHAPE_RULES))


class TestCrossArchive(unittest.TestCase):

    def test_cross_archive_reference_carries_target(self):
        note = _notes("<p>Zie Aanwinsten, 1.11.01.01, inv.nr. 687, (1893 29r).</p>")[0]
        refs = _inv_refs(note)
        self.assertEqual(1, len(refs))
        self.assertEqual(("inventory", "687", "1.11.01.01"), (refs[0].kind, refs[0].raw, refs[0].target_archive))

    def test_cross_archive_variants(self):
        for text in (
            "zie Aanwinsten, 1.11.01.01 inv.nr. 1519 (1926 I 2)",
            "Zie Aanwinsten, 1.11.01.01. inv. nr. 794",
        ):
            refs = _inv_refs(_notes(f"<p>{text}</p>")[0])
            self.assertEqual(1, len(refs), text)
            self.assertEqual("1.11.01.01", refs[0].target_archive, text)

    def test_cross_archive_unparsed_range(self):
        refs = _inv_refs(_notes("<p>zie Archief Staten-Generaal, 1.01.02, inv.nrs. 4841-4844.</p>")[0])
        self.assertEqual(1, len(refs))
        self.assertEqual(("unparsed", "4841-4844", "1.01.02"), (refs[0].kind, refs[0].raw, refs[0].target_archive))

    def test_same_archive_code_gives_none(self):
        refs = _inv_refs(_notes("<p>1.04.02, inv.nr. 5</p>")[0])
        self.assertEqual(1, len(refs))
        self.assertEqual("5", refs[0].raw)
        self.assertIsNone(refs[0].target_archive)

    def test_mixed_list_stops_before_new_reference(self):
        refs = _inv_refs(_notes("<p>Zie inv.nr. 12, 1.11.01.01, inv.nr. 687</p>")[0])
        self.assertEqual([("12", None), ("687", "1.11.01.01")], [(r.raw, r.target_archive) for r in refs])
        self.assertNotIn("1.11.01.01", [r.raw for r in refs])

    def test_patterns_without_archive_group_never_set_target(self):
        base = sn.NA_REFERENCE_PATTERNS
        patterns = sn.ReferencePatterns(
            inventory=re.compile(r"inv\.nr\. (?P<numbers>\d+)"),
            inventory_token=base.inventory_token,
            folio=base.folio,
            date_range=base.date_range,
            date_full=base.date_full,
        )
        refs = _inv_refs(_notes("<p>Zie Aanwinsten, 1.11.01.01, inv.nr. 687</p>", patterns=patterns)[0])
        self.assertEqual(1, len(refs))
        self.assertIsNone(refs[0].target_archive)


class TestParameters(unittest.TestCase):

    def test_custom_shape_rules_are_used(self):
        rules = (sn.ShapeRule("greeting", "says hello", pattern=re.compile(r"^Hello")),)
        notes = _notes("<p>Hello there</p>", shape_rules=rules)
        self.assertEqual(("greeting", "greeting"), (notes[0].shape, notes[0].shape_rule))
        other = _notes("<p>Zie inv. nr. 5</p>", shape_rules=rules)
        self.assertEqual("unclassified", other[0].shape)

    def test_iter_scope_notes_order_and_node_ids(self):
        fa = _finding_aid(
            '<c level="series"><did><unittitle>s</unittitle></did>'
            '<scopecontent><p>first</p></scopecontent>'
            + _component_xml("<scopecontent><p>second</p></scopecontent><scopecontent><p>third</p></scopecontent>")
            + '</c>',
            _component_xml("<scopecontent><p>fourth</p></scopecontent>"),
        )
        notes = list(sn.iter_scope_notes(fa))
        self.assertEqual(["first", "second", "third", "fourth"], [n.text for n in notes])
        self.assertEqual([0, 0, 1, 0], [n.element_index for n in notes])
        self.assertEqual(
            [f"{VOC}:0", f"{VOC}:0.0", f"{VOC}:0.0", f"{VOC}:1"], [n.node_id for n in notes]
        )
        self.assertEqual(4, len(sn.scope_notes_frame(fa)))


REAL = Path(__file__).resolve().parent.parent / "data" / "metadata" / "eads" / "NL-HaNA_1.04.02.xml"


@unittest.skipUnless(REAL.exists(), "real VOC EAD not staged")
class RealFileTests(unittest.TestCase):
    def test_voc_real_counts(self):
        fa = ead_tree.parse_ead(REAL)
        notes = list(sn.iter_scope_notes(fa))
        expected = sum(len(c.other_children.get("scopecontent", [])) for c in fa.walk())
        self.assertEqual(expected, len(notes))
        self.assertTrue(any(r.target_archive == "1.11.01.01" for n in notes for r in n.references))
        pattern = re.compile(r"1\.11\.01\.01[.,]?\s*inv")
        for n in notes:
            if pattern.search(n.text):
                self.assertTrue(
                    all(r.target_archive is not None for r in _inv_refs(n)), n.text
                )


if __name__ == "__main__":
    unittest.main()
