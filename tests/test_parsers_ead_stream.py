"""Tests for `archival_structures.parsers.ead_stream`.

The core claim this module makes is that `parse_ead_streaming` builds the *same* tree as
`ead_tree.parse_ead`, by a different, bounded-memory route. `TestEquivalence` checks that
directly, against fixtures chosen specifically to exercise the two bugs an earlier draft of this
module had and that only a real corpus-wide comparison caught:

- a component with both a `<did>`/other descriptive children *and* nested component children,
  where the naive "clear a finished component, then drop its already-finished preceding
  siblings" memory-freeing idiom would delete the `<did>`/`<odd>` before ever reading them
  (`TestEquivalence.test_did_and_odd_survive_when_component_has_nested_children`);
- a `<dsc>` nested inside a component (Step 3's finding E1), which a naive same-tag-name check
  cannot distinguish from the genuine top-level `<dsc>`
  (`TestEquivalence.test_nested_dsc_does_not_leak_into_the_real_dsc_extra`);
- and a stress fixture wide/deep enough that CPython is likely to reuse a cleared element's
  `id()` for the next one, which silently corrupted sibling-index counting when a `next_index`
  entry was left keyed by a since-freed id() (`TestEquivalence.test_many_siblings_get_consecutive_indices`).

`TestReportAndStrict` covers the `strict`/`ParseReport` contract on malformed input.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archival_structures.parsers import ead_stream, ead_tree


def _write(directory: Path, name: str, content: str) -> Path:
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return path


class StreamTestCase(unittest.TestCase):

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def parse_both(self, name: str, content: str):
        path = _write(self.dir, name, content)
        in_memory = ead_tree.parse_ead(path)
        streaming, report = ead_stream.parse_ead_streaming(path)
        return in_memory, streaming, report

    def assert_equivalent(self, fa1: ead_tree.EADFindingAid, fa2: ead_tree.EADFindingAid):
        self.assertEqual(fa1.agency_code, fa2.agency_code)
        self.assertEqual(fa1.eadid, fa2.eadid)
        self.assertEqual(fa1.title, fa2.title)
        self.assertEqual(fa1.namespace, fa2.namespace)
        self.assertEqual(fa1.component_style, fa2.component_style)
        self.assertEqual(fa1.wrapper, fa2.wrapper)
        self.assertEqual(sorted(fa1.dsc_extra.keys()), sorted(fa2.dsc_extra.keys()))
        self.assertEqual(len(fa1.roots), len(fa2.roots))
        for a, b in zip(fa1.roots, fa2.roots):
            self._assert_component_equivalent(a, b)

    def _assert_component_equivalent(self, a: ead_tree.EADComponent, b: ead_tree.EADComponent):
        self.assertEqual(a.level, b.level, f"level differs at {a.index_path}")
        self.assertEqual(a.title, b.title, f"title differs at {a.index_path}")
        self.assertEqual(a.unitid_values(), b.unitid_values(), f"unitids differ at {a.index_path}")
        self.assertEqual(sorted(a.did_extra.keys()), sorted(b.did_extra.keys()),
                         f"did_extra keys differ at {a.index_path}")
        self.assertEqual(sorted(a.other_children.keys()), sorted(b.other_children.keys()),
                         f"other_children keys differ at {a.index_path}")
        self.assertEqual(a.missing_did, b.missing_did, f"missing_did differs at {a.index_path}")
        self.assertEqual(a.index_path, b.index_path, "index_path differs")
        self.assertEqual(a.level_path, b.level_path, "level_path differs")
        self.assertEqual(len(a.children), len(b.children), f"child count differs at {a.index_path}")
        for ca, cb in zip(a.children, b.children):
            self._assert_component_equivalent(ca, cb)


class TestEquivalence(StreamTestCase):

    def test_did_and_odd_survive_when_component_has_nested_children(self):
        # the exact shape of the bug an earlier draft had: a component with a <did>, an <odd>,
        # AND nested component children -- naive clear-plus-drop-preceding-siblings memory
        # freeing (triggered by the first nested child finishing) would delete <did> and <odd>
        # before the parent's own `end` event ever got to read them
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test">1</eadid></eadheader>
  <archdesc><dsc>
    <c level="file">
      <did><unitid identifier="u1">1</unitid><unittitle>A file</unittitle></did>
      <odd><p>A note.</p></odd>
      <c level="item"><did><unittitle>Item A</unittitle></did></c>
      <c level="item"><did><unittitle>Item B</unittitle></did></c>
    </c>
  </dsc></archdesc>
</ead>"""
        fa1, fa2, report = self.parse_both("nested.xml", xml)
        self.assertTrue(report.ok, list(report))
        self.assert_equivalent(fa1, fa2)
        # and directly assert the bug's specific symptom is gone, not just "trust the diff"
        streamed_file = fa2.roots[0]
        self.assertIn("odd", streamed_file.other_children)
        self.assertEqual(["1"], streamed_file.unitid_values())
        self.assertEqual(2, len(streamed_file.children))

    def test_nested_dsc_does_not_leak_into_the_real_dsc_extra(self):
        # Step 3's finding E1: a <dsc type="othertype"> nested inside a component, holding only
        # a <p> -- must not be mistaken for the genuine top-level <dsc> by a same-tag-name check
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead xmlns="urn:isbn:1-931666-22-9"><eadheader><eadid mainagencycode="NL-Test">1</eadid></eadheader>
  <archdesc><dsc>
    <head>Inventory</head>
    <c01 level="series">
      <did><unitid>1</unitid><unittitle>A series</unittitle></did>
      <c02 level="subseries">
        <did><unitid>1.1</unitid><unittitle>A subseries</unittitle></did>
        <c03>
          <did><unitid type="guid">ABCDEF00000000000000000000000001</unitid></did>
          <dsc type="othertype"><p>A free-text note, not a real hierarchy.</p></dsc>
        </c03>
      </c02>
    </c01>
  </dsc></archdesc>
</ead>"""
        fa1, fa2, report = self.parse_both("nesteddsc.xml", xml)
        self.assertTrue(report.ok, list(report))
        self.assert_equivalent(fa1, fa2)
        # the real, top-level dsc_extra must hold <head> only -- not the nested dsc's <p>
        self.assertEqual(["head"], sorted(fa1.dsc_extra.keys()))
        self.assertEqual(["head"], sorted(fa2.dsc_extra.keys()))
        # the nested <dsc> itself is retained on its component, under other_children
        leaf = fa2.roots[0].children[0].children[0]
        self.assertIn("dsc", leaf.other_children)

    def test_many_siblings_get_consecutive_indices(self):
        # a stress fixture wide and deep enough for CPython to plausibly reuse a cleared
        # element's id() for a later, unrelated one -- which silently corrupted sibling-index
        # counting when a stale `next_index` entry survived under that reused id()
        children = "".join(
            f'<c level="file"><did><unitid>{i}</unitid>'
            f'<unittitle>File {i}</unittitle></did>'
            f'<c level="item"><did><unittitle>Item {i}.1</unittitle></did></c>'
            f'</c>'
            for i in range(300)
        )
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test">1</eadid></eadheader>
  <archdesc><dsc><c level="series"><did><unittitle>S</unittitle></did>{children}</c></dsc></archdesc>
</ead>"""
        fa1, fa2, report = self.parse_both("wide.xml", xml)
        self.assertTrue(report.ok, list(report))
        self.assert_equivalent(fa1, fa2)
        indices = [c.index_path[-1] for c in fa2.roots[0].children]
        self.assertEqual(list(range(300)), indices,
                         "sibling indices must be consecutive, not inherited from a reused id()")

    def test_recursive_no_namespace_profile(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test">1</eadid></eadheader>
  <archdesc><dsc>
    <c level="file"><did><unitid identifier="u1">1</unitid></did></c>
    <c level="series"><did><unitid>I</unitid><unittitle>S</unittitle></did>
      <c level="file"><did><unitid identifier="u2">2</unitid></did></c>
    </c>
  </dsc></archdesc>
</ead>"""
        fa1, fa2, report = self.parse_both("recursive.xml", xml)
        self.assertTrue(report.ok)
        self.assert_equivalent(fa1, fa2)

    def test_numbered_namespaced_profile(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead xmlns="urn:isbn:1-931666-22-9"><eadheader><eadid mainagencycode="NL-UtHUA">842-3</eadid></eadheader>
  <archdesc><dsc>
    <c01 level="series"><did><unitid type="guid">X</unitid><unittitle>S</unittitle></did>
      <c02 level="file"><did><unitid>1</unitid>
        <unitid audience="internal" type="handle">https://example/x</unitid></did></c02>
    </c01>
  </dsc></archdesc>
</ead>"""
        fa1, fa2, report = self.parse_both("numbered.xml", xml)
        self.assertTrue(report.ok)
        self.assert_equivalent(fa1, fa2)
        self.assertEqual("numbered", fa2.component_style)

    def test_enveloped_profile(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<opendata><header><type>EAD</type></header><data>
<ead xmlns="urn:isbn:1-931666-22-9"><eadheader><eadid mainagencycode="NL-AhGldA">2388</eadid></eadheader>
  <archdesc><dsc>
    <c01 level="series"><did><unitid>1</unitid><unittitle>S</unittitle></did>
      <c02 level="file"><did><unitid>1</unitid></did></c02>
    </c01>
  </dsc></archdesc>
</ead>
</data></opendata>"""
        fa1, fa2, report = self.parse_both("enveloped.xml", xml)
        self.assertTrue(report.ok)
        self.assert_equivalent(fa1, fa2)
        self.assertEqual("opendata", fa2.wrapper)

    def test_level_less_and_unusual_otherlevel(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>
  <c><did><unitid>x</unitid><unittitle>No level</unittitle></did></c>
  <c otherlevel="Adres" level="otherlevel"><did><unittitle>An address</unittitle></did></c>
</dsc></archdesc></ead>"""
        fa1, fa2, report = self.parse_both("levelless.xml", xml)
        self.assertTrue(report.ok)
        self.assert_equivalent(fa1, fa2)

    def test_multiple_roots_directly_under_dsc(self):
        # the NL-HaNA_1.05.06 / NL-AsdSAA_395 construct: files as siblings of series under dsc
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>
  <c level="file"><did><unitid>1</unitid></did></c>
  <c level="series"><did><unitid>I</unitid><unittitle>S</unittitle></did>
    <c level="file"><did><unitid>2</unitid></did></c>
  </c>
  <c level="file"><did><unitid>3</unitid></did></c>
</dsc></archdesc></ead>"""
        fa1, fa2, report = self.parse_both("multiroot.xml", xml)
        self.assertTrue(report.ok)
        self.assertEqual(3, len(fa1.roots))
        self.assert_equivalent(fa1, fa2)


class TestCorpusEquivalence(unittest.TestCase):
    """`parse_ead_streaming` against every real finding aid in the corpus, not just fixtures.

    Fixtures pin the specific bugs found; this guards against the next one -- a real corpus is
    bigger and stranger than anything worth hand-writing, and that is exactly what caught the
    `id()`-reuse and nested-`<dsc>` bugs in the first place (neither reproduced on the small
    fixtures alone until they were deliberately shaped to trigger them).
    """

    def test_every_corpus_finding_aid_matches(self):
        corpus_dir = Path("data/metadata/eads")
        if not corpus_dir.exists():
            self.skipTest("corpus directory not present in this checkout")
        paths = sorted(corpus_dir.glob("*.xml"))
        if not paths:
            self.skipTest("no finding aids in the corpus directory")

        mismatches = []
        for path in paths:
            in_memory = ead_tree.parse_ead(path)
            streaming, report = ead_stream.parse_ead_streaming(path)
            if in_memory.parse_error or streaming.parse_error:
                continue  # covered by the robustness tests above, not this comparison
            if (in_memory.agency_code, in_memory.eadid, in_memory.title,
                sorted(in_memory.dsc_extra.keys()), len(in_memory.roots)) != \
               (streaming.agency_code, streaming.eadid, streaming.title,
                sorted(streaming.dsc_extra.keys()), len(streaming.roots)):
                mismatches.append(f"{path.name}: document-level fields differ")
                continue
            for i, (a, b) in enumerate(zip(in_memory.roots, streaming.roots)):
                result = self._first_difference(a, b)
                if result:
                    mismatches.append(f"{path.name} root[{i}]: {result}")
                    break

        self.assertEqual([], mismatches,
                         f"{len(mismatches)} of {len(paths)} finding aid(s) disagree:\n" +
                         "\n".join(mismatches[:10]))

    def _first_difference(self, a, b):
        sig_a = (a.level, a.title, tuple(a.unitid_values()), a.index_path, a.level_path,
                len(a.children))
        sig_b = (b.level, b.title, tuple(b.unitid_values()), b.index_path, b.level_path,
                len(b.children))
        if sig_a != sig_b:
            return f"{sig_a} != {sig_b}"
        for ca, cb in zip(a.children, b.children):
            result = self._first_difference(ca, cb)
            if result:
                return result
        return None


class TestReportAndStrict(StreamTestCase):

    def test_strict_raises_on_malformed_xml(self):
        path = _write(self.dir, "broken.xml", "<ead><archdesc><dsc><c level='file'>")
        with self.assertRaises(Exception):
            ead_stream.parse_ead_streaming(path, strict=True)

    def test_non_strict_collects_a_diagnostic_for_malformed_xml(self):
        path = _write(self.dir, "broken.xml", "<ead><archdesc><dsc><c level='file'>")
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=False)
        self.assertIsNotNone(finding_aid.parse_error)
        self.assertFalse(report.ok)
        self.assertEqual(1, len(report))
        self.assertEqual("xml_syntax_error", report.diagnostics[0].kind)

    def test_missing_did_is_a_diagnostic_not_a_crash(self):
        path = _write(
            self.dir, "nodid.xml",
            "<ead><archdesc><dsc><c level='series'><did><unittitle>S</unittitle></did>"
            "<c level='file'></c></c></dsc></archdesc></ead>",
        )
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=False)
        file_component = next(c for c in finding_aid.walk() if c.level == "file")
        self.assertTrue(file_component.missing_did)
        self.assertFalse(report.ok)
        self.assertEqual("missing_did", report.diagnostics[0].kind)

    def test_missing_did_does_not_raise_even_in_strict_mode(self):
        # missing_did is a structural observation about content, like ead_tree.parse_ead's
        # equivalent contract -- only a malformed *document* (unparseable XML, no <ead>, no
        # <dsc>) raises in strict mode, not a component that happens to lack a <did>
        path = _write(
            self.dir, "nodid.xml",
            "<ead><archdesc><dsc><c level='series'><did><unittitle>S</unittitle></did>"
            "<c level='file'></c></c></dsc></archdesc></ead>",
        )
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=True)
        self.assertIsNone(finding_aid.parse_error)

    def test_no_dsc_raises_in_strict_mode_and_reports_otherwise(self):
        path = _write(self.dir, "nodsc.xml", "<ead><eadheader><eadid>x</eadid></eadheader></ead>")
        with self.assertRaises(ValueError):
            ead_stream.parse_ead_streaming(path, strict=True)
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=False)
        self.assertEqual("no <dsc> element", finding_aid.parse_error)
        self.assertFalse(report.ok)

    def test_no_ead_element_raises_in_strict_mode_and_reports_otherwise(self):
        path = _write(self.dir, "notead.xml", "<mets><fileSec/></mets>")
        with self.assertRaises(ValueError):
            ead_stream.parse_ead_streaming(path, strict=True)
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=False)
        self.assertEqual("no <ead> element found", finding_aid.parse_error)
        self.assertFalse(report.ok)

    def test_clean_parse_has_an_empty_report(self):
        path = _write(
            self.dir, "clean.xml",
            "<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>"
            "<c level='file'><did><unitid>1</unitid></did></c>"
            "</dsc></archdesc></ead>",
        )
        finding_aid, report = ead_stream.parse_ead_streaming(path, strict=False)
        self.assertTrue(report.ok)
        self.assertEqual(0, len(report))
        self.assertIsNone(finding_aid.parse_error)


if __name__ == "__main__":
    unittest.main()
