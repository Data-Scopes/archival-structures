"""Tests for stable node ids, identity-hashable components and `persistent_id`
(`EADComponent.node_id` / `parent_id` / `ancestor_ids` / `persistent_id`)."""

import tempfile
import unittest
from pathlib import Path

from archival_structures.parsers import ead_stream, ead_tree, ead_views

EAD = """<?xml version="1.0" encoding="UTF-8"?>
<ead><eadheader><eadid mainagencycode="NL-Test" countrycode="NL">1.23.45</eadid></eadheader>
<archdesc level="fonds"><dsc>
  <c level="series"><did><unitid>I</unitid><unittitle>S</unittitle></did>
    <c level="file"><did>
      <unitid type="handle">http://x/1</unitid>
      <unitid type="urn:uuid">AAAA-1</unitid><unitid>1</unitid><unittitle>F1</unittitle></did>
      <c level="item"><did><unitid>1a</unitid><unittitle>I</unittitle></did></c>
      <c level="item"><did><unitid>1b</unitid><unittitle>I2</unittitle></did></c>
    </c>
    <c level="file"><did><unitid>2</unitid><unittitle>F2</unittitle></did></c>
  </c>
  <c level="file"><did><unitid>3</unitid><unittitle>F3</unittitle></did></c>
</dsc></archdesc></ead>
"""


class NodeIdTests(unittest.TestCase):
    def setUp(self):
        self.fa = ead_tree.parse_ead_string(EAD)

    def test_format_and_default_archive_id(self):
        self.assertEqual(self.fa.default_archive_id, "1.23.45")
        ids = [i for i, _ in self.fa.iter_node_ids()]
        self.assertEqual(ids, ["1.23.45:0", "1.23.45:0.0", "1.23.45:0.0.0", "1.23.45:0.0.1",
                               "1.23.45:0.1", "1.23.45:1"])
        self.assertEqual(self.fa.roots[0].node_id("x"), "x:0")

    def test_unique_and_stable_across_parses(self):
        a = [i for i, _ in self.fa.iter_node_ids()]
        b = [i for i, _ in ead_tree.parse_ead_string(EAD).iter_node_ids()]
        self.assertEqual(len(set(a)), len(a))
        self.assertEqual(a, b)

    def test_parent_and_ancestor_ids_match_ancestors(self):
        for node_id, c in self.fa.iter_node_ids("a"):
            anc = c.ancestors()
            self.assertEqual(c.ancestor_ids("a"), [x.node_id("a") for x in anc])
            self.assertEqual(c.parent_id("a"), anc[-1].node_id("a") if anc else None)
            self.assertEqual(node_id, c.node_id("a"))
        self.assertIsNone(self.fa.roots[0].parent_id("a"))

    def test_colon_in_archive_id_rejected(self):
        with self.assertRaises(ValueError):
            self.fa.roots[0].node_id("a:b")

    def test_eadid_missing_falls_back_to_stem(self):
        fa = ead_tree.parse_ead_string(EAD.replace("1.23.45", ""), source_name="foo:bar.xml")
        self.assertEqual(fa.default_archive_id, "foo_bar")


class IdentityTests(unittest.TestCase):
    def test_hashable_and_identity_equality(self):
        a = ead_tree.parse_ead_string(EAD)
        b = ead_tree.parse_ead_string(EAD)
        comps = list(a.walk())
        self.assertEqual(len(set(comps)), len(comps))
        lookup = {c: i for i, c in enumerate(comps)}
        self.assertEqual(lookup[comps[3]], 3)
        for x, y in zip(a.walk(), b.walk()):
            self.assertNotEqual(x, y)
            self.assertEqual(x, x)
        self.assertEqual(len(set(a.walk()) | set(b.walk())), 2 * len(comps))


class PersistentIdTests(unittest.TestCase):
    def test_extracted_or_none(self):
        fa = ead_tree.parse_ead_string(EAD)
        got = {c.title: c.persistent_id for c in fa.walk()}
        self.assertEqual(got["F1"], "AAAA-1")
        self.assertEqual(sum(v is not None for v in got.values()), 1)


class StreamParityTests(unittest.TestCase):
    def test_same_node_ids(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "t.xml"
            path.write_text(EAD, encoding="utf-8")
            tree = ead_tree.parse_ead(path)
            stream, _ = ead_stream.parse_ead_streaming(path)
        self.assertEqual([(i, c.persistent_id) for i, c in tree.iter_node_ids()],
                         [(i, c.persistent_id) for i, c in stream.iter_node_ids()])


class ViewTests(unittest.TestCase):
    def test_frames_have_columns(self):
        fa = ead_tree.parse_ead_string(EAD)
        for frame, n in ((ead_views.to_frame(fa, "file"), 3),
                         (ead_views.unit_of_description_frame(fa), None)):
            self.assertIn("node_id", frame.columns)
            self.assertIn("persistent_id", frame.columns)
            self.assertTrue(frame["node_id"].is_unique)
            if n is not None:
                self.assertEqual(len(frame), n)
        files = ead_views.to_frame(fa, "file")
        self.assertEqual(files.loc[files["title"] == "F1", "persistent_id"].iloc[0], "AAAA-1")
        self.assertEqual(files.loc[files["title"] == "F1", "node_id"].iloc[0], "1.23.45:0.0")


REAL = Path(__file__).resolve().parent.parent / "data" / "metadata" / "eads" / "NL-HaNA_1.01.02.xml"


@unittest.skipUnless(REAL.exists(), "real SG EAD not staged")
class RealFileTests(unittest.TestCase):
    def test_sg(self):
        fa = ead_tree.parse_ead(REAL)
        pairs = list(fa.iter_node_ids())
        self.assertEqual(len({i for i, _ in pairs}), len(pairs))
        pids = [c.persistent_id for _, c in pairs]
        self.assertTrue(all(pids))
        self.assertEqual(len(set(pids)), len(pids))


if __name__ == "__main__":
    unittest.main()
