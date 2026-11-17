import gzip
from unittest import TestCase
from tempfile import TemporaryDirectory
from pathlib import Path

from archival_structures.analysis import page_elements as PE

LINES_H = "scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw text edit_flags split_from".split()
LAYOUT_H = "scan_id line_id region_id page layout_region_id layout_line_idx layout_column layout_role".split()
SEG_H = ("scan_id line_id seg_idx char_start char_end x y w h geom_method page layout_region_id line_type type_method "
         "column_override").split()
REG_H = ("scan_id page_side region_id region_xml_id parent_region_xml_id region_tag region_ro_index x y w h n_lines "
         "n_lines_text").split()


def _w(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8', newline='\n') as f:
        f.write('\t'.join(header) + '\n')
        for r in rows:
            f.write('\t'.join(str(v) for v in r) + '\n')


def build(root, drop_layout=None, drop_seg=None, dup=False):
    # two scans, same line ids
    lines = [("s1", "verso", "1", "r1", 0, "tl_1", 10, 20, 300, 30, "", 0.9, "Hello", "Hello", "", ""),
             ("s1", "verso", "1", "r1", 1, "tl_2", 10, 60, 300, 30, "", 0.9, "", "", "", ""),
             ("s1", "verso", "1", "r2", 2, "tl_3", 10, 100, 300, 30, "", 0.9, "Mixed line", "Mixed line", "", ""),
             ("s2", "recto", "1", "r1", 0, "tl_1", 50, 20, 200, 30, "", 0.9, "Datum 1700", "Datum 1700", "", "")]
    if dup:
        lines.append(lines[0])
    _w(root / "lines" / "7.lines.tsv.gz", LINES_H, lines)
    layout = [("s1", "tl_1", "r1", "verso", "L1", 0, 1, "main"), ("s1", "tl_2", "r1", "verso", "L1", 1, 1, "empty"),
              ("s1", "tl_3", "r2", "verso", "L2", 0, 1, "main"), ("s2", "tl_1", "r1", "recto", "L1", 0, 1, "main")]
    if drop_layout:
        layout = [r for r in layout if (r[0], r[1]) != drop_layout]
    _w(root / "layout" / "lines" / "7.tsv.gz", LAYOUT_H, layout)
    segs = [("s1", "tl_1", 0, 0, 5, 10, 20, 300, 30, "m", "verso", "L1", "main_text", "rule", ""),
            ("s1", "tl_3", 0, 0, 3, 10, 100, 40, 30, "m", "verso", "L2", "page_number", "rule", ""),
            ("s1", "tl_3", 1, 3, 10, 60, 100, 250, 30, "m", "verso", "L2", "marginalia", "rule", ""),
            ("s2", "tl_1", 0, 0, 10, 50, 20, 200, 30, "m", "recto", "L1", "date_header", "rule", "")]
    if drop_seg:
        segs = [r for r in segs if (r[0], r[1]) != drop_seg]
    _w(root / "types" / "segments" / "7.tsv.gz", SEG_H, segs)
    regs = [("s1", "verso", "r1", "x1", "", "paragraph", 0, 0, 0, 1, 1, 2, 1),
            ("s1", "verso", "r2", "x2", "", "marginalia", 1, 0, 0, 1, 1, 1, 1),
            ("s2", "recto", "r1", "x1", "", "untagged", 0, 0, 0, 1, 1, 1, 1)]
    _w(root / "regions_meta" / "7.tsv.gz", REG_H, regs)


class TestPageElements(TestCase):

    def setUp(self):
        self.td = TemporaryDirectory()
        self.root = Path(self.td.name)
        self.addCleanup(self.td.cleanup)

    def test_read_order_and_segments(self):
        build(self.root)
        got = PE.read_layer_lines(self.root, "7")
        self.assertEqual([("s1", "tl_1"), ("s1", "tl_3"), ("s2", "tl_1")], [(r["scan_id"], r["line_id"]) for r in got])
        mixed = got[1]
        self.assertEqual("marginalia", mixed["line_type"])   # longer second segment
        self.assertEqual(2, mixed["n_segments"])
        self.assertEqual("marginalia", mixed["region_tag"])
        self.assertEqual(300, got[0]["w"])
        self.assertEqual("L2", mixed["layout_region_id"])
        self.assertEqual("untagged", got[2]["region_tag"])

    def test_scan_filter(self):
        build(self.root)
        got = PE.read_layer_lines(self.root, "7", scan_ids={"s2"})
        self.assertEqual([("s2", "tl_1")], [(r["scan_id"], r["line_id"]) for r in got])

    def test_errors(self):
        build(self.root, dup=True)
        with self.assertRaises(ValueError):
            PE.read_layer_lines(self.root, "7")
        build(self.root, drop_layout=("s2", "tl_1"))
        with self.assertRaises(ValueError):
            PE.read_layer_lines(self.root, "7")
        build(self.root, drop_seg=("s1", "tl_1"))
        with self.assertRaises(ValueError):
            PE.read_layer_lines(self.root, "7")

    def test_layer_call(self):
        build(self.root)
        a, b, c = PE.read_layer_lines(self.root, "7")
        self.assertEqual({"type": "generic:running_text", "placement": "main", "region_type": "generic:running_text",
                          "region_placement": "main"}, PE.layer_call(a))
        self.assertEqual({"type": None, "placement": "margin", "region_type": None, "region_placement": "margin"},
                         PE.layer_call(b))
        self.assertEqual("generic:running_title:date", PE.layer_call(c)["type"])
        self.assertIsNone(PE.layer_call(c)["region_placement"])
        self.assertEqual("generic:list", PE.layer_call({"line_type": "index", "region_tag": None})["type"])

    def test_listlike_records(self):
        build(self.root)
        recs = PE.listlike_records(PE.read_layer_lines(self.root, "7"))
        self.assertEqual([("s1", "L1"), ("s1", "L2"), ("s2", "L1")], [r["group"] for r in recs])
        self.assertEqual((10, 310), (recs[0]["x0"], recs[0]["x1"]))

    def test_type_family(self):
        self.assertEqual("heading", PE.type_family("generic:heading:date"))
        self.assertEqual("noise", PE.type_family("generic:noise"))
        self.assertIsNone(PE.type_family(None))
        self.assertTrue(PE.type_family("generic:running_title:date") in PE.DATE_COMPATIBLE)
