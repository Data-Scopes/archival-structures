from unittest import TestCase

import pagexml.model.physical_document_model as pdm
import pandas as pd

from archival_structures.analysis.opening_detection import (
    PAGE_TYPE_ODD, PAGE_TYPE_OPENING, PAGE_TYPE_SINGLE,
    STRUCTURE_BOOK_OF_OPENINGS, STRUCTURE_MIXED,
    InventoryStructure, classify_inventory_structure, classify_page_types, compute_corpus_median_width,
    find_width_groups, get_opening_features, identify_page_unit_widths, is_opening,
    split_inventory_into_pages, split_scan, split_image,
)
from archival_structures.datasets.images_transcriptions import PAGEXML_DIR, test_pairs


def make_scan(doc_id: str, width: int, height: int, regions=()):
    return pdm.PageXMLScan(
        doc_id=doc_id,
        coords=pdm.Coords.coords_from_box_params(0, 0, width, height),
        text_regions=list(regions),
    )


def make_text_region(doc_id: str, x: int, y: int, w: int, h: int, line_box=None):
    coords = pdm.Coords.coords_from_box_params(x, y, w, h)
    line_box = line_box or (x, y, w, h)
    line = pdm.PageXMLTextLine(doc_id=f"{doc_id}-line",
                               coords=pdm.Coords.coords_from_box_params(*line_box))
    return pdm.PageXMLTextRegion(doc_id=doc_id, coords=coords, lines=[line])


class TestOpeningDetection(TestCase):

    def test_single_page_is_not_opening(self):
        # one region spanning almost the whole (narrow) page -> no central gap
        region = make_text_region('r1', x=50, y=50, w=500, h=900)
        scan = make_scan('single-page', width=600, height=1000, regions=[region])
        self.assertFalse(is_opening(scan))

    def test_two_columns_with_gap_is_opening(self):
        left = make_text_region('left', x=50, y=50, w=400, h=900)
        right = make_text_region('right', x=550, y=50, w=400, h=900)
        scan = make_scan('opening', width=1000, height=1000, regions=[left, right])
        features = get_opening_features(scan)
        self.assertTrue(features.is_opening)
        self.assertEqual(0, features.separation_point.y)
        self.assertGreater(features.gap_width, 0)

    def test_split_scan_assigns_regions_by_side_and_translates_right_side(self):
        left = make_text_region('left', x=50, y=50, w=400, h=900)
        right = make_text_region('right', x=550, y=50, w=400, h=900)
        scan = make_scan('opening', width=1000, height=1000, regions=[left, right])
        features = get_opening_features(scan)

        verso, recto = split_scan(scan, features.separation_point)

        self.assertEqual(['left'], [r.id for r in verso.text_regions])
        self.assertEqual(['right'], [r.id for r in recto.text_regions])
        # recto region's x should be translated relative to the separation point
        sep_x = round(features.separation_point.x)
        self.assertEqual(550 - sep_x, recto.text_regions[0].coords.x)
        # original scan must not be mutated
        self.assertEqual(550, scan.text_regions[1].coords.x)

    def test_split_image_widths_sum_to_original(self):
        left = make_text_region('left', x=50, y=50, w=400, h=900)
        right = make_text_region('right', x=550, y=50, w=400, h=900)
        scan = make_scan('opening', width=1000, height=1000, regions=[left, right])
        features = get_opening_features(scan)

        import numpy as np
        image = np.zeros((1000, 1000, 3), dtype=np.uint8)
        left_img, right_img = split_image(image, features.separation_point)
        self.assertEqual(1000, left_img.shape[1] + right_img.shape[1])

    def test_against_labelled_example_pairs(self):
        # regression check against the only currently-resolvable labelled "two pages" examples
        import pagexml.parser as parser
        checked = 0
        for pair in test_pairs:
            if not pair['pagexml_path'].exists():
                continue
            if 'two pages' not in pair['description']:
                continue
            scan = parser.parse_pagexml_file(str(pair['pagexml_path']))
            self.assertTrue(is_opening(scan), f"expected opening: {pair['description']}")
            checked += 1
        self.assertGreater(checked, 0, "no resolvable 'two pages' examples found in test_pairs")

    def test_wide_opening_with_no_detectable_gap_needs_corpus_width(self):
        # dense handwriting can bridge the gutter with no empty gap at all -- the old gap-only
        # heuristic misclassified this as a single page; width_ratio against the corpus fixes it
        region = make_text_region('r1', x=20, y=50, w=960, h=900)
        scan = make_scan('opening-no-gap', width=1000, height=1000, regions=[region])
        narrow_scan = make_scan('single-page', width=520, height=1000,
                                regions=[make_text_region('r1', x=20, y=50, w=480, h=900)])
        median_width = compute_corpus_median_width([scan, narrow_scan, scan])

        features = get_opening_features(scan, corpus_median_width=median_width)
        self.assertEqual(0, features.gap_width)  # confirms there really is no detectable gap
        self.assertTrue(features.is_opening)
        self.assertFalse(is_opening(narrow_scan, corpus_median_width=median_width))

    def test_against_full_inventory_ground_truth(self):
        # NL-AsnDA_0114.11_1 (630 scans): only the first and last scans are single pages,
        # everything else is a two-page opening -- a strong real-world regression check
        import pagexml.parser as parser
        inventory_dir = PAGEXML_DIR / 'NL-AsnDA' / 'NL-AsnDA_0114.11' / 'NL-AsnDA_0114.11_1'
        if not inventory_dir.exists():
            self.skipTest(f"inventory data not available at {inventory_dir}")
        xml_paths = sorted(inventory_dir.glob('*.xml'))
        scans = [parser.parse_pagexml_file(str(p)) for p in xml_paths]
        median_width = compute_corpus_median_width(scans)

        non_openings = {s.id for s in scans if not is_opening(s, corpus_median_width=median_width)}
        self.assertEqual({'NL-AsnDA_0114.11_1_0001.jpg', 'NL-AsnDA_0114.11_1_0630.jpg'}, non_openings)


def make_width_height_scans(doc_id_prefix: str, width: float, height: float, count: int,
                            height_jitter: float = 0.0) -> list:
    return [make_scan(f'{doc_id_prefix}-{i}', round(width), round(height + i * height_jitter))
            for i in range(count)]


class TestInventoryStructure(TestCase):

    def test_find_width_groups_merges_close_widths_and_separates_distant_ones(self):
        scans = (make_width_height_scans('a', 1000, 1000, 3, height_jitter=1)
                 + make_width_height_scans('b', 2000, 1000, 2, height_jitter=1))
        groups = find_width_groups(scans, merge_frac=0.08)
        self.assertEqual(2, len(groups))
        self.assertEqual({3, 2}, {g.count for g in groups})

    def test_identify_page_unit_widths_finds_valid_book_pair(self):
        # mimics a real book: a minority single-page/cover group, a majority opening group,
        # roughly double the single-page width, plus a couple of negligible odd outliers
        scans = (make_width_height_scans('single', 5000, 3800, 89)
                 + make_width_height_scans('opening', 7300, 5600, 244)
                 + make_width_height_scans('odd', 10000, 9000, 2))
        groups = find_width_groups(scans)
        pair = identify_page_unit_widths(groups, total=len(scans))
        self.assertIsNotNone(pair)
        single, opening = pair
        self.assertAlmostEqual(5000, single.center)
        self.assertAlmostEqual(7300, opening.center)

    def test_identify_page_unit_widths_returns_none_for_dominant_single_mode_with_tail(self):
        # mimics a folder of mostly single sheets with a long tail of other sizes and no real
        # opening mode: the only width-doubling-ish pair available has the "wider" group as a
        # tiny minority, which a real book's opening group never is
        scans = (make_width_height_scans('dominant', 2500, 3300, 972)
                 + make_width_height_scans('tail_a', 1700, 3300, 22)
                 + make_width_height_scans('tail_b', 3400, 3300, 32))
        groups = find_width_groups(scans)
        pair = identify_page_unit_widths(groups, total=len(scans))
        self.assertIsNone(pair)

    def test_classify_page_types_uses_height_to_catch_odd_shaped_outliers(self):
        scans = (make_width_height_scans('single', 5000, 3800, 5)
                 + make_width_height_scans('opening', 7300, 5600, 8)
                 # opening-width but much taller -- e.g. an oversized fold-out
                 + [make_scan('oversized', 7300, 9500)])
        page_types = classify_page_types(scans, min_share=0.1)
        self.assertTrue(all(page_types[f'single-{i}'] == PAGE_TYPE_SINGLE for i in range(5)))
        self.assertTrue(all(page_types[f'opening-{i}'] == PAGE_TYPE_OPENING for i in range(8)))
        self.assertEqual(PAGE_TYPE_ODD, page_types['oversized'])

    def test_classify_page_types_all_odd_when_no_pair_found(self):
        scans = make_width_height_scans('s', 2500, 3300, 5)
        page_types = classify_page_types(scans)
        self.assertTrue(all(t == PAGE_TYPE_ODD for t in page_types))

    def test_classify_inventory_structure_book_of_openings(self):
        scans = (make_width_height_scans('single', 5000, 3800, 89)
                 + make_width_height_scans('opening', 7300, 5600, 244)
                 + make_width_height_scans('odd', 10000, 9000, 2))
        result = classify_inventory_structure(scans)
        self.assertEqual(STRUCTURE_BOOK_OF_OPENINGS, result.structure_type)
        self.assertEqual(5000, result.single_page_width)
        self.assertEqual(7300, result.opening_width)
        self.assertEqual(2, result.type_counts.get(PAGE_TYPE_ODD, 0))

    def test_classify_inventory_structure_mixed_folder(self):
        scans = (make_width_height_scans('dominant', 2500, 3300, 972)
                 + make_width_height_scans('tail_a', 1700, 3300, 22)
                 + make_width_height_scans('tail_b', 3400, 3300, 32))
        result = classify_inventory_structure(scans)
        self.assertEqual(STRUCTURE_MIXED, result.structure_type)
        self.assertIsNone(result.single_page_width)
        self.assertIsNone(result.opening_width)

    def test_against_six_labelled_real_inventories(self):
        import pagexml.parser as parser
        expectations = [
            (('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_1'), STRUCTURE_BOOK_OF_OPENINGS),
            (('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_85'), STRUCTURE_BOOK_OF_OPENINGS),
            (('NL-HaNA', 'NL-HaNA_2.10.50', 'NL-HaNA_2.10.50_619'), STRUCTURE_BOOK_OF_OPENINGS),
            (('NL-HaNA', 'NL-HaNA_1.05.06', 'NL-HaNA_1.05.06_1'), STRUCTURE_BOOK_OF_OPENINGS),
            (('NL-AsdSAA', 'NL-AsdSAA_89', 'NL-AsdSAA_89_3.1'), STRUCTURE_MIXED),
            (('NL-HaNA', 'NL-HaNA_1.05.06', 'NL-HaNA_1.05.06_2'), STRUCTURE_MIXED),
        ]
        checked = 0
        for (institute, archive, inventory_num), expected in expectations:
            inventory_dir = PAGEXML_DIR / institute / archive / inventory_num
            if not inventory_dir.exists():
                continue
            scans = [parser.parse_pagexml_file(str(p)) for p in inventory_dir.glob('*.xml')]
            result = classify_inventory_structure(scans)
            self.assertEqual(expected, result.structure_type,
                             f"{institute}/{archive}/{inventory_num}: expected {expected}, "
                             f"got {result.structure_type} ({result.type_counts})")
            checked += 1
        self.assertGreater(checked, 0, "none of the 6 labelled inventories were found on disk")

    def test_split_inventory_into_pages(self):
        single_cover = make_scan('cover', width=2500, height=4000,
                                 regions=[make_text_region('r1', x=50, y=50, w=2000, h=900)])
        opening = make_scan('opening', width=5000, height=4000, regions=[
            make_text_region('left', x=50, y=50, w=2000, h=900),
            make_text_region('right', x=2950, y=50, w=2000, h=900),
        ])
        odd = make_scan('fold-out', width=9000, height=4000,
                        regions=[make_text_region('r1', x=50, y=50, w=8000, h=900)])
        scans = [single_cover, opening, odd]
        # construct the structure explicitly rather than via classify_inventory_structure --
        # that classification logic is tested separately above; here we only test that
        # split_inventory_into_pages does the right thing *given* a structure
        structure = InventoryStructure(
            structure_type=STRUCTURE_BOOK_OF_OPENINGS, single_page_width=2500, opening_width=5000,
            page_types=pd.Series({'cover': PAGE_TYPE_SINGLE, 'opening': PAGE_TYPE_OPENING,
                                  'fold-out': PAGE_TYPE_ODD}),
            type_counts={PAGE_TYPE_SINGLE: 1, PAGE_TYPE_OPENING: 1, PAGE_TYPE_ODD: 1},
        )

        pages = split_inventory_into_pages(scans, structure=structure)

        # cover stays a single page, the opening becomes two pages, the odd-shaped scan is kept
        self.assertEqual(['cover', 'opening-verso', 'opening-recto', 'fold-out'],
                         [page.id for page in pages])

    def test_split_inventory_into_pages_can_drop_odd_shaped(self):
        single_cover = make_scan('cover', width=2500, height=4000,
                                 regions=[make_text_region('r1', x=50, y=50, w=2000, h=900)])
        odd = make_scan('fold-out', width=9000, height=4000,
                        regions=[make_text_region('r1', x=50, y=50, w=8000, h=900)])
        structure = InventoryStructure(
            structure_type=STRUCTURE_MIXED, single_page_width=None, opening_width=None,
            page_types=pd.Series({'cover': PAGE_TYPE_SINGLE, 'fold-out': PAGE_TYPE_ODD}),
            type_counts={PAGE_TYPE_SINGLE: 1, PAGE_TYPE_ODD: 1},
        )
        pages = split_inventory_into_pages([single_cover, odd], structure=structure, include_odd_shaped=False)
        self.assertEqual(['cover'], [page.id for page in pages])
