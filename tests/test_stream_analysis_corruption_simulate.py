import csv

from archival_structures.annotation.ground_truth import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    write_ground_truth,
)
from archival_structures.stream_analysis.corruption.schema import read_corrupted_stream
from archival_structures.stream_analysis.corruption.simulate import (
    CASE_NAMES,
    simulate_collection,
    simulate_inventory,
    write_manifest,
)


def _write_populated_gt(path):
    gt = InventoryGroundTruth(
        inventory_id='TEST_1',
        ead_ref={'unitid': 'TEST', 'file_unitid': '1'},
        document_type='resolutions_book',
        layers={
            'section': [
                AnnotationElement(id='sec_001', type='index_page', spans=[
                    Span(scan_id=f'TEST_1_{i:04d}.jpg', page_side='full') for i in range(1, 5)
                ]),
                AnnotationElement(id='sec_002', type='resolution_page', spans=[
                    Span(scan_id=f'TEST_1_{i:04d}.jpg', page_side='full') for i in range(5, 9)
                ]),
            ],
        },
    )
    write_ground_truth(gt, path)
    return gt


def _write_unpopulated_gt(path):
    gt = InventoryGroundTruth(
        inventory_id='TEST_2',
        ead_ref={'unitid': 'TEST', 'file_unitid': '2'},
        document_type='resolutions_book',
        layers={},
    )
    write_ground_truth(gt, path)
    return gt


class TestSimulateInventory:
    def test_writes_one_file_per_combination(self, tmp_path):
        gt_path = tmp_path / 'gt' / '1.yaml'
        _write_populated_gt(gt_path)
        output_dir = tmp_path / 'out'

        results = simulate_inventory(
            gt_path, output_dir,
            cases=('intra_document_swap', 'document_level_swap'),
            rates=(0.5, 1.0),
            seeds=(0, 1),
        )

        assert len(results) == 2 * 2 * 2
        for r in results:
            assert r.inventory_id == 'TEST_1'
            assert r.n_units == 8
            assert r.n_documents == 2
            assert r.output_path.exists()

        # every combination is represented exactly once
        combos = {(r.case, r.rate, r.seed) for r in results}
        assert len(combos) == len(results)

    def test_output_round_trips_and_matches_canonical_membership(self, tmp_path):
        gt_path = tmp_path / 'gt' / '1.yaml'
        _write_populated_gt(gt_path)
        output_dir = tmp_path / 'out'

        [result] = simulate_inventory(
            gt_path, output_dir,
            cases=('document_level_swap',), rates=(1.0,), seeds=(0,),
        )
        cs = read_corrupted_stream(result.output_path)
        assert cs.inventory_id == 'TEST_1'
        assert cs.case == 'document_level_swap'
        assert cs.params == {'rate': 1.0}
        assert cs.seed == 0
        assert len(cs.observed) == 8
        assert {u.document_id for u in cs.observed} == {'sec_001', 'sec_002'}

    def test_skips_inventory_with_unpopulated_layer(self, tmp_path):
        gt_path = tmp_path / 'gt' / '2.yaml'
        _write_unpopulated_gt(gt_path)
        output_dir = tmp_path / 'out'

        results = simulate_inventory(gt_path, output_dir)
        assert results == []
        assert not output_dir.exists()

    def test_default_cases_are_all_four(self, tmp_path):
        gt_path = tmp_path / 'gt' / '1.yaml'
        _write_populated_gt(gt_path)
        output_dir = tmp_path / 'out'

        results = simulate_inventory(gt_path, output_dir, rates=(0.5,), seeds=(0,))
        assert {r.case for r in results} == set(CASE_NAMES)


class TestSimulateCollection:
    def test_sweeps_every_file_and_skips_unpopulated_and_broken_ones(self, tmp_path):
        gt_dir = tmp_path / 'gt'
        gt_dir.mkdir()
        _write_populated_gt(gt_dir / '1.yaml')
        _write_unpopulated_gt(gt_dir / '2.yaml')
        (gt_dir / '3-broken.yaml').write_text('not: a valid ground truth file\n')
        output_dir = tmp_path / 'out'

        results = simulate_collection(
            gt_dir, output_dir,
            cases=('intra_document_swap',), rates=(0.5,), seeds=(0,),
        )

        # only the populated file (TEST_1) contributes results
        assert {r.inventory_id for r in results} == {'TEST_1'}
        assert len(results) == 1


class TestWriteManifest:
    def test_writes_expected_header_and_rows(self, tmp_path):
        gt_path = tmp_path / 'gt' / '1.yaml'
        _write_populated_gt(gt_path)
        output_dir = tmp_path / 'out'
        results = simulate_inventory(
            gt_path, output_dir,
            cases=('intra_document_swap',), rates=(0.5,), seeds=(0,),
        )

        manifest_path = tmp_path / 'manifest.csv'
        write_manifest(results, manifest_path)

        with open(manifest_path, newline='', encoding='utf-8') as fh:
            rows = list(csv.reader(fh))

        assert rows[0] == [
            'inventory_id', 'case', 'rate', 'seed',
            'n_units', 'n_documents', 'n_touched', 'output_path',
        ]
        assert len(rows) == 2  # header + one result
        assert rows[1][0] == 'TEST_1'
        assert rows[1][1] == 'intra_document_swap'
