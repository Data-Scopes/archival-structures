import yaml

from archival_structures.annotation.ground_truth import (
    AnnotationElement,
    InventoryGroundTruth,
    Span,
    read_ground_truth,
    write_ground_truth,
)


def _gt(**kwargs) -> InventoryGroundTruth:
    return InventoryGroundTruth(
        inventory_id='NL-HaNA_1.04.02_1068',
        ead_ref={'unitid': 'NL-HaNA_1.04.02', 'file_unitid': '1068'},
        document_type='unspecified',
        layers={'document': [AnnotationElement(
            id='doc_001', type='generale_missive',
            spans=[Span('NL-HaNA_1.04.02_1068_0611.jpg', page_side='full')],
            metadata={'date': '1618-10-05'},
        )]},
        **kwargs,
    )


class TestRoundTrip:
    def test_legacy_file_without_provenance_reads_and_writes_unchanged(self, tmp_path):
        path = tmp_path / 'legacy.yaml'
        write_ground_truth(_gt(), path)
        assert 'provenance' not in yaml.safe_load(path.read_text())
        gt = read_ground_truth(path)
        assert gt.provenance == {} and gt.extra == {}
        assert gt.layers['document'][0].metadata == {'date': '1618-10-05'}

    def test_layer_provenance_round_trips(self, tmp_path):
        provenance = {'document': {
            'source': 'doi:10.34894/SRDMFU', 'method': 'aligned_from_index',
            'coverage': 'partial', 'parameters': {'end_scan_field': 'Eindscan'},
        }}
        path = tmp_path / 'gt.yaml'
        write_ground_truth(_gt(provenance=provenance), path)
        assert read_ground_truth(path).provenance == provenance

    def test_unknown_top_level_keys_are_kept(self, tmp_path):
        path = tmp_path / 'gt.yaml'
        write_ground_truth(_gt(extra={'converter': 'convert_generale_missiven'}), path)
        assert read_ground_truth(path).extra == {'converter': 'convert_generale_missiven'}

    def test_element_level_provenance_override_is_kept_as_metadata(self, tmp_path):
        gt = _gt()
        gt.layers['document'][0].metadata['provenance'] = {'method': 'manual'}
        path = tmp_path / 'gt.yaml'
        write_ground_truth(gt, path)
        element = read_ground_truth(path).layers['document'][0]
        assert element.metadata['provenance'] == {'method': 'manual'}


class TestLayerProvenance:
    def test_defaults_to_manual_and_complete(self):
        assert _gt().layer_provenance('document') == {'method': 'manual', 'coverage': 'complete'}

    def test_recorded_values_override_defaults_and_extra_keys_are_kept(self):
        gt = _gt(provenance={'document': {'coverage': 'partial', 'source': 'x'}})
        assert gt.layer_provenance('document') == {
            'method': 'manual', 'coverage': 'partial', 'source': 'x',
        }
