"""Tests for archival_structures.text_layer.provenance and presets (step 15)."""

import importlib.metadata

import pytest

from archival_structures.text_layer import presets
from archival_structures.text_layer import provenance as prov


def test_check_pagexml_tools_version_uses_minimum():
    assert prov.MIN_PAGEXML_TOOLS == (0, 9, 0)
    assert prov.check_pagexml_tools_version()


def test_pagexml_tools_is_at_least_0_9_0():
    assert prov.check_min_version("pagexml-tools", (0, 9, 0))


def test_package_info_pagexml_tools_version():
    info = prov.package_info("pagexml-tools", "pagexml")
    assert info["version"] == importlib.metadata.version("pagexml-tools")
    assert info == prov.pagexml_tools_info()


def test_package_info_archival_structures_has_git_head():
    info = prov.package_info("archival-structures", "archival_structures")
    assert info["git_head"]
    assert isinstance(info["git_dirty_files"], list)


def test_package_info_module_name_defaults_from_dist_name():
    info = prov.package_info("archival-structures")
    assert info["path"].endswith("archival_structures")


def test_check_min_version():
    assert prov.check_min_version("pagexml-tools", (0, 1, 0))
    with pytest.raises(AssertionError):
        prov.check_min_version("pagexml-tools", (999, 0, 0))


def test_predict_wall_matches_hand_computation():
    fit = {"extract": {"intercept_s": 1.0, "s_per_mb": 2.0},
           "wbc": {"intercept_s": 0.0, "s_per_mb": 1.0},
           "join": {"intercept_s": 0.5, "s_per_mb": 0.0}}
    # 4 jobs of 1 MB on 2 workers: each stage runs 2 rounds
    # extract 2 * (1 + 2) = 6; wbc 2 * 1 = 2; join 2 * 0.5 = 1
    assert presets.predict_wall([1e6] * 4, 2, fit) == pytest.approx(9.0)
    assert presets.predict_wall([1e6] * 4, 2, fit, stages=("extract",)) == pytest.approx(6.0)
    assert presets.predict_seconds(1e6, "wbc", fit) == pytest.approx(1.0)


def test_voc_preset():
    p = presets.PRESETS["voc_v1"]
    assert p is presets.VOC_V1
    assert p.name == "voc_text_layer_v1" and p.untagged_tag == "unclassified"
    assert p.scan_id_prefix == "NL-HaNA_1.04.02"
    assert presets.predict_seconds(0, "join", p.throughput_fit) == 0.0


def test_v3_presets_equal_v2_but_name():
    import dataclasses
    for hv, v2, v3 in (("hand", "sg_1.01.02_v2", "sg_1.01.02_v3"),
                       ("print", "sg_1.10.94_v2", "sg_1.10.94_v3")):
        a = dataclasses.asdict(presets.PRESETS[v2])
        b = dataclasses.asdict(presets.PRESETS[v3])
        diff = {k for k in a if a[k] != b[k]}
        assert diff == {"name", "description"}, (hv, diff)
        assert b["name"].endswith("_text_layer_v3")
        assert "v3" in b["description"] and "v2" not in b["description"]
