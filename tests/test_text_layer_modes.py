"""Tests for the volume-mode (v4) text layer (step 15v). Fixture: tests/fixtures/text_layer/sg/9999."""

import dataclasses
import json
import shutil
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from archival_structures.text_layer import build, line_split as ls
from archival_structures.text_layer.extract_v2 import extract_inventory_v2
from archival_structures.text_layer.io import read_tsv_gz
from archival_structures.text_layer.modes import (
    MODE_REGION_COLUMNS, MODE_SCAN_COLUMNS, column_start_share, decide_mode, extract_inventory_v2_modes, read_mode)
from archival_structures.text_layer.presets import (
    BOX_HEIGHT_V4, MODE_RULE_FIELDS, PRESETS, SPLIT_GATE_V4, inventory_preset)

FIXTURE = Path(__file__).parent / "fixtures" / "text_layer" / "sg" / "9999"
NAME = "NL-HaNA_1.01.02_9999"
V4 = ("sg_1.01.02_v4", "sg_1.10.94_v4")

# asdict of the v1-v3 presets, written from the code before step 15v edited presets.py
FROZEN_PRESETS = {'voc_v1': {'name': 'voc_text_layer_v1',
            'untagged_tag': 'unclassified',
            'scan_id_prefix': 'NL-HaNA_1.04.02',
            'throughput_fit': {'extract': {'intercept_s': 0.4359, 's_per_mb': 0.09122},
                               'wbc': {'intercept_s': 0.0046, 's_per_mb': 0.00382},
                               'join': {'intercept_s': 0.0, 's_per_mb': 0.00977}},
            'output_bytes_per_xml_byte': 0.0233,
            'description': 'GLOBALISE VOC PageXML v2.1; throughput fitted on one 10-core laptop, report/74 '
                           'section 6',
            'format_version': 1,
            'archive_id': None,
            'source_pattern': None,
            'skip_resource_forks': False,
            'merged_line_split': 'off',
            'split_region_tags': (),
            'word_split': 'off',
            'word_split_params': None,
            'layout_params': None,
            'types_params': None,
            'unit_params': None,
            'word_break_rule': None,
            'word_break_stat': 'default',
            'wbc_sanity_in': ('¬',),
            'wbc_sanity_out': (':', '.'),
            'wbc_pinned': None,
            'join_lead_marks': '',
            'join_lead_marks_lower': None,
            'join_upper_guard': False},
 'sg_1.01.02_v2': {'name': 'NL-HaNA_1.01.02_text_layer_v2',
                   'untagged_tag': 'untagged',
                   'scan_id_prefix': 'NL-HaNA_1.01.02',
                   'throughput_fit': None,
                   'output_bytes_per_xml_byte': None,
                   'description': 'States General hand volumes (NL-HaNA 1.01.02), text layer v2 (lines with '
                                  'geometry)',
                   'format_version': 2,
                   'archive_id': 'NL-HaNA_1.01.02',
                   'source_pattern': 'NL-HaNA_1\\.01\\.02_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\\.zip|\\.tar\\.gz|\\.tgz)?',
                   'skip_resource_forks': True,
                   'merged_line_split': 'off',
                   'split_region_tags': (),
                   'word_split': 'off',
                   'word_split_params': {'min_token_len': 8, 'min_part_count': 20, 'split_penalty': 0},
                   'layout_params': {'furniture_patterns': ('\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\d{1,4}\\s*\\W*',
                                                            '\\W*(den\\s+)?\\d{1,2}\\s*\\.?\\s*(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|sep|sept|oct|okt|nov|dec)\\w*\\.?\\W*(\\d{4}\\W*)?',
                                                            '\\W*1[67]\\d\\d\\W*'),
                                     'merge': False},
                   'types_params': {'pn_line': '\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\(?\\s*\\d{1,4}\\s*[.)]?\\W*',
                                    'pn_suffix': '\\s\\(?\\s*(\\d{1,4})\\s*[.)]?\\s*$',
                                    'date_line': '\\W*((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|solis|sond|zond)\\w*\\.?,?\\s+)?(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:,]?\\W*(1[67]\\d\\d\\W*)?',
                                    'date_prefix': '\\W*(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?(?=\\s+\\S)',
                                    'month_only': '\\W*\\d{0,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?\\W*',
                                    'year_line': '\\W*1[67]\\d\\d\\W*',
                                    'signature': '[A-Z]{1,3}\\s*\\d{0,2}|\\d\\s*[A-Z]{1,3}',
                                    'offchain_retype': True,
                                    'offchain_other_bands': (),
                                    'offchain_years_other': True},
                   'unit_params': None,
                   'word_break_rule': None,
                   'word_break_stat': 'default',
                   'wbc_sanity_in': (),
                   'wbc_sanity_out': (':', '.'),
                   'wbc_pinned': {'default': '-=„',
                                  '11281': '-:=„',
                                  '3349': '-„',
                                  '3350': '-„',
                                  '3351': '-„',
                                  '3353': '-„',
                                  '3354': '-„'},
                   'join_lead_marks': ',',
                   'join_lead_marks_lower': None,
                   'join_upper_guard': False},
 'sg_1.10.94_v2': {'name': 'NL-HaNA_1.10.94_text_layer_v2',
                   'untagged_tag': 'untagged',
                   'scan_id_prefix': 'NL-HaNA_1.10.94',
                   'throughput_fit': None,
                   'output_bytes_per_xml_byte': None,
                   'description': 'States General print volumes (NL-HaNA 1.10.94), text layer v2 (lines with '
                                  'geometry)',
                   'format_version': 2,
                   'archive_id': 'NL-HaNA_1.10.94',
                   'source_pattern': 'NL-HaNA_1\\.10\\.94_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\\.zip|\\.tar\\.gz|\\.tgz)?',
                   'skip_resource_forks': True,
                   'merged_line_split': 'on',
                   'split_region_tags': ('index', 'resolution', 'untagged', 'Text'),
                   'word_split': 'off',
                   'word_split_params': {'min_token_len': 8, 'min_part_count': 20, 'split_penalty': 0},
                   'layout_params': {'furniture_patterns': ('\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\d{1,4}\\s*\\W*',
                                                            '\\W*(den\\s+)?\\d{1,2}\\s*\\.?\\s*(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|sep|sept|oct|okt|nov|dec)\\w*\\.?\\W*(\\d{4}\\W*)?',
                                                            '\\W*1[67]\\d\\d\\W*'),
                                     'sort_unchanged_lines': True},
                   'types_params': {'pn_line': '\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\(?\\s*\\d{1,4}\\s*[.)]?\\W*',
                                    'pn_suffix': '\\s\\(?\\s*(\\d{1,4})\\s*[.)]?\\s*$',
                                    'date_line': '\\W*((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|solis|sond|zond)\\w*\\.?,?\\s+)?(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:,]?\\W*(1[67]\\d\\d\\W*)?',
                                    'date_prefix': '\\W*(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?(?=\\s+\\S)',
                                    'month_only': '\\W*\\d{0,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?\\W*',
                                    'year_line': '\\W*1[67]\\d\\d\\W*',
                                    'signature': '[A-Z]{1,3}\\s*\\d{0,2}|\\d\\s*[A-Z]{1,3}',
                                    'column_override': True,
                                    'offchain_retype': True,
                                    'offchain_other_bands': ('bottom',),
                                    'offchain_years_other': True},
                   'unit_params': {'indent_frac': 0.5, 'short_frac': 0.1, 'gap_factor': None},
                   'word_break_rule': None,
                   'word_break_stat': 'default',
                   'wbc_sanity_in': (),
                   'wbc_sanity_out': (':', '.'),
                   'wbc_pinned': {'default': '-=„'},
                   'join_lead_marks': '',
                   'join_lead_marks_lower': {'455': 'J'},
                   'join_upper_guard': False},
 'sg_1.01.02_v3': {'name': 'NL-HaNA_1.01.02_text_layer_v3',
                   'untagged_tag': 'untagged',
                   'scan_id_prefix': 'NL-HaNA_1.01.02',
                   'throughput_fit': None,
                   'output_bytes_per_xml_byte': None,
                   'description': 'States General hand volumes (NL-HaNA 1.01.02), text layer v3 (lines with '
                                  'geometry)',
                   'format_version': 2,
                   'archive_id': 'NL-HaNA_1.01.02',
                   'source_pattern': 'NL-HaNA_1\\.01\\.02_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\\.zip|\\.tar\\.gz|\\.tgz)?',
                   'skip_resource_forks': True,
                   'merged_line_split': 'off',
                   'split_region_tags': (),
                   'word_split': 'off',
                   'word_split_params': {'min_token_len': 8, 'min_part_count': 20, 'split_penalty': 0},
                   'layout_params': {'furniture_patterns': ('\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\d{1,4}\\s*\\W*',
                                                            '\\W*(den\\s+)?\\d{1,2}\\s*\\.?\\s*(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|sep|sept|oct|okt|nov|dec)\\w*\\.?\\W*(\\d{4}\\W*)?',
                                                            '\\W*1[67]\\d\\d\\W*'),
                                     'merge': False},
                   'types_params': {'pn_line': '\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\(?\\s*\\d{1,4}\\s*[.)]?\\W*',
                                    'pn_suffix': '\\s\\(?\\s*(\\d{1,4})\\s*[.)]?\\s*$',
                                    'date_line': '\\W*((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|solis|sond|zond)\\w*\\.?,?\\s+)?(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:,]?\\W*(1[67]\\d\\d\\W*)?',
                                    'date_prefix': '\\W*(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?(?=\\s+\\S)',
                                    'month_only': '\\W*\\d{0,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?\\W*',
                                    'year_line': '\\W*1[67]\\d\\d\\W*',
                                    'signature': '[A-Z]{1,3}\\s*\\d{0,2}|\\d\\s*[A-Z]{1,3}',
                                    'offchain_retype': True,
                                    'offchain_other_bands': (),
                                    'offchain_years_other': True},
                   'unit_params': None,
                   'word_break_rule': None,
                   'word_break_stat': 'default',
                   'wbc_sanity_in': (),
                   'wbc_sanity_out': (':', '.'),
                   'wbc_pinned': {'default': '-=„',
                                  '11281': '-:=„',
                                  '3349': '-„',
                                  '3350': '-„',
                                  '3351': '-„',
                                  '3353': '-„',
                                  '3354': '-„'},
                   'join_lead_marks': ',',
                   'join_lead_marks_lower': None,
                   'join_upper_guard': False},
 'sg_1.10.94_v3': {'name': 'NL-HaNA_1.10.94_text_layer_v3',
                   'untagged_tag': 'untagged',
                   'scan_id_prefix': 'NL-HaNA_1.10.94',
                   'throughput_fit': None,
                   'output_bytes_per_xml_byte': None,
                   'description': 'States General print volumes (NL-HaNA 1.10.94), text layer v3 (lines with '
                                  'geometry)',
                   'format_version': 2,
                   'archive_id': 'NL-HaNA_1.10.94',
                   'source_pattern': 'NL-HaNA_1\\.10\\.94_(?P<inv>[0-9]+[A-Z]*)(?:-pagexml)?(?:\\.zip|\\.tar\\.gz|\\.tgz)?',
                   'skip_resource_forks': True,
                   'merged_line_split': 'on',
                   'split_region_tags': ('index', 'resolution', 'untagged', 'Text'),
                   'word_split': 'off',
                   'word_split_params': {'min_token_len': 8, 'min_part_count': 20, 'split_penalty': 0},
                   'layout_params': {'furniture_patterns': ('\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\d{1,4}\\s*\\W*',
                                                            '\\W*(den\\s+)?\\d{1,2}\\s*\\.?\\s*(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|sep|sept|oct|okt|nov|dec)\\w*\\.?\\W*(\\d{4}\\W*)?',
                                                            '\\W*1[67]\\d\\d\\W*'),
                                     'sort_unchanged_lines': True},
                   'types_params': {'pn_line': '\\W*(fol\\.?|pag\\.?|p\\.)?\\s*\\(?\\s*\\d{1,4}\\s*[.)]?\\W*',
                                    'pn_suffix': '\\s\\(?\\s*(\\d{1,4})\\s*[.)]?\\s*$',
                                    'date_line': '\\W*((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|solis|sond|zond)\\w*\\.?,?\\s+)?(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:,]?\\W*(1[67]\\d\\d\\W*)?',
                                    'date_prefix': '\\W*(den\\s+)?\\d{1,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?(?=\\s+\\S)',
                                    'month_only': '\\W*\\d{0,2}\\s*[.:]?\\s*(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|oct|okt|nov|dec)\\w*\\s*[.:]?\\W*',
                                    'year_line': '\\W*1[67]\\d\\d\\W*',
                                    'signature': '[A-Z]{1,3}\\s*\\d{0,2}|\\d\\s*[A-Z]{1,3}',
                                    'column_override': True,
                                    'offchain_retype': True,
                                    'offchain_other_bands': ('bottom',),
                                    'offchain_years_other': True},
                   'unit_params': {'indent_frac': 0.5, 'short_frac': 0.1, 'gap_factor': None},
                   'word_break_rule': None,
                   'word_break_stat': 'default',
                   'wbc_sanity_in': (),
                   'wbc_sanity_out': (':', '.'),
                   'wbc_pinned': {'default': '-=„'},
                   'join_lead_marks': '',
                   'join_lead_marks_lower': {'455': 'J'},
                   'join_upper_guard': False}}

# fit_split_model(...).to_dict() on the fixture with sg_1.10.94_v3, frozen before the prepass_rows refactor
FROZEN_SPLIT_MODEL = json.loads('''{"finder": {"n_bigram_types": 3, "n_docs": 2, "n_tokens": 5, "n_word_types": 5}, "min_uni_freq": 100, "n_candidates_other_tags_fit": {}, "params": {"left": null, "right": null}, "scorer": "notebook PMI scorer (Splitting-Fagel-Merged-Lines.ipynb cell 32)", "split_region_tags": ["index", "resolution", "untagged", "Text"]}''')


def _rows(h, region="r1", scan="s1", n=4, chars=40):
    return [{"region_id": region, "scan_id": scan, "h": h, "baseline": f"0,{100 * i} 50,{100 * i}",
             "text_raw": "a" * chars} for i in range(n)]


@pytest.fixture
def src(tmp_path):
    d = tmp_path / "src" / NAME
    shutil.copytree(FIXTURE, d)
    return d


def test_v1_v3_presets_unchanged():
    for key, frozen in FROZEN_PRESETS.items():
        now = asdict(PRESETS[key])
        assert now.pop("mode_presets") is None and now.pop("volume_mode") is None
        assert now.pop("mode_overrides") is None
        assert now == frozen, key


def test_inventory_preset():
    v3h, v3p = PRESETS["sg_1.01.02_v3"], PRESETS["sg_1.10.94_v3"]
    for key in V4:
        v4 = PRESETS[key]
        assert v4.mode_presets == {"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}
        assert v4.volume_mode["params"] == {"cut": 87.75, "min_region_chars": 100} == BOX_HEIGHT_V4
        for mode, v3 in (("hand", v3h), ("print", v3p)):
            r, a, b = asdict(inventory_preset(v4, mode)), asdict(v3), asdict(v4)
            for f in MODE_RULE_FIELDS:
                if (key, mode, f) == ("sg_1.01.02_v4", "print", "unit_params"):
                    assert r[f] == {**a[f], "drop_cap_lines": 2}   # step 15x override
                else:
                    assert r[f] == a[f]
            others = [f for f in b if f not in MODE_RULE_FIELDS
                      and f not in ("mode_presets", "volume_mode", "mode_overrides")]
            assert all(r[f] == b[f] for f in others)
            assert r["mode_presets"] is None and r["volume_mode"] is None and r["mode_overrides"] is None
    h = asdict(inventory_preset(PRESETS["sg_1.01.02_v4"], "hand"))
    a = asdict(v3h)
    assert {f for f in h if h[f] != a[f]} == {"name", "description"}
    assert PRESETS["sg_1.10.94_v4"].volume_mode["method"] == "fixed"
    assert PRESETS["sg_1.10.94_v4"].volume_mode["mode"] == "print"
    assert PRESETS["sg_1.01.02_v4"].volume_mode["method"] == "box_height"
    assert inventory_preset(v3h, "print") is v3h
    with pytest.raises(ValueError):
        inventory_preset(PRESETS["sg_1.01.02_v4"], "mixed")


def test_inventory_preset_overrides():
    from archival_structures.text_layer.presets import SG_PRINT_UNIT_PARAMS
    v4 = PRESETS["sg_1.01.02_v4"]
    assert inventory_preset(v4, "print").unit_params == {**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}
    assert inventory_preset(v4, "hand").unit_params == PRESETS["sg_1.01.02_v3"].unit_params
    assert PRESETS["sg_1.10.94_v4"].mode_overrides is None
    assert (inventory_preset(PRESETS["sg_1.10.94_v4"], "print").unit_params
            == PRESETS["sg_1.10.94_v3"].unit_params)
    bad = replace(v4, mode_overrides={"print": {"name": "x"}})
    with pytest.raises(ValueError):
        inventory_preset(bad, "print")


def test_prepass_rows_matches_fit(src):
    preset = PRESETS["sg_1.10.94_v3"]
    assert ls.fit_split_model("9999", src, preset).to_dict() == FROZEN_SPLIT_MODEL
    rows, tags, info = ls.prepass_rows("9999", src, preset)
    assert info["n_scans"] == 4 and info["n_parse_fail"] == 0 and len(info["scans"]) == 4
    assert set(info["scans"][0]) == {"scan_id", "page_w", "page_h", "n_lines_text"}
    assert rows and set(tags) and all("h" in r and "baseline" in r for r in rows)  # raw rows


def test_decide_mode():
    p = PRESETS["sg_1.01.02_v4"]
    r = decide_mode(_rows(87.75), p)
    assert (r["mode"], r["method"], r["value"]) == ("print", "box_height", 87.75)
    assert decide_mode(_rows(88), p)["mode"] == "hand"
    r = decide_mode(_rows(88, n=2), p)   # no qualifying region
    assert (r["mode"], r["method"], r["value"]) == ("hand", "default", None)
    r = decide_mode(_rows(120), PRESETS["sg_1.10.94_v4"])
    assert (r["mode"], r["method"]) == ("print", "fixed")
    assert (r["box_height_mode"], r["box_height_method"]) == ("hand", "box_height")
    assert r["scans"][0]["scan_id"] == "s1" and r["regions"][0]["region_id"] == "r1"


def _fixed(m):
    return replace(PRESETS["sg_1.01.02_v4"],
                   volume_mode={"method": "fixed", "mode": m, "params": BOX_HEIGHT_V4})


def test_extract_modes_hand_and_print(src, tmp_path):
    ref = tmp_path / "ref"
    extract_inventory_v2("9999", src, ref, PRESETS["sg_1.01.02_v3"])
    out = tmp_path / "hand"
    res = extract_inventory_v2_modes("9999", src, out, _fixed("hand"))
    assert res["mode"] == "hand" and res["mode_method"] == "fixed"
    assert not (out / "split_model").exists()
    assert list(read_tsv_gz(out / "lines" / "9999.lines.tsv.gz")) == \
        list(read_tsv_gz(ref / "lines" / "9999.lines.tsv.gz"))
    assert read_mode(out, "9999") == "hand"
    meta = json.loads((out / "mode" / "9999.json").read_text())
    assert {"mode", "method", "value", "margin", "n_chars", "n_regions", "params", "inv", "n_scans",
            "n_parse_fail", "n_scans_sided", "scan_minority_share", "box_height_mode",
            "box_height_method"} <= set(meta)
    assert meta["n_scans"] == 4 and "scans" not in meta and "regions" not in meta
    scans = list(read_tsv_gz(out / "mode" / "scans" / "9999.tsv.gz"))
    assert list(scans[0]) == MODE_SCAN_COLUMNS and len(scans) == 4
    regions = list(read_tsv_gz(out / "mode" / "regions" / "9999.tsv.gz"))
    assert not regions or list(regions[0]) == MODE_REGION_COLUMNS
    outp = tmp_path / "print"
    res = extract_inventory_v2_modes("9999", src, outp, _fixed("print"))
    assert res["mode"] == "print" and "split" in res
    assert (outp / "split_model" / "9999.json").exists()
    assert (outp / "split_stats" / "9999.json").exists()


def test_build_v4_all_on_fixture(src, tmp_path):
    out = tmp_path / "out"
    args = ["--pagexml-dir", str(src.parent), "--out-dir", str(out), "--preset", "sg_1.01.02_v4"]
    assert build.main(args + ["--stage", "all", "--workers", "1"]) == 0
    layer = out / "NL-HaNA_1.01.02_text_layer_v4"
    mode = json.loads((layer / "mode" / "9999.json").read_text())["mode"]
    for stage in ("layout", "types", "units"):
        assert set(json.loads((layer / stage / "params.json").read_text())) >= {"hand", "print"}
    assert (layer / "join" / "units" / "9999.tsv.gz").exists()
    assert read_mode(layer, "9999") == mode
    assert (layer / "split_model" / "9999.json").exists() == (mode == "print")


def test_read_mode_missing(tmp_path):
    with pytest.raises(FileNotFoundError) as e:
        read_mode(tmp_path, "9999")
    assert "9999.json" in str(e.value) and "extract stage" in str(e.value)


def _grows(n, x, w, side="left", chars=30):
    return [{"page_side": side, "x": x, "w": w, "text_raw": "a" * chars} for _ in range(n)]


def test_column_start_share():
    one = _grows(60, 100, 1600)
    assert column_start_share(one) == (0.0, 60)
    two = _grows(30, 100, 780, "right") + _grows(30, 900, 780, "right")
    share, n = column_start_share(two)
    assert n == 60 and abs(share - 0.5) < 0.02
    assert column_start_share(_grows(10, 100, 780)) == (None, 10)
    noisy = one + _grows(20, "", "") + _grows(20, 900, 780, chars=5)
    assert column_start_share(noisy) == (0.0, 60)


def test_extract_split_gate(src, tmp_path):
    def gated(min_share):
        return replace(PRESETS["sg_1.01.02_v4"],
                       volume_mode={"method": "fixed", "mode": "print", "params": BOX_HEIGHT_V4,
                                    "split_gate": {**SPLIT_GATE_V4, "min_rows": 1, "min_chars": 1, "min_share": min_share}})
    out = tmp_path / "off"
    extract_inventory_v2_modes("9999", src, out, gated(1.1))
    assert not (out / "split_model" / "9999.json").exists()
    assert json.loads((out / "mode" / "9999.json").read_text())["split_gate"]["split"] is False
    assert all("split_from" not in r or not r["split_from"]
               for r in read_tsv_gz(out / "lines" / "9999.lines.tsv.gz"))
    out = tmp_path / "on"
    extract_inventory_v2_modes("9999", src, out, gated(0.0))
    assert (out / "split_model" / "9999.json").exists()
    assert json.loads((out / "mode" / "9999.json").read_text())["split_gate"]["split"] is True
    # 15wr review: a share that cannot be measured keeps the splitter, whatever min_share says
    out = tmp_path / "unmeasured"
    preset = gated(1.1)
    preset = replace(preset, volume_mode={**preset.volume_mode,
                                          "split_gate": {**preset.volume_mode["split_gate"], "min_chars": 10 ** 6}})
    extract_inventory_v2_modes("9999", src, out, preset)
    assert (out / "split_model" / "9999.json").exists()
    assert json.loads((out / "mode" / "9999.json").read_text())["split_gate"] == {
        "share": None, "n_rows": 0, "min_share": 1.1, "split": True}


# ---- step 15y: pins, in-place rebuild


def test_decide_mode_pins():
    p = PRESETS["sg_1.01.02_v4"]
    pinned = replace(p, volume_mode={**p.volume_mode, "pins": {"9999": "hand"}})
    r = decide_mode(_rows(80), pinned, inv="9999")
    assert (r["mode"], r["method"]) == ("hand", "pinned")
    assert (r["box_height_mode"], r["box_height_method"]) == ("print", "box_height")
    assert r["value"] == 80
    other = decide_mode(_rows(80), pinned, inv="1234")
    assert (other["mode"], other["method"]) == ("print", "box_height") and "box_height_mode" not in other
    assert decide_mode(_rows(80), pinned)["method"] == "box_height"           # inv=None: unpinned
    assert decide_mode(_rows(80), p, inv="9999")["method"] == "box_height"    # no pins key
    pin_print = replace(p, volume_mode={**p.volume_mode, "pins": {"9999": "print"}})
    r = decide_mode(_rows(120), pin_print, inv="9999")
    assert (r["mode"], r["method"], r["box_height_mode"]) == ("print", "pinned", "hand")
    fixed = replace(PRESETS["sg_1.10.94_v4"],
                    volume_mode={**PRESETS["sg_1.10.94_v4"].volume_mode, "pins": {"9999": "hand"}})
    r = decide_mode(_rows(120), fixed, inv="9999")   # the measured result, also under `fixed`
    assert (r["mode"], r["method"], r["box_height_mode"], r["box_height_method"]) == \
        ("hand", "pinned", "hand", "box_height")
    bad = replace(p, volume_mode={**p.volume_mode, "pins": {"9999": "mixed"}})
    with pytest.raises(ValueError):
        decide_mode(_rows(80), bad, inv="9999")


def test_v4_pins():
    hand = ["12297", "3433", "3436", "3439", "3442", "3445", "3463", "3466", "3469", "3472", "3475", "3489", "3726",
            "4792"]
    assert PRESETS["sg_1.01.02_v4"].volume_mode["pins"] == {**{i: "hand" for i in hand}, "12163": "print",
                                                           "12165": "print"}
    assert "pins" not in PRESETS["sg_1.10.94_v4"].volume_mode


def _same_files(root_a, root_b, prefix):
    """{relative path: decompressed content} of the files named `<prefix>.*`; JSON without seconds/elapsed*."""
    import gzip

    def read(root):
        out = {}
        for p in Path(root).rglob(f"{prefix}.*"):
            raw = gzip.open(p, "rb").read() if p.suffix == ".gz" else p.read_bytes()
            if p.name.endswith(".json"):
                raw = json.dumps({k: v for k, v in json.loads(raw).items()
                                  if k != "seconds" and not k.startswith("elapsed")}, sort_keys=True).encode()
            out[str(p.relative_to(root))] = raw
        return out
    return read(root_a), read(root_b)


def test_rebuild_pinned_in_place(src, tmp_path, monkeypatch):
    base = PRESETS["sg_1.01.02_v4"]
    gate = {**SPLIT_GATE_V4, "min_rows": 1, "min_chars": 1, "min_share": 0.0}

    def preset(pin):
        return replace(base, volume_mode={**base.volume_mode, "split_gate": gate, "pins": {"9999": pin}})
    args = ["--pagexml-dir", str(src.parent), "--preset", "sg_1.01.02_v4", "--workers", "1"]
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset("print"))
    out = tmp_path / "inplace"
    assert build.main(args + ["--out-dir", str(out), "--stage", "all"]) == 0
    layer = out / "NL-HaNA_1.01.02_text_layer_v4"
    assert (layer / "split_model" / "9999.json").exists() and (layer / "split_stats" / "9999.json").exists()
    monkeypatch.setitem(PRESETS, "sg_1.01.02_v4", preset("hand"))
    assert build.main(args + ["--out-dir", str(out), "--stage", "extract", "--force", "--inventories", "9999"]) == 0
    assert not (layer / "split_model" / "9999.json").exists()
    assert not (layer / "split_stats" / "9999.json").exists()
    assert build.main(args + ["--out-dir", str(out), "--stage", "all"]) == 0
    meta = json.loads((layer / "mode" / "9999.json").read_text())
    assert (meta["mode"], meta["method"]) == ("hand", "pinned")
    fresh = tmp_path / "fresh"
    assert build.main(args + ["--out-dir", str(fresh), "--stage", "all"]) == 0
    a, b = _same_files(layer, fresh / "NL-HaNA_1.01.02_text_layer_v4", "9999")
    assert a.keys() == b.keys() and len(a) > 10
    assert all(a[k] == b[k] for k in a), [k for k in a if a[k] != b[k]]


def test_extract_seconds_include_prepass(src, tmp_path):
    from archival_structures.text_layer.modes import extract_inventory_v2_modes
    result = extract_inventory_v2_modes("9999", src, tmp_path / "out", PRESETS["sg_1.01.02_v4"])
    assert result["seconds"] >= result["prepass_seconds"] > 0
