"""Step 22a (phase 16): the VOC preset is the SG v5 preset with only the archive plumbing changed."""
import re

import pytest

from archival_structures.text_layer import io
from archival_structures.text_layer.presets import (
    MODE_RULE_FIELDS, PRESETS, SG_1_01_02_V5, VOC_1_04_02_V5SG, VOC_SOURCE_PATTERN, inventory_preset)


def test_source_pattern_matches():
    m = re.fullmatch(VOC_SOURCE_PATTERN, "1068.zip")
    assert m and m.group("inv") == "1068"
    m = re.fullmatch(VOC_SOURCE_PATTERN, "1457A.zip")
    assert m and m.group("inv") == "1457A"
    for bad in ("NL-HaNA_1.04.02_1068.zip", "1068.tar.gz", "1068"):
        assert re.fullmatch(VOC_SOURCE_PATTERN, bad) is None


def test_discover(tmp_path):
    for n in ("1068.zip", "1457A.zip", "._1068.zip", "notes.txt"):
        (tmp_path / n).write_bytes(b"")
    found = io.discover_inventories_v2(tmp_path, VOC_SOURCE_PATTERN)
    assert set(found) == {"1068", "1457A"}


def test_preset_fields():
    assert PRESETS["voc_1.04.02_v5sg"] is VOC_1_04_02_V5SG
    p = VOC_1_04_02_V5SG
    assert p.archive_id == "NL-HaNA_1.04.02"
    assert p.format_version == 2
    assert p.skip_resource_forks is True
    assert p.wbc_pinned == {"default": "-=„"}
    assert "pins" not in p.volume_mode
    assert p.volume_mode["params"]["cut"] == 87.75
    assert p.name.endswith("_v5")
    assert p.join_lead_marks_lower is None


@pytest.mark.parametrize("mode", ["hand", "print"])
def test_rules_unchanged(mode):
    a = inventory_preset(VOC_1_04_02_V5SG, mode)
    b = inventory_preset(SG_1_01_02_V5, mode)
    for f in MODE_RULE_FIELDS:
        assert getattr(a, f) == getattr(b, f), f
