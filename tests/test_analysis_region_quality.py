"""Tests for ``archival_structures.analysis.region_quality`` (phase 08, step 16)."""

from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd
import pytest

import archival_structures.analysis.region_quality as rq
from archival_structures.analysis.region_quality import (
    DEFAULT_KEEP_CLASSES,
    FEATURE_NAMES,
    QUALITY_CLASSES,
    QualityThresholds,
    classify_features,
    is_kept,
    quality_frame,
    region_features,
    score_region,
)

FIXTURE = Path(__file__).parent / "fixtures" / "region_quality"
LEXICON = frozenset(
    {"van", "de", "en", "den", "te", "resolutie", "staten", "generaal", "brief", "heeft", "ripperda"}
)


def _score(text):
    return score_region(text, lexicon=LEXICON)


def test_text_is_never_mutated():
    text = "  Een Brief Van Ripperda  "
    features = region_features(text, lexicon=LEXICON)
    assert text == "  Een Brief Van Ripperda  "
    assert all(not isinstance(v, str) for v in features.values())
    assert tuple(features) == FEATURE_NAMES and len(FEATURE_NAMES) == 16


def test_no_region_type_literal_in_rules():
    source = Path(rq.__file__).read_text(encoding="utf-8")
    assert "main_text" not in source and "margin" not in source


def test_score_is_deterministic_and_monotone():
    good = "van de resolutie heeft brief van staten generaal"
    bad = "xzq vbk qjw zzzz!!!! #### &&&&"
    assert _score(good) == _score(good)
    assert _score(good).score > _score(bad).score


def test_rules_record_every_firing():
    verdict = _score("!!@@##$$")
    assert verdict.rules == ("long_nonalpha_run", "low_alpha_content", "short_region")


def test_classes_are_exactly_three():
    texts = ["van de resolutie heeft brief", "!!!!", "1", "", "xyzxyz qwqwqw",
             "de Staten Generaal heeft een resolutie van brief"]
    for text in texts:
        assert _score(text).quality_class in QUALITY_CLASSES
    assert QUALITY_CLASSES == ("usable", "fragment", "noise")


def test_default_keep_excludes_only_noise():
    assert DEFAULT_KEEP_CLASSES == {"usable", "fragment"}
    assert is_kept("usable") and is_kept("fragment") and not is_kept("noise")
    assert is_kept("noise", keep_classes=frozenset({"noise"}))


@pytest.mark.parametrize("text", ["Ontvangen en geresumeerd", "zie resolutie van den Staten Generaal"])
def test_short_complete_note_is_not_noise(text):
    assert _score(text).quality_class != "noise"


def test_bare_folio_number_is_exempted_and_kept():
    verdict = _score("241")
    assert "numeric_content_exempt" in verdict.rules
    assert "low_alpha_content" not in verdict.rules
    assert verdict.quality_class != "noise"


def test_letter_only_low_alpha_region_is_not_exempted():
    verdict = _score("!!@@##$$")
    assert "low_alpha_content" in verdict.rules
    assert "numeric_content_exempt" not in verdict.rules


def test_numeric_exempt_does_not_waive_long_nonalpha_run():
    verdict = _score("50:00:08")
    assert "numeric_content_exempt" in verdict.rules
    assert "long_nonalpha_run" in verdict.rules
    assert verdict.quality_class == "noise"


def test_no_http_client_is_imported():
    source = Path(rq.__file__).read_text(encoding="utf-8")
    for name in ("requests", "urllib", "httpx", "http.client", "aiohttp", "socket"):
        assert not re.search(rf"^\s*(import|from)\s+{re.escape(name)}\b", source, re.M)


def test_quality_frame_reconciles():
    frame = pd.DataFrame(
        {"region_id": ["a", "b", "c", "d"], "text": ["van de resolutie heeft brief", "!!!!", "241", ""], "x": [4, 3, 2, 1]},
        index=[10, 11, 12, 13],
    )
    out = quality_frame(frame, lexicon=LEXICON)
    assert list(out.index) == [10, 11, 12, 13] and list(out["x"]) == [4, 3, 2, 1]
    assert list(out["region_id"]) == ["a", "b", "c", "d"]
    assert out["quality_class"].value_counts().sum() == len(frame)
    assert set(FEATURE_NAMES) <= set(out.columns)
    assert out["rules"].iloc[1] == "|".join(_score("!!!!").rules)
    assert "quality_class" not in frame.columns  # input untouched
    slim = quality_frame(frame, lexicon=LEXICON, id_col=None, with_features=False)
    assert not set(FEATURE_NAMES) & set(slim.columns)


def test_quality_frame_empty():
    frame = pd.DataFrame({"region_id": [], "text": []})
    out = quality_frame(frame, lexicon=LEXICON)
    assert len(out) == 0
    assert {"quality_class", "score", "rules", *FEATURE_NAMES} <= set(out.columns)


def test_thresholds_round_trip_and_unknown_key():
    t = QualityThresholds()
    assert QualityThresholds.from_dict(t.as_dict()) == t
    assert QualityThresholds.from_dict({"alpha_ratio_low": 0.4}).alpha_ratio_low == 0.4
    assert QualityThresholds.from_dict({"alpha_ratio_low": 0.4}).max_tokens_for_short == 2
    with pytest.raises(ValueError):
        QualityThresholds.from_dict({"no_such_threshold": 1.0})


def test_bbox_features():
    none = region_features("de brief", lexicon=LEXICON)
    for k in ("bbox_w", "bbox_h", "bbox_area", "bbox_aspect_ratio"):
        assert math.isnan(none[k])
    flat = region_features("de brief", lexicon=LEXICON, bbox=(1, 2, 30, 0))
    assert flat["bbox_aspect_ratio"] == math.inf and flat["bbox_area"] == 0.0
    box = region_features("de brief", lexicon=LEXICON, bbox=(1, 2, 30, 10))
    assert (box["bbox_w"], box["bbox_h"], box["bbox_area"], box["bbox_aspect_ratio"]) == (30.0, 10.0, 300.0, 3.0)


def _gold():
    from_fixture = pd.read_csv(FIXTURE / "gold_rows.tsv", sep="\t", quoting=3, keep_default_na=False)
    lex = frozenset((FIXTURE / "lexicon_subset.txt").read_text(encoding="utf-8").split("\n")) - {""}
    return from_fixture, lex


def _reproduce(thresholds, which):
    gold, lex = _gold()
    assert len(gold) == 300
    bad = 0
    for r in gold.itertuples():
        v = score_region(r.text, lexicon=lex, thresholds=thresholds)
        if (v.quality_class != getattr(r, f"lota_class_{which}")
                or "|".join(v.rules) != getattr(r, f"lota_rules_{which}")
                or abs(v.score - float(getattr(r, f"lota_score_{which}"))) > 1e-9):
            bad += 1
    return bad


def test_gold_rows_reproduce_lota_after():
    assert _reproduce(QualityThresholds(), "after") == 0


def test_gold_rows_reproduce_lota_before():
    assert _reproduce(QualityThresholds(digit_vs_punct_fraction_for_numeric_exempt=math.inf), "before") == 0


def _metrics(thresholds):
    gold, lex = _gold()
    df = quality_frame(gold, lexicon=lex, id_col=None, with_features=False, thresholds=thresholds)
    ho = df[df.split == "held_out"]
    keep_g = ho.gold_label.isin({"usable", "fragment"})
    keep_p = ho.quality_class.map(is_kept)
    s02 = ho.sheet == "02"
    tp = (keep_g & keep_p & s02).sum()
    return {
        "P": tp / (keep_p & s02).sum(), "R": tp / (keep_g & s02).sum(), "n02": int(s02.sum()), "n": len(ho),
        "kd": (keep_g == keep_p).mean(), "3": (ho.gold_label == ho.quality_class).mean(),
    }


def test_gold_metrics():
    after = _metrics(QualityThresholds())
    assert (after["n02"], after["n"]) == (75, 150)
    assert (round(after["P"], 3), round(after["R"], 3)) == (0.973, 0.986)
    assert (round(after["kd"], 3), round(after["3"], 3)) == (0.893, 0.820)
    before = _metrics(QualityThresholds(digit_vs_punct_fraction_for_numeric_exempt=math.inf))
    assert (round(before["P"], 3), round(before["R"], 3)) == (0.973, 0.973)
    assert (round(before["kd"], 3), round(before["3"], 3)) == (0.807, 0.727)
