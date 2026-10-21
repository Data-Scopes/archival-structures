"""Synthetic tests for archival_structures.text_layer.word_breaks."""

import pandas as pd

from archival_structures.text_layer import word_breaks as wb
from archival_structures.text_layer.io import write_json_atomic, write_plain_tsv

WORDS = ["compagnie", "schepen", "retour", "factorij", "gouverneur", "provisie"]


def _inventory(n_pairs=1100, with_break=True):
    """Regions of 10 lines (n_pairs pairs per 10 regions...). Each pair: 'woord¬' + 'vervolg ...'.
    The joined word occurs mid-line in filler lines. '.' and ':' end lines but never join."""
    regions = []
    texts = []
    for i in range(n_pairs):
        w = WORDS[i % len(WORDS)]
        cut = len(w) // 2
        mark = "¬" if with_break else "-"
        texts.append(f"de {w[:cut]}{mark}")
        texts.append(f"{w[cut:]} van den heer")
        texts.append(f"aen xyz{i % 7}. het {w} ende {w} gaen.")
        texts.append("qq abc:")
        texts.append(f"zz de Heer.")
        if len(texts) >= 10:
            regions.append(texts)
            texts = []
    if texts:
        regions.append(texts)
    return regions


def _stats_frame(regions, inv="1"):
    rows, _ = wb.word_break_stats(regions, inv)
    return pd.DataFrame(rows)


def test_bar_detected_and_abbreviation_marks_rejected():
    regions = _inventory()
    assert sum(len(r) for r in regions) >= 2000
    stats = _stats_frame(regions)
    per, corpus = wb.decide_word_breaks(stats, {"1": int(stats["n_lines"].iloc[0])})
    row = per[0]
    assert row["mode"] == "per_char"
    status = dict(s.rsplit(":", 1) for s in row["char_status"].split(" "))
    assert status["¬"] == "derived_in"
    assert status["."] == "derived_out"
    assert status[":"] == "derived_out"
    assert "¬" in row["wbc_set"]
    assert corpus["sanity_pass"] is True


def test_sanity_fails_when_bar_never_occurs():
    stats = _stats_frame(_inventory(with_break=False))
    _, corpus = wb.decide_word_breaks(stats, {"1": int(stats["n_lines"].iloc[0])})
    assert corpus["sanity_pass"] is False


def test_sanity_lists_are_parameters():
    stats = _stats_frame(_inventory())
    _, corpus = wb.decide_word_breaks(stats, {"1": 5000}, sanity_in=["."], sanity_out=["¬"])
    assert corpus["sanity_in"] == ["."] and corpus["sanity_out"] == ["¬"]
    assert corpus["sanity_pass"] is False


def test_small_inventory_takes_default_whole():
    big = _stats_frame(_inventory(), "1")
    small = _stats_frame(_inventory(n_pairs=100), "2")
    stats = pd.concat([big, small], ignore_index=True)
    per, corpus = wb.decide_word_breaks(stats, {"1": int(big["n_lines"].iloc[0]),
                                                "2": int(small["n_lines"].iloc[0])})
    assert corpus["default_set"] == ["¬"]
    small_row = per[1]
    assert small_row["mode"] == "whole_default"
    assert small_row["wbc_set"] == "¬"
    assert small_row["differs_from_default"] is False


def test_thin_evidence_takes_default_membership():
    big = _stats_frame(_inventory(), "1")
    regions = _inventory()
    # a third inventory where '¬' is attached at line end fewer than 30 times
    thin = [[f"de woord{i}¬" if i < 10 else f"de woord{i} ende" for i in range(2100)]]
    thin_stats = _stats_frame(thin, "3")
    stats = pd.concat([big, thin_stats], ignore_index=True)
    per, _ = wb.decide_word_breaks(stats, {"1": 5000, "3": int(thin_stats["n_lines"].iloc[0])})
    row = per[1]
    assert row["mode"] == "per_char"
    assert "¬:default" in row["char_status"]
    assert row["wbc_set"] == "¬"


def test_empty_inputs_do_not_raise():
    rows, info = wb.word_break_stats([], "1")
    assert rows == [] and info["n_lines"] == 0
    rows, info = wb.word_break_stats([["", ""], [""]], "1")
    assert rows == [] and info["n_lines"] == 0


def test_generator_regions_are_materialised():
    regions = _inventory(n_pairs=30)
    rows_list, _ = wb.word_break_stats(regions, "1")
    rows_gen, _ = wb.word_break_stats((iter(r) for r in regions), "1")
    assert rows_list == rows_gen


def test_read_word_break_sets_round_trip(tmp_path):
    cols = ["inventory_number", "n_lines", "mode", "wbc_set", "differs_from_default",
            "char_status"]
    write_plain_tsv(tmp_path / "wbc" / "wbc_per_inventory.tsv", cols, [
        ["1", 5000, "per_char", "¬=", True, "¬:derived_in =:derived_in"],
        ["9626A", 10, "whole_default", "¬„", False, "¬:default „:default"],
        ["2", 3000, "per_char", "", True, ""]])
    write_json_atomic(tmp_path / "wbc" / "corpus_default.json", {"default_set": ["¬", "„"]})
    sets, default = wb.read_word_break_sets(tmp_path)
    assert sets == {"1": {"¬", "="}, "9626A": {"¬", "„"}, "2": set()}
    assert default == {"¬", "„"}


# --- step 15hj2: pinned sets in decide_v2 --------------------------------------------------------

def _v2_run(tmp_path, pinned):
    import dataclasses
    from types import SimpleNamespace
    from archival_structures.text_layer.presets import SG_HAND_V2
    (tmp_path / "wbc").mkdir(exist_ok=True)
    preset = dataclasses.replace(SG_HAND_V2, wbc_pinned=pinned)
    return SimpleNamespace(preset=preset, root=tmp_path)


def _v2_stats():
    """Two inventories; '.' is made to pass the rule exactly like the bar does."""
    frames = []
    for inv in ("1", "2"):
        f = _stats_frame(_inventory(), inv=inv)
        f["all_freq_robust"] = f["all_freq"]
        f["end_rel_frac_robust"] = f["end_rel_frac"]
        bar = f[f["char"] == "¬"].iloc[0]
        for col in f.columns:
            if col not in ("inventory_number", "char", "char_name", "n_lines") \
                    and not col.startswith("total_"):
                f.loc[f["char"] == ".", col] = bar[col]
        frames.append(f)
    return pd.concat(frames, ignore_index=True)


def test_decide_v2_pinned(tmp_path):
    from archival_structures.text_layer.build import decide_v2
    stats = _v2_stats()
    lines = {"1": 5000, "2": 5000}
    run = _v2_run(tmp_path, {"default": "-=„", "2": "-:=„"})
    rows, corpus = decide_v2(run, stats, lines)
    by = {r["inventory_number"]: r for r in rows}
    assert by["1"]["wbc_set"] == "".join(sorted("-=„"))
    assert by["2"]["wbc_set"] == "".join(sorted("-:=„"))
    assert by["1"]["mode"] == by["2"]["mode"] == "pinned"
    assert by["1"]["differs_from_default"] is False and by["2"]["differs_from_default"] is True
    assert "." in corpus["derived_default_set"]
    assert corpus["pooled_rule_sanity_pass"] is False
    assert corpus["sanity_pass"] is True
    assert corpus["default_set"] == sorted("-=„")
    assert corpus["pinned"] == {"default": "-=„", "2": "-:=„"}
    assert corpus["rule_text"].endswith("(wbc_pinned).")
    assert ".:derived_in/pinned_out" in by["1"]["char_status"]


def test_decide_v2_unpinned_unchanged(tmp_path):
    from archival_structures.text_layer.build import decide_v2
    stats = _v2_stats()
    lines = {"1": 5000, "2": 5000}
    rows, corpus = decide_v2(_v2_run(tmp_path, None), stats, lines)
    expect_rows, expect_corpus = wb.decide_word_breaks(
        stats, lines, sanity_in=[], sanity_out=[":", "."])
    assert rows == expect_rows and corpus == expect_corpus
    assert corpus["sanity_pass"] is False and "pinned" not in corpus


def test_sg_hand_pins():
    """Step 15i2 (UA-011 b): hand_tar inventories whose derived set lacks `=` pin `-„`."""
    from archival_structures.text_layer.presets import SG_HAND_V2
    p = SG_HAND_V2.wbc_pinned
    assert p == {"default": "-=„", "11281": "-:=„", "3349": "-„", "3350": "-„", "3351": "-„",
                 "3353": "-„", "3354": "-„"}
    for inv in ("3352", "3771", "11182"):
        assert inv not in p and p["default"] == "-=„"


def test_sg_hand_pins_applied(tmp_path):
    """The pinned sets of SG_HAND_V2 are what decide_v2 applies (3349 without `=`, 3352 with)."""
    from archival_structures.text_layer.build import decide_v2
    from archival_structures.text_layer.presets import SG_HAND_V2
    assert SG_HAND_V2.join_lead_marks == ","
    frames = []
    for inv in ("3349", "3352"):
        f = _stats_frame(_inventory(), inv=inv)
        f["all_freq_robust"] = f["all_freq"]
        f["end_rel_frac_robust"] = f["end_rel_frac"]
        frames.append(f)
    run = _v2_run(tmp_path, SG_HAND_V2.wbc_pinned)
    rows, corpus = decide_v2(run, pd.concat(frames, ignore_index=True), {"3349": 5000, "3352": 5000})
    sets = {r["inventory_number"]: r["wbc_set"] for r in rows}
    assert sets == {"3349": "-„", "3352": "-=„"}
    assert corpus["default_set"] == sorted("-=„")
