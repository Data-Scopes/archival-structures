"""Join tests ported from NLP's tests/test_voc_text_layer.py (step 15).

These pin behaviour, not a library version: a failure under another pagexml-tools version is a
finding to report, not a test to relax.
"""

from collections import Counter

from archival_structures.text_layer import join as vtl



DEFAULT = set("¬„")


def join(lines, wbcs=DEFAULT, **kwargs):
    stats = Counter()
    return vtl.join_region(lines, wbcs, stats=stats, **kwargs), stats


# --- library behaviour the guards exist for (unguarded) -------------------

def test_library_unguarded_reproduces_the_documented_failures():
    assert vtl.library_join(["…Comp„", "„agnie heeft"], DEFAULT) == "…Comp„agnie heeft"
    assert vtl.library_join(["8835„5„", "1200„3„8"], DEFAULT) == "8835„51200„3„8"
    assert vtl.library_join(["8835-", "400"], set("-")) == "8835400"
    assert vtl.library_join(["Anno 1759 —", "den"], set("—")) == "Anno 1759 den"


# --- G-digit ---------------------------------------------------------------

def test_g_digit_fires_on_ledger_notation():
    text, stats = join(["8835„5„", "1200„3„8"])
    assert text == "8835„5„ 1200„3„8"
    assert stats["g_digit|„"] == 1


def test_g_digit_fires_with_hyphen():
    text, stats = join(["8835-", "400"], wbcs=set("-"))
    assert text == "8835- 400"
    assert stats["g_digit|-"] == 1


def test_g_digit_does_not_fire_on_a_word_break():
    text, stats = join(["de Comp„", "agnie heeft"])
    assert text == "de Compagnie heeft"
    assert stats["g_digit|„"] == 0
    assert stats["merge|„"] == 1


# --- G-standalone ----------------------------------------------------------

def test_g_standalone_fires_on_a_spaced_dash():
    text, stats = join(["Anno 1759 —", "den"], wbcs=set("—"))
    assert text == "Anno 1759 — den"
    assert stats["g_standalone|—"] == 1


def test_g_standalone_fires_on_a_line_of_only_marks():
    text, stats = join(["—", "den"], wbcs=set("—"))
    assert text == "— den"
    assert stats["g_standalone|—"] == 1


def test_g_standalone_does_not_fire_on_an_attached_mark():
    text, stats = join(["Anno ge—", "daen"], wbcs=set("—"))
    assert text == "Anno gedaen"
    assert stats["g_standalone|—"] == 0


# --- guards added after the pass-1 error analysis (report/74 §5) ----------

def test_g_digit_left_blocks_number_plus_word():
    text, stats = join(["2500„", "tesamen bharen 6512."])
    assert text == "2500„ tesamen bharen 6512."
    assert stats["g_digit_left|„"] == 1


def test_g_punct_left_blocks_abbreviation_plus_filler():
    text, stats = join(["wanneer d' E:„", "Comp:s die alleen"])
    assert text == "wanneer d' E:„ Comp:s die alleen"
    assert stats["g_punct_left|„"] == 1


def test_g_digit_right_blocks_word_plus_amount():
    text, stats = join(["nodige huijs onkosten beloopen„", "123:16:14"])
    assert text == "nodige huijs onkosten beloopen„ 123:16:14"
    assert stats["g_digit_right|„"] == 1


def test_spec_guards_only_reproduce_the_step_file_behaviour():
    text, _ = join(["2500„", "tesamen"], guards=vtl.SPEC_GUARDS)
    assert text == "2500tesamen"


# --- both-ends fix ---------------------------------------------------------

def test_both_ends_fix_on_last_line():
    text, stats = join(["…Comp„", "„agnie heeft"])
    assert text == "…Compagnie heeft"
    assert stats["both_ends_fix|„|last_line"] == 1


def test_both_ends_fix_on_inner_line():
    text, stats = join(["de Comp„", "„agnie", "heeft"])
    assert text == "de Compagnie heeft"
    assert stats["both_ends_fix|„|inner_line"] == 1


def test_both_ends_fix_with_mixed_marks():
    text, _ = join(["onkost reecq: vande verstreckte pro¬", "„visie en andre"])
    assert text == "onkost reecq: vande verstreckte provisie en andre"


def test_start_only_is_left_raw_and_counted():
    text, stats = join(["hem in het binnenste fort met een groo", "„te suit"])
    assert text == "hem in het binnenste fort met een groo „te suit"
    assert stats["start_only|„"] == 1


# --- region edges ------------------------------------------------------------

def test_region_edges_are_preserved():
    text, _ = join(["„wel nu deese", "Jaar gem: 'sComp:s„"])
    assert text == "„wel nu deese Jaar gem: 'sComp:s„"


def test_single_line_region_is_verbatim():
    text, _ = join(["„agnie heeft ge„"])
    assert text == "„agnie heeft ge„"


# --- empty regions, empty lines ---------------------------------------------

def test_empty_region():
    text, stats = join([])
    assert text == ""
    assert stats["regions_empty_text"] == 1


def test_region_of_only_empty_lines():
    text, stats = join(["", ""])
    assert text == ""
    assert stats["lines_empty_dropped"] == 2


def test_empty_lines_are_dropped_between_lines():
    text, _ = join(["de Comp„", "", "agnie heeft"])
    assert text == "de Compagnie heeft"


# --- WBC set ----------------------------------------------------------------

def test_set_without_hyphen_leaves_hyphen_unjoined():
    text, _ = join(["ge-", "daen"], wbcs=set("¬„"))
    assert text == "ge- daen"


def test_set_with_hyphen_joins():
    text, _ = join(["ge-", "daen"], wbcs=set("¬„-"))
    assert text == "gedaen"


def test_syllable_repetition_is_counted_not_fixed():
    stats = Counter()
    text = vtl.join_region(["mijn al„", "alderminsten"], DEFAULT, stats=stats,
                           vocab={"alderminsten"})
    assert text == "mijn alalderminsten"
    assert stats["diag_syllable_repetition|„"] == 1


