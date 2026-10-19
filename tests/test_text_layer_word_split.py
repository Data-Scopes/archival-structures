import json
import random
import sys

from archival_structures.text_layer import build
from archival_structures.text_layer.io import read_tsv_gz, write_tsv_gz
from archival_structures.text_layer.word_split import (
    WordSplitParams, apply_word_split, build_vocabulary, repair_line, segment, token_core)

HEADER = ["scan_id", "page_side", "column", "region_id", "line_idx", "line_id", "x", "y", "w", "h",
          "baseline", "conf", "text_raw", "text", "edit_flags", "split_from"]
P = WordSplitParams(min_token_len=8, min_part_count=3, split_penalty=0)
WORDS = ["dat", "wy", "noch", "ders", "Hondertsten", "penningh", "toekomende", "Inge-"]


def corpus(reps=3000):
    """Few word types with large counts: a 4-part split beats the unsplit score log(0.5/total)
    only when the parts are this frequent."""
    return ["dat wy noch ders", "Hondertsten penningh toekomende Inge-"] * reps


def test_token_core_keeps_break_mark():
    assert token_core("Inge-") == ("", "Inge", "-")
    assert token_core("woordt„") == ("", "woordt", "„")
    assert token_core("(Hondert,") == ("(", "Hondert", ",")


def test_segment_simple():
    vocab = build_vocabulary(corpus())
    total = sum(vocab.values())
    assert segment("datwynochders", vocab, total, P) == ["dat", "wy", "noch", "ders"]
    assert segment("zzzzzzzzzz", vocab, total, P) is None


def test_attested_word_not_split():
    vocab = build_vocabulary(corpus() + ["datwynoch"] * 4)
    text, edits = repair_line("datwynoch", vocab, sum(vocab.values()), P)
    assert text == "datwynoch" and edits == []


def test_min_part_len_and_max_parts():
    vocab = build_vocabulary(corpus() + ["a b c d e f"] * 3000)
    total = sum(vocab.values())
    p5 = WordSplitParams(max_parts=4, min_part_count=3, split_penalty=0)
    assert segment("datwynochdersdat", vocab, total, p5) is None  # needs 5 parts
    assert segment("datwynochders", vocab, total, p5) == ["dat", "wy", "noch", "ders"]
    p2 = WordSplitParams(min_part_len=2, min_part_count=3, split_penalty=0)
    assert segment("adatwynoch", vocab, total, p2) is None  # 'a' is shorter than min_part_len


def test_repair_line_only_inserts_spaces():
    vocab = build_vocabulary(corpus())
    total = sum(vocab.values())
    rng = random.Random(1)
    for _ in range(200):
        toks = [rng.choice(WORDS) for _ in range(rng.randint(1, 5))]
        raw = " ".join("".join(toks[i:i + 2]) if rng.random() < 0.5 else toks[i]
                       for i in range(0, len(toks)))
        out, edits = repair_line(raw, vocab, total, P)
        assert out.replace(" ", "") == raw.replace(" ", "")
        assert out.count(" ") - raw.count(" ") == sum(len(parts) - 1 for _, _, parts in edits)


def test_casing_preserved():
    vocab = build_vocabulary(corpus())
    out, edits = repair_line("HondertstenpenninghtoekomendeInge-", vocab, sum(vocab.values()), P)
    assert out == "Hondertsten penningh toekomende Inge-"
    assert edits[0][2] == ["Hondertsten", "penningh", "toekomende", "Inge"]


def _row(i, raw, flags=""):
    r = dict.fromkeys(HEADER, "")
    r.update(scan_id=f"s{i}", line_id=f"l{i}", text_raw=raw, text=raw, edit_flags=flags)
    return r


def test_flags_preserve_split_flags():
    vocab = build_vocabulary(corpus())
    rows = [_row(1, "dat wydatnoch", "split:hyphen"), _row(2, "dat wy", "wordsplit:9;split:bigram")]
    new = apply_word_split(rows, vocab, P)
    assert new[0]["edit_flags"] == "split:hyphen;wordsplit:1"
    assert new[1]["edit_flags"] == "split:bigram" and new[1]["text"] == "dat wy"


def test_wordfix_stage_idempotent(tmp_path):
    lines = tmp_path / "NL-HaNA_1.10.94_text_layer_v2" / "lines"
    rows = [_row(i, t) for i, t in enumerate(corpus(500) + ["datwynochders", "Hondertstenpenninghtoekomende"])]
    write_tsv_gz(lines / "455.lines.tsv.gz", HEADER, ([r[c] for c in HEADER] for r in rows))
    args = ["--out-dir", str(tmp_path), "--preset", "sg_1.10.94_v2", "--stage", "wordfix",
            "--word-params", json.dumps({"min_token_len": 8, "min_part_count": 3, "split_penalty": 0})]
    assert build.main(args) == 0
    first = (lines / "455.lines.tsv.gz").read_bytes()
    edits_first = (tmp_path / "NL-HaNA_1.10.94_text_layer_v2" / "word_split" / "edits" / "455.tsv.gz").read_bytes()
    assert build.main(args) == 0
    assert (lines / "455.lines.tsv.gz").read_bytes() == first
    assert (tmp_path / "NL-HaNA_1.10.94_text_layer_v2" / "word_split" / "edits" / "455.tsv.gz").read_bytes() == edits_first
    out = list(read_tsv_gz(lines / "455.lines.tsv.gz"))
    assert out[-2]["text"] == "dat wy noch ders" and out[-2]["edit_flags"] == "wordsplit:1"
