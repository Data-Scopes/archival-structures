"""Build tests/fixtures/region_quality/{gold_rows.tsv,lexicon_subset.txt} from LotA's gold sheets and
stored scorings. Run from the LotA root with LotA's venv:
    venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/step-16/build_fixture.py
Writes only under the AS fixture dir. Reads LotA gold/ and output/ (never writes there)."""
import csv
import gzip
import re

import pandas as pd

L = "/Users/Shared/claude_projects/data_scopes/language-of-the-archive"
AS = "/Users/marijnkoolen/Code/Huygens/archival-structures"
OUT = f"{AS}/tests/fixtures/region_quality"
SHEETS = {"02": ("02-region-quality-done.tsv", "gold-scored-{}.tsv"),
          "02c": ("02c-short-region-quality-done.tsv", "gold-scored-supplement-{}.tsv")}
STRIP = re.compile(r"^\W+|\W+$")
KW = dict(sep="\t", quoting=3, keep_default_na=False)


def main():
    lex = frozenset(l.strip() for l in gzip.open(
        f"{L}/output/lexicon/fasttext-dim_384-window_10-min_count_100-case_lower-vocab.txt.gz", "rt") if l.strip())
    rows = []
    for sheet, (gold_name, scored) in SHEETS.items():
        g = pd.read_csv(f"{L}/gold/{gold_name}", usecols=["row_no", "text", "label"], **KW)
        after = pd.read_csv(f"{L}/output/quality/1.01.02-" + scored.format("after"), **KW,
                            usecols=["row_no", "text", "predicted_class", "score", "rules_fired"])
        before = pd.read_csv(f"{L}/output/quality/1.01.02-" + scored.format("before"), **KW,
                             usecols=["row_no", "text", "predicted_class", "score", "rules_fired"])
        for name, ref in (("after", after), ("before", before)):
            assert len(ref) == len(g) == 150
            assert (g.text.values == ref.text.values).all(), (sheet, name, "text mismatch")
            assert (g.row_no.values == ref.row_no.values).all()
        for i in range(len(g)):
            rn = int(g.row_no[i])
            rows.append({"sheet": sheet, "row_no": rn, "split": "tune" if rn % 2 else "held_out",
                         "text": g.text[i], "gold_label": g.label[i],
                         "lota_class_after": after.predicted_class[i], "lota_score_after": after.score[i],
                         "lota_rules_after": after.rules_fired[i],
                         "lota_class_before": before.predicted_class[i], "lota_score_before": before.score[i],
                         "lota_rules_before": before.rules_fired[i]})
    df = pd.DataFrame(rows)
    df.to_csv(f"{OUT}/gold_rows.tsv", sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar=None)
    toks = set()
    for t in df.text:
        for tok in t.split():
            c = STRIP.sub("", tok).lower()
            if c and c in lex:
                toks.add(c)
    with open(f"{OUT}/lexicon_subset.txt", "w", encoding="utf-8") as fh:
        for t in sorted(toks):
            fh.write(t + "\n")
    print("rows", len(df), "lexicon terms", len(toks))


if __name__ == "__main__":
    main()
