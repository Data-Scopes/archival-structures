# region_quality fixture (phase 08, step 16)

Provenance: built on 2026-10-21 by
`reports/data/phase-08-region-quality-and-duplicates/step-16/build_fixture.py` from the
language-of-the-archive (LotA) gold sheets `gold/02-region-quality-done.tsv` and
`gold/02c-short-region-quality-done.tsv` (150 rows each; SG = NL-HaNA 1.01.02, Dutch HTR) and LotA's stored
scorings `output/quality/1.01.02-gold-scored{,-supplement}-{after,before}.tsv`.

- `gold_rows.tsv`: 300 rows. `sheet`, `row_no`, `split` (odd `row_no` = `tune`, even = `held_out`), `text`,
  `gold_label`, then LotA's `class/score/rules` from the `-after` files (current code) and the `-before`
  files (pre-retune code, reproduced with `digit_vs_punct_fraction_for_numeric_exempt=inf`). Read with
  `sep="\t", quoting=3, keep_default_na=False`.
- `lexicon_subset.txt`: the 2,837 lower-case lexicon terms that occur as a lookup form of a gold token, out of
  LotA's 233,538-term fastText vocabulary. Not a usable lexicon; it only makes the gold rows reproducible.
