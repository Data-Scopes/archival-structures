# Phase 08 planning scripts (2026-10-21)

Run from the LotA root with LotA's interpreter (they import `archival_language` and read LotA
`output/`), writing nothing:

    cd /Users/Shared/claude_projects/data_scopes/language-of-the-archive
    venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/planning/gold_check.py
    venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/planning/iq_check.py 20261021 200
    venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/planning/dup_check.py 3779 3767 3822 638 1576 2588 4000
    <AS>/.venv/bin/python <AS>/reports/data/phase-08-region-quality-and-duplicates/planning/reshoots.py output/integrity/1.01.02-duplicate-candidates.tsv

| Script | Planning result |
|---|---|
| `gold_check.py` | 0 mismatches on 4 × 150 rows (`-after` with current thresholds, `-before` with the exemption threshold set to `inf`); 3,789 cleaned gold tokens, 2,837 in the 233,538-term lexicon |
| `iq_check.py 20261021 200` | 0 mismatches against `output/quality/1.01.02-inventory-quality.tsv` on 200 seeded inventories (428,544 regions, 37 s) |
| `dup_check.py …` | 7 of 7 inventories identical to the stored candidate table (3779 4, 3767 5, 3822 6, 638 2,681, 1576 87, 2588 5, 4000 1 rows) |
| `reshoots.py` | 13/13, 23 rows, all adjacent; stored table 48,473 rows, 14,994 adjacent |
