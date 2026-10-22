# Phase 09 planning scripts

Run from the AS root with `.venv/bin/python -I`. A *layer base* is a directory with `hand/` and
`print/`, each holding one `*_text_layer_v*` root (e.g. `data/text_layer_v2_dev/15ir-a`). The
scripts only read layers (and LotA's REPUBLIC paragraphs) and write their own `--out`/`--json`
files. The steps of phase 09 reuse them; each docstring gives the exact definitions.

| Script | What it measures | Used by |
|---|---|---|
| `common.py` | helpers: layer roots, TSV reader, baseline y, inventory lists | all |
| `order.py` | jumps and same-row swaps per layout `change` class; `--simulate` the row sort | 15j, 15m |
| `bridge.py` | merged print regions whose pieces hold a guard-blocked pair | planning (F09-P.1) |
| `pn_offchain.py` | page-number segments by band, shape and gap; seeded sample with context | 15k, 15m |
| `merges.py` | right side of every merge; boundary kinds; seeded samples | 15l, 15m |
| `crosswalk.py` | REPUBLIC crosswalk match × role; `end`-role sample | 15k, 15m |
| `compare.py` | table-by-table comparison of two layer bases | every step |
| `protect.py` | size+mtime hashes of the protected trees (`--skip` own dev dirs) | every step |

Outputs from planning (on `15ir-a`): `order_15ir-a_print.json`, `bridge_15ir-a.tsv`,
`pn_15ir-a.json`, `pn_sample_planning.tsv`, `merges_15ir-a.json`, `merges_sample_planning.tsv`,
`crosswalk_15ir-a.json`, `crosswalk_sample_planning.tsv`.
