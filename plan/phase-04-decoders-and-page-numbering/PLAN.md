# Phase 04: Decoders and page numbering

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> not read this file. Created and planned 2026-10-14 (Opus) from
> `docs/shared_library/upstream_inventory.md` C9, C11, C13.

## Goal

AS gets the sequence decoders (`stream_analysis/decoding/`: semi-Markov, CRF with its training
loop, BiLSTM), one Nationaal Archief scan-id parser (`model/scan_id.py`), and the page-number
alignment core with a region-table anchor extractor and a GLOBALISE index loader
(`analysis/page_numbering.py`, `datasets/globalise_index.py`). NLP's decoder and alignment
modules become shims and **reproduce their stored outputs byte for byte**. LotA's adapters
parse scan ids through AS.

## Owns

- **AS:**
  - `stream_analysis/decoding/` (new);
  - `model/scan_id.py`, `analysis/page_numbering.py`, `datasets/globalise_index.py` (all new);
  - their tests; two lines in `tests/test_packaging_core_imports.py`;
  - the `scipy` line in the core `dependencies` of `pyproject.toml`.
- **NLP:**
  - `src/startpage/{semi_markov,crf_sequence,bilstm_sequence,globalise_alignment}.py`;
  - in `scripts/`: the CRF helpers in `train_crf_sequence.py` and `train_multitask_crf.py`,
    and `start_score_from_emissions` in `train_semi_markov_duration.py`;
  - new `scripts/verify_as_decoders.py` and `scripts/verify_as_globalise_alignment.py`;
  - new `output/as-phase-04-originals/`.
- **LotA:** `archival_language/ead/adapters.py` (`_BaseAdapter.scan_id`/`parse_scan_id` only).

No other phase is active. Phase 05 (text layer) will own NLP `voc_text_layer.py`; this phase
reads the layer's files but doesn't edit its code.

## Sources

The planning session read these sections, and no others:
- `upstream_inventory.md`: glossary L183–203, C9 L311–322, C11 L335–346, C13 L358–366,
  §1 item 6 L86–96, §4 steps 10–12 L454–456, §6 L498–549.
- NLP and LotA code through three Explore subagents (signatures, line ranges, tests,
  importers, outputs), plus NLP and LotA `CLAUDE.md` in summary.
- Prototypes in the scratchpad (read-only on the siblings): see *Planning checks*.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 10 | Decoders in AS | — | 4 ported + edge + brute-force CRF tests; pmf `array_equal` to the scipy reference; equivalence with NLP originals (200/200 decodes, CRF init/viterbi/nll, `train_crf` 3/3 seeds) | [step-10](step-10-decoders.md) |
| 10b | NLP decoder shims; reproduction | 10 | `verify_as_decoders.py` exit 0: semi-Markov 4/4 CSVs byte-identical to stored, start_only 6/6 rows equal, A/B 5/5; NLP 21 passed | [step-10b](step-10b-nlp-decoder-shims.md) |
| 11 | `ScanId` in AS; LotA adapters delegate | — | synthetic grammar tests; LotA `test_adapters` 11 passed unchanged, suite 316 passed (excl. `test_derive_voc`); GT 21,623/21,623 rows parse; text-layer sample counts | [step-11](step-11-scan-id.md) |
| 12 | Page-numbering core, region anchors, GLOBALISE loader | 11 | synthetic tests; AS = NLP on the full index for 6/6 configs; `anchors_array` 467/467; region-anchor agreement measured (2555: 374/374) | [step-12](step-12-page-numbering.md) |
| 12b | NLP alignment shim; reproduction | 11, 12 | `verify_as_globalise_alignment.py` exit 0: 9/9 CSVs byte-identical, manifest equal without `git_head`, A/B 10/10 | [step-12b](step-12b-nlp-alignment-shim.md) |

All five briefs were written at planning time. 10 ∥ 11 may run in parallel (disjoint modules;
they share only the changelog and the report, both edited by appending with Edit). Then 10b ∥
12, then 12b.

## Why this order

Each sibling step comes after its AS step (phase-03 decision P8: one brief per side keeps
both under budget and isolates AS from sibling edits). 11 comes before 12 because the
alignment core needs `scan_number`, and 12b's `scan_no` shim is the first NLP use of it. The
decoders don't depend on the scan ids.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D2 | Light core plus extras | Decoders' torch modules are lazy: `import ...decoding` must not import torch | Inventory §6 |
| D8 | The GLOBALISE index is passed by path | No index data in `tests/`; synthetic CSV fixtures only | Inventory §6; the index is 117 MB |
| P1 | scipy becomes a core dependency | `scipy>=1.10` in core; `semi_markov` uses `scipy.special.ndtr` | User 2026-10-14. `ndtr` is bit-identical to `norm.cdf` (checked on 200,001 points); erfc drifts ≤6e-13 (18/500 pmf values differ). sklearn (core) already requires scipy≥1.10, so nothing new is installed |
| P2 | `train_crf` moves to AS; both NLP copies delegate | `train_crf_sequence.train_crf/dev_f1` and `train_multitask_crf.train_crf/dev_f1/collapsed_f1/decode_sequences` become wrappers | User 2026-10-14. The two copies are equivalent (stored CRF rows equal to full precision in 3 CSVs, all 6 seeds) |
| P3 | AS decoders return `list[bool]` | `decode_semi_markov` → bools; NLP's shim converts back to 0/1 ints | Phase-03 P1: canonical labels are bool |
| P4 | Port exactly; quirks are documented, not fixed | Banker's rounding, `max_scan or s`, the `+extrapolated` suffix on clamping, LIS "later duplicate wins", argmax tie order: all kept | Byte-identical reproduction is the acceptance; fixing them is a separate, measured change |
| P5 | C13 scope: AS + LotA adapters only | NLP `scan_no` switches in 12b. `voc_text_layer.inventory_sort_key` (phase 05), `separator_sheets._scan_no` (phase 07) and `layout_features._inventory_number_sort_key` stay; 6 ad hoc LotA sites are follow-ups | User 2026-10-14. `InventoryNumber` already sorts all 6,914 VOC inventories as NLP's keys do |
| P6 | `ScanId` grammar | `<institution>_<archive>_<inventory>_<digits>[.ext]`; `parse` raises, `try_parse` returns None; `digits=4` default; LotA's adapter keeps returning `None` for an id with an extension | LotA behaviour unchanged; NA grammar is the named default (`NA_INSTITUTION`, `DEFAULT_DIGITS`) |
| P7 | `AlignmentConfig` keeps exactly NLP's 4 bool fields | Numeric knobs (bands 3/10, edge 6, run window 3, slopes 1.5/0.75) are keyword parameters with those defaults | The build script writes `cfg` fields into the manifest; extra fields would change it |
| P8 | Anchors for the reproduction stay the mined CSV | `page_number_anchors` (regions table) is measured in 12, not adopted | Switching sources would change NLP's numbers; the agreement and accuracy are findings |
| P9 | Torch work in NLP uses 8 threads | `torch.set_num_threads(8)` in verify scripts | User 2026-10-14 (NLP `CLAUDE.md` asks) |

## Hazards

1. **Protected NLP outputs** (untracked): `semi_markov_duration_*_scorefix.csv`,
   `semi_markov_gain_vs_start_rate_scorefix.csv`, `multitask_crf_*.csv`,
   `crf_sequence_*_scorefix.csv`, `output/splits/*`, `output/globalise_alignment/*`,
   `pagenumber_sequence_features.csv`, `vertical_gap_features.csv`. The scripts write to module
   globals read at call time: the semi-Markov script's four `*_OUT_PATH` names, the build
   script's `OUT`, and `train_multitask_crf.main()` writes via `OUTPUT_DIR`, so never call it.
   Hash before and after (copied into 10b, 12b).
2. `build_globalise_alignment.main()` calls `git rev-parse HEAD` in the cwd. Run it with
   `cwd=<NLP>`; the planning run died at L230 when run from the scratchpad (in 12b).
3. `scripts/error_analysis_4_1b_candidates.py` runs at import and writes into
   `output/globalise_alignment/`. Don't import it (in 12b).
4. **Environment:** `fuzzy_search.similarity` is missing. NLP `test_voc_text_layer.py` and LotA
   `test_derive_voc.py` fail on import (F03-R.3). LotA baseline: 316 passed with
   `--ignore=tests/test_derive_voc.py` (in 11).
5. The text layer's `manifest.json` records paths on an external drive. Never follow them; the
   layer is local under NLP `data/voc_text_layer_v1/` (in 11, 12).
6. The build script's assertion has 0.001 of headroom on `exact` (stored 0.6190 vs 0.615 ±
   0.005) (in 12b).
7. **Leakage** (`CLAUDE.md`): the duration prior is fitted on train documents only, per split
   kind, and held-out early stopping uses a train-internal 20% slice. Reproduction keeps both.
   The reviewer checks that no AS default fits on evaluation data.

## Planning checks (2026-10-14, scratchpad; sibling files untouched, hashes checked)

- Original `train_semi_markov_duration.py` (8 threads): 4/4 CSVs byte-identical to the stored
  ones, in 75.6 s.
- Original `run_config("start_only")`: 6/6 metric rows `.equals()` the stored rows, in 79.4 s.
- Original `build_globalise_alignment.py`: 9/9 CSVs byte-identical (the manifest wasn't
  written, see hazard 2).
- `InventoryNumber` order = NLP `inventory_sort_key` order on all 6,914 text-layer
  inventories; 0 unparseable; suffixes `A–E, I, II` (54 inventories).
- GT: 21,623 `Scan File_Name` rows (20,905 distinct) all match `NL-HaNA_1.04.02_<inv>_\d{4}`.
- LotA `test_adapters.py` 11 passed; AS suite 717 passed.

## Open questions for planning (settled 2026-10-14)

- *Do the C9/C11/C13 line numbers hold?* Yes, give or take a line. The briefs carry anchors.
- *scipy or erf?* scipy (P1). *Does `train_crf` move?* Yes, both copies delegate (P2).
  *C13 scope?* AS + LotA (P5). *How many cores?* 8 (P9).
- *Which CRF config to reproduce?* `start_only` (2 states, so `n_states ≠ 3` is exercised); the
  3-state loop is exercised by the semi-Markov script.
- *Does the alignment reproduce today?* Yes (planning checks).

## Review checklist

- [x] Every step's acceptance met, with numbers in `PROGRESS.md`, re-run by the reviewer:
      both verify scripts exit 0; equivalence scripts from 10 and 12 re-run.
- [x] AS suite green; NLP 21 passed; LotA 316 passed (excl. `test_derive_voc`).
- [x] `import archival_structures.stream_analysis.decoding` doesn't import torch (packaging test).
- [x] No VOC constants in `page_numbering.py` or `scan_id.py` (only named NA defaults).
- [x] Protected NLP outputs unchanged (hashes and mtimes).
- [x] Leakage: prior fitted on train only; `check_nested_split` not bypassed by the shims.
- [x] `changelog.md` has steps 10–12 under *Unreleased*, with NLP and LotA imports listed.
- [x] `reports/phase-04-decoders-and-page-numbering.md` final; findings in
      `reports/index.md`; `make reports` clean.
- [x] `Brief defects` entries answered.

## Hand-off

Reviewed 2026-10-14: accept with follow-ups ([REVIEW](REVIEW.md)). Not yet released (to go
into v0.8.0). Later phases can assume:

- **Decoders:** `archival_structures.stream_analysis.decoding`.
  - `semi_markov` is light (numpy + scipy); `crf`/`bilstm` need the `[torch]` extra and load
    lazily.
  - Labels are bool. `train_crf` takes NLP's sequence dicts (`features`, `labels`, `mask`, and
    `page_ids`/`inventory_number` for `decode_core`).
  - On held-out data, emission-only start scores are a weak input for the semi-Markov decoder
    (F04-R.1).
- **Scan ids:** `archival_structures.model.scan_id`.
  - `ScanId.parse` (strict, raises) / `try_parse`; `scan_number`; `parse_doc_id`.
  - The NA grammar holds on every GT row and every sampled text-layer id.
  - Phase 05 (text layer) should use it for `inventory_sort_key` and the `_{inv}_` check.
    `InventoryNumber` sorts all 6,914 VOC inventories as NLP does (F04-P.5).
- **Page numbering:** `archival_structures.analysis.page_numbering` plus
  `datasets.globalise_index.load_globalise_index(path)`.
  - `page_number_anchors(regions_df)` works on the text layer's `regions/<inv>.tsv.gz` and
    equals the mined anchors (F04-12.2), so phase 05's layer is enough to align any inventory.
- **NLP verify scripts** `verify_as_decoders.py` and `verify_as_globalise_alignment.py`
  reproduce the stored outputs. Re-run them after any change to these AS modules.
- **Environment:** run NLP pytest with `tests/test_evaluation.py` before `tests/test_semi_markov.py`,
  or with `PYTHONPATH=src` (F04-R.3). LotA `test_derive_voc.py` and NLP `test_voc_text_layer.py`
  still fail on `fuzzy_search.similarity` (F03-R.3); phase 05 should fix that first.
