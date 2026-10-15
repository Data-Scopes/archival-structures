# Phase 05 report: Text layer (v1 producer into AS)

> **Status:** final (reviewed 2026-10-15; code review in
> [`REVIEW.md`](../plan/phase-05-text-layer/REVIEW.md)).
> **Phase plan:** [`plan/phase-05-text-layer/PLAN.md`](../plan/phase-05-text-layer/PLAN.md).
> **Code state:** AS unreleased after v0.8.0 (working tree 2026-10-15, before step 15r); NLP
> working tree after step 15b; LotA unchanged.
> **Last updated:** 2026-10-15, by the phase review (Opus).

## Summary

AS now holds the producer of the shared text layer: `archival_structures.text_layer`, with the
console script `as-build-text-layer`. It covers PageXML extraction, a per-inventory
word-break detector, the guarded line join, provenance, a resumable driver, presets and a layer
comparison tool. The VOC values are the preset `voc_v1`. NLP's `voc_text_layer.py` is a shim
and its build script a wrapper. AS's pins moved to pagexml-tools 0.9.0 and fuzzy-search 3.

The move changes no output:
- NLP's local 22-inventory build is reproduced 157/157 by AS functions, the CLI, the NLP
  wrapper and the saved original code, all checked again by the review (F05-15.1, F05-15c.1,
  F05-15b.1);
- the user's rebuild of 20 inventories from GLOBALISE v2.1 equals v1 byte for byte (F05-R.1);
- the outputs are the same under pandas 2.3.3 and 3.0.6 (F05-R.7).

What a reader should know:
- NLP's local PageXML zips (a 2023 export) are not v1's input. They hold the same lines and
  texts but in a worse order: paragraph regions run top to bottom in 51% of scans against 97%
  in v1, and the line order differs in 4% of regions. So build from v2.1 for analysis and use
  the zips for tests (F05-P.1, F05-15.4, F05-15.5).
- The word-break rule is conservative and misses `„` systematically. 696 of 6,914 inventories
  (10%) don't join on `„`, mostly because `„` is also a frequent mid-line abbreviation mark;
  in a sample most of the missed line ends were real breaks (F05-14.4, F05-R.4). This is a
  property of v1, kept on purpose; a v2 rule is phase 06.
- 98 of 4.8 million v1 scan ids don't fit the 4-digit pattern (F05-15.3). pagexml-tools 0.9.0
  still can't parse nested `TextRegion`s (F05-13.3).
- Before the release, step 15r fixes two pandas-3 failures outside the text layer and raises
  the run-time pagexml-tools check to 0.9.0 (F05-R.2, F05-R.6). It also documents that a
  subset run must not write into an existing layer (F05-R.5).

## Findings

One row per finding. The ID is `F<phase>-<step>.<n>`. Planning findings use `F05-P.<n>`, the
review's own findings `F05-R.<n>`. IDs never change and are never reused; a wrong finding is
marked `withdrawn`.

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F05-P.1 | NLP's 504 local PageXML zips are not v1's input. Rebuilt from them, 34 inventories have the same line multisets, region keys and region metadata as v1 (34/34), but a different region order (34/34) and line order in 8,626 of 160,132 regions; all 8,141 differing region texts are in those regions. So v1 can't be reproduced byte for byte from local input | data-property | text-layer, voc, nlp | confirmed | [Planning](#planning) |
| F05-P.2 | Under AS's `.venv` (pagexml-tools 0.8.1 from PyPI, fuzzy-search 2.5.0), NLP's current module rebuilds the 22 annotated inventories identically to NLP's own local build: 157/157 files (TSVs byte-identical, stats JSONs equal without `source`) in 221 s | result | text-layer, voc, nlp | confirmed | [Planning](#planning) |
| F05-P.3 | `decide_wbc` on v1's `wbc_stats.tsv` (109,984 rows, 6,913 inventories) reproduces `wbc_per_inventory.tsv` and `corpus_default.json` byte for byte (default {¬, „}; 1,461 inventories deviate; 172 take the default whole), and v1's lines reproduce v1's stats parts (8/8) | result | text-layer, word-breaks, voc | confirmed | [Planning](#planning) |
| F05-P.4 | PyPI pagexml-tools 0.8.1 equals commit `0f39fad` apart from the version string and a one-line `vertical_distance` fix. v1's four "dirty" files match the import-only commit `7b2b721` (orjson, the fuzzy-search-3 path), whose parser gives the same extract output as 0.8.1 | method | text-layer, pagexml-tools, provenance | confirmed | [Planning](#planning) |
| F05-P.5 | pagexml-tools 0.8.1 imports `fuzzy_search.similarity`, which exists only in fuzzy-search 2.x. NLP (3.0.4) and LotA (3.0.4) can't import the joiner: 23 of NLP's 29 text-layer tests fail at run time (all 29 collect), and LotA's `test_derive_voc.py` fails at collection. AS's pin `fuzzy-search <4` allows the same break. *Resolved by pagexml-tools 0.9.0 and step 13's pins (F05-13.1)* | open-issue | environment, pagexml-tools, nlp, lota | confirmed | [Planning](#planning) |
| F05-P.6 | v1's extract stats over 6,914 inventories: 4,798,666 scans, 234,261 without text regions, and 0 parse errors, 0 scan-id mismatches, 0 nested regions, 0 regions without coordinates. Those code paths are exercised only by the synthetic fixture | data-property | text-layer, voc, scan-id | confirmed | [Planning](#planning) |
| F05-P.7 | pagexml-tools 0.8.1 can't parse a PageXML file with nested `TextRegion`s (`TypeError` from `set += set` in `get_all_text_regions`, `pagexml_document_model.py` L447). It also derives a box from the lines of a region without `Coords`, and drops a region with neither `Coords` nor lines | open-issue | pagexml-tools, text-layer | confirmed | [Planning](#planning) |
| F05-14.1 | `archival_structures.text_layer.{io,word_breaks}` reproduce v1 exactly: `corpus_default.json` and `wbc_per_inventory.tsv` byte-identical (default {¬, „}; 1,461 deviate; 172 whole default), `wbc_stats.tsv` rewrite byte-identical, 52/52 stats parts recomputed from v1's lines identical, and the `InventoryNumber` sort key gives NLP's order on all 6,914 names | result | text-layer, word-breaks, voc | confirmed | [Step 14](#step-14-text-layer-io-and-word-break-detector) |
| F05-14.2 | Of 6,914 inventories, 5,453 (78.9%) take the default {¬, „}; of the 1,461 that deviate, 717 add a char (`=` in 672, `-` 89, `:` 14, `!` 12, `—` 11, `.` 9 over the 795 that add), 590 drop `„` only, 44 drop `¬` only, 32 drop both, 78 add and drop | data-property | text-layer, word-breaks, voc | confirmed | [Step 14](#step-14-text-layer-io-and-word-break-detector) |
| F05-14.3 | Threshold sensitivity: of 45,976 (inventory, char) rows with `n_end_attached` >= 30 and `n_lines` >= 2000, 3,963 lie within +-10% of `min_join_vocab_frac` (574) or `min_end_rel_frac` (3,412); 684 of them (238 and 446) pass the other criterion, so a +-10% change could flip them | method | text-layer, word-breaks, thresholds | confirmed | [Step 14](#step-14-text-layer-io-and-word-break-detector) |
| F05-14.4 | In a seeded sample of 10 deviating inventories, 4 add `=` plausibly (a real break: `uw=` / `HoogEdelens`), and 6 drop a genuine mark (`„` 5, `¬` 1) that fails a threshold narrowly or because the mark is frequent: `end_rel_frac` 4.90 and 4.36 against 5.0, `join_vocab_frac` 0.206-0.235 against 0.25. The rule misses real breaks where the mark is very frequent or the vocabulary small. *Revised by the review: 5 drop a genuine `„`; the sixth (1068, `¬`, `adel¬` + `vervoert`) is ambiguous, as the step's own text says. The pattern is systematic (F05-R.4)* | negative-result | text-layer, word-breaks, voc | revised | [Step 14](#step-14-text-layer-io-and-word-break-detector) |
| F05-15.1 | `text_layer.extract/join` with the step-14 modules rebuild NLP's 22-inventory reference byte-identically: 157/157 files (22 x 5 tsv, 3 `wbc/` files, 44 JSONs without `source`), in 39 s with 8 workers | result | text-layer, voc, reproduction | confirmed | [Step 15](#step-15-text-layer-library) |
| F05-15.2 | A/B against a copy of NLP's original module on 20 further local inventories with v1's word-break sets: 140/140 files equal (100 tsv, 40 JSONs without `source`); CPU time 134 s (AS) vs 123 s (NLP) | result | text-layer, voc, reproduction | confirmed | [Step 15](#step-15-text-layer-library) |
| F05-15.3 | 98 of 4,798,666 distinct v1 scan ids (6,914 inventories) are irregular, in 3 inventories: 10090 (95 ids, two-digit numbers `..._10090_64.jpg`), 1186 (`0571A`, `0571B`), 9817 (`P01`). User decision: `scan_id_matches` parses with `ScanId` for any digit width and, for ids that do not parse at all, falls back to NLP's `_{inv}_` substring test; with it 0 ids mismatch | data-property | text-layer, scan-id, voc | confirmed | [Step 15](#step-15-text-layer-library) |
| F05-15.4 | Local 2023 export vs v1 on 42 inventories (313,209 regions): line multisets, region key sets and `regions_meta` values equal in 42/42; the line order differs in 12,834 regions (4.1%) and 12,173 region texts differ, all in those regions; region order differs in 41/42 inventories (9,057 scans), scan order in 0 | data-property | text-layer, input-order, voc | confirmed | [Step 15](#step-15-text-layer-library) |
| F05-15.5 | v1's order reads better than the 2023 export's: paragraph regions are in top-to-bottom order in 97.2% of scans in v1 (18,273 of 18,801 scans with 2+ paragraphs) and in 51.2% in the rebuilt layer (9,623); in 10 sampled regions v1's line order is right in 9 and undecidable in 1. The rebuilt order moves isolated short lines (dateline, page number, empty line) to the region end or swaps adjacent lines | data-property | text-layer, input-order, reading-order | confirmed | [Step 15](#step-15-text-layer-library) |
| F05-15c.1 | `as-build-text-layer --stage all --write-events --workers 8` on the 22 reference inventories reproduces NLP's `voc_text_layer_v1` 157/157 (110 TSVs byte-identical, 44 JSONs equal without `source`, 3/3 `wbc/` files), in 34.6 s wall; the v2.1 recipe on 3 inventories against the same reference gives exit 0 (15/15) | result | text-layer, voc, reproduction, nlp | confirmed | [Step 15c](#step-15c-text-layer-driver-and-cli) |
| F05-15c.2 | The driver's manifest is a strict superset of NLP's: per stage the keys differ only by `archival_structures` and `preset`, layer-level values are equal; values that differ are `git_head` (now AS HEAD with `git_dirty_files`), `pagexml_tools` (0.8.1 from site-packages, no git state, vs an editable 0.8.0 checkout), `python`, timestamps, `argv`, and `params` (`pagexml_dir`, `out_dir`, `force`, `preset`) | behaviour-change | text-layer, provenance, nlp | confirmed | [Step 15c](#step-15c-text-layer-driver-and-cli) |
| F05-15c.3 | The `--estimate` fit predicts 30.8 s for the 22 on 8 workers (extract 26.8, wbc 1.1, join 2.8); measured 34.6 s wall (log stages 29 + 2 + 4 s), ratio 1.12 (extract 1.08, wbc ~1.8, join ~1.4); the output prediction (51 MB) matches the measured 49.8 MB (lines 33.1 + regions 16.7) | method | text-layer, throughput | confirmed | [Step 15c](#step-15c-text-layer-driver-and-cli) |
| F05-15c.4 | A rerun of the same command is a no-op (every stage "0 to do"/"nothing to do", 0.4 s, no mtime under `lines/` or `regions/` changes) but, as in NLP's script, it still rewrites the manifest stage entries (`finished_utc`, `argv`, `params`, incl. `force`), so the manifest records the last invocation, not the one that produced the files | open-issue | text-layer, provenance | confirmed | [Step 15c](#step-15c-text-layer-driver-and-cli) |
| F05-13.1 | AS `.venv` now has pagexml-tools 0.9.0 with fuzzy-search 3.0.4 (from 0.8.1 / 2.5.0); NLP and LotA already import the 0.9.0 checkout (`pagexml.__version__` 0.9.0, `c112a28`), so `pagexml.helper.pagexml_helper` and `LineCharAnalyser` import in all three. LotA `test_derive_voc.py` now collects and passes (11), LotA full suite 327 passed (316 + 11), NLP 29 + 21 passed | result | pagexml-tools, environments, fuzzy-search | confirmed | [Step 13](#step-13-pagexml-tools-090) |
| F05-13.2 | The 15c reproduction under 0.9.0 is unchanged: 157/157 (110 TSVs byte-equal, 44 JSONs equal, 3/3 run files) in 35.9 s wall; protected files (7,079) unchanged. Of the 9 `pagexml/` files that differ from 0.8.1, none touches the extract, join or word-break text path, so the 6,892 inventories not rebuilt are expected to be unchanged too | result | text-layer, reproduction, pagexml-tools | confirmed | [Step 13](#step-13-pagexml-tools-090) |
| F05-13.3 | Nested TextRegions still fail to parse in 0.9.0 (`set += set` at `pagexml_document_model.py` L447): the fixture scan `_0003.xml` is still a recorded parse error. The test is renamed `test_nested_regions_fail_in_pagexml`; to be reported upstream | open-issue | pagexml-tools, nested-regions | confirmed | [Step 13](#step-13-pagexml-tools-090) |
| F05-13.4 | The NLP and LotA dist-info metadata is stale: `importlib.metadata.version("pagexml-tools")` gives 0.8.0 in both although the editable checkout is 0.9.0, so manifests built there would record `pagexml_tools.version` 0.8.0. Fix (user): `pip install --no-deps -e /Users/marijnkoolen/Code/Huygens/pagexml-tools` with each of the two interpreters. *Resolved by the user by 2026-10-15 (all three report 0.9.0); archival-structures' own dist-info is stale in the same way (F05-R.3)* | open-issue | pagexml-tools, provenance, environments | confirmed | [Step 13](#step-13-pagexml-tools-090) |
| F05-13.5 | 0.9.0 fixes `vertical_distance` for baseline-bearing lines (spurious `return abs(bottom1 - bottom2)` removed, from `7b2b721`'s branch). One AS test pinned the old behaviour (`test_gap_is_zero_for_overlapping_slanted_baselines` asserted 20); it now asserts 0. AS's `line_vertical_gap` workaround is now redundant for this case but still correct | behaviour-change | pagexml-tools, neighbourhood-analysis | confirmed | [Step 13](#step-13-pagexml-tools-090) |
| F05-15b.1 | NLP's `voc_text_layer` shim plus the `build_voc_text_layer.py` wrapper rebuild the 22 inventories 157/157 identical to NLP's stored build and 157/157 to a run of the saved original code (32.7 s and 33.6 s, 8 workers); the shim's `decide_wbc` reproduces v1's `wbc_per_inventory.tsv` and `corpus_default.json` 2/2; NLP tests 29 + 21 pass | result | text-layer, voc, nlp, reproduction | confirmed | [Step 15b](#step-15b-nlp-text-layer-shim) |
| F05-15b.2 | Through the wrapper NLP's manifest stages gain `archival_structures` and `preset` and carry AS's `git_head`; all other differing values from the original run are timestamps or `argv`/`params` that differ only in `out_dir`/`preset` (no "other") | behaviour-change | text-layer, provenance, nlp | confirmed | [Step 15b](#step-15b-nlp-text-layer-shim) |
| F05-15b.3 | NLP runs still record `pagexml-tools=0.8.0` in `progress.log` and the manifest, because of the stale dist-info (F05-13.4), although the code is 0.9.0. *Resolved for pagexml-tools (the review's NLP run records 0.9.0); the same run records archival-structures 0.1.0 (F05-R.3)* | open-issue | pagexml-tools, provenance, environments | confirmed | [Step 15b](#step-15b-nlp-text-layer-shim) |
| F05-R.1 | P3d, run by the user on 2026-10-15: the AS CLI rebuilt 20 inventories from GLOBALISE v2.1 (the v1 input) with v1's word-break files, and `compare` against v1 gave `IDENTICAL (100 comparisons)`. `lines`, `regions_meta` and `regions` are byte-equal 20/20; `extract_stats` and `join_stats` are JSON-equal and byte-equal 20/20. With v1's input the producer reproduces v1 byte for byte, which the local zips can't show (F05-P.1) | result | text-layer, voc, reproduction | confirmed | [Review](#review) |
| F05-R.2 | pandas 3.0.6 (in AS's `.venv` since 2026-10-15; `pyproject.toml` allows `<4`) fails 2 AS tests outside the text layer. `segment_into_elements` takes a NaN label (pandas 3 stores `None` in a str `Series` as NaN) for a real label, giving 3 elements instead of 2; `load_globalise_index` returns `StringDtype`, not `object`. The fix is step 15r | open-issue | pandas, environment, sequence-patterns, globalise | confirmed | [Review](#review) |
| F05-R.3 | archival-structures' dist-info is stale in the siblings (0.1.0 in NLP's `venv`, 0.3.0 in LotA's), so a layer built through NLP's wrapper records `archival_structures.version` 0.1.0 in `manifest.json` (seen in the review's run); `git_head` and `git_dirty_files` are correct | open-issue | provenance, environments, nlp, lota | confirmed | [Review](#review) |
| F05-R.4 | The `„` miss is systematic. 696 of 6,914 inventories are in `per_char` mode without `„`: 446 fail only `end_rel_frac`, 148 only `join_vocab_frac`, 102 both (medians among those failing: `end_rel_frac` 3.99 against 5.0, `join_vocab_frac` 0.19 against 0.25). Where `end_rel_frac` fails, `„` is common overall (median 8,361 occurrences against 711 in the 148 where it passes): it doubles as an abbreviation mark (`a„o`, `p„s`). In a fresh sample of 6 inventories, 13 of 18 attached `„` line ends were real breaks, 2 were not, 3 ambiguous | negative-result | text-layer, word-breaks, voc, thresholds | confirmed | [Review](#review) |
| F05-R.5 | The `wbc` stage re-derives `corpus_default.json` and `wbc_per_inventory.tsv` from the run's inventories only and overwrites them (as in NLP). A subset run pointed at an existing layer (now under `shared-data/`) would replace v1's decision, and `join` would rewrite regions whose set changed | open-issue | text-layer, provenance, shared-data | confirmed | [Review](#review) |
| F05-R.6 | The driver's run-time check `MIN_PAGEXML_TOOLS` is still 0.8.0 while the pin is `>=0.9.0`, so it accepts 0.8.x, under which the joiner can't import with fuzzy-search 3 (F05-P.5). Raised to 0.9.0 in step 15r | open-issue | text-layer, pagexml-tools, provenance | confirmed | [Review](#review) |
| F05-R.7 | The text layer is the same under pandas 2.3.3 and 3.0.6: NLP's wrapper (pandas 2.3.3) and AS's CLI and library (3.0.6) both reproduce the 157 reference files, and `decide_word_breaks` reproduces v1's decision files under 3.0.6 | result | text-layer, pandas, reproduction | confirmed | [Review](#review) |

- **Kind:** `result`, `behaviour-change`, `bug-fixed`, `data-property`, `negative-result`,
  `open-issue`, `method` (see `plan/templates/REPORT.md`).
- **Status:** `draft` (step), `confirmed` or `revised` (review), `withdrawn`.

## Steps

### Planning

Planning session (Opus, 2026-10-14); no code changed. The prototypes ran in the scratchpad
under AS's `.venv`. They loaded a copy of NLP's `voc_text_layer.py` (sha256 `a36d08fd6660a227…`)
and wrote nothing to the siblings. The scripts are in
[`data/phase-05-text-layer/planning/`](data/phase-05-text-layer/planning/), and the
synthetic fixture inventory in [`data/phase-05-text-layer/fixture_9999A/`](data/phase-05-text-layer/fixture_9999A/).

- **F05-P.1.** First, 13 local inventories were rebuilt and compared with v1. The `.gz` files
  differed, so the comparison was repeated on decompressed content: the row counts were equal,
  but the order wasn't.
  - Inventory 9098, scan `_0001`: v1 puts the XML's last `TextLine` (index 31, top y = 628)
    second, between lines 0 (y = 581) and 1 (y = 732). The local XML (`<Created>2023-09-01`)
    has it last, and the rebuild keeps XML order.
  - The rebuild is the same under `PYTHONHASHSEED` 0–3 and random, and the same with
    `7b2b721`'s code.
  - NLP's 22-inventory build from the same local zips matches the rebuild, not v1 (2555:
    lines, regions_meta and regions all byte-identical).
  - So the input differs, not the code: v1 was built from GLOBALISE v2.1 on an external disk
    (`vs_v1.py` on the 13 plus the 22 rebuilds; 34 distinct inventories, 160,132 regions with
    lines).
- **F05-P.2.** `proto22.py`. The 157 files are, per inventory, `lines`, `regions_meta`,
  `regions`, `join_events` and the `wbc/parts` file (22 × 5), plus `wbc_stats.tsv`,
  `corpus_default.json` and `wbc_per_inventory.tsv`, plus 44 stats JSONs. Steps 15, 15c and
  15b use this build as the byte reference (decision P3).
- **F05-P.3.** `wbc_decide.py`. Inventory order: the `lines/` stems sorted by NLP's
  `inventory_sort_key`, with `n_lines` from the first stats row (0 for the one inventory without
  candidates). The parts check read v1's `lines/` and `regions_meta/` and wrote only to the
  scratchpad.
- **F05-P.4.** File-by-file diff of the installed package (CRLF stripped) against
  `git show 0f39fad0:pagexml/<file>`: two files differ, `__init__.py` (version) and
  `physical_document_model.py` (one line). An Explore subagent's comparison of `7b2b721`: the four
  files differ only in imports, plus a `'text_regions' not in page_doc.stats` guard in
  `get_line_format_json`.
- **F05-P.5.** NLP's `venv` takes pagexml (the checkout at `d1f0838`, "prepare release 0.8.1")
  and fuzzy-search 3.0.4 from Homebrew site-packages. LotA's `venv` has editable installs of both.
  The import chain is `pagexml_helper` L13 → `text_stats` L12 → `text_helper` L5. The user
  releases 0.8.2 from the fix branch (P2); step 13 pins it.
- **F05-P.6.** Summed over `extract_stats/*.json`.
- **F05-P.7.** `bisect.py` on [`nested_fixture.xml`](data/phase-05-text-layer/planning/nested_fixture.xml):
  removing the nested region makes the file parse; removing the region without `Coords`
  doesn't. Suggested to the user for 0.8.2: `text_regions |= …`.

Left for the next phase (P1, P11): v2 line geometry and page size; empty-scan rows; the SG
preset (audit: `docs/audits/sg_paragraph_tsv_audit.md` §2–3); a region-end word-break statistic
(`position="region_end"`).

### Step 14: text-layer I/O and word-break detector

**What changed.** New package `archival_structures/text_layer/` with `io.py` (NLP's I/O, input
discovery and `read_region_lines`, ported verbatim; `inventory_sort_key` now uses
`InventoryNumber`) and `word_breaks.py` (the detector under AS names, `DEFAULT_WORD_BREAK_RULE`,
`word_break_stats(regions, inventory_number)`, `decide_word_breaks(..., *, sanity_in, sanity_out)`).
Neither imports pagexml or pandas at module level (tested with the heavy modules blocked).
Tests: 22 in the three targeted files (9 io, 8 word breaks, packaging 5 including the new one);
full suite 908 passed (890 before). Scripts:
[`data/phase-05-text-layer/step-14/`](data/phase-05-text-layer/step-14/) (`check.py`, `ea.py`,
`pairs.py`). V1 sizes and mtimes of `wbc/*` and `manifest.json` were unchanged after all runs.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | `corpus_default.json`, `wbc_per_inventory.tsv` vs v1 | 2/2 byte-identical; default `['¬', '„']`; `differs_from_default` 1,461; `whole_default` 172; 1 inventory without stats rows |
| 2 | `wbc_stats.tsv` rewrite with `format_stat` | byte-identical |
| 3 | 52 stats parts from v1 lines | 52/52 identical (15 s in total) |
| 4 | sort order vs NLP's key | equal on 6,914/6,914 names |
| 5 | full suite, protected files | 908 passed; v1 sizes and mtimes unchanged |

**Error analysis.** Read: the `wbc_set` tabulation over all 6,914 inventories, the deviation
categories for all 1,461, the near-threshold counts over 45,976 rows, and 10 deviating
inventories sampled with `random.Random(14)` from the sorted deviating list (1068, 8575, 9840,
9183, 7724, 2914, 3132, 3008, 3248, 10502), each with its per-char statistics and a line pair
read from the lines sidecar (the fifth attached occurrence of the decisive char, streamed, not
read in full).

- `wbc_set` top 10: `¬„` 5,453; `¬` 590; `=¬„` 588; `-=¬„` 63; `=¬` 63; `„` 44; empty 32; `-¬„` 19; `!¬„` 8; `=¬—„` 7. All 1,461 deviating inventories are in `per_char` mode.
- Deviation categories: adds a char only 717 (`=` in 672 of the 795 that add, `-` 89, `:` 14, `!` 12, `—` 11, `.` 9, `/` 1, `,` 1; counts overlap because some add several), drops `„` only 590, drops `¬` only 44, drops both 32, adds and drops 78. Adds of `.` and `:` are at most 23 inventories, against the sanity rule that rejects them pooled.
- Near threshold (no tuning): 574 rows within +-10% of 0.25 on `join_vocab_frac`, 3,412 within +-10% of 5.0 on `end_rel_frac`; those where the other criterion passes: 238 and 446. By char the flippable ones are mostly `„` (125 and 299) and `=` (12 and 115).
- Sample of 10 (4 plausible, 6 suspect):
  - plausible, adds `=`: 8575 (`en provizie en tot nadere ordre van uw=` | `HoogEdelens, derzelver…`), 9840 (`dria pas=` | `d'aanplanters`; the next line does not continue the word, but `join_vocab_frac` 0.42), 9183 (`geklommen oor=` | `spronkelijk, door het…`), 3248 (`met dit oog=` | `=merk over, …`: a mark on both sides of the break).
  - suspect, a genuine mark dropped: 7724 `„` (`geres„` | `pecteerde famielle…`; `join_vocab_frac` 0.210 < 0.25), 2914 `„` (`end_rel_frac` 4.36 < 5.0, `join_vocab_frac` 0.71), 3132 `„` (`de„` | `„sen als de…`; 3.82), 3008 `„` (`Domi„` | `nicus von staaden…`; 4.90 against 5.0), 10502 `„` (`join_vocab_frac` 0.235 with 5.2k lines, while `=` is added correctly: `Fon=` | `taine`), 1068 `¬` (`adel¬` | `vervoert heeft…`; `join_vocab_frac` 0.206, `end_rel_frac` 40.8: the join `adelvervoert` is not a word, so this one is ambiguous).
  - 9840 and 8575 show that the pair read from the sidecar is not always a continuation (the next line is a different region part), which `join_vocab_frac` handles by being a fraction, not a certainty.
- Conclusion: the port is exact. The detector is conservative: where it deviates it adds `=` plausibly, but in 590+32 inventories it drops `„`, which in the sample was a genuine break mark failing `end_rel_frac` or `join_vocab_frac` narrowly. A mark that is frequent overall has a low enrichment ratio (`„` ends 1,004 attached lines of 39,698 in 3008 and still scores 4.90). Not tuned: this is a finding for the phase review, and for a v2 rule.

**Brief defects.** (1) `read_region_lines` is described in the ported docstring as returning
`(scan_id, region_id, texts)`, but it returns `(row_dict, texts)` where `row_dict` is the
`regions_meta` row; the docstring was kept and the tests use the real shape. (2) The brief's
`<scratchpad>/step14/check.py` is run with `-I`; that works because `archival_structures` is
installed editable in `.venv`. (3) The `¬` test data needs letter-before-mark for `:` lines
(the first draft used a digit, so `:` had no `n_end_attached` row); no brief change needed.

### Step 15: text-layer library

**What changed.** New `text_layer/extract.py` (stage 1; `region_tag_of` takes `untagged_tag`;
`scan_id_matches` replaces the `_{inv}_` test and `extract_inventory` takes `untagged_tag` and
`scan_id_prefix`), `join.py` (stage 3, verbatim), `provenance.py` (`package_info` generalises
`pagexml_tools_info`; `check_min_version`) and `presets.py` (`LayerPreset`, `VOC_V1`, `PRESETS`,
`predict_*` with the fit passed in). No pagexml or pandas import at module level (tested). Tests:
`test_text_layer_join.py` 23 (ported), `test_text_layer_extract.py` (fixture `9999A`, 4 files,
`scan_id_matches` cases, `region_tag_of`), `test_text_layer_provenance.py` (includes the
`predict_wall` hand computation), packaging 5. Targeted run: 73 passed; full suite 959 passed
(908 before). Scripts: [`data/phase-05-text-layer/step-15/`](data/phase-05-text-layer/step-15/)
(`check.py`, `ea.py`). Protected state (sha256 of 7,079 files; size and mtime of v1 `wbc/`,
`manifest.json`, `status.tsv`) was unchanged after every run.

**Results against the acceptance criteria.**

| # | Criterion | Result |
|---|---|---|
| 1 | 22 inventories byte-identical to NLP's reference | 157/157; 35 s extract and parts, 4 s decide and join (8 workers) |
| 2 | A/B on 20 more inventories | 140/140 equal; wall 40 s (8 workers) |
| 3 | `scan_id_matches` False for 0 ids across v1 | met after the user's decision (below): 0 of 4,798,666 ids in 6,914 inventories mismatch, 10 s. With the first, strict 4-digit version 98 ids in 3 inventories failed |
| 4 | tests, suite, protected hashes | 73 targeted and 959 full passed (after the decision); 157/157 re-run: 157/157 in 42 s; hashes unchanged |

**Error analysis.** Reading: (a) the 98 failing scan ids; (b) the rebuilt layer (22 + 20
inventories) against v1 ignoring order; (c) 10 regions whose line order differs
(`random.Random(15)` over all 12,834, differing window printed and read); (d) 5 scans whose
region order differs (same seed over 9,057), compared by paragraph top-y; (e) the paragraph
top-to-bottom check over all 18,801 scans.

- (a) The three families are id formats the 4-digit `ScanId` did not accept: `..._10090_64.jpg`
  (95 ids, two digits), `..._1186_0571A/B.jpg` (2), `..._9817_P01.jpg` (1). User decision: parse with `ScanId(digits=None)` and fall back to the `_{inv}_` substring test only for ids that do not parse (`1186` `0571A`, `9817` `P01`); a parseable id with the wrong prefix or inventory is still False.
- (b) Line multisets per region equal in 42/42, region key sets equal 42/42, `regions_meta` values
  equal 42/42. Line order differs in 12,834 of 313,209 regions (planning: 8,626 of 160,132 on 34
  inventories, 5.4%; here 4.1%); 12,173 texts differ, every one in a region with a different line
  order. Scan order never differs; region order within scans differs in 9,057 scans. By line-id
  sequence, 8,520 order differences are "one line moved" (removing one element makes the
  sequences equal) and 4,314 are other permutations.
- (c) Sample of 10: 6 have one or two isolated short lines moved far, mostly to the region end,
  with the rest shifting up (1267 `Batavia`; a dateline `Den 16„' April in de Stad Cochim.` in
  10359 moved from first to last; 1120 an empty line and `9`; 10115 the page number `108`; 10359
  `—` lines; 8809 `Palioolse advancen. . . ƒ 90. 16. 12.`); 4 are adjacent transpositions
  (10115_0650, 2555, 9541 `„met`, 1274 `gedaan` / `Ende gepasseert`). No column interleaving, no
  "other". Verdict per region: v1 is right in 9 (1274: v1 `en cencere waerheijt, aldus — | gedaan
  | Ende gepasseert, opt`, rebuilt puts `Ende gepasseert` before `gedaan`; 10115_0650: v1
  `na dat | de geinterrog: aan Le Maitre | een enkelde ordre gegeven` reads as a sentence, rebuilt
  splits it; 2555: v1 `dagelijxe omme gang gehad heeft | gelijk deselve ...`), undecidable in 1
  (1267, the `Sambij` swap).
- (d) In 5/5 sampled scans v1's paragraph regions run top to bottom (9626A_0013 top-y 613, 898,
  1843, 2617) and the rebuilt order moves one region to the end or reverses them (613 last;
  1274_0252: 449, 2210, 2201, 2142 against v1 449, 2142, 2201, 2210).
- (e) Over all 18,801 scans with two or more paragraph regions: v1 monotone 18,273 (97.2%),
  rebuilt 9,623 (51.2%).
- Conclusion: the port is exact on both references. The 2023 export has the same lines and texts
  but a less coherent order, which changes the running text in 4.1% of regions, so a layer built
  from the local zips serves tests and development, not as a replacement for v1. The 98 scan-id
  failures are an input-format question. This reading is by text and box position; no scan image
  was consulted.

**Deviation (user decision).** `scan_id_matches` is not the brief's version: it uses `ScanId.try_parse(digits=None)` with the substring fallback for non-parsing ids (F05-15.3).

**Brief defects.** (1) Acceptance 3 expects 0 mismatches with the brief's `scan_id_matches`; the data holds 98 irregular ids (F05-15.3). (2) The
error analysis quotes planning counts for 34 inventories but asks for 42; the numbers above are
for 42. (3) The A/B needs NLP's `wbc_stats_part` and `join_inventory` names, which the brief does
not list. (4) The brief's range for `join_inventory` ends at L842, the function ends at L843;
ported whole. (5) "About 4 min in one process" is 39 s with 8 workers for the 22.

### Step 15c: text-layer driver and CLI

**What changed.** New `archival_structures.text_layer.build` (NLP's `scripts/build_voc_text_layer.py`
ported: same flags, log lines, status columns and skip rules; plus `--preset`; `FORCE` global became
`Run.force`; manifest gains `archival_structures` and `preset`, `git_head` is AS's) and the console
script `as-build-text-layer` (`pyproject.toml`; registered with `.venv/bin/python -m pip install
--no-deps -e .`). New `text_layer.compare` (`compare_layers`, `compare_run_files`, `summarise`,
`main`; pandas imported lazily so the module imports without pandas). New `docs/text_layer.md`
(Guides toctree) with the v2.1 recipe. Tests: `tests/test_text_layer_build.py` (9),
`tests/test_text_layer_compare.py` (7), packaging test extended. Brief:
[`step-15c-build-driver.md`](../plan/phase-05-text-layer/step-15c-build-driver.md).

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. CLI reproduces `<REF>` (22 inventories, `--stage all --write-events --workers 8`) | 110 TSV byte-equal, 44 JSON equal, 3/3 run files, 157/157 | 110/110, 44/44 (22/44 also byte-equal: the 22 `join_stats`; the 22 `extract_stats` differ only in `source`), 3/3; **157/157**; wall 34.6 s (extract 29 s, wbc 2 s, join 4 s from `progress.log`) |
| 2. Manifest superset | ref keys within new keys, new minus ref = {`archival_structures`, `preset`}; layer-level values equal | true for extract, wbc and join; layer-level `layer`, `row_order`, `columns`, `format`, `untagged_region_tag` equal and the layer key sets are equal |
| 3. Resumable | 0 to do; no mtime changes under `lines/`, `regions/` | `[extract] 22 already done, 0 to do`, `[wbc] 22 stats parts already done, 0 to do`, `[join] 22 already done ..., 0 to do`; mtimes of the 44 files unchanged; 0.4 s |
| 4. v2.1 recipe locally (`<IN>` = `<NLP>/pagexml`, 1120,2555,8834, wbc files from `<REF>`) | exit 0 | exit 0, `IDENTICAL (15 comparisons)`: lines, regions_meta, regions byte-equal 3/3 each, `extract_stats` and `join_stats` equal 3/3; 18 s |
| 5. `--help`, `--estimate` | 13 NLP flags + `--preset`; prediction printed | 14 option lines (`--allow-partial --estimate --force --ignore-sanity --inventories --inventories-file --limit --out-dir --pagexml-dir --preset --stage --wbc-stats-only --workers --write-events`); estimate prints "all three stages: 0.01 h (0.5 min) on 8 workers" |
| 6. Tests, protected hashes | green; unchanged | targeted 89 passed, full AS suite 975 passed (959 before); 7,079 protected files (sha256 of `<REF>`, size+mtime of `<V1>` `wbc/`, `manifest.json`, `status.tsv`), 0 differences after all runs |

**Error analysis.**

- *What was read:* the whole manifest diff (3 stages x every key, `check.py`), every non-progress
  line of `progress.log` (9 lines), the stdout of the first run for anything outside the log
  format, and the recipe's `compare` output per kind.
- *Categories (manifest, per stage; all 3 stages alike):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | New keys, as designed | 2 | `archival_structures: {git_head: f680648..., git_dirty_files: ["archival_structures/text_layer/"], version: "0.8.0", path: ...}`, `preset: "voc_v1"` |
  | `git_head` now AS's | 1 | `899a79e...` (NLP) -> `f680648...` (AS) |
  | `pagexml_tools` value | 1 | ref: editable 0.8.0 at `0f39fad`, 4 dirty files; new: `{version: "0.8.1", path: .../site-packages/pagexml}`, no git state |
  | Run environment | 3 | `python` 3.11.16 -> 3.11.17; `finished_utc`; `platform` equal |
  | `argv`, `params` | 2 | `pagexml_dir`: `pagexml` -> absolute path; `out_dir`; `force`: true -> false; `preset`: absent -> `voc_v1` |

  Non-progress `progress.log` lines: `start`, per stage "N already done, M to do" and "N
  inventories, 8 workers", the `[wbc] corpus default ['¬', '„'] (pooled rule ['¬', '„']),
  sanity_pass=True, 3/22 inventories differ` line (equal to the reference's decision), `end
  stage=all failures=0`. No FAILED line, no warning. The pooled-distribution table goes to stdout
  only, as in NLP.
- *Estimate vs measured:* see F05-15c.3. The fit comes from one laptop and was not refitted. The
  small stages are underestimated more (wbc 1.8x, join 1.4x) because pool start-up and
  per-process imports do not scale with bytes, but they are 6 s of 35.
- *Conclusion:* the port changes nothing in the layer. The only behavioural change is the
  provenance in the manifest, which is additive. The manifest's `pagexml_tools` entry loses
  `git_dirty_files` when pagexml-tools is a PyPI install (nothing to report), so a v2.1 build from
  the release is distinguishable from a dirty checkout by that key's absence. A rerun still
  overwrites the manifest entry (F05-15c.4); this matches NLP and was kept (port, don't improve).

**Findings:** F05-15c.1-4.

**Open issues.** F05-15c.4 (manifest records the last invocation). The user runs the v2.1 recipe
(`docs/text_layer.md`, "Checking a rebuild against v1") on the external disk.

**Brief defects.** (1) `.venv/bin/pip` does not exist; the install ran as `.venv/bin/python -m pip
install --no-deps -e .`. (2) The brief says 157/157 are "110 TSV + 44 JSON + 3"; that is 157, but
the 22 `extract_stats` JSONs are byte-equal only after dropping `source`, so "byte-equal" holds
for 110 + 22 + 3 = 135 files. (3) The brief's compare spec leaves `compare_run_files` row labels
open; the `inventory_number` column holds the relative path and `kind` is `run_file`. (4) The
no-pagexml test in `test_packaging_core_imports.py` also asserts that pandas is not imported, so
`compare` imports pandas lazily (the brief says only "no pagexml import"). (5) The recipe lists
`<NLP>/data/voc_text_layer_v1` as the compare target; all 20 recipe inventories have a row in v1's
`wbc_per_inventory.tsv` (checked), so `join` needs no `--allow-partial`.

### Step 13: pagexml-tools 0.9.0

**What changed.** The release is 0.9.0, not 0.8.2 (brief amendment). Pins in `pyproject.toml`:
`pagexml-tools (>=0.9.0,<1.0.0)`, `fuzzy-search (>=3.0.0,<4.0.0)`. AS `.venv` installed with
`.venv/bin/python -m pip install "pagexml-tools==0.9.0"` (nothing else). New test
`test_pagexml_tools_is_at_least_0_9_0`; the nested-region test renamed (F05-13.3); one
neighbourhood test updated (F05-13.5); `docs/text_layer.md` limits heading; changelog entry.
Brief: [`step-13-pagexml-tools-pins.md`](../plan/phase-05-text-layer/step-13-pagexml-tools-pins.md).

**Environments (pagexml-tools / fuzzy-search / lxml / nltk / py7zr / pytest).**

| Env | Before | After |
|---|---|---|
| AS `.venv` | 0.8.1 / 2.5.0 / 5.4.0 / 3.9.2 / 0.20.8 / 8.4.2 | 0.9.0 / 3.0.4 / 6.1.0 / 3.10.3 / 1.1.3 / 9.1.1 (plus pyppmd 1.3.1) |
| NLP `venv` | metadata 0.8.0 (code 0.9.0 from checkout) / 3.0.4 | unchanged |
| LotA `venv` | metadata 0.8.0 (code 0.9.0 from checkout) / 3.0.4 | unchanged |

Import check (`reports/data/phase-05-text-layer/step-13/envcheck.py`, siblings with
`PYTHONNOUSERSITE=1`): `pagexml.helper.pagexml_helper` and `LineCharAnalyser` import in all three,
before and after. The siblings already imported before this step because the user had checked out
the release; the checkout is at `c112a28` (contains `3e751c8`, "set version to 0.9.0").

**Results against the criteria.**
1. Imports OK in three environments; `pagexml.__version__` 0.9.0 in all three (siblings' metadata 0.8.0, F05-13.4).
2. `as-build-text-layer --stage all --inventories <22> --write-events --workers 8` into a fresh scratch dir: exit 0, 35.9 s wall, `check.py`: TSV byte-equal 110/110, JSON equal 44/44 (byte-equal 22/44, the `source` key as in 15c), run files 3/3, total 157/157, no differing rows; manifest differences as in 15c.
3. Pins updated; provenance test asserts >= 0.9.0.
4. AS targeted (`tests/test_text_layer_*.py`, `test_analysis_text_analysis.py`, `test_packaging_core_imports.py`) 100 passed; AS full suite 976 (975 + the new provenance test): first run 975 passed, 1 failed (F05-13.5), then green. NLP 29 passed, 21 passed. LotA `test_derive_voc.py` 11 passed; full suite 327 passed.
5. Protected state (sha256 of `<REF>`, size and mtime of `<V1>/wbc`, manifest, status): 7,079 files, 0 differences.

**Error analysis.** The 0.8.1 to 0.9.0 diff of `pagexml/` (9 files, 237 diff lines) read hunk by hunk:

| Class | Hunks | Where |
|---|---|---|
| import-only / cosmetic | 9 | `__version__`; `orjson as json` in `parser.py`, `text_stats.py`, `pagexml_document_model.py`; `fuzzy_search.analysis.similarity` in `text_helper.py`; import reordering and signature reformatting in `pagexml_document_model.py`; `Counter[int]` annotations in `parsers/column_parser.py` |
| bug fix | 3 | `get_doc_stats` sets `text_regions` = 0 when missing (`stats.py`, `7b2b721`); `get_line_format_json` no longer reads `num_text_regions` (uses `stats`, `text_helper.py`); `vertical_distance` no longer returns the baseline-bottom offset (`physical_document_model.py`) |
| behaviour change | 2 | `column_parser.py`: derived columns now wrap their lines in a `PageXMLTextRegion` (`make_derived_column`, `make_column_range_columns`) |
Not touched: `parser.py` apart from the import; the text-line, region and `get_all_text_regions` code paths AS uses. The orjson switch changes `json.dumps` to return bytes in modules that use it; none of these is on the AS extract path (reproduction 157/157 confirms). So the 6,892 inventories not rebuilt are expected to be byte-identical: the extract reads the same attributes through unchanged parser code. The behaviour changes affect only column-derivation code, which AS does not call. This is argued from the diff and the 22-inventory reproduction, not measured on them.

The one AS failure came from reading the 0.9.0 change: `test_gap_is_zero_for_overlapping_slanted_baselines` asserted the buggy upstream value 20 as a "regression test" of AS's workaround; with the fix upstream the value is 0. The assertion was flipped; the workaround test (`line_vertical_gap` equals 0) was unaffected. The docstring in `analysis/neighbourhood_analysis.py` L74 still describes the bug as present; left as is (it describes why the helper exists).
Sibling tests: all pass; LotA `test_derive_voc.py` needed no change, so none of the earlier failures was a 0.9.0 regression.

**Findings:** F05-13.1-5.

**Open issues.** F05-13.3 (nested regions, upstream), F05-13.4 (stale metadata in sibling environments).

**Brief defects.** (1) The brief's deliverables and tests still say 0.8.2; the amendment covers it. (2) It did not say to update `tests/test_analysis_neighbourhood_analysis.py`, which pinned 0.8.1's `vertical_distance` bug. (3) The siblings' `CLAUDE.md` were not read: this step edits no sibling file. (4) Backgrounding with `nohup` fails in the agent shell ("can't detach from console"); the reproduction ran in the foreground.

### Step 15b: NLP text-layer shim

**What changed (all in NLP).** `src/startpage/voc_text_layer.py` (879 lines) is now a 4-function
shim over `archival_structures.text_layer` (all 62 top-level names of the original resolve; the
originals are saved under `output/as-phase-05-originals/{src,scripts}/`). `scripts/build_voc_text_layer.py`
is the wrapper from the brief. New `scripts/verify_as_text_layer.py` (modes `current`, `originals`,
`both`; each build in a subprocess with cwd = NLP). `validate_voc_text_layer.py` and
`tests/test_voc_text_layer.py` untouched. No AS code changed; AS changelog gets a bullet under Step 15.
Brief: [`step-15b-nlp-text-layer-shim.md`](../plan/phase-05-text-layer/step-15b-nlp-text-layer-shim.md).

**Results against the criteria.** Environment: NLP `venv`, `PYTHONNOUSERSITE=1`, pagexml-tools 0.9.0
(checkout), 8 workers.

| Check | Result | Time |
|---|---|---|
| builds | `current` exit 0, `originals` exit 0 | 32.7 s, 33.6 s wall |
| 1 stored (current vs `<REF>`) | 110/110 TSV byte-equal, 44/44 JSON equal, 3/3 run files = **157/157** | 2.5 s |
| 2 A/B (current vs originals) | **157/157**; manifest layer values equal; per stage new keys = {archival_structures, preset}, lost keys none | 2.3 s |
| 3 decide (shim `decide_wbc` on v1 stats) | **2/2** identical; 6,914 inventories, default {¬, „}, 1,461 differ, 172 whole default | 1.5 s |
| 4 names | 17/17 importer names resolve | |
| 5 protected | 41,692 files (`<REF>` sha, `<V1>` size/mtime plus sha of `wbc/*`, manifest, status, validation dir), 0 changed | |

Tests: NLP `test_voc_text_layer.py` 29 passed; `test_evaluation.py` + `test_semi_markov.py` 21 passed;
`validate_voc_text_layer.py --help` exit 0; `grep -c "^def "` on the shim = 4 (the thin wrappers);
AS full suite 976 passed.

**Error analysis.** (a) Manifest, current vs originals, every differing value per stage (extract,
wbc, join; the same in each): `archival_structures`, `preset` (new keys); `finished_utc` (timestamp);
`git_head` (AS HEAD vs NLP HEAD, F05-15c.2); `argv` and `params` equal once `--out-dir` and the new
`preset` param are removed. Category "other": none. (b) `progress.log`, 75 lines each: after
removing timestamps, durations and the `n/22` counter the multisets of lines are equal (0 lines only
on one side); the raw diff is only the completion order of the parallel pool (e.g. 1529 before 1388).
(c) Both logs say `pagexml-tools=0.8.0`: the stale dist-info metadata (F05-13.4) is still what
manifests record in NLP. (d) Not sampled: individual output rows, because the outputs are byte-equal.

**Findings.**
F05-15b.1: the shim and wrapper reproduce NLP's stored build 157/157 and a run of the original code
157/157 (32.7 s and 33.6 s); decision 2/2. F05-15b.2: the manifest produced through the wrapper is
the originals' plus `archival_structures` and `preset` per stage (behaviour change of F05-15c.2, now
visible in NLP); `git_head` is AS's. F05-15b.3: NLP manifests still record pagexml-tools 0.8.0
until the user's reinstall (F05-13.4).

**Brief defects.** (1) "Verify" did not say where `<scratchpad>` is when run by a reviewer; used the
given step15b scratchpad. (2) The runner for the originals needs `sys.argv` set (the original script
records `argv`); without it the manifest `argv` differs (fixed in the verify script). (3) Columns
for `wbc_per_inventory.tsv` in check 3 were "as above" in a step-14 brief the session did not read;
taken from `build.stage_wbc`. (4) `rm -rf` of a scratch dir is not allowed in the agent shell; a
second out-dir was used.

**Open issues.** F05-13.4 (stale metadata), D5 data move (user).

### Addendum: step 15r (review fixes)

Run on 2026-10-15 by a Sonnet 5.5 session, after the phase review ([brief](../plan/phase-05-text-layer/step-15r-review-fixes.md)). The review is final; no finding status was changed and no finding row was added.

**What changed.**
- `analysis.sequence_patterns.segment_into_elements` uses a new helper `_is_unlabelled`, so NaN and `pd.NA` labels count as unlabelled like `None` and `-1` (F05-R.2). New test `test_nan_labels_break_a_run` (object Series holding NaN).
- `tests/test_datasets_globalise_index.py::test_load_all` asserts `pd.api.types.is_string_dtype(...)` instead of `dtype == object`. `load_globalise_index` is unchanged.
- `text_layer.provenance.MIN_PAGEXML_TOOLS` is `(0, 9, 0)` (F05-R.6). The `build.py` docstring says 0.9.0, and the provenance test is renamed `test_check_pagexml_tools_version_uses_minimum` and asserts the constant.
- Docs (F05-R.5, D5): `docs/text_layer.md` uses `<DS>/shared-data/voc_text_layer_v1` (`<NLP>` is gone) and has the new "never build into an existing shared layer with a subset" paragraph. The `build.py` docstring has the same warning.
- Changelog: the step 13 bullet on `MIN_PAGEXML_TOOLS` is updated and a `### Step 15r` section is added (affected sibling imports: NLP `startpage.voc_text_layer.MIN_PAGEXML_TOOLS` and `check_pagexml_tools_version`).

**Results.**

| Check | Result |
|---|---|
| AS `pytest --continue-on-collection-errors tests` | 799 passed, 2 skipped (torch), 0 failed, 6 collection errors (matplotlib x4, ipywidgets x2) |
| `as-build-text-layer` on the 22 inventories (8 workers) | exit 0, `failures=0`; `check.py`: `TOTAL 157 / 157` |
| NLP `verify_as_text_layer.py` | `ALL PASS`, exit 0 |
| NLP `test_voc_text_layer.py` | 29 passed |
| NLP `test_evaluation.py test_semi_markov.py` | 21 passed |

Before 15r the suite gave 796 passed, 2 skipped, 2 failed. The three extra passes are the two repaired tests and the new test.

**Error analysis.** Under pandas 3.0.6, `pd.Series({...: 'body', ...: None, ...: 'body'})` has dtype `str` and stores the middle label as `nan`. Labels per line, old test (the unchanged `test_unlabelled_lines_break_a_run`): l0 `'body'`, l1 `nan`, l2 `'body'`; `_is_unlabelled` gives False, True, False and 2 elements, `body` [l0] and `body` [l2]. The pre-15r code (re-run from `HEAD`) gives 3 elements for both tests: `body` [l0], `nan` [l1], `body` [l2]. It treats NaN as a label, so the unlabelled line becomes a spurious one-line element of type `nan`. New test (object Series with `float('nan')`): the same labels and the same 2 elements, so both paths hit the NaN branch. `pagexml-tools` metadata versions (with `PYTHONNOUSERSITE=1`): AS `.venv` 0.9.0 (pandas 3.0.6), NLP `venv` 0.9.0 (pandas 2.3.3), LotA `venv` 0.9.0 (pandas 3.0.6).

**Brief defects.**
- Verify 1 asked for at least 976 passed with 0 errors. That count was step 13's with all modules collectable. Six modules cannot be collected in this `.venv` (matplotlib, ipywidgets not installed), so the reachable number is 799 passed. STATUS.md said 7 uncollectable modules (including torch x1). This run has 6 errors and 2 torch skips; both torch-dependent modules skip through `importorskip`, so no torch collection error occurs. The cause of the old count of 7 was not investigated.
- The brief's new wording for the `segment_into_elements` docstring made that line longer than 100 characters, so it was rewrapped.
- The brief did not say where `inv22.txt` ends (newline or not); `check.py` strips it, so it does not matter.

**Note for the user.** Four AS test modules need matplotlib and two need ipywidgets, so the AS suite still has 6 collection errors until those packages are installed (both are in the `[notebook]` extra: `.venv/bin/python -m pip install -e '.[notebook]'`). They are unrelated to this step.

## Review

Phase review (Opus, 2026-10-15). The code review is in
[`REVIEW.md`](../plan/phase-05-text-layer/REVIEW.md); verdict: accept with follow-ups, with
the review-fixes step 15r to run before the release.

**Acceptance, re-run by the reviewer.** The checks ran sequentially from one script, with the
protected NLP trees snapshotted before and checked after (7,079 entries, 0 differences):
- step 14 `check.py`: decision files 2/2, stats rewrite identical, parts 52/52, sort order
  6,914/6,914;
- step 15 `check.py`: 22 inventories 157/157; A/B 140/140; `scan_id_matches` 0 of 4,798,666;
- step 15c: the CLI on the 22 157/157 (36.6 s, 8 workers); manifest a superset in all three
  stages; the rerun a no-op (0.42 s, 44 mtimes unchanged); the recipe `IDENTICAL (15
  comparisons)`; `--estimate` 0.5 min;
- step 13: the joiner imports in all three environments, and all now report pagexml-tools
  0.9.0;
- NLP `verify_as_text_layer.py`: exit 0, ALL PASS. The first run failed check 5 because the
  user's D5 data move ran at the same moment; the second, after the move, passed;
- NLP `test_voc_text_layer.py` 29 passed, `test_evaluation.py test_semi_markov.py` 21 passed;
  LotA suite 327 passed.

AS's full suite could not be re-run as logged. Since the user's reinstall of 2026-10-15,
AS's `.venv` lacks pytest's `py.py` shim and the `vision` extra. With the shim supplied from
the scratchpad:
- the suite gives 737 passed, 2 failed (pandas 3, F05-R.2) and 13 modules uncollectable;
- the text-layer tests give 101 passed and 1 skipped (torch).

**Findings.** All 28 planning and step findings were checked against the code or a re-run:
27 are confirmed and 1 is revised. F05-14.4 has 5 genuine `„` drops, plus one ambiguous `¬`.
F05-P.5, F05-13.4 and F05-15b.3 are confirmed and marked resolved. The review adds F05-R.1 to
F05-R.7.

**Leakage.** The phase has no learned model and no split. Its acceptance numbers are byte or
JSON comparisons against stored outputs. The one calibrated component, the word-break rule,
was fixed on the 22 annotated inventories from text statistics, without start-page labels. The
error-analysis samples (step 14 seed 14, review seed 505) contain none of those 22.

**The reviewer's own error analysis.**
- *Word breaks.* The six statistics step 14 quotes (for 3008, 2914, 7724, 10502, 3132 and
  1068) reproduce from v1's `wbc_stats.tsv`. All 696 per-char inventories without `„` were then
  split by the criterion that fails (F05-R.4). Six of them, sampled with seed 505 (3427, 4398,
  7565, 4434, 7838, 9603), were read through 3 attached `„` line ends each:
  - real breaks: `gequali„ | ficeert`, `pa„ | pieren`, `per„ | zoonen`, `nee„ | =men`,
    `even„ | „gem:`, `gene„ | „raal`, `be„ | „scheiden`, `bloe„ | men`, `djaja„ | diningrat`,
    `begun„ | „stigt`, `bewindheb„ | beren`, `genereu„ | „se`, `souwwae„ | „ijen`;
  - not a break: `Comptoir„ | van`, the signature line `Eij„ | Samarang`;
  - ambiguous: `uitgele„ | gedepecheerd`, `optile„ | maar`, `Op„ | n`.

  So the rule loses a real join in most of these line ends. The cause is the enrichment ratio,
  not the evidence threshold: these inventories have many attached occurrences (51 to 1,621).
  This is recorded, not tuned.
- *Input order.* On the 22, every one of 190,954 regions has the same line multiset in v1 and
  in the rebuild. Four regions whose line order differs were read (seed 505):
  - 8834: v1 `tuijn bestonden al„ | „leen in 23 p„s diese | tot portonavo` joins to *alleen*.
    The rebuild moves `„leen…` to the region end, so the join is lost;
  - 1547: v1 `1: rolleser — | abdul van battavia | 1: tamboer | andre van Colombo` keeps each
    rank with its name. The rebuild moves the first rank line down;
  - 1274 (table fragments) and 1539 (a stray fragment `rmijn`): undecidable.

  This agrees with F05-15.5. Line order affects the join itself, not only reading.
- *Manifests.* The review's NLP-wrapper manifest records `pagexml_tools.version` 0.9.0, so
  F05-15b.3 is resolved for pagexml-tools. The same manifest records `archival_structures.version`
  0.1.0, from NLP's stale dist-info (F05-R.3).

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F05-R.2 | Make `segment_into_elements` treat NaN/`pd.NA` labels as unlabelled; relax the `globalise_index` dtype test | step 15r (before the release) |
| F05-R.6 | `MIN_PAGEXML_TOOLS` → 0.9.0 | step 15r |
| F05-R.5 | Document the subset-run hazard (15r). Then a guard in the driver: refuse to overwrite the run-level `wbc/` decision when the run's inventories are a strict subset of the layer's, unless `--force` | step 15r (docs); phase 06 (guard) |
| F05-R.3 | Reinstall AS editable (`pip install --no-deps -e <AS>`) in NLP's and LotA's `venv`, so manifests record the right version | user (phase-01 precondition) |
| F05-R.4, F05-14.4, F05-14.3 | A v2 word-break statistic that is robust to a mark's mid-line use (e.g. enrichment among letter-attached occurrences only), measured against the v1 rule on a labelled sample before adopting it; with the planned region-end statistic | phase 06 |
| REVIEW #7 | `LayerPreset` carries the word-break rule and the sanity anchors, so the SG preset can have its own | phase 06 |
| F05-15c.4 | Keep a per-invocation history in the manifest instead of overwriting the stage entry | phase 06 |
| F05-13.3, F05-P.7 | Report the nested-`TextRegion` `set += set` bug (`get_all_text_regions`, L447) upstream | user (pagexml-tools) |
| REVIEW #11 | AS `.venv`: restore pytest's `py.py` (`pip install --force-reinstall --no-deps pytest==9.1.1`) and the `vision` extra (`pip install -e '.[vision]'`) | user (precondition of 15r) |
| D5 | Update `shared-data/README.md`: v1 has moved; producer AS `text_layer`, preset `voc_v1` | user |
| step 14 brief defect | `io.read_region_lines` docstring describes the wrong return shape | a later AS step |
| packaging | `poetry lock` (pins changed in step 13) | user |

## Reproduce

All commands are run from the repo root named. `<NLP>` is
`/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`. The planning scripts expect
`vtl_orig.py`, a copy of `<NLP>/src/startpage/voc_text_layer.py`, next to them in a scratch
directory.

- F05-P.2: `.venv/bin/python -I proto22.py <scratch>/out22`.
- F05-P.3: `.venv/bin/python -I wbc_decide.py <scratch>/decide`.
- F05-P.1: `.venv/bin/python -I vs_v1.py <scratch>/out22`.
- F05-P.7: `.venv/bin/python -I bisect.py nested_fixture.xml`.
- F05-14.1: `.venv/bin/python -I reports/data/phase-05-text-layer/step-14/check.py <scratch>/out`; F05-14.2-4: `ea.py`, `pairs.py` in the same directory.
- F05-15.1-3: `.venv/bin/python -I reports/data/phase-05-text-layer/step-15/check.py <scratch> [ref] [ab] [scanid]` (run `scanid` with a separate `<scratch>/sid`); F05-15.4-5: `ea.py <scratch>` after `ref` and `ab`.
- F05-15c.1-4: from the repo root, with `<scratch>/inv22.txt` holding the 22 inventory numbers comma-separated, run the criterion 1 command (`.venv/bin/as-build-text-layer --pagexml-dir <NLP>/pagexml --out-dir <scratch>/r22 --stage all --inventories $(cat <scratch>/inv22.txt) --write-events --workers 8`), then `.venv/bin/python -I reports/data/phase-05-text-layer/step-15c/check.py <scratch>` (criteria 1-2, manifest diff, log lines), rerun the same command for criterion 3, `sh reports/data/phase-05-text-layer/step-15c/recipe.sh <scratch>/r3` (criterion 4), and `protect.py snapshot|verify <json>` around the runs.
- Review (F05-R.1–7): `sh reports/data/phase-05-text-layer/review/run_all.sh <scratch>`
  re-runs every acceptance check above in sequence (siblings with `PYTHONNOUSERSITE=1`); NLP's
  `verify_as_text_layer.py` exit code must be read on its own (the script pipes through
  `tail`). F05-R.4: `.venv/bin/python -I reports/data/phase-05-text-layer/review/ea_wbc.py`.
  Input order: `ea_order.py <layer root of a 22-inventory rebuild>`. F05-R.1 is the user's P3d
  run (`docs/text_layer.md`, *Checking a rebuild against v1*).
- F05-13.1-5: `.venv/bin/python -I reports/data/phase-05-text-layer/step-13/envcheck.py` with each of the three interpreters (siblings with `PYTHONNOUSERSITE=1`); then the criterion 2 command of F05-15c.1-4 into a fresh `<scratch>` and its `check.py`; `git -C <pagexml-tools> diff d1f0838 HEAD -- pagexml/` for the hunk classification.
