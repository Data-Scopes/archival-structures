# Phase 05: Text layer (v1 producer into AS)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`). Created and planned 2026-10-14 (Opus) from
> `docs/shared_library/upstream_inventory.md` C14, C15 and §4 steps 13–15.

## Goal

AS gets the text-layer producer behind NLP's `voc_text_layer_v1` as a new core subpackage
`archival_structures.text_layer` (I/O, extract, per-inventory word-break detector, join,
provenance, a resumable build driver with the console entry point `as-build-text-layer`, and a
layer comparison tool). The VOC values are a named preset. AS's pins move to the pagexml-tools
release that the user cuts for this phase (0.8.2). NLP's `voc_text_layer.py` becomes a shim and its
build script a wrapper, and both **reproduce NLP's stored local build byte for byte**. v2 (line
geometry) and the SG preset are the next phase (P1).

## Owns

- **AS:** `archival_structures/text_layer/` (new); `tests/test_text_layer_*.py` and
  `tests/fixtures/text_layer/` (new); two lines in `tests/test_packaging_core_imports.py`;
  `pyproject.toml` (`[project.scripts]` in 15c, the pagexml-tools/fuzzy-search pins in 13);
  `docs/text_layer.md` (new).
- **NLP:** `src/startpage/voc_text_layer.py`, `scripts/build_voc_text_layer.py`; new
  `scripts/verify_as_text_layer.py` and `output/as-phase-05-originals/`.
  `scripts/validate_voc_text_layer.py` stays NLP research code and is not edited.
- **LotA:** nothing is edited. Step 13 runs LotA's tests read-only.

No other phase is active.

## Sources

The planning session read these sections, and no others:
- `upstream_inventory.md`: glossary L183–203, §1 item 5 L79–85, C14 L368–379, C15 L381–394,
  §4 steps 13–15 L457–459 and "later" L463, §5 D4/D5 L483–487, §6 L498–549.
- `docs/cross-project-review.md` L21–53, L100–135, L405–457; `docs/audits/sg_paragraph_tsv_audit.md`
  L1–220 (for the next phase's SG preset; not used by any step here).
- NLP `src/startpage/voc_text_layer.py` L1–138, L232–511, L648–879, and
  `scripts/build_voc_text_layer.py` L1–260, L330–427, read directly. The rest of NLP, LotA,
  pagexml-tools and the SG data were read through two Explore subagents.
- Prototypes in the scratchpad (read-only on the siblings): see *Planning checks*.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 14 | `text_layer.io` + word-break detector (C14) | — | 5 ported + synthetic tests; `decide_word_breaks` on v1 stats → 2/2 files byte-identical; stats parts 50/50 byte-identical from v1 lines; sort order = NLP on 6,914 | [step-14](step-14-word-breaks.md) |
| 15 | extract, join, provenance, presets (C15 library) | 14 | 24 ported + fixture tests; 22 annotated rebuilt with AS functions 157/157 byte-identical to NLP's local build; A/B vs original module 20/20; ScanId check 0 mismatches on 6,914 | [step-15](step-15-text-layer-library.md) |
| 15c | build driver, `as-build-text-layer`, layer compare, docs | 15 | driver tests; CLI run on the 22 → 157/157 identical; manifest a superset; v2.1 hand-off written | [step-15c](step-15c-build-driver.md) |
| 13 | pagexml-tools 0.8.2: pins and environments | 15c, **user release** | 3/3 environments import the joiner; CLI run under 0.8.2 157/157 identical; pins updated; LotA `test_derive_voc` collects | [step-13](step-13-pagexml-tools-pins.md) |
| 15b | NLP shim and wrapper; reproduction | 13, 15c | `verify_as_text_layer.py` exit 0: stored 157/157, A/B 157/157, decide 2/2, protected unchanged; NLP 29 + 21 passed | [step-15b](step-15b-nlp-text-layer-shim.md) |

All five briefs were written at planning time. The steps run in sequence: 14 → 15 → 15c → (the user
releases pagexml-tools 0.8.2) → 13 → 15b → (the user moves the v1 data, D5).

## Why this order

14 first: `join` imports the word-break helpers, and `io` is shared by every later module. 15
(library) before 15c (driver), so the reproduction is proven on plain function calls before
the resumable machinery is added. 13 needs the user's release. AS steps don't wait for it,
because AS's `.venv` (0.8.1 + fuzzy-search 2.5.0) already runs the producer (F05-P.2). 15b comes
last because NLP's environment can't run the joiner until 0.8.2 (F05-P.5).

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| D2 | Light core | `text_layer` is core (pagexml-tools, pandas are core deps); its modules import pagexml lazily, inside functions | Inventory §6; NLP's module already does this |
| D4 | v2 + SG preset | Not in this phase (P1) | User 2026-10-14 |
| D5 | Layer data in `shared-data/` | The user moves v1 after 15b; 15b writes the commands into `STATUS.md` (P4) | User 2026-10-14 |
| D8 | Small fixtures in `tests/` | One synthetic PageXML inventory under `tests/fixtures/text_layer/` | Inventory §6 |
| P1 | Split the phase | v2 line geometry and the SG preset are the next phase; phases 06/07 renumber to 07/08 | User 2026-10-14; 7 steps were over budget, and v2 builds on the moved producer |
| P2 | pagexml-tools 0.8.2 | The user releases it from the fix branch (`7b2b721`: fuzzy-search-3 import, orjson). Steps 14/15/15c run on 0.8.1. Step 13 pins `pagexml-tools>=0.8.2` and fuzzy-search to what 0.8.2 declares | User 2026-10-14. Also suggested to the user: fix `get_all_text_regions` (`set += set`, F05-P.7) in the same release |
| P3 | Acceptance references | (a) NLP `output/voc_text_layer/voc_text_layer_v1/` (22 inventories, built from the local zips): byte-identical; (b) A/B against the original NLP module on a local sample; (c) order-insensitive against v1; (d) the user runs a 20-inventory v2.1 sample on the external disk and compares with v1 | User 2026-10-14. v1's input (GLOBALISE v2.1) isn't local, and the local 2023 zips order lines differently (F05-P.1) |
| P4 | Data move | The user, after 15b: `mv` v1 to `shared-data/`, a symlink back in NLP `data/`, repoint LotA | User 2026-10-14 |
| P5 | Port verbatim | Output bytes are the acceptance. Generalise only through parameters whose defaults give the VOC behaviour; keep NLP's quirks (document order, region edges never joined, `_fmt` `.6g`) | As phase 04 P4 |
| P6 | AS names | AS names in `io`/`word_breaks`/`extract`/`join`/`provenance`/`presets`/`build`/`compare` (briefs list them). NLP's shim keeps every old name | One brief per side; NLP importers unchanged |
| P7 | The rule stays a dict | `DEFAULT_WORD_BREAK_RULE` is NLP's `WBC_RULE` verbatim (value + reason); no dataclass | `corpus_default.json` embeds it; a dataclass adds a conversion and a byte risk |
| P8 | C13 hand-off | `inventory_sort_key` uses `InventoryNumber`, and invalid names sort last. The `_{inv}_` check becomes `ScanId.try_parse(prefix=preset.scan_id_prefix)` plus an inventory equality check | Phase-04 hand-off; F04-P.5 (same order); v1 has 0 mismatches (F05-P.6) |
| P9 | Manifest is a superset | Layer-level keys unchanged. Per stage: `git_head` is now AS's HEAD (the producer), plus new `archival_structures` (`package_info`) and `preset` keys | Provenance (§2); NLP's `git_head` was empty in v1 anyway |
| P10 | NLP script becomes a wrapper | `build_voc_text_layer.py` keeps its path and flags and calls AS `build.main`; the handoff docstring moves to AS `build.py` / `docs/text_layer.md` | NLP docs and handoff commands keep working |
| P11 | No v2 in the move | No line geometry, no page size, no empty-scan rows, no `position="region_end"` word-break statistic | Inventory C15 risk: "don't fold v2 changes into the move" |
| P12 | 8 workers | `--workers 8` for verification runs | NLP `CLAUDE.md`: up to 8 of 10 cores (phase-04 P9) |

## Hazards

1. **Protected outputs.**
   - NLP `data/voc_text_layer_v1/` (12 GB). Never write there. Check it with a listing of every
     file's size and mtime, plus sha256 of `wbc/*`, `manifest.json` and `status.tsv`.
   - NLP `output/voc_text_layer/voc_text_layer_v1/` (the P3a reference): sha256 of every file.
   - Every build run uses `--out-dir` in the scratchpad. Copied into 14, 15, 15c, 13, 15b.
2. The local zips are not v1's input, so **never expect byte identity with v1 from a local
   rebuild**. Compare v1 order-insensitively (F05-P.1). Copied into 15, 15c, 15b.
3. **pagexml-tools 0.8.1 cannot parse nested `TextRegion`s** (`TypeError` at
   `pagexml_document_model.py` L447). A region without `Coords` gets a box derived from its lines,
   and one with neither `Coords` nor lines is dropped. So the fixture's nested-region file is a
   parse-error case today (F05-P.7). Copied into 15 and 13.
4. **Environment:** NLP's `venv` and LotA's `venv` can't import `pagexml.helper.pagexml_helper`
   until 0.8.2 (fuzzy-search 3.0.4 lacks `fuzzy_search.similarity`). NLP's
   `tests/test_voc_text_layer.py` *collects* 29 but 23 fail at run time. AS's `.venv` is fine.
   Copied into 13 and 15b.
5. AS's editable install has stale metadata (0.1.0) and no entry points. 15c runs
   `.venv/bin/pip install --no-deps -e .` once, to register `as-build-text-layer`.
6. `extract_stats/<inv>.json` records the absolute `source` path. Compare it without `source`.
   v1's paths point to an external drive: never follow them. Copied into 15, 15c, 15b.
7. NLP's original build script derives `PROJECT_ROOT` from `__file__` and inserts `src/` into
   `sys.path`. An A/B run of a saved copy must set `__file__` to the real path (15b).

## Planning checks (2026-10-14, scratchpad; sibling files untouched)

Scripts are kept in `reports/data/phase-05-text-layer/planning/`. `vtl_orig.py` there means a copy
of NLP `voc_text_layer.py` (sha256 `a36d08fd6660a227…`).
- Under AS `.venv` (pagexml-tools 0.8.1, fuzzy-search 2.5.0), the current NLP module rebuilt the
  22 annotated inventories from the local zips. Against NLP's local build all 157 compared files
  are identical: 110 per-inventory TSV files (byte comparison), 3 WBC run files, and 44 stats
  JSONs (compared as JSON, without `source`). It took 221 s in one process (F05-P.2).
- `decide_wbc` on v1 `wbc_stats.tsv` gives `wbc_per_inventory.tsv` and `corpus_default.json`
  byte-identical; the `_fmt` rewrite of `wbc_stats.tsv` is byte-identical too (F05-P.3).
- Against v1, 34 local rebuilds (13 + the 22, 2555 in both) have equal line multisets, region keys and metadata,
  but a different order (F05-P.1).
- PyPI 0.8.1 is `0f39fad` except `__version__` and the one-line `vertical_distance` fix.
  `7b2b721` gives the same extract hash as 0.8.1 on 9098 (F05-P.4).

## Open questions for planning (settled 2026-10-14)

- *Do the C14/C15 line numbers hold?* Yes, every one (the briefs carry anchors).
  `pagexml_tools_info` is in the module (L106), not the script.
- *Is step 13 a session step?* Partly. The user releases; step 13 pins and verifies (P2).
- *Can v1 be rebuilt locally?* No (P3, F05-P.1).
- *Scope?* Split (P1). *Data move?* The user, after 15b (P4).
- *Does `decide_wbc` reproduce from `wbc_stats.tsv`?* Yes (F05-P.3).

## Review checklist

Ticked by the review (2026-10-15); evidence in [`REVIEW.md`](REVIEW.md).

- [x] Every step's acceptance re-run by the reviewer: the step-15 reproduction script, the 15c
      CLI run on the 22, and `verify_as_text_layer.py` exit 0.
- [~] AS suite green: not reproducible in today's `.venv` (pytest's `py.py` and the `vision`
      extra are missing since the user's reinstall; pandas 3 fails 2 tests outside the text
      layer, step 15r). Text-layer tests 101 passed. NLP 29 + 21 passed; LotA 327 passed.
- [x] `import archival_structures.text_layer.<each module>` imports no pagexml (packaging test).
- [x] No VOC constants outside `presets.py` (and the rule's documented VOC calibration).
- [x] Protected NLP outputs unchanged (hazard 1).
- [x] `changelog.md`: steps 13–15 under *Unreleased*, with NLP imports listed.
- [x] The user's v2.1 sample result recorded (P3d): F05-R.1.
- [x] Report final; findings in `reports/index.md`; `make reports` clean.
- [x] `Brief defects` answered.

## Hand-off

Reviewed 2026-10-15 (Opus): **accept with follow-ups**. Run step 15r
([brief](step-15r-review-fixes.md)) before the release.

- **For whoever uses the layer:** v1 now lives in `<DS>/shared-data/voc_text_layer_v1` (D5 done
  by the user, 2026-10-15). NLP's and LotA's `data/voc_text_layer_v1` are symlinks to it. The
  producer is `as-build-text-layer` (preset `voc_v1`), and it reproduces v1 from v2.1 input
  (F05-R.1). Build a sample into a fresh `--out-dir`, never into the shared layer (F05-R.5).
- **For phase 06 (v2 + SG preset):**
  - `LayerPreset` needs the word-break rule and the sanity anchors (REVIEW #7);
  - a guard against subset runs overwriting the run-level `wbc/` decision (F05-R.5);
  - a manifest history instead of last-invocation overwrite (F05-15c.4);
  - a word-break statistic that copes with `„`'s mid-line use (F05-R.4), measured on a
    labelled sample before adoption, alongside the planned region-end statistic.

  The P11 list (line geometry, page size, empty-scan rows) still stands. Build v2 from v2.1
  input, not from the local zips (F05-15.5).
- **For the user:**
  - restore AS `.venv` (pytest `py.py`, `.[vision]`);
  - reinstall AS editable in the NLP and LotA venvs (F05-R.3);
  - update `shared-data/README.md`;
  - report nested regions upstream (F05-13.3);
  - `poetry lock`.
