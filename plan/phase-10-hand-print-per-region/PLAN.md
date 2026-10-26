# Phase 10: Hand and print per volume, from the PageXML (text layer v4)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **Status:** reviewed 2026-10-23 (negative result; `REVIEW.md`). Planned 2026-10-23 (Opus); **re-planned 2026-10-23** after 15p's stop rule fired (UA-018 (a)) and the
> geometry measurement F10-P.6 (UA-019 (b)). The first plan's per-region switching (old P2, P3, P5, P7) is replaced;
> the old briefs 15r–15t are void.

## Goal

The text layer no longer assumes that an archive is either handwritten or printed. Each **inventory** gets a mode
(`hand`/`print`) decided from its own PageXML line geometry, and the whole build of that inventory uses the hand or
print rules of the v3 presets. Region and scan labels are written as diagnostics, but they switch no rules. The rule
is fitted on the dev volumes and tested once, with a stop rule, on a random silver sample. A v4 dev layer on the
local inventories shows the result, and a run kit without hand/print "sides" lets the user run the full corpus.
Nothing is published in this phase.

## Owns

- `archival_structures/analysis/hand_print.py` (new) and its tests;
- `archival_structures/text_layer/modes.py` (new); `presets.py` (v4 presets, two new fields); `build.py` (per-inventory
  preset resolution in extract, layout, types, units, join); `line_split.py` (factor out the pre-pass of
  `fit_split_model` only, behaviour unchanged);
- `scripts/run_text_layer_v3_full.py` → a v4 successor; `docs/text_layer.md`;
- `reports/data/phase-10-hand-print-per-region/`. Not: `layout.py`, `types.py`, `units.py`, `join_v2.py`,
  `page_structure.py`, `join.py`/v1 paths, `text_extent.py` (phase 11).

## Sources

- `plan/CONTEXT.md` §1–§3 (pipeline and accepted deviations); phase-09 `PLAN.md` decisions P2, P4, P6–P11.
- Report sections *Planning* (incl. the re-plan paragraph) and *Step 15p*; F10-P.1–P.6, F10-15p.1–8.
- Code anchors (2026-10-23, re-plan): `build.py` `stage_extract` L266 (split choice L280), `stage_layout` L349
  (params L353–354), `stage_types` L389 (L392–393), `stage_units` L427 (params L432–434, REPUBLIC L445–456, call
  L462), `stage_join` L646 (lead marks L682, L687); `line_split.py` `fit_split_model` L1386 (pre-pass L1396–1410),
  `fit_split_model_from_rows` L1317, `make_split_hook` L1742, `extract_inventory_v2_split` L1747;
  `extract_v2.py` `extract_scan_v2` L64, `extract_inventory_v2` L180; `io.py` `iter_pagexml` L160.
- Fields that differ between `sg_1.01.02_v3` and `sg_1.10.94_v3` (re-plan, by `asdict`): identity `name,
  description, archive_id, scan_id_prefix, source_pattern`; per archive `wbc_pinned, join_lead_marks_lower`; **rules**
  `merged_line_split, split_region_tags, layout_params, types_params, unit_params, join_lead_marks`.

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 15p | Hand/print census of 1.01.02 (density) | nothing | done: stop rule fired (F10-15p.1) | `step-15p-hand-print-census.md` |
| 15q | Volume-mode classifier and its one test | 15p, UA-020/021 | `analysis/hand_print.py` + tests; dev cut reproduces 1.529/1.534; **stop rule** on the 60-volume random sample (weighted ≥ 0.90 per class); 455/456 transfer; UA-017 volumes read | `step-15q-volume-mode-classifier.md` |
| 15r | Volume-mode build (v4 presets) | 15q passed | `text_layer/modes.py`, v4 presets, per-inventory rules in all stages; `15r-a` on the 16 dev inventories: hand = `15m-a` hand, 455/456 = `15m-a` print, 3771–3823 print-built; 3771 crosswalk rate | `step-15r-volume-mode-build.md` |
| 15s | v4 dev build with staged volumes, audit, run kit | 15r | `15s-a` from scratch incl. staged volumes; audit without fixed lists; `run_text_layer_v4_full.py`; UAs for the full run and a v4 pilot | `step-15s-v4-build-and-run-kit.md` |

Void briefs (kept for history, not to be run): `step-15q-region-classifier.md`, `step-15r-mode-aware-extract-layout.md`,
`step-15s-mode-aware-types-units-join.md`, `step-15t-v4-build-and-run-kit.md`.

## Why this order

15q decides whether the geometry rule holds out of sample, before any build code depends on it; if its stop rule
fires, the phase goes to review as a negative result. 15r changes the build in one go: with one mode per inventory,
every stage just reads that inventory's resolved preset, so the comparison against `15m-a` is exact (hand inventories
and 455/456 byte-identical, the six printed 1.01.02 volumes rebuilt with the print rules). 15s rebuilds from scratch,
adds the staged volumes, audits, and hands over.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | Phase 10 is the hand/print work only; C10, C12, C18 docs and `text_extent` (UA-014 a) are phase 11 | No step touches `text_extent.py`, `stream_analysis/`, `docs/challenges/` | User, 2026-10-23 |
| P2 | **(revised, UA-019 (b))** The unit of decision is the **inventory**. No metadata (EAD) decides a mode. Region and scan labels are diagnostics only. Mixed volumes get one mode, by scan majority; that limit is documented, not hidden | No per-region or per-page rule switching; no EAD in the build | User, 2026-10-23. Region labels are too noisy (F10-P.6); the first P2 ("region") failed twice (F10-15p.1, F10-P.6) |
| P3 | **(replaced)** Feature `hrel` per region = median line-box `h` / median baseline pitch (≥ 3 text lines, ≥ 2 positive pitches). Scan value = char-weighted median `hrel` over its featured regions; a scan is labelled if those regions hold ≥ 200 chars. Scan `print` if value ≤ `t_lo`, `hand` if ≥ `t_hi`, else `uncertain`; `t_print` = p95 of print dev scans, `t_hand` = p05 of hand dev scans, `(t_lo, t_hi) = sorted(...)`. Volume: `print_scan_share` = print / (print + hand) scans; mode `print` if ≥ 0.5, else `hand`; no labelled scans → `hand` (`default`) | Thresholds computed by `fit` from the 14 dev volumes, rounded to 3 decimals; never hand-set; never fitted on silver | Fixed before any silver geometry is seen (re-plan measured 1.529 / 1.534; dev 14/14 by majority, margin ≤ 0.19 vs ≥ 0.91) |
| P4 | Dev set = the 14 local 1.01.02 inventories (hand 3349–3354, 11182, 11281; print 3771–3773, 3821–3823). 455/456 are the transfer check, not fit. EAD series give **silver** labels for evaluation only | Never fit on silver; report agreement | CLAUDE.md leakage rule |
| P5 | **(replaced)** `mode/<inv>.json` records `mode`, `method` ∈ `geometry`, `default`, `fixed`, the scan counts and share; `mode/scans/<inv>.tsv.gz` and `mode/regions/<inv>.tsv.gz` hold the diagnostics. No v3 table gains a column | v4 tables keep the v3 columns (LotA reads by name) | Transparent; the old per-region columns are not needed |
| P6 | v4 presets `sg_1.01.02_v4`, `sg_1.10.94_v4` (layer names `NL-HaNA_*_text_layer_v4`) = their v3 base plus `mode_presets = {"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}` and `volume_mode` (classifier config). Per inventory, `inventory_preset(preset, mode)` copies **only the six rule fields** from `mode_presets[mode]`; identity and per-archive fields stay the v4 preset's | v1–v3 presets unchanged byte for byte | No parameter duplication |
| P7 | **(revised)** Granularity: every rule per inventory. The split model is fitted only for print inventories, as v3 print does | — | Follows from P2 |
| P8 | Crosswalk for printed 1.01.02 volumes (`shared-data/NL-HaNA_1.01.02/print_paragraphs_v1/`, 3760–3864): 15r **measures** the 3771 match classes; wired only via `--republic-dir` | Report the rate; no default path | Unknown whether REPUBLIC line ids match (F10-P.5) |
| P9 | Dev layers: `data/text_layer_v2_dev/<step>-a/{1.01.02,1.10.94}/` (one sub-layer per archive) | Comparers map v3 `hand`→`1.01.02`, `print`→`1.10.94` | P2 |
| P10 | **Stop rule (15q):** on the 60-volume random sample (`planning-15q/silver_sample.tsv`), stratum-weighted agreement ≥ 0.90 for silver `print` **and** for silver `hand`. A volume that fails to parse or is `default` counts as wrong. Report Wilson 95% intervals per stratum. If it fails: stop, report, phase review; no tuning | The UA-017 volumes are diagnostic only (picked for difficulty) | Pre-registered 2026-10-23 |
| P11 | 1.10.94 transfer: if 455 or 456 is not `print` under the frozen 1.01.02 rule, `sg_1.10.94_v4` gets `volume_mode = {"method": "fixed", "mode": "print"}` (as v3), recorded as an assumption finding | Decided in 15q, applied in 15r | 455/456 sit near the cut (F10-P.6); no hand material in 1.10.94 to fit on |

## Hazards

- **Segmentation model.** `hrel` depends on the line-polygon convention of the Laypa/Loghi model; 455/456 (another
  model) shift it (P11). A 1.01.02 volume segmented by another model could shift too: 15q and 15s list volumes whose
  scan values pile up within ±0.05 of the cut.
- **Pre-pass cost.** One extra parse per inventory (two for print: classify + fit share one pre-pass, then extract).
  15s measures it for the full-run prediction.
- **Suffixed inventories** (audit D3): group by the inventory parsed from `scan_id`, not `inv_num`.
- Every 1.01.02 region is tagged `Text`, a print split tag: in a print inventory every region may split (as in 455).
- Only local PageXML: the 16 pilot sources, the 12 UA-017 tars and the 60 UA-020 tars in `shared-data/.../pagexml/`.
  Never touch the external drive. The 60 tars need group read (UA-021).
- coreshare: one budget of 8 workers; launch with `coreshare.py run`; never hardcode a worker count.
- Preset-pinning tests (`test_presets_voc_unchanged`, `test_v3_presets_equal_v2_but_name`, `test_hook_off_for_hand_preset`)
  stay green; v4 gets its own tests.

## Open questions for planning: settled 2026-10-23

- Scope and granularity: user (P1; P2 revised by UA-019 (b)).
- Is density separable? No (F10-15p.1). Is size-independent geometry? Per volume yes in sample, per region no (F10-P.6).
- Where is hand/print hard-wired? *Sources* above; the run kit's `SIDES` dict and the audit's `base/"hand"` too.

## Review checklist

Reviewed 2026-10-23 (Opus 5.5); evidence in `REVIEW.md`.

- [x] Every step's acceptance criteria met, with numbers in `PROGRESS.md`, re-run by the reviewer (15q; 15r/15s not run, P10).
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider` (1,209 passed, 11 skipped; LotA 368 passed).
- [x] Error analysis read individual volumes, scans and regions (each brief's list), written up in the report.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Public API changes in `docs/shared_library/changelog.md` (new module only; no `LayerPreset` fields or `mode/` tables, as 15r did not run).
- [x] No archive-specific knowledge in `hand_print.py`: thresholds are parameters, fitted by a documented procedure.
- [x] Leakage: thresholds from dev only; silver used once, for the stop rule (P3, P4, P10).
- [x] v1–v3 presets and their dev layers reproduce (no `text_layer/` change); `15r-a` void.
- [x] `Brief defects` entries answered (`REVIEW.md` *Brief quality*).

## Hand-off

- **Outcome:** negative result. 15p (density) and 15q (`hrel` volume mode) both failed their pre-registered stop
  rules; 15r and 15s were not run. No build code changed; v1–v3 presets and layers are untouched. The v2/v3 pilots
  still build the printed 3771–3773, 3821–3823 with the hand rules (F10-P.1, documented as a known gap).
- **Left in the library:** `archival_structures/analysis/hand_print.py` (unused; docstring says "negative result"),
  Unreleased in the changelog.
- **Reusable for a next attempt:** region-feature caches `data/hand_print_geom/15q-a/` (14 dev, 455/456, 60 sample,
  12 staged), `step-15q/validate.py` (prepass/fit/evaluate), the 15r/15s briefs (unchanged, unrun), P11 (1.10.94
  fixed `print`). The 60-volume sample is spent (F10-R.5).
- **Lead:** absolute line-box height (F10-R.3), exploratory only.
- **Next:** UA-022 (a new pre-registered test / EAD series modes / close). Phase 11 is independent.
