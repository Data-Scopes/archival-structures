# Phase 12: Hand and print by line-box height (one pre-registered test)

> **Audience:** the Opus sessions that plan and review this phase. Implementation sessions do
> **not** read this file. Whatever a step must honour is copied into its step brief.
> **Budget:** ~200 lines (`plan/README.md`).
> **Status:** planned 2026-10-23 (Opus 5.5), from the phase-10 hand-off and UA-022 (a); 15v/15w planned 2026-10-25
> after 15u passed; reviewed 2026-10-25 (accept with follow-ups); 15wr and 15x written in review; 15wr and 15x reviewed
> 2026-10-26 (accept; `REVIEW.md` § Step 15wr review, § Step 15x review). Full run checked 2026-10-27. Addendum
> steps 15y (pins, in-place rebuild, debts, throughput) and 15z (P21 reads and pins) planned 2026-10-27 (Opus 5.5); both done,
> the drive rebuild checked, and reviewed 2026-10-27 (accept with follow-ups, `REVIEW.md` § Steps 15y and 15z review); 15yr written.

## Goal

Test, once and with a stop rule fixed now, whether the absolute line-box height of an inventory (F10-R.3) tells a
printed SG volume from a handwritten one. If it passes, the phase-10 v4 build (one mode per inventory, P2 of phase 10)
is implemented with this rule; if it fails, the phase closes as a negative result and the user picks phase 10's
fallback (EAD series modes or close; UA-022 options (b)/(c)). Nothing is published in this phase.

## Owns

- `archival_structures/analysis/hand_print.py` (additive: box-height rule) and `tests/test_analysis_hand_print.py`;
- `reports/data/phase-12-hand-print-box-height/`; `data/hand_print_geom/15u-a/` (new cache);
- since 15u passed: `text_layer/modes.py` (new), `presets.py` (v4), `build.py`, `line_split.py` (pre-pass refactor),
  `scripts/run_text_layer_v4_full.py` and its tests, `docs/text_layer.md` (v4 section), `data/text_layer_v4_staged/`,
  `data/text_layer_v2_dev/15v-*`, `15w-*`, `reports/data/phase-12-hand-print-box-height/step-15{v,w}/`;
- after the review: 15wr `text_layer/modes.py`, `presets.py`, `step-15w/audit.py`, the run script, `15wr-*`; 15x
  `text_layer/units.py`, `presets.py`, `15x-*`, `step-15{wr,x}/`;
- 15y: `text_layer/modes.py`, `presets.py`, `build.py` (stage skip and staleness), `units.py` (stale crosswalk),
  `scripts/run_text_layer_v4_full.py`, `step-15w/audit.py`, `15y-*`, `step-15y/`; 15z: `presets.py` (pins), `15z-*`,
  `data/text_layer_v4_staged_15z/`, `step-15z/`; 15yr: `build.py` (upstream gate, recorded sizes), `modes.py` (extract timing),
  the run script, `scripts/stage_text_layer_v4_full.py`, `step-15w/audit.py`, their tests, `15yr-*`, `step-15yr/`.

## Sources

- Phase 10: `plan/phase-10-hand-print-per-region/PLAN.md` *Hand-off*, `REVIEW.md`; report sections *Step 15q* and
  *Review* (F10-15q.1–7, F10-R.1–5); the unrun briefs `step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`.
- Code (2026-10-23): `hand_print.py` `region_features` L117, `scan_value` L137, `volume_mode_from_features` L173;
  15q harness `reports/data/phase-10-hand-print-per-region/step-15q/validate.py` (`_prepass_one` L68,
  `cmd_prepass` L107, `load_regions` L125, `wilson` L201, `cmd_evaluate` L211); `extract_v2.extract_scan_v2` L64
  (scan row has `page_w`, `page_h`).
- Planning measurement: `reports/data/phase-10-hand-print-per-region/review/h_by_volume.{py,tsv}` (76 labelled
  volumes: print 65.0–86.0 px, hand 89.5–173.0 px).

## Steps

| Step | Title | Depends on | Acceptance (short) | Brief |
|---|---|---|---|---|
| 15u | Box-height rule: fit on the 76, one test on a fresh sample | UA-023 (staging) | done, **passed** 60/60 (F12-15u.1); reviewed 2026-10-25 | `step-15u-box-height-test.md` |
| 15v | Volume-mode build (v4) with the box-height rule | 15u passed | `modes.py`, v4 presets, per-inventory rules; `15v-a` (16): hand = `15m-a` hand, 455/456 = `15m-a` print, 3771–3823 print-built; 16/16 box values = `fit_volumes.tsv`; crosswalk bar 18/20 | `step-15v-volume-mode-build.md` |
| 15w | v4 dev build with every local volume, audit, run kit | 15v | `15w-a` (196 + 2) = `15v-a` on the 16; 168/168 values = 15u references (70 `print` in 1.01.02); discovering audit (178/4 on `15m-a`); `run_text_layer_v4_full.py`; reads near the cut; UA-024/025 | `step-15w-v4-build-and-run-kit.md` |
| 15wr | Split gate for single-column print volumes (review fix, F12-R.4) | phase review | done, bar **passed** 19/20 (F12-15wr.2); 53 gated, 32 split; reviewed 2026-10-26 (accept, F12-R.8–R.10) | `step-15wr-review-fixes.md` |
| 15x | Drop-cap-aware print unit rule (UA-026 (b)) | 15wr (bar passed) | done, bars **passed** (A 59/60, B 0.9927, C 19/0; F12-15x.1); `15x-a`; reviewed 2026-10-26 (accept, F12-R.11–R.15; UA-029 open) | `step-15x-drop-cap-units.md` |
| 15y | Mode pins, in-place rebuild, resume debts, throughput model (P24–P27) | full run checked | pin 3463; `--rebuild` on a copy of `15x-a` changes only 3463 (= fresh `15y-a`); resume no-op; summary (9122, 0); fit in/out of sample | `step-15y-pins-and-rebuild.md` |
| 15z | Read the 37 off-series modes and pin them (P21, P28) | 15y, UA-034 | 37/37 values reproduce; 185 verdicts; pins by the rule; `15z-b` rebuild; check page; drive UA | `step-15z-read-and-pin.md` |
| 15yr | Review fixes (P30): upstream gate, audit keys and last rows, stager deletions, extract timing, whole-layer time terms | 15y/15z review | done, reviewed 2026-10-28 (accept; `done_inventories` fixed in review); case A blocked; audit `15y-b` 3,592/33/0; no-op 1.2 s; factor 1.817 | `step-15yr-review-fixes.md` |

## Why this order

15u is the only open question. The 15r/15s briefs of phase 10 were written for an unknown per-inventory rule and
need only a new `decide_mode` and its params, so writing their successors before 15u's result would be wasted if
the stop rule fires. After a pass, a short Opus session writes 15v/15w with the frozen `cut.json` copied in.

## Decisions that bind this phase

| # | Decision | Rule for steps | Rationale |
|---|---|---|---|
| P1 | **Feature:** region `h_med` (median line-box `h` over non-empty lines) for every region that has a 15q `hrel` feature (≥ 3 non-empty lines, ≥ 2 positive baseline pitches) and `n_chars` ≥ 100. **Volume value** = char-weighted median of those `h_med` (sort by `h_med`; value at `bisect_left(cumsum, total / 2)`). Raw pixels, no normalisation | Copy exactly; no scan labels decide anything | The definition measured in F10-R.3 (`h_by_volume.py`), so planning numbers reproduce. Normalising by `page_w` is untested and the fit set lacks `page_w` for 60 volumes |
| P2 | **Fit set = 76 labelled volumes:** 14 dev (class by P4 of phase 10), 455/456 (`print`), the 60 spent phase-10 sample volumes (EAD silver). Replaces phase-10 P4's "never fit on silver" | Fit only on these; never on a 15u test or diagnostic volume | User, 2026-10-23 (chat). No leakage: the test sample is fresh and disjoint (F10-R.5) |
| P3 | **Cut:** if max(print) < min(hand): cut = their midpoint; else the cut minimising balanced error (mean of the two class error rates) over midpoints of consecutive sorted values, ties → the lowest. Rounded to 2 decimals. Volume `print` iff value ≤ cut; no qualifying region → `hand`, `method="default"` | Computed by `fit`, never hand-set; planning value 87.75 (86.0 / 89.5) | Pre-registered before any test geometry |
| P4 | **Test sample** `planning/test_sample.tsv` (seed 20261212): the five phase-10 strata, 10/20/10/10/10 volumes, excluding dev, the 60 spent and the 12 UA-017 volumes; 5 ordered reserves per stratum, used in order only for a sample tar that is missing; weights = phase-10 stratum sizes (123, 421, 99, 64, 76) | Reserves only for missing tars; record each substitution | Same strata and weights as 15q, so the two results compare |
| P5 | **Stop rule:** stratum-weighted agreement ≥ 0.90 for silver `print` and for silver `hand`; a parse failure or `default` counts as wrong; Wilson 95% per stratum. Fail → stop, report, phase review; no tuning | As phase-10 P10 | Pre-registered 2026-10-23 |
| P6 | **Diagnostic set:** 20 random volumes outside every silver group (same file); no pass/fail; read for what a full run would meet | Never used to fit or to judge the stop rule | Silver covers 5 series; the corpus has thousands of others |
| P7 | 1.10.94 stays `fixed`/`print` in any v4 preset (phase-10 P11) | 15v copies it | Only two volumes, no hand material to test on |
| P8 | Mixed volumes get one mode (phase-10 P2); scan box heights are diagnostics | 15u reports the minority-scan share | User, UA-019 (b) |
| P9 | **v4 mode rule** (2026-10-25): `decide_mode` = `box_height_mode(region_features(raw pre-pass line rows), BoxHeightParams(87.75, 100))`; presets carry `volume_mode = {"method": "box_height", "params": …}` (1.10.94: `fixed`/`print`, box result kept as `box_height_mode`). Methods = `BOX_HEIGHT_METHODS` | 15v copies the dict; `modes.py` holds no cut | Frozen by 15u (`cut.json` sha256 `ce58c333…`); P7 |
| P10 | `prepass_rows` returns **raw** `extract_scan_v2` rows; only the split-model fit applies `_int_row` | Box values must equal 15u's exactly (16/16 in 15v, 168/168 in 15w) | 15u measured raw rows; the split model was fitted on `_int_row` rows (checked in code) |
| P11 | Phase-10 15r/15s design carried over: six `MODE_RULE_FIELDS` (re-measured 2026-10-25 by `asdict`), `inventory_preset`, per-mode `params.json`, one pre-pass + one extract per inventory | As phase-10 P6/P7 | LotA reads no `params.json`, manifest `*_params` or `mode/` (checked 2026-10-25), so the per-mode form is safe |
| P12 | REPUBLIC for printed 1.01.02: `print_paragraphs_v1` (3760–3864) passed in 15v, used only for print-mode inventories; **usable iff ≥ 18/20** matched rows read correct (bar fixed now); 15w passes it only if usable | Hand inventories get `republic_path=None` | Phase-10 P8; 455 has 7 unmatched of ~29k |
| P13 | 15w scope: every local volume = 14 pilot + 182 staged 1.01.02 tars (188 in shared-data minus 3349–3354) + 455/456; references for 168, none for the 30 reserves | Never link a tar twice | PLAN row "all staged"; reserves cost little and add unread volumes |
| P14 | 15w reads (15u review): every volume within ±10 px (39 + 456 at planning) and every `print` outside silver print (3463, 9486 at planning), plus unread minority volumes | Read lists fixed in the brief | F12-R.2: the likely error is hand called `print` |
| P15 | **Print units of printed 1.01.02 volumes** (2026-10-25): 15w keeps the print unit rules and documents the drop-cap cut as a known issue; step 15x then writes a drop-cap-aware print unit rule with its own hand-read bar and rebuilds | 15w changes no unit rule; 15x fixes its bar before reading any output of the new rule | User, UA-026 (a) + (b); F12-15v.4, F12-R.3 |
| P16 | **Split gate** (phase review): in `sg_1.01.02_v4` print mode the merged-line splitter runs only if `column_start_share` of the raw pre-pass rows (lines ≥ 20 chars; per page side with ≥ 50 such lines, the share starting at ≥ left + 0.4 × span; pooled) is ≥ 0.35, or cannot be measured. Recorded as `split_gate` in `mode/<inv>.json`. 1.10.94 has no gate | Fixed values; no tuning | F12-R.4: 46/46 sampled splits wrong in single-column series; measured gap 0.225 → 0.496 (`review/column_share_pooled.tsv`); 456 (0.164) is two-column with merged lines, hence 1.01.02 only |
| P17 | **15wr bar**: the largest `main_text` unit on each of the 20 scans in `review/reads_15wr.tsv` (drawn by the review, seed 20261027); pass iff ≥ 18 `in order`; fail → negative result, stop, no tuning, 15x waits | Read list fixed before the step | Like P12/phase-09 bars |
| P18 | **15x rule**: an `indent`-only print cut within `drop_cap_lines` = 2 main lines of the last kept cut is dropped; set through the new `LayerPreset.mode_overrides` on `sg_1.01.02_v4` print only; `UnitParams.to_dict` omits the field when `None` | Hand, 1.10.94, v1–v3 byte-identical | F12-R.6: indent-only cuts at d ≤ 2 are 44.9% of print cuts, 1.1% of decision/*Dat* cuts; drop caps span 2–3 lines (d=3 only 367) |
| P19 | **15x bars** (fixed 2026-10-25, before the rule existed): A precision of dropped cuts ≥ 0.90 on the 20 scans of `review/reads_15x.tsv` (seed 20261026; ≤ 60 read, seed 20261029); B decision/*Dat*/*IS na* unit starts kept ≥ 0.97 (all 31 *gedrukte resoluties*); C pages worse ≤ 2 and better ≥ 12. Fail → negative result, override removed, no tuning, UA | *Nouvelles*/*plakkaten*: diagnostic only (10 reads each, seed 20261028) | UA-026 (b) |
| P20 | v4 stays `text_layer_v4`: `sg_1.01.02_v4` changes behaviour in place (15wr, 15x) as long as no v4 layer is published (UA-025 open) | A published pilot before 15x is reported, not rebuilt | Unpublished preset; changelog records the change |
| P21 | **Handwritten volumes on the `print` side** (2026-10-25): after the full v4 run, a short step lists every `print` volume of 1.01.02 outside the printed EAD series (*gedrukte resoluties*, *nouvelles*, *plakkaten*), reads them, pins the handwritten ones to `hand` with a per-inventory pin in the v4 preset (like `wbc_pinned`) and rebuilds only those. No EAD title rule in the build | The box-height rule stays the only mode source before the run; the pin step is planned after UA-024's run | User, UA-028 (a); F12-R.5, F12-R.1 (the same list catches scan-resolution shifts) |
| P22 | **Drop-cap rule scope** (2026-10-26): the 15x rule stays for every print-mode 1.01.02 volume, *nouvelles* and *plakkaten* included; a letter-part exemption (UA-029 option (d)) is a later step only if a sibling needs the *nouvelles*' letter structure | `sg_1.01.02_v4` unchanged; no follow-up step now | User, UA-029 (a); F12-R.13 (readable precision 27/28 *plakkaten*, 30/36 *nouvelles*), F12-R.14 |
| P23 | **v4 pilot published** (2026-10-26): `shared-data/*/text_layer_v4/` holds `15x-a` for 1.01.02 (it replaced in place the `15w-a` copy published the same day, before any consumer linked it) and the `15w-a` copy for 1.10.94 (data tables identical). P20's "as long as no v4 layer is published" has now ended | A later change to `sg_1.01.02_v4` output (such as the P21 pins) must be planned either as a new layer version or as the full run replacing the pilot, with a shared-data changelog line | User, UA-025 (a) and UA-030 (a) |

| P24 | **Pins** (2026-10-27): `volume_mode["pins"] = {inv: mode}` overrides the measured mode (method `pinned`, measured result kept as `box_height_mode`); `MODE_PINS_V4` on `sg_1.01.02_v4` only. 15y pins 3463 → `hand` (15w read 5/5 handwritten, F12-R.9) | 15z adds pins only by its rule | P21; per inventory, no EAD title rule |
| P25 | **In-place rebuild** (2026-10-27): `--rebuild ARCHIVE=F` = forced extract of F, then `--stage all` over the whole layer. v2 layout/types/units skip done inventories (unless forced, params changed, or the upstream stage's row is newer); wbc parts and join use the same staleness; stale split files and crosswalks are removed | Fixes debt 1 (the resume redid layout/types for 3.3 h) | The subset rule of `docs/text_layer.md` stays: wbc is decided over all inventories |
| P26 | Run summary counts the last row per `(inv, stage)` (debt 2) | 15y | The full run logged 6 failed rows although all were done |
| P27 | **Throughput model**: per-stage OLS on the full run's last done rows (1.01.02 pooled over modes; 1.10.94 slope only); wall = pool stages longest-first + serial stages summed; `--want` gives the low and `--min` the high bound; no accuracy bar, in- and out-of-sample errors reported | 15y | Replaces the dev-build constants (`SLOW_FACTOR = 3`) |
| P28 | **15z read list and pin rule** (fixed 2026-10-27, before any read): the 35 `print` volumes outside the printed series plus 12163/12165 (`hand`, silver print; user 2026-10-27); 5 scans each, weighted by characters (`planning/reads_15z.tsv`, seed 20261104); pin iff ≥ 4 of 5 read scans are the other class; 2–3 → mixed, no pin, UA; reads from text only, no mode or value shown; a blind check page for the user before the drive rebuild | 15z copies it | UA-028 (a) extended to the reverse direction by the user |
| P29 | Parallel layout/types/units (3.9 of 7.1 h of the full run run serially): deferred | Not in 15y/15z | User, 2026-10-27; the 15y model quantifies the gain |
| P30 | **15yr** (review, 2026-10-27): a v2 stage never processes an inventory whose upstream last row is not `done`; v2 builds reuse the sizes of a recorded extract row when the stored size matches; v4 extract rows time the whole call; the run script drops `--estimate` and adds whole-layer terms; audit keys `<archive>/<inv>` and done by last row; the stager mirrors deletions (capped) | No rule, pin, preset or output changes; dev clone `15yr-a` | F12-R.17, R.19, R.20, F12-15z.9–.11; the index/register text defects (F12-R.21, R.22) are a new phase, not 15yr |

## Hazards

- **Scan resolution / campaign.** Pixel height depends on DPI; the five silver series may share one scan campaign. 15u
  records `page_w`/`page_h` per test scan and reports them per class; the diagnostic set shows other series.
- **Thin margin** (86.0 / 89.5 in the fit set): report every test volume within ±5 px of the cut.
- **Segmentation model:** 455/456 (another Laypa model) fit the print side here, but a third model could differ.
- **Only local PageXML:** the 78 staged tars, the pilot sources, and the 110 UA-023 tars. Never the external drive.
- coreshare: 8 workers shared; launch with `coreshare.py run`; never hardcode a worker count.
- The 15q caches `data/hand_print_geom/15q-a/` and its `thresholds.json` are protected (fit input); so are the 15u
  outputs and `data/hand_print_geom/15u-a/` (15v/15w reference values).
- **15v/15w:** a hand inventory's output changing in `15v-a` means a rule field leaked through `Run` (compare is exact);
  `stage_units` passes `republic_path` to every inventory today, so mode runs must null it for hand ones; the corpus
  `wbc` default of the 1.01.02 root changes as the six printed volumes leave the hand rules (pinned sets keep the
  per-inventory output; a difference is a stop). 3463 (silver hand, 86.5 px) will be print-built: 15w reads it.
- coreshare was busy at planning (another project held 4 workers); steps check `status` first.
- **15y/15z:** `data/text_layer_v4_full/run_v4.log` holds the drive path: briefs and reports never quote it. The run
  script's default `--work-dir` is the one the user's drive runs use; local runs pass their own. A stage skip that
  ignores a newer upstream row would keep stale tables silently: 15y's resume no-op and compare checks guard it.

## Open questions for planning: settled 2026-10-23 (15u), 2026-10-25 (15v/15w)

- Fit set: the user chose the 76 labelled volumes (P2). Feature definition: as measured (P1). Normalisation: none (P1).
- Sample, weights, reserves: P4. Non-silver diagnostic: P6.
- 15v/15w (facts checked, no user decision needed): rule and params P9; pre-pass rows P10; build design P11;
  crosswalk P12; scope P13; read lists P14. The user's decisions come at the end of 15w (UA-024 full run, UA-025 pilot).

## Review checklist

All ticked in the phase review (2026-10-25), with evidence in `REVIEW.md`; 15wr and 15x get their own reviews.

- [x] 15u acceptance met and re-run by the reviewer (fit values, `cut.json` sha256 and its timestamp before the test
      prepass, stop rule from the volume tables).
- [x] AS tests: `.venv/bin/python -m pytest -q -p no:cacheprovider`; LotA suite with its own interpreter.
- [x] Error analysis read individual volumes and scans (brief list), written up in the report.
- [x] Phase report final; findings in `reports/index.md`; `make reports` clean.
- [x] Public API changes in `docs/shared_library/changelog.md`.
- [x] No archive knowledge in `hand_print.py`: the cut is a required parameter.
- [x] Leakage: no test or diagnostic volume in the fit; the cut frozen before the test prepass.
- [x] v1–v3 presets and dev layers reproduce; `15v-a` hand and 455/456 equal `15m-a`; 168/168 box values (15w).
- [x] Crosswalk bar (P12) and the 15w read lists (P14) applied as fixed; audit reproduces 178/4 on `15m-a`.
- [x] `Brief defects` entries answered.

## Hand-off

Reviewed 2026-10-25 (Opus 5.5): **accept with follow-ups** ([REVIEW](REVIEW.md)).

- **What holds.** The box-height mode (cut 87.75 px, raw pixels, frozen by 15u) passed its pre-registered test (60/60)
  and reproduces exactly in the v4 build (168/168). 50 of 51 near-cut volumes have the right mode. The v4 build, the
  per-mode `params.json`, the run kit and the discovering audit work, and v1–v3 output is unchanged.
- **What does not hold yet.** (1) The 1.10.94 print rules do not fit 1.01.02's single-column print series: the
  splitter cuts about a million lines (F12-R.4). Step 15wr adds a split gate. (2) The print unit rules cut after the
  drop caps in the printed resolutions (F12-15v.4). Step 15x is UA-026 (b), with bars fixed in its brief. (3) The
  mode errors at scale are handwritten index volumes (F12-R.5, about 9 of 251 expected): UA-028 (a), decision P21,
  a list-read-pin step after the full run.
- **Order.** 15wr (Sonnet) → its review → 15x (Sonnet) → its review → the user's UA-024 (full run) and UA-025 (pilot),
  which wait for both. If 15wr's bar fails, 15x does not start and the review decides.
- **Release.** Nothing released in this phase yet. v0.15.0 is recommended after the 15x review: 15q, 15u, 15v, 15w,
  15wr, 15x (all additive, plus the unpublished `sg_1.01.02_v4` behaviour). No sibling import is affected.
- **Update after the 15x review (2026-10-26, Opus 5.5).** 15wr and 15x are done and accepted; the v4 dev reference is
  `data/text_layer_v2_dev/15x-a` (196 + 2; protected from now on). The phase is **done**. What remains is the user's:
  UA-024 (full run; recommended (a) after the release), UA-025 (pilot from `15x-a`, or wait), UA-029 (drop-cap rule in the
  single-column series; recommended (a), the default). After the full run, the P21 step (UA-028 (a)) is planned as a
  short addendum step `15y` of this phase or as the first step of the next v4 phase, whichever the planner prefers; it
  needs the run's `mode/` tables. **Release now: v0.15.0** (15q, 15u, 15v, 15w, 15wr, 15x; all additive API plus the
  unpublished `sg_1.01.02_v4` behaviour; no sibling import affected).
- **Addendum planned 2026-10-27 (Opus 5.5).** P21 is split in two: 15y (Sonnet; mechanics, runs now) and 15z (Sonnet;
  reads and pins, after UA-034). Then the user's drive rebuild and staging (a UA from 15z), an Opus check of the staged
  rebuild, which writes the publication UA (P23: the pinned full run replaces the pilot), and a short review of 15y/15z.
- **For later phases.** The REPUBLIC crosswalk is unusable for 1.01.02 (F12-15v.5). When rules move to a new series,
  read each stage's output per series (F12-R.7). Check for scan-resolution shifts in a full run (F12-R.1): list every
  `print` volume outside the printed series.
- **Review of 15y/15z (2026-10-27, Opus 5.5): accept with follow-ups** ([REVIEW](REVIEW.md#steps-15y-and-15z-review-2026-10-27)).
  The pins, the pin rule, the in-place and the drive rebuild are right and reproduce. Fixes go to 15yr (Sonnet, then a short
  review): a failed upstream row made later stages rebuild from stale inputs (F12-R.19), the audit keys and done-ness
  (F12-15z.9, F12-R.20), the stager (F12-15z.10), and the time model (the extract rows left out the pre-pass, F12-R.17; two
  9-minute sizing scans, one for a no-op `--estimate`, F12-15z.11). The reads put the pins in another light: the `hand` pins fix
  the label of the index volumes but not their text. The hand rules make one unit per region and keep two-column regions in XML
  order, in all hand volumes (F12-R.21, F12-R.22). That is the next text-layer phase, not part of 15yr. **Release:** v0.17.0 after
  the 15yr review (15y, 15z, 15yr; additive API plus `sg_1.01.02_v4` behaviour; no sibling import affected).
- **Review of 15yr (2026-10-28, Opus 5.5): accept** ([REVIEW](REVIEW.md#step-15yr-review-2026-10-28)). The staleness gate, recorded
  sizes, whole-call timing, audit keys and stager deletions work and reproduce. Fixed in review: `done_inventories` had to compare
  with the layer's stages. The 15x-a validation is +12% for 1.01.02 alone, not +33% (F12-15yr.4 revised). **The phase is done.**
  **Release now: v0.17.0** (15y, 15z, 15yr). The next text-layer phase is the register/index volumes (`plan/CONTEXT.md` §4).
