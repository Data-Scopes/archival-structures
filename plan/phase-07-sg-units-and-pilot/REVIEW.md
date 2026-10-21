# Phase 07 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-21. Four review subagents (layout, types, units, join/wbc/build) read
  the diff and probed edge cases; every defect below was re-checked by the reviewer on `15i-b`.
- **Verdict:** **accept with follow-ups.** The pilot builds, reproduces and passes its audit, v1 is
  byte-identical and both siblings are green. Five code defects reach the published
  `text_layer_v2` (rows 1–5, F07-R.1–R.5). None corrupts offsets or ids, but two change what LotA reads (print
  line order, a stray comma inside 1,8k hand words). Step 15ir
  ([brief](step-15ir-review-fixes.md)) fixes them into a dev layer; republishing is UA-013.

## Checklist

- [x] **Acceptance re-run** (table below). Audit on `15i-b`: 176 / 182, the same 6 merge-share
      failures; `rows116.py --layer 15i-b`: 0 of 116 × 33 cells differ from `step-15i2/rows116.tsv`.
- [x] **Published copies = `15i-b`.** `diff -rq` of both `text_layer_v2/` dirs against `15i-b`: only
      `manifest.json` (adds `publication`, no other key differs), and `progress.log` / `status.tsv`
      left out, as Part B says. 238 / 52 files.
- [x] **v1 byte-identical:** the phase-05 check re-run into the scratchpad gives `TOTAL 157 / 157`,
      "no differing rows" (manifests gain only `allow_subset_wbc` in `params`).
- [x] **AS suite:** `.venv/bin/python -m pytest -q -p no:cacheprovider` → 1,101 passed (67.9 s).
      **NLP** `venv/bin/python -m pytest -q -p no:cacheprovider tests` → 56 passed; **LotA**
      `venv/bin/python -m pytest -q -p no:cacheprovider tests` → 367 passed, 1 xfailed. Both import AS
      from the working tree.
- [x] **No write through a symlink; v1 inputs untouched.** Part B's protected-hash diff lists only the
      shared-data README and the new dirs; no `15i-src` target changed (15i2 hashes identical).
- [x] **Leakage (15h cue fit): clean.** `paragraph_features` uses per-region geometry only (medians
      per region, no labels); the grid runs on `pairs["dev"]` (455 even scans) and test is scored once;
      REPUBLIC is read only after the units are built (`units.py:449`), so the published units do not
      depend on it. Caveat: the `split_half` crosswalk class was added after looking at 456, which is
      all test (it changes the test reference, not the fitted cues): F07-15h.3 revised.
- [x] **Error analysis per step read individual cases** and is in the report; the reviewer's own reading
      is below. Two steps' numbers are revised (counters, 11182 share).
- [x] **Changelog vs API:** the entries match the signatures, with gaps listed in 15ir (missing
      `units` names, `decide_v2`, optional `--pagexml-dir`; the 15fq2 "defaults unchanged" is wrong).
- [ ] **The user signs off on the 116-row result.** Predicted 0.526 (bound 0.560) against 0.80; the
      real check is LotA's re-alignment (D19). Open; not a review defect.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| all | AS suite | 1,101 passed | 1,101 passed | yes |
| 15hj/15hj2 | v1 check | 157 / 157 | 157 / 157 | yes |
| 15i/15i2 | audit `15i-b` | 176 / 182, 6 merge-share | 176 / 182, same 6 | yes |
| 15i/15i2 | 116 rows | share free 0.526, bound 0.560 | 0.526, 0.560; 0 cells differ | yes |
| 15i B | published = `15i-b` | tables equal | `diff -rq`: manifest only | yes |
| 15hj | offsets | 0 violations | 0 in 1.65 M segments (subagent, full re-derivation) | yes |
| 15h | `n_id_collisions` | 58 / 32 | 58 / 32 (`units/stats.json`) | yes; cause revised (F07-R.3) |
| 15fq2 | `n_column_blocked` | 62 / 113 | 62 / 113 | yes; inflated (F07-R.5) |

Layout, types and units acceptance of 15fq–15h were not rebuilt: `15i-b` reproduces `15hj-b` table for
table (F07-15i.1), and the audit re-run covers their invariants.

## Findings

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 (F07-R.1) | defect | `analysis/page_structure.py:303` `split_regions_on_line_field` | Column-split print pieces keep XML line order. 455: 546 regions jump back > 50 px (416 `split_at_column`, 127 `unchanged`); units with a jump 335 (455), 160 (456); `455_0010` reads y 2952 → 3102 → 1371 → … → 1322. Rows 153, 185, 275 of the 116 sit in such units | 15ir fix 1 (sort split pieces); unchanged regions left (R5), reported |
| 2 (F07-R.2) | defect | `text_layer/join_v2.py:55` | Scribes repeat the break mark at the next line start, read as `,`: 1,305 of 4,699 merges in 11182 ("gero,ijeert"), 336 in 3352, 170 in 11281. The merge is right but the comma stays in the word. Explains most of 11182's 0.699 (lower or punctuation 0.977). None of the 116 rows | 15ir fix 5 (`join_lead_marks`) |
| 3 (F07-R.3) | defect | `text_layer/units.py:392`, `:213` | Every legacy id is reserved, so a unit whose box equals a legacy box and holds all its lines gets `w+1`: 45 of 58 collisions (455), 32 of 32 (456); those 77 regions map `changed`. F07-15h.8's "all are legacy boxes, map unaffected" is wrong | 15ir fix 4 |
| 4 (F07-R.4) | defect | `text_layer/types.py:275`, `:371`, `:202` | (a) 1,414 band lines that are never chain candidates keep `rule:pn_line`, the in-chain tag; (b) about 87 date-prefix remainders reach `page_number` without the chain; (c) rule 1 splits whole date lines with a year (19: "28. Junij 1650." → date + `running_header`) | 15ir fix 3 |
| 5 (F07-R.5) | defect | `analysis/page_structure.py:188,210`, `layout.py:143` | `n_guard_blocked` / `n_column_blocked` count a relation once per merge pass under each new merged id (probe: 1 relation → 3 entries). F07-15fq.7 (2,274 pairs) and F07-15fq2.1 (62 / 113) are overstated | 15ir fix 2 |
| 6 | debt | `build.py:650` | `--allow-partial` gives a v2 inventory without a decision the pinned *default*, not its own pin (would bring back `=` in 3349) | 15ir fix 6a |
| 7 | debt | `join_v2.py:57`, `:148` | `lead` hard-coded 1 (library strips a second `„` on `„„`; 0 cases now); the offset check compares a prefix only, so a short span is never flagged; 51 merges drop two break chars, against the docs | 15ir fixes 5, 6c |
| 8 | debt | `units.py:35`, `:449` | LotA path hard-coded as the REPUBLIC default; a missing file skips the crosswalk silently; `params.json` doesn't record the file | 15ir fix 6b (warn, record); moving the default out of the library: phase 08 |
| 9 (F07-R.9) | debt | tests | Suite stays green with the page-number chain, `merge=False`, furniture `exclude`, or pin application broken (no test exercises them); "legacy = 15fp" tests have no golden output | 15ir *Tests* |
| 10 | debt | `docs/text_layer.md:255,297,237`; changelog | Docs say `types`/`units` are not in `all` (they are); scans schema lacks `n_column_blocked`; changelog misses public names | 15ir *Docs* |
| 11 | debt | `units.py:270` | `split_original` maps a REPUBLIC `line_end` id to the first half (553 ids); the crosswalk has no role column | Follow-up (phase 08 / v3 schema) |
| 12 | debt | `page_structure.py:138` | The interleave guard is per pair; a third region can bridge it and absorb the note anyway | Follow-up |
| 13 | debt | `types.py:161` | Bottom-band years are typed `page_number` (about 40) | Follow-up |
| 14 (F07-R.8) | observation | manifests in `shared-data` | `text_layer_v2` was built at git head 73e1517 with 8 dirty files; `as_version` 0.9.0 (installed metadata at build time). Its code exists only in the working tree until the user commits | User: commit + release before 15ir (*Release*) |
| 15 | observation | `types.py:318` | `page_w` is the scan width, so `*·page_w` tolerances are twice as loose on openings as on single scans | None (brief-specified) |

## Error analysis read by the reviewer

- **11182 merges** (all 1,416 with a non-lower-case right side counted, 15 read): 1,305 start with `,`, 80
  more `=` + `,`; every one read is a continuation ("bin|,nen", "por|,tien", "geschie|,den"). 3352
  (6 read) and 11281 (6 read, `:` + `,`) show the same convention. 15hj's note ("comma-initial
  fragments … not read further") saw it but did not count it, so F07-15hj.5 / F07-15i.2 blamed
  upper-case right sides for a share that is mostly this.
- **Print line order:** three units read. `455_0010` (above) is scrambled across the column; the two 456
  cases are a drop capital ("DE Resolutien" at y 2315 after its second line) and an index line, minor.
  F07-15fq.8 ("3 of 17 keep a running-head line out of y order") under-states the 455 problem.
- **Twins:** 77 of 77 bumped units whose box is a legacy box hold every line of that legacy region.
- **Types:** the subagent's counts were re-derived on `15i-b` (`rule:pn_line` 10,327, `_offchain` 1,792,
  19 whole date lines split). F07-15g.6's "10,363 in chain" includes the non-candidates.
- The steps' own error analyses otherwise hold up: verdict files exist for each sample, seeds are given,
  and the negative results (hand merges, margin cut, robust statistic, 80% target) are reported as such.

## Brief quality

- **Protected-hash method missing** in six briefs (15fq, 15g, 15fq2, 15h, 15hj2, 15i); every session
  re-invented it. Fixed in `plan/templates/STEP.md` (names the 15i2 `protect.py` format) and in the
  15ir brief.
- **Unmeasured expectations** despite the template rule: 15h "`n_id_collisions` 0", 15i2 "3351 and 3354
  unchanged", 15g2 used 15g-a's count for 15g-b. The planner should run each expected number on the
  layer the step will start from. Recorded in `plan/CONTEXT.md` (planning notes).
- **Layer and path facts** (REPUBLIC file location, audit glob `*_text_layer_v2`, one layer root per
  sub-layer for `wbc`) were found by the sessions; now in `plan/CONTEXT.md`.

## Updates applied

- [x] Phase report: *Summary*, *Review* and *Follow-ups* written; every finding `confirmed`, `revised` or
      `withdrawn`; status *final*
- [x] `reports/index.md`: phase-07 findings appended, phase row added
- [x] `make reports` runs without warnings
- [x] Review-fixes brief written (`step-15ir-review-fixes.md`)
- [x] `docs/shared_library/changelog.md` complete for this phase: the gaps were filled by 15ir's entry
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated; `plan/CONTEXT.md` created
- [x] phase `STATUS.md` overwritten
- [x] Release recommendation: below

## Release

1. **Now, before 15ir:** commit the current tree. It is the code that built the published
   `text_layer_v2`, so that its manifests' `git_head` (73e1517 + dirty files) gets a commit. Release
   it as **v0.11.0** (phase 07: layout, types, units, v2 wbc/join, pinned WBC sets).
2. **After 15ir:** v0.12.0 (review fixes; default layout and types output change for print, a new
   preset field for hand).

## Step 15ir review (2026-10-21)

- **Session:** Opus 5.5, same session as the phase review. 15ir ran as a Sonnet subagent (prompt 2).
  UA-013 was answered (a) before it started, so the brief said "no publication".
- **Verdict:** **accept.** All of F07-R.1–R.5 are fixed in code and in `15ir-a`. Each fix is gated:
  v1 gives 157 / 157 and the `LEGACY_15FP` tests pass unchanged. Release as **v0.12.0**.

| Check | Re-run by the reviewer | Step's claim |
|---|---|---|
| AS suite | 1,123 passed, 97 subtests | same |
| `audit.py` (the 15i original) `--layer 15ir-a` | 178 / 182; fails 3351, 3823, 455, 456 (share) | same |
| `rows116.py`, `15i-b` vs `15ir-a` | 8 cells differ (`print_unit_ids` 153, 238; `print_n_candidate_units` 153, 185, 236, 238, 275, 282); share 0.526 | same |
| letter `,` letter in `join/units.text` | 11182 1,315 → 15; 3352 342 → 6; 455 7 → 7 | 1,856 commas stripped |
| offsets `text[cs:ce] == line.text[lo:…]` | 0 bad in 11182 (57,949 segs), 3352 (97,587), 455 (173,475) | 0 in 16 inventories |
| `split_at_column` regions, > 50 px backward baseline jump | 455 410 → 0; 456 526 → 0; `unchanged` 130 / 0 | same |
| `n_id_collisions` | 455 58 → 0, 456 32 → 0 | same |
| permissions, `make reports` | all new files g+w; 0 warnings | |

**Read in the diff:**
- *Fix 2.* Each pass gets fresh dicts, and the caller gets only the pass that ends the loop. That pass
  runs on the final regions, so no merged-away id can appear.
- *Fix 3.* `whole` moved above rule 1 and has the same value as before: it tests the full line text.
  The relabel tests the candidate's line id per page.
- *Fix 4.* `claimable` excludes the scan's layout ids and an entry is deleted once a unit takes it, so
  no two units hold one id. Units that don't claim still go through the collision loop over `taken`.
- *Fix 5.* `lead[i+1] = len(nxt) - len(stripped)`. The `extra` loop in the span code counts a second
  break character that the library strips by itself. It is heuristic (prefix matching), but the
  full-span check backs it up: 0 violations. An empty next text is safe, because
  `"".isalpha()` is false.

**New findings:**

| # | Finding | ID |
|---|---|---|
| 1 | The re-run reproduces the step | F07-R.10 |
| 2 | `claimable` is built by scanning all legacy rows per scan. 455 units: 4.0 → 10.0 s, about 6.5 s of it this loop. Group by scan once before `text_layer_v3` | F07-R.11 |
| 3 | The baseline sort swaps same-row fragments (+49 pairs in 455, +564 in 456). That is small next to the 936 regions it fixes, but a row-tolerant key would remove it | F07-15ir.3 |

**Brief quality.** The step had to re-measure three numbers:
- 11 date lines was really 19;
- the comma counts included `,` + space;
- 455 had 13 collisions only with fix 4 alone.

All three come from defect 2 of `plan/CONTEXT.md` §5: when one step combines several fixes, measure each
expected number with **all** of them applied, or say which fix it belongs to alone. The step also found
that the changelog had no *Unreleased* heading after the release. Both points are now in
`plan/CONTEXT.md` §5.

