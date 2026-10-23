# Resolved user actions (AS)

### UA-001 Acceptance target for the 116 labelled hand–print rows (2026-10-17, from phase-06 planning)
- **Action:** decide the target share of the 116 labelled rows (LotA `gold/02d-…-rescaled.tsv`)
  that must be free of the five input artefacts on the regenerated layer (baseline 56%).
- **Options:** (a) ≥ 80%; (b) ≥ 90%; (c) no numeric target.
- **Response:** a
- **Resolved 2026-10-17:** the target is **≥ 80%**. Recorded in
  `plan/phase-07-sg-units-and-pilot/PLAN.md` (open questions) and its `STATUS.md`. It applies to
  step 15i's acceptance run.

### UA-002 Merge threshold factor for the line splitter (2026-10-17, from step 15e)
- **Action:** decide the factor in `derive_split_params` (`archival_structures/text_layer/line_split.py`):
  `min_merged_width = round(1.22 * default_line_width)`. On 456 it gives 1,127 px; the notebook
  uses 1,100. With 1.22, 192 merged lines with a short right part (1,100-1,126 px) stay whole.
- **Options:** (a) keep 1.22; (b) change the factor to 1.19 (456: 1,100 px; 455: 1,072 px), re-run
  and look at the false candidates in the 455 margin. Recommended: (b).
- **Response:** b
- **Resolved 2026-10-18 (phase-06 review):** the review read 25 of the 45 lines that 1.19 adds in
  455; about 23 are real merged lines (F06-R.3). The change (keyword `merged_width_factor`,
  default 1.19), the re-run and the reading of the new splits are step 15fr
  (`plan/phase-06-text-layer-v2-sg/step-15fr-review-fixes.md`). F06-15e.5 is marked revised.

### UA-003 Glued-word repair is off for both SG presets (2026-10-17, from step 15f)
- **Action:** decide what to do about glued words; step 15f's segmenter missed the 0.90 bar
  (real precision print 0.62, hand 0.16), so `word_split="off"` in both SG presets.
- **Options:** (a) leave off; (b) a new step with a lexicon of genuine word forms; (c) repair
  only at query time in the consumer. Recommended: (a).
- **Response:** a
- **Resolved 2026-10-18 (phase-06 review):** nothing changes in the code; both presets stay
  `word_split="off"`, and `text` equals `text_raw`. Phase 07's open question on a segmenter
  redesign is closed (`plan/phase-07-sg-units-and-pilot/PLAN.md`). Glued tokens (about 5,950 of
  ≥ 18 letters in 455 + 456) stay in the print layer.

### UA-004 Phase-06 findings for `docs/findings.md` (2026-10-18, from the phase-06 review)
- **Action:** Decide which phase-06 findings go into `docs/findings.md` (for people reusing AS).
  Candidates: F06-15d.1, F06-15d.3, F06-15e.6, F06-15f.2 + .4, F06-P.2.
- **Options:** (a) add all five; (b) add the ones you list; (c) add none. Recommended: (a).
- **Response:** a
- **Resolved 2026-10-20 (phase-07 planning):** all five added to `docs/findings.md` as a new
  section *States General PageXML and the v2 text layer* (before *Tooling*), with F06-15f.1 and
  F06-R.5 cited for the glued-word numbers.

### UA-005 pagexml-tools traps found while planning step 15fp (2026-10-18, from 15fp pre-planning)
- **Action:** Decide whether to fix the traps of F07-P.7 (and F07-15fp.11) in pagexml-tools and
  release 0.9.1.
- **Options:** (a) fix upstream, and AS pins the release in a later step; (b) leave it.
- **Response:** a
- **Resolved 2026-10-20 (phase-07 planning):** the user fixed them on branch `bug_fixes`
  (`99df1e3`, `16a51d0`). Checked by running the local copy without installing it: six of the
  seven are fixed. `PageXMLPage.stats` still raises `TypeError` (F07-P.9), which is now UA-007.
  AS stays on the 0.9.0 wheel for 15fq–15hj; step 15i pins 0.9.1 once it is released (its
  step 0).

### UA-006 How 15g–15i use the 15fp layout, given its negative results (2026-10-19, from step 15fp)
- **Action:** Decide how the planning of 15g–15i (prompt 1) treats the layout layer
  `data/text_layer_v2_dev/15fp-a/`. Details are in `reports/phase-07-sg-units-and-pilot.md`,
  step 15fp:
  - merge precision is 0.82 in hand and 0.13 in print, against a 0.90 bar (F07-15fp.2);
  - print regions are page-level and interleave the two text columns (F07-15fp.3);
  - in hand 3349–3353 the split cuts the recto's margin notes off to the verso, in 11 of 30
    scans read (F07-15fp.4);
  - `min_coverage` sometimes misses the gutter by a whole page (F07-15fp.5);
  - registers fall outside the role rule (F07-15fp.8).
- **Options:**
  - (a) add a fix step **15fq** before 15g, with fresh validation on new seeded samples. It
    would (1) place the hand gutter just left of the recto's margin column, (2) split print
    layout regions on the line `column` field, and (3) keep page furniture (page numbers,
    running dates, catch words) out of merges;
  - (b) plan 15g–15i on the layout as it is and state the limits above in their acceptance
    criteria;
  - (c) plan 15g–15i on the input regions and take only `page` from the layout, flagging
    3349–3353 pages as suspect.

  Recommended: (a). Units (15h) and the old → new `doc_id` mapping are built from these
  tables, so the errors would reach LotA.
- **Default if unanswered:** the 15g–15i planning session works from (b) and lists the limits
  as open issues.
- **Blocks:** the scope of the 15g–15i planning (prompt 1), not the start of that session.
- **Response:** a
- **Resolved 2026-10-20 (step 15fp session):** a fix step **15fq** comes before 15g, with the
  three fixes listed under (a) and fresh validation on new seeded samples. The next session is
  phase planning (prompt 1), which writes the 15fq brief first and then 15g–15i on its output.
  Recorded in `plan/phase-07-sg-units-and-pilot/STATUS.md` and the 15fp block of `PROGRESS.md`.

### UA-008 145 print layout regions still hold both columns (2026-10-21, from step 15fq)
- **Action:** **Decide** how to treat print layout regions whose lines carry both `column` 1
  and 2. Measured on `data/text_layer_v2_dev/15fq-b/print/` (same in `15fq-a`): 36 of 8,742
  regions in 455 and 109 of 5,191 in 456 (109 of the 205 merges in 456). Cause: the merge joins
  a column-1 piece with an overlapping column-2 piece (title pages; pages where a wide line
  carries a column label). Acceptance criterion 6 of 15fq asked for 0. The sample precision of
  the print merges is 28/30 = 0.93, so the brief's fallback did not trigger for print.
- **Options:** (a) accept; 15h splits units on the line `column` field where `column_value` is
  empty; (b) a small follow-up step "15fq2": pieces of a column split with different non-empty
  `column` values never link in the merge (a structural rule, no threshold), then re-run print
  and re-check criterion 6; (c) switch print merges off too (`merge=False` in `SG_PRINT_V2`),
  at the cost of the y-order repair that makes 28 of 30 sampled print merges right.
  Recommended: (b).
- **Default if unanswered:** (a). 15g runs on `15fq-b`; 15h's print rule "a print layout
  region is one column" does not hold for 145 regions.
- **Blocks:** nothing before 15h; 15h's print paragraph rules.
- **Response:** b (user, in chat, 2026-10-21) b
- **Resolved 2026-10-21 (step 15fq2 session):** option (b) implemented as `LayoutParams.column_merge_guard`. On `15fq-c` 0 print layout regions carry both columns (was 36 in 455 and 109 in 456); 145 mixed regions became 303 pure ones; 30 read: 27 right, 3 ambiguous, 0 wrong. Types re-run as `15g-b`. 15h runs on `15g-b`. Report: § Step 15fq2.


### UA-009 `rule:margin_prefix` cuts are mostly wrong (2026-10-21, from step 15g)
- **Action:** **Decide** what 15h/15i do with the 18,983 lines the margin-prefix rule cuts in
  two (1.1% of 1,644,839 text lines). Error analysis (9 decided of 10 read, seed 17): 2 cuts
  right ("Praside | d'Heer van Kersbergen", "Magt. van Herstal | ende Ingezetenen"), 7 wrong
  (the whole line is a margin note, or a main-text word is cut off: "naar | 4o05 Mog.").
  The month-only prefix ("Augt. | haer Ho:Mog:", 423 lines) and `date_prefix` (10/10 right)
  and `pn_suffix` (10/10) are fine. Table: `reports/data/phase-07-sg-units-and-pilot/step-15g/ea_verdicts.tsv`.
  Files: `archival_structures/text_layer/types.py` (`segment_page`, in-line rule 2), `TypeParams.margin_tol`
  (0.02) and `cut_max_dist` (0.04).
- **Options:** (a) keep as built (the brief's rule); 15h joins the two halves of a
  `rule:margin_prefix` cut back into one unit unless the prefix is `date_header`; (b) a small
  follow-up "15g2": keep only month-only prefixes (`date_header`), no other margin cuts, and
  re-run types (about 1 minute); (c) raise `margin_tol` to 0.03 and re-measure (a threshold
  search; the negative result above stays the reference). Recommended: (b).
- **Default if unanswered:** (a).
- **Blocks:** nothing before 15h; 15h unit text quality for ~19k lines.
- **Response:** b
- **Resolved 2026-10-21 (step 15g2 session):** option (b) implemented as `TypeParams.margin_cut="month_only"`. 423 month-only cuts remain (was 18,975 in `15g-b`; 18,552 non-month cuts removed); 30 formerly cut lines read: 0 wrong, 2 ambiguous. Types re-run as `15g-c`; 15h runs on `15g-c`. Report: § Step 15g2.

### UA-010 Word-break sets for the SG pilot: the derived sets fail the sanity anchors (2026-10-21, from step 15hj)
- **Action:** **Decide** how the SG presets get their word-break (WBC) sets for 15i. The per-inventory
  rule gives these sets (identical with the default and the robust statistic; table in the report,
  *Step 15hj*): hand_tar `„` (3349, 3350, 3353), `=„` (3352), empty (3351, 3354); hand_local `-=„`
  except 3822 `-.=„` and 11281 `-:=„`; print 455 `-.=„`, 456 `-=„`. Pooled over the layer's
  inventories `:` passes the rule in hand_local and `.` in print, so the sanity anchors (`:` and `.`
  must fail) are False and `join` refuses without `--ignore-sanity`; the 15hj layer was joined with
  it (recorded in the manifest). Evidence from hand-read samples: `.` as break char is right 11/20
  (455) and 4/10 (3822, the 4 includes OCR-noisy joins), `:` in 11281 is right 9/10 (it is that
  scribe's hyphen), `=` right 7/10; and the rule *misses* `„` in 3351 and 3354 (join_vocab_frac 0.12
  and 0.08, small vocabulary), where 12 of 12 line ends read are genuine `„` breaks.
- **Options:** (a) keep the derived sets and `--ignore-sanity` (3822: 21,486 merges at 0.78
  lower-case right sides, 455: 50,164 at 0.88); (b) pin the SG sets in the presets to `-=„`, never
  `.` or `:` (3822 lower-case share 0.957, 455 0.891; 11281 loses 5,216 genuine `:` merges; 3351 and
  3354 gain their `„` merges); (c) as (b), plus a per-inventory `:` for 11281. Recommended: (b), and
  (c) if the 11281 text matters for the pilot.
- **Default if unanswered:** 15i uses (a) with `--ignore-sanity`.
- **Blocks:** the 15i pilot build (step 0 of its brief decides the WBC sets).
- **Response:** c (user, in chat, 2026-10-21)
- **Resolved 2026-10-21 (step 15hj2 session):** option (c) implemented as `LayerPreset.wbc_pinned` (`-=„`, 11281 `-:=„`). `sanity_pass` true in all three sub-layers; `join` ran without `--ignore-sanity`. 3822 lower-case share 0.779 -> 0.957, 455 0.877 -> 0.891; 3351/3354 gained 174/126 merges. Layer `15hj-b`. Report: § Step 15hj2.

### UA-011 Check the pilot crops and the 116-row evidence, then agree to publish (2026-10-21, from step 15i Part A)
- **Action:**
  **Run** (AS root): `open reports/data/phase-07-sg-units-and-pilot/step-15i/crops/index.html`.
  Check that the red (hand) and blue (print) boxes cover the tokens on your rows 39, 60 and 241 and on the
  30 random rows. Note that row 60 is unlabelled in the gold sheet. Then read the evidence in the report
  (`reports/phase-07-sg-units-and-pilot.md`, *Step 15i*; table `reports/data/phase-07-sg-units-and-pilot/step-15i/rows116.tsv`).
  Result to weigh: predicted share free 0.526 (61/116), upper bound 0.560, target 0.80, old baseline 0.56: 40 of
  the 51 artefact rows persist (26 missing whitespace, repair off per UA-003; 14 HTR/OCR errors); the layer fixes
  6 (unit boundaries). The audit passes except the reported merge share below 0.95 in 6 inventories.
  **Decide** (also the word-break pin): step 15hj2's error analysis found the `=` merges that the pin
  UA-010 (c) adds in hand_tar (3349, 3350, 3353; 821 merges, lower-case right-side share 0.74) only 11 of 20
  right, 5 wrong (glued words, e.g. "aen=" + "haer", "missiven=" + "vanden"), 4 unclear. The `„` merges added
  (297) have share 0.95.
- **Options:**
  (a) publish the pilot as planned (pinned `-=„`, as built);
  (b) fix first, then rebuild (list what): for example drop `=` from the pinned set of the hand_tar inventories
  (3349-3354; it removes about 821 merges, 3351 and 3354 keep their `„`), which needs a changed preset, a rebuild of
  `15i-a` and a new audit;
  (c) do not publish.
  Recommended: (b) with `=` dropped for hand_tar, if the 11/20 matters to LotA; (a) if LotA only needs the
  units and offsets (the offsets are exact either way and `n_breaks_merged`/`boundary_kind` let LotA see each merge).
- **Default if unanswered:** nothing is published.
- **Blocks:** Part B of step 15i (publication to `shared-data/`) and LotA's re-alignment (hand-off 4).
- **Response:** b (user, in chat, 2026-10-21; drop `=` for hand_tar)
- **Resolved 2026-10-21 (step 15i2 session):** `=` dropped from the pinned set of 3349, 3350, 3351, 3353 and 3354 (`SG_HAND_V2.wbc_pinned`); hand rebuilt as `15i-b/hand`, print copied. 820 merges removed in 3349/3350/3353 (276, 226, 318; 818 at `=`) and 3 in 3351; lower-case share of the three 0.981 to 0.989. Report: § Step 15i2. Follow-up: UA-012.

### UA-012 Check the rebuilt pilot (15i-b) and decide whether to publish it (2026-10-21, from step 15i2)
- **Action:** **Decide** whether to publish `data/text_layer_v2_dev/15i-b` (hand rebuilt with `=` dropped for
  3349, 3350, 3351, 3353, 3354; print identical to 15i-a) to `shared-data/NL-HaNA_{1.01.02,1.10.94}/text_layer_v2/`.
  Numbers (report `reports/phase-07-sg-units-and-pilot.md`, *Step 15i2*): 820 merges fewer in 3349/3350/3353 (+3 in
  3351); lower-case right-side share of those three 0.981 to 0.989; read sample of 20 removed `=` merges: 11 right to
  split, 6 wrong (compounds such as "Lant|zaten" broken again), 3 unclear; audit 176/182 as before (6 known merge-share
  failures); 116-row evidence unchanged (share free 0.526, bound 0.560, target 0.80). The 116 rows are
  identical to 15i-a, so the crops you checked for UA-011 still apply.
- **Options:** (a) publish `15i-b` (Part B of step 15i, new Sonnet session); (b) fix first, then rebuild (list what;
  for example a capital/digit-right guard in `join.py`, which would address 17,128 wrong merges overall and the
  capital-right `=` cases); (c) do not publish.
  Recommended: (a) if LotA's re-alignment (hand-off 4) is the next need, since the layer offsets are exact and
  `n_breaks_merged`/`boundary_kind` expose every merge; (b) only if LotA needs cleaner tokens before aligning.
- **Default if unanswered:** nothing is published.
- **Blocks:** 15i Part B (publication to `shared-data/`) and LotA's re-alignment (hand-off 4).
- **Response:** a
- **Resolved 2026-10-21:** answered (a). `15i-b` published by step 15i Part B to `shared-data/NL-HaNA_{1.01.02,1.10.94}/text_layer_v2/`; audit 176/182 on the copies; catalogue, changelog, LotA note and AS `docs/text_layer.md` written.

### UA-013 What happens to the published `text_layer_v2` after the review fixes (2026-10-21, from the phase-07 review)
- **Action:** **Decide.** The review found four defects in the published pilot
  (`plan/phase-07-sg-units-and-pilot/REVIEW.md`, rows 1–4):
  - print line order scrambled in 335 units of 455 (rows 153, 185 and 275 of the 116 are affected);
  - a stray continuation comma inside 1,8k hand words, mostly in 11182, 3352 and 11281 (none of the
    116 rows);
  - 77 print legacy ids lost needlessly;
  - wrong page-number method tags.

  Step 15ir fixes them into the dev layer `data/text_layer_v2_dev/15ir-a/`. It does not touch
  `shared-data`.
- **Options:**
  - (a) Keep `text_layer_v2` as published. 15ir adds a *Known issues* note to LotA §6 and to
    `docs/text_layer.md`, and the fixes go into the full SG run (`text_layer_v3`, D17).
  - (b) After 15ir, publish `15ir-a` as a new pilot version, `text_layer_v2_1` next to `v2`. This
    means a new catalogue row and a LotA note, and LotA re-aligns on it.
  - (c) As (b), but it replaces `v2` before LotA has used it. That needs the shared-data README's
    in-place rule waived.

  Recommended: (a), because the defects touch 3 of the 116 rows and the ≥ 80% target is out of
  reach for other reasons (glue, HTR/OCR).
- **Default if unanswered:** (a).
- **Blocks:** nothing in 15ir. Under (b) or (c), a publication step after 15ir.
- **Response:** a
- **Resolved 2026-10-21 (phase-07 review session):** answered (a). `text_layer_v2` stays as published; step 15ir (brief updated) adds the *Known issues* notes to `docs/text_layer.md` and LotA §6 and builds the fixes into the dev layer `15ir-a` only; the fixes reach SG data in the full run `text_layer_v3` (D17).

### UA-014 NLP's two requests for `text_extent` after the `early_end` verdict (2026-10-21, from phase-08 planning)
- **Action:** **Decide** whether AS adds NLP's two options to
  `archival_structures/analysis/text_extent.py`. The verdict and the requests are in
  `docs/from-nlp-start-page-detection.md` §1. The two options:
  1. `top`/`bot` over body-text regions only (a region-type filter);
  2. a continuous margin-gap column, `bot - stable_bot` and the top equivalent, next to the class.

  Both would be additive: new keyword arguments and new columns, with defaults that leave the
  current output unchanged. NLP imports `compute_page_text_extent`, `estimate_stable_margins` and
  `classify_text_extent`.
- **Options:**
  (a) both options, as one small step in phase 10 (optional), with tests and an NLP re-check of
      its report/84 numbers;
  (b) only the margin-gap column (cheap, generic; the body-text gain was found post hoc and is
      small: recall 0.315 → 0.342);
  (c) neither: the verdict is recorded and `text_extent` stays as it is.

  Recommended: (a). Both are additive and generic. The region-type list must be a parameter,
  not NLP's three types hard-coded, and the default stays "all regions".
- **Default if unanswered:** (c). Nothing changes in `text_extent`; NLP keeps computing the
  variants in its own scripts.
- **Blocks:** nothing in phase 08. It only decides whether phase 10 gets a `text_extent` step.
- **Response:** a
- **Resolved 2026-10-21 (phase-08 implementation session):** answered (a). Phase 10 (optional) gets a `text_extent` step with both additive options: a region-type filter for `top`/`bot` (types as a parameter, default all regions) and margin-gap columns (`bot - stable_bot` and the top equivalent), with tests and an NLP re-check of its report/84 numbers. Recorded in `plan/PLAN.md` (row 10), `plan/CONTEXT.md` (sibling requests) and `docs/from-nlp-start-page-detection.md` (AS status).

### UA-016 Free disk space for staging the compressed PageXML (2026-10-22, from the disk-space audit)
- **Action:** **Decide** which groups of `docs/audits/disk_space_2026-10-22.md` to clear, then **Run** that file's
  *Commands* blocks for them. The volume has 20 GiB free. The audit lists A (redundant, 2.0 GiB), B (superseded, 5.4),
  C (compress, 3.8 net) and D (move to the external drive, 36.8; 16.4 of it in AS) across AS, LotA, NLP, republic,
  republic-segmentation and shared-data. Nothing has been changed yet.
- **Options:** (a) A only. (b) A + B + the AS rows of C and D, about 26 GiB; no sibling's active work is touched
  (B removes only NLP resume checkpoints and a LotA venv that its `CLAUDE.md` does not use). (c) everything, about
  48 GiB; first ask NLP about `output/embeddings` and the CSV paths, and republic about its two fastText models. (d) name
  rows in **Response**. Recommended: (b). Run the C block for the 8 SG pilot inventories only after UA-015 item 1.
- **Default if unanswered:** nothing is removed or moved; the staging for UA-015 stays limited to the 20 GiB free.
- **Status:** the user chose (b) in chat on 2026-10-22. Done: D (AS) moved; C packed the 6 other archives' PageXML
  (2026-10-22, file counts checked). Still open: removing `15fr-a`/`15hj-a` (copied, partly deleted), A, the B removals,
  and the C block for the 8 pilot inventories (after UA-015).
- **Blocks:** staging the PageXML for UA-015 beyond what fits in 20 GiB.
- **Response:**
- **Resolved 2026-10-22 (same session):** answered (b) in chat, not in this field. The user moved D (AS) to the drive and ran A and B. I packed C (the other 6 archives' PageXML as `.tar.gz`, file counts checked) and removed the two partly deleted dev layers' permission block. The C block for the 8 pilot inventories is dropped: UA-015 (rewritten) reads them in place. Free space went from 20 to about 46 GiB. The disk clean-up made 11 local-data tests fail; they now skip (F09-15n.4). Details: `docs/audits/disk_space_2026-10-22.md`.

### UA-015 Run the full `text_layer_v3` build from an external drive (2026-10-22, from step 15m; rewritten after step 15n)
- **Action:** **Decide** the scope, then **Run** `scripts/run_text_layer_v3_full.py` yourself. The local disk can't hold
  the PageXML (UA-016), so the inputs and the layer live on your drive. The script's docstring is the manual
  (`.venv/bin/python scripts/run_text_layer_v3_full.py --help`).
  1. On the drive, put one dir of hand archives (`NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`, `.zip` or a dir of that name)
     and one of print archives (`NL-HaNA_1.10.94_<inv>.zip`, or `.tar.gz`). The 14 pilot inventories are local
     already: pass their dirs as extra `--hand-src`/`--print-src` sources (lower priority), as below.
  2. REPUBLIC paragraphs exist only for print 455 and 456 (LotA). Other print inventories build without the line-id
     crosswalk; the script warns.
  3. **Run** from `/Users/marijnkoolen/Code/Huygens/archival-structures` (replace the `<drive>/…` placeholders):
     ```
     D=/Users/Shared/claude_projects/data_scopes
     .venv/bin/python scripts/run_text_layer_v3_full.py --check-only \
       --hand-src <drive>/sg_hand --hand-src $D/shared-data/NL-HaNA_1.01.02/pagexml --hand-src data/PageXML/NL-HaNA/NL-HaNA_1.01.02 \
       --print-src <drive>/sg_print --print-src $D/shared-data/NL-HaNA_1.10.94/pagexml \
       --republic-dir $D/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir <drive>/text_layer_v3
     ```
     It prints the inventory counts, the predicted output size (about 0.1 x the XML) and a time range, and checks the
     free space on the drive. Then run the same command with `--limit 1 --out-dir <drive>/text_layer_v3_smoke`, and
     then without `--check-only`. After an interruption, rerun the same command: it resumes. The log is
     `<drive>/text_layer_v3/run_v3.log`. For a subset, add `--hand-inventories-file F` (one inventory per line).
  4. Afterwards, tell a session the counts in the log's last lines; publishing to `shared-data` is a separate decision,
     because the layer may not fit locally.
- **On hold (2026-10-23):** don't run the script yet. The user pointed out that the full SG corpus is 9,122 inventories
  in 1.01.02, whose hand/print split is mostly unknown and includes mixed volumes. AS treats 1.01.02 as hand and 1.10.94
  as print because the presets are keyed by archive, a leftover of the pilot (see the chat of 2026-10-23). The scope
  question below replaces the earlier one.
- **Options:** (a) build only what LotA needs next (the hand inventories of the years it aligns, plus their print
  volumes), with the current presets; (b) all 9,122 with the hand preset, flagged in the manifest as untested on printed
  and mixed volumes (about 400 GB of XML in, 40 GB of layer out, estimated from the pilot); (c) first plan a phase that
  removes the archive-level hand/print split (choose the parameters per page or per region from the layout), then run;
  (d) later. Recommended: (a) now and (c) in phase 10 planning.
- **Default if unanswered:** no full run; the pilot `text_layer_v2` in shared-data stays the only published layer.
- **Blocks:** only the `text_layer_v3` build and its publication.
- **Response:** (a): for now only the local pilot inventories, published to shared-data as a v3 pilot (LotA request, docs/from-language-of-the-archive.md §7). Full-run scope later, with (c) in phase 10 planning.
- **Resolved 2026-10-23:** (a), restricted to the local pilot. Step 15o published the v3 pilot (dev layer `15n-a`, the
  14 hand + 2 print pilot inventories) to `shared-data/NL-HaNA_1.01.02/text_layer_v3/` and
  `shared-data/NL-HaNA_1.10.94/text_layer_v3/`, `coverage: partial`, audit 178/182; LotA notified in its
  `docs/from-archival-structures.md` §9. The script was not run; the full-run scope and option (c) go to phase 10
  planning (`plan/PLAN.md` row 10). A full run will be a new layer version.
