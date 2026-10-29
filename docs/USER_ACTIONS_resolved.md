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

### UA-017 Stage PageXML of 12 more 1.01.02 inventories for the hand/print classifier (2026-10-23, from step 15p)
- **Action:** **Run** in any shell, with `SRC` set to the folder on your drive that holds the
  `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` files (the agent never touches the drive):
  ```
  SRC=/path/to/drive/folder      # <- edit
  DST=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml
  for i in 3712 11117 46 4623 12169 12184 3463 3166 11930 9469 9486 4617; do
    cp "$SRC/NL-HaNA_1.01.02_${i}-pagexml.tar.gz" "$DST/"
  done
  chmod g+w "$DST"/NL-HaNA_1.01.02_*-pagexml.tar.gz
  ```
  Why these (list and reasons: `reports/data/phase-10-hand-print-per-region/step-15p/staging.tsv`):
  5 `mixed` volumes spread over the print-share range (3712, 11117, 46, 4623, 12169), 3 silver
  contradictions (12184 print->mixed, 3463 hand->print, 3166 hand->mixed), 2 odd-material volumes
  (11930 dense regions, 9469 layout outlier), 2 `print`-labelled volumes outside the silver groups
  (9486, 4617). Free disk on 2026-10-23: 158 GB; the six local hand tars are 6-62 MB each.
- **Options:** (a) stage all 12; (b) stage a subset (any); (c) stage none. Recommended: (a). Note:
  step 15p's stop rule fired (silver agreement below 90%), so 15q will be re-planned; the volumes
  still serve any region classifier that replaces plain density, because they are exactly the
  contradictory and mixed material.
- **Default if unanswered:** 15q validates on the 16 local inventories only (the 14 dev volumes plus
  455/456).
- **Blocks:** 15q's staged-volume validation (nothing else).
- **Response:** Done.
- **Resolved 2026-10-23:** all 12 tars are in `shared-data/NL-HaNA_1.01.02/pagexml/`. The command above was wrong: it
  ran `chmod g+w` on files that had no group read (`-rw--w----`), so the agent could not read them. The user fixed
  that the same day, and the re-planning session checked that all 12 open (`tar -t`). They are diagnostic material
  for 15q (contradictory and mixed cases), not part of its stop-rule sample (UA-020).

### UA-018 How phase 10 continues after the density stop rule fired (2026-10-23, from the 15p review)
- **Action:** **Decide.** Step 15p's census (report section *Step 15p*, F10-15p.1–3) failed decision P3's stop
  rule: 72.0% of silver-print and 84.7% of silver-hand volumes get their class (needed: 90%). The review
  re-computed both numbers from `inventories.tsv`. The failures sit in two series: printed *plakkaten* (67 of 76
  `mixed`, large type lowers density) and neat dense hand in the net-resoluties (19 `print`, 37 `mixed`); the
  *gedrukte resoluties* (99/99) and *nouvelles* (64/64) are all right. Steps 15q–15t assume the density rule,
  so they were not run.
- **Options:**
  (a) Opus re-plans 15q around size-independent line geometry from the PageXML (e.g. right-edge alignment of
  lines, line-height and line-spacing regularity, chars per line height), fitted on the dev volumes and
  evaluated on staged silver volumes with the same 90% stop rule. Needs a larger staging list than UA-017
  (silver *plakkaten* and dense-hand net-resoluties). It may fail again; then (b) or (c).
  (b) Volume-level modes from the EAD: *gedrukte resoluties*, *nouvelles* and *plakkaten* are `print`, everything
  else `hand`; region-level modes stay out of v4. Fixes the known gap (3771–3773, 3821–3823) fast, but assumes
  that series titles are right and volumes are not mixed, which the census shows is not always so (F10-15p.5–6).
  (c) Close phase 10 as a negative result: publish nothing new, keep the census tables as a map of where print
  is likely, and move on to phase 11.
  — recommended: (a). It is the only option that keeps "as few assumptions as possible", and the build already
  reads PageXML lines, so the features exist where v4 needs them. Post-hoc observation, not a basis for a rule:
  a volume print char share ≥ 0.5 would have put 239/239 print and 502/548 hand right, but that cut was chosen
  after seeing the silver labels.
- **Default if unanswered:** nothing runs; phase 10 stays stopped after 15p.
- **Blocks:** steps 15q–15t of phase 10 (phase 11 is independent).
- **Response:** a
- **Resolved 2026-10-23:** (a) acted on. The re-planning session measured line geometry on the dev volumes before
  writing a brief (report, *Planning*, F10-P.6). One size-independent feature separates them (line-box height /
  baseline pitch), but in sample only 11 of 14 dev volumes get their class under the 0.9/0.1 volume rule. Running
  15q as written would very likely fail the stop rule, so how to go on is the new decision UA-019. Staging for it
  is UA-020.

### UA-019 Line geometry separates the dev volumes, but not under the 0.9/0.1 volume rule (2026-10-23, from the 15q re-plan)
- **Action:** **Decide.** The re-plan measured size-independent line geometry on the 14 dev volumes (report section
  *Planning*, F10-P.6; script `reports/data/phase-10-hand-print-per-region/planning-15q/geom_features.py`).
  Nothing was fitted on silver material. One feature works: `hrel` = median line-box height / median baseline pitch
  per region (hand ascenders and descenders make the box tall relative to the line spacing). Per region: AUC 0.91, and
  0.94 for 3350 against print. Per volume, the medians do not overlap: hand 1.62–1.81, print 1.37–1.46. The other
  features (chars per pitch length, right-edge alignment, baseline straightness, spacing regularity, HTR confidence)
  have AUC 0.50–0.71 and add nothing in a leave-volumes-out logistic regression (0.885 against 0.913 for `hrel` alone).
  Per page (char-weighted median, pages ≥ 200 chars), a cut at 1.53 fitted on dev labels these shares of pages
  `print`: hand volumes 0.00–0.19 (11281 0.13, 3351 0.19, 3354 0.16), print volumes 0.91–0.98. **Under the P3 volume
  rule (print ≥ 0.9, hand ≤ 0.1, else `mixed`), 3 of the 8 hand volumes are already `mixed` in sample**, so the
  90% stop rule would very likely fail on silver too. 455/456 (other Laypa model) have region medians of 1.52/1.57,
  right at the cut, so transfer to 1.10.94 is doubtful.
- **Options:**
  (a) Run 15q as a test registered in advance, with the rules unchanged: the `hrel` page cut frozen from dev, the
  0.9/0.1 volume rule, and the 90% stop rule on the 60-volume random silver sample (UA-020). It probably ends in a
  clean negative result for geometry. Cost: staging plus one Sonnet step.
  (b) **Volume modes from the volume's own PageXML.** The build uses one mode per volume: the page majority of
  `hrel` (print share of pages ≥ 0.5), fixed now, before any silver geometry is seen. Region and page labels are
  still written as diagnostic columns, but they switch no rules. Test: the same random sample and 90% stop rule,
  with volume class = page majority. In sample 14/14, with a wide margin (≤ 0.19 against ≥ 0.91). This drops P2's
  per-region switching: truly mixed volumes, such as the English newspaper pages in 7461–7463, get one mode. It
  trusts no metadata. Caveat: under a majority criterion, density would also have passed after the fact
  (239/239, 502/548), so density cannot be re-scored on this criterion as if it had been registered in advance.
  (c) Volume modes from the EAD series (the old UA-018 (b)).
  (d) Close phase 10 as a negative result (the old UA-018 (c)).
  Recommended: (b). It is the cleanest separation found, it fixes the known gap (3771–3773, 3821–3823) from the
  data, not from series titles, and per-region switching can come back later if a better region signal turns up.
- **Default if unanswered:** nothing runs; phase 10 stays stopped after 15p.
- **Blocks:** steps 15q–15t (phase 11 is independent).
- **Response (given in chat, 2026-10-23):** b

- **Resolved 2026-10-23:** (b) applied. Phase 10 re-planned the same day: `PLAN.md` decisions P2 (inventory is the
  unit), P3 (the `hrel` rule), P5, P7, P10 (stop rule on the 60-volume sample), P11 (1.10.94 fallback); new briefs
  `step-15q-volume-mode-classifier.md`, `step-15r-volume-mode-build.md`, `step-15s-v4-build-and-run-kit.md`; the
  old 15q–15t briefs are marked void.

### UA-020 Stage PageXML of a 60-volume random silver sample for the 15q test (2026-10-23, from the 15q re-plan)
- **Action:** **Run** in any shell, with `SRC` set to the drive folder that holds the
  `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` files (the agent never touches the drive). Note the `g+rw`: the agent
  account must be able to read the files.
  ```
  SRC=/path/to/drive/folder      # <- edit
  DST=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml
  for i in 3126 3138 3180 3192 3199 3217 3222 3233 3235 3238 3254 3256 3265 3303 3320 3368 3394 3401 3428 3455 \
           3456 3459 3487 3492 3525 3546 3572 3579 3590 3630 3769 3794 3795 3796 3811 3841 3847 3849 3853 3857 \
           12100 12102 12109 12114 12116 12120 12121 12131 12135 12140 12162 12170 12180 12182 12187 12194 12197 \
           12220 12228 12232; do
    cp "$SRC/NL-HaNA_1.01.02_${i}-pagexml.tar.gz" "$DST/"
  done
  chmod g+rw "$DST"/NL-HaNA_1.01.02_*-pagexml.tar.gz
  ```
  The sample (`reports/data/phase-10-hand-print-per-region/planning-15q/silver_sample.tsv`, seed 20261023) is
  stratified by EAD series, excluding the dev volumes: 10 each of *gedrukte resoluties*, *nouvelles* and
  *plakkaten*, 10 *eerste serie* and 20 *tweede serie* net-resoluties. Agreement is weighted by stratum size. It is
  drawn at random, not for difficulty (unlike UA-017), so it can carry the stop rule. Estimated size about 2 GB
  (15 MB per million chars, from the 12 UA-017 tars); free on `/Users/Shared`: 155 GB.
- **Options:** (a) stage all 60; (b) stage nothing (only if UA-019 is (c) or (d)). Recommended: (a). It serves both
  UA-019 (a) and (b), so it can run before you decide.
- **Default if unanswered:** 15q has no silver sample. It validates on the dev volumes and the 12 UA-017 volumes
  only and cannot apply the stop rule.
- **Blocks:** 15q's stop-rule test (only under UA-019 (a) or (b)).
- **Response (given in chat, 2026-10-23):** inventories copied.
- **Resolved 2026-10-23:** all 60 tars are present, but with mode `-rw--w----` (no group read), so the agent cannot
  open them. The read permission is UA-021. 15q's brief uses the 60 volumes for its stop-rule part C.

### UA-021 Give the agent read access to the 60 UA-020 tars (2026-10-23, from the 15q re-plan)
- **Action:** **Run** in any shell:
  ```
  chmod g+r /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_*-pagexml.tar.gz
  ```
  All 60 UA-020 tars are `-rw--w----` (group write, no group read). The UA-020 command ended with
  `chmod g+rw`; perhaps the `cp` loop's shell stopped before it, or the source files' mode was kept.
  Check: `ls -l /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/ | awk '{print $1}' | sort | uniq -c`
  should show only `-rw-rw----`.
- **Options:** (a) run it. Recommended: (a).
- **Default if unanswered:** 15q runs parts A, B and D, then stops before part C (the stop rule), so 15r cannot start.
- **Blocks:** 15q part C, and through it 15r and 15s.
- **Response:** a, done
- **Resolved 2026-10-23:** all 78 tars in `shared-data/NL-HaNA_1.01.02/pagexml/` are `-rw-rw----` and open for
  the agent (`tar tzf` checked). 15q part C runs.

### UA-022 How the hand/print work continues after two failed tests (2026-10-23, from the phase-10 review)
- **Action:** **Decide** which option the next planning session takes; write the letter under **Response**.
  Phase 10 is closed as a negative result: density (15p, F10-15p.1) and the `hrel` line-geometry rule (15q,
  F10-15q.1: weighted agreement 0.768 hand / 0.893 print against 0.90) both failed their pre-registered tests, and
  `hrel` labels the printed 1.10.94 volumes `hand` (F10-15q.4). The review found why (F10-R.1): the ratio tracks a
  scribe's line spacing and the segmentation model's box convention. In an exploratory, post hoc look, absolute
  line-box height separates all 76 labelled volumes (print 65–86 px, hand 89.5–173 px; F10-R.3), with a thin margin
  and a known resolution caveat. The 60-volume sample is spent (F10-R.5). Report:
  `reports/phase-10-hand-print-per-region.md` (*Summary*, *Review*).
- **Options:**
  (a) a new phase: a pre-registered box-height rule (fit on the 14 dev volumes, rule and stop rule fixed before any
  new data), tested once on a **new** random 60-volume silver sample (you stage about 2 GB of tars again, via a
  staging UA); 1.10.94 gets a fixed `print` mode (P11). If it passes, the unrun 15r/15s briefs are reused with the new
  feature;
  (b) take the mode from the EAD series (*gedrukte resoluties*, *nouvelles*, *plakkaten* → `print`, every other
  volume `hand`), recorded as `method: fixed` with the EAD as source. Fast; it reverses P2 ("no metadata decides a
  mode"), relies on series titles ("gedrukt" also matches the handwritten *Geschreven indices* 3865–3868, F10-P.4),
  and misses printed inserts such as the English newspapers in 7461–7463, 8611;
  (c) close the hand/print work: the full SG run uses the v3 per-archive rules, and the printed 1.01.02 volumes stay
  a documented known gap (as in the v2/v3 pilots).
  Recommended: (a). It is cheap (the 15q code and caches are reusable), it keeps the decision in the PageXML, and a
  negative result closes the question with (b) or (c) as the fallback.
- **Default if unanswered:** nothing runs; the v3 pilots stay as published; phase 11 (independent) can go ahead.
- **Blocks:** the full SG text-layer run and any v4 layer.
- **Response:** a
- **Resolved 2026-10-23:** a new phase (12) tests a pre-registered line-box-height rule once on a fresh random
  silver sample; planned by Opus from the phase-10 hand-off. Phase 10 stays closed as a negative result.

### UA-023 Stage 110 PageXML tars for the phase-12 box-height test (2026-10-23, from phase-12 planning)
- **Action:** **Run** in `/Users/marijnkoolen/Code/Huygens/archival-structures`, with `SRC` set to the drive folder
  that holds the `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` files (the agent never touches the drive). `install -m 0664`
  copies each tar group-readable (UA-020's copies ended up without group read, UA-021):
  ```
  SRC=/path/to/drive/folder      # <- edit
  DST=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml
  for i in $(tail -n +2 reports/data/phase-12-hand-print-box-height/planning/test_sample.tsv | cut -f1); do
    f="$SRC/NL-HaNA_1.01.02_${i}-pagexml.tar.gz"
    if [ -f "$f" ]; then install -m 0664 "$f" "$DST/"; else echo "missing $i"; fi
  done
  ls -l "$DST" | awk 'NR>1{print $1}' | sort | uniq -c
  ```
  The list is the fresh test sample (`test_sample.tsv`, seed 20261212): 60 silver volumes in the phase-10 strata, 20
  random non-silver volumes, and 5 ordered reserves per stratum (30), which 15u uses only for a missing sample tar.
  None of them has been seen. About 2.1 GB for the 80, 2.9 GB with reserves (15 MB per million chars); 151 GB free
  on `/Users/Shared`. The last line should show only `-rw-rw-r--` (or `-rw-rw----`) modes.
- **Options:** (a) stage all 110; (b) stage none (the phase stops before 15u). Recommended: (a).
- **Default if unanswered:** 15u cannot start; phase 12 waits.
- **Blocks:** 15u, and through it 15v/15w and the full SG run.
- **Response:** a
- **Resolved 2026-10-25:** all 110 tars staged in `shared-data/NL-HaNA_1.01.02/pagexml/` (mode `-rw-rw-r--`,
  2.96 GB); each one exists, is readable and opens as a tar, so no reserve replaces a missing tar. 15u is unblocked.

### UA-027 Reading of the REPUBLIC crosswalk bar for printed 1.01.02 volumes (2026-10-25, from step 15v)
- **Action:** Decide whether "same paragraph" in the 15v 18/20 crosswalk bar means unit = paragraph (strict, 2/20)
  or matched line within the paragraph (lenient, 20/20).
- **Response:** none (withdrawn before the user answered).
- **Withdrawn 2026-10-25 by the 15v review (Opus):** moot. The bar meant the line-level reading (20/20), but the
  matches cover only 204 of 40,750 REPUBLIC line ids (0.5%; F12-15v.5, F12-15v.6), so the crosswalk carries no
  information for 1.01.02 on either reading. 15w passes no `--republic-dir` for 1.01.02 (brief updated).

### UA-026 Unit rules for the printed 1.01.02 volumes (2026-10-25, from step 15v)
- **Action:** Decide which unit rules the v4 presets use for a 1.01.02 volume whose mode is `print` (the print rules
  cut a paragraph after its drop-cap lines, F12-15v.4, but separate decisions and *Dat …* clauses, F12-R.3).
- **Options:** (a) keep the print rules for 15w and the run kit, document the cut; (b) a follow-up step for a
  drop-cap-aware print unit rule with its own hand-read bar; (c) hand unit logic via a new preset field.
  Recommended: (a) now and (b) as the follow-up.
- **Response:** (a) now and (b) as the follow-up.
- **Resolved 2026-10-25 (Opus):** decision P15 in `plan/phase-12-hand-print-box-height/PLAN.md`. The 15w brief keeps
  the print unit rules and documents the drop-cap cut as a known issue in `docs/text_layer.md`; new step 15x
  (drop-cap-aware print unit rule, own hand-read bar, rebuild of the print-mode 1.01.02 volumes) is added to the
  phase-12 step table, its brief to be written by Opus after 15w.

### UA-028 Handwritten index volumes built as `print` in a full v4 run (2026-10-25, from the phase-12 review)
- **Action:** Decide how the full run treats handwritten index volumes whose small hand falls below the 87.75 px cut
  (3463 "1740 index", 86.5 px, built as `print`; about 9 of 251 index-like volumes expected on the `print` side, F12-R.5).
- **Options:** (a) after the full run, list every `print` volume outside the printed EAD series, read them, pin the
  handwritten ones to `hand` and rebuild those; (b) before the run, pin every volume whose EAD title contains "index";
  (c) accept and document. Recommended: (a).
- **Response:** a
- **Resolved 2026-10-25 (Opus):** decision P21 in `plan/phase-12-hand-print-box-height/PLAN.md` (also in its review
  hand-off and `plan/CONTEXT.md` §4). The list-read-pin step is planned after the full run (UA-024); the build keeps
  the box-height rule as its only mode source.

### UA-024 Run the full v4 text layer build on the external drive (2026-10-25, from step 15w)
- **Action:** **Decide**, then **Run** (in the AS root, on the machine that has the drive). Only two placeholders:
  `<1.01.02-dir>` is the drive directory holding all 9,122 `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` (hand and print
  mixed; each volume's mode comes from its own PageXML), and `<drive>` the mount point for the output dirs. The 1.10.94
  inputs are local: the PageXML zips `NL-HaNA_1.10.94_455.zip`/`_456.zip` in shared-data, and the REPUBLIC
  `paragraphs-455.tsv.gz`/`paragraphs-456.tsv.gz` in LotA's data dir (the same paths 15w/15wr/15x built from). No
  `--republic-dir` for 1.01.02. *(Paths made explicit 2026-10-26, at the user's question.)*
  ```
  .venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=<1.01.02-dir> --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir <drive>/layer
  .venv/bin/python scripts/run_text_layer_v4_full.py --limit 1 --src 1.01.02=<1.01.02-dir> --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir <drive>/layer_smoke
  .venv/bin/python scripts/run_text_layer_v4_full.py --src 1.01.02=<1.01.02-dir> --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir <drive>/layer
  ```
  (the `--out-dir`s must exist; rerun the third command to resume).
- **Evidence:** the box-height rule was 60/60 on the test volumes in the five silver series (F12-15u.1); the diagnostic
  volumes test hand recall only (F12-15u.6); about 5 of 783 silver volumes are expected wrong (F12-R.2). 15w read the 51
  volumes within 10 px of the cut (147 scans): 50 are right, 1 is wrong (3463, handwritten, built as `print`); no
  handwritten slip was found in a printed volume (160 minority scans). The audit passes all 3,289 non-share
  invariants on the 198 volumes of `15w-a`; the 75 lower-case merge shares below 0.95 are reported only. The print unit
  rules still cut a printed paragraph after its drop-cap lines (UA-026 (a), step 15x planned), and two-column pages
  of the nouvelles can interleave columns in one unit. Prediction for about 400 GB of XML: output about 42 GiB; 67,000
  worker-seconds, so 2.3 h (8 workers, lower bound) to 7 h (3 x); the dev build ran at 1.4 x the lower bound, so expect
  about 3.5 h with 8 workers. The USB drive may be slower.
  **Updated prediction after step 15wr (2026-10-26):** with the split gate the print rate falls from 4.173e-7 to 2.402e-7
  worker-s per XML byte; at 10% print the 1.01.02 rate is 1.50e-7 (was 1.67e-7), so about 60,000 worker-seconds (2.1 h with
  8 workers as a lower bound, about 3.1 h at 1.4 x); output about 0.107 per XML byte (about 40 GiB).
  **Review note (15wr review, 2026-10-26):** the "interleaved columns" of the *nouvelles* above were the wrong splits;
  15wr fixed them (accepted, F12-15wr.1–3). The recommendation stays (c): wait for 15x and its review.
  **Review note (15x review, 2026-10-26):** 15x is done and accepted (bars A 59/60, B 0.9927, C 19/20 better; re-run
  reproduces). The drop-cap rule changes only the units stage, so the cost prediction above stands. No step blocks the
  run now. **Recommended now: (a)**, after the v0.15.0 release, so that the layer is built from released code. If you
  answer UA-029 with (b), that follow-up step comes first; with (a) or no answer the commands above build what is
  reviewed. After the run, the P21 step lists, reads and pins the handwritten `print` volumes (UA-028).
- **Review note (phase review, 2026-10-25):** the phase review found that the print rules also cut about a million
  ordinary lines in two in the single-column print series (*nouvelles*, *plakkaten*; F12-R.4). A full run now would
  carry that into some 140 volumes. Step 15wr fixes it with a split gate and re-measures the cost, and step 15x follows
  it. The recommendation is therefore (c), after 15wr and 15x. The handwritten index volumes on the `print` side are
  UA-028.
- **Options:** (a) all 9,122 + 1.10.94; (b) a subset file (`--inventories-file 1.01.02=F`, e.g. the volumes LotA aligns);
  (c) later, after steps 15wr and 15x. Recommended: (c).
- **Default if unanswered:** no run.
- **Blocks:** the full v4 layer (and LotA/NLP use of v4 beyond the 198 dev volumes).
- **Response:**
- **Resolved 2026-10-26 (Opus):** the user said in the chat that they ran the commands. The output is on the
  external drive, which sessions do not access, so the run is not verified here. Next: the P21 pin step (UA-028 (a)),
  which needs the full layer staged locally.

### UA-025 Publish the 198-volume v4 dev layer as a pilot (2026-10-25, from step 15w)
- **Action:** **Decide**; for (a) **Run** in the AS root (check the shared-data `README.md` rules first; the agent never writes there):
  ```
  cp -R data/text_layer_v2_dev/15w-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4 /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
  cp -R data/text_layer_v2_dev/15w-a/1.10.94/NL-HaNA_1.10.94_text_layer_v4 /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/text_layer_v4
  ```
  and add the two rows to the shared-data `README.md` (`coverage: partial`, 196 + 2 volumes, 2.7 GiB).
- **Options:** (a) publish now, so LotA's printed 1.01.02 volumes (3771-3773, 3821-3823) get the print rules; (b) wait
  for the full run or step 15x. Note for (a): volume 3463 (handwritten) is in the layer as `print`.
- **Review note (phase review, 2026-10-25):** `15w-a` has wrongly split lines in its 53 *nouvelles*, *plakkaten* and
  *Processtukken* volumes (F12-R.4), fixed in step 15wr. Recommended: (b). If you want a pilot sooner, publish
  15wr's `15wr-a` instead of `15w-a`; the same commands work with `15wr-a` in place of `15w-a`.
  **Review note (15x review, 2026-10-26):** the reviewed v4 dev layer is now `15x-a` (split gate and drop-cap rule); a
  pilot, if any, should be `15x-a`: the same commands with `15x-a` in place of `15w-a`. 3463 is still `print` in it (the
  P21 pin comes after the full run). Recommended: (b) if the full run (UA-024) is coming soon, since it replaces the
  pilot; (a) with `15x-a` if LotA needs the printed 1.01.02 volumes before then.
- **Default if unanswered:** nothing published.
- **Blocks:** LotA's use of 3771-3823 as print volumes in v4.
- **Response:**
- **Resolved 2026-10-26 (Opus):** the user said in the chat that they ran the commands. The copy is `15w-a` (manifests
  identical to `data/text_layer_v2_dev/15w-a/`, sizes equal), not the reviewed `15x-a`. Catalogue rows and a changelog
  line were added to the shared-data `README.md`, with the `15w-a` defects listed. Replacing it with `15x-a` is UA-030.

### UA-029 Drop-cap rule in the single-column print series? (2026-10-26, from step 15x)
- **Action:** **Decide.** Step 15x's pre-registered diagnostic read 10 suppressed cuts per series: *nouvelles* 7/10 `right`
  (3 wrong: one unreadable, a dateline after a signature, a closing formula), *plakkaten* 6/10 (4 of the 4 wrong are
  unreadable HTR fragments or table junk, counted `wrong`). The brief's trigger (fewer than 7 of 10 in a series) fired for
  *plakkaten*. The rule dropped 34,385 cuts in the 25 *nouvelles*, 18,572 in the 27 *plakkaten* and 62 in 9486
  (`step-15x/counts_per_series.txt`, `verdicts.tsv`, `diag_nouv_plak.txt`). Today `sg_1.01.02_v4` applies the rule to every
  print-mode volume (`15x-a`).
- **Options:** (a) keep: the rule stays for all print-mode 1.01.02 volumes; (b) restrict it to the two-column volumes
  (`split_gate.split` true in `mode/<inv>.json`: the 31 *gedrukte resoluties* and 3463), as a small follow-up step that
  gates the override and rebuilds only the 53 gated volumes' units; (c) drop the rule for the whole series
  (not recommended: bars A-C passed on the *gedrukte resoluties*). Recommended: (b), because the evidence of precision
  exists only for the *gedrukte resoluties* (59/60) and the two single-column series are not validated.
- **Default if unanswered:** (a); `15x-a` stays the v4 dev reference.
- **Blocks:** the units of the 53 single-column volumes in the full run (UA-024) and in a pilot (UA-025); nothing else.
- **Review note (15x review, 2026-10-26):** the trigger fired on input quality, not on rule errors: all 4 *plakkaten*
  "wrong" were unreadable fragments. I read 30 more dropped cuts per series (seed 20261031, fixed before drawing;
  `review/reads_15x_extra_20261031.txt`; the pre-registered 6/10 and 7/10 stay as they are). *Plakkaten*: 21 right,
  1 wrong, 8 unreadable input. *Nouvelles*: 23 right, 4 wrong, 3 unreadable. On readable cuts, both reads together:
  *plakkaten* 27/28 (0.96), *nouvelles* 30/36 (0.83). The *nouvelles* errors are letter parts beside an indent (closing
  formula "Waar mede, &c.", signature "Getee- / kent" after the dateline, "Art. 1."); the join can then glue words across
  them (12118: "Getee|Waar meede, &c.", which is the 33rd reported audit failure). Merging unreadable fragments changes
  nothing that could be read. So the rule helps in both series: in the *nouvelles* it removes about five wrong cuts
  (lines beside a dropped initial, as in the resolutions) for each wrong merge. What each option costs, extrapolated
  from the 40 reads per series: (a) about 26,000 right merges and 5,000 wrong ones in the *nouvelles*, about 12,500 and 500
  in the *plakkaten*; (b) gives up all of those right merges to avoid the wrong ones; (c) also gives up the validated
  gain in the *gedrukte resoluties*. A fourth option, (d), is a later step that keeps the rule but exempts letter parts
  (closing formula, dateline, signature, `Art.` patterns) with its own read bar. No layout measure separates the
  *nouvelles* from the *plakkaten* (both single-column), so a rule for one series only would need a per-inventory list.
  **Recommended now: (a)**, with (d) only if a sibling needs the letter structure of the *nouvelles*. (a) is also the
  default, so UA-024 can run without waiting for this answer.
- **Response:** a
- **Resolved 2026-10-26 (Opus):** decision P22 in `plan/phase-12-hand-print-box-height/PLAN.md`. The rule stays for every
  print-mode 1.01.02 volume; `sg_1.01.02_v4` and `15x-a` are unchanged; no follow-up step. Option (d) is kept only as a
  possible later step.

### UA-030 Replace the published v4 pilot (`15w-a`) with `15x-a` (2026-10-26, from the UA-025 follow-up session)
- **Action:** **Decide**; for (a) **Run** in the AS root (the agent cannot do it: the copied `text_layer_v4/` dirs are
  `drwxr-xr-x`, without group write):
  ```
  rm -rf /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
  cp -R data/text_layer_v2_dev/15x-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4 /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
  chmod -R g+w /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4 /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/text_layer_v4
  ```
  1.10.94 needs no replacement: its data tables are identical in `15x-a` (only run logs and `stats.json` differ).
- **Evidence:** the copy made for UA-025 is `15w-a` (manifests identical to `data/text_layer_v2_dev/15w-a`, sizes
  equal). `15w-a` has about 1.06 million wrongly split lines in 53 single-column print volumes (F12-R.4) and the
  drop-cap unit cut; `15x-a` is the reviewed build with both fixed. The shared-data README row lists these defects.
  No project links `text_layer_v4/` yet, so replacing it in place breaks no consumer (the protocol's "new vN+1, never an
  in-place rebuild" rule protects consumers).
- **Options:** (a) replace 1.01.02 with `15x-a` now; (b) keep `15w-a` with the defects documented until the full run
  replaces the pilot. Recommended: (a), so that LotA's printed volumes (3771-3773, 3821-3823) are read with the fixed rules.
  After (a), a session updates the README row (build name, defects list, changelog line).
- **Default if unanswered:** `15w-a` stays published, with its defects listed in the shared-data README.
- **Blocks:** nothing directly; a consumer that links `text_layer_v4/` before (a) gets the wrong splits.
- **Response:** a, done.
- **Resolved 2026-10-26 (Opus):** checked that `shared-data/NL-HaNA_1.01.02/text_layer_v4/` equals
  `data/text_layer_v2_dev/15x-a/1.01.02/NL-HaNA_1.01.02_text_layer_v4` (`diff -rq` empty, sizes equal) and that both v4
  dirs are group-writable. The shared-data `README.md` row now describes `15x-a`, with a changelog line for the in-place
  replacement. Decision P23 in the phase-12 plan.

### UA-031 Start phase 11 while the P21 step waits for the full run's files? (2026-10-26, from the UA-030 follow-up session)
- **Action:** **Decide.**
- **Context:** phase 12's last step (P21 pins, as step 15y in phase 12) needs the full v4 run's `mode/` and
  `status.tsv` staged locally. Phase 11 (optional: NLP `text_extent` options, UA-014 (a); C10, C12, C18 docs) touches
  modules disjoint from phase 12 and can start now. No other phase is planned.
- **Options:** (a) plan phase 11 now (Opus planning session), with 15y run once the files are staged; (b) do 15y first
  and plan nothing else until it is reviewed; (c) a different next phase (name it, e.g. the phase-09 leftovers in
  `plan/CONTEXT.md` §4). Recommended: (a), because phase 11 is not blocked and NLP has waited for `text_extent` since
  2026-10-21.
- **Default if unanswered:** (b): no new phase; 15y is planned once the full run's files are staged.
- **Blocks:** planning of any phase after 12.
- **Response:** a
- **Resolved 2026-10-27 (Opus, phase-11 planning):** answered (a). Phase 11 is planned as `plan/phase-11-text-extent-and-nlp-ports/` (steps 18a–18d, decisions P1–P10) and implemented in the same session; phase-12 step 15y still waits for the full v4 run's `mode/` and `status.tsv`.

### UA-032 Install AS editable into NLP's venv, so NLP can switch to the phase-11 ports (2026-10-27, from phase-11 planning)
- **Action:** **Run**, in `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`:
  ```
  PYTHONNOUSERSITE=1 venv/bin/python -m pip install -e /Users/marijnkoolen/Code/Huygens/archival-structures
  ```
  then `chmod -R g+w venv/lib/python3.14/site-packages` (the agent account must be able to read and update it).
  NLP's venv holds a non-editable copy of AS **0.13.0** (no `direct_url.json`), so NLP can't see the phase-11 names
  (`text_extent` options, `segment_dp`, `textiling_depth_scores`, `separator_sheets`). NLP's own
  `docs/plan_model_alternatives/CONTEXT.md` L269–270 says AS is installed editable; that note is stale.
- **Options:** (a) editable install now (recommended: NLP's shims for C10/C12 and its `early_end_*` scripts can then
  use AS directly); (b) install the next release (`pip install <AS>@v0.16.0`) after you tag it; (c) leave NLP on 0.13.0.
- **Default if unanswered:** (c). NLP keeps its own copies; phase 11 is unaffected (its checks ran NLP's interpreter
  with `PYTHONPATH=<AS>`, without changing NLP's venv).
- **Blocks:** a later NLP shim step (NLP `unsupervised_segmentation.py` and `separator_sheets.py` → re-exports).
- **Response:** a, done.
- **Resolved 2026-10-27 (Opus, phase-11 follow-up):** done. NLP's venv imports AS 0.16.0 editable from the AS root (`PYTHONNOUSERSITE=1 venv/bin/python`; `separator_sheets.apply_rule` and `stream_analysis.segment_dp` import). The NLP shim step (C10/C12 re-exports, `early_end_*` scripts) can now be planned.

### UA-033 `make docs` fails: no Sphinx in `.venv` (2026-10-27, from phase-11 planning)
- **Action:** **Run**, in `/Users/marijnkoolen/Code/Huygens/archival-structures`:
  ```
  .venv/bin/python -m pip install sphinx furo myst-parser nbsphinx && chmod -R g+w .venv
  ```
  `docs/Makefile` prefers `.venv/bin/sphinx-build`; there is none, so it falls back to `/opt/homebrew`'s sphinx, which
  fails with `ModuleNotFoundError: No module named 'pygments'`. `docs/conf.py` also loads `nbsphinx` (dropped when missing, L89); check `extensions` for any other package
  the build needs (e.g. `myst-nb`) and add it to the command.
- **Options:** (a) install as above (recommended); (b) fix the Homebrew Python instead; (c) leave it.
- **Default if unanswered:** (c). The docs are not built; phase-11 step 18d's pages are checked as Markdown only.
- **Blocks:** nothing in phase 11; building the Sphinx site.
- **Response:** a, done.
- **Resolved 2026-10-27 (Opus, phase-11 follow-up):** done. `make docs` builds (`.venv/bin/sphinx-build`): "build succeeded, 54 warnings"; none from `findings.md` or `challenges/leakage_and_splits.md`. Most warnings are older: 33 header-level jumps, 10 pages outside a toctree, 8 missing anchors into `challenges/page_order_problems`.

### UA-034 Stage 35 PageXML tars for the P21 pin step (2026-10-27, from the planning of steps 15y/15z)
- **Action:** copy the 35 tars from the drive's 1.01.02 directory into `shared-data/NL-HaNA_1.01.02/pagexml/` (`cp -p`, then `chmod g+w`).
- **Options:** (a) stage now; (b) stage later; (c) skip P21 for these volumes. Recommended: (a).
- **Default if unanswered:** 15z does not start; only 3463 gets a pin (15y).
- **Blocks:** step 15z, and with it the pinned full layer and its publication (P23).
- **Response:** a, done.
- **Resolved 2026-10-27 (15z session):** done. The user staged the tars (223 files in the directory). Their mode 600 (kept by `cp -p`) blocked reading by the agent account until the user fixed group (`shared-projects`) and mode (group-readable; 0 unreadable). Consumed by step 15z.

### UA-036 Volume 4016 (1685 oktober): read 3 printed, 2 handwritten; pin it? (2026-10-27, from step 15z)
- **Action:** **Decide** (open the 5 scans through the check page of UA-035, inventory 4016). Step 15z's rule pins a volume only at 4 of 5
  opposite reads; 4016 has 2 handwritten and 3 printed reads, so it stays as built (`print`, box height 82.5 px, split gate off). The
  printed reads are copies of treaty and resolution texts (scans 0053, 0070, 0102); the handwritten ones are a hand-written extract and
  an advice text (0064, 0128). It is a genuinely mixed volume. This is the only mixed volume of the 37.
- **Options:** (a) leave as built (recommended: no rule applies); (b) pin to `hand`; (c) pin to `print` (no change, documents the choice).
  For (b): **Edit** `archival_structures/text_layer/presets.py`, `MODE_PINS_V4`, add `"4016": "hand",` after `"3726": "hand",`; add `4016` to
  `step-15z/pins_drive.txt` before running UA-035, and ask for a test update.
- **Default if unanswered:** (a).
- **Blocks:** nothing.
- **Response:** a
- **Resolved 2026-10-27 (Opus session after 15z):** (a), 4016 stays as built (`print`), with no pin. The user's image check (UA-035 part 1) confirms the 3/2 split. By the same check 2600 (3 handwritten / 2 printed) and 9264 (2 / 3) are mixed too. They are treated like 4016 and stay as built. Recorded in the report (§ Step 15z, *The user's check*; F12-15z.7) and in the phase `STATUS.md`.

### UA-035 Check the 16 mode pins, then rebuild them in the full v4 layer on the drive (2026-10-27, from step 15z)
- **Action:** two parts.
  1. **Check** (about 20 min): open `reports/data/phase-12-hand-print-box-height/step-15z/check_page.html` in a browser. It is blind
     (no predicted mode). For each of the 37 volumes read the 5 scans and press Handwritten / Printed / Mixed / Unsure; press Export and
     save `check_verdicts.json` in `reports/data/phase-12-hand-print-box-height/step-15z/`. The agent pins (step 15z, rule: at least 4 of 5
     read scans of the other class): 14 volumes to `hand` (the 11 *tweede serie* index volumes 3433, 3436, 3439, 3442, 3445, 3463, 3466, 3469,
     3472, 3475, 3489, plus 3726, 4792, 12297) and 2 to `print` (12163, 12165). The page does not say which.
  2. **Run** in the AS root (`/Users/marijnkoolen/Code/Huygens/archival-structures`), on the machine with the drive, from this working tree
     (the pins are in `presets.py`, unreleased). `<1.01.02-dir>` is the drive directory with the `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`
     files (the `--src 1.01.02=` of UA-024), `<drive>/layer` the full run's `--out-dir`:
     ```
     .venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild 1.01.02=reports/data/phase-12-hand-print-box-height/step-15z/pins_drive.txt --src 1.01.02=<1.01.02-dir> --out-dir <drive>/layer --check-only
     .venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild 1.01.02=reports/data/phase-12-hand-print-box-height/step-15z/pins_drive.txt --src 1.01.02=<1.01.02-dir> --out-dir <drive>/layer
     .venv/bin/python scripts/stage_text_layer_v4_full.py --from <drive>/layer --to data/text_layer_v4_full --inventories 3463 12163 12165 12297 3433 3436 3439 3442 3445 3466 3469 3472 3475 3489 3726 4792 1 9302 11102 11103 11191 11265 --max-inventories 60
     ```
     `pins_drive.txt` has 16 inventories (the 15 new pins plus 3463, which the full run built `print`). The `--check-only` line must say
     "rebuild: 16 of 9122 inventories". The rebuild redoes those 16 volumes in place (forced extract, then every stage over the
     layer) and writes a new `audit.json`. Predicted time from the 15y model: the 16 volumes (about 442 MiB XML) take 1-2 min; the pooled
     word-break statistics over 9,122 inventories 5-20 min; the audit over 9,124 inventories about 45 min (0.3 s per inventory, measured
     on the 37 local volumes); staging a few minutes. Total about 1.2 h (1-1.5 h). The last command overwrites the staged copy of
     the 16 volumes (and of `audit.json`, `run_v4.log`, `wbc/`) in `data/text_layer_v4_full`.
- **Options:** (a) check, then run (recommended); (b) run without the check; (c) do not rebuild.
- **Default if unanswered:** (c). The drive layer keeps the box-height modes (278 `print` volumes, of which 14 are handwritten by the
  15z reads, and 2 printed placcaten built `hand`); the pilot is not replaced.
- **Blocks:** the pinned full layer, the Opus check of the staged rebuild, and the publication of the full layer (P23).
- **Response:** Part 1: done. About my notes: I've only used "Printed" and "Handwritten" when all fives images were the same. When they were not, I used mixed and in the notes field indicated which image is which script type. In some "Printed" volumes I've indicated Gothic (as that might have lower OCR quality). Feedback about this kind of judgements: using crops, this is extemely easy and fast. If more is needed, it's even faster to have keyboard shortcuts and judge per crop, with keyboard shortcuts for Next and Previous as well. 
- **Resolved 2026-10-27 (Opus check after the rebuild):** part 1 was recorded earlier (F12-15z.7). Part 2: the user ran the rebuild and the staging. The staged copy is checked against `15z-b`. The 16 volumes equal it in all 288 data files and in their audit counts. The log shows 0 failures, "rebuild: 16 of 9122" and 266 `print` / 8,856 `hand`. The audit has 161,644 pass / 2,756 fail (+1: 4792's reported merge share). Report § *After 15z: check of the staged drive rebuild* (F12-15z.8-11). Publication: UA-037.

### UA-037 Publish the pinned v4 layer: the full layer does not fit on the internal disk (2026-10-27, from the Opus check after UA-035)
- **Action:** **Decide**. For (a), first **Run** in the AS root (`/Users/marijnkoolen/Code/Huygens/archival-structures`), on the
  machine with the drive. `<drive>/layer` is the full run's `--out-dir`, as in UA-035. This takes a few minutes and copies about 100 MB:
  ```
  .venv/bin/python scripts/stage_text_layer_v4_full.py --from <drive>/layer --to data/text_layer_v4_full --inventories 11281 3107 3127 3166 751 12114 12144 12170 3769 3848 --max-inventories 60
  ```
  These are 10 pilot volumes (5 `hand`, 5 `print`, seed 20261027). The next session checks that they equal the published pilot,
  then plans the patch.
- **Evidence:** the rebuild is right (F12-15z.8). P23 planned for the pinned full run to replace the pilot in
  `shared-data/NL-HaNA_1.01.02/text_layer_v4/`. But the full 1.01.02 layer is 39,426 MiB, and the internal disk has 1.3 GiB free
  (100% used on 2026-10-27). The pilot (`15x-a`, 196 volumes, 2.5 GiB) has one of the 16 pins, 3463, still built `print`. Its corpus
  word-break set and per-inventory pins match the full layer's. No sibling reads `text_layer_v4` yet, so a change in place breaks
  nothing.
  **Update 2026-10-27:** the 10 pilot volumes are staged. 192 of their 194 files equal the pilot. The other 2 are 11281's
  `extract_stats` `source` and `scans` `member`: provenance only, because the pilot read 11281 from an unpacked directory and the
  drive run from the tar. So the pilot equals the full layer on its volumes, except 3463. The internal disk now has 87 GiB free,
  so (b) fits: about 48 GiB would be left after the copy.
- **Options:**
  (a) patch the pilot: after the check above, a Sonnet step writes a publish script. The script replaces 3463 in the pilot with the
  full layer's 3463 (from `data/text_layer_v4_full`, without the stale split files of F12-15z.10) and writes the manifest, README
  row and changelog line. You run it, since only you write to `shared-data`. Coverage stays `partial`, with the size unchanged.
  (b) publish the full layer: free at least 45 GiB on the internal disk, then copy the 39 GiB from the drive over the pilot.
  Coverage becomes `complete`.
  (c) keep the pilot as it is. The README row says 3463 is `print` in the pilot and that the pinned full layer exists only on the
  drive, where agent sessions cannot read it.
  Recommended: (a) now, and (b) later if a sibling needs volumes outside the 196.
- **Default if unanswered:** (c), without the README note. The pilot keeps 3463 as `print`, and the pinned full layer stays on the drive only.
- **Blocks:** publishing the pins (P23). Nothing else; no sibling reads v4 yet.
- **Response:** a (given in the chat, 2026-10-27)
- **Resolved 2026-10-27 (Opus, after the drive-rebuild check):** (a). No publish script is needed: `data/text_layer_v2_dev/15y-b` is the patched pilot. It is the published `15x-a` with 3463 rebuilt in place by `--rebuild` (step 15y). Checked: 3,772 files equal the pilot; the 30 that differ are 3463's files and the layer summaries (stats, `wbc/` with only 3463's row changed, default `['-', '=', '„']` unchanged; status, manifest, progress); 3463's split files are gone. 3463 equals the drive's in 18 of 18 data files. Audit 3,592 pass, 33 reported (as `15x-a`). The copy is UA-038.

### UA-038 Replace the published v4 pilot (`15x-a`) with `15y-b`, which has 3463 pinned `hand` (2026-10-27, from UA-037 (a))
- **Action:** two parts.
  1. **Run** in the AS root (`/Users/marijnkoolen/Code/Huygens/archival-structures`); about 2.5 GiB, a minute or two:
     ```
     rm -rf /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
     cp -R data/text_layer_v2_dev/15y-b/1.01.02/NL-HaNA_1.01.02_text_layer_v4 /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
     chmod -R g+w /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v4
     ```
     1.10.94 is not touched: `15y-b`'s 1.10.94 is byte-equal to the pilot's.
  2. **Edit** `/Users/Shared/claude_projects/data_scopes/shared-data/README.md`, *Catalogue*: replace the whole row that starts with
     `` | `NL-HaNA_1.01.02/text_layer_v4/` `` with
     ```
     | `NL-HaNA_1.01.02/text_layer_v4/` | SG text layer v4 (pilot, `coverage: partial`): 196 inventories, each built as `hand` (112) or `print` (84) from its own line-box heights (AS phase 12), or pinned after reading (3463 `hand`, AS P21); `mode/<inv>.json` records the mode and method. Same layout as v3 plus `mode/`, `split_stats/`, `split_model/`. No REPUBLIC crosswalk for 1.01.02. AS dev build `15y-b` = `15x-a` (line splitting only in the two-column print volumes, step 15wr; drop-cap-aware print units, step 15x) with 3463 rebuilt in place (step 15y). Its 196 volumes equal those of the pinned full SG run, which is kept on the producer's external drive (AS F12-15z.8). **Known issues:** pinned `hand` index volumes such as 3463 are under-segmented (AS F12-15z.3); in the *nouvelles* the drop-cap rule also merges some letter parts (closing formula, dateline, signature), about 1 wrong for every 5 right merges (AS F12-R.13, kept by AS UA-029 (a)); 0.7% of decision/*Dat* unit starts lost (F12-15x.3). v2 and v3 stay until no row lists a consumer | producer AS `text_layer`, preset `sg_1.01.02_v4`; built 2026-10-26 at AS git head 7f09d07 plus uncommitted phase-12 changes, 3463 rebuilt 2026-10-27 at 4b9f5c7 (0.16.0) plus uncommitted steps 15y/15z; pagexml-tools 0.9.0; sources and stage history in `manifest.json`. Audit: 3,592 invariants pass; 33 lower-case merge shares below 0.95 (reported, 0 judged failures) | 2026-10-27 (2.6 GB; replaced in place twice, see changelog) | none yet |
     ```
     and add at the end of *Changelog*:
     ```
     - 2026-10-27: `NL-HaNA_1.01.02/text_layer_v4/` replaced in place by AS build `15y-b` (AS UA-037 (a)), before any project linked it: 3463 (a handwritten index volume) is now `hand` (AS P21 pin). The other 195 volumes are byte-equal, and all 196 equal the pinned full SG run. 1.10.94 unchanged.
     ```
- **Default if unanswered:** the pilot keeps 3463 as `print`, and the README still says the full run will replace the pilot.
- **Blocks:** nothing else; no sibling reads v4 yet.
- **Response:** done (given in the chat, 2026-10-27)
- **Resolved 2026-10-27 (Opus, same session):** checked. `diff -rq` between `15y-b`'s 1.01.02 layer and `shared-data/NL-HaNA_1.01.02/text_layer_v4` is empty (3,802 files each, all group-writable); `mode/3463.json` is `hand`/`pinned`, with no `split_model/3463.json`. The README row names `15y-b`, and the changelog line of 2026-10-27 is present. P23 is closed for the pilot.

### UA-040 Does v5 ship the hand column split? Bar B1 failed (2026-10-28, from phase-13 step 19b)
- **Action:** Decide. Evidence: `reports/phase-13-register-index-volumes.md` section "Step 19b" and
  `reports/data/phase-13-register-index-volumes/step-19b/{b1_verdicts.tsv,b1_reads.txt}`. B1 (pre-registered): right/(right+wrong)
  >= 0.90 and <= 2 wrong among the 20 not-index-like cases. Result: 29 right / 9 wrong = 0.763, 4 wrong among the not-index-like
  cases, 2 unclear/unreadable. The split cuts the hand interleave from 2,612 to 980 units (0.65% to 0.24%) but turns 2,760 regions
  into 16,134 pieces (+10,637 main_text units) and 9 of 40 read cases are wrong (fragments, column order).
- **Options:** (a) v5 ships without the split (`hand_column_split=None`, nothing else changes) -- recommended: the bar failed and
  the brief forbids tuning on the same reads; (b) ship the split as it is anyway; (c) one new pre-registered step with a
  changed rule (span lines only if truly merged/wide, fixed order of pieces) and a fresh read sample and bar, then decide.
- **Default if unanswered:** (a); steps 19c-19e do not use the split, but they do not wait for the answer except on this point.
- **Blocks:** steps 19c, 19d and 19e (as the brief says) until answered; the `hand_column_split` code stays in the library, off.
- **Reviewer note (2026-10-28, phase-13 review of 19a–19d):** recommend (a) for 19b's split as built, and (c) in a
  narrower form: step 19f (`plan/phase-13-register-index-volumes/step-19f-column-order-v2.md`). Re-running 19b's
  checks reproduced every number. The B1 failure comes from the pre-registered rule, not the code: lines classed by
  extent, and pieces ordered by the page-level column grouping (report § Review, F13-R.7). 19f keeps each two-column
  region as **one** region and re-orders its lines, left column then right, with lines classed by where they start.
  That adds no units (19b added 10,637) and cannot misorder pieces. It gets a fresh read bar (same thresholds, seed
  20261125). Choose (b) never: 9 of 38 read splits were wrong. If you choose 19f, answer UA-042 too.
- **Response:** (a) for 19b's split as built, plus the reviewer's narrowed (c), step 19f (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (Opus, orchestrating session):** `hand_column_split` as built stays off in v5; step 19f (column order v2, bar B2) launched. Outcome in the phase-13 `STATUS.md` and report § Step 19f.

### UA-041 Does v5 ship the index entry rule? Entry bar failed (2026-10-28, from phase-13 step 19d)
- **Action:** Decide. Evidence: `reports/phase-13-register-index-volumes.md` section "Step 19d" and
  `reports/data/phase-13-register-index-volumes/step-19d/{eval_test.json,error_verdicts.tsv,error_reads.txt}`. Bar (pre-registered,
  20 test sides, once): P >= 0.90 and R >= 0.85 and F1 > v4 hand F1. Chosen on dev: E5 (indent + short_prev, the print rule on
  flagged sides). Test: P 0.8683, R 0.7092, F1 0.7807 (v4 hand: P 0.9741, R 0.4502, F1 0.6158): F1 passes, P and R fail. 26 of the
  40 read errors come from two columns interleaved in one layout region (the 19b problem), 11 from index entries typed
  `marginalia`/`other`.
- **Options:** (a) v5 without the entry rule (`index_entry=None`; hand units unchanged) -- recommended: the bar failed and the brief
  forbids tuning; (b) ship E5 on flagged sides anyway (starts recall 0.45 -> 0.71, precision 0.97 -> 0.87; +65% main_text units on
  flagged sides); (c) one new pre-registered step that first fixes the column interleave (UA-040 option c) and re-tests the entry rule.
- **Default if unanswered:** (a).
- **Blocks:** step 19e (what the v5 preset sets); the code stays in the library, off.
- **Reviewer note (2026-10-28, phase-13 review of 19a–19d):** recommend (a), and not (c) as worded. The text above
  overstates interleaving: 14 of the 26 "interleave" errors sit in one-column layout regions. Over all 200 test
  errors, 58 are in column-mixed regions, 38 in lines typed `index`/`marginalia` (no main unit) and 104 are cue
  errors (report § Review, F13-R.1–R.5). Even a perfect column fix caps E5's recall at 0.775 (< 0.85). E5 was also
  the weakest choice: E3–E5 were within 0.017 F1 on the 10 dev sides, and on test E3/E4 reach F1 0.859 vs E5 0.781.
  What would likely pass is step 19g (`plan/phase-13-register-index-volumes/step-19g-index-entry-v2.md`): cues
  outdent + date + page reference, no cut before a line of only page numbers, `index`-typed lines as mains. It is
  chosen on the 30 spent sides and tested once on 20 fresh sides read first, with the same bar. On the spent sides
  (post hoc, not a bar) that design gives P 0.896 / R 0.864 on the v4 layout and P 0.940 / R 0.882 with 19f's column
  order. (b) would ship a rule whose precision falls from 0.97 to 0.87.
- **Response:** (a), plus the reviewer's step 19g (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (Opus, orchestrating session):** the 19d rule (E5) stays off in v5; step 19g (entry rule v2, fresh gold, same bar) runs after 19f. Outcome in the phase-13 `STATUS.md` and report § Step 19g.

### UA-042 Scope of step 19e: run the follow-ups 19f/19g first? (2026-10-28, from the phase-13 review of 19a–19d)
- **Action:** Decide. Evidence: `reports/phase-13-register-index-volumes.md` § Review and
  `plan/phase-13-register-index-volumes/REVIEW.md`. What v5 contains depends on the bars that pass. Today only the
  index-side detector passed (D1, D2), and on its own it would add one table (`layout/pages`) while leaving every hand
  unit as in v4. That is not worth a full v5 rebuild of the 8,856 hand volumes.
- **Options:** (a) run 19f (column order v2, read bar B2), then 19g (entry rule v2, fresh gold, same entry bar; on
  19f's clones if B2 passes and you accept it, else on the v4 layout), then re-plan 19e (Opus) to ship what passed.
  About two Sonnet sessions plus your read of the 19g result. (b) run only 19g on the v4 layout (skip the column
  work). (c) skip both: 19e ships v5 = v4 + the detector table. (d) close phase 13 without a v5: the code stays in the
  library, off. Recommended: (a); (b) if you want the shorter path (its post-hoc precision margin is thin: 0.896 on
  the spent sides).
- **Default if unanswered:** nothing runs; 19e stays blocked.
- **Blocks:** steps 19f, 19g and 19e.
- **Response:** (a) (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (Opus, orchestrating session):** 19f runs, then 19g (on 19f's clones if B2 passes, else on the v4 layout), then 19e is re-planned (Opus) to ship what passed. UA-039 stays open; it gates 19e only.

### UA-039 Blind check of 30 index-side labels (2026-10-28, from phase-13 step 19a)
- **Action:** Run (from the AS root): `open reports/data/phase-13-register-index-volumes/step-19a/check_page.html`.
  (Rebuilt 2026-10-28 at your request: each item now has an NA image crop of the quoted lines and one of the whole
  page side; verdicts already entered are kept.)
  Judge the 30 page sides (Index / Not index / Unsure) against the scans, press Export and save the
  file as `reports/data/phase-13-register-index-volumes/step-19a/check_verdicts.json`.
- **Options:** (a) do the check (about 30 min); (b) skip it: step 19e then states that the gold is
  unchecked. Recommended: (a).
- **Default if unanswered:** step 19e does not start.
- **Blocks:** step 19e only (19b–19d run on the agent labels).
- **Reviewer note (2026-10-28, phase-13 review of 19a–19d):** recommend (a). Bar D2 passed exactly on its
  threshold (54/60), so one label you overturn can flip it. The check covers side labels only; the entry labels
  (S/C) have no independent check, and the follow-up 19g reads fresh ones.
- **Reviewer note (2026-10-28, review of 19f/19g):** still recommend (a); the weight is slightly lower. 19g read 22
  detector-flagged sides it had not seen before and found 20 index pages, 1 running text and 1 unreadable, which supports
  the detector's precision independently of 19a's labels. The check is still the only human look at the gold, and D2
  still sits exactly on its bar.
- **Response:** (a) done: 30 verdicts exported as `step-19a/check_verdicts (1).json` (browser name; copied unchanged to `check_verdicts.json` by the orchestrating session) (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (step 19e):** (a). `check_verdicts.json` against `side_verdicts.tsv`: 27 of 27 judged items agree (agreement 1.000, no disagreement; 3 `unsure`, read_ids 17, 23, 54, left out; the agent said index on all three). Gate passed; D2 gold stands, with the three unsure sides unconfirmed.

### UA-043 Does v5 ship the column reorder (bar B2 failed narrowly)? (2026-10-28, from phase-13 step 19f)
- **Action:** Decide. Bar B2 (pre-registered): right/(right+wrong) >= 0.90 over 40 read regions and <= 2 wrong among the 20 `other`.
  Result: 35 right, 4 wrong, 1 unclear = 35/39 = 0.897 (fail by one case); other wrong = 2 (pass). If my one `unclear` (case 23, a
  names list in two columns whose first right-column line belongs before the left list) were `right` it would be 36/40 = 0.900. Details:
  `reports/phase-13-register-index-volumes.md` section "Step 19f", `reports/data/phase-13-register-index-volumes/step-19f/b2_verdicts.tsv`.
- **Options:** (a) treat B2 as failed as the rule says: v5 does not ship the reorder (hand interleave stays 2,612, or 980 with 19b's
  split, which also failed); 19g then runs on the v4-layout clones `19c-a`/`19c-z`. (b) Ship the reorder anyway
  (`SG_HAND_COLUMN_ORDER_V5`, interleave 843 of 399,429 units, 0.21%, unit count -0.07%) and run 19g on `19f-a`/`19f-z`; this overrides a
  pre-registered bar, so the report would say so. (c) Fresh B2 read on 40 new regions (the 3,066 reordered regions allow it) before
  deciding. Recommended: (a) now, and (c) only if the reorder matters for the entry rule in 19g.
- **Default if unanswered:** (a): 19g starts on `19c-a`/`19c-z`; the reorder is not in v5.
- **Blocks:** which clones 19g uses ("19g base" in `STATUS.md`) and what 19e puts in the v5 hand preset.
- **Reviewer note (2026-10-28, review of 19f/19g):** recommend **(a)**. (1) The fail is fair: case 23 is not `right` under the
  rubric (the text opens at the top of the right column), and two `right` calls (22, 07) are lenient, so a fresh read of the
  same rule (c) would be a coin toss around 0.90 and re-testing until it passes is not a test. (2) The entry rule no longer
  needs it: 19g ran on the v4 layout, and on the same fresh sides X4 scores F1 0.9242 on the reorder layout against 0.9244
  (diagnostic, F13-R.11). So this UA now decides only the reorder itself, which un-interleaves about 1,770 hand units
  (0.44%) and misorders roughly 1 region in 10 of the 3,066 it touches. (b) is defensible if you value the 1,770 units
  more than the pre-registered bar; the report then says the bar was overridden. A better rule (row-pairing guard, split x
  from the overlap band, F13-R.10) is a later phase with a new bar. 19g has run, so the "19g base" part of this UA is moot.
- **Response:** (a) (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (step 19e):** (a). Branch A: v5 has no column reorder (`SG_1_01_02_V5` without `hand_column_split`).

### UA-044 Parquet reader for the optional silver check in 19e (2026-10-28, from the review of 19f/19g)
- **Action:** Decide. Step 19e can score the index detector against `shared-data/NL-HaNA_1.01.02/republic_segmentation_v1/`
  (resolution pages are not index pages; 27 of the 125 local hand volumes are in that layer). Reading it needs `pyarrow`,
  which AS's `.venv` does not have.
- **Options:** (a) the 19e session installs it: `cd /Users/marijnkoolen/Code/Huygens/archival-structures &&
  .venv/bin/python -m pip install pyarrow && chmod -R g+w .venv` (adds a dependency to the dev venv only, not to
  `pyproject.toml`); (b) no install: 19e reads the three columns it needs (`scan_id`, `page_id`, `unit_type`) with LotA's
  interpreter (`PYTHONNOUSERSITE=1 /Users/Shared/claude_projects/data_scopes/language-of-the-archive/venv/bin/python`, which
  has pyarrow 26.0.0), read-only, into a TSV under `reports/data/phase-13-register-index-volumes/step-19e/`; (c) skip the
  silver check. Recommended: (a), since AS will need a Parquet reader anyway once it links this layer for the print
  crosswalk (`docs/from-republic-segmentation.md`).
- **Default if unanswered:** (c): 19e runs without the silver check and the report says so.
- **Blocks:** only the optional silver check in 19e (step 7); nothing else.
- **Response:** (a), pyarrow already installed in `.venv` (26.0.0) (given in the chat, 2026-10-28)
- **Resolved 2026-10-28 (step 19e):** (a). pyarrow 26.0.0 was already in `.venv`; nothing installed. Silver check run (`step-19e/silver.py`).

### UA-007 `PageXMLPage.stats` still raises before pagexml-tools 0.9.1 (2026-10-20, from phase-07 planning)
- **Action:** **Edit** `/Users/marijnkoolen/Code/Huygens/pagexml-tools/pagexml/model/pagexml_document_model.py`,
  `PageXMLPage.stats` (L1176–1196 at commit `16a51d0`). Replace
  ```python
              "text_regions": len(self.num_text_regions),
              "table_regions": len(self.num_table_regions)
  ```
  with
  ```python
              "text_regions": self.num_text_regions,
              "table_regions": self.num_table_regions
  ```
  and, in the `if self.extra:` branch, replace `sum(tr for tr in self.extra if isinstance(tr,
  PageXMLTextRegion))` with `sum(1 for tr in self.extra if isinstance(tr, PageXMLTextRegion))`
  (the same for `PageXMLTableRegion`). Then release 0.9.1 and fill in **Response**.
  Checked at `16a51d0` (2026-10-20): `stats` raises `TypeError: object of type 'int' has no
  len()` on a page with columns, with `text_regions` or with `extra`, because `num_text_regions`
  is already an int (L392–393). AS's print splitter calls `page.stats['lines']`
  (`text_layer/line_split.py:1059`), so the print extract would crash under this copy. The other
  six fixes of UA-005 are in place (F07-P.9).
- **Options:** (a) fix and release 0.9.1; (b) fix it later.
  Recommended: (a).
- **Default if unanswered:** AS stays on the 0.9.0 wheel with its workarounds; step 15i skips
  the pin.
- **Blocks:** only the pin in step 15i (step 0). Steps 15fq–15hj run on 0.9.0 either way.
- **Response:** release 0.9.1 is done.
- **Resolved 2026-10-29 (USER_ACTIONS session):** pagexml-tools 0.9.1 is on PyPI (`pip index versions`, 2026-10-29); `.venv` still has 0.9.0. Not pinned yet: the v5 full run (UA-045) and its rebuilds run on 0.9.0, so the pin (install, `pyproject.toml` `>=0.9.1,<1.0.0`, `MIN_PAGEXML_TOOLS`, drop the workarounds F07-P.7, F07-15fp.11, F07-P.9, regression build; the procedure of step 15m Part 0) is a step of the next phase, after the v5 full layer is reviewed (UA-047). Recorded in `plan/CONTEXT.md`.

### UA-045 Run the full v5 text layer build of 1.01.02 on the external drive (2026-10-28, from step 19e)
- **Action:** Decide, then Run (in the AS root, on the machine that has the drive; `<1.01.02-dir>` = the drive directory
  with all 9,122 `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`, `<drive>` = the output mount point; both `<drive>/layer_v5` and
  `<drive>/layer_v5_inputs` must exist; no `--republic-dir`; rerun the last command to resume):
  ```
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --check-only --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --limit 1 --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5_smoke
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --src 1.01.02=<1.01.02-dir> --work-dir <drive>/layer_v5_inputs --out-dir <drive>/layer_v5
  ```
- **Options:** (a) run it; (b) not now. Recommended: (a) if you want v5 for all 9,124 volumes.
- **Evidence:** dev build `19e-a` (231 volumes, 4 workers): 60 min wall, 0 failed rows, audit 0 judged failures. Stage seconds
  v5/v4 on the same 231 volumes: layout 0.94, types 0.96, units 0.99, join 0.90, wbc 0.88 (extract is the same code; its
  1.51 here is machine load), so the time is the v4 full run's: 11.5 h wall (10.5 h of work plus a 1 h crash gap),
  predicted 3.5 to 7 h for the stages by `--check-only` (it prints the range); the USB drive may be slower. Output about 39 GiB.
- **Default if unanswered:** nothing runs; v4 stays the only full layer.
- **Blocks:** a full v5 layer (index entries in the hand volumes outside the 125 local ones).
- **Reviewer note (2026-10-28, final phase-13 review):** still recommend (a); the run does not need to wait. (1) The run
  script's final audit was the v4 audit and did not check the v5 invariants; the review fixed that (it now runs
  `step-19e/audit.py`, same checks plus the v5 ones, no extra time), so read `n_fail_judged` in `<drive>/layer_v5/audit.json`
  as usual. (2) Create `<drive>/layer_v5_smoke` too before the second command: the script refuses an `--out-dir` that does
  not exist. (3) Expect detector false positives outside the index and register volumes: in the dev build about 1 side in
  1,000 there (rosters, accounts, letters), re-cut at entry cues, no text lost (F13-R.13); a guard is a later rule, not part
  of this run. The commands were checked to parse (`--check-only` on the local sources: 231 inventories).
- **Update (2026-10-29, after the first full run stopped at units):** the layout stage failed on 7 volumes with
  non-numeric scan numbers and interleaved the parts of 16 multi-part hand volumes; fixed in `text_layer/layout.py`
  (phase-13 `PROGRESS.md`, *Post-phase fix*). Resume with the two commands below (AS root; `<drive>` as above, i.e. the
  directory holding `text_layer_v5/` and `text_layer_v5_inputs/`): the first keeps the 16 old `layout/pages` tables for a
  before/after check, the second is the run command plus `--rebuild` of the 23 volumes (forced extract of those 23, then
  every stage; the other 9,099 keep their extract/layout/types).
  ```
  P=<drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5/layout/pages; for i in $(grep -v '^#' reports/data/phase-13-register-index-volumes/step-19e/rebuild_scan_order.txt); do [ -f "$P/$i.tsv.gz" ] && cp -p "$P/$i.tsv.gz" data/text_layer_v5_full/pages_before_fix/; done; chmod -R g+rwX data/text_layer_v5_full/pages_before_fix; ls data/text_layer_v5_full/pages_before_fix | wc -l
  .venv/bin/python scripts/run_text_layer_v4_full.py --version v5 --only 1.01.02 --rebuild 1.01.02=reports/data/phase-13-register-index-volumes/step-19e/rebuild_scan_order.txt --src 1.01.02=<1.01.02-dir> --work-dir <drive>/text_layer_v5_inputs --out-dir <drive>/text_layer_v5
  ```
- **Response:** I ran both commands. Tell me what to copy to the local drive and the command for it.
- **Resolved 2026-10-29 (USER_ACTIONS session):** run and `--rebuild` of the 23 volumes done by the user; the 16 old `layout/pages` tables are in `data/text_layer_v5_full/pages_before_fix/` (16 files). Staging of the review files is UA-047.

### UA-046 Publish the v5 pilot in `shared-data/` (2026-10-28, from step 19e)
- **Action:** Decide. `data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/` (231 volumes: the 125 hand volumes
  with index sides and entry units, 106 print volumes equal to v4) would become `shared-data/NL-HaNA_1.01.02/text_layer_v5/`.
- **Options:** (a) publish it (a later session copies it, edits the shared-data README and the changelog line, as with
  UA-038); (b) wait for the full run (UA-045); (c) not at all. Recommended: (a), so LotA can read index entries now.
- **Default if unanswered:** nothing is published.
- **Blocks:** LotA's use of v5.
- **Reviewer note (2026-10-28, final phase-13 review):** still recommend (a). Every 19e number reproduces and the print
  volumes equal v4. Known false positives in the pilot: 54 flagged sides that are not index pages in 18 of the 125 hand
  volumes (most in 8706 accounts, 4831 rosters, 3199/3315 accounts), 0.18% of the hand units, re-cut but no text lost;
  the README line should name them with the other known limits (`docs/text_layer.md`, *Text layer v5*).
- **Response:** a
- **Resolved 2026-10-29 (USER_ACTIONS session):** (a). `19e-a` copied unchanged to `shared-data/NL-HaNA_1.01.02/text_layer_v5/` (2.7 GB, `diff -rq` identical, `chmod -R g+w`); catalogue row (with the 54 false-positive sides of F13-R.13 and the other known limits) and changelog line in the shared-data README; LotA `docs/from-archival-structures.md` §10; AS `docs/shared_library/changelog.md` and `docs/text_layer.md` (*Text layer v5*, *Published pilot*). None of the 23 volumes of the 2026-10-29 layout fix is in the pilot.

### UA-047 Stage the review files of the v5 full run (2026-10-29, from UA-045)
- **Action:** Run, in the AS root (`/Users/Shared/claude_projects/data_scopes/archival-structures`), on the machine with
  the drive; `<drive>` as in UA-045 (the directory holding `text_layer_v5/`). The first command is a dry run: it prints the
  failing audit invariants, what it would copy and the total size, and writes nothing. The second copies.
  ```
  .venv/bin/python scripts/stage_text_layer_v4_full.py --from <drive>/text_layer_v5 --to data/text_layer_v5_full --inventories $(grep -v '^#' reports/data/phase-13-register-index-volumes/step-19e/rebuild_scan_order.txt) --max-inventories 60 --dry-run
  .venv/bin/python scripts/stage_text_layer_v4_full.py --from <drive>/text_layer_v5 --to data/text_layer_v5_full --inventories $(grep -v '^#' reports/data/phase-13-register-index-volumes/step-19e/rebuild_scan_order.txt) --max-inventories 60
  ```
  It copies `audit.json`, `run_v4.log`, the layer's `status.tsv`, `manifest.json`, `progress.log`, `*/params.json`,
  `mode/`, `scans/`, `extract_stats/`, `split_model/`, `split_stats/`, `wbc/` (without `wbc/parts/`), and every
  per-inventory file of the 23 rebuilt volumes plus any volume named in a judged audit failure (at most 60). Expect about
  1 GB (the v4 full staging was 0.9 GB). It overwrites the older `status.tsv`, `manifest.json`, `progress.log` and
  `run_v4.log` already in `data/text_layer_v5_full/` and leaves `pages_before_fix/` alone. Checked 2026-10-29 with a dry
  run on the v5 dev build `19e-a`: it finds the `*_text_layer_v5` layer. The 16 tables for the before/after check are then in
  `data/text_layer_v5_full/1.01.02/NL-HaNA_1.01.02_text_layer_v5/layout/pages/`.
- **Options:** (a) run both commands; (b) not now. Recommended: (a).
- **Default if unanswered:** nothing is staged; the full v5 layer stays unreviewed on the drive.
- **Blocks:** the before/after error analysis of the layout fix (index flags of the 16 multi-part volumes, the 7 volumes
  that failed), the check of `n_fail_judged`, and any catalogue entry for the full v5 layer.
- **Response:**
- **Resolved 2026-10-29 (USER_ACTIONS session):** done by the user (given in the chat). 789 MB staged; all 9,122 volumes done, audit 0 judged failures; before/after check in phase-13 `PROGRESS.md` (*Full v5 run checked*): 1 flag lost, correctly, in 11086; the 7 failed volumes now build.

### UA-048 pagexml-tools 0.9.1 in the NLP and LotA venvs (2026-10-29, from phase-14 planning)
- **Action:** Decide, then Run. Step 20a raises AS's `MIN_PAGEXML_TOOLS` to (0, 9, 1). NLP re-exports AS's version check
  (`tests/test_voc_text_layer.py::test_pagexml_tools_version_is_at_least_080`), and both sibling venvs hold an editable
  install of your checkout `/Users/marijnkoolen/Code/Huygens/pagexml-tools` (branch `bug_fixes`, HEAD `1712888`), whose
  `pyproject.toml` still says `version = "0.9.0"`. So the siblings must report 0.9.1 before 20a finishes.
  - (a) Install the PyPI release in both (replaces the editable checkout):
    ```
    cd /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection && PYTHONNOUSERSITE=1 venv/bin/python -m pip install 'pagexml-tools==0.9.1' && chmod -R g+w venv/lib/python3.14/site-packages
    cd /Users/Shared/claude_projects/data_scopes/language-of-the-archive && PYTHONNOUSERSITE=1 venv/bin/python -m pip install 'pagexml-tools==0.9.1' && chmod -R g+w venv/lib/python3.14/site-packages
    ```
  - (b) Keep the editable checkout: **Edit** `/Users/marijnkoolen/Code/Huygens/pagexml-tools/pyproject.toml`, set
    `version = "0.9.1"` (only if the checkout holds the 0.9.1 code), then in each sibling root run
    `PYTHONNOUSERSITE=1 venv/bin/python -m pip install -e /Users/marijnkoolen/Code/Huygens/pagexml-tools` and the same `chmod`.
  - Check (either way), in each sibling root: `PYTHONNOUSERSITE=1 venv/bin/python -c "import importlib.metadata as m; print(m.version('pagexml-tools'))"` → `0.9.1`.
- **Options:** (a) PyPI release; (b) editable checkout bumped to 0.9.1. Recommended: (a), so all three projects run the
  same released code as AS.
- **Default if unanswered:** step 20a does parts 0–5 (AS pin, regression build, data link) and stops as `partial`; the
  NLP suite would fail its version test until this is done, so 20a does not finish and 20b does not start.
- **Blocks:** 20a part 6 (sibling suites) and step 20b. Not 20c, 20d, 20e.
- **Response:** (a): installed 0.9.1 in both venvs (given in the chat, 2026-10-29)
- **Resolved 2026-10-29 (phase-14 orchestration session):** checked: NLP and LotA `venv` both import pagexml-tools 0.9.1 from `site-packages` (PyPI release, not the checkout). 20a part 6 and 20b are unblocked.

### UA-049 pagexml-tools 0.9.1 parses nested regions: extract output changes (2026-10-29, from step 20a)
- **Action:** Decide. With 0.9.1, `text_layer.extract` parses nested TextRegions (fixture 9999A_0003) where 0.9.0 raised
  `TypeError` and skipped the scan; 7 tests in `tests/test_text_layer_extract.py` fail. Step 20a stopped before the
  regression build.
- **Options:** (a) keep the 0.9.1 pin; let the session run the regression build (`19e-a` vs `20a-pin`, 43-70 min) to see
  whether any real v5 table changes, then update the 7 tests to the new behaviour; if tables change, rebuild later as v6.
  (b) keep the pin but make extract skip scans with nested regions (preserves 0.9.0 output; needs a code change).
  (c) revert the pin to 0.9.0 (NLP and LotA venvs already hold 0.9.1). Recommended: (a).
- **Default if unanswered:** the pin edits stay in the working tree; the AS suite shows 7 failures; 20b and later wait.
- **Blocks:** step 20a part 4, hence 20b (needs 20a part 6, which is done) is not blocked in practice; the phase review is.
- **Evidence (orchestrator, 2026-10-29):** under 0.9.0 a scan with nested regions lands in `extract_stats` `parse_errors`.
  In the full v5 run (all 9,122 volumes, `data/text_layer_v5_full/…/extract_stats/`) and in `19e-a` (231) the only parse
  errors are 6 METS `doc.xml` files (not pages); **0 scans failed on nested regions**. So the 0.9.1 change touches the
  synthetic fixture only, not any existing v5 table. The regression build still checks the rest of 0.9.1.
- **Response:** (a) (given in the chat, 2026-10-29)
- **Resolved 2026-10-29 (phase-14 orchestration session):** (a). Step 20a resumes: regression build `19e-a` vs `20a-pin`, then the 7 extract tests and the docs move to the 0.9.1 behaviour (nested regions parse); stop again if a real table differs.

### UA-051 Bar F missed by one row: 33/35 = 0.943 against 0.95 (2026-10-29, from phase-14 step 20d)
- **Action:** Decide whether 20e proceeds with the frozen folio map. The two misses (3590 loc. 23: duplicated printed `23`,
  chain keeps the later scan; 3611 loc. 486: skipped printed numbers, 486 not typed `page_number`) are in
  `reports/phase-14-index-links.md` § Step 20d. Read rows alone: 27/28 = 0.964; dev 16/16.
- **Options:** (a) proceed with 20e and report bar F as missed by one row (map frozen, links carry the `confidence` field);
  (b) stop phase 14 here; (c) allow a rule change (e.g. tie rule for duplicated numbers, 3-digit number pattern, which also
  fixes 3712's mode), which makes the test bar no longer pre-registered. Recommended: (a).
- **Default if unanswered:** 20e does not run.
- **Blocks:** 20e.
- **Response:** (a) (given in the chat, 2026-10-29)
- **Resolved 2026-10-29 (phase-14 orchestration session):** (a). Bar F stays recorded as failed (negative result, F14-20d.2); 20e runs on the frozen map; a map v2 (tie rule for duplicated printed numbers, 3712's verso years) is a follow-up that needs fresh gold.

### UA-050 Confirm 20 index links on the scans (2026-10-29, from phase-14 step 20c)
- **Action:** Run (AS root): `open reports/data/phase-14-index-links/step-20c/check_page.html`. Answer the two
  questions per item (Yes / No / Unsure), then press Export and save the file as
  `reports/data/phase-14-index-links/step-20c/check_verdicts.json`.
  Updated 2026-10-29 (your request): each item now shows three image crops (Nationaal Archief IIIF, needs internet), each
  with the extracted text of those lines below it: the entry, the top of the target right-hand page with the folio
  number (question 1), and the lines where the agent found the matter (question 2; not shown when the agent judged it
  absent). Each crop has a `larger` link. Items and the storage key are unchanged, so answers already given are kept.
  Updated again 2026-10-29 (your request): two fold-outs per item show the full extracted text of the right-hand page of
  the first scan and the left-hand page of the second (question 2), with margin notes marked and the evidence lines
  highlighted.
- **Options:** (a) check (about 20 min with the crops); (b) skip: the links are published as unchecked. Recommended: (a).
- **Default if unanswered:** the phase review states that the gold is unchecked.
- **Blocks:** the publication decision for `index_links_v1` only (20d and 20e run on the agent gold).
- **Response:** verdicts exported to `check_verdicts.json` (given in the chat, 2026-10-29)
- **Resolved 2026-10-29:** all 20 items answered: q1 (folio) 18 Yes / 2 No, q2 (matter) 17 Yes / 2 No / 1 Unsure; all 11
  test items Yes/Yes. The 2 wrong links (dev volume 3471, locators 94 and 452) are entry-side errors, not folio errors.
  Recorded as F14-20c.8 (F14-20c.2 revised) in `reports/phase-14-index-links.md` § Step 20c "User check", and in
  `plan/phase-14-index-links/PROGRESS.md`. `entry_gold.tsv` is not edited. UA-053 (publication) is no longer waiting on it.

### UA-052 Run the index-links build on the full v5 layer (2026-10-29, from phase-14 step 20e; updated by the phase review)
- **Action:** Run, in the AS root, on the machine with the drive, **after step 20r** (its builder records a failing volume
  and goes on instead of stopping the run). `<drive>` = the directory holding `text_layer_v5/` (UA-045). Create the output
  directory, smoke-test, run in full (rerun the third command to resume), then copy back:
  `mkdir -p <drive>/index_links_v1`
  `.venv/bin/python scripts/build_index_links.py --layer <drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5 --out-dir <drive>/index_links_v1 --limit 3`
  `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label index-links-full --want 4 --min 1 --wait -- .venv/bin/python scripts/build_index_links.py --layer <drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5 --out-dir <drive>/index_links_v1 --workers '{workers}'`
  `mkdir -p data/index_links_full && cp <drive>/index_links_v1/NL-HaNA_1.01.02_index_links_v1/volumes.tsv <drive>/index_links_v1/NL-HaNA_1.01.02_index_links_v1/manifest.json data/index_links_full/ && chmod -R g+w data/index_links_full`
  Measured cost on the dev build: 19.4 s for 41 volumes single-process (0.47 s per front-index volume); the 125 hand
  volumes of the dev layer (41 with a front index) took 10.6 s with 3 workers.
- **Review decision (2026-10-29):** the variant stays **L2**, the default (chosen on dev before the test; bar L failed on
  precision, 61/66 = 0.924; bar E passed, 33/35). L3 is not adopted: its better test numbers come from a mistaken test
  run, so choosing it would be a choice on test (F14-R.2). Do not pass `--variant`.
- **Options:** (a) run as above with L2 after 20r; (b) skip until an index-links v2 exists. Recommended: (a): it costs
  minutes, the layer records its variant, and the counts (how many full-layer volumes have a front index) size the
  next phase.
- **Default if unanswered:** links exist for the 41 local volumes only.
- **Blocks:** links beyond the local volumes; the full-layer counts for planning an index-links v2.
- **Response:** done
- **Resolved 2026-10-29:** run done; `data/index_links_full/` holds `volumes.tsv` and `manifest.json`: 412 of 8,856
  inventories with a front index, none failed, 629,912 entries, 902,031 of 914,994 locators linked; the 41 dev volumes
  identical to the 20r dev build. F14-20e.9, F14-20e.10 and section *After the review* in
  `reports/phase-14-index-links.md`; `docs/index_links.md` *Full run*; phase STATUS/PROGRESS, `plan/CONTEXT.md`.

### UA-053 Publish `index_links_v1`? (2026-10-29, from the phase-14 review)
- **Action:** Decide. `index_links_v1` (P10) links front-index entries to folio scans; the dev build covers the 41 local
  volumes (`data/index_links_dev/20e-a/`), the full run is UA-052. Its bars: F 33/35 = 0.943 (failed, bar 0.95),
  L precision 0.924 (failed, bar 0.95) and recall 1.000, E 0.943 (passed, bar 0.90); about 0.87 of emitted links point to
  the right scan on the test rows (estimate). Known limits in `docs/index_links.md` (year-like anchors give `high` links
  in 6 of 41 volumes, F14-R.3; `extrapolated` links are mostly noise, F14-R.5). The user checked 20 gold links (UA-050, F14-20c.8): all 11 test links
  confirmed; the 2 wrong ones (dev) are entry-side errors.
- **Options:** (a) do not publish v1; publish a v2 after the next index-links phase meets its bars on fresh gold;
  (b) publish the full-run layer in `shared-data/NL-HaNA_1.01.02/index_links_v1/` (after UA-052, and UA-050 if you check
  it) as `coverage: partial`, with the failed bars and known limits in the catalogue row; (c) publish the 41-volume dev
  build now, the same way. Recommended: (a): two of three bars failed, the main defects are known and fixable, and no
  sibling project reads index links yet.
- **Default if unanswered:** nothing is published; the layer stays in `data/index_links_dev/` (and on the drive after UA-052).
- **Blocks:** publication only; nothing in AS or the siblings.
- **Response:** a
- **Resolved 2026-10-29:** (a). `index_links_v1` is not published; the builds stay in `data/index_links_dev/` and on the
  drive. A v2 follows an index-links v2 phase with bars on fresh gold. Recorded in the phase-14 report (*After the
  review*, *Follow-ups*), `docs/index_links.md`, `plan/PLAN.md`, the phase PLAN and STATUS, `plan/CONTEXT.md`.
