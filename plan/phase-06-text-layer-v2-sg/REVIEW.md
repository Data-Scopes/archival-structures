# Phase 06 review

> Written by the phase-review session (Opus). Review the code as written, not as described in
> `PROGRESS.md`: the gap between the two is the most useful thing a review finds.

- **Session:** Opus 5.5, 2026-10-18
- **Verdict:** **accept with follow-ups.** Step 15fr ([brief](step-15fr-review-fixes.md)) applies
  the user's UA-002 answer (factor 1.19) and two small documentation fixes. Run it before the
  release.

## Checklist

- [x] **Every step's acceptance criteria met, re-run by the reviewer.** The builds ran from one
      script into fresh directories `data/text_layer_v2_dev/06R-a/`, `06R-v1check/`, `06R-ab/`
      and `06R-15f/`. The 10 protected input files had the same sha256 and mtime before and after.
      Every logged number reproduces (table below), and every rebuilt table is **byte-identical**
      to the step's own output: hand 56 of 56 files (15d-a), print 12 of 12 including
      `split_model/` and `split_stats/` (15e-a).
- [x] **AS tests:** `.venv/bin/python -m pytest -q -p no:cacheprovider --continue-on-collection-errors`
      gives 1010 passed, 0 failed, 0 collection errors (64.98 s). **Siblings** (nothing edited):
      NLP `venv/bin/python -m pytest -q -p no:cacheprovider tests` gives 56 passed. NLP imports AS
      from the working tree (`archival_structures.__file__` is in AS); its metadata says 0.9.0.
      LotA: see *Acceptance, re-run*. Running NLP's pytest from its root without `tests` collects
      `output/as-phase-04-originals/tests/` and stops with 2 import-file-mismatch errors. That is
      an NLP layout issue, not a phase-06 one.
- [x] **Error analysis read individual cases and is in the phase report.** Each step lists what
      it read, how it was sampled, categories with counts and verbatim examples. The reviewer's
      own reading (below) agrees with it.
- [x] **Phase report final; findings in `reports/index.md`; `make reports` clean.** See *Updates
      applied*.
- [~] **Changelog: the v2 API, presets, `io` additions and the `nltk` dep** are there. Missing:
      the 15e names `SplitModel`, `SplitHook`, `fit_split_model_from_rows`, `read_line_rows`,
      `is_candidate_row`, `training_docs_from_lines`; and the new `format_version` key in every
      `manifest.json`, `voc_v1` included (`build.py:160`). Added in 15fr.
- [~] **Ported code carries its attribution and its ported test.**
      `tests/test_text_layer_line_split.py::TestLineSplitter::test_split_word_candidates` is
      ported, and the module docstring (`line_split.py:1-20`) names source, commit `47dfc010`,
      licence and every change. Missing: the MIT copyright notice (republic-project has no
      `LICENSE` file; its authors are in `pyproject.toml`). Added in 15fr.
- [x] **No archive-specific constants in core.** Archive ids, source patterns, scan-id prefixes,
      `split_region_tags` and `word_split*` are in `presets.py`. The remaining constants in core
      are method defaults taken from the REPUBLIC notebook: the factor (1.22, to become a
      keyword with default 1.19), the 0.35–0.6 column band, `min_uni_freq=100`,
      `column_sep_width=20`, `min_chars=20`. They are documented in `docs/text_layer.md`.
      `legacy_region_text` hard-codes `{-, „}`. That is the legacy TSV's join by definition, and
      the function's name says so.
- [x] **v1 157/157 byte-identical; no write under `shared-data/` or a sibling.**
      `06R-v1check/check.txt`: `TOTAL 157 / 157`. `find shared-data -newermt '2026-10-17 08:45'`
      finds only Finder `.DS_Store` files. Every run of the phase wrote to
      `data/text_layer_v2_dev/` (gitignored).
- [x] **Leakage: 15f's evaluation split is by scan, and the vocabulary excludes the test scans.**
      `evaluate.py:25-36`: `sha1(scan_id) % 10` (0 test, 1 dev, 2–9 train), and
      `build_vocabulary(by["train"])` (L112). The selection rule (within 0.02 of the best dev
      exact rate, then the most conservative) is the brief's (step brief L95-98), fixed before
      the run. The reviewer's re-run of `test` gives JSON equal to the logged `dev` and `test`
      blocks for both archives. 15e has no split: the scorer and the parameters are fitted
      transductively on the inventory's own non-candidate lines, with no labels. Its
      acceptance is an exact A/B and read precision. So there is nothing to leak.
- [x] **`Brief defects` answered:** see *Brief quality*.

## Acceptance, re-run

| Step | Criterion | Logged | Re-run | Match |
|---|---|---|---|---|
| 15d | 1 legacy regression, 14 inventories | doc_id sets equal 14/14, text 151,246/151,246 | `legacy_check.py 06R-a`: identical output to `15d-a/legacy_check.txt` (`diff` empty), `LEGACY_OK True` | yes |
| 15d | 2 uniqueness | 0 duplicates, 16 inventories | 0 / 0 in all 16. Now also on the **split** print layer (halves `_L`/`_R` are unique) | yes |
| 15d | 3 coverage | scans = XML members 16/16; scan_stats equal for 14 | same | yes |
| 15d | 4 v1 | `TOTAL 157 / 157` | `TOTAL 157 / 157` (30 s) | yes |
| 15d | 5 pilot extract | 16/16 exit 0; 21.2 + 28.0 + 41.4 s | exit 0; 21 + 28 s (hand). Print now includes the split: 249 s | yes |
| 15e | 1 A/B vs the original | 1,421/1,421 and 1,435/1,435 identical, 0 raising | `ab_republic.py` into `06R-ab/`: 455 1,421/1,421, 456 1,435/1,435 identical, 0 raising (169 s, 177 s); JSON equal to `15e-ab/` apart from `seconds` | yes |
| 15e | 2 split counts | 455: 11,470 / 2,398 + 9,064 / 8; 456: 64,096 / 12,670 + 51,393 / 36; 0 page errors | identical (`split_stats` byte-identical); 2 and 7 degenerate splits, 0 alignment failures | yes |
| 15e | 3 conservation | 0 exceptions | `ea.py check`: 0 of 11,462 and 0 of 64,063 | yes |
| 15e | 4 geometry | 0 and 4 outside the box; L.x < R.x 100% | same; in the 4 (all 456), both halves reach 27–86 px below the original box's bottom | yes |
| 15e | 4 column | 171,925 and 174,219 non-empty lines with 1/2 | 174,222 lines have a column in 456: 174,219 with text plus 3 whitespace-only | yes |
| 15e | 7 v1, suite | 157/157; 1002 passed | 157/157; 1010 (15f's tests included) | yes |
| 15f | 1 evaluation | print test exact 0.310, P 0.984, R 0.273; hand 0.376 / 0.9995 / 0.288 | `evaluate.py {print,hand} test` → JSON equal to `15f-a/eval/*_test.json` (dev and test blocks; line counts per split equal) | yes |
| 15f | 3 conservation, idempotence | 0 violations | covered by `test_wordfix_stage_idempotent` and `test_repair_line_only_inserts_spaces` (both pass); production run not repeated | partly (tests only) |
| 15f | 4 hand-read precision | print 0.62, hand 0.16 | re-judged the 50 hand repairs: same verdicts within ±1 (below) | yes |

LotA suite (`PYTHONNOUSERSITE=1 venv/bin/python -m pytest -q -p no:cacheprovider tests`): 318
passed, 9 failed. All 9 are `test_corpus_registry.py::test_cli_choices_are_config_derived[...]`
with `PathOutsideRootsError: .../shared-data/NL-HaNA_1.01.02/ead/source/NL-HaNA_1.01.02.xml is
not under any allowed root`. The roots in the error lack `../shared-data`, which LotA's
`config/archives.toml:26` does list, so the test probably builds its config from a fixture
without it. That is a LotA test/configuration issue (finding 10), not an AS
one: phase 06 changed nothing LotA imports.

## Findings

Most serious first. **Defect** means must fix (it goes into a review-fixes brief), **debt**
means recorded and deferred, and **observation** means no action.

| # | Kind | File:line | Finding | Action |
|---|---|---|---|---|
| 1 | defect (user decision) | `text_layer/line_split.py:1216` | The factor is a fixed 1.22. UA-002 was answered (b): use 1.19. The review read 25 of the 45 lines that 1.19 adds in 455, and about 23 are real merged lines (F06-R.3) | 15fr §1: keyword `merged_width_factor=1.19`; docs, test, changelog; re-run and read the new splits |
| 2 | debt | `text_layer/line_split.py:5`, `tests/test_text_layer_line_split.py:19` | The MIT port names the licence but not the copyright holders | 15fr §2 |
| 3 | debt | `docs/shared_library/changelog.md` (*Step 15e*, *Step 15d*) | Six public names of 15e are unlisted. `manifest.json` gains `format_version` for every preset (`build.py:160`), so "`voc_v1` output is byte-identical" holds for the tables, not the manifest | 15fr §4 |
| 4 | observation | `text_layer/line_split.py:1648-1662`, `extract_v2.py:127` | A split half's `text_raw` is a piece of an HTR line, not a PageXML `TextEquiv`. The merged original's text is not stored. `" ".join(L, R)` restores it, except in 87 split lines (1 in 455, 86 in 456) where a double space was collapsed. Documented in `docs/text_layer.md`, but it bends P7 | phase 07: the `doc_id` mapping and char offsets go through `split_from` (F06-R.4) |
| 5 | observation | `text_layer/build.py:285-336` (`stage_wordfix`) | Rewrites `lines/<inv>.lines.tsv.gz` in place, and its vocabulary depends on which inventories the run selects (`vocab_source` records it). That is safe while `word_split` is off. If it is ever turned on, a subset run gives different repairs than a full run, as with `wbc` (F05-R.5) | phase 07, if word repair returns |
| 6 | observation | `text_layer/word_split.py:17` | `KEEP_TRAIL` is unused | 15fr §3 |
| 7 | observation | `reports/data/phase-06-text-layer-v2-sg/step-15e/ab_republic.py:3` | The usage line said `python -I` while the docstring says `-I` can't be used | fixed by the review (one line) |
| 8 | observation | working tree | Not part of phase 06, but it will ship with the next release: the Python 3.14 classifier (`pyproject.toml`), CI and Read the Docs on 3.14, the `CLAUDE.md` line on `shared-data`. Also, `.venv-3.11/` is untracked and not ignored (`.gitignore:135` ignores `.venv` only) | user (see *For you* in the chat) |
| 9 | observation | republic-project `long_line_splitter.py` | The original's `page_split_columns` raises on every page under pagexml-tools ≥ 0.8.0 (F06-15e.6). Only AS's port is fixed | user (republic-project), if it still runs the splitter |
| 10 | observation | LotA `tests/test_corpus_registry.py` | 9 LotA tests fail with `PathOutsideRootsError` for the EAD under `shared-data/`. Unrelated to AS | LotA (its next session); reported in the chat |

The code is otherwise clean. Details checked:
- `extract_v2` drops `_deelopname` scans before the hook (`_is_dropped`) and keeps zero-line
  regions.
- The hook works on copies (`copy_line`) and writes back only after the pieces' joined text
  equals the original (`_write_back` aligns, or counts an alignment failure and leaves the
  region unchanged: 0 in both inventories). It restores `conf` and the word elements.
- Degenerate splits leave the line whole.
- `stage_wordfix` recomputes `text` from `text_raw`, so it is idempotent; the test pins this.

## Error analysis read by the reviewer

All samples were drawn from the existing layers. The scripts are in the review's scratchpad;
the commands are in the report's *Review* section.

- **Bigram splits** (seed 606, 8 per inventory, fresh): 11 of 16 correct. 5 are off at the
  boundary:
  - k=1 `... ten eynde lons, | que sadie Altesse` (`Batail-` above R);
  - k=1 `... 's Landts | gerechtige que` (`gerechtig-` + `heydt` below L);
  - k=2 `... uytsprake | van sijne Philip` (`Majesteyt.` below L);
  - k=3 `... sy sullen | daer toe af-. Ten sevenden` (`secteren` below L);
  - one inside a glued hyphen: `en redt-rende`.

  This agrees with 15e's 22 of 30 plus 6 off by 1–2. One case is off by 3, which 15e didn't
  see. Glued hyphens without a space (`redt-rende`, `Re-tie`) are a failure kind that neither
  step of the splitter can fix.
- **Hyphen splits** (seed 607, 6 per inventory): 12 of 12 correct. In every case L ends in `-`
  and the line below L in column 1 continues the word (`aenge- | sien`, `Artisle- | rye`).
- **Unsplit candidates** (seed 609, 6 per inventory): 455's are merged lines that failed
  (`Honden, gekomen uyt Engelandt, wedertekee-insereer.`, page-bottom lines). 456's are single
  lines with an indented wide box (`sonder betalinge van 's Landts gerechtigheydt,` x 2924,
  w 1425). This matches 15e's categories.
- **UA-002 margin, 455** (seed 608): factor 1.19 lowers the threshold to 1,072/1,070 px (left
  and right) and adds 18 + 27 = 45 candidates to the 11,470. Of 25 read, about 23 are merged
  lines with a short right part (`het welck haer Hoogh Mogende na dese Provincie te doen.`;
  French resolutions on 455_0222/0223). About 2 are plausibly single wide lines (`konnen
  genieten, vermits dienaengaende een Ex-`). With 15e's 19 of 20 for 456, this supports 1.19
  (F06-R.3).
- **15f hand repairs:** all 50 of `ea_hand.txt` re-read. Clearly correct: `Zeelandende`,
  `ingroote`, `suppltenin`, `Bergenop`, `enderhalven`, `Roermondestadt`, and arguably
  `vandenselven` and `delijcke`. That makes 6–8, in line with the logged 8. Most errors detach
  `-de`/`-en` or `op`/`voor`/`aen` from genuine words. The most frequent production repairs
  (`vandenselven`, `vanderselver`, `deswegens`, `naerdien`, 18–19 times each) are spellings that
  are written as one word in the period, so even the "correct" count is generous. The negative
  result stands (F06-R.5).
- **15d:** not re-sampled. The rebuilt tables are byte-identical, and the legacy check compares
  every row, so the 15d analysis rests on the same data. Its unread classes (sloped baselines,
  overhang > 100 px in 455) are stated as unread in the report.

## Brief quality

| Step | Brief defect (from `PROGRESS.md`) | Answer / change |
|---|---|---|
| 15d | `skip_resource_forks` is redundant (`_is_pagexml_member` already skips `._*`) | Kept as an explicit guard. Fine |
| 15d | The hint "no `-`/`„` at a region end" in `test_legacy_region_text` was wrong | The step tested the real behaviour. The planner should have run the hint on the library |
| 15d | `deelopname` scan ids don't parse with `ScanId` | Handled by the specified fallback. Phase 08 owns `ScanId` |
| 15d | Stale suite baseline (6 collection errors) | Template: take baselines from a run on the planning day (already in `STEP.md`: "expected test counts come from `pytest --collect-only`") |
| 15e | "The original imports cleanly" was true, but it fails at run time under pagexml-tools ≥ 0.8 | **Template change:** a port brief runs the original once under the current pins at planning time, not only imports it |
| 15e | Expected hand candidate rates near 0; measured 40–87% | **Template change:** a brief marks each expectation about data as measured (with the number) or unmeasured |
| 15e | `x` never occurs; `copy_line` drops `conf`; lines without text get no `column` | Properties of the original. Documented |
| 15e | (planner check before 15e) tag filter and median width wrong for 455/456 (F06-P.5) | Caught before the step ran, by a planner spot-check on 15d output. Keep doing this between dependent steps |
| 15f | No CLI handling for a stage without PageXML input | The step added it (`--pagexml-dir` optional for `wordfix`). Acceptable |
| 15f | No rule for judging ambiguous compounds | **Template change:** a hand-read precision bar comes with the judging rule for ambiguous cases |
| 15f | `log(0.5/total)` makes `max_parts=4` unreachable | A planner design error, recorded in phase 07's open questions (UA-003 answered (a), so it is not pursued) |

The three template changes are added to `plan/templates/STEP.md` (*Constraints* and *Error
analysis*).

## Updates applied

- [x] Phase report `reports/phase-06-text-layer-v2-sg.md`: *Summary*, *Review* and *Follow-ups*
      written; every finding is `confirmed` or `revised`; status set to *final*
- [x] `reports/index.md`: the phase's findings appended (with Edit), and its phase row added
- [x] `make reports` runs without warnings
- [x] Review-fixes brief written (`step-15fr-review-fixes.md`)
- [ ] `docs/shared_library/changelog.md` complete for this phase: after 15fr (§4)
- [x] phase `PLAN.md` *Hand-off* filled in; `plan/PLAN.md` row updated
- [x] phase `STATUS.md` overwritten
- [x] sibling `STATUS.md` notes: not needed (no sibling was edited)
- [x] UA-002 and UA-003 acted on and moved to `docs/USER_ACTIONS_resolved.md`
- [x] Release recommendation for the user: **v0.10.0** after 15fr (see the hand-off)
