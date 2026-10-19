# Step 15fr: phase 06 review fixes

> **Phase:** 06, Text layer v2 — SG line geometry and layout repair. **Depends on:** 15d, 15e,
> 15f (all done). **Model:** Sonnet. **Written:** 2026-10-18 by the phase-06 review (Opus).
> **Read:** `plan/README.md` (*Rules for every session* only), `plan/phase-06-text-layer-v2-sg/STATUS.md`,
> then this file. **Read nothing else unless this brief names it.**
> **Budget:** one session. The only behaviour change is the default factor in §1.

## Context

The phase review (`plan/phase-06-text-layer-v2-sg/REVIEW.md`) accepted the phase. Three items
remain before the release:

1. **UA-002, answered (b) by the user:** the merged-line threshold factor becomes **1.19**
   (was 1.22). With 1.22, 192 real merged lines in 456 (1,100-1,126 px wide, short right part)
   stay whole. The review read 25 of the 45 lines that 1.19 adds in 455 and found about 23 of
   them to be real merged lines, so few false candidates are expected.
2. The ported module names the MIT licence but not the copyright holders.
3. The changelog misses some public names of 15e and one manifest change.

## Inputs

All paths are relative to the AS root `/Users/marijnkoolen/Code/Huygens/archival-structures`.
Line numbers were checked on 2026-10-18. The grep anchors are in backticks.

- `archival_structures/text_layer/line_split.py`:
  - module docstring L1-20 (anchor `Licence: MIT (per republic-project's`);
  - `def derive_split_params(` L1187-1218. The docstring sentence at L1199 reads
    ``width. ``min_merged_width`` is 1.22 times the default width (the notebook's 1100/900)."""``.
    L1216 reads `min_merged_width=round(1.22 * width), default_line_width=width,`.
  - Its callers pass `(rows, side, split_region_tags)` positionally, so a new keyword argument
    with a default changes nothing for them.
- `tests/test_text_layer_line_split.py`:
  - L19 `# ported from republic-project tests/test_line_splitter.py`;
  - L202 in `test_derive_split_params`:
    `assert params.min_merged_width == round(1.22 * 900) == 1098`.
- `docs/text_layer.md` L148-149 (section *Merged-line splitter*), anchor `1.22 times that wide`.
- `archival_structures/text_layer/word_split.py` L17: `KEEP_TRAIL = ("-", "„")`. Nothing uses it.
- `docs/shared_library/changelog.md`, section *Unreleased* → `### Step 15e: merged-line splitter`.
- Inputs for the re-run (read only):
  - print PageXML `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml/`
    (`NL-HaNA_1.10.94_455.zip`, `_456.zip`);
  - the baseline split layer (1.22): `data/text_layer_v2_dev/15e-a/print/NL-HaNA_1.10.94_text_layer_v2/`;
  - the unsplit 15d layer: `data/text_layer_v2_dev/15d-a/print/NL-HaNA_1.10.94_text_layer_v2/`,
    plus `15d-a/` as a whole for `rates.py`.
- Scripts (they exist; don't rewrite them):
  - `reports/data/phase-06-text-layer-v2-sg/step-15e/ea.py` (`check RUN_DIR OUT D15_DIR` gives
    text conservation, geometry and column coverage);
  - `.../step-15e/rates.py` (`rates.py 15D_RUN_DIR OUT.tsv`; it calls `derive_split_params`, so it
    picks up the new default).

## Changes

### 1. Factor 1.19 (UA-002)

- `derive_split_params(line_rows, page_side, split_region_tags=(), min_chars=20, merged_width_factor=1.19)`.
  Add the keyword argument last, and use `min_merged_width=round(merged_width_factor * width)`.
  Docstring: "``min_merged_width`` is `merged_width_factor` (default 1.19) times the default
  width: the notebook's 1100 px over 456's derived 924 px. 1.22 (1100/900) left 192 merged
  lines with a short right part unsplit in 456 (UA-002)."
- `tests/test_text_layer_line_split.py` L202: `assert params.min_merged_width == round(1.19 * 900) == 1071`.
  Add one assertion to the same test:
  `assert ls.derive_split_params(rows(specs), "left", merged_width_factor=1.22).min_merged_width == 1098`.
- `docs/text_layer.md` L148-149: "at least 1.22 times that wide" becomes "at least 1.19 times
  that wide (`merged_width_factor`)".
- Don't add a preset field. Don't change any ported function (everything above
  `# --- AS adapter ---`).

### 2. Attribution

- `line_split.py` docstring: replace the line `Licence: MIT (per republic-project's ``pyproject.toml``).` with
  ```
  Copyright (c) the republic-project authors (Marijn Koolen, Rik Hoekstra, Hayco de Jong).
  Licence: MIT (per republic-project's ``pyproject.toml``); the MIT permission notice is the one
  in AS's ``LICENSE``.
  ```
- `tests/test_text_layer_line_split.py` L19: append
  ` (Copyright (c) the republic-project authors, MIT)`.

### 3. Small clean-ups

- `word_split.py`: delete `KEEP_TRAIL` (L17) and the blank line after it, if that leaves exactly
  two blank lines before `@dataclass`.

### 4. Changelog (Edit only, never rewrite the file)

Under *Unreleased*, add `### Step 15fr: phase 06 review fixes` **above** `### Step 15f`:

- `text_layer.line_split.derive_split_params` has a new keyword `merged_width_factor`, default
  **1.19** (was a fixed 1.22). The `sg_1.10.94_v2` line tables change: more lines are split
  (counts in the phase-06 report, addendum 15fr). No sibling import changes.
- `manifest.json` of every layer (also `voc_v1`) has a new key `format_version` (1 or 2), since
  step 15d. The `voc_v1` tables are unchanged.
- Step 15e also made these names public: `SplitModel`, `SplitHook`, `fit_split_model_from_rows`,
  `read_line_rows`, `is_candidate_row`, `training_docs_from_lines`.

## Constraints

- Never `git commit`; never bump the version; `chmod g+w` everything you create.
- **Protected inputs:** everything under `/Users/Shared/claude_projects/data_scopes/` (shared-data,
  LotA, NLP) and `/Users/marijnkoolen/Code/Huygens/republic-project`. Before the first run, write
  `sha256 mtime path` of the two print zips to `data/text_layer_v2_dev/15fr-protected-before.txt`.
  Write the same to `...-after.txt` after the last run, and check that they are equal.
- Don't overwrite or rewrite `15d-a/`, `15e-a/`, `15e-b/` or `15f-a/`. Give every run its own new
  directory (`rm -rf` is denied in the agent shell, and `&`/`nohup` fail).

## Tests

- `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_line_split.py`: all
  pass.
- Full suite: `.venv/bin/python -m pytest -q -p no:cacheprovider --continue-on-collection-errors`.
  The review measured 1010 passed and 0 collection errors on 2026-10-18. Expect 1010 passed
  (no new test functions, one changed).

## Acceptance criteria

1. **Re-run:**
   `.venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir data/text_layer_v2_dev/15fr-a/print --preset sg_1.10.94_v2 --workers 8`
   exits 0. Then, for 455 and 456, report from `split_stats/<inv>.json` (and
   `split_model/<inv>.json`), next to 15e-a's values:
   - `min_merged_width` per side;
   - candidates before;
   - split by hyphen and by bigram;
   - unsplit after;
   - page errors (must be 0).

   For 456, the review expects the split count to rise by about 190 (the notebook-parameter run
   had 64,289 candidates and split 64,255). For 455, it expects about 45 more candidates. A large deviation is
   reported, not tuned.
2. `ea.py check data/text_layer_v2_dev/15fr-a/print/NL-HaNA_1.10.94_text_layer_v2 <OUT> data/text_layer_v2_dev/15d-a/print/NL-HaNA_1.10.94_text_layer_v2`:
   text conservation 0 exceptions; halves within the box: no more than 4 exceptions in 456 and
   0 in 455 (15e's values); `L.x < R.x` for 100%.
3. `rates.py data/text_layer_v2_dev/15d-a data/text_layer_v2_dev/15fr-rates/rates.tsv`. Report
   the 16 rows as in 15e's rates table (candidates, rate), with the change against 15e.
4. Tests as above. Protected inputs unchanged.

The user decides when the step is done.

## Error analysis

- **New splits:** the originals that are split in `15fr-a` but not in `15e-a`. Find them by the
  `split_from` values that occur in 15fr-a and not in 15e-a. Read 20 per inventory (seed 18;
  all of them if fewer). Show each with its L, R, the line below L in column 1 and the line
  above R in column 2. Classify each as: correct; off by k words; not a merged line; other.
- **New candidates still unsplit:** candidates after in 15fr-a that weren't candidates in 15e-a.
  Read all of them, up to 20. Classify each as in 15e: a single wide line, or a merged line that
  failed (and why).
- Conclude in two sentences whether 1.19 adds false splits.

## When done

1. The report `reports/phase-06-text-layer-v2-sg.md` is **final**. Append a subsection
   `### Addendum: step 15fr (review fixes)` at the end of *Steps* (after *Step 15f*, before
   `## Review`). It holds what changed, the acceptance numbers against 15e-a, and the error
   analysis. Don't add finding rows and don't change finding statuses. A new observation goes
   into the addendum's text as a note for the user. Run `make reports` and check that it gives
   no warnings.
2. Append a `PROGRESS.md` block (template in `plan/templates/PROGRESS.md`; fields: session,
   files changed, public API, tests, acceptance, report link, error analysis, deviations, brief
   defects, open questions).
3. Overwrite `STATUS.md`: "15fr done; ready for release". *Facts later steps need*: the new
   print layer dir `data/text_layer_v2_dev/15fr-a/print/` replaces `15e-a` as the split print
   layer for phase 07.
