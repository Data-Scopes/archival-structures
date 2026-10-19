# Step 15f: glued-word repair

> **Phase:** 06, Text layer v2. **Depends on:** 15e (split print lines), 15d.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md`. If you have to explore anyway, record it under *Brief defects*.

## Context

The print HTR often drops the space between words: `Vranckrijcktoeleggen`,
`wordendoorwietedoen,`, `HondertstenpenninghtoekomendeInge-`. In 150 scans of 455, 1,251 tokens
had ≥18 word characters. The hand has the problem less often: 16 tokens in 150 scans of 3349,
some glued (`Nassaugeimpetreerd`, `doormarschvantroupes`) and some genuine
(`onordentelijcheijt`).

The PageXML `Word` elements don't help. They are the line text split on spaces (100% equal,
planning check C2). So the repair must be text-based.

Hand-off R3 asks for:
- the method;
- its error rate;
- a flag on each line saying what changed.

This step builds a conservative unigram segmenter that only inserts spaces. It measures the
segmenter on held-out data, then applies it as a resumable stage.

## Inputs

- The print lines after the split: 15e's run dir (in `STATUS.md` *Facts later steps need*),
  `…/NL-HaNA_1.10.94_text_layer_v2/lines/<inv>.lines.tsv.gz`.
- The hand lines: 15d's `data/text_layer_v2_dev/15d-a/hand_tar/NL-HaNA_1.01.02_text_layer_v2/lines/`
  (3349–3354).
- The line columns are `scan_id page_side column region_id line_idx line_id x y w h baseline
  conf text_raw text edit_flags split_from`. `edit_flags` is `;`-joined (15e writes
  `split:hyphen` and `split:bigram`).
- Read them with `archival_structures.text_layer.io.read_tsv_gz(path)` (yields dict rows) and write
  with `io.write_tsv_gz(path, header, rows)` (deterministic, atomic).
- `text_layer/presets.py`: `LayerPreset` (it has `format_version`, `merged_line_split`,
  `split_region_tags`, …), `SG_HAND_V2`, `SG_PRINT_V2`.
- `text_layer/build.py`: the v2 extract stage and the stage dispatch; `--stage all` for v2 runs
  extract only.
- Check in `STATUS.md` that `text_raw` of split halves is the half of the original raw text. If
  it isn't, stop and ask.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/word_split.py` (new) | `WordSplitParams` (frozen dataclass: `min_token_len`, `min_part_len=2`, `min_part_count`, `max_parts=4`, `split_penalty`); `token_core(token) -> tuple[str, str, str]` (leading punctuation, core, trailing punctuation); `build_vocabulary(line_texts) -> collections.Counter` (lower-cased cores of alphabetic tokens); `segment(core, vocab, total, params) -> list[str] or None`; `repair_line(text, vocab, total, params) -> tuple[str, list[tuple[int, str, list[str]]]]`; `apply_word_split(rows, vocab, params) -> rows` (sets `text` and `edit_flags`) |
| `text_layer/presets.py` | `LayerPreset` gains `word_split: str = "off"` and `word_split_params: Optional[Mapping] = None`. The SG presets are set per the outcome (criterion 4) |
| `text_layer/build.py` | New v2 stage `wordfix`, run by `--stage all` after extract when `preset.word_split == "on"`. It: builds the vocabulary from the selected inventories' `text_raw`, or reads `--word-vocab PATH`; writes `word_split/vocab.tsv.gz` and `word_split/params.json`; rewrites `lines/<inv>.lines.tsv.gz` (atomically); writes `word_split/edits/<inv>.tsv.gz` (`line_id`, `token_index`, `original`, `replacement`). The stage is **idempotent**: `text` is recomputed from `text_raw`; earlier `wordsplit:*` flags are replaced; other flags are kept |
| `tests/test_text_layer_word_split.py` (new) | tests below |
| `reports/data/phase-06-text-layer-v2-sg/step-15f/` | `evaluate.py` (dev/test), `ea.py` |
| `docs/text_layer.md`, changelog (Edit) | method, params, flags; `### Step 15f: glued-word repair` |
| `reports/phase-06-text-layer-v2-sg.md` | `### Step 15f` section |

## Behaviour to implement

- **Tokens:** split `text_raw` on single spaces. `token_core` strips leading and trailing
  characters that are not letters (`str.isalpha` false), except that a trailing `-` or `„` (a
  word break) stays in the trailing part.
- **Candidates:** the core is all letters; `len(core) >= min_token_len`; and
  `vocab[core.lower()] < min_part_count` (an attested word is never split).
- **`segment`:** a Viterbi search over split points (≤ `max_parts` parts, each ≥ `min_part_len`
  characters). Each part `p` must have `vocab[p.lower()] >= min_part_count`. The score is
  `Σ log(count(p)/total) − split_penalty·(parts−1)`. Return the best segmentation with ≥2 parts
  only if its score is greater than the unsplit score `log(0.5/total)`. Otherwise return None.
  Ties go to fewer parts, then to the leftmost split.
- **`repair_line`:** reassemble as `lead + " ".join(parts with the original casing, sliced from
  the core) + trail`, keeping the original casing. **Only spaces are inserted.** Removing the
  inserted spaces must give `text_raw` back.
- **Flags:** `wordsplit:<n tokens split>`.

## Evaluation protocol (decide the parameters before applying)

Run it separately for **print** (455+456 split lines) and **hand** (3349–3354).
- **Splits by scan:** `int(sha1(scan_id).hexdigest(), 16) % 10`. 0 is test, 1 is dev, 2–9 are
  train.
- **The vocabulary** comes from train-scan lines only, for evaluation.
- **Synthetic glued tokens** come from dev and test lines:
  - every pair, and every triple, of adjacent tokens whose cores are all letters and whose glued
    length is `>= min_token_len`;
  - sample at most 5,000 per split (seed 17);
  - the truth is the original token boundaries.
- **Metrics:**
  - exact-segmentation rate (all boundaries right);
  - boundary precision and recall;
  - the no-split rate.
- **Real-token behaviour** on dev and test: for long OOV tokens that occur naturally (not
  synthetic), report the share that the method splits. There is no truth for these; the error
  analysis reads them.
- **Tuning:**
  - the grid is `min_token_len ∈ {8, 10, 12}`, `min_part_count ∈ {3, 5, 10, 20}`,
    `split_penalty ∈ {0, 2, 4, 6}`;
  - choose on **dev**: among the settings within 0.02 of the best exact-segmentation rate, take
    the most conservative one (highest `min_part_count`, then highest `split_penalty`, then
    highest `min_token_len`);
  - then report **test once**.
- **Leakage:** the test scans are excluded from the vocabulary used for evaluation. The
  production run (all scans) uses a vocabulary over all scans; that is fine, because nothing
  there is scored.

## Constraints

- Never `git commit`; never bump the version; `chmod g+w` everything you create.
- **Protected:** `shared-data/`, the sibling repos, and 15d's and 15e's run dirs. **Copy** the
  input layers you need into a new dir `data/text_layer_v2_dev/15f-<run>/` and run `wordfix`
  there. Don't rewrite lines in 15d/15e dirs.
- No new dependencies. Pure Python with `collections` and `math`.
- Don't change `extract.py`, `join.py`, `word_breaks.py`, `line_split.py`, or the v1 code
  paths.
- **Precision bar, fixed in advance:** if the hand-read precision of real repairs (see *Error
  analysis*) is **below 0.90** for an archive, set `word_split="off"` in that archive's preset
  and report it as a negative result. Don't tune further to pass the bar.
- Agent shell: no `nohup`/`&`/`rm -rf`.

## Tests

- `test_token_core_keeps_break_mark` (`Inge-`, `woordt„`, `(Hondert,`).
- `test_segment_simple`: vocab {dat, wy, noch, ders, …}.
- `test_attested_word_not_split`.
- `test_min_part_len_and_max_parts`.
- `test_repair_line_only_inserts_spaces`: property over 200 random lines from a small synthetic
  corpus.
- `test_casing_preserved` (`HondertstenpenninghtoekomendeInge-` → `Hondertsten penningh
  toekomende Inge-`).
- `test_wordfix_stage_idempotent`: running twice gives byte-identical lines.
- `test_flags_preserve_split_flags`.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py`, then
  the full suite with `--continue-on-collection-errors`.

## Acceptance criteria

1. **Evaluation table** per archive: the chosen parameters, then the dev and test values of
   exact rate, boundary P and R, no-split rate, and real-token split share. Test is reported
   once.
2. **The production run** (CLI `--stage wordfix`, with all scans in the vocabulary) on the copied
   print and hand layers. Report per inventory:
   - lines changed;
   - tokens split;
   - tokens that remain ≥18 characters (before against after).
3. **Text conservation:** in 100% of changed lines, `text.replace(" ", "")` ==
   `text_raw.replace(" ", "")`, and the inserted spaces equal the count in `edits`.
4. **Hand-read precision** of 50 real repairs per archive (print 50, hand min(50, all)): the
   count correct, wrong boundary, should not split, and the precision. Apply the 0.90 bar and set
   the preset accordingly.
5. The v1 regression still prints `TOTAL 157 / 157` (the commands are in 15d's brief, repeated in
   `STATUS.md`; use a new dir `15f-v1check`). AS suite green.

The user decides when the step is done.

## Error analysis

Seed 17; read all cases if there are fewer.
- Read 50 real print repairs and 50 (or all) real hand repairs, each with its line context.
  Classify: correct; wrong boundary; genuine word wrongly split (a compound or a proper name);
  abbreviation; Latin or French.
- Read 30 long tokens that remain unsplit in print. Classify: genuine long word; glued but
  missed (why: a part below `min_part_count`, too many parts, an OCR error inside a part); other.
- **Expected findings:**
  - F06-15f.1 the test metrics;
  - F06-15f.2 real precision per archive and the on/off outcome;
  - F06-15f.3 residual glued tokens;
  - F06-15f.4 the error categories.

## Do not touch

`extract.py`, `join.py`, `word_breaks.py`, `line_split.py`, `compare.py`; siblings;
`shared-data/`; earlier run dirs.

## When done

1. Add `### Step 15f: glued-word repair` to `reports/phase-06-text-layer-v2-sg.md` (results,
   error analysis, findings `F06-15f.n` as `draft`) and a *Reproduce* line. Run `make reports`.
2. Append a PROGRESS block (fields as in 15d's brief: session, status, files, public API, tests,
   acceptance with numbers, report link `#step-15f-glued-word-repair`, error analysis,
   deviations, brief defects, open questions).
3. Overwrite `STATUS.md`: the phase is ready for review if 15d–15f are done. Under *Facts later
   steps need*, record the final run dirs and the preset states.
