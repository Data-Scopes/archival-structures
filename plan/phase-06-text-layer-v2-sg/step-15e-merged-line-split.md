# Step 15e: merged-line splitter (port of republic's `long_line_splitter.py`)

> **Phase:** 06, Text layer v2. **Depends on:** 15d (its `extract_v2`, presets and run dirs).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md`. If you have to explore anyway, record it under *Brief defects*.

## Context

In the printed States General volumes (NL-HaNA 1.10.94 inventories 455/456; also printed
1.01.02 volumes such as 3771), the HTR sometimes merges a line of the left column and the line
beside it in the right column into one line. LotA's paragraphs then mix columns (hand-off R2).

republic-project has a splitter that "works partly" (the user). The user decided that **AS ports
it, so that it becomes part of AS**. This step:
1. ports it verbatim and proves the port matches the original;
2. adapts it to AS's v2 extract as a `scan_hook`, with parameters derived per inventory and a
   bigram scorer fitted in-corpus;
3. reports **how many lines were split and how many merged lines remained unsplit**.

## Inputs

All read only. Never write under `/Users/Shared/claude_projects/data_scopes/` or
`/Users/marijnkoolen/Code/Huygens/republic-project/`.

**republic-project** (`R=/Users/marijnkoolen/Code/Huygens/republic-project`, MIT licence per its
`pyproject.toml`; no LICENSE file):
- **Source:** `$R/republic/parser/pagexml/long_line_splitter.py` (1,073 lines, last commit
  `47dfc010`, 2026-01-23). Read it all; you port it. Its top-level names:
  - helpers: `width_in_range`, `get_line_sequences_in_width_range`, `Word`, `get_words`,
    `get_split_indexes`, `SplitWord`, `ColumnLines`, `LineInfo`, `add_word_info`;
  - merged-line context: `MergedLineContext`, `get_dist_to_other`,
    `extract_left_right_split_from_combination`, `extract_left_right_splits_from_regex_match`;
  - geometry: `make_split_lines`, `make_split_baseline`, `make_split_coords`;
  - `LineSplitter` (L397: `__init__(min_merged_width, default_line_width,
    default_line_num_chars, text_left_boundary, text_right_boundary, column_sep_width=20)`);
  - candidates: `get_split_word_candidates`, `make_page_merged_line_contexts`;
  - splitting: `replace_merged_line`, `update_merged_line_context`,
    `page_split_merged_hyphenated_lines`, `split_merged_non_hyphenated_lines`,
    `get_page_content_lines`, `is_merged_line_distractor`,
    `page_split_merged_non_hyphenated_lines`;
  - choosing a split: `evaluate_combination`, `select_best_combination`, `print_comb`,
    `make_bigram_combinations`;
  - columns: `make_column_text_region`, `determine_line_column_side`, `make_column`,
    `page_split_columns` (L1024).
- **Its imports** (all in pagexml-tools 0.9.0 except the two republic ones):
  - `pagexml.model.physical_document_model as pdm`;
  - `nltk.collocations.BigramCollocationFinder`;
  - `pagexml.analysis.layout_stats.sort_coords_above_below_baseline` and
    `compute_baseline_distances`;
  - `republic.helper.pagexml_helper.horizontal_group_lines` (at `$R/republic/helper/pagexml_helper.py`
    L336–368; port it);
  - `republic.parser.pagexml.generic_pagexml_parser.copy_page`, which is only a re-export of
    `pagexml.helper.pagexml_helper.copy_page` (import that instead).
- **Test to port:** `$R/tests/test_line_splitter.py` (74 lines, `test_split_word_candidates`; it
  uses `pagexml.parser.json_to_pagexml_line`).
- **Scorer** (notebook `Splitting-Fagel-Merged-Lines.ipynb` cell 32, verbatim; `finder` is an
  NLTK `BigramCollocationFinder`, `bigram_measures = BigramAssocMeasures()`):
  ```python
  def make_scorer(min_uni_freq: int = 100, non_score: float = 0.0):
      _SMALL = 1e-10
      def scorer(w1, w2):
          bf = finder.ngram_fd[(w1, w2)] if (w1, w2) in finder.ngram_fd else 0
          if bf > 0:
              return finder.score_ngram(bigram_measures.pmi, w1, w2)
          wf1 = finder.word_fd[w1] if w1 in finder.word_fd else 0
          wf2 = finder.word_fd[w2] if w2 in finder.word_fd else 0
          if bf == 0 and min(wf1, wf2) > min_uni_freq:
              return np.log(_SMALL / np.exp(np.log(wf1 / finder.N) + np.log(wf2 / finder.N)))
          return non_score
      return scorer
  ```
  The finder was built with `BigramCollocationFinder.from_documents(docs)`. Each doc is the token
  list (`text.split(' ')`) of a run of consecutive non-merged lines; a merged line ends a doc.
- **Notebook parameters** (inventory 456): `LineSplitter(min_merged_width=1100,
  default_line_width=900, text_left_boundary=450, text_right_boundary=2350,
  default_line_num_chars=45)`.
- **Notebook pipeline per page:**
  1. `page_split_merged_hyphenated_lines(splitter, page)`;
  2. `page_split_merged_non_hyphenated_lines(splitter, page, scorer)`, inside `try/except`,
     falling back to the unsplit page;
  3. separately, `page_split_columns(splitter, page)`.

**A/B input** (REPUBLIC page JSON, the original's own input):
`/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/pages/raw/raw_pages-{455,456}.jsonl.gz`
- One JSON page per line (1,442 pages for 455).
- Load each with `republic.helper.pagexml_helper.json_to_pagexml_page(json.loads(line))`
  (`$R/republic/helper/pagexml_helper.py` L515), with `sys.path.insert(0, R)`.
- Keep the pages where `page.has_type('resolution_page')`.
- The original imports cleanly under AS `.venv` that way (checked 2026-10-17).

**AS** (from 15d; check `STATUS.md` *Facts later steps need* for the run dirs):
- `text_layer/extract_v2.py`:
  - `extract_inventory_v2(inv, source, out_root, preset, *, scan_hook=None)`;
  - the line columns `scan_id page_side column region_id line_idx line_id x y w h baseline conf
    text_raw text edit_flags split_from`;
  - `column`, `edit_flags` and `split_from` are read from `line.metadata`.
- `presets.py`: `SG_PRINT_V2` (`merged_line_split="on"`), `SG_HAND_V2` (`"off"`).
- `build.py`: the v2 extract stage.
- 15d's lines output for all 16 inventories: `data/text_layer_v2_dev/15d-a/{hand_tar,hand_local,print}/<layer>/lines/`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/line_split.py` (new) | Verbatim port of all names above plus `horizontal_group_lines`. Header docstring: source path, commit `47dfc010`, MIT, "ported verbatim; changes listed below", followed by the list. nltk is imported lazily or only under `TYPE_CHECKING` |
| same module, AS additions below a `# --- AS adapter ---` line | `SplitParams` (frozen dataclass: the 5 `LineSplitter` args + `column_sep_width=20` + `split_region_tags: tuple`); `derive_split_params(line_rows, page_side) -> SplitParams`; `train_bigram_finder(docs)`; `make_pmi_scorer(finder, min_uni_freq=100, non_score=0.0)` (the notebook scorer with `finder` passed in); `merged_line_candidates(line_rows, params) -> list[dict]`; `fit_split_model(inv, source, preset, lines_path=None) -> SplitModel` (params per page side + scorer); `make_split_hook(model) -> Callable[[PageXMLScan], PageXMLScan]` |
| `text_layer/build.py` | For a v2 preset with `merged_line_split == "on"`, the extract stage fits the model, then extracts with the hook. It writes `split_model/<inv>.json` (params, finder sizes) and `split_stats/<inv>.json`. With `"off"`, behaviour is unchanged |
| `presets.py` | `LayerPreset` gains `split_region_tags: tuple = ()`. `SG_PRINT_V2` uses `("index", "resolution", "untagged", "Text")` (see *Behaviour* 1). VOC and hand presets are unchanged |
| `pyproject.toml` | add `nltk (>=3.9)` to the core dependencies, in the existing style |
| `tests/test_text_layer_line_split.py` (new) | the ported test plus the tests below |
| `docs/text_layer.md`, changelog | v2 section addition; `### Step 15e: merged-line splitter` (Edit) |
| `reports/data/phase-06-text-layer-v2-sg/step-15e/` | `ab_republic.py`, `rates.py`, `ea.py` |
| `reports/phase-06-text-layer-v2-sg.md` | `### Step 15e` section |

## Behaviour to implement

**Port.**
- Keep every function body as it is. Allowed changes:
  - import paths;
  - `print` debug output kept, but only under `debug > 0`, as it is already;
  - a type hint `ngram_scorer: Callable[[str, str], float]` (it is used as a callable);
  - fixes for crashes that the A/B shows. Each fix is listed in the header and gets a test.
- No behaviour change hidden in a "cleanup".

**Adapter.** Use the original functions; do not re-implement them.
1. **Lines that take part:** lines whose region tag is in `params.split_region_tags`. For
   `SG_PRINT_V2` that is `("index", "resolution", "untagged", "Text")`. Measured on 15d's output
   (lines / merge candidates):

   | Inventory | `index` | `resolution` | `untagged` | `Text` | other tags |
   |---|---|---|---|---|---|
   | 455 | 130,877 / 9,737 (684 scans) | 28,442 / 1,664 | 1,261 / 67 | – | catch-word 1,116, page-number 122, header 111, signature-mark 76, date 6; all 0 candidates |
   | 456 | – | – | – | 111,209 / 64,289 (all regions) | – |

   Laypa's `index` tag in 455 is clearly not limited to index pages: it spans scans 4–717. The
   page-furniture tags are **not split**, but their candidate counts are reported (criterion 6).
2. **Pages:** per scan and page side (a line's centre x against `page_w/2`, as in 15d), build a
   `pdm.PageXMLPage`:
   - coords derived from those lines;
   - one `PageXMLColumn` holding the participating regions (copied with `copy_page`, or built
     with `pdm` constructors; the regions keep their ids);
   - `page.columns` set so that `get_page_content_lines` sees them.
3. **`derive_split_params(line_rows, page_side)`**, per inventory and page side, from 15d-style
   line rows of the participating tags with ≥20 characters:
   - `text_left_boundary` = 5th percentile of `x`;
   - `text_right_boundary` = 95th percentile of `x + w`;
   - `span = right − left`;
   - column lines are the rows with `0.35·span ≤ w ≤ 0.6·span`;
   - `default_line_width` = median `w` of the column lines;
   - `default_line_num_chars` = median `len(text_raw)` of the column lines;
   - `min_merged_width = round(1.22 * default_line_width)` (the notebook's 1100/900);
   - `column_sep_width = 20`.

   **Don't use the plain median width.** In 456 most participating lines are merged (median w
   is 1,810 px), so the median would be the merged width. Planning measured these values on all
   lines with ≥20 characters:

   | Inventory | left side x p5 / right p95 | right side x p5 / right p95 | lines 700–1,050 px, median characters |
   |---|---|---|---|
   | 455 | 385 / 2,269 | 2,343 / 4,316 | 46 |
   | 456 | 384 / 2,329 | 2,348 / 4,364 | 45 |

   So expect about 900 px and 45 characters. Report the derived values for 456 next to the
   notebook's 900/45/1100/450/2350.
4. **Scorer:**
   - docs are runs of consecutive non-candidate lines, per region, in document order;
   - candidates follow `LineSplitter.is_merged_line`'s rule: `len(text) >= default_line_num_chars`
     and `w >= min_merged_width`;
   - the scorer is fitted on the inventory's own participating lines (`fit_split_model`, first
     pass) with `make_pmi_scorer(min_uni_freq=100)`.

   No labels are involved, so there is no leakage, but say so in the report.
5. **Hook:** per page side, run
   1. hyphenated;
   2. non-hyphenated in `try/except Exception` (count errors and keep the unsplit page; don't
      catch `BaseException`);
   3. `page_split_columns`.

   Then write the results back into the scan's regions:
   - a merged line is replaced, at its position, by its halves `{line_id}_L` and `{line_id}_R`;
   - each half's `metadata` gets `split_from = <original id>`, `edit_flags = "split:hyphen"` or
     `"split:bigram"`, and `column = "1"` or `"2"`;
   - every other participating line gets `column` from `page_split_columns` (`"1"` left, `"2"`
     right, `"x"` extra);
   - text, coords and baseline come from the split lines.

   The hook must be deterministic.
6. **Stats** (`split_stats/<inv>.json`):
   - `n_lines_participating`;
   - `n_candidates_before`;
   - `n_split_hyphen`, `n_split_bigram`;
   - `n_candidates_after` (merged lines left unsplit);
   - `n_page_errors`, with error types;
   - `n_candidates_other_tags`;
   - `params`.

## Constraints

- Never `git commit`; never bump the version; `chmod g+w` everything you create.
- **Protected:** everything under `/Users/Shared/claude_projects/data_scopes/`, republic-project,
  and 15d's run dirs (read them; don't write into them). Record sha256 and mtime of the two
  print zips and the two `raw_pages` files before and after.
- Outputs go to `data/text_layer_v2_dev/15e-<run>/` (new dirs) or `tmp_path`.
- A/B runs operate on `copy.deepcopy` of each page, for both sides (the original mutates in
  places).
- Don't change `extract.py`, `join.py`, `word_breaks.py`, or the v1 code paths.
- Agent shell: no `nohup`/`&`, no `rm -rf`. Install with `.venv/bin/python -m pip`, then
  `chmod g+w`.

## Tests

- `test_split_word_candidates`, ported from republic.
- `test_hyphenated_merged_line_split`: a synthetic page, one merged line `"… Cor- respondentie …"`
  at about 2× width. It splits at the hyphen; the halves' boxes lie inside the original; and
  `L.text + " " + R.text == original`.
- `test_non_hyphenated_split_uses_scorer`: a synthetic page and a stub scorer, so the best split
  is known.
- `test_page_split_columns_sides`.
- `test_derive_split_params`: synthetic rows give the known medians and percentiles.
- `test_hook_sets_metadata_and_ids`.
- `test_hook_off_for_hand_preset`: `extract_inventory_v2` output for the fixture is
  byte-identical with and without a no-op hook.
- `test_make_pmi_scorer_matches_notebook`: a tiny finder; compare with values computed by hand.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_*.py`, then
  the full suite with `--continue-on-collection-errors`.

## Acceptance criteria

1. **Port fidelity (A/B)** — `ab_republic.py`:
   - for every resolution page of 455 and 456: build one shared scorer from those pages' lines,
     then run the original and the port with the notebook parameters on deep copies;
   - the pipeline is hyphenated → non-hyphenated → columns;
   - compare per page, per column, in order: `(line.id, line.text, coords.points,
     baseline.points)`, and the set of pages that raise.
   - **Required: identical on 100% of pages.** Report the pages compared, identical, and raising
     (both sides). If they differ, stop and report the first 5 diffs. Don't adjust the port to
     "mostly" match.
2. **Pilot split run.** The CLI, `--preset sg_1.10.94_v2`, the shared-data print dir,
   `--workers 8`, into `data/text_layer_v2_dev/15e-a/print/`. For each of 455 and 456, report:
   - candidates before;
   - split by hyphen and by bigram;
   - unsplit after;
   - page errors.
3. **Text conservation:** for 100% of splits, `L.text_raw + " " + R.text_raw` equals the
   original line's `text_raw`, with whitespace normalised to single spaces. List the exceptions.
4. **Geometry:**
   - the halves' boxes lie within the original box ±2 px;
   - `L.x < R.x`;
   - every participating line on a page side with ≥1 split has `column ∈ {1,2,x}`;
   - report the shares.
5. **Parameters:** a table of 456's derived parameters against the notebook values. Re-run 456
   with the notebook parameters into `15e-b/` and report both sets of counts. Don't choose
   between them on the counts alone: the error analysis decides, and the default stays
   *derived* unless the analysis shows it is worse.
6. **Rates for phase 07:** run `rates.py` on 15d's lines for all 16 inventories. For each, report:
   - the derived parameters;
   - the candidate rate among participating lines, and among all lines with ≥20 characters;
   - the counts per region tag.

   (Hand 3349–3354 and 11182/11281 are expected to be near 0; printed 3771–3823 are unknown.)
7. The v1 regression from 15d still prints `TOTAL 157 / 157` (same commands, a new dir
   `15e-v1check`). AS suite green.

The user decides when the step is done.

## Error analysis

Seed 16; if there are fewer cases, read all of them.
- Read 30 hyphen splits and 30 bigram splits from 15e-a (original line, L, R, neighbours above
  and below). Classify each: correct; split point off by k words; not a merged line (a full-width
  heading or a centred title); other.
- Read 30 lines that are still candidates after the split. Classify each: a legitimate
  full-width line (heading, date line, index); a failed split (why); other.
- Read 10 random split pages' column assignment (left/right line sequences): wrong side, `x`
  lines.
- Compare a correct-split rate between the derived and notebook parameters on the same 30+30
  originals where both split.
- **Expected findings:**
  - F06-15e.1 A/B fidelity;
  - F06-15e.2 split and unsplit counts (R2's required numbers);
  - F06-15e.3 split precision by kind;
  - F06-15e.4 candidate rates across the 16 inventories (input to the `auto` rule in phase 07);
  - F06-15e.5 derived against notebook parameters.

## Do not touch

`extract.py`, `join.py`, `word_breaks.py`, `compare.py`; the sibling repos and republic-project;
`shared-data/`; 15d's run dirs.

## When done

1. Add `### Step 15e: merged-line splitter` to `reports/phase-06-text-layer-v2-sg.md` (results,
   error analysis, findings `F06-15e.n` as `draft`) and a *Reproduce* line. Run `make reports`.
2. Append the PROGRESS block (same fields as the template in `plan/templates/STEP.md` *When
   done*: session, status, files, public API, tests, acceptance with numbers, report link
   `#step-15e-merged-line-splitter`, error analysis, deviations, brief defects, open
   questions).
3. Overwrite `STATUS.md`. Under *Facts later steps need*, record the run dir of the split print
   layer and whether `text_raw` of the halves is the half of the original raw text (15f relies
   on it).
