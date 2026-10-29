# Upstream inventory: making AS the shared library for LotA and NLP

*Compiled 2026-10-13. This is a planning document: nothing in the three projects was changed
when writing it.*

The user has decided that **AS** (`archival-structures`, the Python package
`archival_structures`) becomes the shared library that two sibling projects depend on:

- **LotA**: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive` (package `archival_language`)
- **NLP**: `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection` (not a package; code lives in `src/startpage/`, `src/region_relations/` and `scripts/`)

The list of what to move comes from `docs/cross-project-review.md` §§3, 4, 5, 6 and 9. It also
covers the tooling that produced the shared VOC text layer, `NLP/data/voc_text_layer_v1/`.

Below, "upstream" means moving code from a sibling into AS so that the sibling imports it from
AS afterwards. "Research code" means code that encodes one project's experimental protocol and
stays in that project.

The document has four parts:

1. Cross-cutting findings that affect every move.
2. How the siblings depend on AS today, and how they should.
3. One table per component.
4. A proposed order of small, independently testable steps.

The decisions the user needs to make are collected in §5.

---

## 1. Cross-cutting findings

1. **Importing anything under `archival_structures.stream_analysis` loads the heavy stack.**
   `stream_analysis/__init__.py` eagerly imports four modules:
   - `overview.clustering`, which needs `hdbscan` and `umap`;
   - `overview.embeddings`, which needs `torch` and `transformers`;
   - `overview.layout_analysis`, which needs `cv2` and `skimage`;
   - `groundtruth.boundary_annotation`, which needs `ipywidgets` and `IPython`.

   Python runs a package's `__init__.py` before any submodule, so even
   `import archival_structures.stream_analysis.corruption.metrics` pulls all of these in.
   LotA's venv has none of them, because it installs AS with `--no-deps` (see §2). NLP has
   torch but not hdbscan, umap or ipywidgets.

   So no new module should land under `stream_analysis/` until that `__init__.py` imports
   lazily (step 1 in §4). Lazy importing here means a module-level `__getattr__` (PEP 562)
   that imports a submodule only when one of its names is first used. Existing
   `from archival_structures.stream_analysis import window_diff` calls keep working.
2. **AS's dependency list is all-or-nothing.** `pyproject.toml` lists 23 runtime dependencies,
   including torch, transformers, hdbscan, umap-learn, opencv, scikit-image, ipywidgets,
   ipycanvas, ipyevents, matplotlib, seaborn and anthropic.
   - Most of the code to be moved needs only numpy, pandas, lxml, scikit-learn and
     pagexml-tools. torch is needed only by the CRF and BiLSTM decoders.
   - **No component in this inventory needs a third-party library that AS doesn't already
     declare**, with one exception: `scipy`, used by `semi_markov._lognormal_cdf`. scipy already
     comes in through pagexml-tools and scikit-learn, and the one call can be replaced by
     `math.erf`.
   - LotA's region-quality filter loads its lexicon with `gensim`, and LotA's title
     normalisation uses `formula_detection`. Both of those loaders stay in LotA; AS takes the
     lexicon as a plain set of words (see C16).
   - The recommendation is to split AS's dependencies into a light core plus optional
     *extras*: named groups of dependencies installed only on request, e.g.
     `pip install archival-structures[vision]`. See §2.
3. **Dependency version conflicts with NLP.**
   - NLP's `requirements.txt` pins `pandas==3.0.5`, but AS requires `pandas<3.0.0`.
   - It pins `torch==2.11.0`, but AS requires `torch>=2.12.0`.
   - NLP's interpreter actually has pandas 2.3.1, scikit-learn 1.8.0 and torch 2.12.1, and
     scikit-learn 1.8.0 is below AS's `>=1.9.0`.

   In other words, `requirements.txt` does not describe the environment NLP actually runs in.
   Before NLP depends on AS through pip, AS's lower bounds should be relaxed to what NLP
   actually runs on (`scikit-learn>=1.8`, `torch>=2.11`), or NLP's environment must be upgraded.
   AS's `pandas<3` cap also needs a test run on pandas 3.
4. **AS code that LotA depends on is not committed.**
   - `ead_tree`, `ead_fields`, `ead_views`, `ead_digital`, `ead_harvest`, `ead_stream` and
     `ead_survey`, plus their tests, are untracked in AS's git working tree.
   - `git describe` gives `v0.3.0-9-ga84fce2`.
   - Both siblings import from the live working tree, so neither can name the AS version it
     ran against.
5. **The shared VOC text layer was built against a modified pagexml-tools checkout.**
   - `manifest.json` records pagexml-tools 0.8.0 at `0f39fad0`, with four modified files:
     `text_stats.py`, `text_helper.py`, `pagexml_document_model.py` and `parser.py`.
   - The NLP `git_head` is empty in every stage.

   So the layer cannot be rebuilt from released dependencies. If the producer moves into AS,
   those pagexml-tools changes must be released first, or AS's results will not match v1.
6. **Scan-id and inventory-id parsing is re-implemented four times.** The National Archives
   scan-id grammar is `NL-HaNA_<archive>_<inventory>_<NNNN>.jpg`. Separate implementations:
   - `NLP globalise_alignment.scan_no`;
   - `NLP separator_sheets._scan_no`;
   - the `NLP voc_text_layer` sanity check and `inventory_sort_key`, plus
     `layout_features._inventory_number_sort_key`;
   - `LotA ead/adapters._BaseAdapter.parse_scan_id`.

   AS already has `model/inventory_number.InventoryNumber`, which LotA uses in 23 modules. A
   shared scan-id helper next to it (C13) removes the duplication, including the suffixed
   inventories `4848A` and `9626A`.
7. **Two claims in the review need correcting.**
   - The region-quality filter's keep precision/recall of 0.97/0.97 was measured on the
     **75 held-out rows** of a 150-row gold sheet (LotA `report/05`), not on 300 regions. After
     the `numeric_content_exempt` retune, the combined 150 held-out rows of the two sheets give
     keep/drop accuracy 0.893, with usable P 0.973 / R 0.870 (LotA `report/09`).
   - The `derdenSeptember` glue (no space before `<unitdate>`) is a defect in **LotA's**
     `element_text`, not in AS's `_text_of`. AS's only defect is the `<lb/>` glue (see C1).

---

## 2. How the siblings depend on AS

### Today

| | LotA | NLP |
|---|---|---|
| Mechanism | `make venv` runs `pip install --no-deps -e /Users/Shared/claude_projects/data_scopes/archival-structures`; the same is done for pagexml-tools, fuzzy-search and formula_detection | Homebrew Python 3.11 has `site-packages/archival_structures.pth` pointing at the AS repo. NLP's `venv` inherits it via `include-system-site-packages = true` |
| Effect | `venv/.../archival_structures.pth` adds the **AS repo root** to `sys.path`. LotA sees the AS working tree, including uncommitted files | same. The stale `archival_structures-0.1.0.dist-info` claims 0.1.0 while AS is at 0.3.0 |
| Version pin | none; a comment in LotA `pyproject.toml` says "0.3.0" | none |
| Why `--no-deps` | AS would pull in torch, transformers, umap and opencv, and its pandas/lxml ranges conflict (`LotA/docs/environment.md`) | — |
| What is imported | `InventoryNumber`/`InvalidInventoryNumberError` (23 package modules plus 12 tests); `ead_tree.EADComponent/EADFindingAid/parse_ead/parse_ead_string/extra_census`; `ead_fields.identify_inventory_number/parse_unitdate`; `ead_views.unit_of_description_frame`; `ead_digital.summarise_digitisation` | only the legacy `parsers.ead_parser.get_inventory_info`, in two scripts |
| Own code | installed editable (`pip install -e .`) | not installable. 93 scripts and 2 tests do `sys.path.insert(0, .../src)`; 14 insert `scripts/`; 3 hardcode a formula_detection path |

An *editable install* (`pip install -e`) puts a pointer to the source checkout on the import
path instead of copying files. Edits show up immediately, but nothing records which version was
used.

A side effect of the `.pth` pointing at the repo root: AS's loose top-level scripts are
importable as modules in both siblings. These are `make_iventory_paragraphs.py`,
`download_sample_eads.py`, `download_thumbnails.py` and `test_clustering_big_archives.py`.

### Proposed

1. **Light core plus extras in AS `pyproject.toml`:**
   - **core:** numpy, pandas, lxml, pagexml-tools, fuzzy-search, orjson, pyyaml, scikit-learn,
     tqdm;
   - **`[torch]`:** torch, for the CRF/BiLSTM decoders and DINOv2 embeddings;
   - **`[vision]`:** transformers, opencv-python, scikit-image, pillow, hdbscan, umap-learn;
   - **`[notebook]`:** ipywidgets, ipycanvas, ipyevents, ipython, matplotlib, seaborn;
   - **`[llm]`:** anthropic.

   Modules that need an extra import it lazily, inside the function, or guard it with an
   informative `ImportError`. LotA can then drop `--no-deps` for AS and install it normally
   with only the core.
2. **Pin to a tag, but keep editing possible.** Both siblings should declare
   `archival-structures` as a real dependency with a minimum version:
   - LotA in `pyproject.toml`;
   - NLP in a new minimal `pyproject.toml`, or at least `requirements.txt`.

   During active co-development they keep the editable path install:
   - LotA: its existing `make venv`, without `--no-deps`;
   - NLP: `venv/bin/pip install -e /Users/Shared/claude_projects/data_scopes/archival-structures`
     *inside its own venv*, replacing the Homebrew `.pth`.

   Reproducibility comes from two checks:
   - Each sibling's environment test (LotA already has `tests/test_environment.py`) asserts
     `importlib.metadata.version("archival-structures") >= "<tag>"`.
   - Each output manifest records the AS version, the git HEAD and the dirty-file list, the way
     NLP's text-layer manifest already does for pagexml-tools (`pagexml_tools_info()`; that
     helper moves to AS in C15).

   For a frozen, citable run, a sibling installs from a tag instead:
   `pip install "archival-structures @ git+file:///Users/Shared/claude_projects/data_scopes/archival-structures@v0.4.0"`.
   **This only works for committed code**, so the user has to commit and tag AS (decision D1).
3. **One version per step.** Each step in §4 ends with a version bump and tag on AS, and with a
   raised minimum version in the sibling that changes its imports.
4. **NLP shims during the transition.** NLP's 21 importers of `startpage.evaluation` (and the
   other moved modules) need not change at once. Each moved `src/startpage/<module>.py` becomes
   a *shim*: a few lines that re-export the AS names, plus any NLP adapter such as the
   `"start_page"` string-label conversion. Callers can then be migrated gradually.

---

## 3. Component tables

Each table lists:
- the source;
- size and third-party dependencies;
- coupling that must be turned into parameters;
- existing tests;
- the proposed AS destination;
- what the sibling imports afterwards and what stays behind;
- risks.

Line numbers refer to the files as of 2026-10-13.

### Glossary for this section

| Term | Meaning |
|---|---|
| **Pk** | Beeferman et al.'s segmentation error. Slide a window of k pages along the sequence and count how often reference and prediction disagree on whether the two ends of the window fall in the same segment. 0 is perfect |
| **WindowDiff** | Pevzner & Hearst's variant of Pk. It counts windows where the *number* of boundaries differs, which penalises near-misses less harshly than missed or extra boundaries |
| **±k tolerance F1** | Start-page precision/recall/F1 where a predicted start within k pages of a true start counts as a hit; matching is one-to-one |
| **Segment exact match** | The share of true documents whose start *and* end are both predicted exactly |
| **Macro / worst-inventory** | A metric averaged over inventories (macro), and the score of the worst inventory |
| **Bootstrap CI over inventories** | A confidence interval obtained by resampling whole inventories with replacement, so that within-inventory correlation is respected |
| **Start-rate calibration** | Per inventory: the predicted share of start pages against the true share |
| **Run / `positions` / `is_last_page`** | A run is a contiguous stretch of physical page positions in an evaluation subset. Metrics are computed per run so that windows never span a gap. `is_last_page` marks runs cut off before the inventory ends, so the final segment is not treated as complete |
| **Document-atomic split** | A train/dev/test split in which no document is cut across partitions |
| **Context padding** | Extra pages added at the edges of a chunk (`role=context`) that a model may look at but that are never scored (`eval_mask=0`) |
| **Inventory-held-out split** | Whole inventories are kept out of training, to test generalisation to unseen volumes |
| **CRF** | Linear-chain conditional random field: per-page emission scores plus a learned state-to-state transition matrix. **Viterbi** decoding finds the best label path |
| **Semi-Markov decoding** | Dynamic programming over *segments* rather than pages, with an explicit prior on segment length (here a lognormal) |
| **LIS** | Longest increasing subsequence, used to find the longest chain of HTR-read page numbers that increases with scan order |
| **Foliation / pagination** | Numbering every leaf (recto and verso share one number, so the step is 2 scans per number) versus numbering every page (step 1) |
| **MinHash / LSH** | A compact sketch of a text's set of character n-grams ("shingles"). Locality-sensitive hashing bands the sketches so that likely near-duplicates collide cheaply, and they are then confirmed by exact **Jaccard** similarity (intersection over union of the shingle sets) |
| **Word-break character (WBC)** | A line-final mark such as `¬` or `„` meaning "this word continues on the next line"; joining lines removes it and closes the gap |

---

### C1. Correct EAD text extraction across `<lb/>` (`element_text` / `_text_of` fix)

| | |
|---|---|
| **Source** | LotA `archival_language/ead/scopecontent_survey.py` `element_text(element) -> str`, L46–66 (file 334 lines). AS targets: `parsers/ead_tree.py` `_text_of`, L211–219 (used for `EADComponent.title`, also via `ead_stream`), and `parsers/ead_fields.py` L278 (`PhysDesc.raw_text`), which uses the same `itertext()` recipe |
| **Behaviour today** | **AS:** `" ".join("".join(el.itertext()).split())`. Glues across a bare `<lb/>` (`eerste<lb/>deel` → `eerstedeel`), but keeps whitespace at child edges correctly (`derden<unitdate> September</unitdate>` → `derden September`). **LotA `element_text`:** `<lb/>` → space (correct), but it collapses and strips *each child* before joining. That loses whitespace at child edges (`derdenSeptember`, `foo<emph>bar </emph>baz` → `foobarbaz`), and it can add a space before punctuation (`Eems<lb/>. Met` → `Eems . Met`) |
| **Deps** | lxml only |
| **Coupling** | none |
| **Tests today** | AS: no test contains `<lb`. LotA: `test_scopecontent_survey.py::test_element_text_normalises_whitespace` (inline `<lb/>` only) |
| **Destination** | A new public helper `element_text(element, *, line_break=" ")` in AS `parsers/ead_harvest.py`, which already holds `local_name`; or a small new `parsers/ead_text.py`. Algorithm: **one walk where `<lb/>` contributes `" "`, comments and processing instructions contribute only their tail, every other child is descended *raw*, and whitespace is collapsed once at the end.** `_text_of` and `ead_fields` L278 call it. Do **not** port LotA's function verbatim |
| **Tests to write** | `<lb/>` inside a word boundary; `<lb/>` before punctuation; child-edge whitespace (`<unitdate>`, `<emph>`); comment between words; nested `<lb/>` inside `<unitdate>`; regression that every title already correct in AS's EAD sample corpus is unchanged |
| **Sibling afterwards** | LotA imports `element_text` from AS and deletes its own. This **also changes LotA's `ScopeNote.text`**, which drops the child-edge glue, so scope-note outputs must be regenerated |
| **Risks** | **Double-fix interaction with LotA's F4 compensation** (`facets/title_norm.py`). Simulated with a monkey-patched `_text_of`: with the single-collapse fix, F4 is idempotent (re-applying it changes nothing), and the code cannot produce double spaces (`_collapse` follows every insertion). But three things change. (a) `_structural_date_spans` (L382–416) rebuilds the raw string with a *plain itertext join* and requires it to equal `node.title`, so every title containing both `<lb/>` and `<unitdate>` would silently fall back to regex date location. It must switch to the same lb-aware walk *in the same step*. (b) `lb_adjusted` (L507–509) would almost never be true, and `test_title_norm.py::test_lb_adjusted_flag` (L183) fails. (c) A verbatim port of LotA's `element_text` would *introduce* `derdenSeptember` into AS titles and remove the ground truth F4 relies on. Separately, LotA has an existing bug: `<lb/>` inside `<unitdate>` already violates the `date_span` invariant |

### C2. Stable EAD node id, and hashable components

| | |
|---|---|
| **Source** | LotA `archival_language/ead/hierarchy.py` `_node_id(archive_id, index_path)` L38 → `f"{archive_id}:{'.'.join(map(str, index_path))}"` (file 168 lines). `model/node.py` `ArchiveNode` frozen dataclass L37, with `parent_id` and `ancestor_ids` (68 lines). `index_path` itself is already computed by AS (`ead_tree.build_component` L322–329; roots ~L380), and `ead_stream` reproduces it |
| **Deps** | none |
| **Coupling** | Uses the archive id as a prefix (LotA passes `ArchiveConfig` ids such as `1.04.02`). Two LotA modules parse ids with `partition(":")`, which assumes no colon in the archive id |
| **Tests today** | LotA `tests/test_hierarchy.py`, 11 tests on real SG/VOC: unique ids, stable across reparse, 500-node round trip, hash/eq across parses, 1:1 frame join, ancestor ids. They need LotA config, so they should be re-expressed on AS's own `ead_samples/` |
| **Destination** | AS `parsers/ead_tree.py`: an `EADComponent.node_id(archive_id)` method (or an `archive_id` stored on `EADFindingAid` and a `node_id` property), plus `parent_id`/`ancestor_ids`. **Make `EADComponent` hash and compare by identity** (`@dataclass(eq=False)`). Today it is unhashable, and `__eq__` recurses over every child. AS `parsers/ead_views.py`: a `node_id` column in `unit_of_description_frame`, replacing the internal `id(c)` keying (L123) |
| **Sibling afterwards** | LotA keeps `ArchiveNode` (it carries LotA fields) but takes the id from AS. `ArchiveTree.frame()` joins on `node_id` instead of the `citation` string (`file:sourceline`) |
| **Risks** | Changing `EADComponent.__eq__` from structural to identity comparison may break AS tests or callers that compare two parses (`grep "== " tests/test_parsers_ead_*`). The separator character becomes part of a public contract (decision D6) |

### C3. Scope-note (`<scopecontent>`) parser and reference extractor

| | |
|---|---|
| **Source** | LotA `archival_language/ead/scopecontent.py` (344 lines): `ScopeReference(kind, raw, inventory_number, folio)` L112, extractors `_inventory_references` L126, `_folio_references` L146, `_external_references` L153, `_extract_date_strings` L180, `_segments` L214, `ScopeNote` L270, `parse_scope_notes(node, *, tags=("scopecontent",))` L291, `scope_notes_frame(tree, *, tags)` L326. The shape classifier is `ShapeRule`/`SHAPE_RULES`/`classify_shape(text, tags)` in `scopecontent_survey.py` L126–229 (14 rules plus "unclassified" = 15 shapes). CLI: `cli/parse_scopecontent.py` (162 lines) |
| **Deps** | pandas, lxml, AS `InventoryNumber` |
| **Coupling** | `SHAPE_RULES` and the reference regexes are Dutch/Nationaal Archief specific (`Exh.`, `St.-Gen.`, `Bevat`, `zie inv.nr.`, `ontbr(eek\|eken)`). Takes a LotA `ArchiveNode`; needs the `EADComponent` instead. The CLI hardcodes `DEFAULT_ARCHIVE_IDS = ("1.01.02","1.04.02")` and LotA's `output/` |
| **Known bug (unfixed)** | 83 VOC notes refer into another archive ("Aanwinsten, 1.11.01.01, inv.nr. 687") and are extracted as `inventory:687`, which would mis-join to VOC 687. `ScopeReference` has no target-archive field, and the regex ignores text before the `inv.nr.` cue |
| **Tests today** | LotA `tests/test_scopecontent.py`, 11 tests (count parity, element index, unclassified, empty, lb, inventory parse, dash range, folio, dates, picklable, frame join); 7 in `test_scopecontent_survey.py`. No cross-archive test |
| **Destination** | New AS module `parsers/ead_scope_notes.py` rather than appending to `ead_fields.py` (317 lines already). It would hold `parse_scope_notes(component, *, tags=..., shape_rules=NA_SHAPE_RULES)`, `ScopeNote`, `ScopeReference` with a new **`target_archive: Optional[str]`**, and `classify_shape`. The rule table is a parameter, with the Nationaal Archief set as the shipped default. Text comes from C1's `element_text` |
| **Sibling afterwards** | LotA: `from archival_structures.parsers.ead_scope_notes import parse_scope_notes, ScopeReference`. The CLI and survey/census functions (`survey_tree`, `census_tags_and_attributes`) stay in LotA |
| **Risks** | Fix the Aanwinsten bug in the same step, with a new test. Otherwise AS publishes a wrong join. Output text changes because of C1, so LotA's `output/scope-notes/*.tsv.gz` must be regenerated and diffed: the only expected differences are child-edge spaces and the 83 notes |

### C4. Structural `<unitdate>` excision with character spans

| | |
|---|---|
| **Source** | LotA `facets/title_norm.py` (716 lines): `_DATE_SENTINEL` L354, `_collapse_with_map` L357–371, `_unitdate_children` L374, `_structural_date_spans` L382–416, `_structural_text_and_prose` L433–466, and the regex fallback in `normalise_title` L538–552 |
| **Deps** | stdlib and lxml for the excision itself. The rest of `title_norm` needs `fuzzy_search.Tokenizer` and `formula_detection.normalise_spelling` |
| **Coupling** | `ABBREVIATION_HEADS` (Dutch); `ArchiveConfig.normalise_spelling`. Neither is needed for the excision |
| **Tests today** | LotA `test_title_norm.py` (22 tests); the date ones are `test_date_span_indexes_original`, `test_repeated_year_is_not_ambiguous`, `test_double_space_inside_unitdate_is_located`, `test_every_unitdate_child_is_excised`, `test_did_date_with_unitdate_child_still_excised` |
| **Destination** | AS `parsers/ead_fields.py`: `split_unittitle_dates(unittitle_element) -> (prose: str, date_spans: list[tuple[int,int]])`, with spans indexing into the C1-normalised title. Build it on C1's walk (replace each `<unitdate>` with a sentinel and collapse once), so the reconstruction-mismatch fallback disappears |
| **Sibling afterwards** | LotA `normalise_title` calls the AS helper and keeps abbreviation handling, spelling normalisation and facet logic. F4 mechanism (a) is no longer needed; mechanism (b), `_split_fallback`, stays as a safety net |
| **Risks** | Depends on C1. LotA's ambiguity counts (6,264 VOC dates ambiguous, 125 not locatable with `str.find`) will change, so report the new numbers instead of assuming them |

### C5. Digitisation evidence declared per archive

| | |
|---|---|
| **Source** | LotA `config.py` `ArchiveConfig.digitisation_mechanisms` L71 (rationale L82–97), with values in `config/archives.toml`: `dao` for SG/VOC, `daogrp` for BHIC/Zeeland, `assets` for Amsterdam. Also `ead/digitisation.py` (190 lines), `_has_evidence` L76 (duplicated at `corpus/coverage.py:155`) |
| **Deps** | pandas |
| **Coupling** | The per-archive values live in LotA config |
| **Tests today** | LotA `test_digitisation.py`, 11 tests (real SG/VOC); AS `test_parsers_ead_digital.py`, 15 tests |
| **Destination** | AS `parsers/ead_digital.py`: `DigitisationSummary.has_evidence(mechanisms: Collection[str]) -> bool`, next to the existing `is_digitised` (L234). `is_digitised` stays handle-inclusive, for backward compatibility, and its docstring should say that a handle is not evidence of digitisation. Optionally add a per-repository default map in `parsers/ead_survey.py`, which already knows the 20 repositories |
| **Sibling afterwards** | LotA calls `summary.has_evidence(cfg.digitisation_mechanisms)`; `branch_coverage`/`dark_branches` stay in LotA |
| **Risks** | Small. Don't change `is_digitised` semantics silently |

### C6. Segmentation evaluation suite (`evaluation.py`), merged with AS `corruption/metrics.py`

| | |
|---|---|
| **Source** | NLP `src/startpage/evaluation.py` (514 lines). Public: `build_eval_frame` L35, `page_level_prf` L73, `page_level_prf_with_counts` L87, `iter_contiguous_runs` L107, `tolerance_prf` L181 (and private `_tolerance_prf_runs` L174, used by `evaluate_all`), `default_k` L223, `pk` L273, `window_diff` L279, `segment_exact_match` L319, `PerInventoryResult` L343, `per_inventory` L346, `start_rate_calibration` L385, `bootstrap_ci` L406, `bootstrap_pooled_f1` L421, `evaluate_all` L444, `format_report` L490 |
| **Deps** | numpy, pandas, scikit-learn (`precision_recall_fscore_support`). All already in AS |
| **Coupling** | No imports from the rest of NLP. Conventions to parameterise: (1) the positive label is the string `START = "start_page"`; (2) `build_eval_frame` parses `scan_id` as `"{inventory}/{scanname}"`; (3) `full_sequences` has the shape `{inv: [(scanname, label, page_type), ...]}`; (4) callers must drop `role=context` rows first |
| **Tests today** | NLP `tests/test_evaluation.py` (268 lines, 17 tests): perfect/all-negative/all-positive, off-by-one, tolerance no double credit and no crossing inventories or runs, Pk/WindowDiff at junctions, segment exact match with truncated tail, `default_k` pooled, `per_inventory`, bootstrap, `build_eval_frame`, `evaluate_all` on degenerate inventories, calibration. One test (`test_page_level_prf_reproduces_existing_knn_numbers`, L250) needs a 60 MB `.npz` and `startpage.train`, and stays in NLP. NLP has no pytest config; tests run as scripts |
| **AS overlap** | `stream_analysis/corruption/metrics.py` (209 lines; 16 tests in `tests/test_stream_analysis_corruption_metrics.py`): `PRF1`, `true_start_indices`, `observed_start_labels`, `score_start_classification(list[bool], list[bool])`, `window_diff(list[bool], list[bool], k=None)`, `true_same_document_pairs`, `score_same_document_pairs`, `pairs_to_groups`, `grouping_ari`. AS has no Pk, tolerance F1, segment exact match, per-inventory aggregation, bootstrap or run handling. NLP has no pairwise (Framing B) or ARI metrics |
| **Destination** | New `archival_structures/stream_analysis/evaluation/` (after step 1 makes `stream_analysis` light): **`segmentation.py`** for the per-sequence metrics (`pk`, `window_diff`, `tolerance_prf`, `segment_exact_match`, `default_k`, `iter_contiguous_runs`, `page_level_prf`); **`aggregate.py`** for per-inventory, macro/worst, calibration, bootstrap, `evaluate_all`, `format_report`, `build_eval_frame`. **Merge, don't duplicate:** `corruption/metrics.window_diff` and `score_start_classification` become thin wrappers over `evaluation.segmentation`. The pairwise, ARI and provenance-aware functions stay in `corruption/metrics.py`, which is where they belong |
| **Conventions to settle** | (a) **Labels:** canonical input is a `list[bool]` "is start" (AS style). NLP adapts through `positive="start_page"`, or a converter in its shim. *Silent hazard today:* bool labels given to NLP's functions produce all-zero gaps, and string labels given to AS's are all truthy. Both should validate their input type. (b) **Default k** differs: NLP uses `round((n/S)/2)` and AS uses `round((n−1)/(2S))`, with S = number of segments (e.g. n=6, S=1 gives 3 vs 2). Choose one (decision D3). (c) **Edge cases:** NLP returns NaN for n<2 and silently clips k to the number of gaps; AS raises on n<2, on k>gaps and on length mismatch. Proposal: raise on length mismatch (always a caller bug); return NaN for n<2 (needed for per-inventory aggregation over one-page runs); clip k with a warning |
| **Sibling afterwards** | NLP `src/startpage/evaluation.py` becomes a shim: `from archival_structures.stream_analysis.evaluation import *`, plus `START` and a string-label adapter. Its 21 importers are unchanged at first. Nothing stays in NLP except the kNN-reproduction test |
| **Acceptance** | Port the 16 portable NLP tests and keep AS's 16. Add a shared fixture asserting both old implementations give the same WindowDiff for equal explicit k. NLP's `output/trivial_baseline_metrics.csv` and one stored model's `evaluate_all` output are reproduced exactly through the shim |
| **Risks** | Whichever default k is chosen, either NLP's reported Pk/WindowDiff numbers or AS's metric-test expected values (the "2/3 at default k=1" case) change. Lock the chosen behaviour in a test before touching callers |

### C7. Chunk splits: document-atomic long chunks with context, and the inventory-held-out split

| | |
|---|---|
| **Source** | NLP `src/startpage/long_subsequence_chunk_split.py` (541 lines). L1–448 are already generic: `Page(page_id, is_doc_start, is_doc_end)` L14, `Document` L21, `Chunk` L32, `SubSequence` L60, `LongSequenceSplit` L104 (`by_split`, `to_long_dataframe`), `parse_documents` L149, `partition_file_into_chunks` L177, `assign_splits_and_pad_context` L231, `build_archival_dataset_splits` L298, `build_subsequences` L337, `validate_subsequences` L373. Only the wrapper `build_long_sequence_chunk_split` L449 is VOC-specific. NLP `src/startpage/splits.py` (312 lines) has `_select_held_out` and `_build_inventory_held_out` L212 (the inventory-held-out split), the validators `_check_duplicate_page_ids`, `_assert_no_document_split_across_splits` and `_validate_split`, `load_or_build_split(kind, seed, force, **params)` L231 (CSV plus JSON manifest cache) and `iter_sequences` L292. Legacy `src/startpage/chunk_split.py` (146 lines) |
| **Deps** | stdlib; pandas (lazy) |
| **Coupling** | (1) `load_top_of_page_scannames()` reads the fixed `output/start_page_position.csv` and is called *inside* `build_chunk_split`, the long-chunk wrapper and `_assert_no_document_split_across_splits`. Only starts at the top of a page may be cut points (NLP trap 4); a mid-page start shares its page with the previous document. That set must be passed in. (2) The wrapper derives page ids by parsing the VOC scan-name suffix (`_`-split, 4-digit zero pad), and raises `KeyError` for an inventory with no top-of-page start. (3) `splits.py` has VOC constants `DEFAULT_HELD_OUT={"8834","3891","1547","2775","8121"}` and `KNOWN_SPLIT_COVERAGE_GAPS` (inventory 8023 gets no test pages), plus `SPLITS_DIR`, chmod and git-HEAD side effects. (4) Library functions call `print`. (5) `chunk_split` uses one `random.Random` across inventories, so the result depends on dict iteration order. (6) `test_ratio` is accepted but unused |
| **Tests today** | **None** for any split code |
| **Destination** | `archival_structures/stream_analysis/evaluation/splits.py`: the generic L1–448 (prints become `logging`), `build_inventory_held_out(sequences, held_out: Collection[str] \| None, seed, n_held_out=5)`, and validators made public (`check_no_document_crosses_partitions`, `check_no_duplicate_page_ids`, `check_context_not_scored`). Also a **`check_nested_split(component_split, downstream_split)`** that asserts the leakage rule "a component model's training split must nest inside the downstream split". NLP's `CLAUDE.md` states that rule but does not check it in code |
| **Sibling afterwards** | NLP keeps `splits.load_or_build_split` (cache, manifest, VOC held-out set, known gaps, top-of-page CSV) as a thin layer that calls AS. `chunk_split.py` stays in NLP as legacy code (its numbers are flagged "do not cite") |
| **Acceptance** | New tests: synthetic files where documents tile exactly; an oversized document becomes its own chunk; context stays in bounds and is never scored; held-out inventories are whole; a duplicate page id across partitions raises; nesting check. **NLP's cached split CSVs (`output/splits/`: long_chunk K=12 W=3, inventory_held_out seeds 0–3) are rebuilt byte-identical** through the AS code, otherwise every reported number moves |
| **Risks** | The 10-slot striding and the greedy rebalance have odd edge behaviour (small files get no test pages). Keep it as is for reproducibility, document it, and offer a corrected strategy only as an opt-in |

### C8. Trivial baselines

| | |
|---|---|
| **Source** | NLP `scripts/run_trivial_baselines.py` (203 lines). All logic is inside the script: `predictions_majority`, `predictions_from_period_fn(full_sequences, period_fn)` (every-k, corpus rate or the per-inventory oracle rate), `predictions_length_prior_hsmm(full_sequences, mean_segment_length, seed)` (geometric segment lengths, seeds 42–44), plus the helpers `train_start_rate`, `core_rows`, `label_lookup`, `predict_for_scan_ids`, `flatten_metrics`, `score`. `scripts/baseline_reference_table.py` (230 lines) duplicates some helpers and runs kNN/LogReg; that is research code |
| **Deps** | numpy |
| **Coupling** | NLP string labels, `full_sequences` shape, `splits.load_or_build_split`, `config.OUTPUT_DIR` |
| **Tests today** | none |
| **Destination** | `archival_structures/stream_analysis/evaluation/baselines.py`: `majority(n)`, `every_k(n, k, offset=0)`, `length_prior(n, mean_length, rng)`, and `baseline_suite(sequences, train_start_rate, seeds)`, returning boolean predictions per sequence. Also a helper so that *every* AS harness result (the corruption simulation) reports these floors |
| **Sibling afterwards** | NLP's script keeps its CLI and its split choice, and calls AS. The oracle variant (`every_kth_own_rate_ORACLE`) is labelled clearly in AS or stays in NLP |
| **Acceptance** | Unit tests on synthetic sequences; NLP reproduces majority Pk/WindowDiff 0.303 and the whole `trivial_baseline_metrics.csv` |
| **Risks** | Depends on C6 (metric conventions). The length prior needs seed-for-seed equality with `np.random.default_rng` |

### C9. Sequence decoders: semi-Markov, CRF, BiLSTM

| | |
|---|---|
| **Source** | NLP `src/startpage/semi_markov.py` (102 lines): `fit_lognormal_duration` L27, `_lognormal_cdf` L36, `log_duration_pmf(mu, sigma, max_duration)` L45, `decode_semi_markov(start_scores, log_pmf, max_duration)` L58 (O(T·D) dynamic programming). `src/startpage/crf_sequence.py` (106 lines): `LinearChainCRF(nn.Module)` L32 with `emission_scores`, `_partition`, `_constrained_partition` (partial labels via a mask), `nll`, `viterbi`, `argmax_emissions`; module constants `START, INSIDE, END = 0,1,2` that the class doesn't use. `src/startpage/bilstm_sequence.py` (32 lines): `BiLSTMTagger`. Generic glue in scripts: `start_score_from_emissions(model, features)` in `scripts/train_semi_markov_duration.py` L98, and the generalised training loop `train_crf(..., n_states, start_states, seed)` L191, `collapsed_f1` L173 and `decode_sequences` L225 in `scripts/train_multitask_crf.py` (434 lines). That generalised version should be the basis, not the older one in `train_crf_sequence.py` |
| **Deps** | semi-Markov: numpy plus scipy (one `norm.cdf`; replace it with `math.erf` or declare scipy). CRF/BiLSTM: **torch** (already an AS dependency; should become the `[torch]` extra). Not used anywhere: sklearn-crfsuite, pytorch-crf |
| **Coupling** | Practically none in `src/`. Scripts are tied to NLP feature tables, `OUTPUT_DIR` CSVs, `"start_page"` strings and `"{inv}/{pid}"` ids |
| **Tests today** | `tests/test_semi_markov.py` (48 lines, 4 tests: MLE recovery, pmf normalises, clean decode, prior pulls lengths). None for the CRF or BiLSTM |
| **Destination** | `archival_structures/stream_analysis/decoding/`: `semi_markov.py` (numpy only), `crf.py` (torch, imported lazily; `LinearChainCRF` plus `train_crf` with early stopping), `bilstm.py`. They sit next to `stream_analysis/sequence_analysis.py`, which has transition matrices and run-length encoding but no decoder. `start_score_from_emissions` goes with `semi_markov`. Optional improvements, as separate later steps: end scores and a batched forward pass |
| **Sibling afterwards** | NLP shims for the three `src` modules. All `train_*` scripts, feature construction (`build_feature_matrix`, `fit_scaler_train_only`, `SCALAR_FEATURES`) and state configs (`CONFIGS`) stay in NLP |
| **Acceptance** | Port the 4 semi-Markov tests and add edge cases (T=1, `max_duration < T`). New CRF tests: Viterbi equals brute force over all paths for T≤5, `_partition` equals brute-force log-sum-exp, the constrained partition with a full mask equals the gold path score. NLP reruns `train_semi_markov_duration.py` and one CRF config with the same seeds and gets the same metrics (on CPU; GPU nondeterminism is a caveat) |
| **Risks** | Low. The review's finding that the choice of decoder matters little (0.78–0.80 F1, inside a ±0.14 CI) argues for moving these as utilities, not as a headline feature |

### C10. Unsupervised segmentation (DP, TextTiling) — optional

| | |
|---|---|
| **Source** | NLP `src/startpage/unsupervised_segmentation.py` (327 lines): `segment_dp(similarities, prior_mean_length, length_penalty_weight, max_segment_length)` L92, `_geometric_length_penalty` L86, `calibrate_length_penalty_weight` L167, `textiling_depth_scores(similarities, window, depth_threshold)` L201, `ChangePointResult` L252, `predicted_start_rate` L284. `build_similarity_sequences`, `boundaries_to_page_results` and `run_unsupervised_segmentation` are NLP-coupled |
| **Deps** | numpy, pandas |
| **Tests today** | none |
| **Destination** | Extra methods in AS `stream_analysis/overview/subsequence_detection.py`, which already has `compute_adjacent_similarities`, `detect_boundaries_threshold` and `detect_boundaries_changepoint` and uses the same "list of boundary indices" convention |
| **Sibling afterwards** | NLP imports `segment_dp`/`textiling_depth_scores` from AS |
| **Risks** | NLP found both score *below* the majority baseline for document starts. Document them as section-level tools, consistent with the granularity rule in the review §1.3 |
| **Done (phase 11, 2026-10-27)** | Ported as step 18b. Two corrections to this row: the segmenters return segment *start* indices (incl. 0), not the module's gap indices (converters `starts_to_boundaries`/`boundaries_to_starts` added); and the granularity rule is a hypothesis to test (review §0 decision 9), documented in `docs/challenges/document_segmentation.md` |

### C11. Foliation / page-number alignment with the GLOBALISE index

| | |
|---|---|
| **Source** | NLP `src/startpage/globalise_alignment.py` (242 lines). Generic core: `lis_indices(values)` L79 (patience sort, O(n log n)), `main_chain(anchors)` L95, `extra_runs(anchors, keep, min_len=3, max_rounds=4)` L102 (numbering restarts), `numbering_mode(chain)` L117 (median Δscan/Δpage: ≥1.5 foliation, ≥0.75 pagination, else spread), `_interp(f, run, extrap, max_scan)` L130 (folio → scan), `_band(dist, inside)` L144 (`BANDS=(3,10)`: high / medium / low confidence by distance to nearest anchor; extrapolated is always low), `_locate` L150, `Config(mode, runs, extrap, dup_first)` L40, `align_inventory(docs, anchors, cfg, max_scan)` L170, `align_all(index_df, anchors_by_inv, cfg, max_scans)` L203. Loaders: `load_index` L52, `load_anchors` L62, `load_spine` L71. Validation: `load_gold_starts` L213, `score_start_accuracy` L229, `accuracy_summary` L240. Anchor mining: `scripts/mine_catchword_pagenum_linespacing.py` (224 lines) takes regions with Laypa tag `page-number`, text matching `^\d+$`, last one per page, and writes `output/pagenumber_sequence_features.csv`. Protocol: `scripts/build_globalise_alignment.py` (244 lines) |
| **Deps** | numpy, pandas; difflib (stdlib) for catchwords |
| **Coupling** | `from .config import OUTPUT_DIR`. The hardcoded `INDEX_PATH` is the 117 MB GLOBALISE CSV in `NLP/metadata/digitised_indexes/`. `INDEX_COLS` maps GLOBALISE column names (`INVENTORY NUMBER`, `FOLIONUMBER (START OF DOCUMENT)`, …). `scan_no()` assumes the VOC file-name grammar. `load_index` turns non-numeric folios into NaN (`method="no_folio"`). Anchor mining only covers the 504 local PageXML zips and rejects Roman numerals and `12v` |
| **Tests today** | none |
| **Destination** | AS **`analysis/page_numbering.py`**, the module the review proposes. It holds the generic core, with public names `longest_increasing_chain`, `numbering_restarts`, `numbering_mode`, `interpolate_folio_to_scan`, `confidence_band`, `align_documents(docs[folio_start, folio_end], anchors[scan_no, page_number], cfg)`. **Anchor extraction from a region table** goes there too: `page_number_anchors(regions_df, tag="page-number", pattern=r"^\d+$", scan_no=...)`. That works directly on the shared text layer's `regions` files (`region_tag=="page-number"`), so anchors become available for all 6,914 inventories without re-parsing PageXML. Catchword-chain and transition classification (ok/skip/reset) are useful as an ordering-principle detector and could follow in the same module. **GLOBALISE index loader:** `datasets/globalise_index.py` with `load_globalise_index(path, inventories=None)` and an overridable column map. The path is always passed in; the data stays where it is (decision D8) |
| **Sibling afterwards** | NLP: `globalise_alignment.py` becomes a shim plus its gold loaders and scorers (NLP annotation CSVs); `build_globalise_alignment.py` (report set, dev-rule refinement, silver export) stays NLP research code. AS gains the "external ordering signal" that `page_order_problems.md` asks for |
| **Acceptance** | New tests on synthetic anchors: LIS with noise, restart detection, foliation vs pagination slopes, interpolation and clamping, band thresholds. NLP's `build_globalise_alignment.py` contains a hard assertion that the baseline reproduces (exact 0.615, ±1 0.873, ±2 0.928); it must still pass when run on AS code, and `document_alignment.csv` must be identical |
| **Risks** | The scan-number convention (use C13). `dtype=str` and the numeric coercion are VOC-index behaviour; keep them in the loader, not in the core |

### C12. Separator-sheet rule — optional, low priority

| | |
|---|---|
| **Source** | NLP `src/startpage/separator_sheets.py` (64 lines): `is_separator_sheet(text)`, `detect_sheets(df)`, `apply_rule(df, sheets, convention, threshold)` |
| **Deps** | re, numpy, pandas |
| **Coupling** | Dutch/VOC regex ("No: 7.", "Nr 63"), 4-digit scan-id convention |
| **Tests today** | none |
| **Destination** | Later, alongside C11 as a page-type cue, with the regex as a parameter. Not part of the first wave |

### C13. National Archives scan-id helpers (new, consolidating four copies)

| | |
|---|---|
| **Source** | LotA `ead/adapters.py` `_BaseAdapter.scan_id` L55 (`f"{ead_id}_{inv}_{n:04d}"`) and `parse_scan_id` L58–70 (returns `InventoryNumber`); NLP `globalise_alignment.scan_no` L47, `separator_sheets._scan_no` L41, `voc_text_layer.inventory_sort_key` L134 and the `f"_{inv}_"` check L314, `layout_features._inventory_number_sort_key` L263. LotA `corpus/tsv_source.parse_doc_id` L51 parses pagexml-tools' `{scan_id}-text_region-x-y-w-h` doc ids |
| **Deps** | stdlib |
| **Destination** | AS `model/scan_id.py`: `ScanId(archive, inventory: InventoryNumber, number: int, ext)` with `parse(scan_id, *, digits=4)` / `format()`, plus `parse_doc_id`. Archive prefix and digit count are parameters, with the NA grammar as the default |
| **Sibling afterwards** | LotA adapters call it (SG/VOC adapter classes, `pages_per_scan` and staging stay in LotA); NLP's text layer, alignment and separator code call it |
| **Tests** | Port LotA `test_adapters.py`'s grammar cases (11 tests, currently against real config) as synthetic ones; add suffixed inventories (`4848A`, `5745A/B`, `9626A`) |

### C14. Per-inventory word-break detector

| | |
|---|---|
| **Source** | Stage 2 of NLP `src/startpage/voc_text_layer.py`: `WBC_RULE` L66–96 (`min_end_attached=30`, `min_join_vocab_frac=0.25`, `min_end_rel_frac=5.0`, `min_inventory_lines=2000`, `corpus_default_min_share=0.5`, each with a stated reason), `SANITY_IN=["¬"]`, `SANITY_OUT=[":","."]` L98–99, `is_candidate_char` L385, `char_label` L389, `mid_line_vocabulary` L393, `ends_attached` L406, `strip_leading_marks` L411, `joined_word` L421, `first_non_wbc` L431, `inventory_wbc_stats(inv, out_root)` L436, `WBC_STATS_COLUMNS` L512, `wbc_stats_part` L525, `pooled_stats` L534, `rule_passes` L558, `decide_wbc(stats, inventory_lines, rule)` L564, `load_wbc_sets` L637. The method: for each candidate line-final character, measure how often it is attached to a word end and whether removing it and joining the two fragments yields a word attested mid-line in the same inventory. Then decide per inventory whether it is a word break, with a corpus default for small inventories |
| **Deps** | pagexml-tools (`LineCharAnalyser`); pandas |
| **Coupling** | `inventory_wbc_stats` reads the layer's `lines/<inv>.lines.tsv.gz` by path, so it should take an iterable of line strings. Thresholds were calibrated on VOC but are already parameters |
| **Tests today** | none (the 29 text-layer tests cover the join, not the detection) |
| **Destination** | AS **`text_layer/word_breaks.py`** (new subpackage, see C15): `word_break_stats(line_texts) -> DataFrame`, `decide_word_breaks(stats, n_lines, rule=DEFAULT_RULE)`, `WordBreakRule` dataclass |
| **Sibling afterwards** | NLP's build script calls AS. **LotA and SG:** this is the tool that should settle the open `„` question (review §7.5), measured per archive and separately at line ends and region ends. The second measurement needs a small extension: `position="line_end" \| "region_end"` |
| **Acceptance** | Synthetic tests: `¬` detected; `.`/`:` rejected; small inventory falls back to the default. Recomputing `decide_wbc` from the existing `wbc/wbc_stats.tsv` reproduces `wbc/wbc_per_inventory.tsv` and `wbc/corpus_default.json` exactly (default set `{¬, „}`; 1,461 of 6,914 inventories deviate) |
| **Risks** | `LineCharAnalyser` lives in the modified pagexml-tools checkout (§1.5); confirm the released version gives the same counts |

### C15. Text-layer producer (the tooling behind `voc_text_layer_v1`)

| | |
|---|---|
| **What exists** | `NLP/data/voc_text_layer_v1/`: a real directory (no symlinks) with `lines/` (6,914 files, 7.8 GB), `regions/` (3.9 GB), `regions_meta/`, `extract_stats/`, `join_stats/`, `wbc/`, `status.tsv`, `progress.log` and `manifest.json`. All three stages finished on 2026-10-08 with `failed: []`; the input was the GLOBALISE PageXML v2.1 on an external drive. Columns: lines `scan_id, region_id, line_idx, line_id, line_text`; regions `scan_id, region_id, region_box, region_tag, region_text`; regions_meta `… n_lines`. **No line geometry and no page dimensions**; rows are in document order, not reading order. LotA reaches it through the symlink `LotA/data/voc_text_layer_v1 → ../../nlp-start-page-detection/data/voc_text_layer_v1`, but no LotA code reads it yet (planned in LotA Phase 7; consumer spec in `LotA/plan/CONTEXT.md` §7.4.1) |
| **Source** | NLP `src/startpage/voc_text_layer.py` (879 lines; stdlib at top, with pagexml and pandas imported lazily, and no `startpage.config` import). I/O: `sanitise` L140, `format_box` L153, `write_tsv_gz` L166 (deterministic gzip with mtime 0, atomic replace), `read_tsv_gz` L185, `write_json_atomic` L204, `write_plain_tsv` L208, `discover_inventories` L222, `input_bytes` L240, `iter_pagexml` L254. Extract: `iter_text_regions` L273, `region_tag_of` L281, `extract_inventory` L293 (`pagexml.parser.parse_pagexml_file`). Word breaks: C14. Join: `library_join` L654–666 (calls pagexml `make_text_region_text`), `SPEC_GUARDS`/`ALL_GUARDS` L675–676, `classify_boundary` L679, `join_region` L713, `join_vocabulary` L787, `join_inventory` L801. Throughput model L853–879. Driver: `scripts/build_voc_text_layer.py` (427 lines): argparse, `class Run` L108 (append-only `status.tsv`, `progress.log`, `update_manifest` L141 with git HEAD and `pagexml_tools_info()`), `run_pool` L190 (`ProcessPoolExecutor`, largest first, resumable per stage). Validation: `scripts/validate_voc_text_layer.py` (263 lines; compares against NLP's older `region_paragraphs.tsv.gz`) |
| **Deps** | pagexml-tools, pandas; stdlib multiprocessing/gzip |
| **Coupling** | `LAYER_NAME="voc_text_layer_v1"`; `UNTAGGED_TAG="unclassified"`; the `_{inv}_` scan-id check; throughput constants fitted on one laptop; the driver's only NLP tie is the `sys.path` insert. Inventory discovery assumes `<inv>.zip` or `<inv>/` |
| **Tests today** | `tests/test_voc_text_layer.py` (249 lines, 29 tests): join guards, both-ends fix, empty and single-line regions, hyphen sets, sanitise, TSV round trip and determinism, `format_box`, suffixed inventory sorting and discovery, pagexml-tools version. Not covered: `extract_inventory` (no PageXML fixture), WBC stages, `join_inventory` |
| **Destination** | New AS subpackage **`archival_structures/text_layer/`**: `io.py` (TSV/JSON I/O, sanitise, box format, discovery); `extract.py`; `word_breaks.py` (C14); `join.py`; `provenance.py` (`pagexml_tools_info`, a generalised `package_info(name)`, the manifest writer, also usable by the siblings for §2's provenance records); `build.py` (the `Run`/status/pool driver as a library), plus a console entry point `as-build-text-layer` in `pyproject.toml`. Layer name, untagged tag and scan-id grammar (C13) are parameters, with the VOC values as a named preset. The CLI keeps the same flags |
| **Sibling afterwards** | NLP's `voc_text_layer.py` becomes a shim and `build_voc_text_layer.py` a two-line wrapper (or is retired in favour of the entry point); `validate_voc_text_layer.py` stays NLP research code. LotA's future `ParagraphSource` for the layer reads with AS `text_layer.io.read_tsv_gz`, so `QUOTE_NONE` and the sanitisation rules have a single owner. LotA's `corpus/derive_voc.py` (369 lines, its own PageXML-to-paragraph pipeline, superseded by its D2 amendment) can be retired rather than moved |
| **Acceptance** | Port the 29 tests and add an extract test on a small committed PageXML fixture (one scan with nested regions, an empty region and a region without coordinates). Then **rebuild a sample of inventories from local PageXML** (NLP's local zips, not the external drive) and require byte-identical `lines/`, `regions_meta/` and `regions/` files against v1. This depends on §1.5: the pagexml-tools changes must be released first. Same `manifest.json` structure |
| **Later, separate decisions** | (a) **v2 with line geometry** (line box or baseline) and page width and height. This is what would make AS's `text_extent`, `empty_regions` and `opening_detection` usable downstream, resolving the tension around LotA's D1 rule (decision D4). (b) **SG preset:** the same producer replaces AS's root-level `make_iventory_paragraphs.py` (219 lines), with its defects listed in the review §4.1: hardcoded `T7_Data` paths, SG-only regex, dropped empty scans, collapsed suffixed inventories, line geometry discarded, fixed `„` word break. (c) An explicit empty-scan row (5.08% of SG scans have no text regions) |
| **Risks** | Moving the producer must not change v1. The determinism of `write_tsv_gz` makes a byte comparison possible, so use it. Don't fold v2 changes into the move |

### C16. Region-quality filter (usable / fragment / noise)

| | |
|---|---|
| **Source** | LotA `archival_language/corpus/quality.py` (436 lines): `QUALITY_CLASSES` L32, `DEFAULT_KEEP_CLASSES` L42, `is_kept` L45, `THRESHOLDS` L60–103, `QualityVerdict` L106, `_find_model_file` L114, `load_lexicon(*, config)` L131, `region_features(region, *, lexicon)` L173, `classify_features(features, *, region_id)` L246–321 (a rule cascade starting at 0.5: lexicon-hit bonuses, penalties for singleton ratio, non-alphabetic runs, low alphabetic ratio unless `numeric_content_exempt` L285–301, and ≤2 tokens; cut-offs noise <0.35, fragment <0.50), `score_region` L324, `quality_frame` L348, `inventory_quality` L369 (thread pool) |
| **Deps** | pandas; **gensim**, lazily, only in `load_lexicon` (fastText vocabulary). Not in AS |
| **Coupling** | `load_lexicon` reads LotA `config.resources["fasttext_dir"]` and caches to LotA `output/lexicon/`. LotA's `Region`/`BBox` models; AS `InventoryNumber` |
| **Validation** | Keep P/R 0.97/0.97 on 75 held-out rows of `LotA/gold/02-region-quality-done.tsv` (`report/05`). After the retune, accuracy 0.893 on 150 held-out rows of that sheet plus `02c-short-region-quality-done.tsv` (`report/09`) |
| **Tests today** | `tests/test_corpus_quality.py`, 15 synthetic tests (immutability, monotone, exemption cases, lexicon cache with gensim mocked, seed reproducibility) |
| **Destination** | AS **`analysis/region_quality.py`**: `region_features(text, *, lexicon: AbstractSet[str], bbox=None)`, `classify_features`, `score_region`, `quality_frame`, `QualityThresholds` dataclass (the current values as defaults). The lexicon is **injected** as a set of words; AS's existing `utils/wordlist.read_wordlist` can supply one, so no gensim is needed. It sits next to `text_analysis`/`token_analysis`, whose running-text checks are unvalidated and could later be scored against the same gold |
| **Sibling afterwards** | LotA keeps `load_lexicon` (gensim, config, cache) and its `Region` model, and calls AS with `region.text`. Gold sheets and sampling stay in LotA |
| **Acceptance** | Port the 15 tests (the lexicon-cache test stays in LotA). LotA re-scores both gold sheets through AS and reproduces the `report/05` and `report/09` numbers exactly |
| **Risks** | The thresholds are tuned on SG/VOC Dutch; document them as such. Decide whether a copy of the gold rows (a few hundred short texts) becomes an AS test fixture (decision D8) |

### C17. MinHash duplicate / re-shoot detector

| | |
|---|---|
| **Source** | LotA `archival_language/corpus/integrity.py` (554 lines). Constants L58–72: `_SHINGLE_LENGTH=5` (characters), `_MIN_SHINGLES_FOR_COMPARISON=100`, `_NUM_MINHASHES=128`, 16 bands × 8 rows, prime 2³¹−1, `_MINHASH_SEED=20261004`, `DEFAULT_DUPLICATE_THRESHOLD=0.6`. Functions: `_shingles` L403, `_minhash_coefficients` L418, `_minhash_sketch` L428 (crc32 plus vectorised universal hash), `_lsh_candidates` L442, `duplicate_candidates(source, inv, *, adapter, threshold)` L459–554. **Hand-rolled; datasketch is not used.** Candidates are LSH collisions **plus all adjacent-scan pairs**, confirmed by exact Jaccard ≥ threshold; the output reports `jaccard_estimated`, `jaccard_exact` and `adjacent`. (The review's "restricting to adjacent scans removes 96% of false positives" describes a filter on that output, not the candidate generation) |
| **Deps** | numpy, pandas, zlib |
| **Coupling** | Takes a LotA `ParagraphSource` and `ArchiveAdapter`. `_malformed_count_for_inventory` L329 reads the private `source._files` |
| **Tests today** | `tests/test_corpus_integrity.py`, 16 synthetic tests; the duplicate ones are "re-shoot found" and "near-empty excluded" |
| **Destination** | AS **`analysis/duplicate_pages.py`**: `duplicate_page_candidates(page_texts: Sequence[tuple[page_key, text]], *, adjacency=True, threshold=0.6, shingle_length=5, num_hashes=128, bands=16, seed=...)`. The input is plain ordered page texts, so no source or adapter. **Paired with a new corruption operator** `page_duplication` (and possibly `page_drop`) in `stream_analysis/corruption/operators.py` (review §3.4); that is new work, a step of its own |
| **Sibling afterwards** | LotA builds the page-text list from its source and calls AS; the `cli/corpus_integrity.py` reporting stays in LotA |
| **Acceptance** | Port the duplicate tests. LotA reproduces 13 of 13 documented SG re-shoots and the same candidate table on its integrity run |
| **Not moved yet** | `page_units` (the bounding-box page splitter, same file L308) and `corpus/layout_type.py` (122 lines). They overlap with AS `opening_detection`, and the review (§6) recommends AS's width-based `find_width_groups`/`classify_page_types` instead, which is better validated. Revisit once AS has region-bbox-only entry points |

### C18. Items in the review that are not code moves (for completeness)

| Item | Where it goes in AS | Note |
|---|---|---|
| Leakage and split rules (NLP `CLAUDE.md`) | `docs/challenges/` as standing methodology; C7 adds `check_nested_split` | docs only |
| `provenance` and `confidence` per element in `annotation/ground_truth.py` | `annotation/ground_truth.py` | new; needed by the converters |
| Ground-truth converters (NLP's 22 annotation CSVs → `document` layer; GLOBALISE plus C11 → silver layer; *Generale Missiven*; SG sessions/resolutions) | `annotation/converters/` | new. The NLP CSV reader (`labels.process_labels`, `annotation_io`) is the reference; NLP keeps its own copy for its experiments |
| `segment_dp`, TextTiling | C10 | optional |
| NLP layout heuristics (`layout_features.compute_region_gap_features`, `flag_table_like_regions`, `extract_region_features`, zip/dir page iterator) | check overlap with `analysis/empty_regions`, `text_extent` and `region_calculus` first | the review §5.3 says AS's versions are better; not moved now |
| Region-bbox-only entry points for AS layout tools | `analysis/empty_regions`, `opening_detection` | new; alternative to text-layer v2 (decision D4) |

---

## 4. Proposed order of work

Each step is small enough to review on its own, leaves all three projects working, and ends
with an AS version bump and tag. **"Acceptance"** is the check that must pass before the next
step starts.

| # | What moves / changes | Tests to port or write | Acceptance check | Sibling import change |
|---|---|---|---|---|
| 0 | **Baseline.** The user reviews and commits AS's untracked parsers, corruption, boundary-annotation and test files; tag `v0.3.1` | none new | `git status` in AS is clean for `archival_structures/` and `tests/`; full AS test suite passes | LotA and NLP record the AS version and HEAD in their environment test and manifests |
| 1 | **Packaging.** Lazy `stream_analysis/__init__.py` (PEP 562); split dependencies into core plus `[torch]`, `[vision]`, `[notebook]`, `[llm]`; relax lower bounds (`scikit-learn>=1.8`, `torch>=2.11`); test pandas 3 | a test that imports `archival_structures.parsers.*`, `model.*` and `stream_analysis.corruption` in a fresh venv with core dependencies only | that test passes; the old `from archival_structures.stream_analysis import X` names all still resolve | LotA drops `--no-deps` for AS. NLP installs AS editable into its own venv instead of via Homebrew site-packages |
| 2 | **C1** `element_text` fix in `_text_of` and `ead_fields` L278. **Same step, LotA side:** `_structural_date_spans` uses the lb-aware walk; `test_lb_adjusted_flag` updated | 6 new AS tests (C1 list) | AS: the 9 glued SG titles are fixed and no other title in the sample corpus changes. LotA: test suite green; rerunning `normalise_titles` shows `date_locator` structural for every title that has both `<lb/>` and `<unitdate>`, and differences only in `lb_adjusted` | LotA imports `element_text` from AS and deletes its copy; regenerate scope-note and title outputs |
| 3 | **C2** node id, identity-hashable `EADComponent`, `node_id` column in `ead_views` | port the config-free parts of LotA `test_hierarchy.py` onto AS `ead_samples/` | AS tests green; LotA `ArchiveTree.frame()` joins on `node_id` and still asserts 1:1 | LotA `hierarchy._node_id` → AS |
| 4 | **C5** `DigitisationSummary.has_evidence` | 3–4 new tests | LotA `test_digitisation.py` unchanged results | LotA `digitisation._has_evidence` and `coverage._has_evidence` → AS |
| 5 | **C3** scope-note parser into `parsers/ead_scope_notes.py`, **with the Aanwinsten fix** (`target_archive`) | port 11 + 7 LotA tests (synthetic where possible); new cross-archive test | LotA's regenerated `scope-notes/*.tsv.gz` differ from the old ones only in the 83 Aanwinsten notes and in child-edge spaces | LotA `ead/scopecontent.py` → AS import; the CLI and survey stay |
| 6 | **C4** `split_unittitle_dates` | port the 5 LotA date tests | LotA `title_norm` output identical apart from the ambiguity counts, which are reported | LotA `title_norm` mechanism (a) → AS call |
| 7 | **C6** evaluation suite plus merge with `corruption/metrics.py`; default-k decision applied | port 16 NLP tests; keep AS's 16; shared WindowDiff fixture | NLP `trivial_baseline_metrics.csv` and one stored `evaluate_all` output identical through the shim | NLP `src/startpage/evaluation.py` → shim |
| 8 | **C7** splits (generic long-chunk, held-out builder, validators, nesting check) | new tests (C7 list); none exist today | NLP's cached split CSVs rebuilt byte-identical | NLP `long_subsequence_chunk_split.py` → shim; `splits.py` keeps its cache layer and calls AS |
| 9 | **C8** trivial baselines; the corruption harness reports them | new unit tests | majority Pk/WindowDiff 0.303 reproduced; harness reports carry floors | NLP `run_trivial_baselines.py` calls AS |
| 10 | **C9** decoders (`semi_markov`, then `crf` and `bilstm` behind `[torch]`) | port 4 + edge cases; new brute-force CRF tests | NLP semi-Markov and one CRF config reproduce on CPU with fixed seeds | NLP `semi_markov.py`, `crf_sequence.py`, `bilstm_sequence.py` → shims |
| 11 | **C13** scan-id helpers | synthetic grammar tests incl. suffixed inventories | LotA `test_adapters.py` green with adapters delegating to AS | LotA adapters; NLP `scan_no`/`_scan_no`/sort keys |
| 12 | **C11** page-numbering core, `page_number_anchors` from the region table, GLOBALISE loader | new synthetic tests | NLP `build_globalise_alignment.py` built-in assertion passes on AS code; `document_alignment.csv` identical | NLP `globalise_alignment.py` → shim (gold loaders stay) |
| 13 | **pagexml-tools:** release the 4 locally modified files (outside AS, prerequisite for 14–15) | pagexml-tools' own tests | a released version gives identical join output on a sample | none |
| 14 | **C14** word-break detector | new synthetic tests | recomputed `wbc_per_inventory.tsv` and `corpus_default.json` identical from `wbc_stats.tsv` | NLP build script calls AS |
| 15 | **C15** text-layer producer (`text_layer/` + entry point) | port 29 tests; new PageXML-fixture extract test | sample rebuild from local PageXML is byte-identical to v1 | NLP `voc_text_layer.py` → shim; LotA's planned `ParagraphSource` reads through AS `text_layer.io` |
| 16 | **C16** region-quality filter (lexicon injected) | port 14 of 15 tests | LotA reproduces the `report/05` and `report/09` figures | LotA `corpus/quality.py` keeps `load_lexicon`, calls AS |
| 17 | **C17** duplicate detector; then a separate step adds the `page_duplication` corruption operator | port the duplicate tests; operator tests next to `test_stream_analysis_corruption.py` | LotA 13/13 SG re-shoots and identical candidate table | LotA `corpus/integrity.duplicate_candidates` → AS |
| 18 | Optional: **C10**, **C12**; docs (C18 leakage rules, granularity rule) | new tests for DP/TextTiling | — | NLP `unsupervised_segmentation.py` → shim |
| later | Text-layer v2 (line geometry, page size, empty-scan rows) and the SG preset that retires `make_iventory_paragraphs.py`; ground-truth converters and the provenance field | — | — | — |

Steps 2–6 (EAD work, mostly for LotA) and 7–12 (evaluation and sequence work, mostly for NLP)
are independent of each other and can run in parallel. Within each group, the order matters
where noted: 4–6 follow 2; 8 and 9 follow 7. Steps 14–15 need step 13.

---

## 5. Decisions for the user

1. **D1 — Committing AS.** Steps 0 onward need AS's untracked EAD, corruption and test files
   committed and tagged. Will you commit them yourself, as in the siblings' "never `git commit`"
   rule, or delegate it? Pinning a sibling to a tag (§2) is impossible until then.
2. **D2 — Packaging split.** Do you accept a light core plus extras, and relaxing AS's
   `scikit-learn`/`torch` lower bounds to what NLP actually runs (1.8 / 2.11)? Do you want NLP
   to get a minimal `pyproject.toml`, which would also remove 100+ `sys.path` inserts, or only
   an editable AS install in its venv?
3. **D3 — WindowDiff/Pk default k.** Use NLP's `round((n/S)/2)` (keeps NLP's published numbers)
   or AS's `round((n−1)/(2S))` (keeps AS's tests)? Also confirm: NaN for n<2, raise on length
   mismatch, and clip k with a warning.
4. **D4 — Text layer v2.** Add per-line geometry and page size to the shared layer, so AS's
   layout tools work downstream? Or build region-bbox-only entry points in AS instead? Should
   the same producer get an SG preset that replaces `make_iventory_paragraphs.py`?
5. **D5 — Text-layer ownership.** Once the producer is in AS, where does the layer *data* live,
   and who rebuilds it? It stays in NLP's `data/` today, with a LotA symlink.
6. **D6 — Node-id format.** Keep `archive:1.2.3` (colon separator, archive id as given), and
   accept identity equality for `EADComponent`?
7. **D7 — Aanwinsten references.** Should cross-archive scope-note references carry the target
   archive code, e.g. `1.11.01.01`, and be excluded from VOC joins? That is the proposed fix.
8. **D8 — Data in AS.** May small gold extracts (region-quality rows) and a small PageXML
   fixture be copied into AS `tests/`? The GLOBALISE index (117 MB) should stay in NLP and be
   passed by path.
9. **D9 — Oracle baseline.** Ship NLP's per-inventory "own rate" every-k baseline in AS, clearly
   labelled as an oracle, or keep it in NLP?

## 6. User decisions (2026-10-14)

1. **D1.** pagexml-tools is at 0.8.1, which is enough for now; a newer release is pending a
   PR. The user commits the reviewed AS changes and makes a new AS release, which unblocks
   pinning the siblings to a tag.
2. **D2.** Yes to a light core plus optional extras, and to loosening the minimum versions.
   **NLP gets a `pyproject.toml`.**
3. **D3.** NLP is the more specific project, so it **overrides** AS's generic defaults. AS keeps
   its own default `k = round((n−1)/(2S))`, and NLP passes its `round((n/S)/2)` explicitly, so
   NLP's published numbers stay reproducible. The edge cases proposed in §5 (NaN for n<2, raise
   on length mismatch, clip k with a warning) are adopted as AS defaults, since nobody objected.
4. **D4.** Text layer v2 adds **per-line geometry**, and **an SG preset replaces
   `make_iventory_paragraphs.py`**. A full SG run (9,122 inventories) cannot be staged on the
   local disk, so the user runs it on an external disk. The preset must therefore be
   handoff-ready: `argparse` with explicit paths, a docstring listing its inputs, resumable,
   logging progress to a file, no prompts, and a `--limit N` smoke-test mode that runs on the
   8 locally available SG inventories.
5. **D5.** Layer data lives in `/Users/Shared/claude_projects/data_scopes/shared-data/`
   (created 2026-10-14, with a README). `voc_text_layer_v1/` moves there from NLP when its
   producer moves into AS.
6. **D6.** The user's answer specifies the **scan-id** format for NL-HaNA archives:
   `NL-HaNA_<archive>_<inventory>_<NNNN>.jpg`. The **EAD node id keeps the proposed format**
   `archive:1.2.3` (confirmed 2026-10-14). **`EADComponent` compares and hashes by identity** (`eq=False`, confirmed 2026-10-14). Three
   notions of "same" are kept apart: identity means the same Python object; `node_id`
   (`archive:1.2.3`) means the same position in this file, stable across re-parses; and a new
   optional **`persistent_id`**, taken from `<unitid type="urn:uuid">` where an export has one,
   means the same archival unit across exports. Every component in NL-HaNA 1.01.02 and
   1.04.02 has one; none of the other 47 corpus finding aids does. No cross-export comparison
   is needed yet.
7. **D7.** Cross-archive scope-note references carry the **target archive code** (e.g.
   `1.11.01.01`).
8. **D8.** Yes, copy small test fixtures into AS `tests/`. The GLOBALISE index now lives in
   the central store (`ground_truth/NL-HaNA_1.04.02/globalise_digitized_indexes/`) and is
   passed by path, not copied into tests.
9. **D9.** Move the oracle baseline into AS, **clearly labelled as an oracle** (it uses each
   inventory's own start rate, so it is not a legitimate predictor).
10. **LotA `lb_adjusted`** (asked after step 2): retire the column. After C1 it is always False.
    It is removed in step 6 (`plan/phase-02-ead-for-lota/`).

### SG paragraph TSV decisions (audit questions, 2026-10-14)

1. **The LotA copy is canonical.** AS `data/sg_letters/` is the derived copy.
2. Uncommitted pagexml-tools code may have been used, but if so it was committed later and
   released in an earlier version.
3. **Retire `region_type`.** Recomputing it needs column detection, a significant task of its
   own, with no suitable ground truth yet.
4. **Which breaks to merge is inventory-dependent and must be tested per inventory.** Breaks at
   region ends are a separate problem: finding which other region continues the last line,
   possibly on another scan or page.
5. **Drop `deelopname` (partial-shot) scans.** They are a tiny fraction of the total. Add a
   detector for scans whose ids break the scan-id format.
6. The full regeneration runs on the user's external disk (see D4).
