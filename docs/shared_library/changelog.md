# Shared-library changelog

Changes to archival-structures' public API or packaging that affect the sibling projects
(language-of-the-archive = LotA, nlp-start-page-detection = NLP). Steps refer to
`upstream_inventory.md` §4. Newest first.

## Unreleased (after v0.6.0)

### Phase 03 review fix: `intra_document_swap` provenance (bug fix)

- `corruption.operators.intra_document_swap` swapped the two units but left their provenance
  entries in place, so `provenance[k]` no longer described `observed[k]` and
  `observed_start_labels` did not follow a moved start page. The entries now move with their
  units. `swap_and_insertion` inherited the bug and is fixed with it. The other operators were
  already correct. For the same seed the corrupted stream is unchanged; only the provenance
  differs. **Corrupted-stream YAMLs written with `intra_document_swap` or `swap_and_insertion`
  before this fix carry wrong provenance and should be regenerated.** New test:
  `test_provenance_points_at_the_unit_in_its_position` checks all four operators, 5 seeds each.
- **Affected sibling imports:** none (no sibling uses the corruption operators).

### Step 7: segmentation evaluation suite (C6)

- New package `archival_structures.stream_analysis.evaluation` (`segmentation`, `aggregate`),
  ported from NLP's `startpage/evaluation.py`; import from the package, not from
  `stream_analysis` (no lazy export, because `window_diff` clashes with the corruption one).
  Labels are `bool` (`check_labels` raises `TypeError` otherwise). `default_k(labels,
  convention="gaps"|"pages")`; `pk`/`window_diff`/`evaluate_all` take `k=None, *,
  k_convention="gaps"`. NLP's numbers need `k_convention="pages"`. Length mismatches raise
  `ValueError`; `WindowClippedWarning` is emitted once per call when k is clipped.
  `build_eval_frame(..., sequences, sep="/")` renames `scanname` to `page_id`, takes
  `{inventory: [page_id, ...]}` and raises on duplicate scan ids.
- **Behaviour change in `corruption.metrics`:** `score_start_classification`,
  `score_same_document_pairs` and `window_diff` now raise `TypeError` on non-bool labels.
  `window_diff` keeps its strict `ValueError` contract; values are unchanged. `_to_gaps` removed
  (private).
- **Affected sibling imports:** none yet. NLP's `startpage.evaluation` becomes a shim in step 07b
  (string labels, `scanname` column, tuple-shaped `full_sequences`, `k_convention="pages"`).
- **Step 07b (NLP, no AS change):** NLP's `startpage.evaluation` is now a shim over this package.
  It keeps 17 names: `START`, `build_eval_frame`, `page_level_prf`, `page_level_prf_with_counts`,
  `iter_contiguous_runs`, `tolerance_prf`, `default_k`, `pk`, `window_diff`, `segment_exact_match`,
  `PerInventoryResult`, `per_inventory`, `start_rate_calibration`, `bootstrap_ci`,
  `bootstrap_pooled_f1`, `evaluate_all`, `format_report` (plus private `_tolerance_prf_runs`). It
  converts string labels to bool, extracts page ids from tuple-shaped `full_sequences`, renames
  `page_id` back to `scanname`, and passes `k_convention="pages"` explicitly (D3). The 21 NLP
  importers are unchanged; `scripts/verify_as_evaluation_shim.py` shows the stored numbers are
  reproduced exactly.

### Step 8: chunk splits and validators (C7)

- New module `archival_structures.stream_analysis.evaluation.splits` (also re-exported from
  `evaluation`). Ported verbatim from NLP's `long_subsequence_chunk_split.py`: `Page`,
  `Document`, `Chunk`, `SubSequence`, `LongSequenceSplit`, `parse_documents`,
  `partition_file_into_chunks`, `assign_splits_and_pad_context`,
  `build_archival_dataset_splits`, `build_subsequences`, `validate_subsequences`. `print`
  became `logging` (`info` for stats, `warning` for the duplicate NOTE); P2 edge behaviour
  (10-slot striding, greedy rebalance) is unchanged and documented.
- New: `pages_from_starts`, `build_long_sequence_split(archival_files, *, ...)`,
  `build_inventory_held_out(sequences, held_out=None, seed=0, n_held_out=5)`, `core_rows`,
  `eval_scan_ids`, `SplitValidationError` (an `AssertionError`), `check_no_duplicate_page_ids`,
  `check_no_document_crosses_partitions`, `check_context_not_scored`, `check_split_coverage`,
  `check_nested_split`.
- **Affected sibling imports (NLP):** `startpage/long_subsequence_chunk_split.py` re-exports the
  AS names and keeps `TRAIN_FRAC`, `DEV_FRAC` and `build_long_sequence_chunk_split` (same
  signature; the VOC end-id rule is replaced by page adjacency, identical on all 22
  inventories); `startpage/splits.py` private helpers (`_check_duplicate_page_ids`,
  `_assert_no_document_split_across_splits`, `_validate_split`, `_select_held_out` returns
  `None` for seed >= 1, `_build_inventory_held_out`) call AS. All seven cached split CSVs
  rebuild byte-identically (`scripts/verify_as_splits.py`). NLP's stdout lines "Total Chunks
  Generated", "Split Distribution" and the duplicate/gap NOTEs now go to `logging`.

### Step 9: trivial baselines (C8)

- New module `archival_structures.stream_analysis.evaluation.baselines` (re-exported from
  `evaluation`): `majority`, `every_k`, `length_prior`, `oracle_every_k_own_rate` (an ORACLE,
  not a legitimate predictor), `train_start_rate`, `BaselineRun`, `baseline_suite`,
  `baseline_floors`. Bool predictions; `length_prior` reproduces NLP's `default_rng` loop seed
  for seed.
- New: `evaluation.aggregate.flatten_metrics(results)` (NLP's 23 metric columns, verbatim) and
  `corruption.start_classification_floors(observed, provenance, canonical, **kwargs)`
  (re-exported from `corruption`; imports `baselines` lazily).
- **Affected sibling imports (NLP):** `scripts/run_trivial_baselines.py` imports
  `baseline_suite`, `eval_scan_ids`, `flatten_metrics`, `train_start_rate` from AS, drops its
  local copies and gains `--out PATH`. It reproduces the stored CSV in 344 of 360 cells (16
  random bootstrap cells excluded). `scripts/verify_as_evaluation_shim.py` now takes the
  pre-refactor helpers from `output/as-phase-03-originals/run_trivial_baselines.py`.

## v0.6.0 (2026-10-14)

### Step 6: `split_unittitle_dates` (C4); LotA `lb_adjusted` retired

- New function `archival_structures.parsers.ead_fields.split_unittitle_dates(unittitle_element)
  -> (prose, spans)`: one span per direct `<unitdate>` child (`None` for an empty one), into
  `element_text(unittitle_element)`; `prose` is that text with the spans cut out. Purely
  additive in AS.
- `parsers/ead_text.py` gets a private hook (`_SPAN_START`, `_SPAN_END`, `_raw_marked_text`; the
  `<lb/>` no-space-before regex ignores the markers). `element_text` and `raw_element_text` are
  unchanged.
- **LotA imports changed:** `facets/title_norm.py` calls `ead_fields.split_unittitle_dates`. Deleted
  there: `_structural_date_spans`, `_structural_text_and_prose`, `_collapse_with_map`,
  `_unitdate_children`, `_retag_ranges`, the sentinel constants and the regex fallback
  (`date_locator == "fallback"` can no longer occur). **Removed:** `NormalisedTitle.lb_adjusted`,
  the `lb_adjusted` column of `title_frame` / `*-normalised-titles.tsv.gz`, and the census column
  `n_lb_adjusted` (`cli/normalise_titles.py`). All other title columns and census cells are
  unchanged on the 9 archives (0 changed cells).

### Step 5: scope-note parser (C3) with cross-archive references

- New module `archival_structures.parsers.ead_scope_notes`: `ScopeNote`, `ScopeReference`,
  `ShapeRule`, `NA_SHAPE_RULES`, `classify_shape(text, tags, rules=NA_SHAPE_RULES)`,
  `ReferencePatterns`, `NA_REFERENCE_PATTERNS`, `descendant_tags`, `LB_TAG`,
  `parse_scope_notes(component, archive_id, *, source_path, tags, shape_rules, patterns)`,
  `iter_scope_notes(finding_aid, ...)` and `scope_notes_frame(finding_aid, ...)`. Purely
  additive in AS.
- `ScopeReference` gains `target_archive: str | None` (None = the current archive). The NA
  inventory pattern reads an optional archive code before the cue (`Zie Aanwinsten, 1.11.01.01,
  inv.nr. 687`), and the number list stops before an archive code that starts a new reference.
  **Behaviour change vs LotA's old parser:** such references now carry `target_archive` and must
  not be joined to the current archive's inventory.
- `target_archive` is also None when the written code *equals* the `archive_id` passed in
  (string equality). That holds for LotA's SG and VOC, whose ids are the bare NA codes
  `1.01.02`/`1.04.02`. A caller whose id carries a prefix (e.g. `NL-HaNA_2.10.01`) would see its
  own code as a target (phase-02 review, F02-R.6).
- `_segments` applies the `element_text` rule: a `<lb/>` directly before punctuation adds no
  space (`Eems<lb/>. Met` is `Eems. Met`). The AS copy differs from LotA's old `_segments` only
  here; SG and VOC `segments` are unchanged (0 cells).
- **LotA imports changed:** `ead/scopecontent.py` (`ScopeNote`, `ScopeReference`, and
  `parse_scope_notes` as `_parse_component`), `ead/scopecontent_survey.py` (`LB_TAG`,
  `NA_SHAPE_RULES`, `ShapeRule`, `classify_shape`, `descendant_tags`; `SHAPE_RULES` kept as an
  alias), `cli/parse_scopecontent.py` (appends `:{target_archive}` to a serialised reference
  only when it is set). On SG + VOC only the `references` column changes: 91 cells, all gained a
  suffix.

### Step 3c follow-up: `element_text` drops the `<lb/>` space before quotes

- `ead_text.element_text` / `raw_element_text`: a `<lb/>` directly before `'`, `"`, `\u2019` or
  `\u201d` now contributes nothing, like the existing `. , ; : ! ? ) ]` set (`1674<lb/>'` is
  `1674'`, not `1674 '`). Behaviour change, no signature change. Trade-off (user decision,
  2026-10-14): an ASCII quote that *opens* a quotation after a line break (`a<lb/>"b"`) now loses
  its space (`a"b"`).
- **Affected LotA imports:** `facets/title_norm.py`, `ead/scopecontent.py`,
  `ead/scopecontent_survey.py` (all via `element_text`/`raw_element_text`). Across the SG and
  VOC titles, 3 SG titles change (`1.01.02:5.2.0.4.2.69`, `5.2.4.85`, `5.2.4.186`); the 7 other
  archives and all scope notes are unchanged.

### Step 4: `DigitisationSummary.has_evidence(mechanisms)` (C5)

- Added `ead_digital.MECHANISM_NAMES` (`("dao", "daogrp", "assets", "handle")`) and
  `DigitisationSummary.has_evidence(mechanisms)`: True if the component carries any of the
  given mechanisms. A bare `str` raises `TypeError`; names outside `MECHANISM_NAMES` raise
  `ValueError`; an empty collection gives False. Backwards compatible: purely additive.
- `is_digitised` is unchanged (handle-inclusive); its docstring now warns that a handle is
  not evidence that scans exist.
- **Sibling code changed in the same step (LotA):** `ead/digitisation.py` and
  `corpus/coverage.py` delete their private `_has_evidence` helpers and call
  `summary.has_evidence(...)`; `NodeDigitisation` gains a last field `summary`
  (`compare=False`, `repr=False`).

### Step 3b: `archive_id` on the EAD frame builders; LotA uses AS node ids

- `ead_views.to_frame(finding_aid, level, archive_id=None)` and
  `ead_views.unit_of_description_frame(finding_aid, archive_id=None)` take an optional
  `archive_id`, the prefix of the `node_id` column. `None` means
  `finding_aid.default_archive_id`, so the default frames are unchanged. A caller passes its
  own id when it differs from the EAD's `<eadid>`. In 7 of LotA's 9 archives it differs: e.g.
  LotA `NL-AsdSAA_89` has eadid `89`, and `GB-3624_oh` has `gb3624-oh`.
- **Sibling code changed in the same step (LotA):**
  - `ead/hierarchy.py`: `_node_id` is deleted and replaced by `EADComponent.node_id`. The
    node index is keyed by the component itself, not `id(component)`. `frame()` takes the AS
    `node_id` column (built with `archive_id=self.archive_id`) instead of joining on `citation`.
  - `model/node.py`: `ArchiveNode.parent_id`/`ancestor_ids` use `make_node_id`.
  - No LotA output changes. Across all 9 archives, node ids and `frame()` rows and columns are
    identical. Details are in LotA `STATUS.md` (2026-10-14, AS step 3b).

## v0.5.0 (2026-10-14)

Steps 1–3 (heading added by the phase-02 review; the release was tagged with these entries still
under *Unreleased*).

### Step 2: EAD text across `<lb/>` (C1)

- New `parsers/ead_text.py` with `element_text(el)` and `raw_element_text(el)`, the
  pre-collapse form for character offsets. The rule:
  - `<lb/>` becomes a space, except directly before `. , ; : ! ? ) ]`;
  - comments and processing instructions contribute only their tail;
  - other markup is descended without adding spaces at element edges, so words like
    `Ge<emph>heim</emph>e` stay whole;
  - whitespace is collapsed once, at the end.
- **Behaviour change:** `EADComponent.title` (via `_text_of`) and `PhysDesc.raw_text` use it. In
  the 52-file corpus, 18 titles change, all of them `<lb/>`-glued (SG 10, VOC 2, 1.05.06 5,
  2.10.50 1): `eerstedeel` → `eerste deel`, `vanGent` → `van Gent`, `G.Voor` → `G. Voor`.
  Three titles gain a space before a probable closing quote (cosmetic). `ead_survey._text_of`,
  used only for structure surveys, is unchanged.
- **Sibling code changed in the same step (LotA):** `element_text` is now imported from AS, and
  the LotA copy in `ead/scopecontent_survey.py` is deleted. `facets/title_norm.py`
  `_structural_date_spans` uses `raw_element_text`, and `tests/test_title_norm.py` is updated.
  `lb_adjusted` is now always False. LotA's normalised-title and scope-note outputs need
  regenerating. Details are in LotA `STATUS.md` (2026-10-14).

### Step 3: EAD node ids and identity-hashable components (C2)

- **Behaviour change:** `EADComponent` now compares and hashes **by identity**
  (`@dataclass(eq=False)`). Before, it was unhashable, and its structural `==` never matched
  across parses because its lxml fields compare by identity. Components can now be used in sets
  and as dictionary keys.
- New in `parsers/ead_tree.py`:
  - `make_node_id(archive_id, index_path)`;
  - `EADComponent.node_id(archive_id)`, `.parent_id(archive_id)`, `.ancestor_ids(archive_id)`;
  - `EADComponent.persistent_id`;
  - `EADFindingAid.default_archive_id`, `.iter_node_ids(archive_id=None)`.

  The node id is `"<archive_id>:<0-based index path joined by '.'>"`, e.g. `1.01.02:0.0.1.1.1`.
  It is byte-identical to LotA's `_node_id` (checked on all 28,573 SG components) and stable
  across re-parses of the same file, though not guaranteed across exports. `persistent_id` is
  the component's `<unitid type="urn:uuid">`, which identifies the same archival unit across
  exports. It is present on every component in NL-HaNA 1.01.02 and 1.04.02, and absent
  elsewhere.
- **Behaviour change:** `ead_views.to_frame` and `unit_of_description_frame` gain two leading
  columns, `node_id` and `persistent_id`. `legacy_compatible_frame` is unchanged.
- **Affected sibling code:** LotA `archival_language/ead/hierarchy.py` (`_node_id` at L38–39,
  the `id(component)` keys at L72/100/103/113, and the citation join in `frame()` at L142–144)
  and `model/node.py` (`parent_id`/`ancestor_ids`) can switch to AS. Not changed yet.

### Step 1: packaging (light core plus extras)

- **Core dependencies:** pagexml-tools, numpy, pandas, lxml, fuzzy-search, orjson, pyyaml,
  scikit-learn, tqdm.
- **Optional extras:**
  - `[torch]`: torch, transformers;
  - `[vision]`: scikit-image, opencv-python, pillow, hdbscan, umap-learn, scipy, ruptures;
  - `[notebook]`: ipywidgets, ipycanvas, ipyevents, ipython, matplotlib, seaborn;
  - `[llm]`: anthropic;
  - `[all]`: everything.

  scipy and ruptures were imported but never declared before; ipython moved from core to
  `[notebook]`.
- **Bounds:** `scikit-learn>=1.8` and `torch>=2.11`, loosened; `pandas>=2.3.3,<4`, widened.
  pandas 3 is verified for core modules only (parsers, annotation, corruption, model: 230
  passed), not for the `[vision]` modules.
- `stream_analysis` and `datasets` now export their names lazily (PEP 562). Every existing
  `from archival_structures.stream_analysis import X` still works, and
  `stream_analysis.corruption`, `parsers.*`, `model.*` and `annotation.*` import without any
  extra. `model/image.py` imports cv2 and PIL only for type checking. Some `analysis` modules
  (the clustering modules, `empty_regions`) still need `[vision]` at import time.
- NLP has a new `pyproject.toml` (package `startpage`, src layout, `archival-structures>=0.4.1`).
- **Still to do (user):**
  - run `poetry lock` in AS;
  - LotA: install AS without `--no-deps`;
  - NLP: install AS and itself editable into `venv/`.
