# Shared-library changelog

Changes to archival-structures' public API or packaging that affect the sibling projects
(language-of-the-archive = LotA, nlp-start-page-detection = NLP). Steps refer to
`upstream_inventory.md` §4. Newest first.

## Unreleased (after v0.4.1)

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
