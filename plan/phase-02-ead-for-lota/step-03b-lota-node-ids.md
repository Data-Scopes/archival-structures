# Step 03b: LotA switches to AS node ids

> **Phase:** 02, EAD for LotA. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

AS step 3 (already done) added node ids to `EADComponent` and made components hash by
identity. LotA still builds its own ids, keys its node index by `id(component)`, and joins
`ArchiveTree.frame()` on the `citation` string (`file:sourceline`). This step makes LotA take
all three from AS. The ids are byte-identical by construction, so no LotA output may change.
The one AS change is an optional `archive_id` argument on the two frame builders, because LotA's
archive id (e.g. `NL-AsdSAA_89`) need not match the EAD's `<eadid>`.

## Inputs

LotA root: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`. **Read LotA
`CLAUDE.md` first** (72 lines); its rules apply to LotA files. LotA has no git history, so
before editing a LotA file, copy it to your scratchpad so you can diff your own change.
Line numbers were checked on 2026-10-14.

AS, already present (`archival_structures/parsers/ead_tree.py`):

```python
def make_node_id(archive_id: str, index_path: Tuple[int, ...]) -> str  # L57; raises ValueError if ':' in archive_id
class EADComponent:  # @dataclass(eq=False): == and hash() are identity-based
    index_path: Tuple[int, ...]
    def node_id(self, archive_id: str) -> str            # L160
    def parent_id(self, archive_id: str) -> Optional[str]  # L164; None for a root
    def ancestor_ids(self, archive_id: str) -> List[str]   # L170; root first, excludes self
class EADFindingAid:
    @property
    def default_archive_id(self) -> str  # L235: eadid (or file stem), ':' replaced by '_'
```

AS `archival_structures/parsers/ead_views.py` (183 lines) today:

```python
def _row_from_component(component, max_ancestry: int, finding_aid: EADFindingAid) -> dict  # L51
    #   L58: "node_id": component.node_id(finding_aid.default_archive_id),
def _frame_from_components(components: List[EADComponent], finding_aid: EADFindingAid) -> pd.DataFrame  # L81
def to_frame(finding_aid: EADFindingAid, level: str) -> pd.DataFrame          # L91
def unit_of_description_frame(finding_aid: EADFindingAid) -> pd.DataFrame     # L104
```

LotA `archival_language/ead/hierarchy.py` (168 lines):

- L38–39, to delete:
  ```python
  def _node_id(archive_id: str, index_path: tuple[int, ...]) -> str:
      return f"{archive_id}:{'.'.join(str(i) for i in index_path)}"
  ```
  It has one call site, L61: `node_id=_node_id(archive_id, component.index_path),`.
- `id()` keying: L58 `nodes_by_component_id: dict[int, ArchiveNode] = {}`, L72
  `nodes_by_component_id[id(component)] = node`, L74 `self._nodes_by_component_id = ...`, L100
  (`walk`) `yield self._nodes_by_component_id[id(component)]`, L103 (`roots`)
  `[self._nodes_by_component_id[id(c)] for c in self.finding_aid.roots]`, L113
  (`node_for_component`) `return self._nodes_by_component_id[id(component)]`.
- `frame()` L132–160. The citation join at L136–138:
  ```python
  frame = ead_views.unit_of_description_frame(self.finding_aid)
  citation_to_node_id = {n.citation: n.node_id for n in self.walk()}
  frame = frame.copy()
  frame["node_id"] = frame["citation"].map(citation_to_node_id)
  ```
  This is followed by an "unmatched" `ValueError` (L140–145), a duplicate check (L146–157) and
  caching (L159–160). The AS frame already has a `node_id` column, built with the eadid
  prefix; L138 overwrites it.

LotA `archival_language/model/node.py` (68 lines): `ArchiveNode` frozen dataclass, L37–49
(fields `node_id, archive_id, index_path, level, title, depth, sourceline, citation, component`;
only `component` has `compare=False`). `parent_id` (L51–58) and `ancestor_ids` (L60–68, returns a
`tuple`) rebuild the id string by hand. The docstring at L39 says the class "Compares and hashes
on `node_id` alone"; that is false (every field except `component` takes part).

Leave alone: `facets/title_vocab.py:321` and `gold/node_sampling.py:148` parse ids with
`partition(":")`. The format does not change, so they stay correct.

## Deliverables

| File | Change |
|---|---|
| AS `parsers/ead_views.py` | `to_frame(finding_aid, level, archive_id: Optional[str] = None)` and `unit_of_description_frame(finding_aid, archive_id: Optional[str] = None)`; thread `archive_id` through `_frame_from_components` and `_row_from_component`; `None` means `finding_aid.default_archive_id` (today's behaviour) |
| AS `tests/test_parsers_ead_views.py` | 2 new tests (below) |
| LotA `ead/hierarchy.py` | delete `_node_id`; `component.node_id(archive_id)`; component-keyed index; `frame()` joins on AS `node_id` |
| LotA `model/node.py` | `parent_id`/`ancestor_ids` built with AS `make_node_id`; docstring corrected |
| AS `docs/shared_library/changelog.md` | `### Step 3b: …` under *Unreleased*, added with **Edit** |
| LotA `STATUS.md` | append a dated section (LotA rule: append, never rewrite) |

## Behaviour to implement

1. **AS `ead_views`.** Add the keyword argument and pass it down. The default must reproduce
   today's frames exactly. Update both docstrings to say what `archive_id` does.
2. **`hierarchy.py`.**
   - L61 becomes `node_id=component.node_id(archive_id),`. Delete `_node_id`.
   - Rename `nodes_by_component_id`/`_nodes_by_component_id` to
     `nodes_by_component`/`_nodes_by_component: dict[EADComponent, ArchiveNode]`, keyed by
     the component itself (identity hash), at all six sites. `EADComponent` is already
     imported (L20).
   - `frame()`: replace L136–138 with
     `frame = ead_views.unit_of_description_frame(self.finding_aid, archive_id=self.archive_id)`.
     Replace the unmatched check with: every `frame["node_id"]` must be a key of `self._nodes`.
     Otherwise raise `ValueError(f"frame() node_id join is not 1:1 for archive {self.archive_id!r}: {len(unmatched)} unmatched node_id(s): {unmatched[:10]}")`.
     Keep the duplicate check and caching. Keep the `citation` column. Update the docstring:
     the join is on `node_id`, not `citation`.
3. **`model/node.py`.**
   `from archival_structures.parsers.ead_tree import make_node_id`. Then `parent_id` returns
   `None` if `len(self.index_path) <= 1`, else `make_node_id(self.archive_id, self.index_path[:-1])`.
   `ancestor_ids` returns
   `tuple(make_node_id(self.archive_id, self.index_path[:n]) for n in range(1, len(self.index_path)))`.
   Don't call `self.component`: `tests/test_scopecontent.py` builds `ArchiveNode`s around a fake
   component. Docstring: "Compares and hashes on every field except `component`. Within one
   parse, `node_id` is unique, so in practice equality means the same node id." Don't change
   the equality behaviour.

## Constraints

- No LotA output may change: node ids, frame rows and frame columns stay identical.
- Don't change `ArchiveNode`'s fields or `ArchiveTree`'s public methods.
- `legacy_compatible_frame` is untouched.
- Never `git commit`; never bump the version; `chmod g+w` new files.

## Tests

- AS `tests/test_parsers_ead_views.py`. Build a finding aid from an inline EAD with
  `ead_tree.parse_ead_string(xml)`:
  - `test_archive_id_override_prefixes_node_id`: `unit_of_description_frame(fa, archive_id="X")`
    gives node ids all starting with `"X:"`, with the same suffixes as the default call;
  - `test_default_archive_id_unchanged`: the default call's `node_id` column equals
    `[c.node_id(fa.default_archive_id) for c in <frame components>]`.
- LotA `tests/test_hierarchy.py` (12 tests, real SG/VOC, about 20 s) must pass unchanged.
- Commands:
  - AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_parsers_ead_views.py tests/test_parsers_ead_node_id.py`, then the full suite (baseline 582 passed, about 100 s).
  - LotA, from its root: `venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py`.
    The baseline is 320 passed. `test_derive_voc.py` fails to import for an unrelated reason
    (`fuzzy_search.similarity`).

## Acceptance criteria

1. **Equivalence on all 9 registered LotA archives.** Before editing, run a scratch script
   (in your scratchpad, not in either repo) that does
   `ArchiveTree.load(aid).frame()[["node_id", "citation"]]` for every `aid` in
   `load_config().archives` and saves each frame to the scratchpad. After editing, run it
   again and compare: identical rows, in the same order, for every archive. Report the row
   count per archive; SG (`1.01.02`) is 17,937 and VOC (`1.04.02`) 16,447. If the old
   `frame()` raised for an archive, record that and say whether the new one succeeds.
2. For every archive, `[n.node_id for n in tree.walk()]` is identical before and after.
3. AS: 584 passed (582 + 2). LotA: 320 passed with the `--ignore` above.
4. `grep -n "id(c\|id(component\|_node_id" archival_language/ead/hierarchy.py` returns nothing.

The user decides when the step is done.

## Error analysis

- List each archive whose `finding_aid.default_archive_id` differs from its LotA archive id,
  and show 3 frame rows with the AS default `node_id` next to the LotA one. These archives are
  why the `archive_id` argument exists.
- For 10 random frame rows per archive (seeded), check that `tree.node(row.node_id).title`
  equals `row.title`. Report any mismatch verbatim.

## Do not touch

- `stream_analysis/` and NLP (phase 03).
- LotA `ead/scopecontent*.py`, `facets/`, `cli/` (later steps).

## When done

1. Append the block below to the phase `PROGRESS.md`.
2. Overwrite the phase `STATUS.md`: this step done, next step, open questions.

```markdown
### Step 03b: LotA switches to AS node ids
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Error analysis:** <what was read; what failures had in common>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
