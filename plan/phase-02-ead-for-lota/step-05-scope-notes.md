# Step 05: C3 scope-note parser into AS, with cross-archive references

> **Phase:** 02, EAD for LotA. **Depends on:** 03b (node ids) and 03c (baselines, `diff_outputs`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

LotA's `<scopecontent>` parser (shape classifier, inventory/folio/external reference
extraction, raw date strings, segmentation) is generic apart from its Dutch/Nationaal Archief
rules, so it moves to a new AS module. The rules become parameters with an `NA_…` default
(decision P3). The move fixes a live bug (decision D7). VOC notes such as
`Zie Aanwinsten, 1.11.01.01, inv.nr. 687` point into another archive, but are extracted as a
plain `inventory:687`, which joins to VOC inventory 687. `ScopeReference` gains
`target_archive: str | None`, and a reference into another archive must never join to the
current archive's inventory. LotA keeps the CLI and the survey/census functions, and its
`ead/scopecontent.py` shrinks to thin wrappers.

## Inputs

LotA root: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`. **Read LotA
`CLAUDE.md` first.** Copy each LotA file to your scratchpad before editing it (no git). Line
numbers were checked on 2026-10-14.

**Port from `archival_language/ead/scopecontent.py` (344 lines), reading these ranges.**
- Helpers `_local_tag` L51–54, `_direct_children` L57–59. Regexes L68–109: `_MONTH`,
  `_DATE_FULL_RE`, `_DATE_RANGE_RE`, `_INV_NUMBER_TOKEN`, `_INV_NUMBER_OR_RANGE`,
  `_INV_SEPARATOR`, `_INVENTORY_REF_RE`, `_INV_TOKEN_RE`, `_FOLIO_RE`, and
  `_EXTERNAL_TAGS = ("extref", "bibref", "title")`.
- `ScopeReference` L112–123 (`kind` is one of `inventory`/`folio`/`external`/`unparsed`;
  `raw: str`; `inventory_number: InventoryNumber | None`; `folio: str | None`).
- Extractors L126–174: `_inventory_references`, `_folio_references`, `_external_references`,
  and `_extract_references` (inventory + folio + external, in that order).
- `_extract_date_strings` L180–208: range matches, then full-date matches, then the text and
  `@normal` of each `<date>` descendant, de-duplicated keeping the first occurrence.
- `_segments` L214–264. It maps `<lb/>` to `" "` without the punctuation rule; port it
  **verbatim** anyway, because outputs must not change.
- `ScopeNote` L270–288, with 12 fields: `node_id, archive_id, source_tag, element_index, shape,
  shape_rule, text, segments, references, date_strings, char_len, citation`.
- `parse_scope_notes` L291–323. For each tag and each element in
  `component.other_children.get(tag, [])`: take `text = element_text(element)`, classify with
  `classify_shape(text, descendant_tags(element))`, and enumerate `element_index` per tag
  (`char_len=len(text)`).
- `scope_notes_frame` L326–337 (`pd.DataFrame([vars(r) for r in rows])`), `_notes_iter` L340–344.

**Port from `archival_language/ead/scopecontent_survey.py` (312 lines):**
- `LB_TAG = "lb"` L37; `descendant_tags` L47–54 (sorted, unique local tags of all descendants);
- `ShapeRule` L104–114 (`name, description, pattern=None, requires_tag=None, requires_empty=False`);
- `SHAPE_RULES` L121–204: 14 rules, in order `empty, list_markup, exhibitum_date,
  former_reference, cross_reference, content_enumeration, short_content_flag, index_note,
  physical_form_note, duplicate_note, continuation_note, gap_note, annotation_note,
  language_note`;
- `classify_shape` L207–223 (first match wins, else `("unclassified", "unclassified")`).

The rest of the survey module stays in LotA (`descendant_tag_counts`, `descendant_attributes`,
`descendant_attribute_name_counts`, `SurveyRow`, `survey_node`, `survey_tree`,
`census_tags_and_attributes`).

**AS, already present:**
- `ead_text.element_text(el) -> str`;
- `model.inventory_number.InventoryNumber(str)`, which raises `InvalidInventoryNumberError`;
- `EADComponent.node_id(archive_id)`, `.cite(path: Optional[Path]) -> str` (`"name:line"`), and
  `.other_children: Dict[str, List[etree._Element]]` (keyed by local tag name);
- `EADFindingAid.walk()` (pre-order), `.path`, `.default_archive_id`;
- `ead_tree.parse_ead_string(xml, source_name="<string>")`.

**LotA callers to switch:**
- `ead/__init__.py` L23–28 re-exports `ScopeNote, ScopeReference, parse_scope_notes,
  scope_notes_frame` (keep them);
- `cli/parse_scopecontent.py` `_serialize_reference` L37–40 returns
  `f"{ref.kind}:{ref.raw}:{inv}:{folio}"`;
- `tests/test_scopecontent_survey.py` L16–19 imports `SHAPE_RULES, classify_shape,
  descendant_tags` from the survey module.

**AS real data** for skip-if-absent tests: `data/metadata/eads/NL-HaNA_1.04.02.xml` (VOC) and
`NL-HaNA_1.01.02.xml` (SG). Copy the `REAL = …` / `@unittest.skipUnless(REAL.exists(), …)`
pattern from `tests/test_parsers_ead_node_id.py` L109–112.

## Deliverables

| File | Change |
|---|---|
| AS `parsers/ead_scope_notes.py` (new) | the API below |
| AS `tests/test_parsers_ead_scope_notes.py` (new) | ported and new tests (below) |
| LotA `ead/scopecontent.py` | thin wrappers over AS (below); all regexes and helpers deleted |
| LotA `ead/scopecontent_survey.py` | `ShapeRule`, `SHAPE_RULES`, `classify_shape`, `descendant_tags`, `LB_TAG` deleted and imported from AS, with `SHAPE_RULES = NA_SHAPE_RULES` kept as an alias |
| LotA `cli/parse_scopecontent.py` | `_serialize_reference` appends `:{target_archive}` **only when it is not None** (user decision, 2026-10-14) |
| LotA `tests/test_scopecontent.py` | the 6 synthetic tests move to AS and are deleted here, together with `_FakeComponent`/`_node` if unused; the 5 real-data tests stay |
| LotA `output/scope-notes/*.tsv.gz` | regenerated |
| AS `docs/shared_library/changelog.md` | `### Step 5: …` under *Unreleased*, added with **Edit**, listing the LotA imports |
| AS `reports/phase-02-ead-for-lota.md` | `### Step 05: …` section (results, error analysis, draft findings `F02-05.n`); then `make reports` |
| LotA `STATUS.md` | append a dated section |

## Behaviour to implement

**AS API** (`from __future__ import annotations`; typing style as in `ead_fields.py`):

```python
LB_TAG = "lb"
def descendant_tags(element) -> Tuple[str, ...]
@dataclass(frozen=True)
class ShapeRule: ...                                   # verbatim
NA_SHAPE_RULES: Tuple[ShapeRule, ...]                  # the 14 rules verbatim, same order, docstrings kept
def classify_shape(text: str, tags: Tuple[str, ...], rules: Sequence[ShapeRule] = NA_SHAPE_RULES) -> Tuple[str, str]

@dataclass(frozen=True)
class ReferencePatterns:
    inventory: Pattern[str]        # must have group "numbers"; may have group "archive"
    inventory_token: Pattern[str]
    folio: Pattern[str]
    date_range: Pattern[str]
    date_full: Pattern[str]
    external_tags: Tuple[str, ...] = ("extref", "bibref", "title")
NA_REFERENCE_PATTERNS: ReferencePatterns

@dataclass(frozen=True)
class ScopeReference:
    kind: str; raw: str; inventory_number: Optional[InventoryNumber]; folio: Optional[str]
    target_archive: Optional[str] = None   # None = the current archive

@dataclass(frozen=True)
class ScopeNote: ...                                   # the 12 fields, verbatim

def parse_scope_notes(component: EADComponent, archive_id: str, *, source_path: Optional[Path] = None,
                      tags: Tuple[str, ...] = ("scopecontent",), shape_rules: Sequence[ShapeRule] = NA_SHAPE_RULES,
                      patterns: ReferencePatterns = NA_REFERENCE_PATTERNS) -> List[ScopeNote]
def iter_scope_notes(finding_aid: EADFindingAid, archive_id: Optional[str] = None, *, tags=..., shape_rules=..., patterns=...) -> Iterator[ScopeNote]
def scope_notes_frame(finding_aid: EADFindingAid, archive_id: Optional[str] = None, *, tags=..., shape_rules=..., patterns=...) -> pd.DataFrame
```

- In `parse_scope_notes`, `node_id = component.node_id(archive_id)` and
  `citation = component.cite(source_path)`. `iter_scope_notes` walks `finding_aid.walk()` in
  pre-order, with `archive_id` defaulting to `finding_aid.default_archive_id` and
  `source_path=finding_aid.path`. `scope_notes_frame` is `pd.DataFrame([vars(n) for n in …])`.
- Private helpers take `patterns` and, for inventory references, `archive_id`. No NA-specific
  regex may be referenced outside the `NA_…` constants (P3).
- **The NA inventory pattern.** Rename the capture group to `numbers`. Put an optional
  archive-code prefix in front of the cue, and stop the number list before an archive code
  that starts a new reference:

  ```python
  _NA_ARCHIVE_CODE = r"\d\.\d{2}(?:\.\d{2,3}){1,3}"           # 1.04.02, 1.11.01.01, 2.21.281.08
  _NA_CUE = r"(?:inv(?:entaris)?\.?\s*nrs?\.?|inventarisnummer)"
  _NA_ARCHIVE_PREFIX = rf"(?:\b(?P<archive>{_NA_ARCHIVE_CODE})\.?\s*,?\s*)?"
  _NA_NEXT_IS_NEW_REF = rf"(?!{_NA_ARCHIVE_CODE}\.?\s*,?\s*{_NA_CUE})"
  inventory = re.compile(
      rf"{_NA_ARCHIVE_PREFIX}{_NA_CUE}\s*"
      rf"(?P<numbers>{_INV_NUMBER_OR_RANGE}(?:{_INV_SEPARATOR}{_NA_NEXT_IS_NEW_REF}{_INV_NUMBER_OR_RANGE})*)",
      re.IGNORECASE)
  ```

  Keep the other regexes verbatim.
- In `_inventory_references`, set `target = match.groupdict().get("archive")`. If `target` is
  not None and `target != archive_id`, every reference from that match (`inventory` or
  `unparsed`) gets `target_archive=target`. Otherwise it gets `None`. Folio and external
  references always get `None`.
- **LotA `ead/scopecontent.py`** becomes:
  - imports of `ScopeNote`, `ScopeReference` and AS `parse_scope_notes` (as
    `_parse_component`);
  - `parse_scope_notes(node: ArchiveNode, *, tags=("scopecontent",)) -> list[ScopeNote]`,
    which returns
    `[dataclasses.replace(n, citation=node.citation) for n in _parse_component(node.component, node.archive_id, tags=tags)]`;
  - `scope_notes_frame(tree, *, tags)` and `_notes_iter(tree, tags)` unchanged in behaviour.

  The module docstring explains the move.

## Constraints

- With `target_archive` None everywhere, the output must be identical to LotA's today. Port
  everything except the inventory regex verbatim. Don't "fix" `_segments`' `<lb/>` handling;
  record it under *Open questions* instead.
- Don't move `survey_tree`, `census_tags_and_attributes` or the CLI to AS.
- The LotA wrapper names and signatures in `ead/__init__.py` stay.
- Never `git commit`; never bump the version; `chmod g+w` new files.

## Tests

AS `tests/test_parsers_ead_scope_notes.py`. Write a helper `_component(scopecontent_xml)` that
wraps `<c level="file"><did><unittitle>t</unittitle></did>{xml}</c>` in a minimal EAD (as in
`tests/test_parsers_ead_digital.py` L17–27, but using `parse_ead_string`). Drop the `xmlns`
attribute from LotA's fragments.

- **Ported from LotA `tests/test_scopecontent.py`** (rewrite onto `_component`; keep the
  assertions): `test_lb_does_not_concatenate_words`,
  `test_inventory_reference_parsed_into_inventory_number`,
  `test_unparseable_reference_is_kept_as_unparsed`, `test_folio_span_is_kept_raw_not_expanded`,
  `test_date_strings_are_raw_substrings_only`, `test_scope_note_is_picklable`.
- **Ported from `test_scopecontent_survey.py`:** `test_classify_shape_examples_from_the_taxonomy`
  (copy it; the LotA copy stays too).
- **New** (all with `archive_id="1.04.02"`; expected matches checked by prototype in planning):

  | Test | Text | Expected |
  |---|---|---|
  | `test_cross_archive_reference_carries_target` | `Zie Aanwinsten, 1.11.01.01, inv.nr. 687, (1893 29r).` | one inventory reference, raw `687`, target `1.11.01.01` |
  | `test_cross_archive_variants` | `zie Aanwinsten, 1.11.01.01 inv.nr. 1519 (1926 I 2)`; `Zie Aanwinsten, 1.11.01.01. inv. nr. 794` | target `1.11.01.01` for both |
  | `test_cross_archive_unparsed_range` | `zie Archief Staten-Generaal, 1.01.02, inv.nrs. 4841-4844.` | `unparsed`, raw `4841-4844`, target `1.01.02` |
  | `test_same_archive_code_gives_none` | `1.04.02, inv.nr. 5` | target None |
  | `test_mixed_list_stops_before_new_reference` | `Zie inv.nr. 12, 1.11.01.01, inv.nr. 687` | `12` (None) and `687` (`1.11.01.01`); no reference with raw `1.11.01.01` |

  Also:
  - `test_custom_shape_rules_are_used` (one custom `ShapeRule`);
  - `test_patterns_without_archive_group_never_set_target`;
  - `test_iter_scope_notes_order_and_node_ids` (two components; pre-order; ids from
    `default_archive_id`);
  - `test_voc_real_counts` (skipUnless):
    - the note count equals `sum(len(c.other_children.get("scopecontent", [])) for c in fa.walk())`;
    - at least one note has target `1.11.01.01`;
    - no note matching `1.11.01.01[.,]?\s*inv` has an inventory reference with target None.

Commands:
- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_parsers_ead_scope_notes.py`,
  then the full suite.
- LotA: the full suite with `--ignore=tests/test_derive_voc.py`. Expect the `STATUS.md` count
  minus 6.

## Acceptance criteria

1. The AS suite is green, with 16 new tests (7 ported, 9 new).
2. The LotA suite is green. `test_scopecontent.py` (5 tests) and `test_scopecontent_survey.py`
   (7) pass.
3. Regenerate with `venv/bin/python -m archival_language.cli.parse_scopecontent` (writes
   `output/scope-notes/`). Diff each archive against `output/baselines/post-as-step-2/scope-notes/`
   with `diff_outputs … --key node_id,source_tag,element_index`. Keys must be identical, and
   **only `references` and `n_references` may change**:
   - `references` changes only where a reference gained a `:<target_archive>` suffix, or where
     an archive code that the old regex swallowed as an inventory token was dropped (e.g.
     `inv.nr. 12, 1.11.01.01, inv.nr. 687` used to give the numbers `12, 1.11.01.01`);
   - `n_references` changes only on rows of the second kind.

   Every other column has 0 changed cells. Count the two kinds separately.
4. Report the number of VOC and SG notes with at least one cross-archive reference, and the
   distinct `target_archive` values with counts. The inventory's earlier figure was 83 VOC
   Aanwinsten notes; report the actual number, don't assert 83.
5. `grep -n "re.compile" archival_language/ead/scopecontent.py` returns nothing.

The user decides when the step is done.

## Error analysis

- Read every changed row, up to 30 (all cross-archive notes if there are 30 or fewer), and
  check that each suffix names the archive written in the text.
- **Misses.** List unchanged rows whose `text` contains `Aanwinsten` or matches
  `\b\d\.\d{2}(?:\.\d{2,3}){1,3}\b`, but have no target. Read up to 30. Classify each as: no
  `inv.nr.` cue (e.g. `zie Aanwinsten, 1.11.01.01 (1893 29r)`), a date or other number, or a
  real miss. Report the counts. Don't extend the regex in this step; record real misses
  under *Open questions*.
- Check that no reference has a raw value matching `_NA_ARCHIVE_CODE` (an archive code
  swallowed as an inventory number).

Expected findings for the report:
- the cross-archive note count per archive, and the distinct targets (behaviour-change);
- the miss categories with counts (open-issue for real misses);
- the number of swallowed archive codes removed (bug-fixed).

## Do not touch

- `stream_analysis/`, NLP (phase 03); AS `ead_fields.py`, `ead_digital.py` (steps 04 and 06).
- LotA `facets/`, `ead/hierarchy.py`, `ead/digitisation.py`.

## When done

1. Write your `### Step 05: …` section in `reports/phase-02-ead-for-lota.md`. Follow
   `plan/templates/REPORT.md`: what changed, results, error analysis (what was read and how it
   was sampled, categories with counts, verbatim examples, a conclusion), and open issues. Add
   one *Findings* row per finding (`F02-05.n`, status `draft`); a negative result is a
   finding too. Don't edit other sections. Run `make reports` from the AS root; it must
   print no warnings.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: this step done, next step, open questions.

```markdown
### Step 05: C3 scope-note parser into AS, with cross-archive references
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 05; findings F02-05.1–n
- **Error analysis:** <two lines at most; the details are in the report>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
