# Step 04: C5 `DigitisationSummary.has_evidence(mechanisms)`

> **Phase:** 02, EAD for LotA. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Which digitisation-linking mechanisms count as evidence that scans exist differs per archive.
For the Nationaal Archief, only `dao` counts: a handle is assigned when an inventory number is
created, before any digitisation. For BHIC it is `daogrp`, and for Amsterdam `assets`. LotA
answers "does this node have evidence?" with two identical private `_has_evidence` helpers. AS
gets one public method, and LotA delegates to it. `is_digitised` keeps its current,
handle-inclusive meaning for backward compatibility (decision P2), and its docstring gains a
warning that a handle is not evidence of digitisation. The optional per-repository default map
in `ead_survey.py` is **not** built (planning decision, 2026-10-14).

## Inputs

AS `archival_structures/parsers/ead_digital.py` (259 lines), checked 2026-10-14. Imports
(L27–32): `from dataclasses import dataclass, field`, `from typing import Dict, List, Optional`.

```python
@dataclass
class DigitisationSummary:            # L209–249
    dao: List[DaoReference] = field(default_factory=list)
    daogrp: List[DaoGroup] = field(default_factory=list)
    assets: Optional[AssetList] = None
    handle_urls: List[str] = field(default_factory=list)

    @property
    def mechanisms(self) -> List[str]:   # L220: subset of ["dao", "daogrp", "assets", "handle"], that order
    @property
    def is_digitised(self) -> bool:      # L234–235
        return bool(self.mechanisms)
    @property
    def n_scans_known(self) -> Optional[int]: ...

def summarise_digitisation(component) -> DigitisationSummary  # L252
```

AS test style: `tests/test_parsers_ead_digital.py` (15 tests, `unittest`). Its helper
`_component(xml)` (L17–27) wraps `xml` in `<ead><eadheader><eadid>1</eadid></eadheader><archdesc><dsc>{xml}</dsc></archdesc></ead>`
and returns `finding_aid.roots[0]`. `class TestSummary` is at L158. Fixtures for each
mechanism are in the same file:
- `<dao>` inside `<did>` (L33–38);
- `<daogrp>` as a sibling of `<did>` (L51–76);
- `<nexus:assets>` (L92–110);
- `<unitid type="handle">` (L142–152).

LotA root: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`. **Read LotA
`CLAUDE.md` first.** Copy each LotA file to your scratchpad before editing it (no git).

- `archival_language/ead/digitisation.py` (190 lines):
  - imports `summarise_digitisation` from AS;
  - `@dataclass(frozen=True) class NodeDigitisation: node_id: str; mechanisms: tuple[str, ...]; is_digitised: bool; n_scans_known: int | None`
    (L52–57). It is built only at L68, in `node_digitisation(node: ArchiveNode) -> NodeDigitisation`
    (L60), from `summary = summarise_digitisation(node.component)`.
  - L76–78, to delete:
    ```python
    def _has_evidence(mechanisms: tuple[str, ...], evidence: frozenset[str]) -> bool:
        """Whether any mechanism on this node is one the archive declares as evidence of scans."""
        return not evidence.isdisjoint(mechanisms)
    ```
    Its one caller is L142, in `branch_coverage`: `if _has_evidence(nd.mechanisms, evidence):`.
    `evidence = frozenset(config.archives[archive_id].digitisation_mechanisms)` (L121).
  - `_is_identifier_only` (L81–84) stays as it is.
- `archival_language/corpus/coverage.py` (479 lines). L155–156 holds the same function without
  a docstring, also to delete. Its callers:
  - L236 (`node_text_coverage`);
  - L336 (`inventory_text_coverage`):
    `_has_evidence(node_digitisation(tree.node(nid)).mechanisms, evidence)`;
  - L416 (`branch_text_coverage`).

  The evidence sets are built at L205, L320 and L397, the same way as in `digitisation.py`.
- Config values (`config/archives.toml`): `1.01.02`, `1.04.02` and `NL-HaNA_2.10.01` → `["dao"]`;
  `NL-HtBHIC_SVO_1595_1649` and `NL-ZlCO_0266` → `["daogrp"]`; `NL-AsdSAA_89` and
  `NL-AsdSAA_5075` → `["assets"]`; `US-CtY-P_ypm.pbar.000001` and `GB-3624_oh` → `[]`.

## Deliverables

| File | Change |
|---|---|
| AS `parsers/ead_digital.py` | `MECHANISM_NAMES: Tuple[str, ...] = ("dao", "daogrp", "assets", "handle")`; `def has_evidence(self, mechanisms: Collection[str]) -> bool`; `is_digitised` docstring |
| AS `tests/test_parsers_ead_digital.py` | `class TestHasEvidence`, 5 tests |
| LotA `ead/digitisation.py` | `NodeDigitisation` gains `summary: DigitisationSummary = field(compare=False, repr=False)` as its last field; `_has_evidence` deleted; caller uses `nd.summary.has_evidence(evidence)` |
| LotA `corpus/coverage.py` | `_has_evidence` deleted; its three callers use `node_digitisation(...).summary.has_evidence(evidence)` |
| AS `docs/shared_library/changelog.md` | `### Step 4: …` under *Unreleased*, added with **Edit** |
| AS `reports/phase-02-ead-for-lota.md` | `### Step 04: …` section (results, error analysis, draft findings `F02-04.n`); then `make reports` |
| LotA `STATUS.md` | append a dated section |

## Behaviour to implement

```python
def has_evidence(self, mechanisms: Collection[str]) -> bool:
    """Whether this component carries any of `mechanisms`, the mechanisms that the archive
    declares as evidence that scans exist (e.g. `{"dao"}` for the Nationaal Archief, where
    every inventory number has a handle whether or not it is digitised).
    An empty `mechanisms` gives False."""
```

- If `mechanisms` is a `str`, raise `TypeError` (`"dao"` would otherwise iterate as letters).
- If any name is not in `MECHANISM_NAMES`, raise `ValueError` listing the unknown names. This
  catches typos in configuration.
- Otherwise return `not set(mechanisms).isdisjoint(self.mechanisms)`.

Add to the `is_digitised` docstring: "True if any of the four mechanisms is present,
**including a bare handle**. A handle is not evidence that scans exist (the Nationaal Archief
assigns one to every inventory number); use `has_evidence` with the archive's declared
mechanisms for that." Don't change its behaviour.

LotA: put `summary` last in `NodeDigitisation`, so the existing keyword construction at L68
only gains `summary=summary`. `compare=False` keeps equality as it is today.

## Constraints

- Don't change `is_digitised`, `mechanisms` or `n_scans_known`.
- Don't add a per-repository default map to `ead_survey.py`.
- `branch_coverage`, `dark_branches` and the coverage functions keep their signatures and
  outputs.
- Never `git commit`; never bump the version; `chmod g+w` new files.

## Tests

AS, `class TestHasEvidence`, using the existing fixtures:
- `test_dao_is_evidence_when_declared`: a `<dao>` component, `has_evidence({"dao"})` is True,
  and `has_evidence({"daogrp"})` is False;
- `test_handle_only_is_not_evidence_for_dao_archive`: a handle-only component has
  `is_digitised` True and `has_evidence({"dao"})` False, but `has_evidence({"handle"})` True;
- `test_empty_declaration_is_never_evidence`: `has_evidence(())` is False for a `<dao>`
  component;
- `test_unknown_mechanism_name_raises`: `has_evidence({"doa"})` raises `ValueError`;
- `test_bare_string_raises`: `has_evidence("dao")` raises `TypeError`.

Commands:
- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_parsers_ead_digital.py`,
  then the full suite. The baseline is 582 passed, or the phase `STATUS.md` count.
- LotA: `venv/bin/python -m pytest -q -p no:cacheprovider tests/test_digitisation.py tests/test_text_coverage.py`,
  then the full suite with `--ignore=tests/test_derive_voc.py`. The baseline is 320, or the
  `STATUS.md` count.

## Acceptance criteria

1. The AS suite gains exactly 5 tests and all pass.
2. LotA `test_digitisation.py` (10 tests, real SG/VOC) and `test_text_coverage.py` (11) pass
   unchanged.
3. **Identical outputs.** Before editing, a scratch script saves
   `branch_coverage(ArchiveTree.load(aid))` and the three `corpus/coverage` frames for SG and
   VOC to your scratchpad. Use the inputs `test_text_coverage.py` uses; if they need staged
   data you don't have, do `branch_coverage` only and say so. After editing, the script's
   output must be `DataFrame.equals` to the saved frames.
4. `grep -rn "_has_evidence" archival_language` returns nothing.

The user decides when the step is done.

## Error analysis

For each of the 9 registered archives:
- count units of description (`tree.frame()` rows) where `summary.is_digitised` is True but
  `summary.has_evidence(declared)` is False;
- read 5 such rows per archive (title, mechanisms);
- confirm they are handle-only, or another undeclared mechanism.

Report the table: archive, declared mechanisms, n units, n `is_digitised`, n `has_evidence`.
For SG, `has_evidence` should be about 60.7% of units. That is a sanity check, not an
assertion; report the actual figure.

Expected findings for the report: the per-archive `is_digitised` versus `has_evidence` table
(data-property), and the SG `has_evidence` share against the user's "about 60%".

## Do not touch

- `stream_analysis/`, NLP (phase 03).
- LotA `ead/hierarchy.py`, `ead/scopecontent*.py`, `facets/` (other steps).

## When done

1. Write your `### Step 04: …` section in `reports/phase-02-ead-for-lota.md`. Follow
   `plan/templates/REPORT.md`: what changed, results, error analysis (what was read and how it
   was sampled, categories with counts, verbatim examples, a conclusion), and open issues. Add
   one *Findings* row per finding (`F02-04.n`, status `draft`); a negative result is a
   finding too. Don't edit other sections. Run `make reports` from the AS root; it must
   print no warnings.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: this step done, next step, open questions.

```markdown
### Step 04: C5 DigitisationSummary.has_evidence
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 04; findings F02-04.1–n
- **Error analysis:** <two lines at most; the details are in the report>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
