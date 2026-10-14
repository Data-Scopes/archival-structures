# Step 06: C4 `split_unittitle_dates` in AS; retire LotA `lb_adjusted`

> **Phase:** 02, EAD for LotA. **Depends on:** 03c (baselines, `diff_outputs`).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

LotA's title normaliser cuts structurally marked dates (`<unitdate>` children of
`<unittitle>`) out of the title text. It does this by rebuilding the text twice with sentinel
characters, plus a regex fallback for when the rebuild does not match the title. With AS's
`element_text` (step 2), this can be done in one walk that cannot mismatch, so the excision
moves into AS `parsers/ead_fields.py` and LotA calls it. The fallback becomes unreachable and is
deleted. LotA's `lb_adjusted` column has been always False since step 2, and is retired (user,
2026-10-14). The LotA titles output must stay identical apart from that column.

## Inputs

LotA root: `/Users/Shared/claude_projects/data_scopes/language-of-the-archive`. **Read LotA
`CLAUDE.md` first.** Copy each LotA file to your scratchpad before editing it (no git). Line
numbers were checked on 2026-10-14.

AS `archival_structures/parsers/ead_text.py` (66 lines):
- `_LB_MARK = "\x00"` (L9);
- `_NO_SPACE_BEFORE = re.compile(_LB_MARK + r"(?=[.,;:!?)\]])")` (L12);
- `_walk` (L22–33);
- `raw_element_text(el)` (L36–42): the `<lb/>` rule applied, no collapse;
- `element_text(el) = " ".join(raw_element_text(el).split())` (L45–66).

AS `parsers/ead_fields.py` (318 lines) already imports `element_text` and `local_name`. The
dates section is L200–246 (`UnitDate`, `parse_unitdate`, which reads `<did><unitdate>` first,
else the **first direct** `<unitdate>` child of `<unittitle>`). Tests are in
`tests/test_parsers_ead_fields.py` (`unittest`; `class TestUnitDate` at L206).

LotA `archival_language/facets/title_norm.py` (725 lines):
- L354 `_DATE_SENTINEL`, L356 `_DATE_START, _DATE_END`.
- L359–373 `_collapse_with_map(raw) -> (collapsed, index_map)`: `index_map[i]` is the
  collapsed index of each non-space raw character, or -1. **Port it into AS** as a private
  helper.
- L376–381 `_unitdate_children` (direct children only).
- L384–425 `_structural_date_spans`, L442–475 `_structural_text_and_prose`: to delete.
- `normalise_title(node, *, config=None) -> NormalisedTitle` L478–609:
  - L513 `text = element_text(unittitle_element)`;
  - L514–518 compute `lb_adjusted`;
  - L530–561 are the structural route (`text, prose = built`; `date_spans` = the non-None
    spans; if `date_source == "unittitle" and date_text` and the first child's span is not
    None, then `date_span` = that span and `date_locator = "structural"`), followed by the
    `elif` regex fallback (`date_locator = "fallback"`, or `retag_ranges` when there are 2 or
    more hits);
  - L563–569 tokenise `prose` and call `_retag_ranges(raw, prose, retag_ranges, "date")`;
  - L571–592 spelling, `_map_to_original(..., skip_spans=date_spans)`, F4 repairs. Keep these.
- `lb_adjusted` sites:
  - `NormalisedTitle` field L144;
  - untitled branch L508;
  - L514–518;
  - L605;
  - `TITLE_FRAME_COLUMNS` L678;
  - `title_frame` L720.

LotA `archival_language/cli/normalise_titles.py` (286 lines), `census_row()` L109–204:
- `n_lb_adjusted` (L47, L116, L133–134, L186);
- `n_other_glue_defect` counts `text != original and not lb_adjusted`;
- date buckets L139–149: `n_date_unittitle_empty / _locatable / _fallback_located /
  _not_locatable`, and `n_date_spans_excised` (L138).

LotA tests: `tests/test_title_norm.py` (452 lines, 23 tests):
- `test_lb_adjusted_flag` at L183 (delete);
- `test_lb_and_unitdate_use_structural_locator` asserts `nt.lb_adjusted is False` at L210
  (drop that line);
- `tests/test_title_vocab.py:54` constructs `NormalisedTitle(..., lb_adjusted=False)` (drop the
  keyword).

The 5 LotA date tests to port (their XML and expectations):
1. `<unittitle>Brief van de <lb/>derde<unitdate normal="1650-09">September 1650</unitdate> daarna</unittitle>`:
   the span text is `September 1650`, and neither `September` nor `1650` is in the prose.
2. `<unittitle>1768. Eerste boek, eerste deel <unitdate normal="1768">1768</unitdate></unittitle>`:
   the span start is `text.rindex("1768")`, and the prose starts with `1768.`.
3. `<unittitle>Stukken <unitdate normal="1576-10/1577-05">1576 oktober 1 - 1577 mei  18</unitdate> ontvangen</unittitle>`:
   `" ".join(text[s:e].split()) == "1576 oktober 1 - 1577 mei 18"`, and the prose is
   `Stukken ontvangen`.
4. `<unittitle>Brief <unitdate normal="1600">1600</unitdate> en <unitdate normal="1601">1601 mei</unitdate> klaar</unittitle>`:
   the span texts are `["1600", "1601 mei"]`, and the prose is `Brief en klaar`.
5. `<unittitle>Brief <unitdate>1600</unitdate> klaar</unittitle>`: the prose is `Brief klaar`.

## Deliverables

| File | Change |
|---|---|
| AS `parsers/ead_text.py` | private hook only. Add `_SPAN_START = "\x01"` and `_SPAN_END = "\x02"`. `_walk(element, parts, marked=frozenset())`: a child in `marked` contributes `_SPAN_START`, its walk, then `_SPAN_END`. `_NO_SPACE_BEFORE` becomes `_LB_MARK + r"(?=[\x01\x02]*[.,;:!?)\]])"`. New `_raw_marked_text(element, marked) -> str`: like `raw_element_text`, with the markers left in. The markers live only in Python strings, never in the lxml tree; lxml rejects control characters, as found in planning. XML 1.0 cannot contain U+0001/U+0002, so `element_text` is unchanged |
| AS `parsers/ead_fields.py` | `def split_unittitle_dates(unittitle_element) -> Tuple[str, List[Optional[Tuple[int, int]]]]` |
| AS `tests/test_parsers_ead_fields.py` | `class TestSplitUnittitleDates`: 5 ported, 5 new, 1 real-data test |
| LotA `facets/title_norm.py` | call AS; delete the sentinel machinery, the regex fallback and `lb_adjusted` |
| LotA `cli/normalise_titles.py` | drop `n_lb_adjusted`; `n_other_glue_defect` = `text != original` |
| LotA `tests/test_title_norm.py`, `tests/test_title_vocab.py` | as above |
| LotA `output/titles/*` | regenerated |
| AS `docs/shared_library/changelog.md` | `### Step 6: …` under *Unreleased*, added with **Edit**. List the new function, the `ead_text` hook (private), the LotA functions deleted, and the removed `lb_adjusted` field/column/census column |
| AS `reports/phase-02-ead-for-lota.md` | `### Step 06: …` section (results, error analysis, draft findings `F02-06.n`); then `make reports` |
| LotA `STATUS.md` | append a dated section |

## Behaviour to implement

`split_unittitle_dates(unittitle_element) -> (prose, spans)`. Let `text = element_text(unittitle_element)`.

- `spans` has one entry per **direct** `<unitdate>` child (local name), in document order. An
  entry is `(s, e)` with `text[s:e] == element_text(child)`, or `None` if the child has no
  non-space text. With no such children, `spans == []` and `prose == text`.
- `prose` is
  `" ".join(<text with each non-None span replaced by " ">.split())`.
- Algorithm (structural; never search the text for the date; no `deepcopy`):
  1. Take `raw = _raw_marked_text(unittitle_element, set(<direct unitdate children>))`. A
     `<lb/>` before a marker that is followed by punctuation is dropped exactly as in
     `element_text`.
  2. Strip the markers, recording each child's raw `[a, b)` (a stack, in document order).
  3. Collapse with `_collapse_with_map`.
  4. Map each span to (first mapped non-space index, last + 1), or `None`.
- **Invariant.** The collapsed string equals `element_text(unittitle_element)`. Check it with
  `assert`; it holds by construction.
- **Checked in planning.** A prototype of exactly this design (in a scratchpad) gives every
  expected value in *Tests* below. On AS's copies of SG and VOC it holds the invariant on every
  title with a direct `<unitdate>`: SG 22,197 titles (7 `None` spans, 680 with 2 or more
  children), VOC 14,505 (7 `None`, 194 with 2 or more). Your real-data test should reproduce
  these counts; assert them.

**LotA `normalise_title`:**
- Replace L530–561 with:
  - `prose, spans = split_unittitle_dates(unittitle_element)`;
  - `date_spans = tuple(s for s in spans if s is not None)`;
  - if `date_source == "unittitle" and date_text and spans and spans[0] is not None`, then
    `date_span = spans[0]` and `date_locator = "structural"`.
- `text` stays `element_text(unittitle_element)`.
- Delete the `elif` fallback. It is unreachable, because `date_source == "unittitle"` implies a
  direct `<unitdate>` child. Delete `retag_ranges` and the `_retag_ranges` call. Delete
  `_retag_ranges`, `_unitdate_children` and `_collapse` only if `grep` shows no other
  reference.
- Remove `lb_adjusted` at every site listed above.
- Update the module docstring where it describes the date mechanism or `lb_adjusted`.
  Mechanism (b), `_split_fallback`, stays.

## Constraints

- No other LotA output column may change. Don't touch abbreviation handling, spelling, F4
  repairs or `_split_fallback`.
- `parse_unitdate` and `element_text` behaviour unchanged. The existing 11 tests in
  `tests/test_parsers_ead_text.py` must pass.
- Never `git commit`; never bump the version; `chmod g+w` new files.

## Tests

`TestSplitUnittitleDates` in AS `tests/test_parsers_ead_fields.py`, built with
`etree.fromstring("<unittitle>…</unittitle>")`:
- **ported:** the five cases above, with `text` taken from `element_text`;
- `test_lb_inside_unitdate`: `<unittitle>Akte <unitdate>1 maart<lb/>1700</unitdate> ok</unittitle>`
  gives the span text `1 maart 1700`;
- `test_lb_before_punctuation_after_unitdate`:
  `<unittitle>Brief <unitdate>1600<lb/></unitdate>. Klaar</unittitle>`. Here `text` is
  `Brief 1600. Klaar`, the span text is `1600`, and the prose is `Brief . Klaar`;
- `test_empty_unitdate_gives_none`: `<unittitle>Brief <unitdate/> klaar</unittitle>` gives
  `spans == [None]` and the prose `Brief klaar`;
- `test_no_unitdate`: `spans == []` and `prose == text`;
- `test_glued_unitdate`: `<unittitle>derden<unitdate>September</unitdate></unittitle>` gives
  the span `(6, 15)` and the prose `derden`;
- `test_real_titles_invariant` (skipUnless `data/metadata/eads/NL-HaNA_1.01.02.xml` and
  `NL-HaNA_1.04.02.xml` exist; pattern in `tests/test_parsers_ead_node_id.py` L109–112): for
  every component whose `unittitle_element` has a direct `<unitdate>`, `text == component.title`
  and every non-None span gives `element_text(child)`.

Commands:
- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_parsers_ead_fields.py tests/test_parsers_ead_text.py`,
  then the full suite.
- LotA: the full suite with `--ignore=tests/test_derive_voc.py`. Expect the `STATUS.md` count
  minus 1 (`test_lb_adjusted_flag`).

## Acceptance criteria

1. The AS suite is green, with 11 new tests.
2. The LotA suite is green, and `test_title_norm.py` has 22 tests.
3. Regenerate: `venv/bin/python -m archival_language.cli.normalise_titles` (all 9 archives,
   writes `output/titles/`). Then, for each `*-normalised-titles.tsv.gz`, run
   `python -m archival_language.cli.diff_outputs <post-baseline> <new> --key node_id`
   against `output/baselines/post-as-step-2/titles/`.
   - Identical keys.
   - The only column difference is `lb_adjusted`, present only in OLD.
   - **0 changed cells** in every shared column, for all 9 archives.
4. Census: `n_lb_adjusted` is gone, and every other cell is identical to the baseline census.
   `token-kinds-by-level.tsv` is byte-identical (`cmp`).
5. Report the date buckets per archive (`n_date_unittitle_locatable / _fallback_located /
   _not_locatable / _empty`, `n_date_spans_excised`). The old figures (6,264 VOC dates
   ambiguous, 125 not locatable) came from an earlier `str.find` approach, so report the
   numbers rather than asserting them. `_fallback_located` must be 0.
6. `grep -n "lb_adjusted\|_structural_date_spans\|_DATE_SENTINEL" -r archival_language tests`
   returns nothing.

The user decides when the step is done.

## Error analysis

The output diff should be empty, so read the AS function's results directly. Read:
- 30 seeded random titles with a `<unitdate>` child, across the 9 archives;
- every title with both `<lb/>` and `<unitdate>` (STATUS reported 23 `<lb/>` titles in SG and
  VOC);
- every title with 2 or more `<unitdate>` children, up to 15;
- every title with a `None` span.

For each, print `text`, the span substrings and `prose`, and check that the prose reads as the
title minus its dates. If acceptance 3 fails, read every changed cell (up to 30) and stop and
ask the user before adapting anything.

Expected findings for the report: the date-bucket table per archive (result), the `None`-span
titles and what they are (data-property), and whether any read title's prose looked wrong.

## Do not touch

- `stream_analysis/`, NLP (phase 03); AS `ead_digital.py`, `ead_scope_notes.py` (steps 04, 05).
- LotA `ead/`, `facets/title_vocab.py`. The other `output/titles/` files (title_vocab, gold) are
  not regenerated. Say in LotA `STATUS.md` that they don't depend on `lb_adjusted`.

## When done

1. Write your `### Step 06: …` section in `reports/phase-02-ead-for-lota.md`. Follow
   `plan/templates/REPORT.md`: what changed, results, error analysis (what was read and how it
   was sampled, categories with counts, verbatim examples, a conclusion), and open issues. Add
   one *Findings* row per finding (`F02-06.n`, status `draft`); a negative result is a
   finding too. Don't edit other sections. Run `make reports` from the AS root; it must
   print no warnings.
2. Append the block below to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: this step done, phase ready for review, open questions.

```markdown
### Step 06: C4 split_unittitle_dates; lb_adjusted retired
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** <signatures added or changed>
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers; the date-bucket table>
- **Report:** `reports/phase-02-ead-for-lota.md` § Step 06; findings F02-06.1–n
- **Error analysis:** <two lines at most; the details are in the report>
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
