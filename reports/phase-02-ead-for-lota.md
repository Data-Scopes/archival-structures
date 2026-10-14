# Phase 02 report: EAD for LotA

> **Status:** final (phase review, 2026-10-14).
> **Phase plan:** [`plan/phase-02-ead-for-lota/PLAN.md`](../plan/phase-02-ead-for-lota/PLAN.md).
> **Code state:** AS unreleased after v0.5.0 (to be released as v0.6.0); LotA 2026-10-14 (step 06).
> **Last updated:** 2026-10-14, by the phase review (Opus).

## Summary

Phase 02 moved LotA's generic EAD logic into AS, and LotA now uses the AS versions. The moved
pieces are node ids (`ead_views` frames take `archive_id`), digitisation evidence
(`DigitisationSummary.has_evidence`), the scope-note parser (new `parsers/ead_scope_notes`, with
`ScopeReference.target_archive`) and structural `<unitdate>` excision from titles
(`ead_fields.split_unittitle_dates`). LotA's copies are deleted, and `lb_adjusted` is retired.
Every regenerated LotA output differs from its baseline only as each step predicted. The review
regenerated all outputs from the current code and got cell-identical files (F02-R.1).
What a reader should know:

- 91 SG + VOC scope notes point into another archive (83 of them to Aanwinsten `1.11.01.01`),
  and they now carry that target instead of joining to the current inventory (F02-05.1).
- A handle is not evidence of scans: SG `is_digitised` overstates `has_evidence` by 7,079 units
  (39.5%), concentrated in whole series (F02-04.2, F02-R.3).
- The marker-in-walk date excision is exact on 40,207 NA titles. In 45% of SG titles it leaves
  a trailing separator in `prose` (F02-R.5, F02-06.5).
- AS frames used on LotA archives must pass `archive_id`: it differs from `<eadid>` in 7 of 9
  archives (F02-03b.2).

## Findings

| ID | Finding (one sentence, with the key number) | Kind | Tags | Status | Section |
|---|---|---|---|---|---|
| F02-P.1 | LotA outputs were not regenerated after AS step 2 (scope notes 2026-10-03, titles 2026-10-08), which led to step 03c | open-issue | lota, scope-notes, titles | confirmed | [Planning](#planning) |
| F02-P.2 | LotA's old inventory regex would swallow an archive code as an inventory number in a mixed list (`inv.nr. 12, 1.11.01.01, inv.nr. 687` gives `12, 1.11.01.01`). This is a property of the regex, not of the data: no such list occurs in SG or VOC (F02-05.3). *Revised by the review: kind was data-property* | open-issue | lota, inventory-numbers | revised | [Planning](#planning) |
| F02-P.3 | lxml rejects control characters in element text, so the span markers live in the text walk | method | titles, ead | confirmed | [Planning](#planning) |
| F02-P.4 | The marker-in-walk invariant holds on 22,197 SG and 14,505 VOC titles | result | sg, voc, titles | confirmed | [Planning](#planning) |
| F02-P.5 | LotA `ArchiveNode`'s docstring overstates its equality | open-issue | lota, node-id | confirmed | [Planning](#planning) |
| F02-P.6 | `_segments` ignores the `<lb/>`-before-punctuation rule (ported verbatim) | open-issue | ead, titles | confirmed | [Planning](#planning) |
| F02-03b.1 | `frame()` rows and node ids are identical before and after on all 9 archives (584 AS and 320 LotA tests pass) | result | lota, node-id, ead | confirmed | [Step 03b](#step-03b-lota-switches-to-as-node-ids) |
| F02-03b.2 | `default_archive_id` differs from the LotA archive id for 7 of 9 archives, so AS frames used on LotA archives must pass `archive_id` | data-property | lota, node-id, ead | confirmed | [Step 03b](#step-03b-lota-switches-to-as-node-ids) |
| F02-03b.3 | `NL-HtBHIC_SVO_1595_1649` has 0 unit-of-description rows; pre-existing and not investigated | open-issue | bhic, ead | confirmed | [Step 03b](#step-03b-lota-switches-to-as-node-ids) |
| F02-03b.4 | The `node_id` dtype on BHIC's empty frame went from `str` to `object`; accepted by the user (2026-10-14) | behaviour-change | bhic, node-id | confirmed | [Step 03b](#step-03b-lota-switches-to-as-node-ids) |
| F02-03b.5 | LotA `model/node.py`'s module docstring (L3–7) was stale: it said upstream has no node identity; fixed 2026-10-14 | bug-fixed | lota, node-id | confirmed | [Step 03b](#step-03b-lota-switches-to-as-node-ids) |
| F02-03c.1 | Step 2's title predictions reproduce exactly on SG + VOC: `lb_adjusted` 23 → 0, `original` 12, `text`/`prose` 11, `date_span` 2, `date_locator` 0 | result | sg, voc, titles, lota | confirmed | [Step 03c](#step-03c-lota-baselines) |
| F02-03c.2 | Scope notes are identical pre vs post step 2 (0 changed cells in 2,176 + 1,471 rows), although step 2 allowed text changes: no SG/VOC `<scopecontent>` has an `<lb/>` glued on both sides, an `<lb/>` before punctuation, or a word touching an inline child | negative-result | sg, voc, scope-notes, lota | confirmed | [Step 03c](#step-03c-lota-baselines) |
| F02-03c.3 | The other 7 archives' titles and the census are unchanged by step 2 (0 cells), except the census rows of SG and VOC (`n_lb_adjusted` 21 → 0 and 2 → 0, SG `n_tokens_total` -6) | data-property | titles, lota | confirmed | [Step 03c](#step-03c-lota-baselines) |
| F02-03c.4 | 3 of the 10 SG `original` changes inserted a space before a closing quote (`1674'` → `1674 '`); the user had AS `element_text` add `' " \u2019 \u201d` to the no-space-before set, which restores `1674'` (3 SG titles, 0 elsewhere), at the cost of ASCII opening quotes after an `<lb/>` | bug-fixed | sg, titles, ead | confirmed | [Step 03c](#step-03c-lota-baselines) |
| F02-03c.5 | The SG 11 `text` changes leave doubled punctuation from the source (`,.`, `::`, `..`): the removed spaces were the only separator | data-property | sg, titles | confirmed | [Step 03c](#step-03c-lota-baselines) |
| F02-03c.6 | The SG `lb_adjusted` count is 21 and VOC 2 (23 total). The 10 SG `original` changes have no `text` change in the same row, but only 7 of them are glue fixes: the other 3 are the space-before-quote regression of F02-03c.4, which the quote rule later reverted. *Revised by the review: the step said all 10 were glue fixes* | result | sg, voc, titles | revised | [Step 03c](#step-03c-lota-baselines) |
| F02-04.1 | `has_evidence` leaves `branch_coverage` and all three `corpus/coverage` frames `DataFrame.equals` on SG and VOC (AS 589, LotA 320 tests pass) | result | lota, digitisation | confirmed | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-04.2 | A handle is not evidence of scans: `is_digitised` overstates `has_evidence` by 7,079 SG units (39.5% of 17,937) and 338 of 386 ZlCO units, but only 84 VOC and 10 HaNA 2.10.01 units | data-property | sg, voc, digitisation, handles | confirmed | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-04.3 | SG `has_evidence` share is 60.5% of `frame()` units (10,857 of 17,937), against the brief's "about 60.7%" | result | sg, digitisation | confirmed | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-04.4 | In every checked archive the `is_digitised`-but-no-evidence units are handle-only; no other undeclared mechanism occurs | data-property | digitisation, ead | confirmed | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-04.5 | `inventory_text_coverage` was only compared on an empty quality frame (0 rows). By code reading the risk is low: its call (`corpus/coverage.py:412`) is the same `nd.summary.has_evidence(evidence)` expression as `node_text_coverage` (`:232`), which was compared on 18,055 SG and 16,548 VOC rows. *Revised by the review* | open-issue | lota, digitisation | revised | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-04.6 | `has_evidence` now raises `ValueError` on a misspelt mechanism in `archives.toml` instead of silently counting nothing; all 9 configured values are valid | behaviour-change | lota, digitisation | confirmed | [Step 04](#step-04-digitisationsummaryhas_evidence) |
| F02-05.1 | 91 notes carry a cross-archive reference: 90 of 1,471 VOC (83 to `1.11.01.01`, 6 to `2.01.27.01`, 1 to `1.01.02`) and 1 of 2,176 SG (`1.10.110`); the brief's "83" is only the Aanwinsten share | behaviour-change | voc, sg, scope-notes, lota | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.2 | SG + VOC scope-note output changes only in the `references` column (91 cells), each gaining a `:<target>` suffix; `n_references` and all other columns have 0 changes, with the same result against `post-as-step-2` and `post-quote-rule` | result | sg, voc, scope-notes, lota | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.3 | The old regex never swallowed an archive code as an inventory number in SG or VOC (0 references with a code as raw value, before or after); the `12, 1.11.01.01, inv.nr. 687` case predicted in F02-P.2 does not occur, so the fix is covered by a synthetic test only | negative-result | voc, sg, scope-notes | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.4 | The only unchanged notes with `Aanwinsten` or an archive code are 2 (1 VOC, 1 SG), neither a real miss: a cue-less `zie Aanwinsten, 1.11.01.01 (1893 29r)` and an SG range of archive numbers `1.02.01 - 1.02.18` | data-property | voc, sg, scope-notes | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.5 | Cross-archive targets are not only Aanwinsten: VOC points to `2.01.27.01` (6 notes, "Aan te vragen als Comité ...") and `1.01.02` (1 note, a 4841-4844 range kept as `unparsed`), SG to `1.10.110` (Legatie archieven) | data-property | voc, sg, scope-notes | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.6 | Cue-less cross-archive notes (`zie Aanwinsten, 1.11.01.01 (1893 29r)`) get no reference at all, so the target inventory number is not extracted from the parenthesised date-like string | open-issue | voc, scope-notes | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-05.7 | `_segments` now applies the `<lb/>`-before-punctuation rule (closes F02-P.6); 0 changed `segments` cells in SG and VOC, as expected from F02-03c.2 | bug-fixed | sg, voc, scope-notes, ead | confirmed | [Step 05](#step-05-c3-scope-note-parser-into-as-with-cross-archive-references) |
| F02-06.1 | Titles output has 0 changed cells in all 9 archives against `post-quote-rule/` (only the `lb_adjusted` column is gone), the census has 0 changed cells and `token-kinds-by-level.tsv` is `cmp`-identical (AS 618, LotA 316 tests pass) | result | lota, titles, sg, voc | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.2 | Date buckets (locatable / fallback / not locatable / empty): SG 22,126 / 0 / 0 / 7, VOC 14,498 / 0 / 0 / 7, HaNA 2.10.01 3,503 / 0 / 0 / 2; the other 6 archives have none, so the regex fallback was never reachable | result | sg, voc, titles, lota | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.3 | All 16 `None`-span titles have an empty `<unitdate normal=".."/>`: the date is then plain text in the title (`1671,`) or absent (`Deel 1`), and nothing is cut | data-property | sg, voc, titles | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.4 | In 6,263 VOC and 64 SG titles the excised date string also occurs elsewhere in `prose` (`1699 1699`, a year prefix plus a year `<unitdate>`); only the structural child is cut, as intended | data-property | voc, sg, titles | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.5 | Cutting a date leaves its separators in `prose`, and in SG this is the common case, not an edge case: 10,004 of 22,197 SG titles with a `<unitdate>` child (45%) have a `prose` ending in `,`, `;` or `:`, and 119 contain `, ,` (`Brieven van ..., , , en`). VOC has 56 and HaNA 2.10.01 has 4. Unchanged from before this step. *Revised by the review: counts added (F02-R.4)* | data-property | sg, voc, titles | revised | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.6 | Only 10 titles have both `<lb/>` and a `<unitdate>` child (8 SG, 2 VOC), not the 23 the earlier count suggested (that was all SG/VOC titles with `<lb/>`) | data-property | sg, voc, titles | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-06.7 | `prose` is now produced by AS, so a LotA test that fakes glue by patching `title_norm.element_text` must patch `split_unittitle_dates` too; the `date` token kind and `n_tokens_date` column remain, always 0 | method | lota, titles | confirmed | [Step 06](#step-06-c4-split_unittitle_dates-in-as-lb_adjusted-retired) |
| F02-R.1 | Regenerating LotA's outputs from the current code gives cell-identical files: 2 scope-note files, 9 title files, the census and `token-kinds-by-level.tsv`. Every step diff (03c, quote rule, 05, 06) reproduces to the cell (AS 618, LotA 316 tests pass) | result | lota, scope-notes, titles | confirmed | [Review](#review) |
| F02-R.2 | The `node_id` sequence of all 9 current title outputs (116,058 nodes) equals that of `pre-as-step-2/`, which LotA's old `_node_id` produced. This independently confirms step 03b on every node | result | lota, node-id | confirmed | [Review](#review) |
| F02-R.3 | SG's units without `dao` evidence cluster by series: 5.2 has 4,012 of 4,468, 1.0 has 1,962 of 3,207 and 6.0 has 290 of 293, while the resolutions (0.0) have 55 of 3,847. Handle-only looks like series not yet scanned, not scattered missing `<dao>`s | data-property | sg, digitisation, handles | confirmed | [Review](#review) |
| F02-R.4 | After date excision, `prose` ends in `,`/`;`/`:` in 10,004 of 22,197 SG titles (45%), 56 VOC and 4 HaNA 2.10.01 titles; 119 SG titles contain `, ,` | data-property | sg, voc, titles | confirmed | [Review](#review) |
| F02-R.5 | `split_unittitle_dates` is exact on all 40,207 titles of SG, VOC and HaNA 2.10.01: `text[s:e] == element_text(child)` for every non-`None` span (0 mismatches). No title has a `<unitdate>` below a direct child, so P10's direct-children rule loses nothing | negative-result | sg, voc, titles, ead | confirmed | [Review](#review) |
| F02-R.6 | P6 (`target_archive` is None when the written code equals `archive_id`) uses string equality. It is right for SG/VOC, whose LotA ids are bare NA codes, but would tag a self-reference as cross-archive under a prefixed id such as `NL-HaNA_2.10.01`. 0 notes in SG, VOC or 2.10.01 name their own code, so P6 is never exercised on real data | open-issue | scope-notes, lota, ead | confirmed | [Review](#review) |
| F02-R.7 | The cost of the quote rule (an opening ASCII quote after `<lb/>` loses its space) has 0 instances in LotA's corpus. `post-as-step-2/` and `post-quote-rule/` differ in 3 SG titles only, all closing quotes, across 9 archives' titles and SG/VOC scope notes | data-property | titles, scope-notes, ead | confirmed | [Review](#review) |
| F02-R.8 | Mid-phase follow-ups (the quote rule and its `post-quote-rule/` baseline) were not propagated into the briefs not yet run, so steps 05 and 06 both recorded the same brief defect | method | plan | confirmed | [Review](#review) |
| F02-R.9 | v0.5.0 was tagged with the step 1–3 changelog entries still under *Unreleased (after v0.4.1)*; the review added a `v0.5.0` heading and a new *Unreleased (after v0.5.0)* | bug-fixed | changelog | confirmed | [Review](#review) |

## Steps

### Planning

Planning session (Opus, 2026-10-14); no code changed. Prototypes lived in the scratchpad only.

- **F02-P.1.** LotA outputs were never regenerated after AS step 2 (scope notes 2026-10-03,
  titles 2026-10-08). This became step 03c.
- **F02-P.2.** The prototype of the new NA inventory regex matched the expected output on 8
  cases. It also showed that LotA's old regex swallows an archive code as an inventory number
  in mixed lists: `inv.nr. 12, 1.11.01.01, inv.nr. 687` gives `12, 1.11.01.01`.
- **F02-P.3.** Putting control-character markers into an lxml tree fails, because lxml rejects
  control characters in element text. The span markers therefore live in the text walk.
- **F02-P.4.** The marker-in-walk design holds the `split_unittitle_dates` invariant on 22,197
  SG and 14,505 VOC titles (counts in brief 06).
- **F02-P.5.** LotA `ArchiveNode`'s docstring overstates its equality. Step 03b fixes it.
- **F02-P.6.** `_segments` ignores the `<lb/>`-before-punctuation rule. It is ported verbatim;
  a review follow-up.

Baselines measured in planning: AS suite 582 passed (97 s); LotA `test_hierarchy` +
`test_digitisation` 22 passed (23 s); LotA `test_derive_voc.py` still fails to import.

### Step 03b: LotA switches to AS node ids

**What changed.** `ead_views.to_frame(finding_aid, level, archive_id=None)` and
`ead_views.unit_of_description_frame(finding_aid, archive_id=None)` take an `archive_id`;
`None` means `finding_aid.default_archive_id`, as before. LotA's `ArchiveTree` now builds its
`frame()` from the AS frame and uses AS node ids (`archival_language/ead/hierarchy.py`,
`model/node.py`); its private `_nodes_by_component_id` is now `_nodes_by_component`. The LotA
public surface is unchanged. Brief: `plan/phase-02-ead-for-lota/step-03b-*.md`. The step ran
on Opus, not Sonnet.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| `frame()[["node_id","citation"]]` before/after, 9 archives | identical rows, same order | identical |
| Full frame (columns, order, index), 8 non-empty archives | identical | identical |
| Walk node ids, `parent_id`, `ancestor_ids`, 9 archives | identical | identical |
| AS full suite | pass | 584 passed (101 s) |
| AS targeted (`test_parsers_ead_views.py`, `test_parsers_ead_node_id.py`) | pass | 25 passed |
| LotA suite (`--ignore=tests/test_derive_voc.py`) | pass | 320 passed (56 s) |
| Grep for the old join | no output | no output (exit 1) |

Frame rows per archive, before and after (identical):

| Archive | Frame rows | Identical |
|---|---|---|
| SG | 17,937 | yes |
| VOC | 16,447 | yes |
| NL-AsdSAA_5075 | 32,224 | yes |
| NL-HaNA_2.10.01 | 4,683 | yes |
| US-CtY-P_ypm.pbar.000001 | 1,426 | yes |
| NL-ZlCO_0266 | 386 | yes |
| NL-AsdSAA_89 | 150 | yes |
| GB-3624_oh | 3 | yes |
| NL-HtBHIC_SVO_1595_1649 | 0 | `node_id` dtype `str` → `object` (empty frame) |

**Error analysis.**

- *What was read:* for the 7 archives whose `default_archive_id` differs from the LotA id, 3
  rows each (first, middle, last); and 10 seeded random rows per archive for titles
  (`random_state=42`; 3 for GB, 0 for BHIC), 73 rows in all.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Archives where `default_archive_id` ≠ LotA id | 7 of 9 | `89:5.0` vs `NL-AsdSAA_89:5.0`; `gb3624-oh:8.9.2.3` vs `GB-3624_oh:8.9.2.3` |
  | Archives where the ids match | 2 of 9 | SG, VOC |
  | Title mismatches in the 73 sampled rows | 0 | — |

  The seven archives and their `default_archive_id` are: BHIC `SVO_1595_1649` (0 rows),
  US-CtY-P `ypm.pbar.000001`, GB-3624_oh `gb3624-oh`, NL-AsdSAA_89 `89`, NL-HaNA_2.10.01
  `2.10.01`, NL-AsdSAA_5075 `5075`, NL-ZlCO_0266 `0266`.
- *Conclusion:* in every compared case the suffixes match and only the prefix differs. Without
  the `archive_id` argument, `frame()`'s membership check would raise for all 7 archives.

**Findings:** F02-03b.1, F02-03b.2, F02-03b.3, F02-03b.4, F02-03b.5.

**Open issues.** None. The user accepted the dtype change and the `model/node.py` module docstring
was updated on 2026-10-14 (docstring only; LotA `test_hierarchy` 11 passed).

### Step 03c: LotA baselines

**What changed.** Brief: [`step-03c-lota-output-baselines.md`](../plan/phase-02-ead-for-lota/step-03c-lota-output-baselines.md).
No AS code changed. In LotA: `output/baselines/pre-as-step-2/` (`cp -p` of the 2 scope-note files, 9 title files, census and token-kinds files, all `cmp`-identical to the canonical files),
`output/baselines/post-as-step-2/` (regenerated; `parse_scopecontent` 3.8 s, `normalise_titles` 19.7 s),
`output/baselines/README.md`, `archival_language/cli/diff_outputs.py` (`diff_frames`, `main`) and
`tests/test_diff_outputs.py` (3 tests). `output/scope-notes/` and `output/titles/` were not touched.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| 1. Baselines | 2 notes + 9 titles + census + token-kinds in post; pre `cmp`-identical | as expected |
| 2. Scope notes keys | identical, 2,176 + 1,471 | identical; **0 changed cells** in all columns |
| 3. Titles SG + VOC | 23 / 12 / 11 / 2 / 0 | 23 (21 SG + 2 VOC) / 12 (10 + 2) / 11 (11 + 0) / 2 (VOC) / 0 |
| 4. Other 7 archives | no prediction | 0 changed cells in every column |
| 5. Census | report changes | 4 cells: `n_lb_adjusted` SG 21 → 0, VOC 2 → 0; SG `n_tokens_total` 390,362 → 390,356, `n_tokens_punct` 70,399 → 70,393 |
| 6. LotA suite | green | 323 passed (320 + 3 new) |

Titles, SG: `original` 10, `text` 11, `prose` 11, `n_tokens` 6, `lb_adjusted` 21, rows with any change 21 of 28,573.
VOC: `original` 2, `date_span_start/end/spans` 2, `lb_adjusted` 2, rows 2 of 17,299. Changed cells are in
`reports/data/phase-02-ead-for-lota/03c-*.tsv`.

**Error analysis.**

- *What was read:* all 23 changed SG/VOC title rows (old and new, with a character diff of the changed spans); the 4 census cells; 10 unchanged titles across all 9 archives (seeded random, `random_state=42`); the raw EADs for SG and VOC, for the `<lb/>` patterns inside `<scopecontent>`. No row changed in the other 7 archives, and none in scope notes, so there was nothing else to read.
- *Categories (23 changed titles):*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | `<lb/>` glue fixed in `original` (word or sentence glue) | 12 (SG 10, VOC 2) | `bemiddelaarop te treden` → `bemiddelaar op te treden`; `eerstedeel` → `eerste deel` |
  | spurious space before punctuation removed in `text` | 11 (SG) | `Oude omslag .` → `Oude omslag.`; `Perzië] ).` → `Perzië]).` |
  | of the `original` changes, a space inserted before a closing quote | 3 (SG) | `Dec 1674'` → `Dec 1674 '`; `[...]'.` → `[...] '.` |
  | `date_span` shifted by one (follows the glue fix) | 2 (VOC) | `39:43` → `40:44` in `1768. Eerste boek: Batavia, eerstedeel 1768` |
  | other | 0 | |

- *Scope notes:* the old and new files are cell-identical. In the raw EADs, SG has 2,194 and VOC 1,506 `<scopecontent>` blocks, 73 and 4 of them with `<lb/>`; none has an `<lb/>` with a non-space character on both sides, none has `<lb/>` before punctuation, and none has a word directly before a `date/emph/persname/geogname/extref` child. So step 2's predicted scope-note changes can exist in other EADs but not in these two.
- *Unchanged sample:* 10 of 10 look normal (date ranges, plain titles); no glue was visible.
- *Conclusion:* step 2 behaves as predicted on titles to the unit. For scope notes the change is a no-op on SG and VOC, so the post baseline is also the pre-step-2 output and the diffs of steps 05 and 06 will isolate their own change. The `<lb/>` rule leaves two small artefacts: doubled punctuation in the source (`,.`, `::`, `..`) now shows up as such, and a space is inserted before a closing quote that follows an `<lb/>`. The latter is a rule gap in AS `element_text`, not a bug in this step.

**Findings:** F02-03c.1 – F02-03c.6.

**Quote follow-up (user decision, 2026-10-14).** AS `element_text` now drops the `<lb/>` space before `'`, `"`, `\u2019` and `\u201d` (changelog entry *Step 3c follow-up*; test `test_lb_before_quote_is_dropped`). Regenerated into `output/baselines/post-quote-rule/`: the 3 SG titles above change in `original`/`text`/`prose` (`[...] '.` → `[...]'.`, 2 `n_tokens`), and nothing else changes: 0 cells in VOC, in the other 7 archives and in both scope-note files. **Steps 05 and 06 diff against `post-quote-rule/`**, not `post-as-step-2/`. The cost is that an ASCII quote opening a quotation after an `<lb/>` loses its space (`a<lb/>"b"` → `a"b"`); no case of it occurs in the 52-file corpus checks here, but the corpus was not searched for it.

**Open issues.** Opening quotes after `<lb/>` (above): not searched for. The census deltas follow from the title changes, with no separate cause.

### Step 04: DigitisationSummary.has_evidence

**What changed.** AS `parsers/ead_digital.py` gains `MECHANISM_NAMES` and
`DigitisationSummary.has_evidence(mechanisms)` (`str` raises `TypeError`, unknown names raise
`ValueError`, empty gives False); the `is_digitised` docstring now warns that a handle is not
evidence. LotA `NodeDigitisation` gains a last field `summary` (`compare=False`, `repr=False`);
the two private `_has_evidence` helpers in `ead/digitisation.py` and `corpus/coverage.py` are
deleted and their four call sites use `summary.has_evidence(evidence)`. Changelog entry
"Step 4" added. Brief: `plan/phase-02-ead-for-lota/step-04-*.md`.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| AS new tests | exactly 5, pass | 5 added; `test_parsers_ead_digital.py` 20 passed; full suite 589 passed (584 + 5) |
| LotA `test_digitisation.py` + `test_text_coverage.py` | pass unchanged | 22 passed (the brief says 10 + 11 = 21) |
| LotA full suite | pass | 320 passed |
| `branch_coverage` + 3 coverage frames, SG and VOC, before/after | `DataFrame.equals` | all 8 frames equal (see caveat) |
| `grep -rn _has_evidence archival_language` | nothing | nothing |

Caveat: the coverage frames were computed with an empty `inventory_quality` frame (the columns
the tests use), because the real quality table is not needed by the tests. So
`inventory_text_coverage` returned 0 rows; `node_text_coverage` (18,055 SG and 16,548 VOC
rows) and `branch_text_coverage` (686 and 365 rows) are real. With no staged inventories,
`text_status` splits only on evidence, which is what `has_evidence` changed.

**Error analysis.** For each archive I walked `tree.frame()` and computed `is_digitised` and
`has_evidence(declared)` per unit. I printed 5 random units (seed 0) from each archive with
`is_digitised` and no evidence, 25 in all.

| Archive | Declared | Units | `is_digitised` | `has_evidence` | Share | Handle-only |
|---|---|---|---|---|---|---|
| SG `1.01.02` | dao | 17,937 | 17,936 | 10,857 | 60.5% | 7,079 |
| VOC `1.04.02` | dao | 16,447 | 16,447 | 16,363 | 99.5% | 84 |
| `NL-HaNA_2.10.01` | dao | 4,683 | 4,683 | 4,673 | 99.8% | 10 |
| `NL-AsdSAA_5075` | assets | 32,224 | 21,754 | 21,754 | 67.5% | 0 |
| `NL-AsdSAA_89` | assets | 150 | 148 | 148 | 98.7% | 0 |
| `NL-ZlCO_0266` | daogrp | 386 | 386 | 48 | 12.4% | 338 |
| `US-CtY-P_ypm.pbar.000001` | none | 1,426 | 0 | 0 | 0% | 0 |
| `GB-3624_oh` | none | 3 | 0 | 0 | 0% | 0 |
| `NL-HtBHIC_SVO_1595_1649` | daogrp | 0 | 0 | 0 | n/a | 0 |

All 7,079 + 84 + 10 + 338 mismatching units have mechanisms `["handle"]` alone; no other
combination appears. The 25 sampled rows are all of that kind. SG examples:
`1.01.02:3.0.0.2` "1636-1638 mei.", `1.01.02:5.2.10.2.70` "Akte van volmacht te Kopenhagen
uitgevaardigd door de koning…". ZlCO examples: `NL-ZlCO_0266:321` "Hasselt 1 kaart. Kaart van
Hasselt. Schematische weergave…". VOC examples are mostly date-range titles (`1.04.02:1.4.4.29.0.2`
"1685 nov. 30 - 1686 juli 1"). Units with no mechanism at all (1 in SG, 10,470 in
`NL-AsdSAA_5075`, 2 in `NL-AsdSAA_89`) are not `has_evidence` mismatches and were not read.

The sample is small and reads the same units that the counts cover; it confirms the category
but not why SG has so many handle-only units (not investigated: scans not yet made, or a
missing `<dao>`).

**Conclusion.** The refactor is behaviour-neutral on SG and VOC. The declared-evidence rule is
doing real work in SG and ZlCO, where a handle-inclusive `is_digitised` would call nearly every
unit digitised. The SG share is 60.5%, close to the user's "about 60%" and to the brief's 60.7%
(the denominator here is all `frame()` units, not inventory numbers, which may explain the
0.2-point gap).

**Open issues.** `inventory_text_coverage` equality was not tested on real quality data.
BHIC still has 0 units (F02-03b.3), so its `daogrp` declaration is untested here.

### Step 05: C3 scope-note parser into AS, with cross-archive references

**What changed.** New AS module `archival_structures/parsers/ead_scope_notes.py` (ported from
LotA `scopecontent.py` and `scopecontent_survey.py`, rules as `NA_SHAPE_RULES` and
`NA_REFERENCE_PATTERNS` parameters). `ScopeReference.target_archive` is new, and the NA
inventory pattern reads an optional archive code before the cue. LotA `ead/scopecontent.py` is
now thin wrappers (no `re.compile`), `scopecontent_survey.py` imports the classifier from AS
(`SHAPE_RULES` alias kept), and the CLI appends `:{target_archive}` only when set. Brief:
[`step-05-scope-notes.md`](../plan/phase-02-ead-for-lota/step-05-scope-notes.md). Changelog entry
*Step 5*. LotA scope notes regenerated.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| AS suite | green, 16 new tests | 606 passed (590 + 16) |
| LotA suite | green, `STATUS` count minus 6 | 317 passed (323 - 6) |
| Keys identical, only `references`/`n_references` change | yes | keys identical; only `references` changed: SG 1, VOC 90 cells; `n_references` 0 |
| Changed `references`: suffix added / swallowed code dropped | counted separately | 91 / 0 |
| Notes with a cross-archive reference | reported, not asserted | VOC 90, SG 1 |
| Distinct targets | reported | `1.11.01.01` 83 and `2.01.27.01` 6 and `1.01.02` 1 (VOC refs); `1.10.110` 1 (SG) |
| `grep re.compile` in LotA `scopecontent.py` | nothing | nothing |

The diff gives the same numbers against `post-as-step-2/` and `post-quote-rule/` (the quote rule
touches titles only). Diff tables: scratchpad only.

**Error analysis.**

- *What was read:* all 1 SG changed row, and 30 of the 90 VOC changed rows (seeded sample,
  `random_state=1`); plus the 7 VOC rows whose target is not `1.11.01.01`, in full. All unchanged
  rows with `Aanwinsten` or an archive code (2). Source: the regenerated
  `output/scope-notes/*.tsv.gz` against the baseline.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Changed: suffix added, same references otherwise | 91 | `Zie Aanwinsten, 1.11.01.01, inv.nr. 541, (1891 8i).` gives `inventory:541:541::1.11.01.01` |
  | Changed: swallowed archive code dropped | 0 | none in SG or VOC |
  | Target other than Aanwinsten | 8 | `Aan te vragen als Comité tot de Zaken van de Oost-Indische Handel en Bezittingen, 2.01.27.01 inv.nr. 66` (6); `zie Archief Staten-Generaal, 1.01.02, inv.nrs. 4841-4844.` gives `unparsed:4841-4844:::1.01.02`; SG `... Legatie archieven, 1.10.110, inv. nr. 458[2]` |
  | Unchanged, cue-less (no `inv.nr.`) | 1 | `zie Aanwinsten, 1.11.01.01 (1893 29r)` |
  | Unchanged, other number | 1 | SG `... nummer toegang 1.02.01 - 1.02.18, p. 125-150.` |
  | Unchanged, real miss | 0 | none |

- *Conclusion:* in all 31 changed rows read, the suffix names the archive written in the text.
  The regex is specific enough for this data: the 3 rows with a non-Aanwinsten target are correct.
  The archive-code-swallowing bug predicted in the inventory has no instance in SG or VOC, so the
  real-data effect of the fix is only the `target_archive` suffix. One consequence for LotA: the
  `inv.nrs. 4841-4844` range stays `unparsed` but now carries its target, so it can be excluded
  from the VOC join as well. No raw value matches an archive code in either output.

**Follow-up (user request, same day).** `_segments` now drops the `<lb/>` space before
punctuation, using the same regex as `element_text`; test
`test_lb_before_punctuation_adds_no_space_in_segments`. Regenerated SG and VOC: `segments`
0 changed cells, so the earlier verbatim port was output-neutral on this data (F02-05.7).

**Findings:** F02-05.1 to F02-05.7.

**Open issues.** Cue-less cross-archive notes yield no reference (F02-05.6); the
regex was not extended in this step. No LotA code outside the CLI reads `references` yet (grep of `archival_language/`), so the
"never join a targeted reference to the current inventory" rule is not exercised anywhere in LotA.

### Step 06: C4 `split_unittitle_dates` in AS; `lb_adjusted` retired

**What changed.** New `ead_fields.split_unittitle_dates` (one walk of the `<unittitle>` with the
`<unitdate>` children bracketed by private markers, then `_collapse_with_map`; the collapsed
string is asserted equal to `element_text`). The marker hook in `ead_text` is private. LotA
`normalise_title` calls it; the sentinel machinery, the regex fallback, `_retag_ranges` and
`lb_adjusted` (field, column, census column) are deleted. LotA `output/titles/` regenerated. Brief:
[`step-06-unittitle-dates.md`](../plan/phase-02-ead-for-lota/step-06-unittitle-dates.md).
Changelog entry *Step 6*.

**Results.**

| Criterion | Expected | Measured |
|---|---|---|
| AS suite | green, 11 new tests | 618 passed (607 + 11) |
| LotA suite | green, `test_title_norm.py` has 22 tests | 316 passed (317 - 1); 22 tests |
| Titles diff vs `post-quote-rule/`, all 9 archives | keys identical, only `lb_adjusted` removed, 0 changed cells | yes: keys identical, `lb_adjusted` only in OLD, 0 changed cells, 0 changed rows |
| Census | `n_lb_adjusted` gone, every other cell identical; token-kinds `cmp` | yes (0 differing cells); `cmp` identical |
| `_fallback_located` | 0 | 0 in all 9 |
| `grep lb_adjusted\|_structural_date_spans\|_DATE_SENTINEL` | nothing | nothing |
| Real-data invariant (SG, VOC) | 22,197 / 14,505 titles, 7 / 7 `None`, 680 / 194 multi | reproduced (asserted in `test_real_titles_invariant`) |

Date buckets per archive (`n_date_unittitle_*`; `_fallback_located` is 0 everywhere):

| Archive | titles with `<unitdate>` child | locatable | not locatable | empty | `n_date_spans_excised` |
|---|---|---|---|---|---|
| NL-HaNA_1.01.02 (SG) | 22,133 | 22,126 | 0 | 7 | 23,076 |
| NL-HaNA_1.04.02 (VOC) | 14,505 | 14,498 | 0 | 7 | 14,692 |
| NL-HaNA_2.10.01 | 3,505 | 3,503 | 0 | 2 | 3,503 |
| other 6 archives | 0 | 0 | 0 | 0 | 0 |

The 22,197 / 3,505 in the AS count differ from the census 22,133 for SG: AS counts every title
with a direct `<unitdate>` child, the census counts titles whose reported date comes from the
`<unittitle>` (a `<did><unitdate>` takes precedence). The old "6,264 ambiguous / 125 not
locatable" figures came from `str.find`; the structural route has no such cases.

**Error analysis.**

- *What was read:* via a scratchpad script over the AS parse of SG, VOC and HaNA 2.10.01 (the
  only archives with a `<unitdate>` child; 40,207 titles): 30 random titles (seed 6), all 10
  titles with `<lb/>` and `<unitdate>`, 15 random titles with 2 or more children (of 874), all 16
  `None`-span titles. For each: `text`, the cut substrings and `prose`.
- *Categories:*

  | Category | Count | Example (verbatim, shortened) |
  |---|---|---|
  | Date-only title, `prose` empty | 15 of 30 random | `1833 dec. 7 - 17` gives `""` |
  | Trailing or mid-title date cut cleanly | 9 of 30 | `Brief van ambassadeur ..., aan griffier Ruysch, 1664 oktober 20` gives `... Ruysch,` |
  | Year twice, one structural | 4 of 30 | `Lands Welvaren: Grootboek en journaal, 1699 1699` gives `... journaal, 1699` |
  | Leading year kept, trailing cut | 2 of 30 | `1768. Eerste boek: Batavia, eerste deel 1768` gives `1768. Eerste boek: Batavia, eerste deel` |
  | `<lb/>` and `<unitdate>`, all read | 10 | `Stukken ... Met retroacta, 1640-1671` gives `... Met retroacta,` |
  | 2 or more children, read | 15 | `Brieven van Mouley Zidan, ..., 1609 mei 12, 1610 februari 2, ... en 1614 oktober 14` gives `Brieven van Mouley Zidan, ..., , , , en` |
  | `None` span: empty `<unitdate/>` | 16 | `<unittitle>1671, <unitdate normal="1671" .../></unittitle>` gives `1671,` (nothing cut) |
  | Wrong cut | 0 | none |

- *Conclusion:* in all 71 reads (30 + 10 + 15 + 16; the samples may overlap) the cut strings equal the date text and
  `prose` reads as the title minus its dates. The visible blemishes are not caused by this
  step: separators left behind (`, , , en`) and a year that occurs both as plain text and as a
  `<unitdate>` are properties of the source, and the output is unchanged. The 16 `None` spans are
  empty `<unitdate/>` elements whose date is not in the element (F02-06.3). The glue repair of F4
  still comes from `_map_to_original`; with `text == element_text`, `original` and `text` agree on
  all rows (0 changed cells).

**Findings:** F02-06.1 to F02-06.7.

**Open issues.** The `date` token kind (always 0) and `n_tokens_date` census column are leftovers
of the retagging path; not removed, as no brief named them. Separators left by a cut date
(F02-06.5) are untouched.

## Review

Phase review (Opus, 2026-10-14). The code review is in
[`plan/phase-02-ead-for-lota/REVIEW.md`](../plan/phase-02-ead-for-lota/REVIEW.md). Verdict:
**accept with follow-ups**. No defect needs a review-fixes step.

**Statuses.** 33 step and planning findings are confirmed and 4 are revised:

- F02-P.2: a property of the regex, not of the data;
- F02-03c.6: 7 of the 10 SG `original` changes are glue fixes, not all 10;
- F02-04.5: the risk is low by code reading;
- F02-06.5: counts added.

None is withdrawn. Each confirmed number was re-measured, except those that depend on state
that no longer exists (03b's before snapshots, 04's before frames). For those, F02-R.2 and code
reading stand in.

**What the reviewer re-ran.**

- Both suites: AS 618 passed (100 s), LotA 316 passed (56 s).
- A fresh regeneration of every LotA output into the scratchpad, diffed against the canonical
  files: 0 changed cells (F02-R.1).
- An independent keyed diff (pandas, not LotA's `diff_outputs`) of each baseline pair:
  - 03c: SG `original` 10 / `text` 11 / `prose` 11 / `n_tokens` 6 / `lb_adjusted` 21; VOC
    `original` 2 / `date_span*` 2 / `lb_adjusted` 2; census 4 cells;
  - quote rule: 3 SG titles;
  - 05: `references` SG 1, VOC 90;
  - 06: only `lb_adjusted` dropped, 0 cells.
- Step 04's per-archive counts (SG 17,937 / 17,936 / 10,857 / 7,079 handle-only; VOC 84; HaNA
  10; ZlCO 338; AsdSAA_5075 0).
- Step 06's census buckets, the 64 / 6,263 repeated-date titles and the 10 titles with both
  `<lb/>` and `<unitdate>`.

**Error analysis by the reviewer.**

- *03c and quote rule.* All 21 changed SG title rows and both VOC rows were read, with a
  character diff of each changed span. The step's reading holds, with one correction. Of the 10
  `original` changes, 7 are glue fixes (`vanGent` → `van Gent`, `4februari` → `4 februari`,
  `Kopie.Akte` → `Kopie. Akte`) and 3 are the space-before-quote regression (`1674'` →
  `1674 '`). The step's table does list the 3 separately, but F02-03c.6 called all 10 glue
  fixes. The quote diff reverts exactly those 3. The 11 `text` changes are all a removed space
  before punctuation; 4 of them expose doubled source punctuation (`Oost-Friesland..`,
  `1788::`, `Denemarken,.`).
- *05.* The SG row and 33 of the 90 VOC rows were read (seeded shuffle, `random_state=7`; 4 of
  them are `2.01.27.01` notes, and the step had read all 7 non-Aanwinsten notes). Mechanically, all 91 changes append a target to an otherwise
  unchanged reference list (0 other changes). Every target read matches the code written in the
  text. The SG reference `inventory:4582` looked suspicious against the report's `458[2]`; the
  EAD has `inv. nr. 4582.`, so it is correct. Both unchanged notes mentioning Aanwinsten or a
  code were re-found (F02-05.4).
- *06.* The cuts were checked mechanically on all 40,207 titles (F02-R.5). By reading, 36 random
  titles (12 per archive, seed 11) cut cleanly. Date-only titles give an empty `prose`, and
  titles with a trailing date keep the comma. Of 14 sampled `prose` values with left-over
  separators, all are a trailing comma or a source construction (`1738,` is a title whose only
  date is plain text). VOC titles with a structural date after a plain-text date keep the plain
  one (`... 1787 feb. 19 1786 - 1787` → `... 1787 feb. 19`), as F02-06.4 says. No wrong cut was
  found.
- *04.* The SG handle-only units were grouped by series (F02-R.3). The step's sample of 25 read
  the same category it counted; the grouping explains where they come from.
- *P6 and the cross-archive rule.* Checked on the AS parse of the three NA archives (F02-R.6).
- *Leakage.* Not applicable. The phase computes no metric, split or fitted statistic; all
  numbers are counts and cell diffs against frozen baselines.

**Review findings:** F02-R.1 to F02-R.9.

## Follow-ups

| From | What | Where it is tracked |
|---|---|---|
| F02-R.6 | Make the P6 self-reference test robust to prefixed ids. Two options: let the caller pass the archive's own code (e.g. `own_codes=("2.10.01",)`), or compare against the code part of `archive_id`. This is needed before LotA parses scope notes of any archive whose id is not a bare NA code | `REVIEW.md` debt #1; next phase touching `ead_scope_notes` |
| F02-06.5, F02-R.4 | Decide whether `prose` should drop separators orphaned by a cut date (45% of SG titles end in one). This changes LotA output, so it needs a user decision | `REVIEW.md` debt #3; LotA title work |
| F02-06.7 | Remove the `date` token kind and the `n_tokens_date` census column (always 0). This changes LotA output columns | `REVIEW.md` debt #4; LotA |
| F02-05.6 | Cue-less cross-archive notes (`zie Aanwinsten, 1.11.01.01 (1893 29r)`, 1 VOC note) yield no reference | `REVIEW.md` observation; LotA scope-note work |
| F02-03b.3 | `NL-HtBHIC_SVO_1595_1649` has 0 unit-of-description rows (28,008 title nodes), so its `daogrp` declaration is untested | `REVIEW.md` observation; LotA |
| F02-R.8 | A session that changes a baseline, a shared constant or an acceptance number mid-phase updates every brief not yet run that names it | `plan/templates/STEP.md` and `plan/README.md` rule 10 (done by the review) |

## Reproduce

Review:

- Regenerate without the CLI's path guard. From the LotA root, run
  `PYTHONPATH=. venv/bin/python <script> <dir>`, where the script calls
  `parse_scopecontent.run(DEFAULT_ARCHIVE_IDS, ("scopecontent",), dir/"scope-notes", limit=None)`
  and `normalise_titles.run(all ids, dir/"titles", config=load_config())`, then writes the
  census. This takes 2 s and 18 s.
- Diff with pandas. Read every file as `dtype=str, keep_default_na=False` and outer-merge on
  `node_id[,source_tag,element_index]` (census: `archive_id`).
- F02-R.3 to R.6: `ead_tree.parse_ead` on LotA `eads/NL-HaNA_{1.01.02,1.04.02,2.10.01}.xml`,
  then `split_unittitle_dates` and `iter_scope_notes(fa, <LotA id>)` over every component.

Step 06:

- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider` (618 passed). LotA, from its root:
  `venv/bin/python -m archival_language.cli.normalise_titles` (about 3 min in total with the census),
  then `venv/bin/python -m archival_language.cli.diff_outputs output/baselines/post-quote-rule/titles/<archive>-normalised-titles.tsv.gz output/titles/<archive>-normalised-titles.tsv.gz --key node_id`
  per archive; `venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py` (316 passed).

Step 03c:

- LotA, from its root: `venv/bin/python -m archival_language.cli.parse_scopecontent --out output/baselines/post-as-step-2/scope-notes`;
  `venv/bin/python -m archival_language.cli.normalise_titles --out output/baselines/post-as-step-2/titles`;
  `venv/bin/python -m archival_language.cli.diff_outputs output/baselines/pre-as-step-2/<f> output/baselines/post-as-step-2/<f> --key <node_id[,source_tag,element_index]>`
  (census: `--key archive_id`).

Step 03b:

- AS: `.venv/bin/python -m pytest -q -p no:cacheprovider` (584 passed); targeted:
  `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_parsers_ead_views.py tests/test_parsers_ead_node_id.py`.
- LotA, from its root:
  `venv/bin/python -m pytest -q -p no:cacheprovider --ignore=tests/test_derive_voc.py` (320 passed).
- Snapshot method: pickle `ArchiveTree.frame()` for the 9 archives before and after the change
  (scratchpad `before/`, `after/`), then compare `node_id`/`citation`, the full frames, and the
  walk's `node_id`, `parent_id` and `ancestor_ids`. Titles: 10 rows per archive with
  `random_state=42`.
