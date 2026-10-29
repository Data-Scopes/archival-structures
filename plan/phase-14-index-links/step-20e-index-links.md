# Step 20e: locators, links and `index_links_v1`

> **Phase:** 14, Index → resolution links. **Depends on:** 20c (gold), 20d (folio map, bar F passed; the map is frozen).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Step 20d built a folio map (printed folio N → scan sides). This step extracts the **locators** (folio numbers) from the
index entries of each front index, links each to its target sides, and writes the result as a derived layer
`NL-HaNA_1.01.02_index_links_v1` built from a v5 text layer (v5 itself is not rebuilt). Four locator variants are fixed
here; one is chosen on the dev gold and tested once (bars L and E). A dev build covers the 41 local front-index
volumes; silver checks (entry words, REPUBLIC) are reported without a bar; a UA hands the user the run on the full v5
layer. If 20d's bar F failed, do not start: `STATUS.md` says so.

## Inputs

- 20d API (copy of its brief; check `STATUS.md` for changes): `archival_structures.analysis.folio_map`
  (`FolioMapParams`, `FolioTarget(number, front, back, position, method, anchor_dist, confidence)`, `FolioMap(mode,
  chain, n_anchors, recto_share; locate(number) -> Optional[FolioTarget]; table() -> list[dict])`, `build_folio_map(
  scan_ids, sides, numbers, *, exclude=(), params=FolioMapParams())`); `archival_structures.analysis.index_links`
  (`front_index_run(flags, *, min_flagged=10, max_first=10, gap=20, min_after=300, max_share=0.6) -> Optional[(first,
  last, end)]`, `VolumeInputs(inv, scan_ids, sides, flags, numbers)`, `read_volume_inputs(layer_root, inv)`). 20d's
  `eval_folio.py` shows the exclusion rule: sides at indices ≤ `front_last` plus every flagged side.
- Layer `L = data/text_layer_v2_dev/19e-a/1.01.02/NL-HaNA_1.01.02_text_layer_v5/` (read only). Entries (phase decision
  P3) = `units/units/<inv>.tsv.gz` rows (`scan_id page unit_id unit_order unit_type unit_method …`) with `unit_type`
  `main_text` on the front-index sides (flagged sides with index ≤ `front_last`), any `unit_method`; text from
  `join/units/<inv>.tsv.gz` (`scan_id unit_id … text`). `mode/<inv>.json` key `"mode"` (`hand`/`print`); only hand
  volumes have `layout/pages/`.
- Gold `reports/data/phase-14-index-links/step-20c/entry_gold.tsv` (`read_id, split, inv, scan_id, page, unit_id,
  used_for, entry_verdict, locators, loc_other, target_locator, target_scan_id, folio_evidence, scans_opened, content,
  adjacent_side, evidence, note`; `locators` = `;`-separated integers). Dev volumes 3254, 3471, 3394, 3308; test 3256,
  3456, 3401, 3611, 3320, 3712, 3590, 3352. 20c's and 20d's facts in `STATUS.md` apply.
- Locator samples (planning reads): `Vriesche Compen. op de fleuijtschepen, 252: 568: 581: 594: 616: 623:`; `Geiden 170,
  375, 483.`; `Uts: openstellen der Vaart. 703, 752. Uto: de Scheepvaart aan de gang geraakt, 858.`; `… van Erechiels
  6e: 147.`; `In de continuatie van 6 Fregat, … 328.`; `Levantschen handel vide 2e. L. Hanicart 342.`
- REPUBLIC (scoring only; never an input to a link): `data/republic_segmentation_v1/lines/inv=<n>/part-0.parquet`
  (link made by 20a; columns `line_id, page_id, scan_id, line_class, text, res_page, seq, unit_type, session_id,
  resolution_id, paragraph_id, n_fragments`; `page_id` = `<scan_id>-page-<k>`, k = 2·scan−2 for the verso and 2·scan−1
  for the recto, scan = the scan number). Covered front-index volumes: 3126, 3254, 3256, 3265, 3274, 3280, 3283, 3290,
  3303, 3307, 3308, 3315, 3320 (13). Read with `pandas.read_parquet(path, columns=[...])` (pyarrow 26.0.0 in `.venv`).
- Provenance: `archival_structures.text_layer.provenance.package_info(dist_name, module_name=None)` → `{"version",
  "path", "git_head", "git_dirty_files"}` (as `build.py` writes in manifests); use `package_info("archival-structures",
  "archival_structures")`.
- coreshare: `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`; multi-worker runs via
  `coreshare.py run --project archival-structures --label 20e-links --want 4 --min 1 --wait -- <cmd> --workers '{workers}'`.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/index_links.py` | `Locator`, `LocatorParams`, `SG_MONTHS`, `SG_LOCATOR_PARAMS`, `extract_locators`, `volume_links` (below) |
| `scripts/build_index_links.py` (new) | the layer builder (below) |
| `tests/test_analysis_index_links.py` | locator and link tests (extend 20d's file) |
| `reports/data/phase-14-index-links/step-20e/` | `select.py` (dev selection), `test_bars.py`, `silver.py`, outputs |
| `data/index_links_dev/20e-a/` | dev build of the 41 volumes |
| `docs/index_links.md` (new) | schema contract of `index_links_v1`, rules, numbers, known limits; link it from `docs/text_layer.md` *Text layer v5* with one line |
| `docs/USER_ACTIONS.md` | one UA (full run), next free ID |
| `docs/shared_library/changelog.md` | `### Step 20e: index links` under *Unreleased* (Edit); affected sibling imports: none |
| `reports/phase-14-index-links.md` | `### Step 20e: …` section, draft findings `F14-20e.n` |

## Behaviour to implement

```python
@dataclass(frozen=True)
class Locator:
    number: int
    char_start: int          # offsets in the unit text
    char_end: int

@dataclass(frozen=True)
class LocatorParams:
    variant: str             # "L1" | "L2" | "L3" | "L4"
    separators: str = r"[\s.,:;]+"
    ordinal_after: str = ""  # L3+: matched right after the digits
    month_after: str = ""    # L3+: matched after r"[\s.,:]*" following the number
    quantity_after: str = "" # L3+: matched after r"[\s.,:]*" following the number
    amount_before: str = ""  # L3+: searched in the 4 characters before the number
    range_slack: int = 6     # L4

SG_MONTHS = (r"(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|augt|sep|sept|"
             r"oct|okt|nov|dec)")   # same alternation as presets._TYPE_MONTHS (private there)
SG_LOCATOR_PARAMS = {v: LocatorParams(variant=v, ordinal_after=r"(e|en|ste|de)\b", month_after=SG_MONTHS,
                                      quantity_after=r"(maenden|maanden|jaren|jaeren|dagen|daegen|weken|man|schepen|"
                                                     r"gulden|guldens|ponden|stuyvers|stuivers)\b",
                                      amount_before=r"(f|ƒ|fl|gl|£)\s*\.?\s*$")
                     for v in ("L1", "L2", "L3", "L4")}

def extract_locators(text: str, params: LocatorParams, *, folio_range: Optional[tuple[int, int]] = None) -> list[Locator]
```

All regexes case-insensitive except the upper-case test. **Candidates:** every match of `(?<!\d)\d{1,4}(?!\d)`.
**Runs:** maximal sequences of candidates where the text between consecutive ones fully matches `separators`. A run
**ends a segment** if the text after its last candidate, with leading `separators` characters removed, is empty or
starts with an upper-case letter (`str.isupper()` on its first character).
- **L1:** the candidates of the run whose remaining text is empty (the trailing run), or none.
- **L2:** the candidates of every segment-ending run.
- **L3:** L2 minus a candidate if `ordinal_after` matches right after its digits, or `month_after`/`quantity_after`
  matches after `[\s.,:]*`, or `amount_before` matches the 4 characters before it.
- **L4:** L3 minus candidates outside `[first − range_slack, last + range_slack]` when `folio_range = (first, last)` is
  given (the volume's chain number range); without a range, L4 = L3.
Return in text order.

```python
def volume_links(inputs: VolumeInputs, units: Sequence[dict], texts: Mapping[tuple[str, str], str],
                 params: LocatorParams, map_params: FolioMapParams = FolioMapParams()) -> Optional[dict]
    # None if front_index_run(inputs.flags) is None. Else {"front": (first, last, end), "map": FolioMap,
    # "entries": [unit dicts on front-index sides with unit_type main_text], "links": [row dicts]}.
    # Link row: scan_id page unit_id loc_idx number char_start char_end front_scan_id front_page back_scan_id back_page
    # method anchor_dist confidence; unlocated: method "none", targets "".
```

**Selection (`select.py`):** for each variant, extract locators from the text of each dev gold row's unit (all 20 dev
rows, `heading` rows with empty gold included), compare with gold as multisets of numbers per entry: TP = multiset
intersection; micro P, R, F1. Choose the max F1, ties → the lower variant. Write `select.tsv` and the per-entry
differences of every variant. Set the module default `DEFAULT_LOCATOR_VARIANT` to the choice.
**Test (`test_bars.py`, once):** bar L on the 40 test rows: P ≥ 0.95, R ≥ 0.90. Bar E on test rows with
`target_scan_id` not in `{"", "none"}` and `folio_evidence` in `{printed, neighbours}`: hit = the gold `target_locator`
is among the extracted numbers **and** `map.locate(target_locator).front[0] == target_scan_id`; share ≥ 0.90.

**`scripts/build_index_links.py`** (docstring: usage, inputs read, outputs, resumable). Arguments: `--layer` (a v5
layer root: the dir holding `layout/`, `units/`, …), `--out-dir` (must exist), `--inventories-file` (optional; default:
every inventory with `layout/pages/<inv>.tsv.gz`), `--workers N` (default 1; `concurrent.futures.ProcessPoolExecutor`),
`--limit N`. Writes `<out-dir>/NL-HaNA_1.01.02_index_links_v1/`: `folio_map/<inv>.tsv.gz` (`FolioMap.table()`),
`links/<inv>.tsv.gz`, `volumes.tsv` (`inv mode n_anchors recto_share chain_len chain_first chain_last front_first
front_last end n_entries n_locators n_linked`, one row per volume with a front index, sorted by inv), `manifest.json`
(layer name, created UTC, source layer path and its `manifest.json` `layer` value, `package_info` of archival-structures,
the variant and all params, counts). Per-inventory files are written with `write_tsv_gz` (atomic); an inventory whose
two files exist is skipped (resume); `volumes.tsv` and the manifest are rebuilt from the per-inventory files at the end.
Inventories without a front index get no files. Run the dev build into `data/index_links_dev/20e-a/` (`mkdir` it,
`chmod g+w`) with coreshare as above.

**Silver (`silver.py`, reported only).** (1) Words: for every located link in the dev build, the entry's words of ≥ 6
letters (lower case, letters only) found on the target sides (`front` + `back`, text from `lines/`) vs on the sides of
folio `number + 3` (control; skip if it does not locate): shares. (2) REPUBLIC, the 13 covered volumes: share of links
whose `front` or `back` side has ≥ 1 REPUBLIC line with a non-empty `resolution_id` (match by `page_id`), vs the same
for folio `number + 3`. Write `silver.tsv` per volume and totals.

**UA** (append with Edit): `### UA-0NN Run the index-links build on the full v5 layer (<date>, from phase-14 step 20e)`.
Action: Run, in the AS root, on the machine with the drive; `<drive>` = the directory holding `text_layer_v5/`
(UA-045); create `<drive>/index_links_v1` first:
`.venv/bin/python scripts/build_index_links.py --layer <drive>/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5
--out-dir <drive>/index_links_v1 --limit 3` then the same without `--limit` and with `--workers 4` (rerun to resume);
then copy back `volumes.tsv` and `manifest.json` (give the `cp` commands into `data/index_links_full/`). State the
measured time per volume of the dev build. Default if unanswered: links exist for the 41 local volumes only. Blocks:
links beyond the local volumes. Response: empty.

## Constraints

- The folio map is frozen: no change to `folio_map.py` behaviour. Variant rules exactly as above; selection on dev only;
  the test runs once. REPUBLIC never feeds a link.
- No edits to `text_layer/`, `page_numbering.py`, any v5 layer. SG values only in `SG_LOCATOR_PARAMS`/`SG_MONTHS`.
- Never `git commit`; `chmod g+w`; zsh; no `rm -rf`, `nohup`, `&`; coreshare for > 1 worker; Edit for shared files.
- Protected: `data/text_layer_v2_dev/19e-a` and `shared-data/` (hash before and after with
  `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 20e-`; only other projects' shared-data
  paths may differ).

## Tests

Extend `tests/test_analysis_index_links.py`. Expected lists (the planner ran the rules above on these texts,
2026-10-29; `L4` with `folio_range=(1, 400)`), as `L1 / L2 / L3 / L4`:

| Text | L1 | L2 | L3 | L4 |
|---|---|---|---|---|
| `Vriesche Compen. op de fleuijtschepen, 252: 568: 581: 594: 616: 623:` | all six | all six | all six | 252 |
| `Geiden 170, 375, 483.` | 170 375 483 | same | same | 170 375 |
| `Uts: openstellen der Vaart. 703, 752. Uto: de Scheepvaart aan de gang geraakt, 858.` | 858 | 703 752 858 | 703 752 858 | — |
| `Missr. van Wartensleben, rak de zaak van Erechiels 6e: 147.` | 147 | 147 | 147 | 147 |
| `In de continuatie van 6 Fregat, ter bewaringe 328.` | 328 | 6 328 | 6 328 | 6 328 |
| `Levantschen handel vide 2e. L. Hanicart 342.` | 342 | 342 | 342 | 342 |
| `Ontfangen 17. April Requeste van Jan 412.` | 412 | 17 412 | 412 | — |
| `Betalinge van 6 maanden Tractement 96.` | 96 | 96 | 96 | 96 |
| `Vereeringe f 150. Aen de Weduwe 77.` | 77 | 150 77 | 77 | 77 |

(`6 Fregat` is a known L2/L3 false positive; no rule is added for it.) Also: `ordinal_after` can only fire when the ordinal letter is upper case (a lower-case letter right after the digits
never ends a segment, so L2 already drops `6e`): test `Regiment 6E. Capiteyn 12.` → L2 `6 12`, L3 `12`; L4 without `folio_range` = L3; offsets slice back to the digits;
`volume_links` on a synthetic volume (front index, 3 entries, a map) gives the expected rows; the builder on a 2-volume
synthetic layer in `tmp_path` (one with, one without a front index) writes the expected files and resumes. Full suite green.

## Acceptance criteria

1. Selection table for L1–L4 on dev (P, R, F1, counts); the choice by the rule.
2. **Bar L** (test, once): P ≥ 0.95 and R ≥ 0.90 (numerators and denominators). **Bar E**: ≥ 0.90. A failed bar →
   negative result; the layer is still built and documented with the measured numbers; the UA says so.
3. Dev build: `volumes.tsv` has 41 rows; every inventory has both files; the manifest counts match the files.
4. Silver totals reported with their controls.
5. Docs, changelog, UA; new tests and the full suite green; protect diff clean.

The user decides when the step is done.

## Error analysis

Read every locator FP and FN on dev and test (all, up to 40; else 40 at `random.Random(20261201)`) and classify:
`ordinal`, `amount`, `quantity`, `date`, `year`, `garbled digits`, `split number`, `mid-entry locator missed`,
`gold error`, `other`. Read every bar-E miss and name the component (locator or map). From the dev build read 20 links
at `random.Random(20261202)` with their target sides (`show_scan.py` from `step-20c/`) and say whether the matter is
there (not a bar; compare with 20c's index accuracy). Read the 10 volumes with the lowest silver word share. Draft
findings: the chosen variant and bars, the error categories, links per volume, silver shares vs control.

## Do not touch

`folio_map.py` behaviour, `text_layer/`, every layer under `data/text_layer_v2_dev/`, the planning and `step-20c/`
data (read only), the siblings, `shared-data/` (read through `data/republic_segmentation_v1` only), the external drive.

## When done

1. Report: `### Step 20e: locators, links and index_links_v1` in `reports/phase-14-index-links.md` (draft findings
   `F14-20e.n`); `make reports` clean.
2. Append to the phase `PROGRESS.md`:

```markdown
### Step 20e: locators, links and index_links_v1
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `analysis.index_links` (`Locator`, `LocatorParams`, `SG_MONTHS`, `SG_LOCATOR_PARAMS`, `DEFAULT_LOCATOR_VARIANT`, `extract_locators`, `volume_links`); `scripts/build_index_links.py`
- **Tests:** <command> → <result>
- **Acceptance:** <criterion by criterion, with numbers>
- **Report:** [`reports/phase-14-index-links.md` § Step 20e](../../reports/phase-14-index-links.md#step-20e-locators-links-and-index_links_v1); findings F14-20e.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** UA-0NN (full run)
```

3. Overwrite the phase `STATUS.md`: all steps' state; next: phase review (prompt 3).
