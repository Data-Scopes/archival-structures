# Step 15m: v3 presets, integrated dev build, run kit

> **Phase:** 09, Text layer v3 preparation. **Depends on:** 15l (its layer `15l-a` is your baseline).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to
> explore the code base anyway, do what you must and record it under *Brief defects*.

## Context

Steps 15j–15l fixed the phase-07 follow-ups. Each was measured on its own layer. This step closes
the phase.
- It adds the **v3 presets**, the names the full SG run will use.
- It builds one dev layer `15m-a` from scratch with them and shows that it equals the chained
  result (`15l-a`).
- It measures the whole phase against `15ir-a` (the v2 code before this phase): audit, the 116
  labelled rows, and the planning metrics.
- It writes the docs, a LotA note and a **run kit**: the commands for the full run, smoke-tested,
  and a UA saying what to stage.

Nothing is published (user decision P1). The pagexml-tools 0.9.1 pin happens only if 0.9.1 is on
PyPI (P12).

## Inputs

- Layers: `data/text_layer_v2_dev/15ir-a` (phase start), `15j-a`, `15k-a`, `15l-a` (final chained).
  Check `STATUS.md` *Facts later steps need* for any change to these names.
- Planning scripts (`reports/data/phase-09-text-layer-v3-prep/planning/`, run from the AS root with
  `.venv/bin/python -I`): `order.py`, `pn_offchain.py`, `merges.py`, `crosswalk.py` (each takes
  `--layer`, `--json`), `compare.py A B`, `protect.py`. All glob `*_text_layer_v*`, so they read v3
  roots.
- Phase-07 audit tools: `reports/data/phase-07-sg-units-and-pilot/step-15ir/audit.py` (root glob
  `*_text_layer_v2` at L47; `--layer`, `--out`) and `rows116.py` (glob at L31; `--layer`, `--out`).
  Each runs in under 30 s. On `15ir-a` they gave: audit 178 / 182, with 3351, 3823, 455 and 456 below
  the 0.95 lower-case merge share; 116 rows predicted share free 0.526, bound 0.560.
- Code anchors (checked 2026-10-21):
  - `text_layer/presets.py`: `SG_HAND_V2` L97–117, `SG_PRINT_V2` L123–141 (line numbers move after
    15j–15l), `PRESETS` L143 (`voc_v1`, `sg_1.01.02_v2`, `sg_1.10.94_v2`).
  - `pyproject.toml` L25 `"pagexml-tools (>=0.9.0,<1.0.0)"`; `archival_structures/text_layer/provenance.py`
    L6 `MIN_PAGEXML_TOOLS = (0, 9, 0)`.
  - Known 0.9.0 workarounds in AS: `analysis/page_structure.py` module docstring L29–31 (regions go
    in `page.columns`/`page.extra`, never `page.text_regions`) and L590
    `page.set_as_parent(extra)  # set_parentage skips extra`; `text_layer/layout.py` `_page_coords`
    L70–75 (the page gets the page box, F07-15fp.11). `line_split.py` L1048 reproduces 0.7.x
    behaviour on purpose. It is not a workaround; keep it.
  - `docs/text_layer.md`: *Published SG pilot (text_layer_v2)* L418–457 (its *Known issues*),
    `## Provenance` L458.
  - LotA `docs/from-archival-structures.md` (in `/Users/Shared/claude_projects/data_scopes/language-of-the-archive/`),
    last section `## 7. Region quality and duplicates in AS (phase 08)`. Read LotA's `CLAUDE.md`
    first.

## Deliverables and behaviour

**Part 0, pin (conditional).** Run `.venv/bin/python -m pip index versions pagexml-tools`.
- **If 0.9.1 is listed:**
  - install it with `.venv/bin/python -m pip install 'pagexml-tools==0.9.1'`, then `chmod -R g+w`
    what pip wrote under `.venv/lib/python*/site-packages/pagexml*`;
  - pin `>=0.9.1,<1.0.0` in `pyproject.toml` and set `MIN_PAGEXML_TOOLS = (0, 9, 1)`;
  - grep `tests/` for the 0.9.0 behaviours you change (`set_parentage`, `text_regions`, `.stats`);
  - drop L590 only if `test_page_parentage` passes without it. Keep the docstring note if it still
    holds;
  - re-run 15l's build into `15m-pin/` and report with `compare.py 15l-a 15m-pin` whether output
    changed;
  - move UA-007 to `docs/USER_ACTIONS_resolved.md` with the date and what was done.
- **Else:** change nothing and record "0.9.1 not on PyPI on <date>". UA-007 stays open.

**Part 1, v3 presets** (`presets.py`). Add:
```python
SG_HAND_V3 = dataclasses.replace(SG_HAND_V2, name="NL-HaNA_1.01.02_text_layer_v3",
                                 description=SG_HAND_V2.description.replace("v2", "v3"))
SG_PRINT_V3 = dataclasses.replace(SG_PRINT_V2, name="NL-HaNA_1.10.94_text_layer_v3",
                                  description=SG_PRINT_V2.description.replace("v2", "v3"))
```
Add both to `PRESETS` as `sg_1.01.02_v3` and `sg_1.10.94_v3`. `format_version` stays 2 (the code
path), and the v2 presets stay. Test `test_v3_presets_equal_v2_but_name`:
`dataclasses.asdict` of v3 and v2 differ only in `name` and `description`.

**Part 2, integrated build** `15m-a` (check `coreshare.py status` first):
```
O=data/text_layer_v2_dev/15m-a; mkdir -p $O/hand $O/print
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15m-hand --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --out-dir $O/hand --preset sg_1.01.02_v3 --stage all --workers {workers}
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures --label 15m-print --want 8 --min 2 --wait -- .venv/bin/as-build-text-layer --pagexml-dir /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --out-dir $O/print --preset sg_1.10.94_v3 --stage all --republic-dir /Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --workers {workers}
```
(If Part 0 pinned 0.9.1, compare against `15m-pin` instead of `15l-a` below.)

**Part 3, phase measurement.**
- Copy `audit.py` and `rows116.py` into `reports/data/phase-09-text-layer-v3-prep/step-15m/`. Change
  only their root glob to `*_text_layer_v*` and the default `--layer`.
- Run both on `15ir-a` and on `15m-a`, and the four planning metrics on both (`--json` into
  `step-15m/`).
- Write one table *v2 dev (15ir-a) → v3 dev (15m-a)*: same-row swaps and jump regions per print
  class, `lines_resorted`, `rule:pn_line_gap`, retyped `other`, `rule:year_offchain`, crosswalk
  `split_original|end` moved, comma/`J`/upper-case merge counts, `g_upper_right`, units and
  `n_id_collisions` per print inventory, audit pass count, the 116-row share and bound.

**Part 4, docs.**
- In `docs/text_layer.md`, a new section `## Text layer v3 (phase 09)` before `## Provenance`:
  - the presets;
  - every output change against `text_layer_v2`, by stage, with the step that made it and its
    numbers from Part 3;
  - the schema additions (`layout/regions.lines_resorted`, `layout/scans.n_regions_resorted`,
    `units/republic_lines.role`, `type_method` `rule:pn_line_gap` / `rule:year_offchain`,
    `line_type` `other` for off-chain numbers where adopted, boundary kind `g_upper_right` where
    adopted);
  - the rule that print builds need `--republic-dir`;
  - the full-run commands (Part 5).
- In *Published SG pilot*, mark which *Known issues* the v3 code fixes.
- LotA (after reading its `CLAUDE.md`): append `## 8. Coming in text_layer_v3 (AS phase 09)` to
  `docs/from-archival-structures.md`. It lists the same schema changes, what LotA's reader will
  need, and that nothing is published yet. Change no LotA code.

**Part 5, run kit.**
- In the docs section, give the two full-run commands. Use the v3 presets,
  `--pagexml-dir` set to the staged shared-data dirs, `--out-dir data/text_layer_v3_full/{hand,print}`,
  `--republic-dir <staged REPUBLIC paragraphs dir>`, workers through coreshare, and `--estimate`
  first.
- Smoke-test each command as written on the pilot sources with `--limit 1` into
  `data/text_layer_v2_dev/15m-smoke/{hand,print}`. Both must exit 0.
- Add **UA-015** to `docs/USER_ACTIONS.md` (format in `CLAUDE.md`):
  - *Stage the inputs for the full `text_layer_v3` run*: which dirs (the 1.01.02 and 1.10.94
    PageXML under `shared-data/<archive>/pagexml/`, and REPUBLIC `paragraphs-<inv>.tsv.gz` for
    every print inventory). REPUBLIC's paragraphs are now only in LotA `data/paragraphs/`; suggest a
    `shared-data` dataset per its README.
  - Options: (a) all inventories; (b) a named subset; (c) later. Recommended: (b), the
    inventories LotA aligns.
  - Default if unanswered: no full run. Blocks: only the `text_layer_v3` build.

## Constraints

- Protected: `planning/protect.py data/text_layer_v2_dev/15m-protected-{before,after}.txt --skip 15m-`;
  empty `diff`. v1: the 15j brief's v1 command with `S=data/text_layer_v2_dev/15m-v1check` →
  `TOTAL 157 / 157`.
- No writes to `shared-data/`. Never `git commit`; never bump the version; `chmod g+w`;
  `.venv/bin/python`; no `rm -rf`, `nohup`, `&`.
- Don't change behaviour in `layout.py`, `types.py`, `units.py`, `join_v2.py` or `join.py`. The
  only code changes are the presets and Part 0.

## Acceptance criteria

1. Part 0 outcome recorded (pinned with `compare.py` result, or "not on PyPI").
2. Builds exit 0, 0 failures, `sanity_pass` true; `test_v3_presets_equal_v2_but_name` passes.
3. `compare.py 15l-a 15m-a` (or `15m-pin`): 0 differing tables; only `manifest.json`, `progress.log`
   and `status.tsv` differ (skipped by the script), plus any file that carries the layer name. List
   those.
4. Part 3 table complete. Audit and 116-row results listed, with every changed invariant and every
   changed row verdict/prediction.
5. Smoke runs exit 0. UA-015 written. `make reports` clean.
6. AS suite green; v1 157/157; NLP suite green (`venv/bin/python -m pytest -q -p no:cacheprovider tests`
   in `/Users/Shared/claude_projects/data_scopes/nlp-start-page-detection`); hashes unchanged.

## Error analysis

- Read every one of the 116 rows whose verdict or prediction changed from `15ir-a`. If none
  changed, read the rows whose print or hand unit text changed (all of them, up to 30). For each,
  say which step's fix changed it and whether the new text is closer to the scan.
- Read 20 `join/units` texts that differ between `15ir-a` and `15m-a` (`Random(101)` over all
  inventories). Classify by cause (15j order, 15k type/unit, 15l mark/guard) and as *better* /
  *worse* / *neutral*.
- Findings `F09-15m.n`: the phase totals (result), the 116-row share and bound, the remaining audit
  failures, and whether the pin changed output.

## Do not touch

`shared-data/`, earlier dev layers, LotA code, other steps' report sections and the report's
*Summary*, *Review* and *Follow-ups*.

## When done

1. `### Step 15m: v3 presets, integrated dev build, run kit` in the report (with the Part 3 table),
   then `make reports`.
2. Changelog `### Step 15m: …` (Edit): the v3 presets, the pin if made.
3. `PROGRESS.md` block; overwrite `STATUS.md` (all steps done; next: phase review, prompt 3 of
   `plan/PROMPTS.md`; UA-015 open for the user).
