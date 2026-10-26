# Step 15wr: split gate for single-column print volumes (review fix)

> **Phase:** 12, Hand and print by line-box height. **Depends on:** 15w (layer `15w-a`), the phase review (F12-R.4).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then this
> file. **Read nothing else unless this brief names it.** Anchors checked 2026-10-25. Record any exploration under
> *Brief defects*.

## Context

The v4 print rules include the 1.10.94 merged-line splitter, which assumes two text columns per page side. In the
single-column print series of 1.01.02 it cuts ordinary lines in two: *nouvelles* 885,967 splits (about 73 per scan),
*plakkaten* 169,527, 9486 4,745, against 617 in the 31 two-column *gedrukte resoluties*. 46 of 46 sampled splits cut
one sentence ("Le Collonel Rendall, qui ‖ avoit éte Gouverneur des Barbades &"), and the halves land in different
layout columns, so units hold half-lines (F12-R.4). This step adds a **split gate**: a 1.01.02 print-mode volume is
split only if its own PageXML shows a two-column layout. It rebuilds the v4 dev layer as `15wr-a` and reads 20 scans
fixed in advance. Step 15x (drop-cap units) builds on `15wr-a`.

## Inputs

- `archival_structures/text_layer/modes.py` (105 lines), as it is now:
  - `decide_mode(line_rows, preset) -> dict` (L23). It returns `box_height_mode` keys plus `scans`, `regions`;
  - `write_mode(out_root, inv, result, info)` (L50). It writes `mode/<inv>.json` from `result` minus `scans`/`regions`,
    then adds `inv, n_scans, n_parse_fail, n_scans_sided, scan_minority_share`;
  - `extract_inventory_v2_modes(inv, source, out_root, preset, *, record_splits=False)` (L85):
    ```python
    rows, tags, info = prepass_rows(inv, source, preset)
    decision = decide_mode(rows, preset)
    write_mode(out_root, inv, decision, info)
    mode = decision["mode"]
    resolved = inventory_preset(preset, mode)
    if resolved.merged_line_split == "on":
        model = fit_split_model_from_rows([_int_row(r) for r in rows], tags,
                                          resolved.split_region_tags, resolved.untagged_tag)
        hook = make_split_hook(model)
        hook.record_splits = record_splits
        result = extract_inventory_v2(inv, source, out_root, resolved, scan_hook=hook)
        _write_split_files(inv, out_root, model, hook, result, record_splits)
    else:
        result = extract_inventory_v2(inv, source, out_root, resolved)
    result["mode"], result["mode_method"] = mode, decision["method"]
    return result
    ```
- Pre-pass rows (`line_split.prepass_rows`, raw `extract_scan_v2` rows): dicts with `page_side` (`left`/`right`), `x`,
  `w` (int, or `""` when the line has no box), `text_raw` (str), among others.
- `archival_structures/text_layer/presets.py`: `BOX_HEIGHT_V4` L181, `_V4_MODE_PRESETS` L184, `SG_1_01_02_V4` L185–189
  (`volume_mode={"method": "box_height", "params": BOX_HEIGHT_V4}`), `SG_1_10_94_V4` L190–194 (`fixed`), `inventory_preset` L201.
  `build.py` decides print units by `run.preset_for(inv).merged_line_split == "on"` (L473–474, `stage_units`), which the gate does not touch,
  so gated volumes keep the print layout, types and unit rules. An empty `column` field is allowed (layout groups it as `''`).
- Audit `reports/data/phase-12-hand-print-box-height/step-15w/audit.py`, `audit_v4` L267: the invariant
  `split_model exists iff mode print` (L298–299) and `no split_from in a hand inventory` (L300–302).
- Run script `scripts/run_text_layer_v4_full.py`: docstring L2–67 (L53 names "a `split_model/` iff `print`"), cost
  constants and their comment L77–89 (`WORKER_SECONDS_PER_XML_BYTE`, `OUTPUT_PER_XML_BYTE`).
- `docs/text_layer.md` § *Text layer v4*: the *Build* paragraph, the *`mode/` tables* paragraph and *Known issue (UA-026,
  …)* (L488).
- Design table (phase review): `reports/data/phase-12-hand-print-box-height/review/column_share_pooled.tsv`
  (`inv, series, n_lines, side_min, side_max, n_split, pooled_mid_start`; 87 print-mode volumes of `15w-a`, measured on
  the pre-split lines rebuilt from the halves): *gedrukte resoluties* 0.496–0.501 (31), 3463 0.415, 455 0.461, 456 0.164,
  *plakkaten* 0.026–0.225 (27), *nouvelles* 0.005–0.064 (25), 9486 0.007.
- Read list, drawn by the review before this step: `reports/data/phase-12-hand-print-box-height/review/reads_15wr.tsv`
  (20 scans: 10 *nouvelles*, 8 *plakkaten* volumes × 1 scan, 9486 × 2).
- Reference layer: `data/text_layer_v2_dev/15w-a/{1.01.02,1.10.94}` (protected). Sources as in 15w (below).

## Deliverables

| File | Change |
|---|---|
| `archival_structures/text_layer/modes.py` | `column_start_share(line_rows, min_chars=20, min_rows=50, mid_frac=0.4) -> tuple[Optional[float], int]`; the gate in `extract_inventory_v2_modes` |
| `archival_structures/text_layer/presets.py` | `SPLIT_GATE_V4 = {"min_share": 0.35, "min_chars": 20, "min_rows": 50, "mid_frac": 0.4}` with a comment (phase-12 review, F12-R.4, P16); `SG_1_01_02_V4.volume_mode` gains `"split_gate": SPLIT_GATE_V4`. `SG_1_10_94_V4` unchanged |
| `reports/data/phase-12-hand-print-box-height/step-15w/audit.py` | invariants below (edit in place; it is the run kit's audit) |
| `scripts/run_text_layer_v4_full.py` | docstring L53 and the gate sentence; print cost constant re-measured on `15wr-a` |
| `tests/test_text_layer_modes.py` | tests below |
| `docs/text_layer.md` | v4 *Build*: the gate; `mode/` tables: the `split_gate` key; replace the *Known issue* paragraph (text below) |
| `docs/shared_library/changelog.md` | `### Step 15wr: split gate for single-column print volumes`, with Edit, after 15w's entry |
| `data/text_layer_v2_dev/15wr-a/` | v4 from scratch, all 198 volumes |
| `reports/data/phase-12-hand-print-box-height/step-15wr/` | scripts, compare and audit outputs, `verdicts.tsv` |
| `reports/phase-12-hand-print-box-height.md` | `### Step 15wr: split gate for single-column print volumes (review fix)`, as a post-review addendum after § Step 15w; findings `F12-15wr.n` |
| `docs/USER_ACTIONS.md` | UA-024 *Evidence*: the new prediction (Edit; never the *Response*) |

## Behaviour to implement

`column_start_share(line_rows, min_chars=20, min_rows=50, mid_frac=0.4)`: keep rows with `len(text_raw) >= min_chars`
and `x`, `w` not `""`. Group them by `page_side`. For each side with at least `min_rows` rows: `left` = 5th percentile of
`x`, `right` = 95th percentile of `x + w` (`numpy.percentile`, linear, as `line_split._percentile`), `span = right - left`,
`k` = rows with `x >= left + mid_frac * span`. Return `(sum k / sum n over those sides, sum n)`, or `(None, n_rows_kept)`
when no side qualifies. No archive knowledge; the docstring says what it measures (lines starting in the right part of
the text span: the second column of a two-column page side).

In `extract_inventory_v2_modes`, after `decide_mode` and **before** `write_mode`:

```python
mode = decision["mode"]
resolved = inventory_preset(preset, mode)
split = resolved.merged_line_split == "on"
gate = (preset.volume_mode or {}).get("split_gate")
if split and gate:
    share, n = column_start_share(rows, min_chars=gate["min_chars"], min_rows=gate["min_rows"],
                                  mid_frac=gate["mid_frac"])
    split = share is None or share >= gate["min_share"]
    decision["split_gate"] = {"share": None if share is None else round(share, 4), "n_rows": n,
                              "min_share": gate["min_share"], "split": split}
write_mode(out_root, inv, decision, info)
```

Then split iff `split`. A volume without a gate, and every hand volume, writes exactly the `mode/<inv>.json` it wrote
before. `share is None` (a volume too small to measure) keeps the splitter, as today.

Audit (`audit_v4`): replace L298–299 by `split_model exists iff mode print and split_gate.split is not False` (a
missing `split_gate` counts as split). Add, for a mode file with `split_gate`: `split == (share is None or share >= min_share)`,
and, when `split` is false, `no split_from in lines/`. The self-test must still pass.

*Known issue* paragraph, new text (keep the heading style):
"**Known issues (UA-026, F12-15v.4, F12-R.3, F12-R.4).** A 1.01.02 volume in `print` mode gets the print unit rules,
which cut a printed paragraph after its drop-cap lines (decisions and *Dat …* clauses are separated correctly); step
15x plans a drop-cap-aware rule. The merged-line splitter runs only where the volume's column-start share is at least
0.35 (`split_gate` in `mode/<inv>.json`): the two-column *gedrukte resoluties* (0.50) are split, the single-column
*nouvelles*, *plakkaten* and *Processtukken* (at most 0.23) are not. Before step 15wr they were split, which cut about a
million ordinary lines in two (`15w-a`)."

## Constraints

- 1.10.94 output and every hand inventory must not change; the 31 *gedrukte resoluties* and 3463 change only in
  `mode/<inv>.json`. Do not touch `line_split.py`, `units.py`, `layout.py`, `types.py`, `hand_print.py`.
- No threshold tuning: `min_share` 0.35, `mid_frac` 0.4, `min_rows` 50, `min_chars` 20 are fixed (P16).
- Protected (hash before and after, as in 15w): `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/planning/protect.py
  reports/data/phase-12-hand-print-box-height/step-15wr/protect_before.txt --skip 15wr` (and `_after`), plus
  `find data/hand_print_geom reports/data/phase-10-hand-print-per-region reports/data/phase-12-hand-print-box-height/step-15u data/text_layer_v2_dev/15w-a -type f | sort | xargs shasum -a 256 > reports/data/phase-12-hand-print-box-height/step-15wr/protect2_before.txt`
  (and `_after`). `shared-data/` changes by other projects are expected (README, new dirs); nothing of yours writes there.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs; `.venv/bin/python`; no `rm -rf`, `nohup`
  or `&`; a new dir per run. Shared files (report, changelog, `STATUS.md`, `PROGRESS.md`, `docs/USER_ACTIONS.md`) only
  with Edit.

## Tests

- `test_column_start_share`: synthetic rows (60 per side, `text_raw` 30 chars): one side with all lines at `x=100,
  w=1600` → share 0.0; one with half at `x=100, w=780` and half at `x=900, w=780` → 0.5 (± 0.02); 10 rows → `(None, 10)`;
  rows with `x == ""` or short text are ignored.
- `test_extract_split_gate(src, tmp_path)` (fixture `src` as in the file): `replace(PRESETS["sg_1.01.02_v4"],
  volume_mode={"method": "fixed", "mode": "print", "params": BOX_HEIGHT_V4, "split_gate": {**SPLIT_GATE_V4,
  "min_rows": 1, "min_share": 1.1}})` → no `split_model/9999.json`, `mode/9999.json` `split_gate.split` is false, no
  `split_from` in `lines/`; with `"min_share": 0.0` → `split_model/9999.json` exists and `split` is true.
- `test_inventory_preset` and `test_v1_v3_presets_unchanged` still pass unchanged; the audit `--self-test` passes.
- Commands: `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_text_layer_modes.py`, then the full suite
  (collect first; before 15wr: 1,246 collected, 1,235 passed, 11 skipped). LotA: no code change, no run needed.

## Steps

1. Code, tests, protect `_before` files. `mkdir -p data/text_layer_v2_dev/15wr-a && chmod g+w data/text_layer_v2_dev/15wr-a`.
2. `python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py status`, then from the AS root (the
   script asks coreshare for workers itself):
   `.venv/bin/python scripts/run_text_layer_v4_full.py --check-only --src 1.01.02=data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 --src 1.01.02=data/text_layer_v4_staged/NL-HaNA_1.01.02 --src 1.10.94=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/pagexml --republic-dir 1.10.94=/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94 --out-dir data/text_layer_v2_dev/15wr-a`
   (expect 196 + 2), then the same without `--check-only` (15w: 5,340 s on 2 workers; less now).
3. Compare: `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15v/compare.py data/text_layer_v2_dev/15w-a data/text_layer_v2_dev/15wr-a --sub-map 1.01.02:1.01.02,1.10.94:1.10.94 --explained-invs <the gated inventories, comma-separated> --out reports/data/phase-12-hand-print-box-height/step-15wr/compare_15w-a.json`.
4. Audit: `.venv/bin/python -I reports/data/phase-12-hand-print-box-height/step-15w/audit.py --layer data/text_layer_v2_dev/15wr-a --out reports/data/phase-12-hand-print-box-height/step-15wr/audit_15wr-a.json`.
5. Gate table `step-15wr/gate.tsv` (inv, series, share, n_rows, split, review `pooled_mid_start`) from the 85 + 2 mode
   files; cost from `15wr-a` `status.tsv` (same method as the comment at L77–89), new constants, UA-024 sentence.
6. Error analysis (below), report, docs, changelog.

## Acceptance criteria

1. New tests pass; full suite green (counts before and after).
2. Gate decisions: every *gedrukte resoluties* volume and 3463 `split` true (share ≥ 0.45); every *nouvelles*,
   *plakkaten* and 9486 volume `split` false (share ≤ 0.30): 53 gated, 32 split. Shares within ±0.03 of the review's
   `pooled_mid_start` (a larger gap: report it). **Stop** if any share lies in 0.30–0.45 or a decision differs.
3. Compare with `15w-a` (decompressed bytes): the 111 hand inventories and 455/456 equal in every per-inventory file; the
   32 split print volumes equal except `mode/<inv>.json` (only the new `split_gate` key; check with a key diff);
   the 53 gated volumes differ (explained), with `split_model/` and `split_stats/` only in `15w-a`; corpus files differ
   only in the 53 inventories' entries (`*/stats.json` per key, `wbc/*`; `pinned` and `default_set` identical). Any other
   difference: stop.
4. 0 lines with `split_from` in the 53 gated volumes; audit on `15wr-a`: 0 judged failures (shares reported).
5. Read bar (below) passed or failed as fixed; protected data unchanged.

## Error analysis (pre-registered; verdicts in `step-15wr/verdicts.tsv`)

- **Bar.** For each of the 20 scans in `review/reads_15wr.tsv`: in `15wr-a`, the `main_text` unit with the most lines
  (ties: lowest `unit_order`); print its lines in unit order (text from `lines/`, order from `units/segments/`), with the
  same scan's largest unit in `15w-a` beside it for context. Verdict per `15wr-a` unit: `in order` (each line continues
  the previous one or starts a new sentence in reading order), `half lines` (a line breaks off and its continuation is
  not the next line: the F12-R.4 defect), `other order error` (lines out of reading order, or two columns mixed),
  `unclear` (OCR too garbled to judge; counts as not in order). **Pass iff `in order` ≥ 18 of 20.** You judge from the
  text, so give the reason for each verdict in one short phrase.
- **Stop rule.** If the bar fails, report it as a negative result (finding), keep the code and `15wr-a` as they are,
  change no threshold, and stop. The phase-review session decides. Step 15x does not start.
- Also read: 5 random lines (seed 20261030) of the 53 gated volumes that `15w-a` split and `15wr-a` does not, both
  versions side by side; and 3 lines of 3771 that are still split (the tariff pages), to confirm nothing changed there.
- Report the split counts per series before and after, units per page per series before and after, and the new print
  cost rate.

## Do not touch

`data/text_layer_v2_dev/{15m-a,15n-a,15o-pub,15i-src,15v-a,15w-a}`, `data/hand_print_geom/`, `reports/data/phase-10-*`,
`reports/data/phase-12-*/step-15u/`, `step-15w/` outputs other than `audit.py`, `review/`, the published layers,
`scripts/run_text_layer_v3_full.py`.

## When done

1. Report section (post-review addendum, after § Step 15w; italic first line "*Post-review addendum (2026-10-…): review
   fix for F12-R.4.*"): what changed, results table against the acceptance criteria, the gate table summary, reads with
   categories, counts and verbatim examples, conclusion. Findings `F12-15wr.n` (`draft`); `make reports` without warnings.
2. Append the block below to `PROGRESS.md`; overwrite `STATUS.md`: 15wr done, next 15x (Sonnet), the facts 15x needs
   (`15wr-a` is its base; the gated inventory list; test counts).

```markdown
### Step 15wr: split gate for single-column print volumes (review fix)
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** `modes.column_start_share`; `presets.SPLIT_GATE_V4`; `sg_1.01.02_v4` behaviour (split gate)
- **Tests:** <command> → <result>
- **Acceptance:** <1–5, with numbers>
- **Report:** [`reports/phase-12-hand-print-box-height.md` § Step 15wr](../../reports/phase-12-hand-print-box-height.md#step-15wr-split-gate-for-single-column-print-volumes-review-fix); findings F12-15wr.1–n
- **Error analysis:** <two lines at most>
- **Deviations from brief:** <or "none">
- **Brief defects:** <or "none">
- **Open questions for the user:** <UA IDs or "none">
```
