# Step 15i2: Drop `=` from the pinned word-break set of hand_tar, rebuild the hand pilot (UA-011 (b))

> **Phase:** 07, SG units, types and the pilot layer. **Depends on:** 15i Part A.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Don't read the
> phase `PLAN.md`. If you have to explore the code base anyway, record it under *Brief defects*.

## Context

The pilot `data/text_layer_v2_dev/15i-a/{hand,print}/` was built with the word-break (WBC) sets
pinned by UA-010 (c): `-=„` for every SG inventory, and `-:=„` for 11281. Step 15hj2's error
analysis found that the `=` merges the pin adds in 3349, 3350 and 3353 (821 merges, lower-case
right-side share 0.74) are right in only 11 of 20, with 5 wrong ("aen=" + "haer", "missiven=" +
"vanden"). The rule had not derived `=` there. The user chose **UA-011 (b)** on 2026-10-21: drop
`=` for the hand_tar inventories and rebuild.

**The rule:** drop `=` in the hand_tar inventories whose *derived* set (15hj) lacks it. These are
3349, 3350, 3351, 3353 and 3354, which derived `„`, `„`, empty, `„` and empty. 3352 derived `=„`,
so it keeps `-=„`. 3351 and 3354 had no `=` merges (their 300 added merges are `„` and `-`), so
their output should not change. The print preset is unchanged.

## Inputs

- `archival_structures/text_layer/presets.py` L109, in `SG_HAND_V2`:
  `wbc_pinned={"default": "-=„", "11281": "-:=„"},`. The mapping is `{"default": chars,
  "<inv>": chars}`. `build.decide_v2` writes `wbc_set = "".join(sorted(pinned.get(inv,
  pinned["default"])))`, with `mode = "pinned"`.
- The pilot build command, from `15i-a/hand/…/manifest.json` `stages.extract.argv`:
  ```
  .venv/bin/python -m archival_structures.text_layer.build --pagexml-dir data/text_layer_v2_dev/15i-src/NL-HaNA_1.01.02 \
      --out-dir data/text_layer_v2_dev/15i-b/hand --preset sg_1.01.02_v2 --stage all --workers 8
  ```
  This took about 2:31 for 14 inventories. `15i-src/` is a directory of symlinks; never write
  through it.
- Part A's scripts in `reports/data/phase-07-sg-units-and-pilot/step-15i/`:
  - `audit.py [--layer DIR] [--out FILE]`;
  - `rows116.py [--layer DIR] [--out FILE]`;
  - `repro.py`, which hardcodes `NEW = Path("data/text_layer_v2_dev/15i-a")` (copy it to
    `step-15i2/` and adapt it);
  - `crops.py`, which writes `crops/` next to itself.

  Their outputs `audit.json`, `rows116.tsv`, `repro.json` and `crops/` are Part A's record and
  are protected. Write new outputs into `reports/data/phase-07-sg-units-and-pilot/step-15i2/`.
- Part A's verdicts: `step-15i/rows116_verdicts.tsv` (hand-read `predicted` for the 51 artefact
  rows).

## Deliverables

| File | Change |
|---|---|
| `text_layer/presets.py` | `SG_HAND_V2.wbc_pinned = {"default": "-=„", "11281": "-:=„", "3349": "-„", "3350": "-„", "3351": "-„", "3353": "-„", "3354": "-„"}`, with a comment citing UA-011 (b) and F07-15hj2 |
| tests | The existing pinned test still passes. Add `test_sg_hand_pins` to `tests/test_text_layer_word_breaks.py`: the `SG_HAND_V2` sets are exactly those above, and 3352, 3771 and 11182 take the default |
| `docs/text_layer.md` | The pinned-sets paragraph: the hand_tar exceptions and why |
| `docs/shared_library/changelog.md` | `### Step 15i2: hand_tar WBC pin without =` at the top of `## Unreleased` (newest first), with Edit |
| `data/text_layer_v2_dev/15i-b/{hand,print}/` | `hand`: a full rebuild (command above). `print`: `cp -R data/text_layer_v2_dev/15i-a/print data/text_layer_v2_dev/15i-b/print`, not rebuilt; the print preset is unchanged |
| `reports/data/phase-07-sg-units-and-pilot/step-15i2/` | `repro.py`, `audit.json`, `rows116.tsv`, `deltas.json`, `ea_verdicts.tsv` |
| `reports/phase-07-sg-units-and-pilot.md` | A new `### Step 15i2: hand_tar pin without =` section after § Step 15i; findings `F07-15i2.n` |
| `docs/USER_ACTIONS.md` | UA-011 → `docs/USER_ACTIONS_resolved.md` (outcome); a new UA (*When done*) |

## Constraints

- No other library change.
- **Protected:** every earlier dev layer (including `15i-a/`, `15i-src/`, `15hj-b/` and
  `15hj2-v1check/`), Part A's outputs in `step-15i/`, all of `shared-data/`, LotA's `data/`,
  `gold/` and `output/`, and NLP's `output/voc_text_layer/`. Hash them before and after with
  `step-15i/protect.py` (read its usage line) into `data/text_layer_v2_dev/15i2-protected-
  {before,after}.txt`. LotA `output/` may change through a concurrent LotA session (seen in 15hj2
  and 15i). List such files, and don't count them as yours.
- Nothing is written under `shared-data/` or LotA. Publication is Part B, after the new UA.
- Never `git commit`; `chmod g+w` new files and directories; `.venv/bin/python`; no `rm -rf`,
  `nohup` or `&`.

## Acceptance criteria

1. The hand build exits 0, without `--ignore-sanity`. `wbc/wbc_per_inventory.tsv` gives
   `-„` for 3349, 3350, 3351, 3353 and 3354, `-:=„` for 11281, and `-=„` for the other 8.
   `corpus_default.json` has `sanity_pass: true`.
2. **Reproducibility against `15i-a/hand`** (decompressed bytes, per inventory):
   - `scans/ regions_meta/ lines/ layout/ types/ units/` are identical in all 14;
   - `join/` is identical in the 9 inventories whose set didn't change (3352 and the 8
     hand_local), and in 3351 and 3354;
   - `join/` differs only in 3349, 3350 and 3353.

   List every other difference and explain it. `15i-b/print` equals `15i-a/print` byte for byte.
3. In 3349, 3350 and 3353: 0 merges at `=`, and the number of merges removed per inventory
   (expected about 821 in all, measured in 15hj2 on the dev layer). The `-` and `„` merges are
   unchanged. Report the lower-case right-side share before and after (15hj's measure).
4. `audit.py --layer data/text_layer_v2_dev/15i-b --out …/step-15i2/audit.json`: the same
   invariants as Part A (176 of 182 passed there, and the 6 failures were the reported merge
   shares). Every invariant that passed still passes. List the failures with causes.
5. `rows116.py --layer data/text_layer_v2_dev/15i-b --out …/step-15i2/rows116.tsv`: 116 rows,
   with hand tokens found for ≥ 110 and print tokens for ≥ 105. Diff it with
   `step-15i/rows116.tsv`: list every row whose hand columns or `predicted` changed, and read
   each one. Re-apply Part A's hand-read verdicts where the row is unchanged. Report the predicted
   share free and the upper bound again.
6. Full AS suite green (1,100 + new); protected hashes unchanged, apart from the concurrent LotA
   files.

Run `crops.py` only if a row's hand boxes changed (criterion 5). If you run it, copy it to
`step-15i2/` first, so that Part A's `crops/` stays as it is.

## Error analysis

Use `random.Random(24)`. Read 20 of the removed `=` merges (3349, 3350, 3353; the boundaries that
`15i-a` merged and `15i-b` keeps apart). Classify each as *right to split* (two words), *wrong to
split* (a word now broken at a real `=` break), or *unclear*. 15hj2 found 11/20 right to merge, so
expect about half wrong to split. That is the cost of (b); report it plainly. Give verbatim
examples.

## When done

1. Write the report section, with criteria 1–6 with numbers, the error analysis and findings
   `F07-15i2.n` (status `draft`), with rows in the *Findings* table. Run `make reports` and fix
   any warnings.
2. Append the progress block (format in `plan/templates/STEP.md`) to `PROGRESS.md`.
3. Update brief 15i Part B (rule 10):
   - copy from `15i-b/` instead of `15i-a/`;
   - in `publication.built_from`, record the **resolved** source paths (follow the `15i-src`
     symlinks) with sha256 of the tar and zip files, and note that `stages.*.argv` and the
     `member`/`source` fields in the copied tables name `data/text_layer_v2_dev/15i-src/…` (dev
     paths). List this under `known_gaps`; don't rewrite the tables;
   - the protected list: add `15i-b`, and Part A's and 15i2's outputs;
   - Part B starts only once the new UA says publish.
4. Move UA-011 to `docs/USER_ACTIONS_resolved.md` (outcome: `=` dropped for 3349, 3350, 3351,
   3353 and 3354; the merge counts). Add the next UA, in the format of the file:
   - **Action:** *Decide* whether to publish `15i-b` (hand rebuilt without `=` for hand_tar, print
     as in Part A) to `shared-data/…/text_layer_v2/`, with the new numbers;
   - **Options:** (a) publish; (b) fix first (list what); (c) don't publish;
   - **Default if unanswered:** nothing is published;
   - **Blocks:** 15i Part B and LotA's re-alignment.
5. Overwrite `STATUS.md` (40 lines max): 15i2 done, waiting for UA-NNN, and the pilot is now
   `15i-b`.
