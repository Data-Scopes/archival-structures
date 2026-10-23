# Step 15o: publish the `text_layer_v3` pilot to shared-data

> **Phase:** 09, Text layer v3 preparation (addendum after the review, like 15n). **Depends on:** 15m, 15n.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase `STATUS.md`, then
> this file, then LotA's `CLAUDE.md` (`/Users/Shared/claude_projects/data_scopes/language-of-the-archive/CLAUDE.md`;
> its rules apply to the one LotA file you edit). **Read nothing else unless this brief names it.** If you have to
> explore anyway, record it under *Brief defects*.

## Context

The user answered UA-015 (2026-10-23) with (a), restricted: publish the v3 code's output on the **local pilot
inventories** to shared-data as a v3 pilot, as LotA asked (AS `docs/from-language-of-the-archive.md` §7). No external
drive is involved. The full run is decided later, in phase 10 planning. LotA's hand–print work needs hand 3349, 3350,
3352, 3353 and print 455, 456; the pilot has all 14 + 2 inventories, so publish all of them.

**What to publish: the dev layer `data/text_layer_v2_dev/15n-a/`**, not `15m-a`. Both were built with presets
`sg_1.01.02_v3` / `sg_1.10.94_v3`; step 15n showed they are equal except for path columns (`scans.member`,
`extract_stats/*.json` `source`). `15n-a` was built from git head `fa5cc63` (the v0.14.0 release), dirty only in
`archival_structures/text_layer/build.py` (a `--republic-dir` help string); `15m-a` came from a dirty pre-release tree.
Its manifest's `version` field reads `0.13.0`: the editable install's metadata is stale. Record `as_version` `0.14.0`
and say so in `known_gaps`.

Layout of `15n-a`: `hand/NL-HaNA_1.01.02_text_layer_v3/` and `print/NL-HaNA_1.10.94_text_layer_v3/` (each with
`manifest.json`, `progress.log`, `status.tsv`, and the table dirs), plus `audit.json` and `run_v3.log` at its root.
Its audit: 178 pass, 4 fail; the 4 are the merge share with a lower-case right side below 0.95 (3351 0.9415,
3823 0.9414, 455 0.9477, 456 0.932). They are reported, not blocking (same as v2 and `15m-a`).

Sources (all local): `data/text_layer_v3_full_inputs/{hand,print}/` holds symlinks. Hand 3349–3354 resolve to
`/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_<inv>-pagexml.tar.gz`,
hand 3771–3773, 3821–3823, 11182, 11281 to directories `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_<inv>`,
print to `…/shared-data/NL-HaNA_1.10.94/pagexml/NL-HaNA_1.10.94_45{5,6}.zip`. REPUBLIC paragraphs:
`/Users/Shared/claude_projects/data_scopes/language-of-the-archive/data/paragraphs/NL-HaNA_1.10.94/paragraphs-45{5,6}.tsv.gz`.
sha256 measured 2026-10-21 (re-check; stop if one differs): 3349 `46ee0a68…7b04`, 3350 `eee1cc91…e527`,
3351 `9f700c6f…c797`, 3352 `2d97790a…4adb1`, 3353 `b5e8a0d0…1169`, 3354 `a8fe7c37…2460`, 455 `3f483416…8584`,
456 `e0ca7d97…39c6`. The full hashes are in the published v2 manifest:
`shared-data/NL-HaNA_1.01.02/text_layer_v2/manifest.json` → `publication.built_from` (hand) and the same key in
`shared-data/NL-HaNA_1.10.94/text_layer_v2/manifest.json` (print). Compare against those.

## Deliverables

| Path | Change |
|---|---|
| `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/text_layer_v3/` | new: copy of `15n-a/hand/NL-HaNA_1.01.02_text_layer_v3/` |
| `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.10.94/text_layer_v3/` | new: copy of `15n-a/print/NL-HaNA_1.10.94_text_layer_v3/` |
| `/Users/Shared/claude_projects/data_scopes/shared-data/README.md` | 2 catalogue rows, 1 changelog line (Edit) |
| LotA `docs/from-archival-structures.md` | a new §9 and a one-line pointer at the top of §8 (Edit) |
| AS `docs/text_layer.md` | section *Published SG pilot (text_layer_v3)* |
| AS `docs/from-language-of-the-archive.md` | one *Done* line at the end of §7 |
| `reports/data/phase-09-text-layer-v3-prep/step-15o/` | `publish_manifest.py`, `audit_published.json`, `protected-{before,after}.txt` |
| `reports/phase-09-text-layer-v3-prep.md` | `### Step 15o: …` after *Step 15n*, before `## Review`; findings `F09-15o.n` (`draft`) |

## Steps

1. **Checks; stop on any failure.** Neither `…/NL-HaNA_1.01.02/text_layer_v3` nor `…/NL-HaNA_1.10.94/text_layer_v3`
   exists. Their parents are real directories (`test -L` false) and writable (`test -w`). Every row of both
   `15n-a/*/*/status.tsv` says done (read its header for the column). The 8 archive sha256s match (above).
2. **Protected hashes before:** sha256 of every file under `shared-data/NL-HaNA_1.01.02/text_layer_v2/`,
   `shared-data/NL-HaNA_1.10.94/text_layer_v2/`, `data/text_layer_v2_dev/15m-a/` and `15n-a/` →
   `protected-before.txt` (sorted, `hash  path`).
3. **Copy** with `cp -R` (never `ln`, never `rsync --delete`), then delete `progress.log` and `status.tsv` **in the
   copies only**, then `chmod -R g+w` both copies.
4. **Audit the published copies.** The audit expects `<layer>/hand/NL-HaNA_1.01.02_text_layer_v3` and
   `<layer>/print/NL-HaNA_1.10.94_text_layer_v3`. Make `data/text_layer_v2_dev/15o-pub/{hand,print}/` with one symlink
   each to the published dirs, then run from the AS root:
   `.venv/bin/python -I reports/data/phase-09-text-layer-v3-prep/step-15m/audit.py --layer data/text_layer_v2_dev/15o-pub --out reports/data/phase-09-text-layer-v3-prep/step-15o/audit_published.json`.
   Expected: 178 pass, 4 fail, the same 4 invariants and values as `15n-a/audit.json`; `counts` equal to
   `15n-a/audit.json` `counts`. Any other difference: stop and report.
5. **Manifest key.** `publish_manifest.py` (argparse, `--dry-run` prints the JSON) adds a key `publication` to each
   copied `manifest.json`, the other keys unchanged, same shape as v2's `publication` (read one v2 manifest's
   `publication` to copy the key order):
   `producer` `"archival-structures"`, `as_version` `"0.14.0"`, `git_head` `"fa5cc63b2655df5d1d267b4af6f6a26d7556c683"`,
   `git_dirty_files` `["archival_structures/text_layer/build.py"]`, `built_by` `"scripts/run_text_layer_v3_full.py (step 15n), dev layer 15n-a"`,
   `layer` `"text_layer_v3"`, `format_version` `2`, `coverage` `"partial"`, `predecessor` `"text_layer_v2"`,
   `inventories` (that side's list), `date` (today), `audit` (the summary of `audit_published.json`: `n_pass`, `n_fail`,
   the failing invariants), `counts` (that side's inventories from `audit_published.json` `counts`),
   `rows116` `{"predicted_share_free": 0.526, "upper_bound": 0.560, "target": 0.80, "source": "AS reports/phase-09-text-layer-v3-prep.md, Step 15m"}`,
   `known_gaps` (below), `schema` `"docs/text_layer.md (AS) §§ layout, types, units, join; changes since v2: § Text layer v3 (phase 09)"`,
   `built_from` (per inventory: `inventory`, `kind` (`tar.gz`/`zip`/`dir`), the **resolved** `path`, `sha256` for
   archives or `n_files` for directories; print also gets `republic_paragraphs` with path and sha256).
   `known_gaps`: (1) `stages.*.argv`, `scans.member` and `extract_stats` `source` name dev paths
   (`data/text_layer_v3_full_inputs/…`); the tables are not rewritten; (2) the stage `version` field says 0.13.0
   (stale install metadata; the code is v0.14.0 at `git_head`); (3) the 4 merge shares below 0.95 (values);
   (4) glued words are not repaired (UA-003); (5) print REPUBLIC paragraphs come from LotA `data/paragraphs/`, not a
   shared-data dataset; (6) pilot inventories only: a full SG run will be a new version, not added into this one
   (the run-level word-break corpus default would change). Write with `json.dump(..., indent=2, ensure_ascii=False)`
   and a trailing newline. Re-run step 4's audit afterwards: the numbers must not change.
6. **Protected hashes after** → `protected-after.txt`; `diff` against before must be empty.
7. **shared-data `README.md`** (Edit only): after the `NL-HaNA_1.10.94/text_layer_v2/` catalogue row, add
   `NL-HaNA_1.01.02/text_layer_v3/` and `NL-HaNA_1.10.94/text_layer_v3/` rows in the same style as the v2 rows
   (what, `coverage: partial`, inventories, producer AS `text_layer` presets `sg_1.01.02_v3`/`sg_1.10.94_v3`, AS 0.14.0
   at `fa5cc63`, audit 178/182 with the 4 merge shares, size from `du -sh`, *used by* `LotA (planned)`). Say that v2
   stays until no row lists a consumer. Add a changelog line after the last one, dated today.
8. **LotA `docs/from-archival-structures.md`** (Edit only): at the start of §8 (*Coming in text_layer_v3*) add one
   line: "Published as a pilot on <date>: see §9." Append `## 9. SG text layer v3 (pilot), published <date>`: the two
   paths; inventories; built from the same code as `15m-a` (equal up to path columns), so every change listed in §8
   holds; the reader changes §8 lists; the 116-row prediction (0.526, bound 0.560); the known gaps; the full run is
   not scheduled (AS phase 10 planning) and will be a new version. Do not touch any other LotA file.
9. **AS `docs/text_layer.md`**: add `## Published SG pilot (text_layer_v3)` directly before `## Provenance`: a path
   table like the v2 one (section *Published SG pilot (text_layer_v2)*), the source dev layer, the known gaps, and a
   pointer to *Text layer v3 (phase 09)* for what changed.
10. **AS `docs/from-language-of-the-archive.md`**: append to §7 one line: "**Done** <date> (AS step 15o): published
    at … ; LotA's §9 in `docs/from-archival-structures.md`."

## Constraints

- Write under shared-data only the two new `text_layer_v3/` dirs and `README.md`; in LotA only the one doc. Never
  write through a symlink (the `15o-pub` links are read by the audit only).
- No library code changes, no new tests (no code paths change). Don't run the build.
- Never `git commit`, never bump the version, `chmod g+w` everything you create, `.venv/bin/python`.

## Acceptance criteria

1. Both copies exist, without `progress.log`/`status.tsv`, group-writable.
2. `audit_published.json`: 178 pass, 4 fail, the 4 known invariants, `counts` equal to `15n-a`'s.
3. Both manifests have `publication` with every field above; the 8 archive hashes match the v2 manifests.
4. `protected-before.txt` = `protected-after.txt`.
5. The README rows and changelog line, LotA §9 and the §8 pointer, and the two AS doc edits are written.

## Error analysis

Short, but read rows, not only counts: from the **published** copies, print the first 3 `join/units` rows of 3349
and of 455 (`text` cut to 150 characters) and the `doc_id_map` row for one 02d `hand_doc_id` of 3349 (from
`reports/data/phase-09-text-layer-v3-prep/step-15m/rows116_15m-a.tsv`, column `hand_doc_id` if present; else say so).
Check that they look like readable resolution text in reading order and that the 455 rows show no column interleave.
Quote them in the report section.

## When done

1. Report section `### Step 15o: v3 pilot publication` (what was published, where, acceptance results, the
   error-analysis quotes, findings `F09-15o.n` as `draft` rows in the *Findings* table). `make reports`, no warnings.
2. Append a `PROGRESS.md` block. Overwrite `STATUS.md`: 15o done, waiting for review; next action: review of 15o
   (Opus), then phase 10 planning.
