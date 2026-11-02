# Step 23a: Fresh gold (folio crops for the user, TOC pages, locators and link targets read by agents)

> **Phase:** 17, VOC rules v1. **Depends on:** nothing. Runs in parallel with 23b (and the dev stages of 23c/23d).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** In
> particular, don't read the phase `PLAN.md` or `docs/shared_library/upstream_inventory.md`;
> what you need from them is copied in here. If you have to explore the code base anyway, do
> what you must and record it under *Brief defects*.

## Context

Phase 17 writes VOC rules for the folio map (23c), a TOC detector (23d) and locators (23e). Each is tested once, on
gold from 24 VOC volumes that no phase has read; the planner drew them by seed and opened nothing. This step builds
those volumes and produces all the test gold before any phase-17 rule sees them:
- **folio anchors**, read by the user on page-top image crops (UA-061);
- **TOC page labels, locators and link targets**, read by Sonnet subagents from the HTR text.

The gold is frozen and hashed. No library code changes.

## Inputs

Paths are relative to the AS root (checked 2026-11-01).
- `reports/data/phase-17-voc-rules-v1/planning/fresh_volumes.tsv`: `rank inv role zip_mb obp_rows`, 40 rows. Ranks
  1–24 are `sample`, 25–40 `reserve`. Zips: `/Volumes/T7_Data/Data/PageXML/Globalise/pagexml_v2.1/<inv>.zip`.
- Phase-16 inputs dir `data/text_layer_voc_inputs/1.04.02/` (567 `<inv>.zip` symlinks). Assert the 40 are disjoint
  from it. Model your staging on `reports/data/phase-16-voc-generalisation/step-22a/stage_inputs.py`.
- Preset `voc_1.04.02_v5sg` (unchanged; `archival_structures/text_layer/presets.py`).
- Layer tables after the build (`<root>` = `data/text_layer_voc_23a/1.04.02/NL-HaNA_1.04.02_text_layer_v5`):
  - `scans/<inv>.scans.tsv.gz` (`scan_id … scan_num … page_w page_h n_lines …`);
  - `lines/<inv>.lines.tsv.gz` (`scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw
    text …`);
  - `layout/lines/<inv>.tsv.gz` (has `scan_id line_id page`; the side key is (scan_id, page), page ∈
    `single`/`verso`/`recto`);
  - `regions_meta/<inv>.tsv.gz` (`scan_id region_id region_tag`).
- Page-number texts per side: `read_volume_inputs(root, inv, number_tags=("page-number",)).numbers`.
  - Signature: `read_volume_inputs(layer_root: Path, inv: str, *, number_tags: Collection[str] = ()) -> VolumeInputs`.
  - `VolumeInputs` has fields `inv scan_ids sides flags numbers`, with `numbers` = {(scan, page): [texts]}.
  - Module: `archival_structures.analysis.index_links`.
  - **Do not pass `flags` or anything from `layout/pages/` to readers.**
- Crops: `/Users/Shared/claude_projects/data_scopes/shared-tools/handprint.py` and `checkpage.py`.
  - Read their README sections `checkpage` and `handprint` in `/Users/Shared/claude_projects/data_scopes/shared-tools/README.md`.
  - Copy the crop logic of `reports/data/phase-16-voc-generalisation/step-22b/build_check_page.py` (from
    `import handprint` to the `json.dump`). It does:
    - `handprint.na_iiif_bases(scan_ids, cache_dir)`, then `handprint.image_size(base, cache_dir, scan)`;
    - `handprint.crop(base, (0, 0, w, y1), (w, h), img, pad=(0, 0), display_w=1000, full_w=2400)` with
      `y1 = max(0.32·h, bottom of a Laypa page-number line in the top half + 40)`, capped at `0.5·h`;
    - it caches the viewer pages and `info.json` in a cache dir (use `step-23a/iiif_cache/`).
- Audit: `reports/data/phase-13-register-index-volumes/step-19e/audit.py --layer <base> --out F` (read `n_fail_judged`).
- Protect: `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT` and
  `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 23`.

## Deliverables

All under `reports/data/phase-17-voc-rules-v1/step-23a/` (`chmod g+w` everything, also on the drive).

| File | Content |
|---|---|
| `stage_inputs.py`, `inventories.txt` | symlinks `data/text_layer_voc_inputs_23a/1.04.02/<inv>.zip` for all 40; asserts disjointness |
| `audit.json`, `protect_{before,after}.txt`, `protect_voc_{before,after}.txt` | build checks |
| `dump.py`, `show.py`, `dumps/` | reader inputs: `<inv>_front.txt`, `<inv>_numbers.tsv` (below); `show.py --inv X --from A --to B` prints scans A..B (scan numbers) as in `_front.txt` |
| `reads/` | raw reader outputs, one TSV per volume and task |
| `sample_lines.py` | draws the link lines (below) |
| `folio_sample.py`, `folio_sample.tsv`, `build_folio_page.py`, `folio_page.html` | the user's crop page |
| `summarise_folio.py` | `folio_verdicts.json` → `gold/folio_gold.tsv` + `gold/folio_gold.done` |
| `gold/toc_labels.tsv`, `gold/link_lines.tsv`, `gold/link_targets.tsv`, `gold/SHA256SUMS`, `gold/agent_gold.done` | frozen agent gold |
| `docs/USER_ACTIONS.md` | UA-061 (below) |
| `reports/phase-17-voc-rules-v1.md` | `### Step 23a: …` section, findings `F17-23a.n` |

## Behaviour to implement

**1. Build.** Run `protect_voc.py step-23a/protect_voc_before.txt` and `protect.py step-23a/protect_before.txt --skip 23`.
Stage, then:
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures \
  --label 23a --want 4 --min 1 --wait -- .venv/bin/as-build-text-layer --preset voc_1.04.02_v5sg \
  --pagexml-dir data/text_layer_voc_inputs_23a/1.04.02 \
  --out-dir /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/23a-a/1.04.02 \
  --stage all --workers '{workers}'
```
Create the link `data/text_layer_voc_23a` → `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_voc/23a-a`.
The 40 zips total about 1 GB, and 22a took about 23 s per volume per worker, so expect minutes. Run the audit
(`--layer data/text_layer_voc_23a`), then both protect scripts with `_after`.

**2. Dumps** (`dump.py`; scripts only, run with `-I`). For each of the 40 volumes:
- `<inv>_front.txt`: scans 1 to min(n, 100) in scan order. Per scan, a header line `== <scan_id> | #<scan_num> |
  sides: <pages> | lines: <n>`, then every non-empty line text in `lines/` file order, each prefixed by its page
  (`[verso]`, `[recto]` or `[single]`).
- `<inv>_numbers.tsv`: one row per side of the whole volume, `scan_id scan_num page texts`, where `texts` is the
  page-number texts joined by ` | ` (empty if none).

**3. TOC labels** (Sonnet reader subagents, `model: "sonnet"`, at most 4 volumes each, in parallel).
- Order: the 24 sample volumes first. Then apply the reserve rule: if fewer than 12 of the 24 have a **front
  register TOC** (≥ 1 scan labelled `toc`/`register` among scans 1–60), read reserves in rank order until 12 do or
  the reserves run out.
- Each reader gets the self-contained prompt below. Merge their outputs into `gold/toc_labels.tsv`: `inv scan_id
  scan_num label list_type note reader`, one row per scan read. The read region is scans 1 to max(60, last `toc`
  scan + 10), capped at 100.

Reader prompt (fill in `<…>`):
> You label VOC archive scans as table-of-contents pages or not, from HTR text. Read only the files named here.
> Do not open other files under `reports/`, `data/` or the layer, and do not run project code.
> For volume `<inv>`, read `<dump path>` and label every scan from scan 1 to scan max(60, last toc scan + 10),
> capped at 100. Labels:
> - `toc`: a page of a list that points into this volume. Give its `list_type`:
>   - `register`: documents listed one by one, often with a date, sender or place, and folio numbers;
>     "Register der brieven/papieren";
>   - `alpha_index`: an alphabetical list of names, places or subjects with folio numbers;
>   - `title`: the title page of such a list;
>   - `other_list`.
> - `no`: anything else (letters, accounts and ledgers, rosters, covers, blank pages with stray text).
> - `no_text`: no HTR lines.
> - `unclear`: you cannot decide; say why in `note`.
>
> On a two-page scan, label `toc` if either page is. Folio numbers can stand in short lines of their own, or as a
> range at the start or end of an entry ("288 a 291", "656 a 668 verso"). An account page with amounts ("ƒ",
> "28249:11:9", "Transport") is `no`.
> Write `<reads path>/toc_<inv>.tsv` with the header `scan_id\tscan_num\tlabel\tlist_type\tnote`, one row per scan.

**Self-check.**
- Draw 30 scans (seed 20261101) from the merged labels, 15 labelled `toc` and 15 `no`, from at least 6 volumes.
- Write their ids to `reads/selfcheck_ids.txt` **before** you look at their labels.
- Label them yourself from the dumps, then compare. Record your labels and the reader's in `reads/selfcheck.tsv`.
- Agreement ≥ 28/30 is required; below that, stop and ask.

**4. Link lines** (`sample_lines.py`, seed 20261101).
- Use the volumes with a front register TOC.
- Candidates: the lines (from `lines/`) on scans labelled `toc` with `list_type` `register`, with ≥ 2 non-space
  characters.
- Draw 8 per volume, or all of them if there are fewer.
- Write `gold/link_lines.tsv` (`inv scan_id page line_id line_idx text gold_folios status note`). `gold_folios` and
  `status` are filled by readers. **Row grain: one row per sampled line.**

**5. Locators and targets** (reader subagents, ≤ 4 volumes each). Prompt, after the same file rules as in 3:
> For each line in `<lines file>` of volume `<inv>`, run `.venv/bin/python -I <show.py> --inv <inv> --from S --to S`
> to see the line with the lines around it.
> - `gold_folios`: the folio numbers at which the documents this line names **start**, separated by `;`.
>   - For a range give its first number ("288 a 291" → 288; "180 verso a 199 verso" → 180).
>   - A line that is only a folio number names that number.
>   - Dates, years, day numbers, amounts and paragraph numbers are not folios.
>   - If the entry's folio stands on another line, it belongs to that line, not this one.
>   - Leave the field empty if the line names none.
> - `status`: `ok`, or `unreadable` (too garbled to decide).
>
> Then, for each folio N you gave, find the scan where that document **begins**:
> 1. Look up where N (or the numbers near it) is printed in `<numbers.tsv>`.
> 2. Read the candidate scans with `show.py`.
> 3. The target is the scan whose text begins the document the entry describes (sender, place or date match).
>
> Write one row per (line, N) to `<reads path>/targets_<inv>.tsv`: `line_id folio target_scan status confidence
> evidence`.
> - `status`: `found` or `not_found`. Use `not_found` when nothing within ±10 scans of where N is printed (or of
>   your estimate, when N is not printed) begins that document.
> - `confidence`: `sure` or `likely`.
> - `evidence`: at most 120 characters: the printed number you saw and the first words of the document.
>
> Do not compute anything with project code.

Merge into `gold/link_lines.tsv` and `gold/link_targets.tsv` (`inv line_id folio target_scan status confidence
evidence reader`). **Row grain: one row per (line, folio).**

**6. Freeze.**
- Write `gold/SHA256SUMS` (sha256 of `toc_labels.tsv`, `link_lines.tsv`, `link_targets.tsv`, and of
  `step-23a/folio_sample.tsv`).
- Write `gold/agent_gold.done` (UTC time, the same hashes). 23d and 23e wait on this file.
- From now on, never edit these files.

**7. Folio crops.**
- `folio_sample.py`: for each of the 24 sample volumes, draw 10 scans uniformly from all its scans, without
  replacement, with `random.Random(f"20261101-{inv}")`. Write `folio_sample.tsv` (`item_id inv scan_id scan_num
  side_layout`). `side_layout` is `openings` if verso+recto sides are ≥ 0.5 of the volume's sides in
  `layout/lines`, else `single`. That gives 240 items.
- `build_folio_page.py`: one crop per item (band as in Inputs).
  - Page parts: `checkpage.BASE_CSS` and `checkpage.HELPERS_JS`, and `CheckStore("as_phase17_23a_folio_v1")`.
  - Per item: the crop (linked to its `full` URL), the inventory and item k/240, and the scan ID with an NA viewer
    link and copy button (`scanLine`).
  - If the crop is None, show "no crop" with the viewer link.
- Input per item:
  - a number field (focused): the folio number on the **front** sheet's top right (on a two-page scan, the right
    page);
  - keys: `Enter` saves and goes to the next item, `n` = no number, `u` = unreadable, `←`/`→` move between items;
  - optional fields: "left page number" (two-page scans with numbered left pages); "number on a sheet beneath" (a
    smaller sheet lies on a larger one whose number shows); a note.
- Instructions at the top: what to read, these definitions with an example each, and the export step (Export →
  `reports/data/phase-17-voc-rules-v1/step-23a/folio_verdicts.json`).
- **Blind:** no HTR text, no page-number text and no prediction in the HTML. Grep the HTML for 3 random line texts
  of the sampled scans, and for `pred`.
- Check the item count (240) and the JS syntax (`osascript -l JavaScript` with `new Function(src)`; see
  `checkpage.py --selftest`).
- `summarise_folio.py`: verdicts → `gold/folio_gold.tsv` (`item_id inv scan_id scan_num side_layout status number
  left_number beneath_number note`; `status` ∈ `number`/`none`/`unreadable`/`missing`, where `missing` means no
  verdict). Then append the hash to `gold/SHA256SUMS` and write `gold/folio_gold.done`. Test it on a fake verdict
  file in the scratchpad. **Do not run it on real data before the user exports.**

**8. UA-061** in `docs/USER_ACTIONS.md` (format of that file):
- Action (Run): `open reports/data/phase-17-voc-rules-v1/step-23a/folio_page.html` from the AS root. Read the 240
  crops (about 30–40 min), then Export into that dir. Then Run `.venv/bin/python -I
  reports/data/phase-17-voc-rules-v1/step-23a/summarise_folio.py`.
- Default if unanswered: bar F is not tested; 23c stops after its dev stage.
- Blocks: the 23c test and the phase review.

## Constraints

- No library code, no test changes. Never `git commit`; never bump the version.
- **Never open** `data/text_layer_voc_23a/*/*/layout/pages/` and never run folio-map, detector or locator code on
  the 40 volumes. Readers get only the dumps, `show.py`, `numbers.tsv` and their own files.
- Drive: read `pagexml_v2.1/`; write only under `…/claude-projects-external/archival-structures/data/text_layer_voc/23a-a/`.
  A hook denial or `Permission denied` is final: stop and ask.
- Agent shell: no `rm -rf`, `nohup`, `&` or `$TMPDIR` (logs go to the scratchpad dir named in your environment); zsh
  (quote `'{workers}'`); BSD `sed -i ''`; never `git stash`/`checkout`/`reset`; a new output dir per run (`23a-b` if
  you must restart).
- Parallel sessions (23b, 23c, 23d may run now): edit only your own row of `STATUS.md`, and edit the report,
  `PROGRESS.md` and `docs/USER_ACTIONS.md` with Edit only.
- An error-analysis sample with fewer cases than asked: read all of them.

## Acceptance criteria

1. 40 volumes staged, disjoint from the 567 (assert in `stage_inputs.py`); `failures=0`; audit `n_fail_judged = 0`.
   Otherwise stop and ask.
2. Every read volume has a label for each scan of its region. `unclear` ≤ 5% of the labelled scans (otherwise stop
   and ask). Self-check ≥ 28/30.
3. Report the number of volumes with a front register TOC (target ≥ 12, using reserves). Fewer after all reserves
   is a finding, not a stop.
4. `link_lines.tsv` has 8 rows per TOC volume (or all candidates), and every one has a `status`.
   `link_targets.tsv` has one row per gold folio. Report the `found`/`not_found` and `sure`/`likely` counts.
5. `folio_page.html` has 240 items, the blind grep is clean, the JS parses, and UA-061 is written.
6. `gold/SHA256SUMS` and `gold/agent_gold.done` exist. The `protect_voc` before/after diff shows only the new 23a
   paths. Any other `protect.py` change is explained (shared-data changes by other projects are expected; name them).

## Error analysis

- Every self-check disagreement, with the text.
- 10 `not_found` targets (all, if fewer): why (no printed number nearby, subdocument, garbled).
- 10 `unreadable` lines (all, if fewer).
- The `list_type` counts and the TOC lengths per volume (min, median, max scans).
- How many fresh volumes have no TOC, and what their front scans hold (one line each, from your own reading).
- Name `data-property` findings: the share of fresh volumes with a front register TOC; where TOCs start and how long
  they run; the share of link lines that name no folio.

## Do not touch

`archival_structures/`, `tests/`, the 22a layer, phase-16 files, and the 23b–23e step dirs.

## When done

1. Add `### Step 23a: Fresh gold` to `reports/phase-17-voc-rules-v1.md` (template `plan/templates/REPORT.md`):
   what was built, gold sizes, label distribution, self-check, TOC volumes, target statistics, the error analysis,
   findings `F17-23a.n` (status `draft`). Then run `make reports` and fix any warning.
2. Append the `PROGRESS.md` block from `plan/templates/STEP.md`.
3. In `STATUS.md`, overwrite only the 23a row and add the gold paths and counts under *Facts later steps need*.
   UA-061 goes under *Blockers*.
