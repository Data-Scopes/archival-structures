# Step 24b: Stage, build and sample (layers for 7 archives, 100 pages, images, reader dumps)

> **Phase:** 18, Page element types v1. **Depends on:** 24a (the seven `*_v5sg` presets).
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** If you have to explore the code base
> anyway, do what you must and record it under *Brief defects*.

## Context

Phase 18 builds whole-page gold for page element types (running text, list, table, heading, marginal note, …) on 100
pages from 10 strata: 60 test pages the user will check, 40 dev pages read by agents only. This step builds text layers
for the 7 archives that have none, draws the pages, fetches one image per page and writes the **reader dumps**: what
the pre-labelling agents of step 24c see. The dumps hold only PageXML text and line geometry, never AS's own calls
(types, layout, `listlike`), because 24d measures those calls against the gold.

## Inputs

- `reports/data/phase-18-page-element-types/planning/volumes.tsv` (sha256
  `5b4d1cc54dca35401bc0f1f61dcf6a7f1ec5cb23d9a4a60dd9fe891e67dbc5ab`; check it first, stop if it differs). Columns
  `stratum archive inv split source n_files`, one row per volume (31 rows). `split` is `test`, `dev`, or `halves` (one
  volume per stratum: dev = first half of its scans, test = second half). Strata: `sg_hand`, `sg_print`, `voc`,
  `na_1.05.06`, `na_2.10.50`, `na_2.10.01`, `asdsaa_5023`, `asdsaa_5075`, `asdsaa_89`, `asnda_0114.11`.
- Existing layers (read only):
  - SG: `/Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/text_layer_v5/1.01.02/NL-HaNA_1.01.02_text_layer_v5`
    (the local `data/text_layer_v5_full/` has only 23 volumes' tables: do not use it for lines);
  - VOC: `data/text_layer_voc_23b/1.04.02/NL-HaNA_1.04.02_text_layer_v6` (a link to the drive).
- Sources to stage: the local tarballs named in `volumes.tsv` (member paths `<archive>/<archive>_<inv>/<file>.xml`;
  members whose base name starts with `._` are AppleDouble files, not PageXML) and the 2.10.01 zips on the drive
  (`/Volumes/T7_Data/Data/PageXML/Ministerie_van_Kolonien/2.10.01/scans/<inv>.zip`, read only).
- Presets (24a): `na_1.05.06_v5sg`, `na_2.10.50_v5sg`, `na_2.10.01_v5sg`, `asdsaa_5023_v5sg`, `asdsaa_5075_v5sg`,
  `asdsaa_89_v5sg`, `asnda_0114.11_v5sg`; layer name `<archive>_text_layer_v5sg`.
- `scans/<inv>.scans.tsv.gz` columns: `scan_id archive_id inv_num inv_dir scan_num scan_variant scan_id_ok member
  status page_w page_h n_regions n_regions_text n_lines n_lines_text n_words`. AsdSAA scan ids are file stems
  (`0001_A14767000001`, `scan_num` empty).
- `lines/<inv>.lines.tsv.gz` columns: `scan_id page_side column region_id line_idx line_id x y w h baseline conf text_raw
  text edit_flags split_from`. Key (scan_id, line_id): line ids repeat across scans in AsdSAA.
- Images:
  - NA (SG, VOC, 2.10.50, 2.10.01): `sys.path.insert(0, "/Users/Shared/claude_projects/data_scopes/shared-tools");
    import handprint`; `handprint.na_iiif_bases(scan_ids, cache_dir) -> {scan_id: base}` (one NA viewer page per
    inventory, cached; network) and `handprint.image_size(base, cache_dir, scan_id) -> (w, h) or None`. Image URL
    `f"{base}/full/1200,/0/default.jpg"`. Planning probe: found for 1.01.02 10115, 1.04.02 2361, 2.10.50 85, 2.10.01
    250; **not** for 1.05.06 (its viewer page has no file list).
  - Thumbs root `T = /Volumes/T7_Data/Data/claude-projects-external/archival-structures/data/thumbs`:
    - 1.05.06: `T/NL-HaNA/NL-HaNA_1.05.06/<inv>/<scan_id>.tif` (600 px wide); convert to PNG (Pillow is in `.venv`);
    - AsdSAA 89: `T/NL-AsdSAA/NL-AsdSAA_89/NL-AsdSAA_89_3.1/KLAB08969<NNNNNN>-info.json` (IIIF Image 3; `id` is the
      base), matched by position: PageXML stem `0377_00377` → `KLAB08969000377` (6-digit zero-padded first number).
      The planner checked one: info width/height 2427×3028 equals the PageXML's. Image URL
      `f"{id}/full/1200,/0/default.jpg"` (planning probe: HTTP 200, JPEG);
    - AsdSAA 5023 / 5075: `T/NL-AsdSAA/NL-AsdSAA_5023/NL-AsdSAA_5023_4/thumb-width_600-scan-<stem>.jpg.png`
      (5075: `NL-AsdSAA_5075/NL-AsdSAA_5075_2409/`);
    - AsnDA: `T/NL-AsnDA/NL-AsnDA_0114.11/NL-AsnDA_0114.11_1/thumb-width_300-scan-<scan_id>.jp2.png` (300 px wide).
- Protect scripts: `reports/data/phase-09-text-layer-v3-prep/planning/protect.py OUT --skip 24` and
  `reports/data/phase-17-voc-rules-v1/planning/protect_voc.py OUT`.
- SG dev layers (for the leakage assert): every `data/text_layer_v2_dev/*/*/*/scans/<inv>.scans.tsv.gz`.

## Deliverables

All under `reports/data/phase-18-page-element-types/step-24b/` (call it `D`) unless stated.

| File | Content |
|---|---|
| `D/stage_inputs.py`, `D/build.sh`, `D/build_log.txt` | staging and the seven CLI runs |
| `data/page_elements_inputs/<archive>/` | staged sources (local, gitignored `data/`) |
| `data/page_elements_layers/24b-a/<archive>/<archive>_text_layer_v5sg/` | the seven layers |
| `D/draw_pages.py`, `D/sample_pages.tsv`, `D/SHA256SUMS` | the page draw |
| `D/fetch_images.py`, `data/page_elements_images/<page_id>.{jpg,png}`, `D/iiif_cache/` | one image per page |
| `D/dump.py`, `D/dumps/<page_id>.txt`, `D/dumps/<page_id>.lines.tsv` | reader inputs |
| `D/protect_{before,after}.txt`, `D/protect_voc_{before,after}.txt` | protect pairs |
| `reports/phase-18-page-element-types.md` | `### Step 24b: …` section, findings `F18-24b.n` |

Scripts run with `.venv/bin/python -I` from the AS root; each starts with a docstring giving its command.

## Behaviour to implement

**0. Protect** (before): both protect scripts with `_before` outputs. Check `volumes.tsv`'s hash.

**1. Stage** (`stage_inputs.py`). For every `volumes.tsv` row whose stratum is not `sg_*` or `voc`:
- tarball sources: extract the members under `<archive>/<archive>_<inv>/` whose base name does not start with `._`
  into `data/page_elements_inputs/<archive>/<archive>_<inv>/` (strip the first path component). Count files; it must
  equal `n_files`;
- 2.10.01: a symlink `data/page_elements_inputs/NL-HaNA_2.10.01/<inv>.zip` → the drive zip.
`chmod -R g+w data/page_elements_inputs`.

**2. Build** (`build.sh`), one run per archive, each into its own new out dir:
```
python3 /Users/Shared/claude_projects/data_scopes/shared-tools/coreshare.py run --project archival-structures \
  --label 24b --want 4 --min 1 --wait -- .venv/bin/as-build-text-layer --preset <key> \
  --pagexml-dir data/page_elements_inputs/<archive> --out-dir data/page_elements_layers/24b-a/<archive> \
  --stage all --workers '{workers}'
```
Expect seconds per volume (planning: 8–25 s per inventory on 1 worker). Acceptance: `status.tsv` lists every staged
inventory with no failure; record per inventory the scans table `status` counts and the `mode/<inv>.json` mode and
method (planning saw `hand`/`default` everywhere). `chmod -R g+w` the layers.

**3. Draw pages** (`draw_pages.py`). For each `volumes.tsv` row, from the volume's layer `scans/` table:
- eligible scans: `status == "ok"`, 3 ≤ `int(n_lines_text)` ≤ 150, and an image source exists (step 4's rule; for NA
  IIIF archives the base is looked up here, for thumbs the file must exist);
- `halves` rows: order the scans as in the scans table, then index `i < n // 2` → dev, else test (n = all rows, before
  eligibility);
- count per volume: test volumes 2 pages (stratum `na_2.10.50`: 3); dev volumes 2; `halves`: 6 test and 4 dev;
- draw `random.Random(f"20261102-{stratum}-{inv}-{split}").sample(sorted(eligible), k)`. If fewer than k are
  eligible, take all and report the shortfall (do not draw elsewhere).
- Page ids `P001`… in this order: test pages, then dev pages; within each, strata in the order listed under *Inputs*,
  then inventory as in `volumes.tsv`, then scan order.
- Assert SG test and dev volumes are not in any SG dev layer run, and no scan is drawn twice.
- `sample_pages.tsv`: `page_id stratum archive inv split scan_id layer_root n_lines_text page_w page_h image_kind
  image_ref image_file`, one row per page (expect 60 test, 40 dev). `image_kind` ∈ `na_iiif`, `iiif3`, `thumb`;
  `image_ref` = the IIIF base or the thumb path. Hash it into `D/SHA256SUMS`.

**4. Images** (`fetch_images.py`), into `data/page_elements_images/`:
- `na_iiif` / `iiif3`: GET `{base}/full/1200,/0/default.jpg` → `<page_id>.jpg` (retry 3× with back-off; stop if any
  fails). For `na_iiif`, also check `image_size` aspect against the PageXML (`|img_h − page_h·img_w/page_w| ≤ 1.5 px`,
  as `handprint.crop` does) and record mismatches. For `iiif3`, check info width/height == page_w/page_h; stop on a
  mismatch (the position mapping would be wrong).
- `thumb`: copy the PNG, or convert the 1.05.06 TIFF to PNG, → `<page_id>.png`; check the aspect within 2% of
  page_w/page_h; stop on a mismatch.
- `chmod g+w` everything.

**5. Dumps** (`dump.py`). Read **only** the volume's `lines/` table (`text_layer.io.read_tsv_gz`), never `layout/`,
`types/`, `regions_meta/` or `data/annotations/`. For each page, the non-empty lines (`text.strip() != ""`, the rule `page_elements.read_layer_lines` uses) in file order:
- `dumps/<page_id>.lines.tsv`: `k line_id region_id x y w h text` (k = 1…n);
- `dumps/<page_id>.txt`: a header line `<page_id> | <archive> <inv> | <scan_id> | page <page_w>×<page_h> px | image
  <absolute image path>`, then one line per text line:
  `L<k> | r<m> | x <x%> y <y%> w <w%> h <h%> | <text>`, where `m` numbers regions by first appearance on the page and
  the percentages are of page width/height, one decimal.

**6. Protect** (after) and diff the pairs: only the new paths may differ.

## Constraints

- Write nothing on the drive. Read only the paths named above. A hook denial or `Permission denied` is final: stop.
- No AS library change in this step. `data/annotations/` is never opened.
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs. No `rm -rf`, `nohup`, `&`, `$TMPDIR`;
  zsh quoting `'{workers}'`; a new output dir per run.

## Tests

No library code. Each script has an assert-based self-check (counts, hashes, disjointness) that runs every time.
Full suite must stay green: `.venv/bin/python -m pytest -q -p no:cacheprovider`.

## Acceptance criteria

1. Seven layers built: every staged inventory in `status.tsv`, 0 failures; staged file counts equal `n_files`.
2. `sample_pages.tsv`: 60 test and 40 dev pages, or a reported shortfall; hashed; SG volumes disjoint from the dev
   runs; no duplicate scan.
3. One image per page, aspect checks passed (mismatches listed); one dump pair per page; every dump line count equals
   the page's non-empty line count.
4. Protect pairs differ only in new paths. Full suite green.

## Error analysis

Open 10 dumps (one per stratum) and their images; for each, note in the report whether the dump's line order follows the
page (columns, margins, openings), how many lines are visibly missing from the PageXML (text on the image with no line),
and any HTR noise lines. These are facts for the 24c rubric, not labels. Findings expected: per stratum, page and line
counts, the share of lines whose text is shorter than 3 characters, and the image source.

## Do not touch

- `archival_structures/` (24a owns it), `data/annotations/`, the SG/VOC layers, `shared-tools/`, sibling repos.

## When done

1. Add `### Step 24b: Stage, build and sample` to `reports/phase-18-page-element-types.md`, following
   `plan/templates/REPORT.md`; findings `F18-24b.n` (draft). Edit only your section and rows. Run `make reports`.
2. Append the PROGRESS block (template in `plan/templates/STEP.md`) to the phase `PROGRESS.md`.
3. Overwrite the phase `STATUS.md`: 24b done, next 24c; add the facts 24c needs (counts, paths) in one line each.
