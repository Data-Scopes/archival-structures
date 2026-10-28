# Step 15z: read the off-series modes and pin them (P21)

> **Phase:** 12, Hand and print by line-box height (post-review addendum). **Depends on:** 15y (pins and `--rebuild`),
> UA-034 (the 35 tars staged). **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file. **Read nothing else unless this brief names it.** Planned 2026-10-27 (Opus). The read
> list, the verdicts and the pin rule were fixed **before any read**; do not change them. Record exploration under
> *Brief defects*.

## Context

The box-height rule (cut 87.75 px) built 278 of the 9,122 full-run volumes `print`. 243 are in the printed EAD series
(*gedrukte resoluties* 105, *nouvelles* 64, *plakkaten* 74). The other 35 are the P21 list. Of these, 12 are *tweede
serie* index volumes (3433–3489), the class F12-R.5 predicted (about 9). The rest are pamphlets, extracts, verbals,
minutes and other bound material. The user added the reverse case (2026-10-27): the two silver-print *plakkaten* built
`hand`, 12163 (89.5 px) and 12165 (88.0 px). This step reads 5 scans of each of the 37 volumes and pins a volume to the
other mode only by the rule below. It rebuilds the pinned volumes locally with 15y's procedure, builds a check page for
the user and writes the run commands for the drive. 3463 is already pinned `hand` (15y). Its reads here are a check.

## Inputs

- After UA-034: `/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/` holds
  `NL-HaNA_1.01.02_<inv>-pagexml.tar.gz` for all 37 (35 new + 3463, 9486 already there). If any is missing, stop and
  name it (STATUS *Open questions*).
- The 37 volumes (full-run `mode/<inv>.json`: mode, value px, `split_gate.split`; EAD title, shortened):

| inv | mode, px | title | inv | mode, px | title |
|---|---|---|---|---|---|
| 12477 | print 74.0 | Reglement op de ambassades, 1700 | 3469 | print 83.0 (split) | 1742 index |
| 9448 | print 75.0 | Deel 1 (verbalen commissies) | 12297 | print 83.0 (split) | Systematische index commissieboeken |
| 12476 | print 76.0 | Stukken, extract-resoluties | 3436 | print 83.0 (split) | 1731 index |
| 9264 | print 76.0 | 'Cedulleboek van behoeftens' | 2586 | print 84.0 | 1753 oktober (minuten) |
| 9275 | print 76.0 | Uittreksel verbalen Van Beverningk | 3472 | print 84.0 (split) | 1743 index |
| 9250 | print 77.0 (split) | Verbaal gedeputeerden | 3433 | print 84.0 | 1730 index |
| 3442 | print 77.0 (split) | 1733 index | 3726 | print 84.5 (split) | "Tafel der respecten" |
| 7462 | print 77.5 | (no title found) | 9634 | print 85.0 | 'Nassauische Stamms-Tafel' |
| 12478 | print 78.0 | Retroacta reglement ambassades | 4792 | print 85.0 | Index (secrete) |
| 2600 | print 78.0 | 1754 december (minuten) | 3475 | print 85.0 (split) | 1744 index |
| 9449 | print 78.0 | Deel 2 (verbalen commissies) | 3489 | print 85.5 (split) | 1747 index 2e halfjaar |
| 8828 | print 78.0 | Bijlagen 1760-1764 (buitenlandse verbalen) | 9396 | print 86.0 | Deel 2 (Admiraliteiten) |
| 8080 | print 79.0 | 1672 (staten van oorlog) | 3466 | print 86.0 (split) | 1741 index |
| 9487 | print 80.5 | Sententies Krijgsraad | 8645 | print 86.0 | 'Placcaten en Edicten … Savoyen' |
| 9253 | print 81.0 | Extracten resoluties SG en Holland | 3463 | print 86.5 (split) | 1740 index (pinned `hand` in 15y) |
| 3445 | print 81.0 (split) | 1734 index | 9486 | print 79.0 | Sententies Krijgsraad (15w: printed 5/5) |
| 9252 | print 81.0 | Memorie van prins Willem V | 12165 | **hand** 88.0 | Gedrukte plakkaten 1631-1640 |
| 3439 | print 82.0 (split) | 1732 index | 12163 | **hand** 89.5 | Gedrukte plakkaten 1611-1620 |
| 4016 | print 82.5 | 1685 oktober (geresumeerde secrete) | | | |

- Read list `reports/data/phase-12-hand-print-box-height/planning/reads_15z.tsv` (185 rows: `read_id, inv, scan_id,
  scan_order, n_chars`; 5 scans per volume, weighted by characters; drawn by `draw_reads_15z.py` in that dir, seed
  20261104).
- Full run (read only): `data/text_layer_v4_full/1.01.02/NL-HaNA_1.01.02_text_layer_v4/mode/` (`<inv>.json`, `scans/`).
  `data/text_layer_v4_full/run_v4.log` names a drive path: never open, list or quote it.
- From 15y (read its STATUS facts): `presets.MODE_PINS_V4` (`{"3463": "hand"}`), `run_text_layer_v4_full.py --rebuild
  ARCHIVE=F`, `step-15y/same_inv.py`, the throughput model in the run script, audit method `pinned`.
- Check page module: `/Users/Shared/claude_projects/data_scopes/shared-tools/checkpage.py` (read its README section
  *checkpage* in `/Users/Shared/claude_projects/data_scopes/shared-tools/README.md`; `checkpage.BASE_CSS`,
  `checkpage.HELPERS_JS` with `scanLine`, `idLine`, `CheckStore`; `checkpage.scan_url(scan_id)`).
- EAD titles: `reports/data/phase-12-hand-print-box-height/review/review_15w.py` `ead_titles(invs)` (reads
  `data/metadata/eads/NL-HaNA_1.01.02.xml`).

## Deliverables

| File | Change |
|---|---|
| `data/text_layer_v4_staged_15z/NL-HaNA_1.01.02/` | 37 symlinks to the shared-data tars |
| `data/text_layer_v2_dev/15z-a/`, `15z-b/` | fresh v4 build of the 37; copy of it rebuilt with the new pins |
| `reports/data/phase-12-hand-print-box-height/step-15z/` | `reads.md` (read file), `verdicts.tsv`, `volumes.tsv`, `pins.txt`, `pins_drive.txt`, compare/audit outputs, `check_page.html`, its builder, protect lists |
| `text_layer/presets.py` | the new pins in `MODE_PINS_V4` (sorted by inventory), with a comment per group naming `step-15z/verdicts.tsv` |
| `tests/test_text_layer_modes.py` | `test_v4_pins` updated to the new dict |
| `docs/text_layer.md` | v4 *Known issues*: the pinned volumes; mixed volumes left as built |
| `docs/shared_library/changelog.md` | `### Step 15z: …` under *Unreleased* (Edit) |
| `docs/USER_ACTIONS.md` | the UAs below |
| `reports/phase-12-hand-print-box-height.md` | `### Step 15z: …` after § Step 15y; findings `F12-15z.n` |

## Steps

1. Protect `_before` (as 15y, `--skip 15z`, and the `find … shasum` list plus `data/text_layer_v2_dev/15y-a`, `15y-b`).
2. Link the 37 tars into `data/text_layer_v4_staged_15z/NL-HaNA_1.01.02/` (`ln -s`, `chmod g+w` the dirs).
3. `coreshare.py status`, then `.venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --src
   1.01.02=data/text_layer_v4_staged_15z/NL-HaNA_1.01.02 --work-dir data/text_layer_v4_15z_inputs --out-dir
   data/text_layer_v2_dev/15z-a --check-only` (expect 37), then without `--check-only`.
4. **Reproduction check:** for each of the 37, `value` and `split_gate` in `15z-a/…/mode/<inv>.json` equal the full run's.
   `mode` equal too, except 3463 (`hand`, `pinned`). If anything else differs: stop (the tar copied may not be the one
   built).
5. Read file `step-15z/reads.md`, in `read_id` order: per read the inventory, the EAD title, the scan id, and the full
   text of the scan's three largest regions (by characters) from `15z-a/…/lines/<inv>.lines.tsv.gz`, lines in reading
   order. **No mode, no box height, no value.** Read it whole and write `verdicts.tsv` (`read_id, inv, scan_id, verdict,
   note`), one row per read, before you compute anything per volume.
6. Volume table `volumes.tsv` (inv, built mode, value, n read, counts per verdict, decision, title) by the pin rule.
   `pins.txt`: the volumes the rule pins whose mode in `15z-a` differs from the pin (one per line).
7. Pins into `presets.MODE_PINS_V4`, test updated, suite green. `cp -R data/text_layer_v2_dev/15z-a
   data/text_layer_v2_dev/15z-b && chmod -R g+w data/text_layer_v2_dev/15z-b`, then the step-3 command with
   `--rebuild 1.01.02=reports/data/phase-12-hand-print-box-height/step-15z/pins.txt --out-dir data/text_layer_v2_dev/15z-b`.
8. Compare `15z-a` vs `15z-b` (`step-15v/compare.py … --sub-map 1.01.02:1.01.02 --explained-invs <pins>`), and the
   audit of `15z-b`. Check page, UAs, report.

## The read rule (pre-registered 2026-10-27)

Verdict per scan, from the text alone: `printed` (typeset: regular lines of similar length, printed page numbers or
signatures like `( 83 )` or `A 2`, catchwords, running heads, consistent spelling and punctuation, a printed title page);
`handwritten` (abbreviations such as `Reg:`, `miss:`, `Ed. Mo.`, `fol.` references in index entries, ragged line
lengths, `=` and `„` break marks at line ends, marginal dates, corrections); `mixed page` (both on one page, e.g. a
printed form filled in by hand, or a handwritten note on a printed page); `table/numbers` (mostly figures; cannot
tell); `ocr_junk` (no readable words); `unreadable input` (too short or empty to judge); `unclear` (readable text that
does not settle it). The reader context is the text only, the EAD title included. Write a short note quoting the cue.

**Pin rule.** For a volume built in mode `m`, let `opp` be the number of its read scans with the verdict of the other
class (`handwritten` for `m = print`, `printed` for `m = hand`) and `same` those with `m`'s class. **Pin to the other
mode iff `opp ≥ 4`** (of 5), or, for a volume with fewer than 5 readable verdicts, iff `same = 0` and `opp ≥ 2`.
`mixed page`, `table/numbers`, `ocr_junk`, `unreadable input` and `unclear` count for neither. A volume with `opp` 2 or 3
is a **mixed volume**: no pin. It goes into UA (b) for the user. Everything else stays as built. Never pin a volume
outside the 37. Never change the cut, the gate or the read list.

Expectations (not bars): most of the 12 *tweede serie* index volumes and 4792 are handwritten (F12-R.5); 9486, 9487 and
the pamphlet-like titles (12477, 12476, 12478, 9275, 9252, 9253, 8645, 9634) are printed; the minutes 2586, 2600, 4016
are a question (minority shares 0.56–0.65); 3463 should come out `handwritten` (15w read 5/5). If 3463 does not, report
it and keep its pin (15y). A volume pinned against its EAD series (for example a *tweede serie* volume read as printed)
is a finding.

## Acceptance criteria

1. Step 4 reproduction: 37/37 values and gates equal, modes equal except 3463.
2. 185 verdict rows; the volume table follows the pin rule (give the counts per decision).
3. Compare `15z-a`/`15z-b`: only the pinned volumes differ; the other volumes' per-inventory files are equal; the corpus
   files differ only in their entries and in `wbc/` pooled stats. `mode/<inv>.json` of a pinned volume: `method` `pinned`,
   `box_height_mode` = its old mode. A volume pinned `hand` has no `split_model/`, `split_stats/` or `split_from` lines.
   A volume pinned `print` passes the split gate as the audit checks it.
4. Audit of `15z-b`: 0 judged failures.
5. Tests green (counts); protected lists unchanged (or differences named as other projects').

## Error analysis

- The verdicts: counts per class and per volume group (index, minutes, pamphlets and extracts, other, *plakkaten*),
  verbatim cues for 2 volumes per group, every `unclear` and `mixed page` with its text. Explain each mixed volume.
- **Pinned volumes before/after.** For each pinned volume, take its first read scan (lowest `read_id`) and put its units
  in `15z-a` and `15z-b` side by side, full text: `better`, `same`, `worse`, `mixed` (closer to the entries or
  paragraphs on the page). Print pins: also say whether the drop-cap rule and the gate behave as in other *plakkaten*.
- **Resolution (F12-R.1).** Median `page_w` and `page_h` of the 37 (from `mode/scans`) against the median of all
  9,122 volumes and of their EAD series (`reports/data/phase-10-hand-print-per-region/step-15p/inventories.tsv`,
  `ancestor_3`). Flag a ratio outside 0.85–1.15. Planning view: ≈ 5,000–5,500 × 3,900–4,300, *plakkaten* about
  4,000 × 2,600 (landscape), so no shift is expected.

## User actions to write (next free ID: check both UA files; UA-034 was the highest at planning)

- **(a) Check the pins, then rebuild on the drive.** **Run** in the AS root, on the machine with the drive, after the
  check page: `.venv/bin/python scripts/run_text_layer_v4_full.py --only 1.01.02 --rebuild
  1.01.02=reports/data/phase-12-hand-print-box-height/step-15z/pins.txt --src 1.01.02=<1.01.02-dir> --out-dir <drive>/layer
  --check-only`, then the same without `--check-only`, then `.venv/bin/python scripts/stage_text_layer_v4_full.py --from
  <drive>/layer --to data/text_layer_v4_full --inventories <the pinned inventories> --max-inventories 60` (`<drive>/layer`:
  the full run's `--out-dir`; `pins.txt` must include 3463 for the drive, because the full run built it `print`: write
  that file as `pins_drive.txt` = 3463 plus the new pins, and put that name in the command). Give the predicted time from
  15y's model (the wbc decision over 9,122 and the about 1 h audit dominate). The check page: open
  `step-15z/check_page.html`, judge each volume, and export `check_verdicts.json` into `step-15z/`. Options: (a) check,
  then run; (b) run without the check; (c) do not rebuild. Default: no rebuild (the full layer keeps the box-height modes).
  Blocks: the pinned full layer and its publication (P23).
- **(b) Mixed volumes** (only if there are any): one entry listing them with their counts. Options: (a) leave as built
  (the default), (b) pin to hand, (c) pin to print.

**Check page** (`checkpage` checklist): instructions at the top (the question is "is this volume's text mainly
handwritten or printed?", with the label definitions above and one example of each); per volume the inventory, the
EAD title, and its 5 read scans as viewer links (`scanLine`) with the first 8 lines of each scan's largest region;
buttons *Handwritten / Printed / Mixed / Unsure* and a note; `CheckStore("as_phase12_15z_hand_print_v1")`; export
`check_verdicts.json`. **Blind:** no mode, value, verdict or pin on the page (before handing it over, grep the HTML
for `"mode"`, `value`, `verdict`, `pinned`, `box_height` and the agent's verdict labels outside the button texts). Item count 37; check the JS with `osascript -l JavaScript`.

## Do not touch

`data/text_layer_v4_full/`, every earlier dev layer (`15m-a` … `15x-a`, `15y-a`, `15y-b`), `data/hand_print_geom/`,
`reports/data/phase-10-*`, `reports/data/phase-12-*/{planning,review,step-15u,step-15w,step-15wr,step-15x,step-15y}/`
(15y's audit and run script only through their CLIs), `shared-data/` (the agent never writes there), the siblings.
Never `git commit`; `chmod g+w`; `.venv/bin/python`; no `rm -rf`, `nohup`, `&`; shared files only with Edit.

## When done

1. Report `### Step 15z: read the off-series modes and pin them (P21)` after § Step 15y: the list, the reads (counts,
   categories, quotes), the volume table, the pins, the before/after reads, the resolution table, conclusion;
   `F12-15z.n` rows (`draft`); `make reports` clean.
2. Changelog (Edit, under *Unreleased*): `### Step 15z: v4 mode pins`: "`sg_1.01.02_v4` `MODE_PINS_V4` gains <n> pins
   (<list> → `hand`; <list> → `print`) from read scans (`step-15z/verdicts.tsv`); behaviour change of the preset; the
   published v4 pilot is not rebuilt (P23). Affected sibling imports: none."
3. `PROGRESS.md` block (template fields as in 15y, title above); overwrite `STATUS.md`: 15z done, next = the user's UA (a),
   then an Opus check of the staged rebuild, which also writes the publication UA (P23: the full run replaces the pilot).
