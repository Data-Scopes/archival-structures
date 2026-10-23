# Disk-space audit: AS, LotA, NLP, republic, republic-segmentation, shared-data (2026-10-22)

Why: the data volume has 20 GiB free (98% used), not enough to stage the compressed PageXML for the
full `text_layer_v3` run (UA-015). This lists what could be removed, compressed or moved to an external
drive. Decision: **UA-016**. Nothing has been changed; every size below is `du -x` on disk (GiB),
measured 2026-10-22.

| project | size | largest parts |
|---|---|---|
| AS | 26.9 | `data/thumbs` 7.8, `data/text_layer_v2_dev` 7.6, `data/PageXML` 3.0, `.venv` 1.7 |
| LotA | 8.0 | `data/paragraphs` 2.0, `data/pagexml` 1.7, `data/scan_info` 0.9, `data/images` 0.8 |
| NLP | 30.3 | `output` 11.1 (models 6.2), `pagexml` 7.9, `resources/fasttext` 3.5, `scans` 3.5 |
| republic | 12.8 | `data/processed` 5.5 (DuckDB 4.3), `output/phase-02-8` 2.2, `resources/embeddings` 1.8 |
| republic-segmentation | 0.6 | `venv` 0.5; nothing to gain |
| shared-data | 37.0 | `voc_text_layer_v1` 13.6, `NL-HaNA_1.01.02` 13.1, `line_class_preprocessed_page_json` 9.9 |

Measured compression ratios: one SG hand inventory as flat PageXML (11182) is 182 MiB on disk and
44 MiB as `.tar.gz` (4.1×). NLP `output/table_region_features.csv` is 948 MiB, and 202 MiB gzipped (4.7×).
Images (`jpg`, `jp2`, `tif`), `.npy`, `.pt`, `.zip` and the `.gz` files gain little from compression.

## A. Redundant: remove (low risk), about 2.0 GiB

| path | GiB | evidence |
|---|---|---|
| republic `output/phase-02-8/step03c/run/` | 1.1 | All 638 files are byte-identical to shared-data `NL-HaNA_1.01.02/print_hand_alignment_v2/` (checked with `cmp`). Replace it with a symlink, as `step03/staging` already does. Caveat: a re-run of republic step 03c would then write into shared-data. |
| AS `docs/_build/` | 0.6 | Sphinx output, git-ignored, rebuilt by `make -C docs html`. |
| AS `venv/` | 0.3 | AS `CLAUDE.md` calls it stale (pagexml-tools 0.7.0); `.venv/` is the one in use. |

## B. Superseded: remove once the owning project agrees, about 5.4 GiB

| path | GiB | evidence |
|---|---|---|
| NLP `output/models/xenc_page/seed{0,1,2}/checkpoint.pt` | 4.2 | Resume state (model, optimiser and epoch; 1.5 GiB each). `best_model.pt` (0.5 GiB each) stays. Last written 2026-10-02. Needed only to resume that training. |
| AS `.venv-3.11/` | 0.8 | Untracked Python 3.11 venv. It is needed only while 3.11 compatibility is tested (AS keeps `requires-python >=3.11` until NLP is on 3.14). |
| LotA `.venv/` | 0.4 | LotA `CLAUDE.md` says to use `venv/` (0.6 GiB). |

## C. Compress, about 3.8 GiB net

| path | GiB now → after | evidence |
|---|---|---|
| AS `data/PageXML/NL-HaNA/NL-HaNA_1.01.02/` (the 8 pilot inventories) | 2.0 → 0 (+0.5 in shared-data) | UA-015 item 1 packs exactly these 8 as tars into shared-data. After that the flat dirs are a second copy. The phase-09 review built 11182 from the tar and got the same tables. The `15i-src` symlinks in the dev dir would dangle. |
| AS `data/PageXML/`, other archives (1.05.06 0.53, AsdSAA_5075 0.22, 2.10.50 0.12, AsnDA_0114.11 0.09, AsdSAA_89/5023 0.04) | 1.0 → about 0.25 | Flat XML, last written 2026-07. Unpack with `tar -xzf` when needed. |
| NLP `output/*.csv` over 20 MiB (13 files) | 2.0 → about 0.4 | Uncompressed feature tables. pandas reads `.csv.gz`, but NLP scripts name the `.csv` paths, so NLP has to update its paths. |

## D. Move to the external drive (inactive, re-downloadable or rebuildable), about 36.8 GiB

| path | GiB | last written | who reads it | notes |
|---|---|---|---|---|
| AS `data/text_layer_v2_dev/` except `15ir-a`, `15m-a`, `15m-v1check` | 6.8 | 2026-10-22 | phase reports (as evidence) | Keep `15m-a` (the v3 reference) and `15ir-a` (the phase-09 start); `plan/CONTEXT.md` names both. The others are phase-06–09 intermediates. Move rather than delete, because the reports cite them. |
| AS `data/thumbs/` | 7.8 | 2026-10-02 | `archival_structures/image/`, `datasets/images_transcriptions.py`, stream-analysis notebooks | No current phase uses it. It can be downloaded again (`notebooks/download_thumbnails.ipynb`). `thumbs/Old/` (1.5) looks superseded: `NL-AsdSAA_89_14` is now split into `NL-AsdSAA_89_14.*`, and the 0.73 zip duplicates the unpacked dir. |
| AS `notebooks/demo/ocr_results/`, `archival_structures/stream_analysis/data/images/` | 0.9 | 2026-06-12 | demo notebooks | The two dirs hold the same 7,428 file names with different bytes (one set is resized). Both are git-ignored. |
| AS `data/stream_analysis/` | 0.9 | 2026-07-02 | stream-analysis notebooks | JSON + `.npy` features. |
| NLP `pagexml/` | 7.9 | 2026-10-17 | 5+ scripts (e.g. `build_region_relations_dataset.py`, `run_ead_sample_predictions_vm.py`) | 504 VOC inventory zips: the source of shared-data `voc_text_layer_v1`. Needed only to rebuild it or run those scripts. |
| NLP `scans/` | 3.5 | 2026-10-13 | error-analysis dossiers | Page images of 23 inventories. |
| NLP `resources/fasttext/` | 3.5 | 2026-09-28 | `weak_pair_corpus.py`, `mine_page_dates.py`, `variant_enrichment_pass.py` | A trained model (ngram vectors 2.9). |
| NLP `output/embeddings/`, `output/attention_pooling_bags/` | 2.0 | 2026-10-21 / 10-01 | NLP experiments | Check with NLP before moving `embeddings` (written yesterday). |
| LotA `data/pagexml/NL-HtBHIC/` | 1.7 | 2022-12-10 | only LotA's phase-03 generality spot-check | Three zips, mode 600 (only the owner can read them). |
| republic `resources/embeddings/` | 1.8 | 2026-09-05 | republic text classification | Two fastText models (`min_count_5`, `min_count_100`, 0.9 each); possibly only one is used. |

## Keep

- **shared-data** (37.0): everything is already gzipped, and every catalogue row has a consumer. The
  one candidate is `print_hand_alignment_v1/` (0.9, superseded by v2), but LotA (Phase 9, 10.3) and
  republic still list it. Removing it is their call under the shared-data README rule.
- republic `data/processed/` (live DuckDB and parquet); LotA `data/paragraphs`, `images`, `scan_info`
  (written in October, in use); venvs in use; `.git` dirs.

## How much each choice frees

| choice | GiB |
|---|---|
| A | 2.0 |
| A + B | 7.4 |
| A + B + C | 11.2 |
| A + B + C + D (AS parts only: dev layers, thumbs, demo images, stream_analysis) | 27.6 |
| everything above | 48.0 |

## Commands (for UA-016; the user runs them)

Each block runs on its own. The move loops delete a source only after `rsync` succeeds. Set `EXT` to a
folder on your external drive first; the paths under it mirror the ones here (`rsync -R` keeps the part
after `/./`).

**A**, from any directory:

    cd /Users/Shared/claude_projects/data_scopes/republic/output/phase-02-8/step03c && rm -r run && ln -s /Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/print_hand_alignment_v2 run
    rm -r /Users/marijnkoolen/Code/Huygens/archival-structures/docs/_build /Users/marijnkoolen/Code/Huygens/archival-structures/venv

**B**, from any directory:

    rm /Users/Shared/claude_projects/data_scopes/nlp-start-page-detection/output/models/xenc_page/seed{0,1,2}/checkpoint.pt
    rm -r /Users/marijnkoolen/Code/Huygens/archival-structures/.venv-3.11 /Users/Shared/claude_projects/data_scopes/language-of-the-archive/.venv

**C, AS PageXML of the other archives**, from `/Users/marijnkoolen/Code/Huygens/archival-structures/data/PageXML`
(restore one with `tar -xzf <name>.tar.gz -C <its parent dir>`):

    for d in NL-HaNA/NL-HaNA_1.05.06 NL-HaNA/NL-HaNA_2.10.50 NL-AsdSAA/NL-AsdSAA_5075 NL-AsdSAA/NL-AsdSAA_89 NL-AsdSAA/NL-AsdSAA_5023 NL-AsnDA/NL-AsnDA_0114.11; do tar -czf "$d.tar.gz" -C "$(dirname $d)" "$(basename $d)" && rm -r "$d"; done

**C, the 8 SG pilot inventories**: only after UA-015 item 1 has written the 8 tars. Run from the AS root:

    for i in 3771 3772 3773 3821 3822 3823 11182 11281; do t=/Users/Shared/claude_projects/data_scopes/shared-data/NL-HaNA_1.01.02/pagexml/NL-HaNA_1.01.02_${i}-pagexml.tar.gz; [ $(tar -tzf $t | grep -c '\.xml$') -eq $(find data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_${i} -name '*.xml' | wc -l) ] && rm -r data/PageXML/NL-HaNA/NL-HaNA_1.01.02/NL-HaNA_1.01.02_${i} || echo "kept $i: count differs"; done

**D, AS parts**:

    EXT=<folder on your external drive>
    cd /Users/marijnkoolen/Code/Huygens/archival-structures
    for p in data/thumbs data/stream_analysis notebooks/demo/ocr_results archival_structures/stream_analysis/data/images $(ls -d data/text_layer_v2_dev/* | grep -v -E '/(15ir-a|15m-a|15m-v1check)$'); do rsync -aR "/Users/marijnkoolen/Code/Huygens/./archival-structures/$p" "$EXT/" && rm -r "$p"; done

**D, sibling parts** (only for the rows the sibling's owner agrees to):

    EXT=<folder on your external drive>
    cd /Users/Shared/claude_projects/data_scopes
    for p in nlp-start-page-detection/pagexml nlp-start-page-detection/scans nlp-start-page-detection/resources/fasttext nlp-start-page-detection/output/attention_pooling_bags language-of-the-archive/data/pagexml/NL-HtBHIC; do rsync -aR "/Users/Shared/claude_projects/data_scopes/./$p" "$EXT/" && rm -r "$p"; done

## Decision (b), 2026-10-22: what moves to the external drive

The user chose UA-016 (b) in chat. The paths below are relative to the AS root; the **D, AS parts** block moves
exactly these. Keep `data/text_layer_v2_dev/15ir-a`, `15m-a` and `15m-v1check`. The `15i-src` folder holds only symlinks.

        8033 MiB  data/thumbs
         958 MiB  data/stream_analysis
         531 MiB  notebooks/demo/ocr_results
         434 MiB  archival_structures/stream_analysis/data/images
           0 MiB  data/text_layer_v2_dev/06R-15f
         146 MiB  data/text_layer_v2_dev/06R-a
           0 MiB  data/text_layer_v2_dev/06R-ab
          59 MiB  data/text_layer_v2_dev/06R-v1check
          59 MiB  data/text_layer_v2_dev/09R-v1check
         143 MiB  data/text_layer_v2_dev/15d-a
           0 MiB  data/text_layer_v2_dev/15d-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15d-v1check
          33 MiB  data/text_layer_v2_dev/15e-a
           0 MiB  data/text_layer_v2_dev/15e-ab
          13 MiB  data/text_layer_v2_dev/15e-b
           0 MiB  data/text_layer_v2_dev/15e-protected-after.txt
           0 MiB  data/text_layer_v2_dev/15e-protected-before.txt
           0 MiB  data/text_layer_v2_dev/15e-rates
          59 MiB  data/text_layer_v2_dev/15e-v1check
          62 MiB  data/text_layer_v2_dev/15f-a
          59 MiB  data/text_layer_v2_dev/15f-v1check
         196 MiB  data/text_layer_v2_dev/15fp-a
           6 MiB  data/text_layer_v2_dev/15fp-protected-after.txt
           6 MiB  data/text_layer_v2_dev/15fp-protected-before.txt
         197 MiB  data/text_layer_v2_dev/15fq-a
         197 MiB  data/text_layer_v2_dev/15fq-b
         197 MiB  data/text_layer_v2_dev/15fq-c
           0 MiB  data/text_layer_v2_dev/15fq-ea
         196 MiB  data/text_layer_v2_dev/15fq-legacy
           6 MiB  data/text_layer_v2_dev/15fq-protected-after.txt
           6 MiB  data/text_layer_v2_dev/15fq-protected-before.txt
          44 MiB  data/text_layer_v2_dev/15fq2-noguard
           6 MiB  data/text_layer_v2_dev/15fq2-protected-after.txt
           6 MiB  data/text_layer_v2_dev/15fq2-protected-before.txt
          33 MiB  data/text_layer_v2_dev/15fr-a
           0 MiB  data/text_layer_v2_dev/15fr-ea.txt
           0 MiB  data/text_layer_v2_dev/15fr-ea15fr.txt
           0 MiB  data/text_layer_v2_dev/15fr-protected-after.txt
           0 MiB  data/text_layer_v2_dev/15fr-protected-before.txt
           0 MiB  data/text_layer_v2_dev/15fr-rates
         255 MiB  data/text_layer_v2_dev/15g-a
         255 MiB  data/text_layer_v2_dev/15g-b
         254 MiB  data/text_layer_v2_dev/15g-c
           6 MiB  data/text_layer_v2_dev/15g-protected-after.txt
           6 MiB  data/text_layer_v2_dev/15g-protected-before.txt
         255 MiB  data/text_layer_v2_dev/15g2-all
           6 MiB  data/text_layer_v2_dev/15g2-protected-after.txt
           6 MiB  data/text_layer_v2_dev/15g2-protected-before.txt
         317 MiB  data/text_layer_v2_dev/15h-a
          12 MiB  data/text_layer_v2_dev/15h-protected-after.txt
          12 MiB  data/text_layer_v2_dev/15h-protected-before.txt
         389 MiB  data/text_layer_v2_dev/15hj-a
         389 MiB  data/text_layer_v2_dev/15hj-b
          13 MiB  data/text_layer_v2_dev/15hj-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15hj-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15hj-v1check
          13 MiB  data/text_layer_v2_dev/15hj2-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15hj2-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15hj2-v1check
         389 MiB  data/text_layer_v2_dev/15i-a
         389 MiB  data/text_layer_v2_dev/15i-b
          13 MiB  data/text_layer_v2_dev/15i-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15i-protected-before.txt
           0 MiB  data/text_layer_v2_dev/15i-src
          13 MiB  data/text_layer_v2_dev/15i2-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15i2-protected-before.txt
          13 MiB  data/text_layer_v2_dev/15ir-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15ir-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15ir-v1check
         388 MiB  data/text_layer_v2_dev/15j-a
          13 MiB  data/text_layer_v2_dev/15j-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15j-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15j-v1check
         388 MiB  data/text_layer_v2_dev/15k-a
         388 MiB  data/text_layer_v2_dev/15k-a-first
          13 MiB  data/text_layer_v2_dev/15k-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15k-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15k-v1check
         388 MiB  data/text_layer_v2_dev/15l-a
          13 MiB  data/text_layer_v2_dev/15l-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15l-protected-before.txt
          59 MiB  data/text_layer_v2_dev/15l-v1check
          13 MiB  data/text_layer_v2_dev/15m-protected-after.txt
          13 MiB  data/text_layer_v2_dev/15m-protected-before.txt
          72 MiB  data/text_layer_v2_dev/15m-smoke
        16.5 GiB  total, 85 paths

## Outcome (2026-10-22)

- The user moved the AS part of D to the drive and ran A and B. A session packed C (the 6 other archives' PageXML as
  `.tar.gz`, with file counts checked). Free space went from 20 to about 46 GiB.
- The C block for the 8 SG pilot inventories is dropped: the full run (UA-015, `scripts/run_text_layer_v3_full.py`)
  reads them in place from `data/PageXML/NL-HaNA/NL-HaNA_1.01.02`.
- Side effect: 11 tests read this local data, 9 from `data/thumbs` and 2 from the packed PageXML of 2.10.50, 1.05.06,
  AsdSAA_89 and AsnDA. They now skip with a pointer here. To run them again, put `data/thumbs` back, or unpack, from
  `data/PageXML/<institute>/`: `tar -xzf <archive>.tar.gz`.
