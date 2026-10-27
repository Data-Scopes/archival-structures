# Leakage and splits

Standing methodology for any experiment that scores a page-stream method (document starts,
boundaries, continuation) against ground truth. The rules come from the NLP start-page project
(`nlp-start-page-detection`), where they were learned on VOC inventories. They are stated here for
any page stream.

## Why

A good number is checked for leakage before it is believed. In NLP, a continuation classifier
reached F1 0.822 in report/20. The classifier's own training split did not nest inside the
downstream evaluation split. With a nested split the same method scored F1 0.635. The gain was
leakage, not signal.

## Leakage channels

1. **Models fit on this corpus whose training split does not nest in the downstream split.**
   This covers continuation classifiers, cross-encoders and metric-learned encoders. Every page a
   component saw in training must be a training page downstream too. Assert the nesting in code with
   `check_nested_split(component_split, downstream_split, *, component_train=("train",),
   downstream_train=("train",))`.
2. **Features over physically adjacent pages at a chunk edge.** A page next to a chunk border sees
   a neighbour from another partition. The residual is known and bounded. Keep it bounded: score
   only the pages of the partition, not the context pages (`check_context_not_scored`), and do not
   add features that reach further than one page.
3. **"Pages since last start" or "distance to previous start" computed from ground truth.** This is
   label leakage. Legitimate versions are positional (page index in the volume) or derived from an
   unsupervised segmentation. `segment_dp` (see [Stream analysis](../stream_analysis.md)) is one
   such source of a start list. Its two constants (the prior mean segment length and the length
   penalty weight) were chosen from corpus label statistics. Re-derive them from train rows only
   before a headline number is quoted.
4. **Normalisation statistics fitted across train and test using labels.** Per-inventory centring
   is fine, because it uses no labels. Anything label-dependent is not. Label-free per-inventory
   statistics include the stable margins of `text_extent` (`compute_corpus_text_extents`).
5. **Thresholds and region types chosen after reading the errors.** These are post hoc and need a
   fresh test on data that was not used to choose them. NLP's body-region variant of the
   `early_end` flag was chosen this way. Phase 11 step 18a reproduced it exactly (precision 0.1705,
   recall 0.3416), which confirms the port, not the variant. A threshold on the bottom gap cannot
   replace a band either, since the END rate is not monotone in the gap (finding F11-18a.3 in
   `reports/phase-11-text-extent-and-nlp-ports.md`, outside this documentation tree).

## Splits and protocol

All functions are in `archival_structures.stream_analysis.evaluation`.

- **Document-atomic chunks.** The primary split cuts long sequences into chunks that never cut a
  document: `splits.build_long_sequence_split`. Check with
  `splits.check_no_document_crosses_partitions`, `splits.check_no_duplicate_page_ids` and
  `splits.check_split_coverage`.
- **Inventory held out.** The secondary split holds out whole inventories:
  `splits.build_inventory_held_out`.
- **Test discipline.** Evaluate on `dev`. Touch `test` at most once per phase, and never to choose
  between variants.
- **Seeds and bootstrap.** Use at least 3 seeds. Give a bootstrap confidence interval over
  inventories, not over pages: `aggregate.bootstrap_ci`. Report per-inventory scores with
  `aggregate.per_inventory`, the full metric set with `aggregate.evaluate_all`, and the predicted
  against true start rate with `aggregate.start_rate_calibration`.
- **Trivial baselines as the floor.** `baselines.baseline_suite`, `baselines.baseline_floors` and
  `baselines.majority`. In NLP the majority baseline beat several real models on Pk and
  WindowDiff, so a method has to clear the floor before it counts.
- **The `k` convention.** Pk and WindowDiff depend on `k`. `segmentation.default_k(true_labels,
  convention="gaps")` is AS's default. NLP's numbers use `convention="pages"`, so pass that when
  comparing with NLP.

## Granularity of the target

A method validated on volume sections is not evidence for documents. A volume section is a run of
pages coarser than a document, for example a session block. DINOv2 subsequence detection, `segment_dp`
and TextTiling find such units. NLP found DP and TextTiling below the majority baseline for VOC
document starts. See [The granularity hypothesis](document_segmentation.md#the-granularity-hypothesis).
