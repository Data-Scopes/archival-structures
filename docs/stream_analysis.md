# Stream analysis: embeddings, clustering, and active-learning ground truth

{mod}`archival_structures.stream_analysis` is a separate concern from the PageXML/image
pipeline covered elsewhere in these docs: rather than analysing one inventory number's already-
transcribed scans, it works on a plain directory of document images (no PageXML required) to
help triage and build ground truth for a large, heterogeneous stream of documents -- "what kinds
of documents are even in here, and which ones should I look at first?" {class}`~archival_structures.stream_analysis.AnalysisConfig`
is the single configuration object threaded through both parts below (build it directly in
Python, or via `AnalysisConfig.from_yaml`/`.from_args`).

```{note}
Don't confuse this package with {mod}`archival_structures.analysis.stream_analysis` (singular
"analysis"), a small, unrelated module that does whole-page luminosity categorisation
(background/inverted/quarantined flags) for ink-colour quality control -- see
[Ink colour, multi-colour text, and missing transcriptions](colour_and_ink.md). The similar
names are coincidental.
```

## Part 1: Overview (`archival_structures.stream_analysis.overview`)

{func}`~archival_structures.stream_analysis.run_overview` runs the full pipeline in one call:

1. **Embeddings** ({mod}`~archival_structures.stream_analysis.overview.embeddings`) -- visual
   embeddings via DINOv2 (recommended -- no text supervision needed) or CLIP, cached to disk.
2. **Layout features** ({mod}`~archival_structures.stream_analysis.overview.layout_analysis`) --
   structural/statistical features that work at thumbnail resolution without OCR (ink density,
   estimated line count, ruled lines, colour profile, ...).
3. **Clustering** ({mod}`~archival_structures.stream_analysis.overview.clustering`) -- UMAP
   dimensionality reduction + HDBSCAN, as two independently cacheable steps so HDBSCAN
   parameters can be swept without recomputing the expensive UMAP fit.
4. **VLM tagging** ({mod}`~archival_structures.stream_analysis.overview.vlm_tagging`, optional)
   -- sends a random sample to the Anthropic API for a structured JSON description, to help
   derive a label vocabulary before annotating by hand. Requires the `ANTHROPIC_API_KEY`
   environment variable; every other part of this subsystem works without it.
5. **Visualisations** ({mod}`~archival_structures.stream_analysis.overview.visualization`) --
   UMAP scatter plot, per-cluster image grids, layout feature distributions, VLM tag summaries.

See the [overview demo](notebooks/stream-analysis-overview-demo) notebook for a full run.

### Subsequence detection (`archival_structures.stream_analysis.overview.subsequence_detection`)

Once DINOv2 embeddings have been extracted for a heterogeneous inventory, the
adjacent cosine similarity between consecutive scans is a strong signal for
visual transitions between document types.  This submodule provides:

- `compute_adjacent_similarities` -- cosine similarity between each consecutive embedding pair.
- `suggest_threshold` -- data-adaptive threshold (percentile of the similarity distribution).
- `detect_boundaries_threshold` -- groups below-threshold positions and picks the local
  minimum from each group as the precise boundary.
- `detect_boundaries_changepoint` -- alternative using *ruptures* Pelt change-point detection
  (optional dependency).
- `score_all_segments` / `detect_book_like_subsequences` -- score each resulting segment for
  visual homogeneity (`mean_similarity`, `min_similarity`, `first_last_similarity`) and
  optionally for HDBSCAN cluster entropy and opening consistency; flags segments as
  `is_book_like` when they exceed configurable thresholds.

See the [subsequence detection demo](notebooks/subsequence-detection-demo) notebook for a
worked example on `NL-AsdSAA_89_3.1` (1035 scans, mixed document types), which validates
the detection against a known book-like run (scans 8–18) and identifies several other
book-like candidates in the same inventory.

## Part 2: Ground truth (`archival_structures.stream_analysis.groundtruth`)

Builds on Part 1's cached outputs:

- **Stratified sampling** ({mod}`~archival_structures.stream_analysis.groundtruth.stratified_sampling`)
  -- samples a fixed number of images per cluster (including outliers sampled separately, since
  they're often the most unusual/informative documents), so rare document types aren't drowned
  out by common ones.
- **Label Studio export** ({mod}`~archival_structures.stream_analysis.groundtruth.label_studio_export`)
  -- exports the sample to Label Studio's import format, with VLM tags embedded as
  pre-annotations if available.
- **Active learning** ({mod}`~archival_structures.stream_analysis.groundtruth.active_learning`)
  -- once you've annotated an initial batch, trains a lightweight classifier on the embeddings,
  ranks unannotated images by prediction uncertainty, and returns a prioritised list of what to
  annotate next. Repeats as you annotate more.
- **Interactive bulk tagging** ({mod}`~archival_structures.stream_analysis.groundtruth.interactive_annotation`)
  -- a paginated, checkbox-selectable thumbnail grid for tagging a cluster at once, with a
  free-text label box. (For tagging into this package's own `ScanAnnotation` ground truth with
  the structured `namespace:type(:subtype)?(#N)?` vocabulary instead of free text, see
  {mod}`archival_structures.datasets.bulk_tagging`, which this module doesn't depend on, by
  design -- it takes a plain list of image paths regardless of where they came from.)

See the [ground truth demo](notebooks/stream-analysis-groundtruth-demo) notebook -- note that
its active-learning step needs real manual annotation in Label Studio first, so it reports what's
missing rather than failing if you run it without having annotated anything yet.

## CLI

`scripts/analyse_documents.py` drives the same pipeline from the command line, for runs you'd
rather not do in a notebook (e.g. on a remote machine).

```{eval-rst}
.. automodule:: archival_structures.stream_analysis.config
   :members:

.. automodule:: archival_structures.stream_analysis.overview.embeddings
   :members:

.. automodule:: archival_structures.stream_analysis.overview.layout_analysis
   :members:

.. automodule:: archival_structures.stream_analysis.overview.clustering
   :members:

.. automodule:: archival_structures.stream_analysis.overview.vlm_tagging
   :members:

.. automodule:: archival_structures.stream_analysis.overview.visualization
   :members:

.. automodule:: archival_structures.stream_analysis.overview.pipeline
   :members:

.. automodule:: archival_structures.stream_analysis.overview.subsequence_detection
   :members:

.. automodule:: archival_structures.stream_analysis.groundtruth.stratified_sampling
   :members:

.. automodule:: archival_structures.stream_analysis.groundtruth.label_studio_export
   :members:

.. automodule:: archival_structures.stream_analysis.groundtruth.active_learning
   :members:

.. automodule:: archival_structures.stream_analysis.groundtruth.pipeline
   :members:

.. automodule:: archival_structures.stream_analysis.groundtruth.interactive_annotation
   :members:

.. automodule:: archival_structures.stream_analysis.utils.image_loader
   :members:
```
