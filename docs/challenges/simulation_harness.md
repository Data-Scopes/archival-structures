# Simulation harness: corrupting page streams at controlled rates

```{note}
Status: **implemented**. All six stages below -- canonical stream extraction, the four
corruption operators, `CorruptedStream`, evaluation metrics (Framing A precision/recall/F1 +
WindowDiff, Framing B precision/recall/F1, framing-independent ARI), and the rate-sweep pipeline
-- are implemented and unit-tested in `archival_structures.stream_analysis.corruption`, plus a
CLI wrapper at `scripts/simulate_corruption.py`. Run end to end against the real
`NL-HaNA_1.01.02` collection (see "Status" below for what that run found). Part of the
[challenges catalog](index.md).
```

Goal: turn the four corruption cases in [Page order problems](page_order_problems.md) into
functions that take a real, ground-truth **canonical** page stream and produce a **corrupted**
observed stream at a controlled rate, with the alignment between the two kept explicit -- so
detection methods (Framing A or Framing B, see
[Document segmentation](document_segmentation.md#whole-page-segmentation)) can be trained and
scored against known-correct answers, across more than one real collection.

## What ground-truth is actually available today

Before designing the corruption operators, it's worth being precise about what canonical
document streams exist in this repo right now, since that determines what the harness can
actually run against on day one.

- **674 real {class}`~archival_structures.annotation.ground_truth.InventoryGroundTruth` YAML
  files** under `data/ground_truth/`, across three collections: `NL-HaNA_1.01.02` (633 files),
  `NL-HaNA_1.10.94` (39 files), `KB_series` (2 files) -- see
  [Ground truth format](../ground_truth_format.md). Every file checked populates only the
  `section` layer (two collections also populate `title_page`). Sections are coarse structural
  blocks -- `index_page`, `resolution_page`, occasionally `non_sg_resolution_page` -- already
  contiguous and in order end to end by construction (`start_page`/`end_page` cover the whole
  volume with no gaps). The finer `session`/`resolution` layers the *format* already supports
  (see the ground-truth format's layer reference) aren't populated in any file checked.
  5 of the 633 `NL-HaNA_1.01.02` files (`3203`, `3865`-`3868`) have no layers populated at all
  yet -- `extract_canonical_stream` raises `KeyError` on these, by design (see its docstring),
  rather than silently returning an empty stream.
- **Section counts confirm the "limited room for Case 2/4" caveat above is not hypothetical**:
  across all 633 `NL-HaNA_1.01.02` files, 294 (46%) have exactly **one** section -- Case 2/4
  are structurally impossible there, at any rate, since both operators no-op below two documents
  -- 238 (38%) have two, 51 have three, 39 have four, 5 have five, and one has seven. Section
  length itself is not the limiting factor (median 124 pages, up to 1311), which is precisely
  what caused the bug below.
- One of those files already contains a real instance of the sub-page boundary case in
  [Document segmentation](document_segmentation.md#sub-page-segmentation), not just a
  hypothetical one: in `data/ground_truth/NL-HaNA_1.01.02/3771.yaml`, scan
  `NL-HaNA_1.01.02_3771_0024.jpg` is split between `sec_001` (`page_side: verso`) and `sec_002`
  (`page_side: recto`) -- the index ends and the resolution body starts on the two halves of the
  same scan.
- The collections whose documents are short and numerous -- `NL-AsnDA_0114.11_1` (notary deeds)
  and `NL-AsdSAA_89_3.1` (mixed correspondence), the most realistic test bed for
  [page order problems](page_order_problems.md), since a 600-page resolution book with 3 sections
  gives almost no room for document-level swaps or foreign insertions -- have 3057
  {class}`~archival_structures.datasets.annotations.ScanAnnotation` region-tag files under
  `data/annotations/`, but **no document-boundary or document-id ground truth at all** yet.
  `doctype:`/`position:` tags ([Vocabulary](../vocabulary.md)) are defined but unused on every
  file checked.

**Net effect:** a real, immediately usable canonical stream exists today only at *section*
granularity, on the resolution-book collections. The more interesting case -- individual
deeds/letters/resolutions as documents -- has no ground truth anywhere yet. The design below
treats "document" as *whichever named, ordered layer of elements the ground truth happens to
provide*, rather than hardcoding section-level granularity, so it keeps working once finer
ground truth exists, but the first real runs will necessarily be on sections.

## Pipeline stages

### 1. Canonical stream extraction

```
extract_canonical_stream(gt: InventoryGroundTruth, layer: str = "section") -> List[StreamUnit]
```

Flattens `gt.layers[layer]` (a list of
{class}`~archival_structures.annotation.ground_truth.AnnotationElement`, already in canonical
order) into a flat sequence, where each `StreamUnit` carries `(scan_id, page_side_or_bbox,
document_id)` -- `document_id` being the owning element's `id`. This is exactly `C` from
[Page order problems](page_order_problems.md#setup-and-notation), with `document_id` playing the
role of which `D_k` each unit belongs to. A boundary scan like `..._0024.jpg` above naturally
produces two distinct units (`page_side: verso` ending one element, `page_side: recto` starting
the next), not a collision -- the two sides carry different `page_side` values. Repeated spans
that *do* reference the exact same `(scan_id, page_side)` within one element's own span list
collapse to a single unit, as a defensive guard against duplicate annotation entries.

Left as an explicit `layer` parameter rather than hardcoded to `section`, so the same function
works today (`layer="section"`) and later once `session`/`resolution` are populated, without
changing any downstream code.

### 2. Corruption operators

Four pure functions matching the [page order problems](page_order_problems.md) cases 1:1, each
`(stream, provenance, rate, rng) -> (observed_stream, provenance)` -- taking the *previous*
provenance list as well as the stream, so operators compose (Case 3 is literally
`intra_document_swap` followed by `foreign_document_insertion`, each threading the other's
provenance through) without losing track of each unit's original canonical position:

```python
def intra_document_swap(stream, provenance, rate, rng) -> tuple[list, list]: ...
def foreign_document_insertion(stream, provenance, rate, rng) -> tuple[list, list]: ...
def swap_and_insertion(stream, provenance, swap_rate, insertion_rate, rng) -> tuple[list, list]: ...
def document_level_swap(stream, provenance, rate, rng) -> tuple[list, list]: ...
```

Each `StreamUnit` already carries its own true `document_id` (see stage 1) -- reordering never
changes that field, only the unit's position -- so `provenance` only needs to track, per
observed position, the unit's *original* index in the very first canonical stream passed into
the chain, plus which operator(s) touched it: `ProvenanceEntry(canonical_index, events)`. A
single seeded `random.Random` instance gets threaded through explicitly rather than relying on
global random state, consistent with how the rest of the package avoids implicit global config
(cf. `AnalysisConfig` in [Stream analysis](../stream_analysis.md)).

Degenerate inputs are explicit no-ops rather than silent failures: a single-page document under
`intra_document_swap`, or fewer than two documents under `foreign_document_insertion`/
`document_level_swap`, simply pass the stream through unchanged.

`document_level_swap`'s default corruption model is a **bounded, local perturbation** -- each
*adjacent* pair of documents in the block order independently swaps with probability `rate`,
rather than drawing an arbitrary global permutation over all `m` documents. This is a design
choice made without separately confirming it: one document ending up next to where it should be
(misfiled relative to a neighbour) is a far more physically plausible corruption for numbered
deeds or dated sessions than an arbitrary reshuffling of the whole document order, but it's worth
revisiting if a global-permutation mode turns out to be needed too.

### 3. Rate parameterisation

Two different senses of "rate" are both defensible and behave differently:

- **Per-document probability** -- e.g. "each document has a 10% chance of getting one
  intra-document swap applied." Simple, interpretable, and what most page-stream-segmentation
  corruption literature uses.
- **Corpus-level target fraction** -- e.g. "5 foreign insertions per 100 pages," which is what
  [Page order problems](page_order_problems.md#open-question-simulating-these-at-controlled-rates)
  itself suggested. Better for comparing corruption *severity* across collections with very
  different document-count/length distributions (a 600-page book with 3 sections vs. a
  300-scan deed folder with 150 documents).

**Decided:** per-document probability is implemented as the primary parameter (simplest to
reason about and to compose across operators). The corpus-level target-fraction wrapper (solving
for the per-document probability numerically to hit a target fraction) is not implemented yet --
tracked as follow-up work once the primary parameter is validated against real data.

### 4. Output format

**Decided:** a new sibling dataclass, `CorruptedStream`
({mod}`archival_structures.stream_analysis.corruption.schema`) -- `inventory_id`, `layer`,
`case`, `params`, `seed`, `observed` (the corrupted stream), and `provenance`, independent of the
`InventoryGroundTruth` schema. This keeps the corruption-simulation concern fully decoupled from
the human-annotation format it's derived from, which matters once corrupted variants get
generated in bulk (many corrupted copies per one real ground-truth file, one per
case/rate/seed combination) rather than one-to-one.

### 5. Evaluation metrics

Tied to the two segmentation framings in
[Document segmentation](document_segmentation.md#whole-page-segmentation), implemented in
`metrics.py`:

- **Framing A** (start-of-document + cut reduction): `score_start_classification`
  (precision/recall/F1) on the per-position `b_i` decision, ground truth from
  `observed_start_labels` -- correctly follows the *canonical* start page even when the
  corruption moved it off the front of its own run (the `pi_b` case in
  [Page order problems](page_order_problems.md#case-1-intra-document-page-swap)). Precision/
  recall degrades sharply under [Case 2/3](page_order_problems.md#case-2-foreign-document-insertion-interleaving)
  in a way that penalises a near-miss boundary as harshly as a completely wrong one, so
  `window_diff` ([Pevzner & Hearst, 2002](https://aclanthology.org/J02-1004/), implemented
  directly rather than pulled in as a dependency) is also provided as a boundary-tolerant
  alternative. Pk (the other standard segmentation metric) was scoped but not implemented -- one
  boundary-tolerant metric covers the need for now.
- **Framing B** (pairwise same-document adjacency): `score_same_document_pairs`
  (precision/recall/F1) on the per-pair `l_i` decision, ground truth from
  `true_same_document_pairs` -- needs only the observed stream, no canonical/provenance, since
  `document_id` travels with each unit regardless of corruption.
- **Framing-independent**: `grouping_ari` (`sklearn.metrics.adjusted_rand_score`, already a core
  dependency) between a predicted per-position document grouping and the true one, ignoring
  stream order and label naming entirely. `pairs_to_groups` converts a Framing-B prediction into
  a grouping so it can be scored this way too. Validated directly against the worked
  [Case 2 example](page_order_problems.md#case-2-foreign-document-insertion-interleaving): a
  Framing-B prediction that gets every single pairwise label exactly right still scores ARI < 1.0,
  because the pairwise-correct grouping still splits one document's pages into two separate
  groups -- exactly the failure mode that example describes.

### 6. Code location

Mirrors the existing `archival_structures.stream_analysis` split
(`overview`/`sequence_analysis`/`groundtruth`, see [Stream analysis](../stream_analysis.md)):
`archival_structures.stream_analysis.corruption` --

```
corruption/
  canonical_stream.py   # stage 1 -- implemented
  operators.py           # stage 2, the four corruption functions -- implemented
  schema.py               # stage 4, CorruptedStream + YAML read/write -- implemented
  metrics.py               # stage 5, Framing A / Framing B / ARI -- implemented
  simulate.py               # stage 6, CLI/pipeline: sweep rates x collections -- implemented
```

`scripts/simulate_corruption.py` is the CLI wrapper around `simulate.py`'s `simulate_collection`.

## Status

Decisions 1-4 below were confirmed; every module in the code-location table above is implemented
and unit-tested (including against the real `NL-HaNA_1.01.02/3771.yaml` file referenced above),
and `scripts/simulate_corruption.py` has been run end to end against real
`NL-HaNA_1.01.02` ground-truth files, producing real `CorruptedStream` YAML output scored with
`metrics.py` against a trivial baseline and an oracle sanity check (both behaved exactly as
designed: the oracle scores perfectly on all three metrics; a "predict nothing ever changes"
baseline scores near-perfectly on Framing B under `document_level_swap`, since that corruption
barely touches pairwise adjacency, but poorly on Framing A recall and on grouping ARI once
sections actually got interleaved).

**Validated against the full `NL-HaNA_1.01.02` collection** (628 of 633 files -- the 5 with no
layers excluded): all three non-composed operators run cleanly across the whole collection with
zero conservation violations (every canonical unit still appears exactly once; document
membership unchanged), at `rate=0.5`. This run also caught and fixed a real bug:
`foreign_document_insertion`'s original target/donor exclusion rule (a document could never be
both in the same call) excluded *every* committed target from the donor pool outright, which
left zero eligible donors whenever every document in a stream happened to qualify as a target
simultaneously -- the common case here, since section lengths are almost always well above the
2-page target-eligibility threshold. On 2-section inventories (38% of the collection) this made
the operator a near-total no-op at moderate-to-high rates. Fixed by assigning target/donor roles
sequentially and locking in both sides of each pairing as it's made, so mutually-eligible targets
can still pair up with each other (one becomes the other's donor) rather than only with a
document that happened not to also roll as a target. A regression test
(`test_pairs_up_targets_even_when_every_document_is_eligible`) covers this directly.

1. **Which ground truth to build against first**: **section-level, now** -- the 674 real files
   under `data/ground_truth/`. Coarser than the deed/letter case the docs' worked examples use,
   but real and immediately available; `extract_canonical_stream`'s `layer` parameter keeps this
   swappable once finer ground truth exists.
2. **Rate semantics**: **both** -- per-document probability implemented now; the corpus-level
   target-fraction wrapper is follow-up work.
3. **Evaluation metric**: not yet locked in -- `metrics.py` isn't built yet, so this is still open
   when that stage starts.
4. **Output format**: **a separate `CorruptedStream` schema**, decoupled from
   `InventoryGroundTruth`.
