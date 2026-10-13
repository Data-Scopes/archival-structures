# Document segmentation

```{note}
Status: **stub**. Problem framing and formal definitions only -- no detection method or
evaluation code yet. Part of the [challenges catalog](index.md).
```

## Problem statement

Given a [page stream](page_stream_terminology.md) `P = (p_1, p_2, ..., p_n)`, *document
segmentation* -- also called *document boundary detection* in the document analysis/recognition
literature, and *page stream segmentation* in some shared-task naming -- recovers the
[document stream](page_stream_terminology.md#the-page-and-document-streams) it encodes: which
pages belong together, and where one document ends and the next begins.

Two genuinely different assumptions get made about what a "document" is allowed to look like,
and it matters which one a given method (or a given piece of ground truth) assumes.

## Whole-page segmentation

The common simplifying assumption: every document is a union of whole pages, and documents
*partition* the page stream with no gaps -- each page belongs to exactly one document, and one
document's pages are contiguous in `P`. Even under this shared assumption, the task can be
*operationalised* in more than one way -- and, as
[Page order problems](page_order_problems.md#case-1-intra-document-page-swap) shows, the choice
matters once the page stream isn't perfectly ordered.

### Framing A: start-of-document classification

The framing most commonly used in practice: reduce segmentation to a single per-page binary
decision, is this page the first page of a new document?

```
b_i = 1  if p_i is the first page of a document
b_i = 0  otherwise
b_1 = 1  (the stream always starts with some document's first page)
```

Predictions are then turned into a document stream by a **cut reduction**: split `P` into runs
between consecutive `1`s in `B = (b_1, ..., b_n)`:

```
d_k = (p_{s_k}, ..., p_{s_{k+1} - 1})
```

where `s_1, s_2, ..., s_m` are the positions where `b_i = 1`, in order, and `s_{m+1} := n + 1`.

This reduction implicitly assumes that a document's true start page is always the *first* page of
that document's own run of positions in the observed stream. That assumption is exactly what an
intra-document page swap can violate -- see
[Page order problems](page_order_problems.md#case-1-intra-document-page-swap) for a worked
example where a perfectly accurate start-page classifier still yields a wrong document stream,
because the cut reduction itself can't express "this document's start page isn't first".

### Framing B: pairwise same-document adjacency

An alternative that sidesteps the cut reduction's assumption: for every adjacent pair of pages in
the stream, decide whether they belong to the same document.

```
l_i = 1  if p_i and p_{i+1} belong to the same document   (i = 1, ..., n-1)
l_i = 0  otherwise
```

The document stream is recovered as the maximal runs of consecutive `l_i = 1` pairs (equivalently,
cut between `p_i` and `p_{i+1}` wherever `l_i = 0`). This framing never has to identify *which*
page is the start -- only whether two neighbours belong together -- which is what makes it robust
to some corruptions Framing A isn't; see
[Page order problems](page_order_problems.md#case-1-intra-document-page-swap).

### Framing A is the default assumption elsewhere in this doc

Both regimes below (whole-page vs. sub-page) are stated in terms of Framing A's `b_i`/cut-reduction
vocabulary, since that's the standard framing in most document-stream-segmentation work -- but the
distinction above is worth keeping in mind whenever a corrupted or only-partially-ordered page
stream is in play, not just a cleanly-ordered one. It says nothing, either way, about how to
handle a page that resumes a document already seen earlier in the stream -- see
[Page order problems](page_order_problems.md#case-2-foreign-document-insertion-interleaving).

## Sub-page segmentation

Documents need not start or end on a page boundary. A document can, for example, start halfway
down the second text column of page 8 and end after ten lines into the first column of page 11 --
sharing both its start page and its end page with other documents.

To model this, refine the page stream into a finer content-unit stream
`U = (u_1, u_2, ..., u_N)` -- e.g. text lines or text regions in reading order -- where each unit
`u_j` has a page and a within-page position (`page(u_j) in {1, ..., n}`), and units are ordered
first by page, then by reading order within the page. A document is then a contiguous span of
this finer stream rather than of `P` directly:

```
d_k = (u_{a_k}, ..., u_{b_k})
a_1 = 1,  b_m = N,  a_{k+1} = b_k + 1
```

Whole-page segmentation is the special case where every `a_k` and `b_k + 1` happens to coincide
with a page boundary in `U`.

## Terminology used here

"Document boundary detection" and "document segmentation" both refer to the whole-page,
first-page-classification problem above unless stated otherwise -- the two literatures (born-
digital/scanned-office documents vs. scholarly/archival document analysis, respectively) use
different names for the same task. This repo's docs default to **document segmentation** for the
general problem, and reserve **boundary detection** for the specific per-page `b_i` decision.

## Why the ordering assumption matters

Both regimes above assume the page stream `P` is a faithful, in-order rendering of document
order. [Page order problems](page_order_problems.md) catalogs the ways that assumption fails in
real digitised material -- swapped pages, pages from another document interleaved in, and whole
documents out of their expected order -- which a robust segmentation method needs to either
tolerate or explicitly detect, rather than silently mis-segment.

## Open questions

- What content-unit granularity should sub-page segmentation default to for this repo's
  PageXML-based pipeline -- text line, text region, or a reading-order-sorted region sequence?
- Sub-page ground truth needs an in-page position, not just a page id.
  {class}`~archival_structures.datasets.annotations.Element` /
  {class}`~archival_structures.datasets.annotations.ElementSpan` in
  [Datasets and ground-truth annotation](../datasets_and_annotation.md) already model cross-page
  document *elements* -- worth checking whether document *boundaries* themselves can reuse that
  schema before inventing a second one.
