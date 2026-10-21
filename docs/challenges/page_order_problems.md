# Page order problems

```{note}
Status: **stub**. Formal problem definitions and worked examples only -- detection methods and
the corruption-simulation harness mentioned at the bottom are future work. Part of the
[challenges catalog](index.md).
```

[Whole-page document segmentation](document_segmentation.md#whole-page-segmentation) reduces
segmentation to a per-page "is this a new document?" decision. That reduction assumes the page
stream is already a faithful, in-order rendering of document order. This page catalogs the ways
that assumption fails, with formal notation for each so later ground-truth generation and
detection work has a shared, unambiguous definition to build against.

## Setup and notation

Let `C = (c_1, c_2, ..., c_n)` be the **canonical page stream**: pages in correct document order
-- the order [whole-page segmentation](document_segmentation.md#whole-page-segmentation) assumes
it's working with. Grouped by document, `C` decomposes into `m` contiguous blocks:

```
C = D_1 . D_2 . ... . D_m        ("." = concatenation)
D_k = (c_{s_k}, ..., c_{e_k})    document k's canonical pages
```

Let `O = (o_1, o_2, ..., o_n)` be the **observed page stream** -- what the digitisation actually
produced, some corrupted rearrangement of `C`. Every case below is a different constraint on how
`O` may differ from `C`.

## Case 1 -- intra-document page swap

Pages *within a single document* appear out of order relative to each other, but the document's
page set and its boundaries relative to neighbouring documents are untouched.

Formally: a permutation `pi` of `{1, ..., n}` that is the identity outside one document block's
index range, applied to `C`: `o_i = c_{pi(i)}`.

**Example.** A 4-page letter `D1 = (c1, c2, c3, c4)` followed by a 2-page receipt
`D2 = (c5, c6)`. Two different permutations restricted to `D1`'s positions show that whether
this case is "invisible" to boundary detection depends on *how the task is modelled* -- not just
on the fact that no document boundary is crossed:

```
Canonical:                C   = (c1, c2, c3, c4, c5, c6)

pi_a (swap positions 2,3): O_a = (c1, c3, c2, c4, c5, c6)
pi_b (swap positions 1,3): O_b = (c3, c2, c1, c4, c5, c6)
```

Both `pi_a` and `pi_b` only rearrange positions *within* `D1`'s index range `{1,...,4}` -- neither
moves a page across the boundary with `D2`. What differs between them is whether `D1`'s true start
page, `c1`, ends up first among `D1`'s own positions in the observed stream: `pi_a` leaves it
there, `pi_b` doesn't. That difference doesn't matter for one way of modelling boundary detection
and matters a great deal for the other -- see
[Document segmentation](document_segmentation.md#whole-page-segmentation) for both framings in
full:

- **Framing B (pairwise same-document adjacency)** is unaffected by *either* permutation. Every
  adjacent pair inside `D1`'s positions `{1,...,4}` is still a pair of `D1` pages regardless of
  which specific page landed where, and the pair straddling the boundary (position 4/5) is still
  correctly cross-document in both `O_a` and `O_b`. The task never has to decide which page is
  the start -- only whether neighbours belong together -- so it simply doesn't notice.

- **Framing A (start-of-document classification + the cut reduction)** behaves differently for
  the two:
  - Under `O_a`, `c1` is still the first page of `D1`'s own run. An oracle classifier predicts
    `B = (1,0,0,0,1,0)`; cutting before each `1` recovers `D1` and `D2` exactly. This permutation
    really is invisible to Framing A.
  - Under `O_b = (c3, c2, c1, c4, c5, c6)`, `c1` has moved to the *third* position of its own
    run. An oracle classifier still correctly flags only `c1` as a start page -- content-based
    start detection doesn't care about stream position -- so `B = (0,0,1,0,1,0)`. But the cut
    reduction splits `P` before every predicted start, i.e. between `c2` (position 2) and `c1`
    (position 3): `D1` comes out as two spurious documents, `(c3, c2)` and `(c1, c4)`, even though
    the classifier itself made no mistake. The error is in the reduction, not the classifier: "cut
    before each predicted start" silently assumes a document's start page is always first in that
    document's own run, which `pi_b` violates.

Reading order *within* a recovered document is a separate, third concern from either framing
above: even under `O_a`, where both framings recover `D1` correctly as one document, its pages
read `c1, c3, c2, c4` -- a discontinuity neither framing's boundary decision has any way to see,
since both operate purely on document membership, not on intra-document page order.

## Case 2 -- foreign-document insertion (interleaving)

Pages from a *different* document interrupt a document's own pages, splitting it into two or more
non-contiguous runs in `O`.

Formally, for two documents `D_i` and `D_j` whose pages get interleaved, the observed stream
restricted to `D_i`'s and `D_j`'s positions is an **interleaving** (shuffle) of the two: each of
`D_i` and `D_j` keeps its own internal order, but their elements are merged arbitrarily. (Every
other document's block stays intact and correctly placed.) The set of all such merges is the
*shuffle product* of two sequences `A = (a_1, ..., a_k)` and `B = (b_1, ..., b_l)`, written
`A shuffle B`: every length-`(k+l)` sequence that restricts to `A`, in order, on the positions its
elements occupy, and to `B`, in order, on the rest. `A shuffle B` has `C(k+l, k)` members.

**Example.** Two 2-page deeds, canonically back to back:

```
D1 = (d1_1, d1_2)
D2 = (d2_1, d2_2)
Canonical: C = (d1_1, d1_2, d2_1, d2_2)
Observed:  O = (d1_1, d2_1, d2_2, d1_2)     -- one member of D1 shuffle D2
```

Whole-page boundary detection **does not degrade gracefully** here, under either
[framing](document_segmentation.md#whole-page-segmentation). Under Framing A, reading `O` left to
right, a detector correctly flags position 2 (`d2_1`) as a new document start -- but position 4
(`d1_2`) is not a new document at all, it's a *return* to `D1`, which the "is this the first page
of a new document" framing has no vocabulary for: it can only say start/continue, never resume.
Framing B does better locally but still fails overall: every pairwise label in
`L = (l_1, l_2, l_3)` is correct (`d1_1`/`d2_1` differ, `d2_1`/`d2_2` match, `d2_2`/`d1_2` differ),
yet grouping same-document pairs into maximal runs still assumes each document occupies one
contiguous run of the stream -- exactly what the interleaving breaks. `D1`'s two pages end up in
two separate one-page groups instead of being recognised as one document, even though the pairwise
task made no mistake.

## Case 3 -- swap and insertion combined

The two effects compose: an intra-document permutation (Case 1) applied to a document's pages
before they get interleaved with another document's pages (Case 2).

**Example.**

```
Canonical: D1 = (c1, c2, c3),  D2 = (c4, c5)
Swap within D1: (c1, c3, c2)
Interleave with D2: O = (c1, c4, c3, c5, c2)   -- one member of (c1,c3,c2) shuffle (c4,c5)
```

Both failure modes from Cases 1 and 2 apply simultaneously: neither within-document reading order
nor document contiguity can be assumed.

## Case 4 -- document-level swap

Distinct from Cases 1-3: those corrupt the *page* stream, without regard to document identity.
Document-level swap instead corrupts the **order of documents**, while every document's own
internal page order stays intact and contiguous. This matters specifically when documents have
an independently expected order -- sequential minute/resolution numbers, chronological
correspondence, numbered deeds in a protocol -- that the physical filing or digitisation is
expected to preserve, but doesn't.

Formally: canonical document order `(D_1, ..., D_m)`; a permutation `sigma` of `{1, ..., m}`
yields the observed document order, concatenated:

```
O = D_{sigma(1)} . D_{sigma(2)} . ... . D_{sigma(m)}
```

Each `D_k` is untouched internally -- only the order the blocks appear in changes.

**Example.** Three deeds with sequential numbers 12, 13, 14 (canonical document order
`D_12, D_13, D_14`); deed 13 was misfiled ahead of deed 12:

```
Observed document order: D_13, D_12, D_14
```

Whole-page boundary detection is **entirely correct** here -- every `b_i` is right, every
document's pages are contiguous and internally ordered. The error is invisible to page-level
boundary detection altogether; catching it needs an external ordering signal (e.g. a deed number
or date read from the document's own text) checked against the expected sequence, not anything
recoverable from the page stream alone.

## Case 5 -- page duplication (re-shoot)

A page is photographed twice and the copy sits right after the original (a **re-shoot**; all 13
documented SG re-shoots are adjacent, see phase 08 step 17). Unlike Cases 1-4 this changes the
stream's **length**: the observed stream has `n + n_copies` positions, and a copy has the same
`canonical_index` as its original.

**Example.** Canonical `D1 = (c1, c2)`, `D2 = (c3)`; observed `(c1, c1, c2, c3)`.

Framing A: the first `c1` is the start of `D1`; the copy is a continuation, so the true labels
are `[True, False, False, True]` (`observed_start_labels` labels the first occurrence of a
canonical index only). A classifier that flags both `c1` positions as starts gets one false
positive and cuts `D1` in two. Framing B: every adjacent pair is still labelled correctly (the
copy and its original share a `document_id`), so grouping into maximal runs is unaffected.

Operator: `page_duplication(stream, provenance, rate, rng)`; the rate is **per page** (expected
copies `rate * n`), one `rng.random()` draw per input page.

## Summary

"Affected?" below is with respect to whichever
[segmentation framing](document_segmentation.md#whole-page-segmentation) is in play -- Framing A
(start-of-document classification + cut reduction) and Framing B (pairwise same-document
adjacency) diverge for Case 1, and inherit that same divergence in Case 3.

| Case | What's permuted | Framing A (start + cut) affected? | Framing B (pairwise adjacency) affected? |
|---|---|---|---|
| 1. Intra-document swap | pages within one document | Only if the swap moves the true start page out of the first position of its own run (e.g. `pi_b`, not `pi_a`) | No, never -- see the worked example above |
| 2. Foreign-document insertion | pages across 2+ documents | Yes -- "start vs. resume" has no vocabulary in this framing | Yes, but not because any pairwise label is wrong -- every `l_i` is correctly predicted; grouping same-document pairs into maximal runs still assumes a document occupies one contiguous run, which the interleaving violates |
| 3. Swap + insertion | both | Yes, and possibly compounded by the swap | Yes, for the same reason as Case 2 |
| 4. Document-level swap | whole documents | No -- not visible to either framing; needs an external ordering signal | No -- same |
| 5. Page duplication (re-shoot) | one page repeated right after itself (stream gets longer) | Yes, if the repeated page is a true start: the copy must be labelled a continuation, not a second start | No -- copy and original are the same document, so the pairwise label is correct |

## Open question: simulating these at controlled rates

Once a ground-truth document stream exists for one or more real collections (see
[Datasets and ground-truth annotation](../datasets_and_annotation.md)), each case above can be
turned into a stream-corruption function parameterised by a rate -- e.g. probability per document
of an intra-document swap, expected number of foreign insertions per 100 pages, probability per
document of a document-level swap -- to generate train/test streams with known corruption for
benchmarking detection methods against multiple collections. Not yet implemented; tracked here as
the next step once this terminology/challenge inventory is in place.
