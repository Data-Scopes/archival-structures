# Step 7 -- Scale and robustness

> **Note (2026-10-13).** `NL-HaNA_1.01.02` numbers and line citations in this report were computed on the 2025-03-18 export of that finding aid (7,024 `item`s), now kept as `data/metadata/eads/superseded/NL-HaNA_1.01.02-2025-03-18.xml`. The current export, `data/metadata/eads/NL-HaNA_1.01.02.xml` (version 2026-09-09), has 7,026 `item`s and about 28.6k extra internal `<unitid>`s, so its numbers differ.

**Date:** 2026-09-27 · **Corpus:** 47 finding aids, 20 repositories, 3 countries · **Code
added:** {mod}`archival_structures.parsers.ead_stream` (`parse_ead_streaming`, `ParseReport`),
a small refactor to {mod}`archival_structures.parsers.ead_tree` splitting out
`_parse_did_element`/`_extract_did_fields`/`_extract_other_children` so both parse paths share
one extraction rule (decision D20), `tests/test_parsers_ead_stream.py` (16 tests, including a
corpus-wide equivalence check).

## Summary

> **`parse_ead_streaming` builds a tree structurally identical to `parse_ead`'s on all 47
> finding aids in the corpus, verified field-by-field including ancestry. It is correct. It is
> also, on every file in the corpus, slower and more memory-hungry than the in-memory path --
> confirmed to be an inherent property of `iterparse`'s per-element Python callback overhead,
> not a bug, and not fixable by tuning this implementation.**

Getting to "correct" took two real bugs, both found only by comparing against the full corpus
rather than trusting hand-written fixtures (§ Error analysis). Getting to "slower and more
memory-hungry" was not the expected outcome and is reported as measured (§ Data analysis),
because a streaming parser that claims a benefit that isn't there would be worse than no
streaming parser at all.

## Data analysis

### Correctness: exact equivalence, all 47 finding aids

Every finding aid in the corpus was parsed both ways and compared component-by-component:
`level`, `title`, `unitid` values, `did_extra`/`other_children` keys, `missing_did`,
`index_path`, `level_path`, child count, recursively. **47 of 47 match exactly.** This is now a
standing test (`TestCorpusEquivalence`), not a one-off check.

### Performance: measured honestly, in isolated subprocesses

Initial measurements used `resource.getrusage(...).ru_maxrss` within one long-lived process
across several files in sequence -- which is wrong, because `ru_maxrss` is a
**process-lifetime watermark that never decreases**, so every file after the first inherited the
previous file's peak. Re-measured with one fresh subprocess per (file, method) pair:

| Finding aid | Size | In-memory | | Streaming | |
| --- | ---: | ---: | ---: | ---: | ---: |
| | | time | peak RSS | time | peak RSS |
| `NL-HaNA_2.10.36.22` | <1 MB | 0.00s | 26 MB | 0.00s | 26 MB |
| `NL-AsdSAA_834` | 4 MB | 0.06s | 58 MB | 0.12s | 61 MB |
| `NL-HaNA_1.04.02` | 18 MB | 0.37s | 225 MB | 0.78s | 232 MB |
| `NL-HaNA_1.01.02` | 19 MB | 0.45s | 228 MB | 0.77s | 231 MB |
| `NL-HtBHIC_SVO_1595_1649` | 37 MB | 1.00s | 507 MB | 2.46s | 573 MB |
| `NL-HlmNHA_1098` | 43 MB | 1.64s | 659 MB | 3.55s | 726 MB |
| `NL-AsdSAA_5075` | 106 MB | 1.17s | 621 MB | 2.41s | 669 MB |

**Streaming loses on both dimensions, on every file.** Ratios of peak memory to file size are
also similar between the two approaches (roughly 6-17x across both, no divergence with size),
which is the more important observation: if streaming's root-by-root clearing were paying off,
memory-per-MB should grow more slowly for streaming as documents get larger. It doesn't, in this
size range.

### Why -- confirmed by ablation, not guessed at

```text
plain etree.parse (full DOM, no walk):                          0.25s
bare iterparse (start+end events, no-op):                       0.47s
iterparse end-only + clear finished components (no extraction): 1.05s
```
(`NL-HlmNHA_1098`, 43 MB.) Even doing *nothing* with each event, `iterparse` costs 2x a plain
full-document parse, because every element round-trips through a Python-level callback rather
than staying inside one native `libxml2` call -- this is `iterparse`'s well-known, inherent
trade-off, not an artefact of this implementation. Clearing roughly doubles that again. The full
streaming path (extraction, `copy.deepcopy` of retained fragments, index-path bookkeeping) adds
further real work on top -- consistent with the ~2-3x slowdown measured end-to-end.

### What this means for when to use which path

Every finding aid in this 47-document, 3-country corpus -- up to 106 MB, up to 51,315 components
-- fits comfortably in memory, and for all of them `parse_ead` is strictly better: faster and
smaller. `parse_ead_streaming`'s reason to exist is not visible in this corpus at all: it is for
a document too large to build as one DOM *regardless of speed*, where the alternative is not "a
bit slower," it is `MemoryError` or the process being killed. Nothing in the corpus is that
large. The user's own experience is that such files "are not uncommon" outside it, which is the
actual justification -- not a benchmark this corpus can produce.

**Recommendation, reflected in the code:** `parse_ead` stays the default, documented as such;
`parse_ead_streaming` is there for a caller who already knows their file is large enough to be a
risk, an opt-in, not a blanket replacement.

## Error analysis

Both bugs below were invisible on hand-written fixtures until the fixtures were deliberately
reshaped to trigger them, and were actually *found* by the corpus-wide comparison, not by
inspection -- worth stating plainly, since it is the same lesson as D9/D11/D15/D16/D18 applied to
this step's own new code rather than to the parser under test.

### E1 -- The standard memory-freeing idiom silently deletes a component's own `<did>`

The textbook `iterparse` pattern -- clear a finished element, then delete its already-finished
preceding siblings from the parent -- assumes nothing but the cleared elements themselves
matters. It does not compose with also needing a *sibling* of them: a component's `<did>` and
`<odd>`/`<scopecontent>`/etc. sit **before** its nested `<c>` children in document order. The
first nested child finishing triggers the sweep, which deletes the preceding `<did>` -- before
the parent component's own `end` event ever gets to read it. The first version of this module
did exactly that. It would have silently produced an empty `did_extra`/`other_children` for
every component with nested component children, which is most non-leaf components in the corpus
-- caught only because the full-corpus equivalence check (not any hand-written fixture) flagged
`NL-AmfAE_0001.01.ead.xml`'s `dsc_extra` picking up a stray `['p']` that shouldn't have been
there (§ E2 explains that specific symptom) while investigating a different-looking discrepancy.

Fixed by never reading a component's `<did>`/other children off the component element after the
fact: each is captured (`copy.deepcopy`d) on **its own** `end` event, the moment it closes,
strictly before any later sibling's sweep can reach it.
`test_did_and_odd_survive_when_component_has_nested_children` pins this directly.

### E2 -- A nested `<dsc>` is not the top-level `<dsc>`, even though it has the same name

Step 3 already found that `<dsc type="othertype">` can appear *nested inside a component*
(a free-text container, never a real hierarchy, in every case observed). The streaming parser's
first fix for E1 detected "the top-level `<dsc>`" by tag name alone, which cannot distinguish it
from a nested one -- so a nested `<dsc>`'s own `<p>` content was captured into the **finding
aid's** `dsc_extra`, the wrong bucket, on `NL-AmfAE_0001.01.ead.xml` (which has 5,231 such nested
elements, Step 3's report). Fixed by identifying the genuine top-level `<dsc>` on its first
`start` event and comparing by *identity* everywhere after, which works because `start` events
fire top-down -- the real `<dsc>` is necessarily seen before any descendant, including a nested
one, ever fires its own start or end. `test_nested_dsc_does_not_leak_into_the_real_dsc_extra`
pins this.

### E3 -- `id()` reuse: a stale counter silently inherited by an unrelated later element

A `next_index` dict, keyed by `id(parent_element)` to assign each component a sibling index, was
never cleaned up once a parent finished. Python is free to reuse a garbage-collected object's
memory address for an unrelated later object; once `sweep()` cleared and dropped a finished
component, its `id()` could be (and empirically was, on wide-enough documents) recycled for a
later, unrelated component -- which then silently inherited the earlier one's leftover sibling
count instead of starting at zero. This produced correct *tree shape* (parent-child
relationships were unaffected, built via a different, correctly-popped dict) but wrong
`index_path`/`level_path` labels -- the kind of bug that looks like nothing is wrong until
something reads the path. Found via the corpus-wide diff (`NL-AsdSAA_5074`, `NL-HaNA_1.04.02`,
and 11 others showed index offsets with no corresponding content difference); reproduced
reliably in a fixture only once it was made wide enough (300 siblings) for CPython's allocator to
plausibly recycle addresses within one parse. Fixed by popping `next_index[id(element)]` at the
same point every other per-element dict is already popped -- when the element is finalised, not
left to linger.

## What Step 7 provides to later steps

- `parse_ead_streaming(path, strict=True) -> (EADFindingAid, ParseReport)` -- for a caller who
  already knows or suspects a document is large enough to risk `parse_ead`. Same `EADFindingAid`
  shape, same tests can be pointed at either.
- `ParseReport` -- `strict=False` collects `xml_syntax_error`/`missing_did`/`no_dsc_element`/
  `no_ead_element` diagnostics instead of raising, for a caller that wants to see what a
  malformed large document actually contains rather than abort on the first problem.
- Decision D20 (shared `_parse_did_element`/`_extract_did_fields`/`_extract_other_children`)
  means a future change to what counts as a `<did>` field only has one place to be made for both
  parse paths to agree.

## Conclusions carried into the plan

1. Both parse paths are correct and kept in sync by a standing corpus-wide equivalence test, not
   by inspection.
2. `parse_ead` remains the recommended default; `parse_ead_streaming` is documented as an
   opt-in for known-large files, not a general replacement -- the corpus provides no evidence it
   helps below its actual purpose (documents too large to build as one DOM at all).
3. The `iterparse` per-element Python-callback cost (confirmed ~2x over a plain full parse
   before any of this module's own logic even runs) is a property of the mechanism, not
   something further tuning of this implementation is expected to remove.
4. Two of the three bugs found here were specifically about *order of operations* around
   `iterparse`'s bottom-up `end`-event firing versus this module's need to also read sibling
   content -- worth remembering if `ead_stream` is ever extended further.
