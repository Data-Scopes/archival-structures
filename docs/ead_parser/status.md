# Status and decisions

Working log for the [EAD parser rewrite](plan.md). Decisions are recorded here when taken, with
the reasoning, so that a later step can overturn one on the evidence rather than by preference.

Last updated: 2026-09-27 (after Step 8 -- all eight planned steps now complete).

## Step status

| Step | State | Report |
| --- | --- | --- |
| 0 -- Baseline and structural survey | **complete** | [step-00-baseline](reports/step-00-baseline.md) |
| 1 -- Widen the corpus | **complete** (47 finding aids, 20 repositories, 3 countries -- D19) | [step-01-corpus](reports/step-01-corpus.md) |
| 2 -- Structural conformance harness | **complete** | [step-02-conformance](reports/step-02-conformance.md) |
| 3 -- The component tree | **complete** | [step-03-component-tree](reports/step-03-component-tree.md) |
| 4 -- `<did>` field extraction | **complete** | [step-04-inventory-numbers](reports/step-04-inventory-numbers.md) |
| 5 -- Digital objects and assets | **complete** | [step-05-digital-objects](reports/step-05-digital-objects.md) |
| 6 -- Flat views | **complete** | [step-06-flat-views](reports/step-06-flat-views.md) |
| 7 -- Scale and robustness | **complete** | [step-07-streaming](reports/step-07-streaming.md) |
| 8 -- Migration and retirement | **complete** (partial: 2 of 4 legacy-API notebooks migrated -- see report) | [step-08-migration](reports/step-08-migration.md) |

## Decisions

### D1 -- Rewrite rather than patch *(2026-09-27)*

The current parser's structure -- one `parse_<level>` function per EAD level, each enumerating
the child tags it accepts -- means every new structural variant needs a new branch in a new
place, and an unhandled one either raises or vanishes. Step 0 found three constructs that
vanish. A level-agnostic tree walk removes the class of bug rather than the instances.

The old module stays importable and untouched until Step 6 has a passing superset check, so the
rewrite is never the only working path.

### D2 -- Node tree as the primary output, tables as views *(2026-09-27)*

Chosen over keeping the flat `pandas.DataFrame` as the primary output. `NL-AsdSAA_834` nests
subseries 8 deep and `NL-HaNA_1.01.02` has 7,024 `item` components (2025-03-18 export; 7,026 in the current one) below file level; neither
fits a fixed `series, subseries_1..2, filegroup, file` column layout, and `max_subseries_depth`
is a lossy parameter on what should be a lossless parse. Tables are derived
(`to_frame(level=...)`), including a compatibility view with the old column order.

### D3 -- Harvest EADs from more repositories *(2026-09-27)*

The corpus has ten finding aids from two exporters, and those two disagree on nearly every
detail that matters (`<unitid label="Inventarisnummer">` vs `@type='ABS'`, inline
`<nexus:asset>` vs `<dao role="METS">`, series-rooted vs file-rooted `<dsc>`). With n=2 there is
no way to tell a general EAD pattern from a house style, so Step 1 harvests from ≥5
repositories before the rewrite fixes its assumptions.

**Outcome:** 14 finding aids over 3 repositories, not ≥5 -- limited by publisher access policy,
not tooling (see D6). Vindicated all the same: the single finding aid from the third repository
exposed two fatal assumptions (D7) that ten finding aids from two repositories had hidden.

### D4 -- Unknown structure is reported, not fatal, and never silent *(2026-09-27)*

The current parser raises `ValueError` on unrecognised child tags -- the right instinct, and its
docstring says so explicitly. But it applies the instinct unevenly: unknown *levels* are
dropped silently while unknown *tags* raise. The rewrite retains unknown levels as components
with their literal `@level` value, collects unmodelled `<did>` children into a counted `extra`
dict, and surfaces both in a `ParseReport`. A `strict=True` flag restores raising, for CI.

### D5 -- Inventory-number identification is explicit and audited *(2026-09-27, superseded by D12)*

`re.match(r"\d+", unitid)` conflates "looks like a number" with "is the inventory number".
Step 4 replaces it with repository-aware rules (`label="Inventarisnummer"`, `@type='ABS'`,
shape heuristic as fallback) and records which rule fired per component, so the report can show
where the rules disagree instead of the choice being invisible.

*(Superseded in substance, not spirit, by D12/D18: the specific rule set sketched here was
refined twice more as more repositories were checked. The principle -- explicit, ranked, audited
rules, never a bare regex -- is what stuck.)*

### D6 -- `opendata.archieven.nl` is collected by hand, not by script *(2026-09-27)*

The portal aggregating EAD from dozens of Dutch regional archives answers 403 to every
non-browser `User-Agent` (tested: absent, `curl`, `Python-urllib`, and two honest strings naming
this project; only a Chrome-like string is served), across its listing, its dataset pages and its
sitemap. Independently, its `robots.txt` disallows `/download/`, which is where the EAD files
are. The metadata is CC0/CC-BY and the portal documents OAI-PMH harvesting elsewhere, so the
block reads as a generic anti-bot default rather than a considered policy -- but it is the
policy that is published, so `ead_harvest` does not fetch from that host.

Instead the tooling supports the client the portal *is* built for: `--download-guide` prints the
per-organisation listing URLs plus a browser-console snippet that turns a filtered listing into
direct download links, and `--ingest DIR` files the results into the corpus, renaming each from
its own `<eadid>`. Asking De Ree for a sanctioned route stays open as a follow-up.

**Why:** the data is open, but overriding a publisher's stated access rules from a committed
script is not our call to make silently, and a manual route exists that nobody has to argue about.

**How to apply:** new EAD sources get a harvest function only if they serve a self-identifying
client; otherwise they get a download guide and an ingest path.

### D7 -- Component recognition is a predicate, not a tag comparison *(2026-09-27)*

Step 1 found that EAD 2002 permits two equivalent spellings of the same hierarchy -- recursive
`<c level="...">` and depth-numbered `<c01>`…`<c12>` -- and permits the EAD namespace to be
declared or omitted. The corpus now contains both. The current parser assumes bare `<c>` in no
namespace, so on `NL-UtHUA_842-3` `read_ead` returns `None` and `get_inventory_info` raises
`TypeError`: 305 components, none reached.

So every "is this a component?" test goes through
{func}`~archival_structures.parsers.ead_harvest.is_component`, and every name comparison through
{func}`~archival_structures.parsers.ead_harvest.local_name`. Both are already written and tested;
Step 3 consumes them rather than reinventing the check inline. `read_ead`'s `root_name='ead'`
comparison needs the same treatment.

**Why:** a tag comparison encodes one exporter's spelling at every site that performs it, so the
assumption is impossible to fix in one place later.

**How to apply:** no `element.tag == '<name>'` anywhere in the new parser.

### D8 -- Levels are data, never an enumeration *(2026-09-27)*

Step 2 measured 27 distinct `@level` values across the corpus, 22 of them free-text Dutch
`otherlevel` values (`Adres` 35,729 · `Inschrijving` 26,131 · `Dossier` 8,620 · … ·
`Affiche` 2), plus 2,593 components carrying **no `@level` at all**. A whitelist cannot be
completed and `child.attrib['level']` cannot be assumed to exist.

So a component's level is stored verbatim as a string, `None` is a valid level, and no code
path branches on a closed set of level names. This supersedes the more cautious wording of D4:
it is not that unknown levels should be *tolerated*, it is that there is no known set.

**Why:** every enumeration in the current parser is a place a future finding aid crashes or
vanishes, and the corpus already contains 22 values nobody would have guessed.

**How to apply:** `if level == 'series'` is a bug. Behaviour keys off tree position and whether
a component carries an inventory number, not off the level's name.

### D9 -- The instrument must not share assumptions with the parser *(2026-09-27)*

`ead_survey` parses with `lxml` while `ead_parser` uses `ElementTree`, and neither imports the
other. That independence immediately earned its keep: the survey caught that 16 finding aids
arrive inside an `<opendata>` envelope whose namespace differs from the `<ead>` within, which my
own Step 1 report had recorded incorrectly.

**Why:** a harness that shares an assumption with the code under test cannot detect that
assumption, so coverage numbers from such a harness are worth little.

**How to apply:** keep `ead_survey` free of `ead_parser` imports. When the two disagree,
investigate before assuming the harness is wrong.

### D10 -- The tree is level-agnostic by construction, not by convention *(2026-09-27)*

`EADComponent` has no field or method that branches on what `level` equals. `build_component`
is the same function whether the component is `series`, `Hinderwetvergunning`, or has no
`@level` at all. This is what let Step 3 reach 100% coverage (243,217/243,217, all 27 levels)
without writing 27 cases: there was nothing to enumerate.

**Why:** D8 established that levels are data, not an enumeration; D10 is that decision reified
in the actual class design, so a future finding aid's 28th `otherlevel` value needs no code
change to be captured (only Step 4/5's *interpretation* of it might need one).

**How to apply:** any later step tempted to add `if component.level == "file": ...` should
instead ask "what property does this component have that I actually care about" (carries a
`<unitid>`? has children? sits at a given depth?) and branch on that.

### D11 -- A harness and a parser can share a blind spot even when they share no code *(2026-09-27)*

D9 established structural independence (different XML library, no shared import) as protection
against a harness that can't catch the parser's mistakes. Step 3 found the limit of that
protection: `ead_survey` and `ead_tree` both locate `<dsc>` as "the first one in document
order," independently, because that is what EAD's schema implies -- and one finding aid
(`NL-AmfAE_0001.01.ead.xml`) has 5,231 *nested* `<dsc>` elements neither instrument descends
into. They happened to agree correctly (the nested ones hold no components -- checked by hand,
Step 3 finding E1), but agreement was not, by itself, proof.

**Why:** two implementations of the same understanding of a format will make the same mistake
about that format, however independently they're coded. Only checking against the actual bytes
settles it.

**How to apply:** when a coverage number looks suspiciously round (100%, 0%) or a census turns
up a large unexplained count, open the source file rather than trusting the number. Step 3's
`other_children['dsc']` count (5,562) is what triggered the check that found this.

### D12 -- Inventory-number identification is four ranked, auditable rules, never a regex over raw text *(2026-09-27)*

The legacy parser's `re.match(r"\d+", unit['unitid'])` was measured to be actively dangerous on
the wider corpus: 29% of `file` components (34,480 of 118,946) carry a `type="guid"` `<unitid>`
that starts with a digit and sits before the real identifier, which the legacy rule would pick
instead. `identify_inventory_number` replaces it with four rules tried in order of how directly
each was measured (label → `type="ABS"` → untyped `@identifier` → shape, with known-excluded
types/labels/URL-or-GUID shapes removed before the shape rule ever runs), and reports which rule
fired (or that none did) rather than returning a bare value.

**Why:** a single heuristic that happens to work on two exporters gives no signal about whether
it will work on an eleventh; a ranked, named rule set gives a per-component audit trail instead.

**How to apply:** any further identification logic (Step 6's ancestry-aware views, say) should
consume `InventoryNumberResult.rule` to know *how* confident a match is, not just what the value
is; `"none"` must stay a legitimate, reportable outcome rather than something a caller is
tempted to paper over with a fallback guess.

### D13 -- A `-`-joined identifier is not fixed by widening `InventoryNumber` *(2026-09-27)*

Archief Eemland (`"3818-059"`) and part of Collectie Overijssel Zwolle's map collection
(`"10-1"`) use hyphen-separated numeric codes that `InventoryNumber`'s tokenizer rejects,
accounting for real, non-trivial gaps (31.5% and 14.4% respectively at their unit-of-description
level). Not fixed here: Stadsarchief Amsterdam and the Nationaal Archief already use `-` for a
*different* purpose at filegroup level -- a range between two whole inventory numbers
(`"44-45"`) -- and `InventoryNumber` is shared infrastructure outside this rewrite's ownership.
Teaching its tokenizer to accept `-` risks silently changing what a hyphenated value *means* for
every existing caller, decided by a rewrite of one corner of the codebase.

**Why:** two repositories use the same character for two different things; resolving that by
widening a shared parsing primitive is a modelling decision with consequences outside this
rewrite's scope, not a bugfix.

**How to apply:** left as an open question (below) for whoever has visibility into
`InventoryNumber`'s other callers. If addressed, it should be addressed as a change to
`InventoryNumber` (or a repository-scoped pre-processing step) with its own tests, not folded
into `ead_fields` silently.

### D14 -- Two UK finding aids found in the corpus directory are not (yet) part of the corpus *(2026-09-27, resolved by D19)*

`birmingham_archives_and_heritage_serivce-gb143-ms2141.xml` and
`oxford_house_archives-gb3624-oh.xml` appeared in `data/metadata/eads/` during Step 4, downloaded
by hand in a browser by the user, outside the Step 1 harvest workflow, with no `corpus.tsv`
entry. (A standalone script, `download_sample_eads.py`, using `Sickle` against Archives Hub UK,
was found alongside them and an earlier revision of this entry wrongly credited it as their
source -- it hit the same User-Agent blockade as everything else attempted for the Step 1
addendum and fetched neither file; corrected.) They were checked structurally (100% tree
coverage on both, confirming D10 generalises) and excluded from every Step 4 figure pending a
scope decision.

**Resolved:** the user added these two deliberately and asked (2026-09-27) that the corpus widen
to more countries and archival systems on purpose -- see D19.

### D19 -- The corpus is deliberately international, not Dutch-only *(2026-09-27)*

The user's own words: *"I think it's good to expand the set with more EADs from archival
institutions that use different archival systems and practices, so from a range of countries."*
This resolves D14 and reframes every "the corpus is Dutch archival practice" statement in this
plan (D1-D13, the Test corpus table, the EAD3 note) as describing where the corpus *started*, not
a boundary it is meant to stay inside.

Registered immediately: `143` → Birmingham Archives and Heritage Service (GB), `3624` → Oxford
House Archive (GB). Both parse to 100% tree coverage (D10 holds outside the Netherlands) and
introduce one new `@level` value (`subfonds`, 14 occurrences) and two new `<did>` children
(`<origination>`, `<repository>`) not seen in any Dutch finding aid -- see
[reports/step-01-corpus.md](reports/step-01-corpus.md)'s addendum.

**Why:** the same reasoning as D3, one level up -- a rewrite validated against one country's
archival conventions cannot tell a genuinely general EAD pattern from a national house style any
better than one validated against two exporters could tell an exporter's style from EAD itself.

**How to apply:** further harvesting should deliberately seek structural and national diversity
(different archival numbering traditions, different EAD profiles) over raw volume, matching D3's
original intent. Automated harvesting hit the same access friction found in D6, now at
international scale (§ addendum) -- Library of Congress's OAI-PMH, through which these two
records were originally fetched, is now behind a JavaScript-challenge Cloudflare block that
persists regardless of `User-Agent`, a step beyond `opendata.archieven.nl`'s UA-sniffing block
and not defeatable by a plain HTTP client at all. Manual collection (D6's pattern) is the
practical path for now; a prioritised candidate list is in the addendum for whoever does that
collection.

### D15 -- `<daogrp>` is not a Nationaal Archief construct; a Step 2 attribution is corrected *(2026-09-27)*

`plan.md` had carried, unverified since Step 2, "`<dao>` and `<daogrp>`... Nationaal Archief:
27,661 and 14,263 respectively." Checking per repository (Step 5) shows the Nationaal Archief
has **zero** `<daogrp>` elements; all 14,263 belong to 7 `opendata.archieven.nl`-family
repositories, 95% of them at Brabants Historisch Informatie Centrum. `plan.md` is corrected in
place with a note.

**Why:** the same lesson as D11 (Step 3), one level more concrete -- a count that was real (Step
2 did correctly find 14,263 `<daogrp>` elements) was attached to the wrong repository because
nothing in Step 2's process checked the attribution specifically, and nothing about writing it
down felt uncertain enough to flag.

**How to apply:** a per-mechanism, per-repository breakdown (not just a corpus total) is now the
minimum bar before naming which repository does something, in this rewrite or its documentation.

### D16 -- Digitisation coverage is measured at the unit-of-description level, never at `file` *(2026-09-27)*

A `c.level == 'file'` query for digitisation links reports Brabants Historisch Informatie Centrum
-- its entire 26,131-component presence in the corpus -- as 0% digitised, because it has no
`file` components at all (its unit is `otherlevel:Inschrijving`, and it is in fact 100% covered
by a handle URL). Same root cause as Step 4's per-repository table; the fix is the same
discipline applied a second time: identify the unit of description per repository first (Step
4's method), then query mechanisms/dates/anything else at that level.

**Why:** "most repositories describe at `file` level" is true on average and false for enough of
the corpus (2+ repositories, Step 2) that any single-level assumption silently erases part of
the corpus's digitisation record rather than reporting it as zero honestly.

**How to apply:** Step 6's `to_frame` views must accept or derive a per-repository (or
per-component-carrying-an-inventory-number) unit rather than hard-coding `level='file'`.

### D17 -- The unit of description is identified functionally, not by level name *(2026-09-27)*

Step 2/4's open question -- answered. `unit_of_description_frame` selects components with a
resolved inventory number (Step 4), preferring the deepest such component in any ancestor chain.
This is correct for the 11 of 13 repositories whose unit is `file` and for the three that are
not (`otherlevel:Inschrijving` at BHIC, `otherlevel:Adres`/`Kaart` at Collectie Overijssel
Zwolle, `otherlevel:Dossier` at Erfgoedcentrum) with no special case naming any of those levels.

**Why:** naming the exception levels explicitly would just be D8's mistake again, one level
removed -- a list of known exceptions instead of a list of known levels, equally incomplete the
moment an untested repository is added.

**How to apply:** a caller wanting "the file-equivalent rows" calls
`unit_of_description_frame`, never `to_frame(level="file")`, unless they specifically want only
components at that literal level (rare, and now a deliberate, named choice when it happens).

### D18 -- `type="obsolete"` is not a non-identifying type; the claim was repository-specific and wrong *(2026-09-27)*

Step 4's `NON_IDENTIFYING_TYPES` included `"obsolete"`, generalised from Archief Eemland's usage
(`"St90-832"`, genuinely superseded). Step 6's row-level diff against the legacy parser found 181
Nationaal Archief inventory numbers (`"3714"`, `"9125"`, ...) silently suppressed by this
exclusion -- at the Nationaal Archief and Erfgoedcentrum Achterhoek en Liemers, `type="obsolete"`
marks a perfectly ordinary, current, plain-digit inventory number (97%+ of both repositories'
values parse cleanly). Removed from the exclusion set; Eemland's own case turned out not to need
it anyway, since a hyphenated value already fails `InventoryNumber`'s shape parse on its own.

**Why:** the same mistake as D9/D11/D15/D16, a fourth time -- a per-repository claim, checked
against one repository, applied to all of them.

**How to apply:** `NON_IDENTIFYING_TYPES` (or any similar exclusion set) may only gain an entry
after checking every repository that uses that `@type`/`@label`/shape, individually, the way
Step 6's table did -- one confirming example is not evidence for a corpus-wide rule.

### D20 -- `<did>`/other-child extraction is one shared implementation, not two *(2026-09-27)*

Step 7's streaming parser needs the same "what counts as a `<did>` field, what counts as an
`other_children` entry" logic as the in-memory parser, but cannot call it the same way: the
streaming path must capture each such element on its *own* `end` event (before a later sibling's
memory-freeing sweep could remove it -- see D21), while the in-memory path reads them directly
off an always-fully-formed element. `_parse_did_element`/`_extract_did_fields`/
`_extract_other_children` in `ead_tree.py` are now shared by both, parameterised only by whether
the caller needs an independent copy (`copy_retained`) or a direct reference.

**Why:** two parse paths with two separate extraction implementations will drift the moment one
of them gains a new field or a new exclusion and the other doesn't -- exactly the class of bug
D9/D11 warn about, pre-empted here by construction rather than caught later by a diff.

**How to apply:** any future third parse path (a hypothetical C-extension-backed one, say) reuses
these same three functions rather than re-deriving the rules.

### D21 -- The standard `iterparse` memory-freeing idiom needs a correctness caveat this project now knows about *(2026-09-27)*

"Clear a finished element, then delete its already-finished preceding siblings" is a well-known
`iterparse` pattern, safe when nothing but the cleared elements matters. Step 7 found it is
**not** safe when a cleared element has non-target siblings that also need to be read (a
component's own `<did>`, sitting before its nested `<c>` children) -- the sweep, triggered by the
first nested child finishing, deletes them before the parent's own `end` event can read them.
Fixed by capturing every such sibling on *its own* `end` event, unconditionally, before any later
sweep can reach it -- never by reading content off an element after the fact if anything else in
its subtree might already have triggered a sweep.

**Why:** this is a correctness pitfall in a widely-documented idiom that most treatments of
`iterparse` don't mention, because most `iterparse` use cases don't need a target element's
non-target siblings at all.

**How to apply:** if `ead_stream` is ever extended to capture more sibling content, capture it on
its own `end` event, not the parent's.

### D22 -- `parse_ead` (in-memory) stays the default; `parse_ead_streaming` is opt-in, not a replacement *(2026-09-27)*

Measured (isolated subprocesses, not a shared long-lived process -- an earlier measurement using
`ru_maxrss` in one process across several files was itself wrong, since that value is a
process-lifetime watermark that never decreases): `parse_ead_streaming` is slower and uses more
peak memory than `parse_ead` on every finding aid in the 47-document corpus, up to 106 MB.
Confirmed by ablation to be `iterparse`'s inherent per-element Python-callback cost (~2x a plain
full-DOM parse before any of this module's own logic runs), not a flaw specific to this
implementation and not something further tuning is expected to remove.

**Why:** a streaming path is only worth its overhead for a document too large to build as one DOM
*at all* -- nothing currently in the corpus is that large, so the benefit cannot be demonstrated
on it, only argued from the user's own experience that such files exist elsewhere.

**How to apply:** don't route every parse through `parse_ead_streaming` "to be safe" -- it is
strictly worse for anything that already fits in memory, which is everything measured so far. Use
it for a file already known or suspected to be large enough to risk the in-memory path.

### D23 -- Notebooks are migrated only when their behaviour can be verified, not just their imports *(2026-09-27)*

Two of the four notebooks referencing the legacy EAD API (`download_thumbnails.ipynb`,
`parsing-EADs.ipynb`) are substantial, execution-dependent analyses -- one references a file path
in a sibling repository not present in this checkout at all. Editing their source cells to call
the new API without being able to run them and see the output would risk producing a notebook
that imports cleanly but is silently wrong, which is worse than one plainly still on the old,
working API. Left unmigrated, reported as such (Step 8's own allowance for this). The other two
notebooks turned out to already reference a stale import path
(`archival_structures.ead_parser`, missing `.parsers.`) predating this rewrite entirely -- not
migrated either, since there is no currently-working behaviour in them to preserve.

**Why:** a migration that cannot be verified by running the thing being migrated is a guess
wearing the shape of a fix.

**How to apply:** if these notebooks are migrated later, do it with the ability to actually
execute them against real data, not by editing cells to look right.

## Open questions

Pruned 2026-09-27: several questions below had been overtaken by later decisions but not marked
as such. Resolved ones are listed at the end for the record, with the decision that closed them,
rather than deleted outright -- consistent with this file's own practice of not rewriting
history.

- **What does "no digital link" actually mean for a repository where a handle URL is present
  on nearly every component?** (Step 5.) A handle can resolve to a catalogue record with no
  images; the EAD alone cannot distinguish "digitised, referenced by handle" from "catalogued
  only, referenced by handle." Only Stadsarchief Amsterdam's assets give a real presence/absence
  signal (42.9% of its files have none). Affects how confidently any downstream consumer can
  report "X% of this repository is digitised." **Still open.**
- **Should `InventoryNumber` support hyphen-separated identifiers?** (D13.) `-` already means
  "range between two numbers" at Amsterdam/Nationaal Archief filegroup level and "part of one
  compound identifier" at Archief Eemland/Collectie Overijssel. Affects 31.5% of Archief
  Eemland's file-level identification and 14.4% of one Zwolle finding aid's map collection.
  **Still open**, deliberately -- needs a decision from whoever can see `InventoryNumber`'s other
  callers, not a guess made inside this rewrite.
- **UK slash-hierarchical reference numbers.** `"MS 2141/A/1/1"`, `"OH/1/2/1/1"` -- a repository
  prefix plus the component's full path through the hierarchy. `InventoryNumber` has no token for
  space or `/`, so these resolve to `"none"` (224/224 for Birmingham; 850/853 for Oxford) --
  correctly, in the sense of not guessing, but not usefully. Same category of question as D13.
  **Still open**, pending more non-Dutch finding aids to weigh the answer against.
- **What is `<dsc type="othertype">` used for elsewhere?** In this corpus's one user of the
  construct (`NL-AmfAE_0001.01.ead.xml`, 5,231 instances) it is a free-text container holding a
  single `<p>`, never a further hierarchy. Not verified against any other exporter; a future
  finding aid could use it differently, and `ead_survey`/`ead_tree` would not currently notice
  (see D11). **Still open.**
- **`item` vs `subfile` semantics, and whether `item`/`subfile` support is general or
  Nationaal-Archief-specific.** `NL-HaNA_1.01.02` uses both (7,024 and 2,222 respectively, in the 2025-03-18 export),
  nested to depth 7, as apparent siblings describing the same kind of thing (Step 0's E2). The
  international corpus (Step 1's addendum) now shows `item` at 4 repositories rather than 1 --
  Birmingham, Oxford and Yale all use it too -- which weakens "NA-specific" without yet
  answering whether it is one universal concept or several repositories' independent, coincidentally-
  same-named ones. **Still open**, evidence base wider than when first raised.
- **What counts as "digitised document" for downstream stream analysis: an `item` component, or
  a run of scan-level assets/`<daogrp>` entries?** They are not the same granularity, and Step 5
  characterised the four *mechanisms* without resolving this specific overlap question (Amsterdam
  has assets without items; `1.01.02` has items without assets/dao at item level). **Still open**
  -- relevant to `archival_structures.stream_analysis`, not decided by this rewrite alone.
- **Empty `<unittitle/>`.** Widespread in the Amsterdam EADs (`NL-AsdSAA_30312` has
  `<unittitle />` on most files, with the date carrying the identifying information). Should a
  component fall back to its date range, its `physloc`, or stay untitled? Affects every
  human-readable view -- `ead_views` currently just leaves `title=None`. **Still open.**

### Resolved

- ~~Should the corpus widen beyond Dutch archival practice?~~ (D14) -- **yes**, by explicit user
  decision; see D19. Corpus is now 47 finding aids / 20 repositories / 3 countries.
- ~~The bare-numeric-code collision risk in `AGENCY_REPOSITORIES`~~ -- fixed, not just noted:
  `extract_ead_identity` now combines a purely-numeric `mainagencycode` with its `@countrycode`
  (`"143"` + `"GB"` -> `"GB-143"`) before it ever reaches that map (built while resolving D19).
- ~~Which component is the unit of description?~~ -- resolved functionally by
  `unit_of_description_frame` (D17, Step 6): the deepest component in any chain with a resolved
  inventory number, never by level name.
- ~~EAD3 not present in the corpus~~ -- overtaken by events: 5 Yale EAD3 finding aids are now in
  the corpus (Step 1's addendum), and the tree builder needed no changes to reach 100% coverage
  on them.
- ~~A third digitisation-linking mechanism (Het Utrechts Archief's handle `<unitid>`)~~ --
  superseded by Step 2/5's fuller count of four mechanisms corpus-wide (`<dao>`, `<daogrp>`,
  Memorix `<asset>`, handle `<unitid>`s), documented in the plan's Step 5 section rather than
  carried here as an open question.
