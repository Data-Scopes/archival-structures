# Step 03d: `reports/` directory and `make reports`

> **Phase:** 02, EAD for LotA. **Depends on:** nothing. Run it **before 03c**, so that every
> later step can write its report section and build it.
> **Model:** Sonnet. **Read:** `plan/README.md` (the *Layout* section and *Rules for every
> session*), the phase `STATUS.md`, `plan/templates/REPORT.md`, then this file. **Read nothing
> else unless this brief names it.** If you have to explore the code base anyway, do what you
> must and record it under *Brief defects*.

## Context

The user wants a written report per phase, including its error analysis, in which findings are
easy to look up. The reports are Markdown under `reports/` at the AS root, and are converted to
HTML by a Makefile target (user, 2026-10-14). `plan/README.md` (*Layout*, rules 8–9) and
`plan/templates/REPORT.md` define the conventions. This step builds the tooling and creates the
index. It also creates the phase-02 report, with the planning findings and step 03b's section
backfilled from `PROGRESS.md`. This is infrastructure; it changes no AS or sibling code.

## Inputs

- AS root: `/Users/marijnkoolen/Code/Huygens/archival-structures`. Check that you can write to it
  and to `Makefile` and `.gitignore` before starting (`test -w`).
- `pandoc 3.12.1` at `/opt/homebrew/bin/pandoc` (checked 2026-10-14).
- AS `Makefile` (49 lines):
  - a header comment (L1–12) listing the targets as `#   make <target>   <description>`;
  - `PYTHON ?= .venv/bin/python`;
  - one `.PHONY` line (L17–18);
  - the targets `version … test docs`. `docs` (L48–49) runs `$(MAKE) -C docs html` (Sphinx);
    leave it alone.
- AS `.gitignore`: it already ignores `docs/_build/` (L75).
- The phase-02 `PROGRESS.md` blocks to backfill from:
  - *Phase planning*, under its *Checked by prototype* and *Side findings* bullets;
  - *Step 03b: LotA switches to AS node ids* (about 60 lines).

  Read only those two blocks.

## Deliverables

| File | Change |
|---|---|
| `Makefile` | targets `reports` and `reports-open`; add both to the header comment and `.PHONY` |
| `.gitignore` | `reports/_build/` |
| `reports/_tools/md-links.lua` | pandoc Lua filter (below) |
| `reports/_tools/report.css` | a small stylesheet (below) |
| `reports/index.md` | landing page: conventions, phase table, findings index (below) |
| `reports/phase-02-ead-for-lota.md` | from `plan/templates/REPORT.md`, status *draft*, with the planning findings and the 03b section filled in |

## Behaviour to implement

**`make reports`**, run from the AS root:

```make
REPORTS_DIR   = reports
REPORTS_BUILD = $(REPORTS_DIR)/_build

reports:
	@command -v pandoc >/dev/null 2>&1 || { echo "pandoc not found (brew install pandoc)"; exit 2; }
	@mkdir -p $(REPORTS_BUILD)
	@set -e; for f in $(REPORTS_DIR)/*.md; do \
	  n=$$(basename "$$f" .md); \
	  pandoc "$$f" --from gfm --to html5 --standalone --toc --toc-depth=3 \
	    --metadata pagetitle="$$n" \
	    --lua-filter $(REPORTS_DIR)/_tools/md-links.lua \
	    --css $(REPORTS_DIR)/_tools/report.css --embed-resources \
	    --resource-path=.:$(REPORTS_DIR) --fail-if-warnings \
	    -o $(REPORTS_BUILD)/$$n.html; \
	  echo "  $(REPORTS_BUILD)/$$n.html"; \
	done
	@chmod -R g+w $(REPORTS_BUILD)

reports-open: reports
	open $(REPORTS_BUILD)/index.html
```

If a flag doesn't work as intended, adapt it, and record the change under *Deviations*.

**`md-links.lua`.** The HTML lives one directory deeper (`reports/_build/`) than the Markdown,
so links need rewriting:
- leave alone any link that has a URL scheme (`https:`, `mailto:`), starts with `#` or starts
  with `/`;
- a relative link to a `.md` file **without a `/`** (another report) becomes `.html`, keeping
  any `#fragment`: `phase-02-ead-for-lota.md#step-03b-…` → `phase-02-ead-for-lota.html#step-03b-…`;
- any other relative link or image source (e.g. `../plan/phase-02-ead-for-lota/PLAN.md`,
  `data/phase-02-ead-for-lota/x.tsv`) gets `../` prepended, so it resolves from `_build/`.

This filter, together with the recipe above, was tested in planning on pandoc 3.12.1. Both
files built with `--fail-if-warnings`, the links came out as intended, the CSS was embedded,
and the heading `### Step 03b: LotA switches` got `id="step-03b-lota-switches"`. Use the filter
as given:

```lua
local function fix(t)
  if t:match("^%a[%w+.-]*:") or t:match("^#") or t:match("^/") or t == "" then return t end
  local path, frag = t:match("^([^#]*)(.*)$")
  if path:match("%.md$") and not path:find("/") then return path:gsub("%.md$", ".html") .. frag end
  return "../" .. t
end
function Link(el) el.target = fix(el.target); return el end
function Image(el) el.src = fix(el.src); return el end
```

**`report.css`** (about 30 lines):
- body `max-width: 60rem`, system sans-serif, line-height 1.5;
- tables collapsed with 1px borders, zebra rows, `font-size: 0.9em`, horizontal scroll on narrow
  screens (wrap with `overflow-x: auto` via `table { display: block; overflow-x: auto; }`);
- `code` in a monospace font with a light background;
- the TOC (`nav#TOC`) boxed;
- a `@media (prefers-color-scheme: dark)` block with readable colours.

**`reports/index.md`:**

```markdown
# AS reports

<3–6 lines: one report per phase; plan/ records how the work went and reports/ what was found;
build with `make reports` (HTML in reports/_build/); conventions in plan/README.md and
plan/templates/REPORT.md.>

## Finding IDs

<The ID scheme `F<phase>-<step>.<n>`, `F<phase>-P.<n>` (planning), `F<phase>-R.<n>` (review);
the kinds and statuses, copied from the template; "grep -rn F02-05.2 reports/ plan/".>

## Phases

| Phase | Report | Status | Updated |
|---|---|---|---|
| 01 | Shared-library foundation (before plan/ existed; see docs/shared_library/changelog.md) | no report | — |
| 02 | [EAD for LotA](phase-02-ead-for-lota.md) | draft | <date> |
| 03 | Evaluation for NLP | not started | — |

## Findings

Confirmed findings of every reviewed phase, appended by each phase review. Draft findings live
in the phase reports until the review.

| ID | Finding | Kind | Tags | Report |
|---|---|---|---|---|
```

The findings table stays empty until phase 02's review.

**`reports/phase-02-ead-for-lota.md`.** Copy the template. Fill in the header (status *draft*;
the phase plan link `../plan/phase-02-ead-for-lota/PLAN.md`; code state "AS unreleased after
v0.4.1"). Create `### Step …` headings for 03b, 03c, 04, 05 and 06 (titles from the phase
`STATUS.md`). Leave all but 03b as `<to be written by the step session>`. 03d itself gets no
section: it is infrastructure, and is recorded in `PROGRESS.md` only.

- **Planning findings.** Add an `### Planning` section before the steps. Turn each planning
  fact below into a row in *Findings* (IDs `F02-P.1`…, status `draft`), with a sentence or two
  in the section:
  1. LotA outputs were not regenerated after AS step 2 (scope notes 2026-10-03, titles
     2026-10-08), which led to step 03c;
  2. LotA's old inventory regex swallows an archive code as an inventory number
     (`inv.nr. 12, 1.11.01.01, inv.nr. 687` → `12, 1.11.01.01`);
  3. lxml rejects control characters in element text, so the span markers live in the
     text walk;
  4. the marker-in-walk invariant holds on 22,197 SG and 14,505 VOC titles;
  5. LotA `ArchiveNode`'s docstring overstates its equality;
  6. `_segments` ignores the `<lb/>`-before-punctuation rule.
- **03b section.** Rewrite the 03b `PROGRESS.md` block into the template's step-section form:
  - *What changed*;
  - *Results*: a table with rows per archive (frame rows, identical yes/no) and the test
    counts;
  - *Error analysis*: the `default_archive_id` versus LotA id comparison for the 7 archives as
    a table, and the 73-row title check;
  - *Findings*;
  - *Open issues*: the two open questions.

  Findings (IDs `F02-03b.1`…, status `draft`) are at least:
  - frame and node ids identical on all 9 archives (result);
  - `default_archive_id` ≠ LotA archive id for 7 of 9 archives, so AS frames used on LotA
    archives must pass `archive_id` (data-property);
  - `NL-HtBHIC_SVO_1595_1649` has 0 unit-of-description rows; pre-existing and not
    investigated (open-issue);
  - the `node_id` dtype on BHIC's empty frame went from `str` to `object`, pending the user
    (behaviour-change);
  - LotA `model/node.py`'s module docstring is stale (open-issue).

  Copy the numbers exactly from `PROGRESS.md`; don't re-run anything.
- *Reproduce:* the 03b commands (AS and LotA test commands, and the scratch snapshot method
  described in the block).

## Constraints

- Don't change the `docs` target or anything under `docs/`.
- Don't put generated HTML anywhere but `reports/_build/`.
- Never `git commit`; `chmod -R g+w reports/` and any file you create.

## Tests

No pytest. Check by running:
1. `make reports` exits 0 and prints 2 files; `--fail-if-warnings` means there were no warnings.
2. `grep -o 'href="[^"]*"' reports/_build/index.html` shows `phase-02-ead-for-lota.html`, not `.md`.
3. In `reports/_build/phase-02-ead-for-lota.html`:
   - the plan link is `../../plan/phase-02-ead-for-lota/PLAN.md` (one `../` from the source
     plus the one the filter adds);
   - the findings-table section links resolve to existing `id=` anchors (check every
     `href="#…"` with grep);
   - there is a `<style>` element (the CSS is embedded).
4. Temporarily remove pandoc from `PATH` (`PATH=/usr/bin:/bin make reports`): it prints the
   "pandoc not found" message and exits 2.

## Acceptance criteria

1. All 4 checks above pass; report the outputs in `PROGRESS.md`.
2. The phase-02 report has 6 planning findings and at least 5 03b findings, every one with an
   ID, kind, tags, status `draft` and a working section link.
3. `make test` is not affected (don't run the full suite for this step; nothing in the package
   changed).

The user decides when the step is done.

## Error analysis

Open both HTML files (`make reports-open`, or read the HTML) and check, as a reader would:
- whether every finding can be found from the index in two clicks;
- whether the tables are readable;
- whether the TOC lists the step sections.

Record anything awkward under *Open questions*, rather than redesigning the template.

## Do not touch

- `archival_structures/`, `tests/`, `docs/`, any sibling.
- Other phases' plan files. The changelog: this step changes no public API.

## When done

1. Append the block below to the phase `PROGRESS.md`.
2. Overwrite the phase `STATUS.md`: 03d done, and under *Facts later steps need* add "write
   your section of `reports/phase-02-ead-for-lota.md` and run `make reports`".

```markdown
### Step 03d: reports/ directory and make reports
- **Session:** <model>, <date>. **Status:** done / blocked / partial
- **Files changed:** <paths>
- **Public API:** none
- **Tests:** <the 4 checks, with output>
- **Acceptance:** <criterion by criterion>
- **Report:** reports/phase-02-ead-for-lota.md (planning findings F02-P.1–6, step 03b F02-03b.1–n)
- **Deviations from brief:** <or "none">
- **Brief defects:** <facts the brief lacked, or "none">
- **Open questions for the user:** <or "none">
```
