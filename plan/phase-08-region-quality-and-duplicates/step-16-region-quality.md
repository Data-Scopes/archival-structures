# Step 16: Region-quality filter (C16)

> **Phase:** 08, Region quality and duplicates. **Depends on:** nothing.
> **Model:** Sonnet. **Read:** `plan/README.md` (*Rules for every session* only), the phase
> `STATUS.md`, then this file, then LotA's `CLAUDE.md` (you edit LotA in Part B). **Read nothing
> else unless this brief names it.** If you have to explore the code base anyway, do what you must
> and record it under *Brief defects*.

## Context

LotA's rule-cascade filter classes a region's text as usable, fragment or noise. It is generic,
so AS takes it as `analysis/region_quality.py`. AS takes the **text** of a region, not LotA's
`Region`, and the lexicon is **injected** as a set of words: no gensim, no LotA config. LotA keeps
`load_lexicon`, its source-level helpers and its CLI, and calls AS. This is a pure port. Every
stored LotA number must reproduce exactly.

## Inputs

LotA root `L=/Users/Shared/claude_projects/data_scopes/language-of-the-archive` (Python 3.14,
`venv/bin/python`; it imports AS from the AS working tree). Checked 2026-10-21; the files have
no uncommitted changes:
- `L/archival_language/corpus/quality.py` (436 lines): `QUALITY_CLASSES` L32,
  `DEFAULT_KEEP_CLASSES` L42, `is_kept` L45, `_STRIP_FOR_LOOKUP_RE` L53, `THRESHOLDS` L60–103,
  `QualityVerdict` L106, `load_lexicon` L131, `_clean_token_for_lookup` L167, `region_features`
  L173–239, `_clip` L242, `classify_features` L246–321, `score_region` L324, `_score_one` L331,
  `quality_frame` L348, `inventory_quality` L369.
- Callers (their signatures must keep working): `cli/region_quality.py` L34–39, L100–101,
  L197–198, L368; `cli/geometry_probe.py` L38, L311; `tests/test_corpus_quality.py` (15 tests).
- LotA `Region` (`model/region.py`) has `.text`, `.bbox` (`BBox(x, y, w, h)` or `None`) and
  `.region_id`. `BBox.area = w*h`; `BBox.aspect_ratio = w/h`, or `math.inf` if `h == 0`.
- Gold: `L/gold/02-region-quality-done.tsv` and `L/gold/02c-short-region-quality-done.tsv`,
  150 rows each, read with `sep="\t", quoting=3, keep_default_na=False`. Columns include
  `row_no, text, label` (and `notes`). Split: odd `row_no` is `tune`, even is `held_out`.
- LotA's stored scorings (`L/output/quality/`), columns `row_no, text, gold_label,
  predicted_class, score, rules_fired ("|"-joined), split, …`: `1.01.02-gold-scored-after.tsv`
  (sheet 02) and `1.01.02-gold-scored-supplement-after.tsv` (02c) are the current code; the
  `-before` pair is the pre-retune code. `1.01.02-gold-scored.tsv` and `-supplement.tsv` are
  **stale copies of `-before`**; don't use them.
- Lexicon cache: `L/output/lexicon/fasttext-dim_384-window_10-min_count_100-case_lower-vocab.txt.gz`,
  one lower-case term per line, 233,538 terms.
- Planning scripts (run from `L` with LotA's venv; they write nothing):
  `reports/data/phase-08-region-quality-and-duplicates/planning/gold_check.py`, `iq_check.py`.

The LotA code to port, verbatim (the AS version replaces `region.text` with `text`, and
`region.bbox` with the tuple `bbox`):

```python
_STRIP_FOR_LOOKUP_RE = re.compile(r"^\W+|\W+$")
def _clean_token_for_lookup(token): return _STRIP_FOR_LOOKUP_RE.sub("", token).lower()
THRESHOLDS = {"lexicon_hit_rate_len_ge4_strong": 0.5, "lexicon_hit_rate_len_ge4_weak": 0.2,
    "min_tokens_for_no_lexicon_penalty": 6, "singleton_token_ratio_noise": 0.5,
    "longest_nonalpha_run_noise": 6, "alpha_ratio_low": 0.3, "max_tokens_for_short": 2,
    "digit_vs_punct_fraction_for_numeric_exempt": 0.5, "score_cutoff_noise_fragment": 0.35,
    "score_cutoff_fragment_usable": 0.50}
# region_features body
    text = region.text; n_chars = len(text); tokens = text.split(); n_tokens = len(tokens)
    n_alpha = sum(1 for c in text if c.isalpha()); n_digit = sum(1 for c in text if c.isdigit())
    n_space = sum(1 for c in text if c.isspace()); n_punct = n_chars - n_alpha - n_digit - n_space
    n_upper = sum(1 for c in text if c.isupper())
    alpha_ratio = n_alpha / n_chars if n_chars else 0.0   # digit_ratio, punct_ratio likewise
    uppercase_ratio = n_upper / n_alpha if n_alpha else 0.0
    token_lens = [len(t) for t in tokens]
    mean_token_len = statistics.fmean(token_lens) if token_lens else 0.0
    max_token_len = float(max(token_lens)) if token_lens else 0.0
    singleton_token_ratio = (sum(1 for l in token_lens if l == 1) / n_tokens if n_tokens else 0.0)
    non_space = "".join(ch for ch in text if not ch.isspace())
    longest_nonalpha_run = 0; current_run = 0
    for ch in non_space:
        if ch.isalpha(): current_run = 0
        else: current_run += 1; longest_nonalpha_run = max(longest_nonalpha_run, current_run)
    cleaned = [c for c in (_clean_token_for_lookup(t) for t in tokens) if c]
    hits_all = [c in lexicon for c in cleaned]
    lexicon_hit_rate = sum(hits_all) / len(hits_all) if hits_all else 0.0
    hits_ge4 = [c in lexicon for c in cleaned if len(c) >= 4]
    lexicon_hit_rate_len_ge4 = sum(hits_ge4) / len(hits_ge4) if hits_ge4 else 0.0
    # bbox_w, bbox_h, bbox_area, bbox_aspect_ratio: float(...) of the bbox values, nan if None
    return {"n_tokens": float(n_tokens), "n_chars": float(n_chars), "alpha_ratio": …,
      "digit_ratio", "punct_ratio", "uppercase_ratio", "mean_token_len", "max_token_len",
      "singleton_token_ratio", "longest_nonalpha_run": float(...), "lexicon_hit_rate",
      "lexicon_hit_rate_len_ge4", "bbox_aspect_ratio", "bbox_w", "bbox_h", "bbox_area"}  # this order
# classify_features body (T = THRESHOLDS)
    score = 0.5; fired = []
    if f["lexicon_hit_rate_len_ge4"] >= T["lexicon_hit_rate_len_ge4_strong"]: score += 0.30; fired.append("strong_lexicon_hits")
    elif f["lexicon_hit_rate_len_ge4"] >= T["lexicon_hit_rate_len_ge4_weak"]: score += 0.15; fired.append("weak_lexicon_hits")
    elif f["n_tokens"] >= T["min_tokens_for_no_lexicon_penalty"] and f["lexicon_hit_rate_len_ge4"] == 0.0:
        score -= 0.10; fired.append("no_lexicon_hits_despite_length")
    if f["singleton_token_ratio"] >= T["singleton_token_ratio_noise"]: score -= 0.20; fired.append("mostly_singleton_tokens")
    if f["longest_nonalpha_run"] >= T["longest_nonalpha_run_noise"]: score -= 0.30; fired.append("long_nonalpha_run")
    if f["alpha_ratio"] < T["alpha_ratio_low"]:
        d, p = f["digit_ratio"], f["punct_ratio"]; denom = d + p
        frac = d / denom if denom else 0.0
        if d > 0.0 and frac >= T["digit_vs_punct_fraction_for_numeric_exempt"]: fired.append("numeric_content_exempt")
        else: score -= 0.15; fired.append("low_alpha_content")
    if f["n_tokens"] <= T["max_tokens_for_short"]: score -= 0.10; fired.append("short_region")
    score = max(0.0, min(1.0, score))
    cls = "noise" if score < T["score_cutoff_noise_fragment"] else "fragment" if score < T["score_cutoff_fragment_usable"] else "usable"
    return QualityVerdict(region_id=region_id, quality_class=cls, score=score, rules=tuple(fired))
```

Keep the operation order **exactly** (float sums). Keep LotA's comments, such as the one on the
named exemption.

## Deliverables

| File | Change |
|---|---|
| `archival_structures/analysis/region_quality.py` (new) | the API below; module docstring says the thresholds were tuned on SG (NL-HaNA 1.01.02) Dutch HTR, LotA reports 05 and 09 |
| `tests/test_analysis_region_quality.py` (new) | tests below |
| `tests/fixtures/region_quality/` (new) | `gold_rows.tsv`, `lexicon_subset.txt`, `README.md` (provenance) |
| `reports/data/phase-08-region-quality-and-duplicates/step-16/` | `build_fixture.py`, `metrics.py`, `lota_check.py` |
| LotA `archival_language/corpus/quality.py` | wrappers (Part B) |
| LotA `tests/test_corpus_quality.py` | + `test_thresholds_match_as_defaults` |
| LotA `docs/from-archival-structures.md` | new `## 7. Region quality and duplicates in AS (phase 08)`, a short paragraph for 16 (step 17 adds to it) |
| `docs/shared_library/changelog.md` | `### Step 16: region-quality filter (C16)` under `## Unreleased`, added with Edit. If there is no `## Unreleased` heading above `## v0.12.0 (2026-10-21)`, add it |
| `reports/phase-08-region-quality-and-duplicates.md` | your `### Step 16` section and findings `F08-16.n` |

## Behaviour to implement (Part A, AS)

```python
QUALITY_CLASSES: tuple[str, ...] = ("usable", "fragment", "noise")
DEFAULT_KEEP_CLASSES: frozenset[str] = frozenset({"usable", "fragment"})
def is_kept(quality_class: str, *, keep_classes: frozenset[str] = DEFAULT_KEEP_CLASSES) -> bool
FEATURE_NAMES: tuple[str, ...]          # the 16 keys, in the order above

@dataclass(frozen=True)
class QualityThresholds:                # fields = THRESHOLDS keys, defaults = the values above
    ...                                 # (keep 6, 6, 2 as ints; annotate float)
    def as_dict(self) -> dict[str, float]
    @classmethod
    def from_dict(cls, values: Mapping[str, float]) -> "QualityThresholds"  # unknown key -> ValueError; missing keys keep defaults
DEFAULT_QUALITY_THRESHOLDS = QualityThresholds()

@dataclass(frozen=True)
class QualityVerdict:                   # same fields as LotA's
    region_id: str; quality_class: str; score: float; rules: tuple[str, ...]

def lexicon_lookup_form(token: str) -> str          # = LotA _clean_token_for_lookup
def region_features(text: str, *, lexicon: AbstractSet[str],
                    bbox: tuple[float, float, float, float] | None = None) -> dict[str, float]
def classify_features(features: Mapping[str, float], *, region_id: str = "",
                      thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS) -> QualityVerdict
def score_region(text: str, *, lexicon: AbstractSet[str], region_id: str = "",
                 bbox: tuple[float, float, float, float] | None = None,
                 thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS) -> QualityVerdict
def quality_frame(regions: pd.DataFrame, *, lexicon: AbstractSet[str], text_col: str = "text",
                  id_col: str | None = "region_id", thresholds: QualityThresholds = DEFAULT_QUALITY_THRESHOLDS,
                  with_features: bool = True) -> pd.DataFrame
```

- `bbox` is `(x, y, w, h)`: `bbox_w = float(w)`, `bbox_h = float(h)`, `bbox_area = float(w*h)`,
  `bbox_aspect_ratio = float(w/h)` or `math.inf` when `h == 0`; all four are `nan` when `bbox`
  is `None`. No rule reads them.
- `lexicon` entries are expected lower-case; the module never lower-cases the lexicon.
- `quality_frame` returns a copy of `regions` in the same row order, plus `quality_class`, `score`
  and `rules` (`"|".join`). With `with_features`, it also adds the 16 `FEATURE_NAMES` columns.
  `id_col=None` gives `region_id=""`. An empty input gives an empty frame that has all the
  columns. No threads.
- Imports only stdlib, pandas and AS. No `main_text`/`margin` literals.

**Part B, LotA (after Part A's tests pass).** In `quality.py`: import `QUALITY_CLASSES,
DEFAULT_KEEP_CLASSES, is_kept, QualityVerdict, QualityThresholds` from AS (still importable from
LotA). Keep the commented `THRESHOLDS` dict as it is. `region_features(region, *, lexicon)`
returns `as_region_features(region.text, lexicon=lexicon, bbox=None if region.bbox is None else
(region.bbox.x, region.bbox.y, region.bbox.w, region.bbox.h))`. `classify_features(features, *,
region_id)` returns `as_classify_features(features, region_id=region_id,
thresholds=QualityThresholds.from_dict(THRESHOLDS))`. `score_region`, `_score_one`,
`quality_frame`, `inventory_quality` and `load_lexicon` stay unchanged. Delete
`_STRIP_FOR_LOOKUP_RE`, `_clean_token_for_lookup` and `_clip`, and the imports that become unused
(`re`, `statistics`; keep `gzip`). Add a one-line comment naming AS as the owner.

## Constraints

- No gensim or datasketch in AS. No changes to LotA's CLI or to the LotA tests that exist (only
  the addition).
- Don't run LotA's `cli.region_quality`: its default outputs are the stored files. Its output
  path must be under LotA's allowed roots; the AS scratchpad is refused. Use the scripts.
- **Protected outputs:** `L/output/quality/*` and `L/gold/*`. Before the first run, record
  `size=N mtime path` for each file, plus sha256 of the two gold sheets and the lexicon cache,
  into `reports/data/…/step-16/protect_before.txt`. Re-check after every run. Adapt
  `reports/data/phase-07-sg-units-and-pilot/step-15i2/protect.py` (skip list: nothing).
- Never `git commit`; never bump the version; `chmod g+w` new files and dirs. No `rm -rf`,
  `nohup` or `&`.

## Tests

`tests/test_analysis_region_quality.py`. The fixture lexicon is `LEXICON = frozenset({"van",
"de", "en", "den", "te", "resolutie", "staten", "generaal", "brief", "heeft", "ripperda"})`.
Ported from LotA, on text instead of `Region` (1–11):
1. `test_text_is_never_mutated` ("  Een Brief Van Ripperda  "; no feature value is a str);
2. `test_no_region_type_literal_in_rules` (no `"main_text"`/`"margin"` in the source);
3. `test_score_is_deterministic_and_monotone` ("van de resolutie heeft brief van staten generaal" > "xzq vbk qjw zzzz!!!! #### &&&&");
4. `test_rules_record_every_firing` ("!!@@##$$": `long_nonalpha_run`, `low_alpha_content`, `short_region`);
5. `test_classes_are_exactly_three` ("van de resolutie heeft brief", "!!!!", "1", "", "xyzxyz qwqwqw", "de Staten Generaal heeft een resolutie van brief");
6. `test_default_keep_excludes_only_noise`;
7. `test_short_complete_note_is_not_noise` ("Ontvangen en geresumeerd", "zie resolutie van den Staten Generaal");
8. `test_bare_folio_number_is_exempted_and_kept` ("241": `numeric_content_exempt`, not `low_alpha_content`, not noise);
9. `test_letter_only_low_alpha_region_is_not_exempted` ("!!@@##$$");
10. `test_numeric_exempt_does_not_waive_long_nonalpha_run` ("50:00:08": exempt, `long_nonalpha_run`, noise);
11. `test_no_http_client_is_imported`.

New (12–18):
12. `test_quality_frame_reconciles` (row count and order kept, class counts sum to n);
13. `test_quality_frame_empty`;
14. `test_thresholds_round_trip_and_unknown_key`;
15. `test_bbox_features` (`None` → nan, `h=0` → inf, area = w·h);
16. `test_gold_rows_reproduce_lota_after` (300/300; class and rules equal, \|Δscore\| ≤ 1e-9);
17. `test_gold_rows_reproduce_lota_before`, with `QualityThresholds(digit_vs_punct_fraction_for_numeric_exempt=math.inf)`;
18. `test_gold_metrics` (the numbers in criterion 2).

Commands: AS `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_analysis_region_quality.py`,
then the full suite (1,123 collected before this step). LotA (from `L`): `make test` (368 before,
369 after) and `make lint`.

## Acceptance criteria

1. Fixture: `build_fixture.py`, run from `L` with LotA's venv, writes `gold_rows.tsv` (300 rows:
   `sheet` (`02`/`02c`), `row_no`, `split`, `text`, `gold_label`, then
   `lota_class/score/rules_after` from the `-after` files and `…_before` from the `-before` files;
   `csv.QUOTE_NONE`, as the source). It also writes `lexicon_subset.txt`: every
   `lexicon_lookup_form` of a gold token that is in the lexicon, sorted. **Expect 2,837 terms**
   (measured at planning: 3,789 cleaned tokens, 2,837 in the lexicon). Assert that each gold
   `text` equals the scored file's `text`, row by row.
2. AS metrics from AS predictions on the fixture (`metrics.py`; keep = usable or fragment), all
   measured from the stored files at planning:

   | View | Before (∞ exemption) | After (defaults) |
   |---|---|---|
   | sheet 02, held-out 75: keep P / R | 0.973 / 0.973 (report/05: 0.97/0.97) | 0.973 / 0.986 |
   | combined held-out 150: keep/drop acc. | 0.807 | 0.893 (report/09) |
   | combined held-out 150: 3-way acc. | 0.727 | 0.820 (report/09) |

3. LotA after Part B, run from `L`:
   - `lota_check.py` (a copy of `gold_check.py` that calls the LotA wrappers and patches
     `THRESHOLDS`, as now) gives `mismatches 0 of 150` four times;
   - `planning/iq_check.py 20261021 200` gives `mismatches []` over 200 inventories and 428,544
     regions (planning: 37 s).
4. AS full suite green (1,123 + your new tests). LotA `make test` 369 passed, `make lint` clean.
5. The protected-output check is unchanged.

The user decides when the step is done.

## Error analysis

On the AS predictions (defaults) for the **combined held-out 150**:
- read **every** keep/drop error (about 16) and **10** usable↔fragment confusions (all, if there
  are fewer; seed 20261021 if there are more);
- classify each into: `bare number / folio`, `abbreviation or proper-name list`,
  `HTR garble`, `short complete phrase`, `gold label debatable`, other (name it);
- give the rules fired, and quote the text, shortened to 60 chars.

The conclusion says which rule pairs cause the remaining errors. Compare with LotA Rule 8: the
`1-3`-token band over-predicts noise about 3×, mainly through `low_alpha_content` +
`short_region`. Say whether the retune fixed that within the held-out half. This is not tuning:
change no threshold.

Findings to expect:
- F08-16.1: the port reproduces 600/600 rows;
- F08-16.2: the report numbers recomputed;
- F08-16.3: the categories of the remaining errors.

## Do not touch

`text_layer/`, `stream_analysis/`, LotA files other than those listed, LotA `gold/` and
`output/`.

## When done

Follow `plan/templates/STEP.md` *When done*: the report section `### Step 16: Region-quality
filter (C16)` and its findings, then `make reports`; a `PROGRESS.md` block; overwrite `STATUS.md`.
