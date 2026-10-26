"""Layer presets and the throughput model (step 15). The only module holding VOC constants."""

import dataclasses
import heapq
from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(frozen=True)
class LayerPreset:
    name: str                      # layer directory name under --out-dir
    untagged_tag: str
    scan_id_prefix: Optional[str]  # e.g. "NL-HaNA_1.04.02"; None = any NA-style prefix
    throughput_fit: Optional[Mapping[str, Mapping[str, float]]] = None
    output_bytes_per_xml_byte: Optional[float] = None
    description: str = ""
    # v2 (phase 06); the defaults leave VOC_V1 unchanged
    format_version: int = 1
    archive_id: Optional[str] = None
    source_pattern: Optional[str] = None   # regex with named group `inv`, fullmatch on entry names
    skip_resource_forks: bool = False
    merged_line_split: str = "off"         # "on": step 15e splits merged print lines
    split_region_tags: tuple = ()          # region tags whose lines take part in the split
    word_split: str = "off"                # "on": step 15f glued-word repair (stage wordfix)
    word_split_params: Optional[Mapping] = None   # WordSplitParams fields
    layout_params: Optional[Mapping] = None       # LayoutParams fields (stage layout, step 15fq)
    types_params: Optional[Mapping] = None        # TypeParams fields (stage types, step 15g)
    unit_params: Optional[Mapping] = None         # UnitParams fields (stage units, step 15h)
    # step 15hj: word-break decision (stage wbc); the defaults equal the v1 constants
    word_break_rule: Optional[Mapping] = None     # None = word_breaks.DEFAULT_WORD_BREAK_RULE
    word_break_stat: str = "default"              # "robust": end_rel_frac from the "„"-robust frequency
    wbc_sanity_in: tuple = ("¬",)                 # chars that must be word breaks on pooled counts
    wbc_sanity_out: tuple = (":", ".")            # chars that must not be
    # step 15hj2: pinned sets {"default": chars, "<inv>": chars}; None = derive with the rule
    wbc_pinned: Optional[Mapping[str, str]] = None
    # step 15ir: characters dropped from the start of a line joined onto a break (when a letter
    # follows): the HTR reads a repeated break mark as "," in 11182 (v2 join only)
    join_lead_marks: str = ""
    # step 15l: per inventory (key an inventory or "default"), characters dropped before a lower-case
    # letter at the start of a joined line (a repeated break mark read as a letter, e.g. "J")
    join_lead_marks_lower: Optional[Mapping[str, str]] = None
    # step 15l: refuse a merge whose right side starts upper-case (boundary kind g_upper_right)
    join_upper_guard: bool = False
    # step 15v (phase 12): v4 presets. mode_presets maps "hand"/"print" to the preset whose rule fields
    # (MODE_RULE_FIELDS) an inventory uses; volume_mode says how the mode of an inventory is decided
    mode_presets: Optional[Mapping[str, str]] = None
    volume_mode: Optional[Mapping] = None
    # step 15x: per mode, rule fields (MODE_RULE_FIELDS) that replace those of the mode preset
    mode_overrides: Optional[Mapping[str, Mapping]] = None


# seconds = intercept + slope * uncompressed PageXML bytes, per inventory and
# per stage, per worker. Fitted on the 27 local inventories (report/74 §6).
# Measured with 8 workers on a 10-core Apple-silicon laptop (extract and wbc
# also on 5 workers for LotA's 5 extra inventories); R^2 0.98 / 0.98 / 0.96.
_VOC_THROUGHPUT_FIT = {
    "extract": {"intercept_s": 0.4359, "s_per_mb": 0.09122},
    "wbc": {"intercept_s": 0.0046, "s_per_mb": 0.00382},
    "join": {"intercept_s": 0.0, "s_per_mb": 0.00977},
}

VOC_V1 = LayerPreset(
    name="voc_text_layer_v1",
    untagged_tag="unclassified",
    scan_id_prefix="NL-HaNA_1.04.02",
    throughput_fit=_VOC_THROUGHPUT_FIT,
    # gzip bytes of lines/ + regions/ per uncompressed PageXML byte (report/74 §6)
    output_bytes_per_xml_byte=0.0233,
    description=("GLOBALISE VOC PageXML v2.1; throughput fitted on one 10-core laptop, "
                 "report/74 section 6"),
)

def _sg_source_pattern(archive):
    return (rf"NL-HaNA_{archive.replace('.', chr(92) + '.')}_(?P<inv>[0-9]+[A-Z]*)"
            r"(?:-pagexml)?(?:\.zip|\.tar\.gz|\.tgz)?")


_MONTHS = (r"(jan|feb|febr|maa|maart|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|"
           r"sep|sept|oct|okt|nov|dec)")
SG_FURNITURE_PATTERNS = (
    r"\W*(fol\.?|pag\.?|p\.)?\s*\d{1,4}\s*\W*",                          # page number "( 93", "383."
    r"\W*(den\s+)?\d{1,2}\s*\.?\s*" + _MONTHS + r"\w*\.?\W*(\d{4}\W*)?",   # "Den 26. Maart.", "1. Meij"
    r"\W*1[67]\d\d\W*",                                                    # running year "1717."
)
SG_LAYOUT_PARAMS = {"furniture_patterns": SG_FURNITURE_PATTERNS}
# Step 15j: print also sorts the lines of unchanged regions by rows (hand keeps XML order).
SG_PRINT_LAYOUT_PARAMS = {**SG_LAYOUT_PARAMS, "sort_unchanged_lines": True}
# Step 15fq: hand merge precision 12/23 (0.52) < 0.90 in the error analysis, so the hand layer
# runs without region merges (the brief's fallback); print keeps them (28/30).
SG_HAND_LAYOUT_PARAMS = {**SG_LAYOUT_PARAMS, "merge": False}

# Step 15g: type patterns (see docs/text_layer.md, Type tables).
_TYPE_MONTHS = (r"(jan|feb|febr|maa|maart|maert|mart|apr|april|mei|mey|meij|jun|junij|jul|julij|aug|augti|"
                r"augt|sep|sept|oct|okt|nov|dec)")
_TYPE_WEEKDAY = (r"((lun|maen|maan|mart|dins|merc|woens|jov|donder|vene|vry|vrid|sabb|sat|zat|domin|"
                 r"solis|sond|zond)\w*\.?,?\s+)?")
SG_TYPE_PATTERNS = {
    "pn_line": r"\W*(fol\.?|pag\.?|p\.)?\s*\(?\s*\d{1,4}\s*[.)]?\W*",
    "pn_suffix": r"\s\(?\s*(\d{1,4})\s*[.)]?\s*$",
    "date_line": (r"\W*" + _TYPE_WEEKDAY + r"(den\s+)?\d{1,2}\s*[.:]?\s*" + _TYPE_MONTHS
                  + r"\w*\s*[.:,]?\W*(1[67]\d\d\W*)?"),
    "date_prefix": (r"\W*(den\s+)?\d{1,2}\s*[.:]?\s*" + _TYPE_MONTHS + r"\w*\s*[.:]?(?=\s+\S)"),
    "month_only": r"\W*\d{0,2}\s*[.:]?\s*" + _TYPE_MONTHS + r"\w*\s*[.:]?\W*",
    "year_line": r"\W*1[67]\d\d\W*",
    "signature": r"[A-Z]{1,3}\s*\d{0,2}|\d\s*[A-Z]{1,3}",
}
# Step 15k, decision P6 (verdicts: reports/data/phase-09-text-layer-v3-prep/step-15k/pn_verdicts.tsv).
# Bars: a band is retyped iff >= 16 of its 20 sampled off-chain numbers are not page numbers;
# years iff >= 30 of 37.
SG_HAND_TYPES_PARAMS = {
    **SG_TYPE_PATTERNS,
    "offchain_retype": True,
    "offchain_other_bands": (),          # hand-top 6/20 not_pn, hand-bottom 12/20 not_pn -> kept
    "offchain_years_other": True,        # years 37/37 not_pn -> other
}
SG_PRINT_TYPES_PARAMS = {
    **SG_TYPE_PATTERNS, "column_override": True,   # F06-15e.8
    "offchain_retype": True,
    "offchain_other_bands": ("bottom",),  # print-top 3/20 not_pn -> kept; print-bottom 19/20 not_pn -> other
    "offchain_years_other": True,        # years 37/37 not_pn -> other
}


SG_HAND_V2 = LayerPreset(
    name="NL-HaNA_1.01.02_text_layer_v2",
    untagged_tag="untagged",
    scan_id_prefix="NL-HaNA_1.01.02",
    description="States General hand volumes (NL-HaNA 1.01.02), text layer v2 (lines with geometry)",
    format_version=2,
    archive_id="NL-HaNA_1.01.02",
    source_pattern=_sg_source_pattern("1.01.02"),
    skip_resource_forks=True,
    merged_line_split="off",
    word_split="off",
    word_split_params={"min_token_len": 8, "min_part_count": 20, "split_penalty": 0},
    layout_params=SG_HAND_LAYOUT_PARAMS,
    types_params=SG_HAND_TYPES_PARAMS,
    wbc_sanity_in=(),
    # UA-011 (b), F07-15hj2: `=` is dropped in the hand_tar inventories whose derived set lacks it
    # (3349, 3350, 3351, 3353, 3354); 3352 derived `=„` and keeps the default.
    wbc_pinned={"default": "-=„", "11281": "-:=„", "3349": "-„", "3350": "-„", "3351": "-„",
                "3353": "-„", "3354": "-„"},
    join_lead_marks=",",   # F07-R.2; 15l: also `,,` and `, ` (comma bar: 67/70 mark, >= 63 -> kept)
    # 15l bars: upper-case merges wrong 24/40 (< 27) -> join_upper_guard stays False
)

# Step 15h: paragraph cues fitted on the dev split (455, even scans) against REPUBLIC paragraphs;
# dev F1 0.784, test F1 0.698 (reported once). See reports/phase-07-sg-units-and-pilot.md.
SG_PRINT_UNIT_PARAMS = {"indent_frac": 0.5, "short_frac": 0.1, "gap_factor": None}

SG_PRINT_V2 = LayerPreset(
    name="NL-HaNA_1.10.94_text_layer_v2",
    untagged_tag="untagged",
    scan_id_prefix="NL-HaNA_1.10.94",
    description="States General print volumes (NL-HaNA 1.10.94), text layer v2 (lines with geometry)",
    format_version=2,
    archive_id="NL-HaNA_1.10.94",
    source_pattern=_sg_source_pattern("1.10.94"),
    skip_resource_forks=True,
    merged_line_split="on",
    split_region_tags=("index", "resolution", "untagged", "Text"),
    word_split="off",
    word_split_params={"min_token_len": 8, "min_part_count": 20, "split_penalty": 0},
    layout_params=SG_PRINT_LAYOUT_PARAMS,
    types_params=SG_PRINT_TYPES_PARAMS,
    unit_params=SG_PRINT_UNIT_PARAMS,
    wbc_sanity_in=(),
    wbc_pinned={"default": "-=„"},
    # 15l bars: 455 J+lower 30/30 mark (>= 27) -> pinned for 455 only; 456 (2/32 mark, real words) not
    join_lead_marks_lower={"455": "J"},
    # 15l bar: upper-case merges wrong 20/40 (< 27) -> join_upper_guard stays False
)

# Step 15m: the names the full SG run (text_layer_v3) uses. Same code path and parameters as v2
# (format_version stays 2); only `name` and `description` differ.
SG_HAND_V3 = dataclasses.replace(SG_HAND_V2, name="NL-HaNA_1.01.02_text_layer_v3",
                                 description=SG_HAND_V2.description.replace("v2", "v3"))
SG_PRINT_V3 = dataclasses.replace(SG_PRINT_V2, name="NL-HaNA_1.10.94_text_layer_v3",
                                  description=SG_PRINT_V2.description.replace("v2", "v3"))

# Step 15v (phase 12): v4 presets. One hand or print mode per inventory, decided by the char-weighted
# median line-box height of the volume (hand_print.box_height_mode); the rules of the v3 presets.
BOX_HEIGHT_V4 = {"cut": 87.75, "min_region_chars": 100}   # step 15u, cut.json sha256 ce58c333...ccb572; F12-15u.1
# Phase-12 review (F12-R.4, P16): the 1.10.94 merged-line splitter assumes two text columns per page side. A
# 1.01.02 print-mode volume is split only if its column-start share (modes.column_start_share) is >= min_share.
SPLIT_GATE_V4 = {"min_share": 0.35, "min_chars": 20, "min_rows": 50, "mid_frac": 0.4}
MODE_RULE_FIELDS = ("merged_line_split", "split_region_tags", "layout_params", "types_params",
                    "unit_params", "join_lead_marks")
_V4_MODE_PRESETS = {"hand": "sg_1.01.02_v3", "print": "sg_1.10.94_v3"}
SG_1_01_02_V4 = dataclasses.replace(
    SG_HAND_V3, name="NL-HaNA_1.01.02_text_layer_v4",
    description="NL-HaNA 1.01.02 text layer v4 (one hand/print mode per volume, by line-box height)",
    mode_presets=_V4_MODE_PRESETS,
    volume_mode={"method": "box_height", "params": BOX_HEIGHT_V4, "split_gate": SPLIT_GATE_V4},
    # UA-026 (b), F12-R.6, P18: in printed 1.01.02 volumes an indent-only cut within 2 main lines of the last
    # kept cut is a line beside a dropped initial, not a paragraph start
    mode_overrides={"print": {"unit_params": {**SG_PRINT_UNIT_PARAMS, "drop_cap_lines": 2}}})
SG_1_10_94_V4 = dataclasses.replace(
    SG_PRINT_V3, name="NL-HaNA_1.10.94_text_layer_v4",
    description="NL-HaNA 1.10.94 text layer v4 (print mode fixed; box height recorded)",
    mode_presets=_V4_MODE_PRESETS,
    volume_mode={"method": "fixed", "mode": "print", "params": BOX_HEIGHT_V4})  # phase-10 P11, phase-12 P7

PRESETS = {"voc_v1": VOC_V1, "sg_1.01.02_v2": SG_HAND_V2, "sg_1.10.94_v2": SG_PRINT_V2,
           "sg_1.01.02_v3": SG_HAND_V3, "sg_1.10.94_v3": SG_PRINT_V3,
           "sg_1.01.02_v4": SG_1_01_02_V4, "sg_1.10.94_v4": SG_1_10_94_V4}


def inventory_preset(preset, mode):
    """The preset an inventory is built with: `preset` with the rule fields of its `mode`."""
    if mode not in ("hand", "print"):
        raise ValueError(f"mode must be 'hand' or 'print', got {mode!r}")
    if preset.mode_presets is None:
        return preset
    src = PRESETS[preset.mode_presets[mode]]
    fields = {f: getattr(src, f) for f in MODE_RULE_FIELDS}
    override = (preset.mode_overrides or {}).get(mode, {})
    bad = sorted(set(override) - set(MODE_RULE_FIELDS))
    if bad:
        raise ValueError(f"mode override outside MODE_RULE_FIELDS: {bad}")
    fields.update(override)
    return dataclasses.replace(preset, **fields, mode_presets=None, volume_mode=None, mode_overrides=None)


def predict_seconds(xml_bytes, stage, fit):
    stage_fit = fit[stage]
    return max(0.0, stage_fit["intercept_s"] + stage_fit["s_per_mb"] * xml_bytes / 1e6)


def predict_wall(per_inventory_xml_bytes, workers, fit, stages=("extract", "wbc", "join")):
    """Wall time with longest-first scheduling on `workers` processes."""
    total = 0.0
    for stage in stages:
        jobs = sorted((predict_seconds(b, stage, fit) for b in per_inventory_xml_bytes),
                      reverse=True)
        heap = [0.0] * max(1, workers)
        for job in jobs:
            heapq.heapreplace(heap, heap[0] + job)
        total += max(heap)
    return total
