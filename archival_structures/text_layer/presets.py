"""Layer presets and the throughput model (step 15). The only module holding VOC constants."""

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
SG_HAND_TYPES_PARAMS = dict(SG_TYPE_PATTERNS)
SG_PRINT_TYPES_PARAMS = {**SG_TYPE_PATTERNS, "column_override": True}   # F06-15e.8


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
    layout_params=SG_LAYOUT_PARAMS,
    types_params=SG_PRINT_TYPES_PARAMS,
    unit_params=SG_PRINT_UNIT_PARAMS,
    wbc_sanity_in=(),
    wbc_pinned={"default": "-=„"},
)

PRESETS = {"voc_v1": VOC_V1, "sg_1.01.02_v2": SG_HAND_V2, "sg_1.10.94_v2": SG_PRINT_V2}


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
