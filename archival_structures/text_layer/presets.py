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

PRESETS = {"voc_v1": VOC_V1}


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
