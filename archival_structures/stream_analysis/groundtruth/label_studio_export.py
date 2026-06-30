"""
Export sampled images to Label Studio import format.

Label Studio expects a JSON array where each element describes one task.
If VLM tags are provided, they are embedded as pre-annotations so
annotators only need to correct rather than create from scratch.

Import instructions
-------------------
1. Create a new Label Studio project.
2. In Settings → Labelling Interface, paste the XML template printed by
   `print_label_config()` (or customise it for your label schema).
3. In the project, click Import → Upload the exported JSON file.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Edit this dict to match your finalised label schema.
# Keys are the Label Studio "from_name" identifiers.
# Values are the choice lists shown to annotators.
# ---------------------------------------------------------------------------
LABEL_CHOICES = {
    "document_type": [
        "letter", "form", "table", "photograph", "map",
        "certificate", "invoice", "register", "newspaper",
        "handwritten_note", "printed_text", "mixed", "other",
    ],
    "writing_mode": ["handwritten", "typed", "printed", "mixed", "none"],
    "colour_profile": ["colour", "grayscale", "sepia", "black_and_white"],
    "condition": ["good", "faded", "damaged", "heavily_damaged"],
    "approximate_text_density": ["none", "sparse", "moderate", "dense"],
    "orientation": ["portrait", "landscape", "square"],
}

BOOLEAN_LABELS = [
    "has_stamp",
    "has_signature",
    "has_table",
    "has_ruled_lines",
    "has_logo_or_letterhead",
    "has_photograph_or_illustration",
]


def print_label_config() -> None:
    """
    Print a Label Studio XML labelling configuration.
    Paste this into Settings → Labelling Interface when setting up your project.
    """
    choices_xml = ""
    for from_name, choices in LABEL_CHOICES.items():
        options = "\n    ".join(f'<Choice value="{c}"/>' for c in choices)
        choices_xml += f"""
  <Choices name="{from_name}" toName="image" choice="single" showInLine="true">
    <Header value="{from_name.replace('_', ' ').title()}"/>
    {options}
  </Choices>
"""

    bool_xml = ""
    for field in BOOLEAN_LABELS:
        bool_xml += f"""
  <Choices name="{field}" toName="image" choice="single" showInLine="true">
    <Header value="{field.replace('_', ' ').title()}"/>
    <Choice value="yes"/>
    <Choice value="no"/>
  </Choices>
"""

    config = f"""<View>
  <Image name="image" value="$image" zoom="true"/>
{choices_xml}{bool_xml}
</View>"""
    print(config)


# ---------------------------------------------------------------------------
# Export functions
# ---------------------------------------------------------------------------

def _vlm_tag_to_predictions(tag: dict) -> list[dict]:
    """Convert a VLM tag dict into Label Studio prediction result items."""
    results = []

    for from_name in LABEL_CHOICES:
        value = tag.get(from_name)
        if value and value in LABEL_CHOICES[from_name]:
            results.append({
                "from_name": from_name,
                "to_name": "image",
                "type": "choices",
                "value": {"choices": [value]},
            })

    for field in BOOLEAN_LABELS:
        value = tag.get(field)
        if value is not None:
            results.append({
                "from_name": field,
                "to_name": "image",
                "type": "choices",
                "value": {"choices": ["yes" if value else "no"]},
            })

    return results


def build_label_studio_tasks(
    sampled_ids: list[str],
    vlm_tags: dict[str, dict] | None = None,
    cluster_assignment: dict[str, int] | None = None,
) -> list[dict]:
    """
    Build the Label Studio task list.

    Args:
        sampled_ids:        flat list of image paths to annotate
        vlm_tags:           optional pre-computed VLM tags
        cluster_assignment: optional dict mapping image_id → cluster_id
                            (stored as metadata for review convenience)

    Returns list of Label Studio task dicts.
    """
    tasks = []

    for img_id in sampled_ids:
        task: dict = {
            "data": {
                "image": img_id,
                "image_id": img_id,
                "cluster": cluster_assignment.get(img_id, -1) if cluster_assignment else -1,
            },
            "annotations": [],
            "predictions": [],
        }

        # Embed VLM tags as pre-annotations so annotators only need to correct
        if vlm_tags and img_id in vlm_tags:
            prediction_results = _vlm_tag_to_predictions(vlm_tags[img_id])
            if prediction_results:
                task["predictions"].append({
                    "model_version": "vlm_pretag",
                    "score": 0.8,
                    "result": prediction_results,
                })
            if "notes" in vlm_tags[img_id]:
                task["data"]["vlm_notes"] = vlm_tags[img_id]["notes"]

        tasks.append(task)

    return tasks


def export_label_studio(
    sampled: dict[int, list[str]],
    config: dict,
    vlm_tags: dict[str, dict] | None = None,
) -> str:
    """
    Build and save the Label Studio import file.

    Args:
        sampled:   cluster_id → [image_id, ...] dict from stratified_sampling
        config:    pipeline config dict (from AnalysisConfig.to_dict())
        vlm_tags:  optional VLM tag dict

    Returns the output file path.
    """
    cluster_assignment: dict[str, int] = {}
    for cluster_id, ids in sampled.items():
        for img_id in ids:
            cluster_assignment[img_id] = cluster_id

    flat_ids = [img_id for ids in sampled.values() for img_id in ids]
    tasks = build_label_studio_tasks(flat_ids, vlm_tags, cluster_assignment)

    out_path = config["groundtruth"]["label_studio_export"]
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(tasks, f, indent=2)

    pre_tagged = sum(1 for t in tasks if t["predictions"])
    logger.info(
        f"Label Studio export: {len(tasks)} tasks "
        f"({pre_tagged} with VLM pre-annotations) → {out_path}"
    )
    return out_path
