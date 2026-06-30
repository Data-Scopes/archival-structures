"""
Auto-tag a random sample of images using a vision-language model.

Sends each image to the Anthropic API and asks for a structured JSON
description of document features. The tags are used to:

  - Derive a vocabulary before creating your annotation schema
  - Pre-populate labels in the ground truth pipeline

Set the ANTHROPIC_API_KEY environment variable before running.
"""

import base64
import json
import logging
import random
import time
from pathlib import Path

import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request
from PIL import Image
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Structured prompt asking the model to return valid JSON
TAGGING_PROMPT = """Analyse this archival document image and return ONLY a JSON object (no markdown, no explanation) with the following fields:

{
  "document_type": one of ["letter", "form", "table", "photograph", "map", "certificate", "invoice", "register", "newspaper", "handwritten_note", "printed_text", "mixed", "other"],
  "writing_mode": one of ["handwritten", "typed", "printed", "mixed", "none"],
  "language_script": brief description, e.g. "Latin", "Arabic", "unclear",
  "has_stamp": true/false,
  "has_signature": true/false,
  "has_table": true/false,
  "has_ruled_lines": true/false,
  "has_logo_or_letterhead": true/false,
  "has_photograph_or_illustration": true/false,
  "has_marginalia": true/false,
  "colour_profile": one of ["colour", "grayscale", "sepia", "black_and_white"],
  "condition": one of ["good", "faded", "damaged", "heavily_damaged"],
  "approximate_text_density": one of ["none", "sparse", "moderate", "dense"],
  "orientation": one of ["portrait", "landscape", "square"],
  "notes": short free-text observations (max 20 words)
}

Respond with the JSON object only."""


def _image_to_base64(path: Path, max_dim: int = 1024) -> tuple[str, str]:
    """
    Load image, optionally downscale to max_dim on the long edge,
    and return (base64_string, media_type).
    """
    img = Image.open(path).convert("RGB")
    # Don't upscale; only shrink if larger than max_dim
    w, h = img.size
    if max(w, h) > max_dim:
        scale = max_dim / max(w, h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    import io
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.standard_b64encode(buf.getvalue()).decode(), "image/jpeg"


def _parse_json_response(raw: str) -> dict:
    """Parse a JSON response, stripping markdown code fences if present."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def _tag_image(client: anthropic.Anthropic, path: Path, model: str) -> dict:
    """Call the API for a single image and parse the JSON response."""
    b64, media_type = _image_to_base64(path)

    message = client.messages.create(
        model=model,
        max_tokens=512,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": TAGGING_PROMPT},
                ],
            }
        ],
    )

    return _parse_json_response(message.content[0].text)


def _build_batch_request(path: Path, model: str) -> Request:
    """Build a single Batches API request for one image."""
    b64, media_type = _image_to_base64(path)
    return Request(
        custom_id=str(path),
        params=MessageCreateParamsNonStreaming(
            model=model,
            max_tokens=512,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": b64,
                            },
                        },
                        {"type": "text", "text": TAGGING_PROMPT},
                    ],
                }
            ],
        ),
    )


def run_vlm_tagging(
    config: dict, image_ids: list[str], use_batches: bool = False
) -> dict[str, dict]:
    """
    Tag a random sample of images and cache results.

    Args:
        config:      pipeline config dict (from AnalysisConfig.to_dict())
        image_ids:   full list of image IDs (paths) from embeddings step
        use_batches: if True, use the Batches API (async, ~50% cheaper, results
                     available within ~1 hour); if False, use the standard API
                     (synchronous, results available immediately).

    Returns dict mapping image_id → tag dict.
    """
    results_file = config["vlm_tagging"]["results_file"]

    if Path(results_file).exists():
        logger.info("Loading cached VLM tags from disk")
        with open(results_file) as f:
            return json.load(f)

    sample_size = min(config["vlm_tagging"]["sample_size"], len(image_ids))
    sampled_ids = random.sample(image_ids, sample_size)
    model = config["vlm_tagging"]["model"]
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

    if use_batches:
        results = _run_vlm_tagging_batches(client, sampled_ids, model, results_file)
    else:
        results = _run_vlm_tagging_sequential(client, sampled_ids, model, results_file)

    return results


def _run_vlm_tagging_sequential(
    client: anthropic.Anthropic,
    sampled_ids: list[str],
    model: str,
    results_file: str,
) -> dict[str, dict]:
    """Tag images one at a time using the standard Messages API."""
    results = {}
    errors = 0

    for img_id in tqdm(sampled_ids, desc="VLM tagging"):
        path = Path(img_id)
        if not path.exists():
            logger.warning(f"Image not found: {img_id}")
            continue
        try:
            tags = _tag_image(client, path, model)
            results[img_id] = tags
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error for {img_id}: {e}")
            errors += 1
        except anthropic.RateLimitError:
            logger.warning("Rate limit hit — sleeping 60 s")
            time.sleep(60)
            try:
                tags = _tag_image(client, path, model)
                results[img_id] = tags
            except Exception:
                errors += 1
        except Exception as e:
            logger.warning(f"Error tagging {img_id}: {e}")
            errors += 1
        time.sleep(0.3)

    logger.info(
        f"Tagged {len(results)}/{len(sampled_ids)} images "
        f"({errors} errors) → {results_file}"
    )
    _save_results(results, results_file)
    return results


def _run_vlm_tagging_batches(
    client: anthropic.Anthropic,
    sampled_ids: list[str],
    model: str,
    results_file: str,
) -> dict[str, dict]:
    """Tag images using the Batches API (~50% cheaper, async, ~1 hour turnaround)."""
    batch_id_file = Path(results_file).parent / "batch_id.txt"

    # Reuse an in-progress batch if we were interrupted mid-poll
    if batch_id_file.exists():
        batch_id = batch_id_file.read_text().strip()
        logger.info(f"Resuming existing batch {batch_id}")
    else:
        requests = []
        for img_id in sampled_ids:
            path = Path(img_id)
            if not path.exists():
                logger.warning(f"Image not found: {img_id}")
                continue
            requests.append(_build_batch_request(path, model))

        batch = client.messages.batches.create(requests=requests)
        batch_id = batch.id
        batch_id_file.parent.mkdir(parents=True, exist_ok=True)
        batch_id_file.write_text(batch_id)
        logger.info(f"Submitted batch {batch_id} ({len(requests)} images)")

    # Poll until complete
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        counts = batch.request_counts
        logger.info(
            f"Batch {batch_id}: {counts.processing} processing, "
            f"{counts.succeeded} succeeded, {counts.errored} errored"
        )
        if batch.processing_status == "ended":
            break
        time.sleep(60)

    # Collect results
    results = {}
    errors = 0
    for result in client.messages.batches.results(batch_id):
        if result.result.type == "succeeded":
            raw = result.result.message.content[0].text
            try:
                results[result.custom_id] = _parse_json_response(raw)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error for {result.custom_id}: {e}")
                errors += 1
        else:
            logger.warning(f"Batch item failed: {result.custom_id} ({result.result.type})")
            errors += 1

    logger.info(
        f"Tagged {len(results)}/{len(sampled_ids)} images "
        f"({errors} errors) → {results_file}"
    )
    _save_results(results, results_file)
    batch_id_file.unlink(missing_ok=True)
    return results


def _save_results(results: dict, results_file: str) -> None:
    Path(results_file).parent.mkdir(parents=True, exist_ok=True)
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)


def summarise_vlm_tags(vlm_tags: dict[str, dict]) -> dict:
    """
    Aggregate tag counts across all tagged images.
    Useful for understanding the vocabulary before building your label schema.
    """
    from collections import Counter

    summary = {}
    categorical_fields = [
        "document_type", "writing_mode", "colour_profile",
        "condition", "approximate_text_density", "orientation",
        "language_script",
    ]
    boolean_fields = [
        "has_stamp", "has_signature", "has_table", "has_ruled_lines",
        "has_logo_or_letterhead", "has_photograph_or_illustration", "has_marginalia",
    ]

    for field in categorical_fields:
        counter = Counter(
            tags[field] for tags in vlm_tags.values() if field in tags
        )
        summary[field] = dict(counter.most_common())

    for field in boolean_fields:
        true_count = sum(
            1 for tags in vlm_tags.values() if tags.get(field) is True
        )
        summary[field] = {
            "true": true_count,
            "false": len(vlm_tags) - true_count,
        }

    return summary
