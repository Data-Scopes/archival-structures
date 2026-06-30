"""
Extract visual embeddings from document images using CLIP or DINOv2.

DINOv2 is recommended: it learns richer visual representations without
needing text supervision, which makes it better at capturing visual
similarity across diverse document types.

Embeddings are cached to disk to avoid re-running on subsequent runs.
"""

import json
import logging
from pathlib import Path

import numpy as np
import torch
from PIL import Image
from tqdm import tqdm
from transformers import AutoImageProcessor, AutoModel, CLIPModel, CLIPProcessor

from archival_structures.stream_analysis.utils.image_loader import (
    collect_image_paths,
    paths_to_ids,
    save_image_ids,
)

logger = logging.getLogger(__name__)


def _get_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


# ---------------------------------------------------------------------------
# Model loaders
# ---------------------------------------------------------------------------

def _load_dinov2(model_name: str, device: torch.device):
    logger.info(f"Loading DINOv2 model: {model_name}")
    processor = AutoImageProcessor.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(device).eval()
    return model, processor


def _load_clip(model_name: str, device: torch.device):
    logger.info(f"Loading CLIP model: {model_name}")
    processor = CLIPProcessor.from_pretrained(model_name)
    model = CLIPModel.from_pretrained(model_name).to(device).eval()
    return model, processor


# ---------------------------------------------------------------------------
# Embedding extraction
# ---------------------------------------------------------------------------

def _embed_batch_dinov2(
    images: list[Image.Image],
    model,
    processor,
    device: torch.device,
) -> np.ndarray:
    inputs = processor(images=images, return_tensors="pt").to(device)
    with torch.no_grad():
        outputs = model(**inputs)
    # CLS token from the last hidden state
    return outputs.last_hidden_state[:, 0].cpu().numpy()


def _embed_batch_clip(
    images: list[Image.Image],
    model,
    processor,
    device: torch.device,
) -> np.ndarray:
    inputs = processor(images=images, return_tensors="pt", padding=True).to(device)
    with torch.no_grad():
        features = model.get_image_features(**inputs)
    return features.cpu().numpy()


def extract_embeddings(config: dict) -> tuple[np.ndarray, list[str]]:
    """
    Extract embeddings for all images and cache them.

    Returns:
        embeddings: float32 array of shape (N, D)
        image_ids:  list of N file path strings
    """
    cache_file = config["embeddings"]["cache_file"]
    ids_file = config["embeddings"]["image_ids_file"]

    # Return cached results if available
    if Path(cache_file).exists() and Path(ids_file).exists():
        logger.info("Loading cached embeddings from disk")
        embeddings = np.load(cache_file)
        with open(ids_file) as f:
            image_ids = json.load(f)
        logger.info(f"Loaded {len(image_ids)} embeddings of dimension {embeddings.shape[1]}")
        return embeddings, image_ids

    # Collect images
    paths = collect_image_paths(
        config["images"]["input_dir"],
        config["images"]["extensions"],
    )
    if not paths:
        raise FileNotFoundError(f"No images found in {config['images']['input_dir']}")

    device = _get_device()
    logger.info(f"Using device: {device}")
    batch_size = config["images"]["batch_size"]
    model_type = config["embeddings"]["model"]

    if model_type == "dinov2":
        model, processor = _load_dinov2(config["embeddings"]["dinov2_model"], device)
        embed_fn = _embed_batch_dinov2
    elif model_type == "clip":
        model, processor = _load_clip(config["embeddings"]["clip_model"], device)
        embed_fn = _embed_batch_clip
    else:
        raise ValueError(f"Unknown model type: {model_type}. Choose 'clip' or 'dinov2'.")

    all_embeddings = []
    valid_ids = []

    for i in tqdm(range(0, len(paths), batch_size), desc="Extracting embeddings"):
        batch_paths = paths[i : i + batch_size]
        batch_images = []
        batch_valid_paths = []

        for p in batch_paths:
            try:
                img = Image.open(p).convert("RGB")
                batch_images.append(img)
                batch_valid_paths.append(p)
            except Exception as e:
                logger.warning(f"Skipping {p}: {e}")

        if not batch_images:
            continue

        try:
            embs = embed_fn(batch_images, model, processor, device)
            all_embeddings.append(embs)
            valid_ids.extend(paths_to_ids(batch_valid_paths))
        except Exception as e:
            logger.error(f"Embedding batch failed: {e}")

    if not all_embeddings:
        raise RuntimeError("No embeddings extracted. Check image paths and model.")

    embeddings = np.vstack(all_embeddings).astype(np.float32)
    image_ids = valid_ids

    # L2-normalise so cosine distance = euclidean distance in UMAP
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    embeddings = embeddings / np.maximum(norms, 1e-8)

    # Cache to disk
    Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
    np.save(cache_file, embeddings)
    save_image_ids(image_ids, ids_file)

    logger.info(f"Extracted and cached {len(image_ids)} embeddings of dimension {embeddings.shape[1]}")
    return embeddings, image_ids
