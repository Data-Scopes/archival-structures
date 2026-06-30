# Document Image Analysis Pipeline

A two-part pipeline for visually classifying digitised archival document images — photos, forms, tables, handwritten and typed documents, stamps, signatures, letterheads, and more — without any ground truth data to start.

## Overview

**Part 1** gets you an overview of what kinds of documents you have. **Part 2** helps you build a balanced, annotated ground truth dataset efficiently.

```
doc_analysis/
├── config.yaml
├── requirements.txt
├── utils/
│   └── image_loader.py          # shared image loading utilities
├── part1_overview/
│   ├── embeddings.py            # CLIP / DINOv2 visual embeddings
│   ├── layout_analysis.py       # thumbnail-safe layout features (no OCR)
│   ├── clustering.py            # UMAP + HDBSCAN clustering
│   ├── vlm_tagging.py           # VLM auto-tagging via Anthropic API
│   ├── visualization.py         # UMAP plots, cluster grids, distributions
│   └── run.py                   # Part 1 entry point
└── part2_groundtruth/
    ├── stratified_sampling.py   # cluster-stratified image sampling
    ├── active_learning.py       # uncertainty-based annotation suggestions
    ├── label_studio_export.py   # Label Studio JSON export with pre-annotations
    └── run.py                   # Part 2 entry point
```

---

## Setup

```bash
pip install -r requirements.txt
```

Edit `config.yaml` and set `images.input_dir` to the folder containing your images. All other settings have sensible defaults.

For VLM tagging (optional), set your Anthropic API key:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

---

## Part 1 — Getting an overview

```bash
python -m part1_overview.run           # all steps including VLM tagging
python -m part1_overview.run --no-vlm  # skip VLM tagging (no API key needed)
```

### What it does

**Step 1: Visual embeddings.** Every image is passed through DINOv2 (or CLIP, configurable) to produce a high-dimensional vector capturing visual similarity. No labels or text understanding needed — the model learns purely from visual appearance.

**Step 2: Layout features.** A set of thumbnail-safe, OCR-free features is extracted per image (see [Layout analysis on thumbnails](#layout-analysis-on-thumbnails) below).

**Step 3: Clustering.** UMAP reduces the embeddings to 2D, then HDBSCAN finds natural groupings. Images that don't fit any cluster are marked as outliers (label `-1`) — these are often the most unusual and interesting documents. The `combine_layout_features` config option blends layout features into the embedding space before clustering, so clusters reflect both visual appearance and document structure.

**Step 4: VLM tagging.** A random sample of images is sent to Claude with a structured prompt asking for document type, writing mode, presence of stamps/signatures/tables/logos, colour profile, condition, and more. The results build a vocabulary of features before you commit to a labelling schema.

**Step 5: Visualisations.** Saved to `outputs/visualizations/`:
- `umap_clusters.png` — UMAP scatter plot coloured by cluster
- `cluster_NNN_grid.png` — image grid for each cluster (representative samples)
- `layout_distributions.png` — box plots of layout features per cluster
- `vlm_tag_summary.png` — bar charts of all VLM tag fields

### Layout analysis on thumbnails

OCR and HTR are unreliable at thumbnail resolution. The layout analysis module instead extracts structural and statistical signals that work reliably even at 100–200 px:

- **Ink density** — fraction of dark pixels; measures overall content density.
- **Text density** — count of small connected components per thousand pixels. Text produces many small, similarly-sized blobs; photographs produce few large ones. This distinguishes text-heavy pages from image-heavy ones without reading a single character.
- **Estimated line count** — peaks in the horizontal projection profile (sum of dark pixels per row after horizontal dilation). Text lines create a regular rhythm that is detectable even at very low resolution.
- **Line coverage** — fraction of rows that contain content; separates dense paragraphs from sparse forms.
- **Column blocks** — number of distinct vertical content columns; detects multi-column layouts.
- **Ruled lines** — long unbroken horizontal and vertical strokes detected via morphological opening; indicates forms and tables.
- **Local variance** — Laplacian variance normalised by image area; high values indicate rich texture (text-heavy or detailed) vs. blank or photographic regions.
- **Colour profile** — grayscale vs. colour, mean brightness, colourfulness (inter-channel standard deviation).
- **Aspect ratio** — portrait, landscape, or square.

Set `layout_weight` in `config.yaml` higher (e.g. `0.5`) if you want clustering to emphasise physical structure over visual appearance.

---

## Part 2 — Creating a balanced ground truth

### 1. Generate a Label Studio import file

```bash
python -m part2_groundtruth.run --export
```

This samples `samples_per_cluster` images from each cluster (configurable, default 30), ensuring rare document types are represented alongside common ones. If VLM tags exist, already-tagged images are prioritised and embedded as pre-annotations, so annotators correct rather than create from scratch — typically 3–5× faster.

### 2. Set up Label Studio

```bash
python -m part2_groundtruth.run --print-label-config
```

Copy the printed XML into **Settings → Labelling Interface** in your Label Studio project, then import the exported JSON file.

> Label Studio is free and open source: https://labelstud.io

### 3. Annotate

Work through the sampled images in Label Studio. The label schema (defined in `label_studio_export.py` under `LABEL_CHOICES`) covers document type, writing mode, colour profile, condition, text density, orientation, and boolean presence fields for stamps, signatures, tables, ruled lines, logos, and illustrations. Adjust it to match your collection before annotating.

Export your completed annotations from Label Studio as JSON and save to the path in `config.yaml` under `groundtruth.label_file`.

### 4. Active learning — find the most valuable images to label next

```bash
python -m part2_groundtruth.run --active-learning
```

Once you have at least ~10 labels across 2+ classes, this trains a logistic regression classifier on top of the visual embeddings and uses **margin sampling** (or entropy, configurable) to find the images the model is most uncertain about. Labelling uncertain images is far more efficient than random sampling — each label provides maximum new information.

The command prints cross-validated accuracy on your current labelled set and saves a prioritised list to `outputs/groundtruth/active_learning_suggestions.json`. Annotate those images, add them to your label file, and re-run. Repeat until accuracy plateaus.

---

## Configuration

All behaviour is controlled by `config.yaml`. Key settings:

| Setting | Default | Notes |
|---|---|---|
| `images.input_dir` | `data/images` | **Set this first** |
| `embeddings.model` | `dinov2` | `dinov2` or `clip` |
| `clustering.combine_layout_features` | `true` | blend layout into embeddings |
| `clustering.layout_weight` | `0.2` | raise to weight structure more |
| `clustering.hdbscan.min_cluster_size` | `10` | raise for fewer, larger clusters |
| `vlm_tagging.sample_size` | `500` | images to auto-tag |
| `groundtruth.samples_per_cluster` | `30` | images per cluster for annotation |
| `active_learning.uncertainty_strategy` | `margin` | `margin` or `entropy` |

All intermediate results (embeddings, layout features, clustering, VLM tags) are cached to `outputs/` and reloaded automatically on subsequent runs.
