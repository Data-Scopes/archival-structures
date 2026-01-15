import numpy as np
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler
from skimage import color
from PIL import Image
from typing import List
from dataclasses import dataclass

from archival_structures.image.image_base import Thumbnail


def get_average_color(pil_image: Image.Image) -> np.ndarray:
    """Calculates the average color of an image in LAB space."""
    # Convert image to RGB if it isn't already
    rgb_image = pil_image.convert("RGB")
    # Resize to a very small size (e.g., 1x1) to get the average color quickly
    # or just take the mean of the numpy array
    img_array = np.array(rgb_image)
    avg_rgb = img_array.mean(axis=(0, 1)) / 255.0  # Normalize to [0, 1]

    # Convert RGB to LAB for better perceptual clustering
    avg_lab = color.rgb2lab(avg_rgb.reshape(1, 1, 3)).reshape(3)
    return avg_lab


def cluster_thumbnails_by_color(thumbnails: List[Thumbnail], n_clusters: int = 3,
                                random_state: int = 42):
    """Clusters a list of Thumbnail objects based on their dominant color.

    LAB Color Space: By using skimage.color.rgb2lab, the clustering will
    be more sensitive to the "tint" (yellow vs. pink) rather than just
    the brightness.

    Average Color: For thumbnails of documents, the average color is
    usually a very strong proxy for the paper color itself, as the text
    occupies a relatively small percentage of the total pixels.
    """
    # 1. Extract features (average LAB color)
    features = [get_average_color(t.image) for t in thumbnails]
    features_array = np.array(features)

    # 2. Apply K-Means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init='auto')
    labels = kmeans.fit_predict(features_array)

    # 3. Group thumbnails by their cluster label
    clustered_results = {i: [] for i in range(n_clusters)}
    for idx, label in enumerate(labels):
        clustered_results[label].append(thumbnails[idx])

    return clustered_results


def cluster_thumbnails_dbscan(thumbnails: List[Thumbnail], eps: float = 0.5, min_samples: int = 5):
    """
    Clusters thumbnails based on color density using DBSCAN.

    Args:
        thumbnails: List of Thumbnail objects.
        eps: The maximum distance between two samples for one to be considered
             as in the neighborhood of the other. Smaller = more strict clusters.
        min_samples: The number of samples in a neighborhood for a point
                     to be considered as a core point.
    """
    # 1. Extract LAB features
    features = np.array([get_average_color(t.image) for t in thumbnails])

    # 2. Scale features (Important for DBSCAN distance calculations)
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # 3. Apply DBSCAN
    # Metric 'euclidean' works well with LAB space
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    labels = dbscan.fit_predict(features_scaled)

    # 4. Group results
    # label -1 is reserved for 'noise' (outliers that didn't fit a cluster)
    unique_labels = set(labels)
    clustered_results = {label: [] for label in unique_labels}

    for idx, label in enumerate(labels):
        clustered_results[label].append(thumbnails[idx])

    # Log some info about the results
    n_clusters = len([l for l in unique_labels if l != -1])
    n_noise = list(labels).count(-1)
    print(f"DBSCAN found {n_clusters} clusters and {n_noise} outliers.")

    return clustered_results

# Example usage:
# clusters = cluster_thumbnails_by_color(my_thumbnail_list, n_clusters=4)
# for cluster_id, docs in clusters.items():
#     print(f"Cluster {cluster_id} has {len(docs)} documents.")


def get_dominant_colors(pil_image: Image.Image, n_colors: int = 2) -> np.ndarray:
    """
    Extracts the N most dominant colors from an image in LAB space.
    Returns a flattened array of colors [L1, A1, B1, L2, A2, B2, ...].
    """
    # 1. Prepare image data
    img = pil_image.convert("RGB")
    img = img.resize((100, 100))  # Resize for speed; enough for dominant colors
    img_array = np.array(img).reshape(-1, 3) / 255.0

    # 2. Find dominant RGB colors using K-Means locally on the image
    # n_init='auto' for speed, we just need a rough estimate of the paper colors
    local_kmeans = KMeans(n_clusters=n_colors, n_init=3, random_state=42)
    local_kmeans.fit(img_array)
    dominant_rgbs = local_kmeans.cluster_centers_

    # 3. Convert to LAB and sort to ensure consistent feature vectors
    # Sorting by 'L' (lightness) helps ensure that if two images have the same
    # two colors, they appear in the same order in the feature vector.
    dominant_labs = color.rgb2lab(dominant_rgbs.reshape(1, n_colors, 3)).reshape(n_colors, 3)
    dominant_labs = dominant_labs[dominant_labs[:, 0].argsort()]

    return dominant_labs.flatten()

def cluster_thumbnails_multi_color(thumbnails: List[Thumbnail],
                                   n_dominant: int = 2,
                                   eps: float = 0.5,
                                   min_samples: int = 5):
    """
    Clusters thumbnails based on a palette of N dominant colors.
    """
    # 1. Extract feature vectors (e.g., 2 colors = 6 features)
    features = np.array([get_dominant_colors(t.image, n_colors=n_dominant) for t in thumbnails])

    # 2. Scale features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # 3. Cluster using DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    labels = dbscan.fit_predict(features_scaled)

    # 4. Group results
    unique_labels = set(labels)
    clustered_results = {label: [] for label in unique_labels}
    for idx, label in enumerate(labels):
        clustered_results[label].append(thumbnails[idx])

    print(f"DBSCAN found {len(unique_labels) - (1 if -1 in unique_labels else 0)} "
          f"multi-color clusters (using {n_dominant} dominant colors).")

    return clustered_results


def get_weighted_dominant_colors(pil_image: Image.Image, n_colors: int = 2) -> np.ndarray:
    """
    Extracts the N most dominant colors and their relative proportions.
    Returns a flattened array: [L1, A1, B1, W1, L2, A2, B2, W2, ...]
    where W is the weight (percentage of pixels) of that color.
    """
    # 1. Prepare image data
    img = pil_image.convert("RGB")
    img = img.resize((100, 100))
    img_array = np.array(img).reshape(-1, 3) / 255.0

    # 2. Local K-Means
    local_kmeans = KMeans(n_clusters=n_colors, n_init=3, random_state=42)
    labels = local_kmeans.fit_predict(img_array)
    dominant_rgbs = local_kmeans.cluster_centers_

    # 3. Calculate weights (proportions)
    counts = np.bincount(labels, minlength=n_colors)
    weights = counts / len(labels)

    # 4. Convert colors to LAB
    dominant_labs = color.rgb2lab(dominant_rgbs.reshape(1, n_colors, 3)).reshape(n_colors, 3)

    # 5. Combine color + weight into a single matrix for sorting
    # Each row: [L, A, B, Weight]
    combined = np.column_stack((dominant_labs, weights))

    # 6. Sort by Lightness (L) to keep feature vectors consistent across images
    combined = combined[combined[:, 0].argsort()]

    return combined.flatten()


def cluster_thumbnails_weighted(thumbnails: List[Thumbnail],
                                n_dominant: int = 2,
                                eps: float = 0.5,
                                min_samples: int = 5):
    """
    Clusters thumbnails based on weighted color palettes.
    """
    # 1. Extract feature vectors including weights
    features = np.array([get_weighted_dominant_colors(t.image, n_colors=n_dominant)
                         for t in thumbnails])

    # 2. Scale features
    # This is critical here because weights (0 to 1) and LAB values (0 to 100)
    # are on very different scales.
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # 3. Cluster using DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    labels = dbscan.fit_predict(features_scaled)

    # 4. Group results
    unique_labels = set(labels)
    clustered_results = {label: [] for label in unique_labels}
    for idx, label in enumerate(labels):
        clustered_results[label].append(thumbnails[idx])

    # Log some info about the results
    n_clusters = len([l for l in unique_labels if l != -1])
    n_noise = list(labels).count(-1)
    print(f"DBSCAN found {n_clusters} weighted multi-colour clusters and "
          f"{n_noise} outliers (using {n_dominant} dominant colors)..")

    return clustered_results

