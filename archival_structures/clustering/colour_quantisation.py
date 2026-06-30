"""Whole-image colour quantisation via k-means (adapted from scikit-learn's colour
quantisation example).

Reduces an image to its `n_colours` most representative colours. Considered a dead end by the
package author for ink/text-colour classification specifically -- see
`archival_structures.clustering.colour_clustering.find_ink_luminosity_class` (multiotsu +
connected-component shape) and `score_multi_colour_text` (chroma spread), which discriminate
ink-vs-paper and single-vs-multiple ink colours far more reliably. `cluster_main_image_colours`
here is still used by `colour_clustering.classify_dominant_colours` and
`determine_number_of_colours` for general whole-image colour clustering.
"""

# Code is copied and modified from: https://scikit-learn.org/1.5/auto_examples/cluster/plot_color_quantization.html
# Authors: Robert Layton <robertlayton@gmail.com>
#          Olivier Grisel <olivier.grisel@ensta.org>
#          Mathieu Blondel <mathieu@mblondel.org>
#
# License: BSD 3 clause

import numpy as np
import numpy.typing as npt
import skimage
from sklearn.cluster import KMeans
from sklearn.datasets import load_sample_image
from sklearn.utils import shuffle


def get_image_array(image: npt.NDArray) -> npt.NDArray:
    """Transform image to a 2D numpy array."""
    h, w, d = tuple(image.shape)
    assert d == 3
    return np.reshape(image, (w * h, d))


def recreate_image(codebook, labels, w, h):
    """Recreate the (compressed) image from the code book & labels"""
    return codebook[labels].reshape(h, w, -1)


def quantise_image_colours(image: npt.NDArray, n_colours: int, convert_to_lab: bool = False):
    """Recreate `image` using only its `n_colours` k-means colour-cluster centroids (a
    posterised version of `image`). If `convert_to_lab`, clusters in LAB space and converts the
    result back to RGB; otherwise clusters directly in `image`'s own colour space."""
    image = skimage.color.rgb2lab(image) if convert_to_lab is True else image
    h, w, d = tuple(image.shape)
    kmeans, labels = cluster_main_image_colours(image, n_colours)
    # recreate the image with the reduced number of colours
    rec_image = kmeans.cluster_centers_[labels].reshape(h, w, -1)
    # convert LAB back to RGB
    if convert_to_lab is True:
        return skimage.color.lab2rgb(rec_image)
    else:
        return rec_image


def cluster_main_image_colours(image: npt.NDArray, n_colours: int, random_state: int = 28590,
                               n_samples: int = 1_000):
    """K-means cluster `image` into `n_colours` colours, fitting on a random sample of
    `n_samples` pixels (for speed) but predicting cluster labels for every pixel. Returns
    `(kmeans, labels)`: the fitted `KMeans` model and a per-pixel cluster-id array."""
    image_array = get_image_array(image)
    # Fitting model on a small sub-sample of the data
    image_array_sample = shuffle(image_array, random_state=random_state, n_samples=n_samples)
    kmeans = KMeans(n_clusters=n_colours, random_state=random_state).fit(image_array_sample)
    # Get labels for all points
    # Predicting color indices on the full image (k-means)
    labels = kmeans.predict(image_array)
    return kmeans, labels


def get_image_colours(image: npt.NDArray):
    """Set of every distinct pixel colour (as a tuple) appearing in `image`."""
    image_array = get_image_array(image)
    return set([tuple(row) for row in image_array])
