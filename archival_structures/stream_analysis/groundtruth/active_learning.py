"""
Active learning loop for efficient ground truth expansion.

After you have annotated an initial set of images (from stratified
sampling), this module:

  1. Trains a lightweight classifier on top of the visual embeddings
  2. Predicts class probabilities for all unannotated images
  3. Ranks them by uncertainty (margin or entropy sampling)
  4. Returns a prioritised list of images to annotate next

The cycle repeats: annotate the suggestions → re-run → get new suggestions.
"""

import json
import logging
from pathlib import Path

import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)


class ActiveLearner:
    """
    Lightweight active learner using logistic regression on visual embeddings.

    Args:
        embeddings:  (N, D) float32 array, aligned with image_ids
        image_ids:   list of N image path strings
        config:      pipeline config dict (from AnalysisConfig.to_dict())
    """

    def __init__(
        self,
        embeddings: np.ndarray,
        image_ids: list[str],
        config: dict,
    ) -> None:
        self.embeddings = embeddings
        self.image_ids = image_ids
        self.id_to_idx = {img_id: i for i, img_id in enumerate(image_ids)}
        self.config = config
        self.labels: dict[str, str] = {}   # image_id → label string
        self.encoder = LabelEncoder()

    # ------------------------------------------------------------------
    # Label management
    # ------------------------------------------------------------------

    def load_labels(self, label_file: str) -> None:
        """
        Load manually-created labels from a JSON file.

        Expected format:
            {"path/to/image.jpg": "handwritten_letter", ...}
        """
        if not Path(label_file).exists():
            logger.warning(f"Label file not found: {label_file}")
            return
        with open(label_file) as f:
            self.labels = json.load(f)
        logger.info(f"Loaded {len(self.labels)} labels from {label_file}")

        from collections import Counter
        counts = Counter(self.labels.values())
        for label, count in counts.most_common():
            logger.info(f"  {label}: {count}")

    def save_labels(self, label_file: str) -> None:
        """Save current labels dict to disk."""
        Path(label_file).parent.mkdir(parents=True, exist_ok=True)
        with open(label_file, "w") as f:
            json.dump(self.labels, f, indent=2)
        logger.info(f"Labels saved → {label_file}")

    def add_label(self, image_id: str, label: str) -> None:
        self.labels[image_id] = label

    @property
    def n_labeled(self) -> int:
        return len(self.labels)

    @property
    def unique_classes(self) -> list[str]:
        return sorted(set(self.labels.values()))

    # ------------------------------------------------------------------
    # Classifier training
    # ------------------------------------------------------------------

    def _build_training_data(self) -> tuple[np.ndarray, np.ndarray]:
        """Return (X, y) arrays for the labelled subset."""
        X, y = [], []
        for img_id, label in self.labels.items():
            idx = self.id_to_idx.get(img_id)
            if idx is None:
                continue
            X.append(self.embeddings[idx])
            y.append(label)
        return np.array(X), np.array(y)

    def train(self) -> None:
        """Fit (or re-fit) the classifier on current labels."""
        if len(self.unique_classes) < 2:
            raise ValueError(
                f"Need at least 2 classes to train; got {self.unique_classes}. "
                "Annotate images from more classes first."
            )

        X, y = self._build_training_data()
        self.encoder.fit(y)
        y_enc = self.encoder.transform(y)

        base_clf = LogisticRegression(
            max_iter=1000,
            C=1.0,
            solver="lbfgs",
            multi_class="multinomial",
        )
        self.clf = CalibratedClassifierCV(base_clf, cv=min(5, len(np.unique(y_enc))))
        self.clf.fit(X, y_enc)
        logger.info(
            f"Classifier trained on {len(X)} samples, "
            f"{len(self.unique_classes)} classes: {self.unique_classes}"
        )

    # ------------------------------------------------------------------
    # Uncertainty sampling
    # ------------------------------------------------------------------

    def _margin_uncertainty(self, probs: np.ndarray) -> np.ndarray:
        """Margin sampling: uncertainty = 1 - (p_top1 - p_top2)."""
        sorted_probs = np.sort(probs, axis=1)[:, ::-1]
        margin = sorted_probs[:, 0] - sorted_probs[:, 1]
        return 1.0 - margin

    def _entropy_uncertainty(self, probs: np.ndarray) -> np.ndarray:
        """Shannon entropy of the predicted distribution."""
        eps = 1e-10
        return -(probs * np.log(probs + eps)).sum(axis=1)

    def suggest_next_batch(self) -> list[dict]:
        """
        Train the classifier and return the most uncertain unlabelled images.

        Returns a list of dicts, sorted by descending uncertainty:
            [{"image_id": ..., "uncertainty": ..., "top_prediction": ...,
              "top_prob": ..., "second_prediction": ..., "second_prob": ...}, ...]
        """
        self.train()

        labeled_set = set(self.labels.keys())
        unlabeled_ids = [img_id for img_id in self.image_ids if img_id not in labeled_set]

        if not unlabeled_ids:
            logger.info("All images are labelled — no suggestions needed.")
            return []

        unlabeled_indices = [self.id_to_idx[img_id] for img_id in unlabeled_ids]
        X_unlabeled = self.embeddings[unlabeled_indices]

        probs = self.clf.predict_proba(X_unlabeled)

        strategy = self.config["active_learning"].get("uncertainty_strategy", "margin")
        if strategy == "entropy":
            uncertainty = self._entropy_uncertainty(probs)
        else:
            uncertainty = self._margin_uncertainty(probs)

        sorted_idx = np.argsort(uncertainty)[::-1]
        batch_size = self.config["active_learning"]["batch_size"]
        top_idx = sorted_idx[:batch_size]

        classes = self.encoder.classes_
        suggestions = []
        for i in top_idx:
            top_two = np.argsort(probs[i])[::-1][:2]
            suggestions.append({
                "image_id": unlabeled_ids[i],
                "uncertainty": round(float(uncertainty[i]), 4),
                "top_prediction": classes[top_two[0]],
                "top_prob": round(float(probs[i][top_two[0]]), 4),
                "second_prediction": classes[top_two[1]] if len(top_two) > 1 else None,
                "second_prob": round(float(probs[i][top_two[1]]), 4) if len(top_two) > 1 else None,
            })

        logger.info(
            f"Suggested {len(suggestions)} images to annotate next "
            f"(strategy={strategy}, {len(unlabeled_ids)} unlabelled remaining)"
        )
        return suggestions

    def save_suggestions(self, suggestions: list[dict]) -> None:
        """Save suggestions to the configured file."""
        out_path = self.config["active_learning"]["suggestions_file"]
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(suggestions, f, indent=2)
        logger.info(f"Suggestions saved → {out_path}")

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def cross_val_score(self) -> float:
        """Cross-validated accuracy estimate on the labelled set."""
        from sklearn.model_selection import cross_val_score
        X, y = self._build_training_data()
        if len(np.unique(y)) < 2:
            return 0.0
        base_clf = LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs")
        scores = cross_val_score(base_clf, X, y, cv=min(5, len(X)), scoring="accuracy")
        return float(scores.mean())
